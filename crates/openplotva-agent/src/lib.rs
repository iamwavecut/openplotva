//! Thin, provider-neutral agent-loop engine.
//!
//! The engine advances a fully serializable run state one discrete step at a
//! time (`advance_one_step`). The caller persists the returned state after each
//! step, which makes runs durable and resumable: a crashed worker reloads the
//! last checkpoint and continues from `step_index` without re-executing
//! completed tool calls. The engine never performs IO itself — model calls and
//! tool dispatch go through the `Reasoner` and `AgentTools` trait seams, which
//! the composition root implements over the real LLM client and tool box.

use std::future::Future;
use std::pin::Pin;

use openplotva_dialog::{
    ChatCompletionTool, NativeToolCall, ToolContext, ToolResult, ToolStep,
    chat_completion_tools_for_names, parse_native_tool_step,
};
use serde::{Deserialize, Serialize};

/// Version marker for the serialized [`AgentState`] shape.
pub const AGENT_STATE_SCHEMA: u16 = 1;

/// Static description of an agent flow. Not serialized; resolved by id on resume.
#[derive(Clone, Debug)]
pub struct AgentProfile {
    /// Stable profile id, e.g. `"search"`.
    pub id: String,
    /// Rendered reasoner system prompt (loaded from a `.prompt` file by the app).
    pub system_prompt: String,
    /// Tool names the reasoner may call (subset of the dialog catalog).
    pub allowed_tools: Vec<String>,
    /// Model name for the reasoning/orchestration calls.
    pub reasoner_model: String,
    /// Model name for the final user-facing synthesis call.
    pub writer_model: String,
    /// Run budgets enforced by the engine.
    pub budgets: AgentBudgets,
    /// Per-call cap for reasoner completions.
    pub reasoner_max_tokens: i32,
    /// Per-call cap for the writer completion.
    pub writer_max_tokens: i32,
}

impl AgentProfile {
    fn tool_specs(&self) -> Vec<ChatCompletionTool> {
        let names: Vec<&str> = self.allowed_tools.iter().map(String::as_str).collect();
        chat_completion_tools_for_names(&names)
    }

    fn allows(&self, tool: &str) -> bool {
        self.allowed_tools.iter().any(|name| name == tool)
    }
}

/// Hard ceilings for a run. All survive resume because they live in the state
/// or are derived from the absolute `started_at_unix_ms` anchor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentBudgets {
    /// Maximum reasoner round-trips before the run stops.
    pub max_steps: u32,
    /// Maximum cumulative prompt+completion tokens across reasoner calls.
    pub max_total_tokens: u64,
    /// Wall-clock ceiling as an absolute deadline (downtime counts). `0` disables.
    pub max_wall_ms: u64,
    /// Maximum total tool calls. `0` disables.
    pub max_tool_calls: u32,
    /// Consecutive retryable tool errors before the run stops. `0` disables.
    pub max_tool_errors: u32,
}

/// Telegram routing seed, copied into each tool's [`ToolContext`].
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentOrigin {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub thread_id: Option<i32>,
    pub user_full_name: String,
}

impl AgentOrigin {
    #[must_use]
    pub fn to_tool_context(&self) -> ToolContext {
        ToolContext {
            chat_id: self.chat_id,
            thread_id: self.thread_id,
            message_id: self.message_id,
            user_id: self.user_id,
            user_full_name: self.user_full_name.clone(),
            ..ToolContext::default()
        }
    }
}

/// The durable, serializable run state — the checkpoint payload.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct AgentState {
    pub schema: u16,
    pub profile_id: String,
    pub goal: String,
    pub origin: AgentOrigin,
    /// Index of the next step to execute; completed steps are `< step_index`.
    pub step_index: u32,
    pub steps: Vec<AgentStep>,
    pub observations: Vec<AgentObservation>,
    pub scratchpad: String,
    pub tokens_spent: u64,
    pub tool_calls_made: u32,
    pub consecutive_tool_errors: u32,
    /// Absolute run start (unix ms); the wall-time budget is measured from here.
    pub started_at_unix_ms: u64,
    /// Set exactly once when the run reaches a terminal state.
    pub outcome: Option<AgentOutcome>,
}

impl AgentState {
    #[must_use]
    pub fn new(
        profile_id: impl Into<String>,
        goal: impl Into<String>,
        origin: AgentOrigin,
        started_at_unix_ms: u64,
    ) -> Self {
        Self {
            schema: AGENT_STATE_SCHEMA,
            profile_id: profile_id.into(),
            goal: goal.into(),
            origin,
            step_index: 0,
            steps: Vec::new(),
            observations: Vec::new(),
            scratchpad: String::new(),
            tokens_spent: 0,
            tool_calls_made: 0,
            consecutive_tool_errors: 0,
            started_at_unix_ms,
            outcome: None,
        }
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.outcome.is_some()
    }

    /// Index of the last committed step (`-1` when nothing is committed yet),
    /// used as the durable checkpoint cursor.
    #[must_use]
    pub fn committed_step(&self) -> i32 {
        i32::try_from(self.step_index).unwrap_or(i32::MAX) - 1
    }
}

/// One journaled decision and its action.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentStep {
    pub index: u32,
    pub thought: String,
    pub action: AgentAction,
}

/// What the reasoner decided to do on a step.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentAction {
    /// Call a tool with the parsed dialog `ToolStep` (boxed: it is much larger
    /// than the `Finish` variant).
    CallTool { step: Box<ToolStep> },
    /// Finish with the reasoner's final text (no tool call).
    Finish { final_text: String },
}

/// A tool result journaled against the step that produced it.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentObservation {
    pub step_index: u32,
    pub tool: String,
    pub result: ToolResult,
}

/// Terminal outcome of a run.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AgentOutcome {
    /// The reasoner produced a final answer (pre-writer synthesis).
    Completed { answer: String },
    /// A budget ceiling stopped the run; `partial` carries best-effort context.
    Stopped { reason: StopReason, partial: String },
    /// The run failed unrecoverably.
    Failed { reason: String },
}

/// Why a run stopped before the reasoner signalled completion.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    MaxSteps,
    MaxTokens,
    MaxWallTime,
    MaxToolCalls,
    MaxToolErrors,
}

impl StopReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MaxSteps => "max_steps",
            Self::MaxTokens => "max_tokens",
            Self::MaxWallTime => "max_wall_time",
            Self::MaxToolCalls => "max_tool_calls",
            Self::MaxToolErrors => "max_tool_errors",
        }
    }
}

/// Provider-neutral chat role for messages handed to the reasoner adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentRole {
    System,
    User,
    Assistant,
    Tool,
}

/// A provider-neutral message the engine builds from state for the reasoner.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentMessage {
    pub role: AgentRole,
    pub content: String,
    /// Tool name for `Tool` role messages.
    pub tool_name: Option<String>,
}

impl AgentMessage {
    #[must_use]
    pub fn new(role: AgentRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
            tool_name: None,
        }
    }

    fn tool(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: AgentRole::Tool,
            content: content.into(),
            tool_name: Some(name.into()),
        }
    }
}

/// A reasoner request assembled by the engine; the adapter maps it to the LLM.
pub struct ReasonerCall {
    pub model: String,
    pub max_tokens: i32,
    pub messages: Vec<AgentMessage>,
    pub tools: Vec<ChatCompletionTool>,
}

/// A reasoner reply the adapter extracts from the LLM response.
#[derive(Clone, Debug, Default)]
pub struct ReasonerReply {
    pub text: String,
    pub tool_calls: Vec<NativeToolCall>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

impl ReasonerReply {
    #[must_use]
    pub fn total_tokens(&self) -> u64 {
        self.prompt_tokens.saturating_add(self.completion_tokens)
    }
}

/// Boxed future returned by the reasoner seam.
pub type ReasonerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ReasonerReply, AgentError>> + Send + 'a>>;

/// Model-call seam: one chat round-trip with optional tool calling.
pub trait Reasoner: Send + Sync {
    fn complete<'a>(&'a self, call: ReasonerCall) -> ReasonerFuture<'a>;
}

/// Boxed future returned by the tool seam.
pub type ToolDispatchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ToolResult, AgentError>> + Send + 'a>>;

/// Tool-dispatch seam: execute one tool step against the real tool box.
pub trait AgentTools: Send + Sync {
    fn dispatch<'a>(&'a self, ctx: ToolContext, step: ToolStep) -> ToolDispatchFuture<'a>;
}

/// Engine-level errors.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AgentError {
    #[error("reasoner failed: {0}")]
    Reasoner(String),
    #[error("tool dispatch failed: {0}")]
    ToolDispatch(String),
    #[error("tool parse failed: {0}")]
    ToolParse(String),
}

/// Result of advancing one step.
pub enum StepProgress {
    /// The run should continue; the caller checkpoints and advances again.
    Continue(AgentState),
    /// The run reached a terminal state (`outcome` is set).
    Terminal(AgentState),
}

/// Advance the run by exactly one step: budget check → one reasoner call →
/// either finish, or dispatch the requested tool and journal the observation.
/// The returned state is the next durable checkpoint.
pub async fn advance_one_step(
    profile: &AgentProfile,
    reasoner: &dyn Reasoner,
    tools: &dyn AgentTools,
    mut state: AgentState,
    now_ms: u64,
) -> Result<StepProgress, AgentError> {
    if let Some(reason) = check_budgets(&profile.budgets, &state, now_ms) {
        let partial = latest_observation_text(&state);
        state.outcome = Some(AgentOutcome::Stopped { reason, partial });
        return Ok(StepProgress::Terminal(state));
    }

    let reply = reasoner
        .complete(ReasonerCall {
            model: profile.reasoner_model.clone(),
            max_tokens: profile.reasoner_max_tokens,
            messages: build_messages(profile, &state),
            tools: profile.tool_specs(),
        })
        .await?;
    state.tokens_spent = state.tokens_spent.saturating_add(reply.total_tokens());

    if reply.tool_calls.is_empty() {
        let answer = reply.text.trim().to_owned();
        state.steps.push(AgentStep {
            index: state.step_index,
            thought: reply.text,
            action: AgentAction::Finish {
                final_text: answer.clone(),
            },
        });
        state.step_index += 1;
        state.outcome = Some(AgentOutcome::Completed { answer });
        return Ok(StepProgress::Terminal(state));
    }

    let step = match parse_native_tool_step(&reply.tool_calls) {
        Ok(step) => step,
        Err(error) => {
            // A malformed tool call is recoverable: record it so the reasoner can
            // see the error and retry, and count it toward the tool-error budget.
            let tool = reply
                .tool_calls
                .first()
                .map(|call| call.function.name.clone())
                .unwrap_or_default();
            let result = ToolResult::failed("tool_parse_error", error.to_string());
            journal_tool_step(
                &mut state,
                reply.text,
                ToolStep {
                    step: tool,
                    ..ToolStep::default()
                },
                result,
            );
            state.consecutive_tool_errors += 1;
            return Ok(StepProgress::Continue(state));
        }
    };

    if !profile.allows(&step.step) {
        let result = ToolResult::failed(
            "tool_not_allowed",
            format!(
                "tool `{}` is not allowed in profile `{}`",
                step.step, profile.id
            ),
        );
        journal_tool_step(&mut state, reply.text, step, result);
        return Ok(StepProgress::Continue(state));
    }

    let result = tools
        .dispatch(state.origin.to_tool_context(), step.clone())
        .await?;
    let retryable_error = result.error.as_ref().is_some_and(|error| error.retryable);
    journal_tool_step(&mut state, reply.text, step, result);
    state.tool_calls_made += 1;
    state.consecutive_tool_errors = if retryable_error {
        state.consecutive_tool_errors + 1
    } else {
        0
    };
    Ok(StepProgress::Continue(state))
}

/// Render the gathered evidence as plain text for the writer synthesis call.
#[must_use]
pub fn render_evidence(state: &AgentState) -> String {
    let mut out = String::new();
    for observation in &state.observations {
        out.push_str(&format!(
            "## Tool `{}` (step {})\n{}\n\n",
            observation.tool,
            observation.step_index,
            render_observation(&observation.result)
        ));
    }
    out.trim_end().to_owned()
}

fn journal_tool_step(state: &mut AgentState, thought: String, step: ToolStep, result: ToolResult) {
    let tool = step.step.clone();
    state.steps.push(AgentStep {
        index: state.step_index,
        thought,
        action: AgentAction::CallTool {
            step: Box::new(step),
        },
    });
    state.observations.push(AgentObservation {
        step_index: state.step_index,
        tool,
        result,
    });
    state.step_index += 1;
}

fn check_budgets(budgets: &AgentBudgets, state: &AgentState, now_ms: u64) -> Option<StopReason> {
    if state.step_index >= budgets.max_steps {
        return Some(StopReason::MaxSteps);
    }
    if budgets.max_total_tokens > 0 && state.tokens_spent >= budgets.max_total_tokens {
        return Some(StopReason::MaxTokens);
    }
    if budgets.max_tool_calls > 0 && state.tool_calls_made >= budgets.max_tool_calls {
        return Some(StopReason::MaxToolCalls);
    }
    if budgets.max_tool_errors > 0 && state.consecutive_tool_errors >= budgets.max_tool_errors {
        return Some(StopReason::MaxToolErrors);
    }
    if budgets.max_wall_ms > 0 {
        let elapsed = now_ms.saturating_sub(state.started_at_unix_ms);
        if elapsed >= budgets.max_wall_ms {
            return Some(StopReason::MaxWallTime);
        }
    }
    None
}

fn build_messages(profile: &AgentProfile, state: &AgentState) -> Vec<AgentMessage> {
    let mut messages = Vec::with_capacity(2 + state.steps.len() * 2);
    messages.push(AgentMessage::new(
        AgentRole::System,
        profile.system_prompt.clone(),
    ));
    messages.push(AgentMessage::new(AgentRole::User, state.goal.clone()));
    for step in &state.steps {
        let AgentAction::CallTool { step: tool_step } = &step.action else {
            continue;
        };
        let call_note = format!(
            "Called tool `{}` with {}",
            tool_step.step,
            tool_step_args_summary(tool_step)
        );
        let assistant = if step.thought.trim().is_empty() {
            call_note
        } else {
            format!("{}\n{}", step.thought.trim(), call_note)
        };
        messages.push(AgentMessage::new(AgentRole::Assistant, assistant));
        if let Some(observation) = state
            .observations
            .iter()
            .find(|observation| observation.step_index == step.index)
        {
            messages.push(AgentMessage::tool(
                tool_step.step.clone(),
                render_observation(&observation.result),
            ));
        }
    }
    messages
}

fn tool_step_args_summary(step: &ToolStep) -> String {
    let mut parts = Vec::new();
    let mut push = |label: &str, value: &str| {
        if !value.trim().is_empty() {
            parts.push(format!("{label}={value}"));
        }
    };
    push("query", &step.query);
    push("url", &step.url);
    push("prompt", &step.prompt);
    push("topic", &step.topic);
    push("text", &step.text);
    push("video", &step.video);
    if parts.is_empty() {
        "(no arguments)".to_owned()
    } else {
        parts.join(", ")
    }
}

fn render_observation(result: &ToolResult) -> String {
    let mut out = String::new();
    if !result.status.trim().is_empty() {
        out.push_str(&format!("status: {}\n", result.status));
    }
    if !result.message.trim().is_empty() {
        out.push_str(&format!("{}\n", result.message));
    }
    if let Some(error) = &result.error {
        out.push_str(&format!(
            "error[{}]: {}{}\n",
            error.code,
            error.reason,
            if error.retryable { " (retryable)" } else { "" }
        ));
    }
    if let Some(data) = &result.data {
        let rendered = serde_json::to_string(data).unwrap_or_else(|_| data.to_string());
        out.push_str(&rendered);
        out.push('\n');
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        "(empty result)".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn latest_observation_text(state: &AgentState) -> String {
    state
        .observations
        .last()
        .map(|observation| render_observation(&observation.result))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_dialog::{NativeToolFunction, STEP_CRAWL_URL, STEP_WEB_SEARCH};
    use serde_json::json;
    use std::sync::Mutex;

    struct ScriptedReasoner {
        replies: Mutex<Vec<ReasonerReply>>,
    }

    impl ScriptedReasoner {
        fn new(replies: Vec<ReasonerReply>) -> Self {
            Self {
                replies: Mutex::new(replies.into_iter().rev().collect()),
            }
        }
    }

    impl Reasoner for ScriptedReasoner {
        fn complete<'a>(&'a self, _call: ReasonerCall) -> ReasonerFuture<'a> {
            let reply = self
                .replies
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .pop();
            Box::pin(async move {
                reply.ok_or_else(|| AgentError::Reasoner("no scripted reply".to_owned()))
            })
        }
    }

    struct EchoTools;

    impl AgentTools for EchoTools {
        fn dispatch<'a>(&'a self, _ctx: ToolContext, step: ToolStep) -> ToolDispatchFuture<'a> {
            Box::pin(async move {
                Ok(ToolResult {
                    status: "ok".to_owned(),
                    message: format!("result for {}", step.step),
                    data: Some(json!({ "query": step.query, "url": step.url })),
                    ..ToolResult::default()
                })
            })
        }
    }

    fn search_call(query: &str) -> NativeToolCall {
        NativeToolCall {
            id: "call_1".to_owned(),
            call_type: "function".to_owned(),
            function: NativeToolFunction {
                name: STEP_WEB_SEARCH.to_owned(),
                arguments: json!({ "query": query }),
            },
        }
    }

    fn profile() -> AgentProfile {
        AgentProfile {
            id: "search".to_owned(),
            system_prompt: "You are a search agent.".to_owned(),
            allowed_tools: vec![STEP_WEB_SEARCH.to_owned(), STEP_CRAWL_URL.to_owned()],
            reasoner_model: "qwen".to_owned(),
            writer_model: "conversational".to_owned(),
            budgets: AgentBudgets {
                max_steps: 8,
                max_total_tokens: 100_000,
                max_wall_ms: 120_000,
                max_tool_calls: 6,
                max_tool_errors: 3,
            },
            reasoner_max_tokens: 2048,
            writer_max_tokens: 2048,
        }
    }

    fn state() -> AgentState {
        AgentState::new("search", "latest rust news", AgentOrigin::default(), 1_000)
    }

    async fn run_to_terminal(
        profile: &AgentProfile,
        reasoner: &dyn Reasoner,
        tools: &dyn AgentTools,
        mut state: AgentState,
    ) -> AgentState {
        loop {
            match advance_one_step(profile, reasoner, tools, state, 2_000)
                .await
                .expect("step")
            {
                StepProgress::Continue(next) => state = next,
                StepProgress::Terminal(next) => return next,
            }
        }
    }

    #[tokio::test]
    async fn loop_calls_tool_then_finishes() {
        let reasoner = ScriptedReasoner::new(vec![
            ReasonerReply {
                text: "searching".to_owned(),
                tool_calls: vec![search_call("rust")],
                prompt_tokens: 10,
                completion_tokens: 5,
            },
            ReasonerReply {
                text: "Here is the answer.".to_owned(),
                tool_calls: Vec::new(),
                prompt_tokens: 8,
                completion_tokens: 4,
            },
        ]);
        let final_state = run_to_terminal(&profile(), &reasoner, &EchoTools, state()).await;

        assert_eq!(final_state.step_index, 2);
        assert_eq!(final_state.tool_calls_made, 1);
        assert_eq!(final_state.observations.len(), 1);
        assert_eq!(final_state.tokens_spent, 27);
        assert!(matches!(
            final_state.outcome,
            Some(AgentOutcome::Completed { answer }) if answer == "Here is the answer."
        ));
    }

    #[tokio::test]
    async fn disallowed_tool_is_recorded_not_dispatched() {
        let mut bad = search_call("x");
        bad.function.name = "draw_image".to_owned();
        bad.function.arguments = json!({ "prompt": "a cat" });
        let reasoner = ScriptedReasoner::new(vec![
            ReasonerReply {
                tool_calls: vec![bad],
                ..ReasonerReply::default()
            },
            ReasonerReply {
                text: "done".to_owned(),
                ..ReasonerReply::default()
            },
        ]);
        let final_state = run_to_terminal(&profile(), &reasoner, &EchoTools, state()).await;
        let observation = &final_state.observations[0];
        assert_eq!(observation.tool, "draw_image");
        let error = observation.result.error.as_ref().expect("error");
        assert_eq!(error.code, "tool_not_allowed");
        // A rejected tool is not counted as a dispatched tool call.
        assert_eq!(final_state.tool_calls_made, 0);
    }

    #[tokio::test]
    async fn budget_stops_run_before_reasoner_call() {
        let mut profile = profile();
        profile.budgets.max_steps = 1;
        // First reply consumes the only allowed step; the second pre-check trips.
        let reasoner = ScriptedReasoner::new(vec![ReasonerReply {
            tool_calls: vec![search_call("rust")],
            ..ReasonerReply::default()
        }]);
        let final_state = run_to_terminal(&profile, &reasoner, &EchoTools, state()).await;
        assert!(matches!(
            final_state.outcome,
            Some(AgentOutcome::Stopped {
                reason: StopReason::MaxSteps,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn resumes_from_serialized_state_without_repeating_steps() {
        let reasoner = ScriptedReasoner::new(vec![
            ReasonerReply {
                tool_calls: vec![search_call("rust")],
                prompt_tokens: 3,
                completion_tokens: 2,
                ..ReasonerReply::default()
            },
            ReasonerReply {
                text: "final".to_owned(),
                ..ReasonerReply::default()
            },
        ]);
        let profile = profile();

        // Run one step, then serialize and drop the in-memory state (a "crash").
        let after_one = match advance_one_step(&profile, &reasoner, &EchoTools, state(), 2_000)
            .await
            .expect("first step")
        {
            StepProgress::Continue(next) => next,
            StepProgress::Terminal(_) => panic!("did not expect terminal"),
        };
        let serialized = serde_json::to_string(&after_one).expect("serialize");
        assert_eq!(after_one.committed_step(), 0);

        // Resume from the serialized checkpoint; the completed step is not re-run.
        let resumed: AgentState = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(resumed.observations.len(), 1);
        let final_state = run_to_terminal(&profile, &reasoner, &EchoTools, resumed).await;
        assert_eq!(final_state.tool_calls_made, 1);
        assert_eq!(final_state.observations.len(), 1);
        assert!(matches!(
            final_state.outcome,
            Some(AgentOutcome::Completed { .. })
        ));
    }
}
