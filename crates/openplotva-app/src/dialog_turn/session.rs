//! The dialog session engine: one turn = an agent session of single-shot chat
//! steps with engine-owned tool execution.
//!
//! Loop protocol (binding decisions of the agentic-dialog plan): assistant
//! text WITH tool calls is sent to the chat immediately and the loop
//! continues; tool calls alone execute and continue; text WITHOUT tool calls
//! is the final answer and ends the turn. `send_message` posts a deliberate
//! intermediate message; `react_to_message` sets a semantic emoji reaction —
//! both are engine-intercepted and never reach the toolbox. A successfully
//! QUEUED generation side effect (draw/song) terminates the session
//! immediately: announcing happens before or with the call, never after.
//!
//! Every branch returns a `TurnResolution`; `finalize_turn` stays the sole
//! writer of status/event/ledger. Job-level `Requeue` is reachable only while
//! nothing was sent — after the first accepted send, failures are terminal
//! (never replay a partially delivered session).

use std::collections::BTreeSet;
use std::{collections::BTreeMap, future::Future, pin::Pin, time::Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use openplotva_core::ToolCall;
use openplotva_dialog::{
    ChatStepRequest, ChatStepToolCall, DialogInput, DialogToolbox, HistoryMessage,
    SESSION_REACT_TO_MESSAGE_SPEC, SESSION_REACTION_ALLOWED_EMOJI, SESSION_SEND_MESSAGE_SPEC,
    STEP_CRAWL_URL, STEP_DRAW_IMAGE, STEP_GENERATE_SONG, STEP_REACT_TO_MESSAGE, STEP_SEND_MESSAGE,
    STEP_UNDERSTAND_MEDIA, STEP_WEB_SEARCH, SessionMessage, SessionToolCall, ToolContext,
    ToolContinuation, ToolResult, ToolStep, ToolsMode, chat_completion_tools_for_specs,
    dialog_tool_context, dialog_tool_continuation, dispatch_dialog_tool, tool_call_arguments,
    turn::{
        ANTI_LOOP_HINT, QueuedSideEffect, SIDE_EFFECT_KIND_IMAGE, SIDE_EFFECT_KIND_MUSIC,
        SIDE_EFFECT_STATE_QUEUED,
    },
};
use openplotva_llm::ChatStepProvider;
use openplotva_taskman::TaskQueueJobEvent;
use serde_json::Value;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

use super::budget::{
    SESSION_TOOL_BUDGET_EXTENSION_GRANTED_KEY, SESSION_TOOL_STAGE, SessionBudget, TURN_DEADLINE,
    TurnBudget,
};
use super::engine::{ANSWER_QUEUED_STAGE, ANSWER_SENT_STAGE, DIALOG_TURN_REGENERATE_STAGE};
use super::links::DialogLinks;
use super::outcome::{JobDisposition, TurnOutcome, TurnResolution, UserSignalPlan};
use crate::dialog_jobs::{
    DialogAnswerSendOptions, DialogJobEffects, DialogJobWorkerQueue, DialogJobWorkerReport,
    DialogToolCallHistoryStore, PROVIDER_EMPTY_RETRY_CODES, PROVIDER_ERROR_RETRY_CODES,
    RetryableDialogProviderFailure, SANITIZED_EMPTY_RETRY_CODES, UNDELIVERABLE_RETRY_CODES,
    handle_retryable_dialog_provider_error, persist_dialog_tool_calls,
    should_suppress_duplicate_bot_reply, validate_dialog_answer_deliverable,
};

/// Job event stage appended after the FIRST outbound send of a session; on
/// re-entry (crash between a send and the status write) the turn resolves
/// `Sent` without replaying anything.
pub const SESSION_MESSAGE_SENT_STAGE: &str = "session_message_sent";
pub const SESSION_INTERMEDIATE_QUEUED_STAGE: &str = "session_intermediate_queued";

/// Job event stage recorded per session LLM iteration (audit only).
pub const SESSION_ITERATION_STAGE: &str = "session_iteration";

/// Job event stage recording why a tool-call batch continued or completed.
pub const SESSION_BATCH_STAGE: &str = "session_batch";

/// A duplicate final answer is regenerated only with this much budget left.
const MIN_REGENERATION_BUDGET: TimeDuration = TimeDuration::seconds(10);

/// A searched answer gets two focused rewrites before the best available final
/// answer is sent, even if the model still omitted a citation.
const MAX_SEARCH_CITATION_REPAIRS: i32 = 2;

const SEARCH_CITATION_HINT: &str = include_str!("../../../../prompts/chat/search_citations.prompt");

const SEARCH_CITATION_REPAIR_HINT: &str = "ОБЯЗАТЕЛЬНАЯ ПРОВЕРКА ИСТОЧНИКОВ: предыдущий черновик финального ответа не содержит требуемой inline-ссылки на реально найденный источник. Перепиши финальный ответ без нового поиска и без упоминания этой проверки. Если ты используешь сведения из web_search/crawl_url, ответ ОБЯЗАН содержать хотя бы одну семантическую inline HTML-ссылку вида <a href=\"URL\">подтверждаемая фраза</a>, где href в точности совпадает с одним из URL в уже имеющихся результатах. Размещай ссылку прямо на подтверждаемом утверждении; не печатай raw URL или отдельную библиографию.";

/// Slice reserved for the final send after a tool call finishes.
const TOOL_RESERVE: TimeDuration = TimeDuration::seconds(20);

/// Submit independent media reads together. The routing capacity pools still
/// serialize calls that land on the same single-slot GPU.
const MAX_PARALLEL_UNDERSTAND_MEDIA_CALLS: usize = 4;

/// Boxed future for semantic reaction execution.
pub type SessionReactionFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Executes the `react_to_message` tool: one bounded `setMessageReaction`.
pub trait SessionReactor: Send + Sync {
    fn react<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
        emoji: &'a str,
    ) -> SessionReactionFuture<'a>;
}

/// Session-engine wiring threaded from the worker options.
#[derive(Clone, Copy)]
pub struct SessionTurnConfig<'a> {
    pub toolbox: &'a dyn DialogToolbox,
    /// `react_to_message` executor; `None` fails the tool gracefully.
    pub reactor: Option<&'a dyn SessionReactor>,
    pub gradius: Option<&'a dyn crate::gradius_ads::GradiusAdAppender>,
    pub max_iterations: i32,
    pub max_messages: i32,
    pub tool_extension_secs: i32,
    pub hard_cap_secs: i32,
    pub max_draws: i32,
    pub max_songs: i32,
}

/// Owned session-engine wiring held by the worker composition root; each
/// turn borrows a [`SessionTurnConfig`] view of it.
pub struct SessionWorkerWiring {
    pub toolbox: std::sync::Arc<dyn DialogToolbox>,
    pub reactor: Option<std::sync::Arc<dyn SessionReactor>>,
    pub gradius: Option<std::sync::Arc<dyn crate::gradius_ads::GradiusAdAppender>>,
    /// Per-(chat, thread) serialization + initiator injection.
    pub registry: std::sync::Arc<super::inbox::DialogSessionRegistry>,
    pub max_iterations: i32,
    pub max_messages: i32,
    pub tool_extension_secs: i32,
    pub hard_cap_secs: i32,
    pub max_draws: i32,
    pub max_songs: i32,
}

impl SessionWorkerWiring {
    #[must_use]
    pub fn turn_config(&self) -> SessionTurnConfig<'_> {
        SessionTurnConfig {
            toolbox: self.toolbox.as_ref(),
            reactor: self.reactor.as_deref(),
            gradius: self.gradius.as_deref(),
            max_iterations: self.max_iterations,
            max_messages: self.max_messages,
            tool_extension_secs: self.tool_extension_secs,
            hard_cap_secs: self.hard_cap_secs,
            max_draws: self.max_draws,
            max_songs: self.max_songs,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SentLog {
    texts: Vec<String>,
    #[serde(default)]
    reply_targets: Vec<i32>,
    intermediate_count: u32,
    total_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionBatchDisposition {
    ContinueForResults,
    CompleteWithSideEffect,
    ContinueWithoutFinal,
}

impl SessionBatchDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ContinueForResults => "continue_for_results",
            Self::CompleteWithSideEffect => "complete_with_side_effect",
            Self::ContinueWithoutFinal => "continue_without_final",
        }
    }
}

impl SentLog {
    fn new() -> Self {
        Self {
            texts: Vec::new(),
            reply_targets: Vec::new(),
            intermediate_count: 0,
            total_count: 0,
        }
    }

    /// Match either one visible delivery or the model's ordered concatenation
    /// of every delivery made so far, independent of HTML-only differences.
    fn matches_delivery(&self, text: &str) -> bool {
        let normalized = canonical_visible_text(text);
        self.texts.contains(&normalized)
            || (self.texts.len() > 1 && self.texts.join(" ") == normalized)
    }

    fn matches_target_delivery(&self, text: &str, message_id: i32) -> bool {
        let normalized = canonical_visible_text(text);
        self.texts
            .iter()
            .zip(&self.reply_targets)
            .any(|(text, target)| *target == message_id && *text == normalized)
    }

    fn record(&mut self, text: &str, intermediate: bool, message_id: i32) {
        self.reply_targets.resize(self.texts.len(), 0);
        self.texts.push(canonical_visible_text(text));
        self.reply_targets.push(message_id);
        self.total_count += 1;
        if intermediate {
            self.intermediate_count += 1;
        }
    }

    fn any(&self) -> bool {
        self.total_count > 0
    }
}

fn canonical_visible_text(text: &str) -> String {
    openplotva_telegram::strip_telegram_html(text)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn collect_web_source_urls(result: &ToolResult, urls: &mut BTreeSet<String>) {
    if let Some(data) = result.data.as_ref() {
        collect_web_source_urls_from_value(data, urls);
    }
}

fn collect_web_source_urls_from_value(value: &Value, urls: &mut BTreeSet<String>) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                if matches!(key.as_str(), "link" | "url")
                    && let Some(url) = value.as_str()
                    && is_http_url(url)
                {
                    urls.insert(url.to_owned());
                }
                collect_web_source_urls_from_value(value, urls);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_web_source_urls_from_value(value, urls);
            }
        }
        Value::String(serialized) => {
            let serialized = serialized.trim();
            if matches!(serialized.as_bytes().first(), Some(b'{') | Some(b'['))
                && let Ok(nested) = serde_json::from_str(serialized)
            {
                collect_web_source_urls_from_value(&nested, urls);
            }
        }
        _ => {}
    }
}

fn is_http_url(value: &str) -> bool {
    value.starts_with("https://") || value.starts_with("http://")
}

fn answer_cites_web_source(answer: &str, web_source_urls: &BTreeSet<String>) -> bool {
    inline_link_targets(answer)
        .iter()
        .any(|target| web_source_urls.contains(target))
}

fn inline_link_targets(answer: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut remainder = answer;
    while let Some(anchor_start) = remainder.find("<a ") {
        remainder = &remainder[anchor_start + 3..];
        let Some(tag_end) = remainder.find('>') else {
            break;
        };
        let tag = &remainder[..tag_end];
        if let Some(href_start) = tag.find("href=\"") {
            let value = &tag[href_start + 6..];
            if let Some(href_end) = value.find('"') {
                targets.push(openplotva_telegram::decode_html_entities(
                    &value[..href_end],
                ));
            }
        }
        remainder = &remainder[tag_end + 1..];
    }
    targets
}

/// Native tool definitions for one session: the shared catalog the toolbox
/// serves plus the engine-intercepted tools. `send_message` and
/// `react_to_message` deliberately never enter the shared catalog constant —
/// the legacy provider loop must not advertise tools it cannot execute.
fn session_native_tools(allow_finish: bool) -> Result<Vec<Value>, String> {
    let names = openplotva_dialog::alternative_dialog_tool_names();
    let mut specs = openplotva_dialog::alternative_dialog_tools()
        .into_iter()
        .filter(|spec| names.contains(&spec.name) && (allow_finish || spec.name != "finish_turn"))
        .collect::<Vec<_>>();
    specs.push(SESSION_SEND_MESSAGE_SPEC);
    specs.push(SESSION_REACT_TO_MESSAGE_SPEC);
    chat_completion_tools_for_specs(&specs)
        .into_iter()
        .map(|tool| serde_json::to_value(tool).map_err(|error| error.to_string()))
        .collect()
}

fn pending_image_tool<'a>(
    context: &'a ToolContext,
    calls: &[ToolCall],
    call_offset: usize,
) -> Option<&'a str> {
    let tool = context.message_meta.requested_tool.as_str();
    matches!(tool, "draw_image" | "draw_api")
        .then_some(tool)
        .filter(|name| {
            !calls
                .iter()
                .skip(call_offset)
                .any(|call| call.name == *name)
        })
}

fn replaces_pending_image_request(pending: bool, requested_tool: &str, text: &str) -> bool {
    if !pending || matches!(requested_tool, "draw_image" | "draw_api") {
        return true;
    }
    let text = text.trim().to_lowercase();
    let text = text
        .strip_prefix("плотва")
        .or_else(|| text.strip_prefix("plotva"))
        .unwrap_or(&text)
        .trim_matches(|c: char| c.is_whitespace() || ",.!?".contains(c));
    matches!(
        text,
        "отмена" | "стоп" | "cancel" | "/cancel" | "/cancel_drawing"
    ) || [
        "не рисуй",
        "не надо рисовать",
        "не нужно рисовать",
        "перестань рисовать",
        "отмени рисунок",
        "отмени генерацию",
        "не генерируй",
        "cancel drawing",
        "stop drawing",
        "do not draw",
        "don't draw",
    ]
    .iter()
    .any(|prefix| {
        text.strip_prefix(prefix)
            .is_some_and(|tail| tail.chars().next().is_none_or(|c| !c.is_alphanumeric()))
    })
}

fn record_rejected_final(agent: &mut openplotva_agent::AgentLoop, text: &str, reason: &str) {
    agent.transcript.push(SessionMessage::Assistant {
        text: text.to_owned(),
        tool_calls: Vec::new(),
    });
    agent.transcript.push(SessionMessage::InjectedUser {
        rendered: format!("Runtime guidance: The preceding draft was not sent. {reason}"),
    });
}

/// Immutable inputs of one session run, borrowed from the turn engine.
pub(crate) struct SessionRunContext<'a> {
    pub item_id: i64,
    pub item_events: &'a [TaskQueueJobEvent],
    pub params: &'a openplotva_taskman::DialogJobParams,
    pub queue_name: &'static str,
    pub max_llm_job_attempts: i32,
    /// In-process duplicate-answer regenerations allowed for this turn.
    pub max_regenerations: i32,
    pub budget: TurnBudget,
    pub now: OffsetDateTime,
    pub routing_events: Option<&'a crate::runtime_routing::RoutingEventReporter>,
    pub item: &'a crate::dialog_jobs::DialogJobWorkItem,
    /// Live inbox of this session (injection enabled); drained per iteration.
    pub inbox: Option<std::sync::Arc<super::inbox::SessionInbox>>,
    /// Agent-run buffer collecting tool results and sent markers.
    pub llm_runs: Option<&'a crate::runtime_llm_runs::RuntimeLlmRunBuffer>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SessionState {
    #[serde(default)]
    requested_tool_call_offset: usize,
    tool_context: ToolContext,
    gift_used: bool,
    budget: SessionBudget,
    agent: openplotva_agent::AgentLoop,
    sent: SentLog,
    side_effect_tickets: Vec<QueuedSideEffect>,
    recorded_tool_calls: Vec<ToolCall>,
    regenerations: i32,
    anti_loop: bool,
    repeated_final_repair: bool,
    requires_novel_final: bool,
    draws_scheduled: i32,
    songs_scheduled: i32,
    reacted_message_ids: BTreeSet<i64>,
    tool_result_cache: BTreeMap<String, (String, ToolResult)>,
    media_reference_aliases: BTreeMap<String, String>,
    successful_web_search: bool,
    web_source_urls: BTreeSet<String>,
    links: DialogLinks,
    search_citation_repairs: i32,
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub(crate) async fn run_dialog_session<Queue, Effects, Materializer, ToolHistory>(
    ctx: SessionRunContext<'_>,
    cfg: &SessionTurnConfig<'_>,
    step_provider: &dyn ChatStepProvider,
    base_input: DialogInput,
    duplicate_guard_history: &[HistoryMessage],
    queue: &Queue,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    report: &mut DialogJobWorkerReport,
) -> TurnResolution
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: crate::dialog_jobs::DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    // Re-entry guard applies only to a terminal answer. Durable intermediate
    // batches are idempotent and must not prematurely complete the session.
    if ctx
        .item_events
        .iter()
        .any(|event| event.stage == SESSION_MESSAGE_SENT_STAGE || event.stage == ANSWER_SENT_STAGE)
    {
        report.sent_answer = true;
        report.resent_skipped = true;
        return TurnResolution {
            outcome: TurnOutcome::Sent {
                parts: 1,
                side_effect_tickets: Vec::new(),
            },
            disposition: JobDisposition::Complete,
        };
    }

    let budget = SessionBudget::new(
        TurnBudget {
            anchor: ctx.budget.anchor,
            limit: ctx.budget.limit.min(TimeDuration::seconds(120)),
        },
        0,
        120,
    );
    let checkpoint = ctx
        .item_events
        .iter()
        .rev()
        .find(|event| event.stage == "agent_checkpoint")
        .and_then(|event| event.data.get("checkpoint"))
        .map(|value| serde_json::from_str::<(DialogInput, SessionState)>(value))
        .transpose();
    let checkpoint = match checkpoint {
        Ok(state) => state,
        Err(error) => {
            return agent_persistence_failed(format!("Invalid stored agent state: {error}"));
        }
    };
    let (base_input, restored) = checkpoint.map_or((base_input.clone(), None), |(input, state)| {
        (input, Some(state))
    });
    let duplicate_guard_history = duplicate_guard_history.to_vec();
    let active_params = crate::dialog_jobs::dialog_job_params_from_input(ctx.params, &base_input);
    let meta = dialog_tool_context(&base_input);
    let native_tools = match session_native_tools(
        active_params
            .meta
            .get("dialog_trigger")
            .and_then(Value::as_str)
            == Some("random"),
    ) {
        Ok(tools) => tools,
        Err(error) => {
            let error = format!("encode session tool definitions: {error}");
            return TurnResolution {
                outcome: TurnOutcome::TerminalFailed {
                    reason: "session_tools_encode",
                    error: error.clone(),
                    user_signal: UserSignalPlan::React,
                },
                disposition: JobDisposition::Fail(error),
            };
        }
    };

    let processing_started = tokio::time::Instant::now();
    let run_id = format!("job-{}", ctx.item_id);
    let agent = openplotva_agent::AgentLoop {
        observed_message_id: active_params.message_id,
        ..openplotva_agent::AgentLoop::default()
    };
    let sent = SentLog::new();
    let side_effect_tickets: Vec<QueuedSideEffect> = Vec::new();
    let recorded_tool_calls: Vec<ToolCall> = Vec::new();
    let regenerations: i32 = 0;
    let anti_loop = false;
    let repeated_final_repair = false;
    // After a work tool, progress messages alone cannot satisfy the answer.
    let requires_novel_final = false;
    let draws_scheduled: i32 = 0;
    let songs_scheduled: i32 = 0;
    let reacted_message_ids: BTreeSet<i64> = BTreeSet::new();
    let tool_result_cache: BTreeMap<String, (String, ToolResult)> = BTreeMap::new();
    let media_reference_aliases: BTreeMap<String, String> = BTreeMap::new();
    let successful_web_search = false;
    let web_source_urls = BTreeSet::new();
    let links = DialogLinks::from_input(&base_input);
    let search_citation_repairs: i32 = 0;
    let max_iterations = cfg.max_iterations.max(1);

    let mut state = restored.unwrap_or(SessionState {
        requested_tool_call_offset: 0,
        tool_context: meta,
        gift_used: false,
        budget,
        agent,
        sent,
        side_effect_tickets,
        recorded_tool_calls,
        regenerations,
        anti_loop,
        repeated_final_repair,
        requires_novel_final,
        draws_scheduled,
        songs_scheduled,
        reacted_message_ids,
        tool_result_cache,
        media_reference_aliases,
        successful_web_search,
        web_source_urls,
        links,
        search_citation_repairs,
    });
    // A crash after a committed intent has an unknown outcome. Never repeat the effect.
    for event in ctx
        .item_events
        .iter()
        .filter(|event| event.stage == "agent_effect")
    {
        if let Some(key) = event.data.get("key") {
            let result = event.data.get("result").and_then(|value| serde_json::from_str(value).ok())
                .unwrap_or_else(|| ToolResult::failed("effect_outcome_unknown", "An earlier attempt started this action. Inspect its status. Do not repeat it."));
            state
                .tool_result_cache
                .insert(key.clone(), ("recovered".into(), result));
            if event.data.get("gift").is_some_and(|value| value == "true") {
                state.gift_used = true;
            }
        }
    }
    if state.tool_result_cache.values().any(|(_, result)| {
        result
            .error
            .as_ref()
            .is_some_and(|error| error.code == "effect_outcome_unknown")
    }) {
        return agent_persistence_failed(
            "An effect was interrupted with an unknown outcome. Automatic replay is disabled."
                .into(),
        );
    }
    let recovered_tickets = state
        .tool_result_cache
        .values()
        .filter_map(|(_, result)| queued_generation_side_effect(result))
        .collect::<Vec<_>>();
    if !recovered_tickets.is_empty() {
        return session_delegated(&state.sent, &recovered_tickets);
    }
    state.agent.tool_attempts = state.agent.tool_attempts.max(
        ctx.item_events
            .iter()
            .filter(|event| event.stage == SESSION_TOOL_STAGE)
            .count()
            .min(32) as u32,
    );
    let ctx = &ctx;
    let base_input = &base_input;
    let active_params = &active_params;
    let native_tools = &native_tools;
    let duplicate_guard_history = &duplicate_guard_history;
    let run_id = &run_id;
    openplotva_agent::run((state, report), |(state, report)| async move {
        if let Err(error) = persist_agent_event(queue, effects, ctx.item_id, "agent_checkpoint", BTreeMap::from([("checkpoint".into(), serde_json::to_string(&(base_input, &state)).expect("serializable agent state"))]), ctx.now).await {
            return std::ops::ControlFlow::Break(agent_persistence_failed(error));
        }
        let SessionState { mut requested_tool_call_offset, mut tool_context, mut gift_used, mut budget, mut agent, mut sent, mut side_effect_tickets, mut recorded_tool_calls, mut regenerations, mut anti_loop, mut repeated_final_repair, mut requires_novel_final, mut draws_scheduled, mut songs_scheduled, mut reacted_message_ids, mut tool_result_cache, mut media_reference_aliases, mut successful_web_search, mut web_source_urls, mut links, mut search_citation_repairs } = state;
        macro_rules! next_step { () => { std::ops::ControlFlow::Continue((SessionState { requested_tool_call_offset, tool_context, gift_used, budget, agent, sent, side_effect_tickets, recorded_tool_calls, regenerations, anti_loop, repeated_final_repair, requires_novel_final, draws_scheduled, songs_scheduled, reacted_message_ids, tool_result_cache, media_reference_aliases, successful_web_search, web_source_urls, links, search_citation_repairs }, report)) }; }

        let previously_observed_message_id = agent.observed_message_id;
        if let Ok(Ok(messages)) = tokio::time::timeout(std::time::Duration::from_secs(2), materializer.observe_dialog_messages(active_params, agent.observed_message_id)).await {
            for mut message in messages {
                if let Some(id) = message.get("message_id").and_then(Value::as_i64).and_then(|id| i32::try_from(id).ok()) {
                    agent.observed_message_id = agent.observed_message_id.max(id);
                }
                message["can_change_task"] = Value::Bool(message.get("user_id").and_then(Value::as_i64) == Some(active_params.user_id));
                if message["can_change_task"] == true && replaces_pending_image_request(
                    pending_image_tool(&tool_context, &recorded_tool_calls, requested_tool_call_offset).is_some(),
                    message["meta"]["requested_tool"].as_str().unwrap_or_default(),
                    message["text"].as_str().unwrap_or_default(),
                ) {
                    tool_context.message_id = message["message_id"].as_i64().and_then(|id| i32::try_from(id).ok()).unwrap_or(tool_context.message_id);
                    tool_context.message_meta.requested_tool = message["meta"]["requested_tool"].as_str().unwrap_or_default().to_owned();
                    requested_tool_call_offset = recorded_tool_calls.len();
                    if let Some(text) = message["text"].as_str() { tool_context.message_text = text.to_owned(); }
                }
                remember_context_images(&mut tool_context, &message);
                links.record_tool_result(&ToolResult {status:"ok".into(), data:Some(message.clone()), ..ToolResult::default()});
                agent.transcript.push(SessionMessage::InjectedUser { rendered:message.to_string() });
            }
        }
        if let Some(inbox) = ctx.inbox.as_ref() {
            let mut incoming = inbox.drain_open();
            incoming.sort_by_key(|message| message.params.message_id);
            for injected in incoming {
                let params = injected.params;
                if let Ok(meta) = serde_json::from_value::<openplotva_core::ChatMessageMeta>(params.meta.clone()) {
                    extend_context_images(&mut tool_context, params.message_id, meta.attachments);
                }
                if params.message_id > previously_observed_message_id && params.message_id >= tool_context.message_id && params.user_id == active_params.user_id
                    && replaces_pending_image_request(
                        pending_image_tool(&tool_context, &recorded_tool_calls, requested_tool_call_offset).is_some(),
                        params.meta["requested_tool"].as_str().unwrap_or_default(),
                        &params.message_text,
                    ) {
                    tool_context.message_id = params.message_id;
                    tool_context.message_meta.requested_tool = params.meta["requested_tool"].as_str().unwrap_or_default().to_owned();
                    requested_tool_call_offset = recorded_tool_calls.len();
                    tool_context.message_text = if params.original_text.trim().is_empty() { params.message_text.clone() } else { params.original_text.clone() };
                }
                if params.message_id <= agent.observed_message_id { continue; }
                agent.observed_message_id = params.message_id;
                links.record_tool_result(&ToolResult {status:"ok".into(), message:params.message_text.clone(), ..ToolResult::default()});
                agent.transcript.push(SessionMessage::InjectedUser {
                    rendered: serde_json::json!({
                        "message_id": params.message_id,
                        "user_id": params.user_id,
                        "name": params.user_full_name,
                        "text": params.message_text,
                        "meta": params.meta,
                        "can_change_task": params.user_id == active_params.user_id,
                    })
                    .to_string(),
                });
            }
        }
        let round_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        let next = agent.next_step(
            std::time::Duration::try_from(budget.remaining(round_now)).unwrap_or_default(),
            max_iterations,
        );
        let iteration = agent.iteration;
        if next == openplotva_agent::NextStep::Exhausted {
            return std::ops::ControlFlow::Break( session_exhausted(ctx.item_id, &sent, &side_effect_tickets, &budget, round_now));
        }

        let force_final = next == openplotva_agent::NextStep::Final;
        let tools = if base_input.disable_tools {
            ToolsMode::Disabled
        } else if force_final || search_citation_repairs > 0 || repeated_final_repair {
            ToolsMode::FinalOnly
        } else {
            ToolsMode::Native((*native_tools).clone())
        };

        let mut input = (*base_input).clone();
        input.message.meta.requested_tool = tool_context.message_meta.requested_tool.clone();
        let mut add_hint = |hint: &str| {
            let rendered = format!("Runtime guidance: {hint}");
            if !agent.transcript.iter().any(|message| matches!(message, SessionMessage::InjectedUser { rendered: prior } if prior == &rendered)) {
                agent.transcript.push(SessionMessage::InjectedUser { rendered });
            }
        };
        if anti_loop { add_hint(ANTI_LOOP_HINT); }
        if !web_source_urls.is_empty() { add_hint(SEARCH_CITATION_HINT.trim()); }
        if search_citation_repairs > 0 { add_hint(SEARCH_CITATION_REPAIR_HINT); }
        if force_final { add_hint("Tool budget is exhausted or the deadline is near. Finish with the available evidence. State any unfinished work briefly."); }
        let provider_deadline = Instant::now()
            + std::time::Duration::try_from(budget.remaining(round_now)).unwrap_or_default();
        let step_started = tokio::time::Instant::now();
        let result = tokio::time::timeout(std::time::Duration::try_from(budget.remaining(round_now)).unwrap_or_default(), TURN_DEADLINE
            .scope(
                Some(provider_deadline),
                step_provider.run_chat_step(ChatStepRequest {
                    input,
                    required_tool: pending_image_tool(&tool_context, &recorded_tool_calls, requested_tool_call_offset).map(str::to_owned),
                    preferred_target: agent.preferred_target.clone(),
                    transcript: agent.transcript.clone(),
                    tools,
                    iteration: usize::try_from(iteration).unwrap_or(1),
                }),
            )).await.unwrap_or_else(|_| Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "Agent turn deadline reached"))));
        let failure_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        let step = match result {
            Ok(step) => step,
            Err(error) if openplotva_llm::is_content_blocked_error(error.as_ref()) => {
                report.content_blocked = true;
                return std::ops::ControlFlow::Break( TurnResolution {
                    outcome: TurnOutcome::NoReplyIntentional {
                        reason: "content_blocked",
                    },
                    disposition: JobDisposition::Complete,
                });
            }
            Err(error) => {
                let retryable_reason = openplotva_llm::retry::retryable_reason(error.as_ref());
                let retry_provider =
                    openplotva_llm::retry::provider_name(error.as_ref()).to_owned();
                let error = error.to_string();
                report.provider_error = Some(error.clone());
                if let Some(reason) = retryable_reason {
                    if sent.any() {
                        // Never replay a partially delivered session: the
                        // user saw messages; a requeue would regenerate and
                        // resend nondeterministically.
                        return std::ops::ControlFlow::Break( TurnResolution {
                            outcome: TurnOutcome::TerminalFailed {
                                reason: "llm_failed_after_partial",
                                error: error.clone(),
                                user_signal: UserSignalPlan::React,
                            },
                            disposition: JobDisposition::Fail(error),
                        });
                    }
                    return std::ops::ControlFlow::Break( handle_retryable_dialog_provider_error(
                        queue,
                        ctx.item,
                        active_params,
                        ctx.routing_events,
                        RetryableDialogProviderFailure {
                            queue_name: ctx.queue_name,
                            provider_name: &retry_provider,
                            reason,
                            codes: PROVIDER_ERROR_RETRY_CODES,
                            error: &error,
                            max_attempts: ctx.max_llm_job_attempts.max(1),
                            now: failure_now,
                            budget_deadline: budget.deadline(),
                        },
                        report,
                    )
                    .await);
                }
                return std::ops::ControlFlow::Break( TurnResolution {
                    outcome: TurnOutcome::TerminalFailed {
                        reason: "provider_error",
                        error: error.clone(),
                        user_signal: UserSignalPlan::React,
                    },
                    disposition: JobDisposition::Fail(error),
                });
            }
        };
        report.session_iterations = iteration;
        report.provider = Some(step.provider.clone());
        agent.preferred_target = (!step.provider.is_empty() && !step.model.is_empty())
            .then(|| (step.provider.clone(), step.model.clone()));
        append_session_iteration_event(
            queue,
            ctx.item_id,
            iteration,
            &step.provider,
            &step.model,
            step_started.elapsed().as_millis(),
            step.tool_calls.len(),
            step.text.len(),
            failure_now,
        )
        .await;

        if step.tool_calls.is_empty() || force_final || search_citation_repairs > 0 {
            // ---- Final-answer exit (text without tool calls). A forced
            // final or citation-repair pass never executes tools; salvaged
            // markup was already stripped by the step provider, so its text
            // stands on its own.
            let raw_answer = step.text.clone();
            let sanitized = links.prepare_response(&raw_answer);
            let image_action_missing = !base_input.disable_tools
                && pending_image_tool(&tool_context, &recorded_tool_calls, requested_tool_call_offset).is_some();
            if sanitized.trim().is_empty() || (image_action_missing
                && (force_final || regenerations >= ctx.max_regenerations.max(0)
                    || budget.remaining(failure_now) < MIN_REGENERATION_BUDGET)) {
                if !side_effect_tickets.is_empty() {
                    // Silent side-effect finish (should have terminated at
                    // the tool batch already; kept as a safety net).
                    return std::ops::ControlFlow::Break( session_delegated(&sent, &side_effect_tickets));
                }
                if sent.any() {
                    let error =
                        "dialog provider returned no final answer after intermediate messages"
                            .to_owned();
                    return std::ops::ControlFlow::Break( TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "empty_final_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    });
                }
                let (codes, error) = if image_action_missing {
                    (
                        SANITIZED_EMPTY_RETRY_CODES,
                        "dialog provider did not execute the explicitly requested image tool",
                    )
                } else if raw_answer.trim().is_empty() {
                    (
                        PROVIDER_EMPTY_RETRY_CODES,
                        "dialog provider returned no answer, response, or queued tool material",
                    )
                } else {
                    (
                        SANITIZED_EMPTY_RETRY_CODES,
                        "dialog answer became empty after sanitization",
                    )
                };
                report.empty_answer_error = Some(error.to_owned());
                return std::ops::ControlFlow::Break( handle_retryable_dialog_provider_error(
                    queue,
                    ctx.item,
                    active_params,
                    ctx.routing_events,
                    RetryableDialogProviderFailure {
                        queue_name: ctx.queue_name,
                        provider_name: step.provider.as_str(),
                        reason: openplotva_llm::retry::FailureReason::ProviderProtocolError,
                        codes,
                        error,
                        max_attempts: ctx.max_llm_job_attempts.max(1),
                        now: failure_now,
                        budget_deadline: budget.deadline(),
                    },
                    report,
                )
                .await);
            }

            if !base_input.disable_tools && !force_final
                && let Some(tool) = pending_image_tool(&tool_context, &recorded_tool_calls, requested_tool_call_offset)
                && regenerations < ctx.max_regenerations.max(0)
                && budget.remaining(failure_now) >= MIN_REGENERATION_BUDGET
            {
                let reason = format!("The user explicitly requested {tool}, but it was not called. Execute the image tool with the user's request; do not replace the action with text.");
                regenerations += 1;
                report.regenerations = regenerations;
                record_rejected_final(&mut agent, &sanitized, &reason);
                return next_step!();
            }

            if !web_source_urls.is_empty() && !answer_cites_web_source(&sanitized, &web_source_urls)
            {
                if search_citation_repairs < MAX_SEARCH_CITATION_REPAIRS
                    && iteration < max_iterations
                    && budget.remaining(failure_now) >= MIN_REGENERATION_BUDGET
                {
                    search_citation_repairs += 1;
                    record_rejected_final(
                        &mut agent,
                        &sanitized,
                        "It does not cite a retrieved source. Include a link returned by the tools.",
                    );
                    tracing::info!(
                        job_id = ctx.item_id,
                        attempt = search_citation_repairs,
                        sources = web_source_urls.len(),
                        "regenerating searched answer without a source citation"
                    );
                    return next_step!();
                }
                tracing::warn!(
                    job_id = ctx.item_id,
                    attempts = search_citation_repairs,
                    sources = web_source_urls.len(),
                    "sending searched answer without a source citation after repair attempts"
                );
            }

            if sent.matches_delivery(&sanitized) {
                if requires_novel_final {
                    if regenerations < ctx.max_regenerations.max(0)
                        && iteration < max_iterations
                        && budget.remaining(failure_now) >= MIN_REGENERATION_BUDGET
                    {
                        regenerations += 1;
                        report.regenerations = regenerations;
                        anti_loop = true;
                        repeated_final_repair = true;
                        record_rejected_final(
                            &mut agent,
                            &sanitized,
                            "It repeats this turn's intermediate messages. Give the completed answer using the tool results.",
                        );
                        append_repeated_final_regeneration_event(
                            queue,
                            ctx.item_id,
                            regenerations,
                            failure_now,
                        )
                        .await;
                        return next_step!();
                    }
                    let error = format!(
                        "dialog final answer only replayed intermediate messages after {regenerations} regeneration(s)"
                    );
                    return std::ops::ControlFlow::Break( TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "repeated_final_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    });
                }
                report.sent_answer = true;
                if let Some(runs) = ctx.llm_runs {
                    runs.mark_round_sent(run_id, crate::runtime_llm_runs::RunRoundSent::Final);
                }
                let sent_now = ctx.now
                    + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
                append_session_sent_marker(queue, ctx.item_id, sent_now).await;
                return std::ops::ControlFlow::Break( TurnResolution {
                    outcome: TurnOutcome::Sent {
                        parts: sent.total_count,
                        side_effect_tickets: ticket_ids(&side_effect_tickets),
                    },
                    disposition: JobDisposition::Complete,
                });
            }
            let (duplicate_message_id, duplicate) =
                should_suppress_duplicate_bot_reply(duplicate_guard_history, &sanitized);
            if duplicate {
                if regenerations < ctx.max_regenerations.max(0)
                    && budget.remaining(failure_now) >= MIN_REGENERATION_BUDGET
                {
                    regenerations += 1;
                    report.regenerations = regenerations;
                    anti_loop = true;
                    record_rejected_final(
                        &mut agent,
                        &sanitized,
                        "It repeats an earlier bot reply. Answer the current request without copying that reply.",
                    );
                    append_session_regeneration_event(
                        queue,
                        ctx.item_id,
                        regenerations,
                        duplicate_message_id,
                        failure_now,
                    )
                    .await;
                    return next_step!();
                }
                report.suppressed_duplicate_message_id = Some(duplicate_message_id);
                let error = format!(
                    "dialog answer duplicated bot message {duplicate_message_id} after {regenerations} regeneration(s)"
                );
                if sent.any() {
                    return std::ops::ControlFlow::Break( TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "duplicate_exhausted_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    });
                }
                return std::ops::ControlFlow::Break( TurnResolution {
                    outcome: TurnOutcome::TerminalFailed {
                        reason: "duplicate_exhausted",
                        error: error.clone(),
                        user_signal: UserSignalPlan::React,
                    },
                    disposition: JobDisposition::Fail(error),
                });
            }

            if let Err(validation) = validate_dialog_answer_deliverable(&sanitized) {
                let error = format!("dialog answer rejected by outbound validation: {validation}");
                report.empty_answer_error = Some(error.clone());
                if sent.any() {
                    return std::ops::ControlFlow::Break( TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "undeliverable_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    });
                }
                return std::ops::ControlFlow::Break( handle_retryable_dialog_provider_error(
                    queue,
                    ctx.item,
                    active_params,
                    ctx.routing_events,
                    RetryableDialogProviderFailure {
                        queue_name: ctx.queue_name,
                        provider_name: step.provider.as_str(),
                        reason: openplotva_llm::retry::FailureReason::ProviderProtocolError,
                        codes: UNDELIVERABLE_RETRY_CODES,
                        error: &error,
                        max_attempts: ctx.max_llm_job_attempts.max(1),
                        now: failure_now,
                        budget_deadline: budget.deadline(),
                    },
                    report,
                )
                .await);
            }

            let mut final_answer = sanitized.clone();
            let mut contains_advertising = false;
            let mut advertising_tail_bytes = None;
            let mut advertising_opportunity_id = None;
            if let Some(gradius) = cfg.gradius {
                let request = crate::gradius_ads::GradiusAdAppendRequest {
                    dialog_job_id: ctx.item_id,
                    attempt_key: format!(
                        "dialog-claim:{}",
                        ctx.item.claim_started_at.unix_timestamp_nanos()
                    ),
                    chat_id: active_params.chat_id,
                    thread_id: active_params.thread_id,
                    user_id: active_params.user_id,
                    user_text: active_params.message_text.clone(),
                    assistant_text: openplotva_telegram::strip_telegram_html(&sanitized),
                    language: base_input.context.locale.clone(),
                    model_version: (!step.model.trim().is_empty()).then(|| step.model.clone()),
                    completed_at: failure_now,
                };
                if crate::dialog_jobs::dialog_response_requires_rich(&sanitized) {
                    if let Err(error) = gradius.record_unsupported_surface(request).await {
                        tracing::warn!(
                            job_id = ctx.item_id,
                            integration_kind = "native_dialogue",
                            outcome = "unsupported_surface",
                            %error,
                            "failed to audit unsupported Gradius rich response surface"
                        );
                    }
                } else {
                    match gradius.append(request).await {
                        Ok(Some(tail)) => {
                            let tail_bytes = tail.html.len();
                            let candidate = format!("{sanitized}\n\n{}", tail.html);
                            if let Err(error) = validate_dialog_answer_deliverable(&candidate) {
                                if let Err(audit_error) = gradius
                                    .mark_render_error(tail.opportunity_id, &error.to_string())
                                    .await
                                {
                                    tracing::warn!(
                                        opportunity_id = tail.opportunity_id,
                                        job_id = ctx.item_id,
                                        %audit_error,
                                        "failed to persist Gradius final-answer render rejection"
                                    );
                                }
                                tracing::warn!(
                                    opportunity_id = tail.opportunity_id,
                                    job_id = ctx.item_id,
                                    %error,
                                    "skipping Gradius ad rejected by final-answer validation"
                                );
                            } else {
                                final_answer = candidate;
                                contains_advertising = true;
                                advertising_tail_bytes = Some(tail_bytes);
                                advertising_opportunity_id = Some(tail.opportunity_id);
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            tracing::warn!(
                                job_id = ctx.item_id,
                                %error,
                                "skipping Gradius ad after fail-closed integration error"
                            );
                        }
                    }
                }
            }

            let send_result = effects
                .send_dialog_answer(
                    ctx.item_id,
                    ctx.item.latest_update_id,
                    active_params,
                    &final_answer,
                    DialogAnswerSendOptions {
                        disable_link_preview: successful_web_search,
                        contains_advertising,
                        advertising_tail_bytes,
                    },
                )
                .await;
            if let (Ok(receipt), Some(opportunity_id), Some(gradius)) = (
                send_result.as_ref(),
                advertising_opportunity_id,
                cfg.gradius,
            ) {
                if let Err(error) = gradius.mark_queued(opportunity_id, &receipt.batch_id).await {
                    tracing::warn!(
                        opportunity_id,
                        job_id = ctx.item_id,
                        integration_kind = "native_dialogue",
                        delivery_state = "queued",
                        %error,
                        "failed to persist Gradius queued delivery state"
                    );
                }
                if receipt.delivery_complete()
                    && let Err(error) = gradius
                        .mark_delivered(opportunity_id, &receipt.batch_id)
                        .await
                {
                    tracing::warn!(
                        opportunity_id,
                        job_id = ctx.item_id,
                        integration_kind = "native_dialogue",
                        delivery_state = "delivered",
                        %error,
                        "failed to persist already-complete Gradius delivery state"
                    );
                }
            }
            if let (Err(send_error), Some(opportunity_id), Some(gradius)) = (
                send_result.as_ref(),
                advertising_opportunity_id,
                cfg.gradius,
            ) {
                let send_error = send_error.to_string();
                if let Err(error) = gradius
                    .mark_delivery_failed(opportunity_id, &send_error)
                    .await
                {
                    tracing::warn!(
                        opportunity_id,
                        job_id = ctx.item_id,
                        integration_kind = "native_dialogue",
                        delivery_state = "failed",
                        %error,
                        "failed to persist Gradius enqueue failure"
                    );
                }
            }

            return std::ops::ControlFlow::Break( match send_result {
                Ok(receipt) if receipt.requires_delivery_wait() => {
                    report.queued_answer = true;
                    let queued_now = ctx.now
                        + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
                    append_session_queued_marker(queue, ctx.item_id, &receipt, queued_now).await;
                    if receipt.delivery_complete() {
                        report.sent_answer = true;
                        TurnResolution {
                            outcome: TurnOutcome::Sent {
                                parts: receipt.operation_ids.len(),
                                side_effect_tickets: ticket_ids(&side_effect_tickets),
                            },
                            disposition: JobDisposition::Complete,
                        }
                    } else {
                        TurnResolution {
                            outcome: TurnOutcome::QueuedForDelivery {
                                batch_id: receipt.batch_id,
                                operation_ids: receipt.operation_ids,
                                side_effect_tickets: ticket_ids(&side_effect_tickets),
                            },
                            disposition: JobDisposition::WaitForDelivery,
                        }
                    }
                }
                Ok(_receipt) => {
                    sent.record(&final_answer, false, active_params.message_id);
                    report.sent_answer = true;
                    if let Some(runs) = ctx.llm_runs {
                        runs.mark_round_sent(run_id, crate::runtime_llm_runs::RunRoundSent::Final);
                    }
                    let sent_now = ctx.now
                        + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
                    append_session_sent_marker(queue, ctx.item_id, sent_now).await;
                    TurnResolution {
                        outcome: TurnOutcome::Sent {
                            parts: sent.total_count,
                            side_effect_tickets: ticket_ids(&side_effect_tickets),
                        },
                        disposition: JobDisposition::Complete,
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    report.send_error = Some(error.clone());
                    TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "send_error",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    }
                }
            });
        }

        // Only explicit send_message calls publish text during tool execution.
        agent.transcript.push(SessionMessage::Assistant {
            text: String::new(),
            tool_calls: step
                .tool_calls
                .iter()
                .map(|call| SessionToolCall {
                    id: call.id.clone(),
                    name: call.step.step.clone(),
                    arguments: tool_call_arguments(&call.step),
                })
                .collect(),
        });
        let mut parallel_media_results = BTreeMap::new();
        let mut batch_side_effects: Vec<QueuedSideEffect> = Vec::new();
        let mut batch_results = Vec::with_capacity(step.tool_calls.len());
        for (call_index, call) in step.tool_calls.iter().enumerate() {
            if !matches!(
                call.step.step.as_str(),
                STEP_SEND_MESSAGE | STEP_REACT_TO_MESSAGE
            ) {
                requires_novel_final = true;
            }
            if call.step.step == STEP_UNDERSTAND_MEDIA
                && agent.tool_attempts < openplotva_agent::MAX_TOOL_CALLS
                && budget.remaining(
                    ctx.now
                        + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default(),
                ) > TOOL_RESERVE
                && !parallel_media_results.contains_key(&call_index)
            {
                let batch_len =
                    consecutive_understand_media_call_count(&step.tool_calls, call_index)
                        .min((openplotva_agent::MAX_TOOL_CALLS - agent.tool_attempts) as usize);
                agent.tool_attempts += batch_len as u32;
                parallel_media_results.extend(
                    execute_parallel_understand_media_calls(
                        &step.tool_calls[call_index..call_index + batch_len],
                        call_index,
                        cfg,
                        &tool_context,
                        active_params.message_id,
                        &media_reference_aliases,
                        &tool_result_cache,
                        &mut budget,
                        processing_started,
                        ctx.now,
                    )
                    .await,
                );
            }
            let tool_started = tokio::time::Instant::now();
            let semantic_key = semantic_tool_call_key(
                active_params.message_id,
                &call.step,
                &media_reference_aliases,
            );
            let mut budget_extension_granted = false;
            let remaining = budget.remaining(
                ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default(),
            );
            let (result, tool_duration_ms, executed) = if let Some(prepared) = parallel_media_results.remove(&call_index) {
                (prepared.result, prepared.duration_ms, true)
            } else if !agent
                .admit_tool(std::time::Duration::try_from(remaining).unwrap_or_default())
            {
                (
                    ToolResult::failed(
                        "tool_budget_exhausted",
                        "Tool limit or deadline reached. Finish using the available results.",
                    ),
                    0,
                    false,
                )

            } else if let Some((original_call_id, cached)) = tool_result_cache.get(&semantic_key) {
                (
                    reused_tool_result(cached, original_call_id),
                    tool_started.elapsed().as_millis(),
                    false,
                )
            } else {
                if effect_tool(&call.step.step)
                    && let Err(error) = persist_agent_event(queue, effects, ctx.item_id, "agent_effect", BTreeMap::from([("key".into(), semantic_key.clone()), ("gift".into(), call.step.gift.to_string())]), failure_now).await {
                        return std::ops::ControlFlow::Break(agent_persistence_failed(error));
                }
                let slice = session_tool_slice(&budget, cfg, failure_now);
                let result = tokio::time::timeout(slice, execute_session_tool(
                    SessionToolExecution {
                        call,
                        cfg,
                        meta: &tool_context,
                        gift_used: &mut gift_used,
                        params: active_params,
                        causation_update_id: ctx.item.latest_update_id,
                        budget: &mut budget,
                        sent: &mut sent,
                        links: &links,
                        draws_scheduled: &mut draws_scheduled,
                        songs_scheduled: &mut songs_scheduled,
                        reacted_message_ids: &mut reacted_message_ids,
                        budget_extension_granted: &mut budget_extension_granted,
                        processing_started,
                        now: ctx.now,
                    },
                    effects,
                    queue,
                    ctx.item_id,
                )).await.unwrap_or_else(|_| ToolResult::failed("effect_outcome_unknown", "The tool deadline expired. Do not repeat an action whose outcome is unknown."));
                (result, tool_started.elapsed().as_millis(), true)
            };
            if executed && effect_tool(&call.step.step)
                && let Err(error) = persist_agent_event(queue, effects, ctx.item_id, "agent_effect", BTreeMap::from([("key".into(), semantic_key.clone()), ("gift".into(), call.step.gift.to_string()), ("result".into(), serde_json::to_string(&result).expect("serializable tool result"))]), failure_now).await {
                    return std::ops::ControlFlow::Break(agent_persistence_failed(error));
            }
            if effect_tool(&call.step.step) && result.error.as_ref().is_some_and(|error| error.code == "effect_outcome_unknown") {
                return std::ops::ControlFlow::Break(agent_persistence_failed("An action timed out with an unknown outcome. Automatic replay is disabled.".into()));
            }
            if executed {
                remember_media_reference_alias(&mut media_reference_aliases, &call.step, &result);
                let resolved_key = semantic_tool_call_key(
                    active_params.message_id,
                    &call.step,
                    &media_reference_aliases,
                );
                let cached = (call.id.clone(), result.clone());
                tool_result_cache.insert(semantic_key, cached.clone());
                tool_result_cache.insert(resolved_key, cached);
            }
            if let Some(effect) = queued_generation_side_effect(&result) {
                batch_side_effects.push(effect);
            }
            if let Some(data) = &result.data { remember_context_images(&mut tool_context, data); }
            links.record_tool_result(&result);
            if matches!(call.step.step.as_str(), STEP_WEB_SEARCH | STEP_CRAWL_URL)
                && result
                    .status
                    .eq_ignore_ascii_case(openplotva_dialog::TOOL_RESULT_STATUS_OK)
            {
                successful_web_search |= call.step.step == STEP_WEB_SEARCH;
                collect_web_source_urls(&result, &mut web_source_urls);
            }
            append_session_tool_event(
                queue,
                ctx.item_id,
                &call.step.step,
                &result.status,
                tool_duration_ms,
                budget_extension_granted,
                ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default(),
            )
            .await;
            if let Some(runs) = ctx.llm_runs {
                runs.record_tool_result(
                    run_id,
                    crate::runtime_llm_runs::RunToolCall {
                        name: call.step.step.clone(),
                        status: result.status.clone(),
                        duration_ms: i64::try_from(tool_duration_ms).ok(),
                        args_json: serde_json::to_string(&call.step).ok(),
                        result_json: serde_json::to_string(&result).ok(),
                    },
                );
                if call.step.step == STEP_SEND_MESSAGE
                    && result.status == openplotva_dialog::TOOL_RESULT_STATUS_OK
                {
                    runs.mark_round_sent(
                        run_id,
                        crate::runtime_llm_runs::RunRoundSent::Intermediate,
                    );
                }
            }
            recorded_tool_calls.push(recorded_session_tool_call(
                &call.step, &result, &call.id, iteration,
            ));
            batch_results.push(result.clone());
            agent.transcript.push(SessionMessage::ToolResult {
                tool_call_id: call.id.clone(),
                name: call.step.step.clone(),
                content: serde_json::to_string(&result)
                    .unwrap_or_else(|_| "{\"status\":\"failed\"}".to_owned()),
            });
        }

        report.session_tool_calls.clone_from(&recorded_tool_calls);
        match persist_dialog_tool_calls(tool_history, active_params, &recorded_tool_calls).await {
            Ok(persisted) => report.persisted_tool_call_history = persisted,
            Err(error) => {
                report.tool_call_history_error = Some(error.to_string());
            }
        }

        if step
            .tool_calls
            .iter()
            .zip(&batch_results)
            .any(|(call, result)| call.step.step == "finish_turn" && result.status == "ok")
        {
            side_effect_tickets.extend(batch_side_effects);
            if !side_effect_tickets.is_empty() {
                return std::ops::ControlFlow::Break(session_delegated(&sent, &side_effect_tickets));
            }
            return std::ops::ControlFlow::Break( TurnResolution {
                outcome: TurnOutcome::NoReplyIntentional {
                    reason: "agent_finished",
                },
                disposition: JobDisposition::Complete,
            });
        }
        let disposition = session_batch_disposition(&step.tool_calls, &batch_results);
        let disposition_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        append_session_batch_event(
            queue,
            ctx.item_id,
            iteration,
            disposition,
            false,
            disposition_now,
        )
        .await;
        side_effect_tickets.extend(batch_side_effects);

        match disposition {
            SessionBatchDisposition::ContinueForResults
            | SessionBatchDisposition::ContinueWithoutFinal => {}
            SessionBatchDisposition::CompleteWithSideEffect => {
                if sent.any() {
                    report.sent_answer = true;
                    append_session_sent_marker(queue, ctx.item_id, disposition_now).await;
                }
                return std::ops::ControlFlow::Break( session_delegated(&sent, &side_effect_tickets));
            }
        }
        next_step!()
    }).await
}

fn extend_context_images(
    context: &mut ToolContext,
    message_id: i32,
    attachments: Vec<openplotva_core::ChatAttachment>,
) {
    context
        .image_reference_ids
        .extend(openplotva_dialog::media_reference_ids(
            message_id,
            &attachments,
        ));
    for attachment in attachments.into_iter().filter(|a| a.kind == "image") {
        if attachment.file_unique_id.is_empty() {
            continue;
        }
        if !context
            .image_attachments
            .iter()
            .any(|existing| existing.file_unique_id == attachment.file_unique_id)
        {
            context.image_attachments.push(attachment);
        }
    }
}

fn remember_context_images(context: &mut ToolContext, data: &Value) {
    if let Some(message) = data.get("message")
        && let Ok(entry) =
            openplotva_history::decode_summary_message_entry_payload(message.to_string().as_bytes())
    {
        extend_context_images(context, entry.message_id, entry.meta.attachments);
    }
    for key in ["messages", "nearby"] {
        if let Some(messages) = data.get(key).and_then(Value::as_array) {
            for message in messages {
                remember_context_images(context, message);
            }
        }
    }
}

fn effect_tool(name: &str) -> bool {
    matches!(
        name,
        "draw_image"
            | "generate_song"
            | "draw_api"
            | "send_message"
            | "react_to_message"
            | "memory_manage"
            | "cancel_drawing"
    )
}

async fn persist_agent_event<
    Q: DialogJobWorkerQueue + Sync + ?Sized,
    E: DialogJobEffects + Sync + ?Sized,
>(
    queue: &Q,
    effects: &E,
    job_id: i64,
    stage: &str,
    data: BTreeMap<String, String>,
    now: OffsetDateTime,
) -> Result<(), String> {
    queue
        .append_dialog_job_event(
            job_id,
            TaskQueueJobEvent {
                level: "info".into(),
                stage: stage.into(),
                data,
                ..TaskQueueJobEvent::default()
            },
            now,
        )
        .await
        .map_err(|error| error.to_string())?;
    effects.persist_agent_state(job_id).await
}

fn agent_persistence_failed(error: String) -> TurnResolution {
    TurnResolution {
        outcome: TurnOutcome::TerminalFailed {
            reason: "agent_persistence_failed",
            error: error.clone(),
            user_signal: UserSignalPlan::React,
        },
        disposition: JobDisposition::Fail(error),
    }
}

fn session_delegated(sent: &SentLog, effects: &[QueuedSideEffect]) -> TurnResolution {
    if sent.any() {
        TurnResolution {
            outcome: TurnOutcome::Sent {
                parts: sent.total_count,
                side_effect_tickets: ticket_ids(effects),
            },
            disposition: JobDisposition::Complete,
        }
    } else {
        TurnResolution {
            outcome: TurnOutcome::SideEffectDelegated {
                tickets: ticket_ids(effects),
                kinds: effects.iter().map(|effect| effect.kind.clone()).collect(),
            },
            disposition: JobDisposition::Complete,
        }
    }
}

fn session_exhausted(
    _item_id: i64,
    sent: &SentLog,
    side_effects: &[QueuedSideEffect],
    budget: &SessionBudget,
    now: OffsetDateTime,
) -> TurnResolution {
    if sent.any() {
        let error = format!(
            "dialog session exhausted after {}s with intermediate messages but no final answer",
            budget.elapsed(now).whole_seconds()
        );
        TurnResolution {
            outcome: TurnOutcome::TerminalFailed {
                reason: "session_exhausted_after_partial",
                error: error.clone(),
                user_signal: UserSignalPlan::React,
            },
            disposition: JobDisposition::Fail(error),
        }
    } else if !side_effects.is_empty() {
        session_delegated(sent, side_effects)
    } else {
        let error = format!(
            "dialog session exhausted after {}s without a final answer",
            budget.elapsed(now).whole_seconds()
        );
        TurnResolution {
            outcome: TurnOutcome::TerminalFailed {
                reason: "session_exhausted",
                error: error.clone(),
                user_signal: UserSignalPlan::React,
            },
            disposition: JobDisposition::Fail(error),
        }
    }
}

fn ticket_ids(effects: &[QueuedSideEffect]) -> Vec<i64> {
    effects
        .iter()
        .filter_map(|effect| effect.ticket_job_id)
        .collect()
}

/// One queued image/music generation carried by a tool result.
fn queued_generation_side_effect(result: &ToolResult) -> Option<QueuedSideEffect> {
    let side_effect = result.side_effect.as_ref()?;
    let queued = side_effect
        .state
        .eq_ignore_ascii_case(SIDE_EFFECT_STATE_QUEUED)
        && matches!(
            side_effect.kind.as_str(),
            SIDE_EFFECT_KIND_IMAGE | SIDE_EFFECT_KIND_MUSIC
        );
    queued.then(|| QueuedSideEffect {
        kind: side_effect.kind.clone(),
        ticket_job_id: side_effect.ticket_id.trim().parse::<i64>().ok(),
        eta: side_effect.eta.clone(),
    })
}

fn session_batch_disposition(
    calls: &[ChatStepToolCall],
    results: &[ToolResult],
) -> SessionBatchDisposition {
    let mut queued_generation = false;

    for (index, call) in calls.iter().enumerate() {
        let Some(continuation) = dialog_tool_continuation(&call.step.step) else {
            return SessionBatchDisposition::ContinueForResults;
        };
        match continuation {
            ToolContinuation::RequiresFollowup => {
                return SessionBatchDisposition::ContinueForResults;
            }
            ToolContinuation::Sidecar => {}
            ToolContinuation::MayTerminateOnSuccess => {
                let Some(result) = results.get(index) else {
                    return SessionBatchDisposition::ContinueForResults;
                };
                if queued_generation_side_effect(result).is_some() {
                    queued_generation = true;
                } else {
                    return SessionBatchDisposition::ContinueForResults;
                }
            }
            ToolContinuation::ExplicitIntermediate => {}
        }
    }

    if queued_generation {
        SessionBatchDisposition::CompleteWithSideEffect
    } else {
        SessionBatchDisposition::ContinueWithoutFinal
    }
}

fn semantic_tool_call_key(
    trigger_message_id: i32,
    step: &ToolStep,
    media_reference_aliases: &BTreeMap<String, String>,
) -> String {
    let mut normalized_step = step.clone();
    if normalized_step.step == STEP_UNDERSTAND_MEDIA {
        let reference = normalize_media_reference(&normalized_step.file_id);
        if let Some(file_unique_id) = media_reference_aliases.get(&reference) {
            normalized_step.file_id.clone_from(file_unique_id);
        }
    }
    let mut value = serde_json::to_value(normalized_step).unwrap_or(Value::Null);
    normalize_semantic_tool_value(&mut value, None);
    format!(
        "{trigger_message_id}:{}",
        serde_json::to_string(&value).unwrap_or_default()
    )
}

fn remember_media_reference_alias(
    aliases: &mut BTreeMap<String, String>,
    step: &ToolStep,
    result: &ToolResult,
) {
    if step.step != STEP_UNDERSTAND_MEDIA {
        return;
    }
    let Some(file_unique_id) = result
        .data
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|data| data.get("file_unique_id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    aliases.insert(
        normalize_media_reference(&step.file_id),
        file_unique_id.to_owned(),
    );
    aliases.insert(
        normalize_media_reference(file_unique_id),
        file_unique_id.to_owned(),
    );
}

fn normalize_media_reference(value: &str) -> String {
    value.trim().replace("\\_", "_")
}

fn normalize_semantic_tool_value(value: &mut Value, key: Option<&str>) {
    match value {
        Value::String(text) => {
            *text = if key == Some("file_id") {
                normalize_media_reference(text)
            } else {
                text.trim().to_owned()
            };
        }
        Value::Array(values) => {
            for value in values {
                normalize_semantic_tool_value(value, None);
            }
        }
        Value::Object(values) => {
            let mut sorted = BTreeMap::new();
            for (key, mut value) in std::mem::take(values) {
                normalize_semantic_tool_value(&mut value, Some(&key));
                sorted.insert(key, value);
            }
            values.extend(sorted);
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn reused_tool_result(cached: &ToolResult, original_call_id: &str) -> ToolResult {
    let mut result = cached.clone();
    let mut data = match result.data.take() {
        Some(Value::Object(data)) => data,
        Some(value) => serde_json::Map::from_iter([("result".to_owned(), value)]),
        None => serde_json::Map::new(),
    };
    data.insert("reused".to_owned(), Value::Bool(true));
    data.insert(
        "reused_from_call_id".to_owned(),
        Value::String(original_call_id.to_owned()),
    );
    result.data = Some(Value::Object(data));
    result
}

struct TimedToolResult {
    result: ToolResult,
    duration_ms: u128,
}

fn consecutive_understand_media_call_count(calls: &[ChatStepToolCall], start: usize) -> usize {
    calls[start..]
        .iter()
        .take_while(|call| call.step.step == STEP_UNDERSTAND_MEDIA)
        .count()
}

#[allow(clippy::too_many_arguments)]
async fn execute_parallel_understand_media_calls(
    calls: &[ChatStepToolCall],
    index_offset: usize,
    cfg: &SessionTurnConfig<'_>,
    meta: &ToolContext,
    trigger_message_id: i32,
    media_reference_aliases: &BTreeMap<String, String>,
    tool_result_cache: &BTreeMap<String, (String, ToolResult)>,
    budget: &mut SessionBudget,
    processing_started: tokio::time::Instant,
    now: OffsetDateTime,
) -> BTreeMap<usize, TimedToolResult> {
    let mut scheduled_keys = BTreeSet::new();
    let mut jobs = Vec::new();
    for (local_index, call) in calls.iter().enumerate() {
        if call.step.step != STEP_UNDERSTAND_MEDIA {
            continue;
        }
        let semantic_key =
            semantic_tool_call_key(trigger_message_id, &call.step, media_reference_aliases);
        if tool_result_cache.contains_key(&semantic_key) || !scheduled_keys.insert(semantic_key) {
            continue;
        }
        budget.extend_for_tool_start();
        let round_now =
            now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        let slice = session_tool_slice(budget, cfg, round_now);
        jobs.push((index_offset + local_index, call.step.clone(), slice));
    }

    let toolbox = cfg.toolbox;
    let mut remaining = jobs.into_iter();
    let mut pending = FuturesUnordered::new();
    for _ in 0..MAX_PARALLEL_UNDERSTAND_MEDIA_CALLS {
        let Some((index, step, slice)) = remaining.next() else {
            break;
        };
        pending.push(execute_timed_session_tool(
            index,
            toolbox,
            meta.clone(),
            step,
            slice,
        ));
    }
    let mut results = BTreeMap::new();
    while let Some((index, result)) = pending.next().await {
        results.insert(index, result);
        if let Some((next_index, step, slice)) = remaining.next() {
            pending.push(execute_timed_session_tool(
                next_index,
                toolbox,
                meta.clone(),
                step,
                slice,
            ));
        }
    }
    results
}

async fn execute_timed_session_tool(
    index: usize,
    toolbox: &dyn DialogToolbox,
    meta: ToolContext,
    step: ToolStep,
    slice: std::time::Duration,
) -> (usize, TimedToolResult) {
    let started = tokio::time::Instant::now();
    let result = dispatch_session_tool_with_timeout(toolbox, &meta, &step, slice).await;
    (
        index,
        TimedToolResult {
            result,
            duration_ms: started.elapsed().as_millis(),
        },
    )
}

fn session_tool_slice(
    budget: &SessionBudget,
    _cfg: &SessionTurnConfig<'_>,
    round_now: OffsetDateTime,
) -> std::time::Duration {
    let slice = (budget.remaining(round_now) - TOOL_RESERVE).max(TimeDuration::milliseconds(1));
    std::time::Duration::try_from(slice).unwrap_or(std::time::Duration::from_secs(1))
}

async fn dispatch_session_tool_with_timeout(
    toolbox: &dyn DialogToolbox,
    meta: &ToolContext,
    step: &ToolStep,
    slice: std::time::Duration,
) -> ToolResult {
    match tokio::time::timeout(slice, dispatch_dialog_tool(toolbox, meta, step)).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => ToolResult::failed("tool_error", error.to_string()),
        Err(_) => ToolResult::failed(
            "tool_timeout",
            format!("tool did not finish within {}s", slice.as_secs()),
        ),
    }
}

struct SessionToolExecution<'a, 'b> {
    call: &'a ChatStepToolCall,
    cfg: &'a SessionTurnConfig<'b>,
    meta: &'a ToolContext,
    gift_used: &'a mut bool,
    params: &'a openplotva_taskman::DialogJobParams,
    causation_update_id: Option<i64>,
    budget: &'a mut SessionBudget,
    sent: &'a mut SentLog,
    links: &'a DialogLinks,
    draws_scheduled: &'a mut i32,
    songs_scheduled: &'a mut i32,
    reacted_message_ids: &'a mut BTreeSet<i64>,
    budget_extension_granted: &'a mut bool,
    processing_started: tokio::time::Instant,
    now: OffsetDateTime,
}

async fn execute_session_tool<Effects, Queue>(
    exec: SessionToolExecution<'_, '_>,
    effects: &Effects,
    queue: &Queue,
    item_id: i64,
) -> ToolResult
where
    Effects: DialogJobEffects + Sync + ?Sized,
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let step = &exec.call.step;
    let mut tool_meta = exec.meta.clone();
    tool_meta.message_meta.agent_gift = false;
    if step.gift {
        let granted = exec
            .params
            .meta
            .get("dialog_trigger")
            .and_then(Value::as_str)
            == Some("random")
            && exec
                .params
                .meta
                .get("gift_opportunity")
                .and_then(Value::as_bool)
                == Some(true);
        if !granted
            || *exec.gift_used
            || *exec.draws_scheduled + *exec.songs_scheduled > 0
            || !step.file_ids.is_empty()
            || !tool_meta.message_meta.attachments.is_empty()
            || !matches!(step.step.as_str(), STEP_DRAW_IMAGE | STEP_GENERATE_SONG)
        {
            return ToolResult::failed(
                "gift_denied",
                "No unused spontaneous gift opportunity, or this is an edit. Apply ordinary requester permissions.",
            );
        }
        *exec.gift_used = true;
        tool_meta.message_meta.agent_gift = true;
    }
    match step.step.as_str() {
        "finish_turn" => {
            if exec
                .params
                .meta
                .get("dialog_trigger")
                .and_then(Value::as_str)
                != Some("random")
            {
                return ToolResult::failed(
                    "answer_required",
                    "Answer or clarify the addressed request",
                );
            }
            ToolResult {
                status: "ok".into(),
                ..ToolResult::default()
            }
        }
        STEP_SEND_MESSAGE => {
            let mut target = exec.params.clone();
            target.meta["agent_final_reply"] = Value::Bool(false);
            if step.target_message_id != 0 {
                let Ok(id) = i32::try_from(step.target_message_id) else {
                    return ToolResult::failed("invalid_target", "Invalid message ID");
                };
                target.message_id = id;
            }
            if target.message_id != exec.params.message_id || !step.quote.is_empty() {
                let result = exec
                    .cfg
                    .toolbox
                    .agent_tool(
                        exec.meta.clone(),
                        ToolStep {
                            step: "get_messages".into(),
                            message_ids: vec![target.message_id],
                            ..ToolStep::default()
                        },
                    )
                    .await;
                let source = result
                    .ok()
                    .and_then(|result| result.data)
                    .and_then(|data| {
                        data.get("messages")
                            .and_then(Value::as_array)
                            .and_then(|messages| {
                                messages.iter().find(|message| {
                                    message["message_id"].as_i64()
                                        == Some(i64::from(target.message_id))
                                })
                            })
                            .cloned()
                    })
                    .and_then(|message| message.get("message").cloned())
                    .and_then(|payload| {
                        openplotva_history::decode_summary_message_entry_payload(
                            payload.to_string().as_bytes(),
                        )
                        .ok()
                    });
                if target.message_id != exec.params.message_id && source.is_none() {
                    return ToolResult::failed(
                        "unknown_target",
                        "Read a retained message from this chat before replying to it",
                    );
                }
                if let Some(source) = source {
                    let source_text = if !source.text.is_empty() {
                        &source.text
                    } else if !source.caption.is_empty() {
                        &source.caption
                    } else {
                        &source.original_text
                    };
                    if let Some(position) = exact_quote_position(source_text, &step.quote) {
                        target.meta["agent_quote"] = Value::String(step.quote.clone());
                        target.meta["agent_quote_position"] = serde_json::json!(position);
                    }
                }
                target.meta["agent_reply_explicit"] = Value::Bool(true);
            }
            let sanitized = exec.links.prepare_response(&step.text);
            if sanitized.trim().is_empty() {
                return ToolResult::failed("empty_text", "message text is empty after sanitizing");
            }
            let round_now = exec.now
                + TimeDuration::try_from(exec.processing_started.elapsed()).unwrap_or_default();
            try_send_intermediate(
                &target,
                effects,
                queue,
                item_id,
                exec.causation_update_id,
                exec.sent,
                exec.cfg.max_messages,
                &sanitized,
                round_now,
            )
            .await
        }
        STEP_REACT_TO_MESSAGE => {
            let emoji = step.emoji.trim();
            if !SESSION_REACTION_ALLOWED_EMOJI.contains(&emoji) {
                return ToolResult::failed(
                    "emoji_not_allowed",
                    "this emoji is not in the allowed reaction list",
                );
            }
            let message_id = if step.target_message_id != 0 {
                step.target_message_id
            } else {
                i64::from(exec.params.message_id)
            };
            if !exec.reacted_message_ids.insert(message_id) {
                return ToolResult::failed(
                    "already_reacted",
                    "you already reacted to this message this turn; a new reaction only replaces it",
                );
            }
            let Some(reactor) = exec.cfg.reactor else {
                return ToolResult::failed("reactions_unavailable", "reactions are not wired");
            };
            // The chat id is always the session's own chat — the model cannot
            // react into other chats no matter what it passes.
            match reactor.react(exec.params.chat_id, message_id, emoji).await {
                Ok(()) => ToolResult {
                    status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
                    message: "reaction set".to_owned(),
                    ..ToolResult::default()
                },
                Err(error) => ToolResult::failed("reaction_failed", error),
            }
        }
        STEP_DRAW_IMAGE if *exec.draws_scheduled >= exec.cfg.max_draws.max(0) => {
            ToolResult::failed(
                "draw_limit",
                "image generation was already scheduled this turn",
            )
        }
        STEP_GENERATE_SONG if *exec.songs_scheduled >= exec.cfg.max_songs.max(0) => {
            ToolResult::failed(
                "song_limit",
                "song generation was already scheduled this turn",
            )
        }
        _ => {
            exec.budget.extend_for_tool_start();
            *exec.budget_extension_granted = true;
            let round_now = exec.now
                + TimeDuration::try_from(exec.processing_started.elapsed()).unwrap_or_default();
            let slice = session_tool_slice(exec.budget, exec.cfg, round_now);
            let result =
                dispatch_session_tool_with_timeout(exec.cfg.toolbox, &tool_meta, step, slice).await;
            if queued_generation_side_effect(&result).is_some() {
                match step.step.as_str() {
                    STEP_DRAW_IMAGE => *exec.draws_scheduled += 1,
                    STEP_GENERATE_SONG => *exec.songs_scheduled += 1,
                    _ => {}
                }
            }
            result
        }
    }
}

fn exact_quote_position(source: &str, quote: &str) -> Option<usize> {
    if quote.is_empty() || quote.chars().count() > 1024 {
        return None;
    }
    source
        .find(quote)
        .map(|position| source[..position].encode_utf16().count())
}

/// Queue one intermediate message, honoring the per-session cap and the
/// duplicate guard; every outcome comes back as a tool result the model reads.
#[allow(clippy::too_many_arguments)]
async fn try_send_intermediate<Effects, Queue>(
    params: &openplotva_taskman::DialogJobParams,
    effects: &Effects,
    queue: &Queue,
    item_id: i64,
    causation_update_id: Option<i64>,
    sent: &mut SentLog,
    max_messages: i32,
    sanitized: &str,
    now: OffsetDateTime,
) -> ToolResult
where
    Effects: DialogJobEffects + Sync + ?Sized,
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    if sent.matches_target_delivery(sanitized, params.message_id) {
        return ToolResult {
            status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
            data: Some(serde_json::json!({"already_delivered": true})),
            message: "This text was already delivered; continue without resending it".to_owned(),
            ..ToolResult::default()
        };
    }
    if i64::from(sent.intermediate_count) >= i64::from(max_messages.max(0)) {
        return ToolResult::failed(
            "message_limit",
            "per-turn message limit reached; write your final answer",
        );
    }
    if let Err(validation) = validate_dialog_answer_deliverable(sanitized) {
        return ToolResult::failed("undeliverable", validation.to_string());
    }
    let first_send = !sent.any();
    let explicit_reply = params
        .meta
        .get("agent_reply_explicit")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let seq = sent.intermediate_count + 1;
    match effects
        .send_dialog_intermediate(
            item_id,
            causation_update_id,
            params,
            sanitized,
            seq,
            first_send || explicit_reply,
        )
        .await
    {
        Ok(()) => {
            sent.record(sanitized, true, params.message_id);
            if first_send {
                append_session_intermediate_marker(queue, item_id, now).await;
            }
            ToolResult {
                status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
                message: "message sent".to_owned(),
                ..ToolResult::default()
            }
        }
        Err(error) => ToolResult::failed("send_failed", error.to_string()),
    }
}

fn recorded_session_tool_call(
    step: &ToolStep,
    result: &ToolResult,
    call_id: &str,
    iteration: i32,
) -> ToolCall {
    let r#ref = if call_id.trim().is_empty() {
        format!("{}-{iteration}", step.step)
    } else {
        call_id.trim().to_owned()
    };
    ToolCall {
        name: step.step.clone(),
        r#ref,
        input: serde_json::to_value(step).ok(),
        output: serde_json::to_value(result).ok(),
        at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .ok()
            .map(|at| at.to_string()),
    }
}

async fn append_session_sent_marker<Queue>(queue: &Queue, job_id: i64, at: OffsetDateTime)
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: SESSION_MESSAGE_SENT_STAGE.to_owned(),
        message: "session message accepted by the outbound queue".to_owned(),
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::warn!(
            error = %error,
            job_id,
            "failed to append session_message_sent marker; a rerun may re-send"
        );
    }
}

async fn append_session_intermediate_marker<Queue>(queue: &Queue, job_id: i64, at: OffsetDateTime)
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let event = TaskQueueJobEvent {
        stage: SESSION_INTERMEDIATE_QUEUED_STAGE.to_owned(),
        message: "accepted by durable outbound queue".to_owned(),
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::warn!(%error, job_id, "failed to append intermediate queued marker");
    }
}

async fn append_session_queued_marker<Queue>(
    queue: &Queue,
    job_id: i64,
    receipt: &crate::dialog_jobs::QueuedBatchReceipt,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert("batch_id".to_owned(), receipt.batch_id.clone());
    data.insert("operation_ids".to_owned(), receipt.operation_ids.join(","));
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: ANSWER_QUEUED_STAGE.to_owned(),
        message: "final dialog answer committed to Telegram outbox".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::warn!(
            error = %error,
            job_id,
            "failed to append answer_queued marker; durable outbox preflight remains authoritative"
        );
    }
}

async fn append_session_regeneration_event<Queue>(
    queue: &Queue,
    job_id: i64,
    attempt: i32,
    duplicate_message_id: i32,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert(
        "duplicate_message_id".to_owned(),
        duplicate_message_id.to_string(),
    );
    data.insert("reason".to_owned(), "dedup_regenerate".to_owned());
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: DIALOG_TURN_REGENERATE_STAGE.to_owned(),
        attempt,
        message: "session final answer duplicated a sent message; regenerating".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::debug!(error = %error, job_id, "failed to append session regeneration event");
    }
}

async fn append_repeated_final_regeneration_event<Queue>(
    queue: &Queue,
    job_id: i64,
    attempt: i32,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: DIALOG_TURN_REGENERATE_STAGE.to_owned(),
        attempt,
        message: "session final answer replayed intermediate messages; regenerating".to_owned(),
        data: BTreeMap::from([("reason".to_owned(), "session_replay_regenerate".to_owned())]),
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::debug!(error = %error, job_id, "failed to append session regeneration event");
    }
}

#[allow(clippy::too_many_arguments)]
async fn append_session_iteration_event<Queue>(
    queue: &Queue,
    job_id: i64,
    iteration: i32,
    provider: &str,
    model: &str,
    latency_ms: u128,
    tool_calls: usize,
    text_len: usize,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert("provider".to_owned(), provider.to_owned());
    data.insert("model".to_owned(), model.to_owned());
    data.insert("latency_ms".to_owned(), latency_ms.to_string());
    data.insert("tool_calls".to_owned(), tool_calls.to_string());
    data.insert("text_len".to_owned(), text_len.to_string());
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: SESSION_ITERATION_STAGE.to_owned(),
        attempt: iteration,
        message: "session iteration completed".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::debug!(error = %error, job_id, "failed to append session iteration event");
    }
}

async fn append_session_tool_event<Queue>(
    queue: &Queue,
    job_id: i64,
    name: &str,
    status: &str,
    duration_ms: u128,
    budget_extension_granted: bool,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert("tool".to_owned(), name.to_owned());
    data.insert("status".to_owned(), status.to_owned());
    data.insert("duration_ms".to_owned(), duration_ms.to_string());
    data.insert(
        SESSION_TOOL_BUDGET_EXTENSION_GRANTED_KEY.to_owned(),
        budget_extension_granted.to_string(),
    );
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: SESSION_TOOL_STAGE.to_owned(),
        message: "session tool executed".to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::debug!(error = %error, job_id, "failed to append session tool event");
    }
}

async fn append_session_batch_event<Queue>(
    queue: &Queue,
    job_id: i64,
    iteration: i32,
    disposition: SessionBatchDisposition,
    step_text_accounted_for: bool,
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let event = TaskQueueJobEvent {
        level: "info".to_owned(),
        stage: SESSION_BATCH_STAGE.to_owned(),
        attempt: iteration,
        message: "session tool batch disposition selected".to_owned(),
        data: BTreeMap::from([
            ("disposition".to_owned(), disposition.as_str().to_owned()),
            (
                "step_text_accounted_for".to_owned(),
                step_text_accounted_for.to_string(),
            ),
        ]),
        ..TaskQueueJobEvent::default()
    };
    if let Err(error) = queue.append_dialog_job_event(job_id, event, at).await {
        tracing::debug!(error = %error, job_id, "failed to append session batch event");
    }
}

/// Output of one captured (non-dispatching) session run for the runtime
/// virtual dialog console: texts in send order plus the recorded tool calls.
pub struct CapturedSessionOutput {
    pub messages: Vec<String>,
    pub tool_calls: Vec<ToolCall>,
    pub provider: String,
}

/// Run the production engine with in-memory delivery and history.
pub async fn run_captured_session(
    step_provider: &dyn ChatStepProvider,
    toolbox: &dyn DialogToolbox,
    base_input: DialogInput,
    max_iterations: i32,
) -> Result<CapturedSessionOutput, String> {
    use crate::dialog_jobs::{BasicDialogInputMaterializer, NoopDialogToolCallHistoryStore};
    let now = OffsetDateTime::now_utc();
    let seed = openplotva_taskman::DialogJobParams {
        chat_id: base_input.context.chat_id,
        message_id: base_input.message.id,
        user_id: base_input.user.id,
        user_full_name: base_input.user.full_name.clone(),
        message_text: base_input.message.text.clone(),
        original_text: String::new(),
        meta: serde_json::Value::Null,
        max_output_tokens: base_input.max_output_tokens,
        thread_id: base_input.context.thread_id,
    };
    let queue = openplotva_taskman::InMemoryTaskQueue::default();
    let job = openplotva_taskman::new_dialog_job_at(seed.clone(), now);
    let id = queue.assign("captured-dialog", job.clone());
    let item = crate::dialog_jobs::DialogJobWorkItem {
        id,
        job,
        events: Vec::new(),
        claim_started_at: now,
        source_update_ids: Vec::new(),
        latest_update_id: None,
    };
    let capture = CaptureDelivery::default();
    let cfg = SessionTurnConfig {
        toolbox,
        reactor: Some(&capture),
        gradius: None,
        max_iterations,
        max_messages: 4,
        tool_extension_secs: 0,
        hard_cap_secs: 120,
        max_draws: 1,
        max_songs: 1,
    };
    let ctx = SessionRunContext {
        item_id: id,
        item_events: &[],
        params: &seed,
        queue_name: "captured-dialog",
        max_llm_job_attempts: 1,
        max_regenerations: 2,
        budget: TurnBudget::from_events(&[], 120, now),
        now,
        routing_events: None,
        item: &item,
        inbox: None,
        llm_runs: None,
    };
    let history = base_input.history.clone();
    let mut report = DialogJobWorkerReport::default();
    let resolution = run_dialog_session(
        ctx,
        &cfg,
        step_provider,
        base_input,
        &history,
        &queue,
        &capture,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        &mut report,
    )
    .await;
    match resolution.disposition {
        JobDisposition::Fail(error) => return Err(error),
        JobDisposition::Requeue(_) => {
            return Err("The captured turn ended without a usable answer".into());
        }
        _ => {}
    }
    if let Some(error) = report.provider_error {
        return Err(error);
    }
    Ok(CapturedSessionOutput {
        messages: capture.messages.into_inner().expect("capture delivery"),
        tool_calls: report.session_tool_calls,
        provider: report.provider.unwrap_or_default(),
    })
}

#[derive(Default)]
struct CaptureDelivery {
    messages: std::sync::Mutex<Vec<String>>,
}
impl SessionReactor for CaptureDelivery {
    fn react<'a>(&'a self, _: i64, _: i64, _: &'a str) -> SessionReactionFuture<'a> {
        Box::pin(async { Ok(()) })
    }
}
impl DialogJobEffects for CaptureDelivery {
    type Error = String;
    fn send_dialog_answer<'a>(
        &'a self,
        _: i64,
        _: Option<i64>,
        _: &'a openplotva_taskman::DialogJobParams,
        answer: &'a str,
        _: DialogAnswerSendOptions,
    ) -> crate::dialog_jobs::DialogJobReceiptFuture<'a, String> {
        Box::pin(async move {
            self.messages
                .lock()
                .expect("capture delivery")
                .push(answer.to_owned());
            Ok(crate::dialog_jobs::QueuedBatchReceipt::dispatcher(
                "capture".to_owned(),
                Vec::new(),
            ))
        })
    }
    fn send_dialog_intermediate<'a>(
        &'a self,
        _: i64,
        _: Option<i64>,
        _: &'a openplotva_taskman::DialogJobParams,
        text: &'a str,
        _: u32,
        _: bool,
    ) -> crate::dialog_jobs::DialogJobEffectFuture<'a, String> {
        Box::pin(async move {
            self.messages
                .lock()
                .expect("capture delivery")
                .push(text.to_owned());
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use openplotva_dialog::{ToolboxFuture, VisionRequest};

    use super::*;

    #[derive(Default)]
    struct BudgetProbe {
        requests: std::sync::Mutex<Vec<ChatStepRequest>>,
        tool_executions: AtomicUsize,
    }
    impl ChatStepProvider for BudgetProbe {
        fn provider_name(&self) -> &str {
            "probe"
        }
        fn supports_native_tools(&self) -> bool {
            true
        }
        fn run_chat_step<'a>(
            &'a self,
            request: ChatStepRequest,
        ) -> openplotva_llm::ChatStepFuture<'a> {
            Box::pin(async move {
                let final_only = matches!(request.tools, ToolsMode::FinalOnly);
                let iteration = request.iteration;
                self.requests.lock().expect("requests").push(request);
                Ok(openplotva_dialog::ChatStepOutput {
                    provider: "probe".into(),
                    model: "probe-model".into(),
                    text: if final_only {
                        "Проверено.".into()
                    } else {
                        String::new()
                    },
                    tool_calls: if final_only {
                        Vec::new()
                    } else {
                        vec![ChatStepToolCall {
                            id: format!("call-{iteration}"),
                            step: ToolStep {
                                step: openplotva_dialog::STEP_CURRENCY_RATES.into(),
                                ..Default::default()
                            },
                            salvaged: false,
                        }]
                    },
                    ..Default::default()
                })
            })
        }
    }
    impl DialogToolbox for BudgetProbe {
        fn currency_rates<'a>(&'a self, _: openplotva_dialog::RatesRequest) -> ToolboxFuture<'a> {
            self.tool_executions.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(ToolResult {
                    status: "ok".into(),
                    data: Some(serde_json::json!({"rate":1})),
                    ..Default::default()
                })
            })
        }
    }
    #[tokio::test]
    async fn captured_and_production_loop_count_cached_calls_and_keep_initial_packet() {
        let probe = BudgetProbe::default();
        let mut input = DialogInput::default();
        input.context.chat_id = -100;
        input.user.id = 99;
        input.message.id = 7;
        input.message.text = "проверь".into();
        let output = run_captured_session(&probe, &probe, input.clone(), 36)
            .await
            .expect("session");
        assert_eq!(output.messages, vec!["Проверено."]);
        assert_eq!(output.tool_calls.len(), 32);
        assert_eq!(probe.tool_executions.load(Ordering::SeqCst), 1);
        let requests = probe.requests.lock().expect("requests");
        assert_eq!(requests.len(), 33);
        assert!(requests.iter().all(|request| request.input == input));
        assert!(requests[0].preferred_target.is_none());
        assert!(requests[1..].iter().all(|request| {
            request.preferred_target.as_ref() == Some(&("probe".into(), "probe-model".into()))
        }));
        assert!(matches!(
            requests.last().expect("last step").tools,
            ToolsMode::FinalOnly
        ));
        assert!(
            requests
                .windows(2)
                .all(|pair| pair[1].transcript.len() > pair[0].transcript.len())
        );
    }

    #[test]
    fn exact_quotes_use_utf16_offsets_and_invalid_quotes_are_optional() {
        assert_eq!(
            exact_quote_position("🐟 Exact words", "Exact words"),
            Some(3)
        );
        assert_eq!(exact_quote_position("Exact words", "exact words"), None);
        assert_eq!(exact_quote_position("Exact words", ""), None);
        assert_eq!(
            exact_quote_position(&"x".repeat(1025), &"x".repeat(1025)),
            None
        );
    }

    #[tokio::test]
    async fn delivered_duplicate_succeeds_without_sending_even_at_message_cap() {
        let params = openplotva_taskman::DialogJobParams {
            chat_id: 1,
            message_id: 7,
            user_id: 2,
            user_full_name: String::new(),
            message_text: String::new(),
            original_text: String::new(),
            meta: Value::Null,
            max_output_tokens: 128,
            thread_id: None,
        };
        let capture = CaptureDelivery::default();
        let queue = openplotva_taskman::InMemoryTaskQueue::default();
        let mut sent = SentLog::new();
        sent.record("Already sent", true, params.message_id);
        let result = try_send_intermediate(
            &params,
            &capture,
            &queue,
            1,
            None,
            &mut sent,
            1,
            "<b>Already sent</b>",
            OffsetDateTime::now_utc(),
        )
        .await;
        assert_eq!(result.status, "ok");
        assert_eq!(result.data.expect("receipt")["already_delivered"], true);
        assert_eq!(sent.intermediate_count, 1);
        assert!(capture.messages.lock().expect("capture").is_empty());
    }

    #[test]
    fn sent_log_matches_html_equivalent_and_aggregate_replays() {
        let mut sent = SentLog::new();
        sent.record("Ну и <b>юмор</b>", true, 7);

        assert!(sent.matches_delivery("<p>Ну и юмор</p>"));
        assert!(sent.matches_target_delivery("<p>Ну и юмор</p>", 7));
        assert!(!sent.matches_target_delivery("<p>Ну и юмор</p>", 8));

        sent.record("Ещё реплика", true, 7);
        assert!(sent.matches_delivery("Ну и юмор\n\nЕщё реплика"));
    }

    #[test]
    fn batch_disposition_follows_tool_semantics_and_results() {
        let call = |name: &str| ChatStepToolCall {
            id: format!("call-{name}"),
            step: ToolStep {
                step: name.to_owned(),
                ..ToolStep::default()
            },
            salvaged: false,
        };
        let ok = || ToolResult {
            status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
            ..ToolResult::default()
        };
        let queued = || ToolResult {
            status: openplotva_dialog::TOOL_RESULT_STATUS_QUEUED.to_owned(),
            side_effect: Some(openplotva_dialog::ToolSideEffect {
                kind: SIDE_EFFECT_KIND_IMAGE.to_owned(),
                state: SIDE_EFFECT_STATE_QUEUED.to_owned(),
                ..openplotva_dialog::ToolSideEffect::default()
            }),
            ..ToolResult::default()
        };

        assert_eq!(
            session_batch_disposition(&[call(STEP_REACT_TO_MESSAGE)], &[ok()]),
            SessionBatchDisposition::ContinueWithoutFinal
        );
        assert_eq!(
            session_batch_disposition(&[call(STEP_DRAW_IMAGE)], &[queued()]),
            SessionBatchDisposition::CompleteWithSideEffect
        );
        assert_eq!(
            session_batch_disposition(
                &[call(STEP_DRAW_IMAGE)],
                &[ToolResult::failed("draw_failed", "draw failed")],
            ),
            SessionBatchDisposition::ContinueForResults
        );
        assert_eq!(
            session_batch_disposition(
                &[call(STEP_DRAW_IMAGE), call(STEP_WEB_SEARCH)],
                &[queued(), ok()],
            ),
            SessionBatchDisposition::ContinueForResults
        );
        assert_eq!(
            session_batch_disposition(&[call(STEP_SEND_MESSAGE)], &[ok()]),
            SessionBatchDisposition::ContinueWithoutFinal
        );
    }

    struct ParallelMediaToolbox {
        barrier: tokio::sync::Barrier,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl ParallelMediaToolbox {
        fn new(parties: usize) -> Self {
            Self {
                barrier: tokio::sync::Barrier::new(parties),
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
            }
        }
    }

    impl DialogToolbox for ParallelMediaToolbox {
        fn understand_media<'a>(&'a self, request: VisionRequest) -> ToolboxFuture<'a> {
            Box::pin(async move {
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(active, Ordering::SeqCst);
                self.barrier.wait().await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ToolResult {
                    status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
                    message: request.file_id,
                    ..ToolResult::default()
                })
            })
        }
    }

    #[test]
    fn session_native_tools_exclude_agent_only_tools() {
        let tools = session_native_tools(true).expect("native tool schemas");
        let names = tools
            .iter()
            .filter_map(|tool| tool.pointer("/function/name").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert!(names.contains(&openplotva_dialog::STEP_MEMORY_SEARCH));
        assert!(names.contains(&STEP_SEND_MESSAGE));
        assert!(names.contains(&STEP_REACT_TO_MESSAGE));
        assert!(names.contains(&"finish_turn"));
        assert!(
            session_native_tools(false)
                .expect("addressed schemas")
                .iter()
                .all(|tool| {
                    tool.pointer("/function/name").and_then(Value::as_str) != Some("finish_turn")
                })
        );
    }

    #[tokio::test]
    async fn independent_understand_media_calls_start_in_parallel_and_keep_call_order() {
        let toolbox = Arc::new(ParallelMediaToolbox::new(2));
        let cfg = SessionTurnConfig {
            toolbox: toolbox.as_ref(),
            reactor: None,
            gradius: None,
            max_iterations: 4,
            max_messages: 4,
            tool_extension_secs: 10,
            hard_cap_secs: 60,
            max_draws: 1,
            max_songs: 1,
        };
        let calls = ["file-a", "file-b"]
            .into_iter()
            .enumerate()
            .map(|(index, file_id)| ChatStepToolCall {
                id: format!("call-{index}"),
                step: ToolStep {
                    step: STEP_UNDERSTAND_MEDIA.to_owned(),
                    file_id: file_id.to_owned(),
                    ..ToolStep::default()
                },
                salvaged: false,
            })
            .collect::<Vec<_>>();
        let now = OffsetDateTime::UNIX_EPOCH;
        let base = TurnBudget::from_events(&[], 30, now);
        let mut budget = SessionBudget::new(base, 10, 60);

        let results = execute_parallel_understand_media_calls(
            &calls,
            0,
            &cfg,
            &ToolContext::default(),
            42,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &mut budget,
            tokio::time::Instant::now(),
            now,
        )
        .await;

        assert_eq!(toolbox.max_active.load(Ordering::SeqCst), 2);
        assert_eq!(results[&0].result.message, "file-a");
        assert_eq!(results[&1].result.message, "file-b");
    }

    #[test]
    fn media_parallelism_does_not_cross_side_effect_tool_boundaries() {
        let call = |id: &str, step: &str| ChatStepToolCall {
            id: id.to_owned(),
            step: ToolStep {
                step: step.to_owned(),
                file_id: id.to_owned(),
                ..ToolStep::default()
            },
            salvaged: false,
        };
        let calls = vec![
            call("announce", STEP_SEND_MESSAGE),
            call("media-a", STEP_UNDERSTAND_MEDIA),
            call("media-b", STEP_UNDERSTAND_MEDIA),
            call("react", STEP_REACT_TO_MESSAGE),
            call("media-c", STEP_UNDERSTAND_MEDIA),
        ];

        assert_eq!(consecutive_understand_media_call_count(&calls, 1), 2);
        assert_eq!(consecutive_understand_media_call_count(&calls, 4), 1);
    }
}
