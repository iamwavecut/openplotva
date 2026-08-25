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
    STEP_DRAW_IMAGE, STEP_GENERATE_SONG, STEP_REACT_TO_MESSAGE, STEP_SEND_MESSAGE,
    STEP_UNDERSTAND_MEDIA, STEP_WEB_SEARCH, SessionMessage, SessionToolCall, ToolContext,
    ToolContinuation, ToolResult, ToolStep, ToolsMode, chat_completion_tools_for_specs,
    dialog_tool_context, dialog_tool_continuation, dispatch_dialog_tool,
    turn::{
        ANTI_LOOP_HINT, QueuedSideEffect, SIDE_EFFECT_KIND_IMAGE, SIDE_EFFECT_KIND_MUSIC,
        SIDE_EFFECT_STATE_QUEUED,
    },
};
use openplotva_llm::ChatStepProvider;
use openplotva_taskman::TaskQueueJobEvent;
use serde_json::Value;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

use super::budget::{SessionBudget, TURN_DEADLINE, TurnBudget};
use super::engine::{ANSWER_QUEUED_STAGE, ANSWER_SENT_STAGE, DIALOG_TURN_REGENERATE_STAGE};
use super::outcome::{JobDisposition, TurnOutcome, TurnResolution, UserSignalPlan};
use crate::dialog_jobs::{
    DialogAnswerSendOptions, DialogJobEffects, DialogJobWorkerQueue, DialogJobWorkerReport,
    DialogToolCallHistoryStore, PROVIDER_EMPTY_RETRY_CODES, PROVIDER_ERROR_RETRY_CODES,
    RetryableDialogProviderFailure, SANITIZED_EMPTY_RETRY_CODES, UNDELIVERABLE_RETRY_CODES,
    handle_retryable_dialog_provider_error, persist_dialog_tool_calls,
    prepare_dialog_chat_response, should_suppress_duplicate_bot_reply,
    validate_dialog_answer_deliverable,
};

/// Job event stage appended after the FIRST outbound send of a session; on
/// re-entry (crash between a send and the status write) the turn resolves
/// `Sent` without replaying anything.
pub const SESSION_MESSAGE_SENT_STAGE: &str = "session_message_sent";
pub const SESSION_INTERMEDIATE_QUEUED_STAGE: &str = "session_intermediate_queued";

/// Job event stage recorded per session LLM iteration (audit only).
pub const SESSION_ITERATION_STAGE: &str = "session_iteration";

/// Job event stage recorded per executed session tool (audit only).
pub const SESSION_TOOL_STAGE: &str = "session_tool";

/// Job event stage recording why a tool-call batch continued or completed.
pub const SESSION_BATCH_STAGE: &str = "session_batch";

/// A session never starts an iteration it cannot plausibly finish.
const MIN_GENERATION_BUDGET: TimeDuration = TimeDuration::seconds(15);

/// A duplicate final answer is regenerated only with this much budget left.
const MIN_REGENERATION_BUDGET: TimeDuration = TimeDuration::seconds(10);

/// A searched answer gets two focused rewrites before the best available final
/// answer is sent, even if the model still omitted a citation.
const MAX_SEARCH_CITATION_REPAIRS: i32 = 2;

const SEARCH_CITATION_REPAIR_HINT: &str = "ОБЯЗАТЕЛЬНАЯ ПРОВЕРКА ИСТОЧНИКОВ: предыдущий черновик финального ответа не содержит требуемой inline-ссылки на реально найденный источник. Перепиши финальный ответ без нового поиска и без упоминания этой проверки. Если ты используешь сведения из web_search/crawl_url, ответ ОБЯЗАН содержать хотя бы одну семантическую inline HTML-ссылку вида <a href=\"URL\">подтверждаемая фраза</a>, где href в точности совпадает с одним из URL в уже имеющихся результатах. Размещай ссылку прямо на подтверждаемом утверждении; не печатай raw URL или отдельную библиографию.";

/// Slice reserved for the final send after a tool call finishes.
const TOOL_RESERVE: TimeDuration = TimeDuration::seconds(5);

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

struct SentLog {
    texts: Vec<String>,
    intermediate_count: u32,
    total_count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionBatchDisposition {
    ContinueForResults,
    CompleteWithSideEffect,
    CompleteAfterSidecars,
    ContinueWithoutFinal,
}

impl SessionBatchDisposition {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ContinueForResults => "continue_for_results",
            Self::CompleteWithSideEffect => "complete_with_side_effect",
            Self::CompleteAfterSidecars => "complete_after_sidecars",
            Self::ContinueWithoutFinal => "continue_without_final",
        }
    }
}

impl SentLog {
    fn new() -> Self {
        Self {
            texts: Vec::new(),
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

    fn record(&mut self, text: &str, intermediate: bool) {
        self.texts.push(canonical_visible_text(text));
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
fn session_native_tools() -> Result<Vec<Value>, String> {
    let names = openplotva_dialog::alternative_dialog_tool_names();
    let mut specs = openplotva_dialog::alternative_dialog_tools()
        .into_iter()
        .filter(|spec| names.contains(&spec.name))
        .collect::<Vec<_>>();
    specs.push(SESSION_SEND_MESSAGE_SPEC);
    specs.push(SESSION_REACT_TO_MESSAGE_SPEC);
    chat_completion_tools_for_specs(&specs)
        .into_iter()
        .map(|tool| serde_json::to_value(tool).map_err(|error| error.to_string()))
        .collect()
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

    let mut budget = SessionBudget::new(ctx.budget, cfg.tool_extension_secs, cfg.hard_cap_secs);
    let mut base_input = base_input;
    let mut duplicate_guard_history = duplicate_guard_history.to_vec();
    let mut active_params =
        crate::dialog_jobs::dialog_job_params_from_input(ctx.params, &base_input);
    let mut meta = dialog_tool_context(&base_input);
    let native_tools = match session_native_tools() {
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
    let mut transcript: Vec<SessionMessage> = Vec::new();
    let mut sent = SentLog::new();
    let mut side_effect_tickets: Vec<QueuedSideEffect> = Vec::new();
    let mut recorded_tool_calls: Vec<ToolCall> = Vec::new();
    let mut regenerations: i32 = 0;
    let mut anti_loop = false;
    let mut repeated_final_repair = false;
    // After a work tool, progress messages alone cannot satisfy the answer.
    let mut requires_novel_final = false;
    let mut draws_scheduled: i32 = 0;
    let mut songs_scheduled: i32 = 0;
    let mut reacted_message_ids: BTreeSet<i64> = BTreeSet::new();
    let mut tool_result_cache: BTreeMap<String, (String, ToolResult)> = BTreeMap::new();
    let mut media_reference_aliases: BTreeMap<String, String> = BTreeMap::new();
    let mut successful_web_search = false;
    let mut web_source_urls = BTreeSet::new();
    let mut search_citation_repairs: i32 = 0;
    let max_iterations = cfg.max_iterations.max(1);

    let mut iteration: i32 = 0;
    loop {
        iteration += 1;
        if let Some(inbox) = ctx.inbox.as_ref()
            && let Some(injected) = inbox.drain_open().into_iter().last()
        {
            let injected_params = injected.params;
            match materializer
                .materialize_dialog_input(&injected_params, OffsetDateTime::now_utc())
                .await
            {
                Ok(input) => {
                    active_params =
                        crate::dialog_jobs::dialog_job_params_from_input(&injected_params, &input);
                    meta = dialog_tool_context(&input);
                    duplicate_guard_history.clone_from(&input.history);
                    base_input = input;
                    transcript.clear();
                    tool_result_cache.clear();
                    media_reference_aliases.clear();
                    successful_web_search = false;
                    web_source_urls.clear();
                    search_citation_repairs = 0;
                    repeated_final_repair = false;
                }
                Err(crate::dialog_jobs::DialogInputMaterializationError::SenderNotMember {
                    ..
                }) => {
                    tracing::debug!(
                        chat_id = injected_params.chat_id,
                        user_id = injected_params.user_id,
                        "dropping injected message from a departed member"
                    );
                }
                Err(error) => {
                    let error = format!("materialize injected dialog input: {error}");
                    report.materialization_error = Some(error.clone());
                    return TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "injected_input_materialization",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    };
                }
            }
        }
        let round_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        if budget.remaining(round_now) < MIN_GENERATION_BUDGET || iteration > max_iterations {
            return session_exhausted(ctx.item_id, &sent, &side_effect_tickets, &budget, round_now);
        }

        let force_final = iteration == max_iterations;
        let tools = if base_input.disable_tools {
            ToolsMode::Disabled
        } else if force_final || search_citation_repairs > 0 || repeated_final_repair {
            ToolsMode::FinalOnly
        } else {
            ToolsMode::Native(native_tools.clone())
        };

        let mut input = base_input.clone();
        if anti_loop {
            input.reference_context.push(ANTI_LOOP_HINT.to_owned());
        }
        if search_citation_repairs > 0 {
            input
                .reference_context
                .push(SEARCH_CITATION_REPAIR_HINT.to_owned());
        }
        let provider_deadline = Instant::now()
            + std::time::Duration::try_from(budget.remaining(round_now)).unwrap_or_default();
        let step_started = tokio::time::Instant::now();
        let result = TURN_DEADLINE
            .scope(
                Some(provider_deadline),
                step_provider.run_chat_step(ChatStepRequest {
                    input,
                    transcript: transcript.clone(),
                    tools,
                    iteration: usize::try_from(iteration).unwrap_or(1),
                }),
            )
            .await;
        let failure_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        let step = match result {
            Ok(step) => step,
            Err(error) if openplotva_llm::is_content_blocked_error(error.as_ref()) => {
                report.content_blocked = true;
                return TurnResolution {
                    outcome: TurnOutcome::NoReplyIntentional {
                        reason: "content_blocked",
                    },
                    disposition: JobDisposition::Complete,
                };
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
                        return TurnResolution {
                            outcome: TurnOutcome::TerminalFailed {
                                reason: "llm_failed_after_partial",
                                error: error.clone(),
                                user_signal: UserSignalPlan::React,
                            },
                            disposition: JobDisposition::Fail(error),
                        };
                    }
                    return handle_retryable_dialog_provider_error(
                        queue,
                        ctx.item,
                        &active_params,
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
                    .await;
                }
                return TurnResolution {
                    outcome: TurnOutcome::TerminalFailed {
                        reason: "provider_error",
                        error: error.clone(),
                        user_signal: UserSignalPlan::React,
                    },
                    disposition: JobDisposition::Fail(error),
                };
            }
        };
        report.session_iterations = iteration;
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
            let sanitized = prepare_dialog_chat_response(&raw_answer);
            if sanitized.trim().is_empty() {
                if !side_effect_tickets.is_empty() {
                    // Silent side-effect finish (should have terminated at
                    // the tool batch already; kept as a safety net).
                    return session_delegated(&sent, &side_effect_tickets);
                }
                if sent.any() {
                    let error =
                        "dialog provider returned no final answer after intermediate messages"
                            .to_owned();
                    return TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "empty_final_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    };
                }
                let (codes, error) = if raw_answer.trim().is_empty() {
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
                return handle_retryable_dialog_provider_error(
                    queue,
                    ctx.item,
                    &active_params,
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
                .await;
            }

            if !web_source_urls.is_empty() && !answer_cites_web_source(&sanitized, &web_source_urls)
            {
                if search_citation_repairs < MAX_SEARCH_CITATION_REPAIRS
                    && iteration < max_iterations
                    && budget.remaining(failure_now) >= MIN_REGENERATION_BUDGET
                {
                    search_citation_repairs += 1;
                    tracing::info!(
                        job_id = ctx.item_id,
                        attempt = search_citation_repairs,
                        sources = web_source_urls.len(),
                        "regenerating searched answer without a source citation"
                    );
                    continue;
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
                        append_repeated_final_regeneration_event(
                            queue,
                            ctx.item_id,
                            regenerations,
                            failure_now,
                        )
                        .await;
                        continue;
                    }
                    let error = format!(
                        "dialog final answer only replayed intermediate messages after {regenerations} regeneration(s)"
                    );
                    return TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "repeated_final_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    };
                }
                report.sent_answer = true;
                if let Some(runs) = ctx.llm_runs {
                    runs.mark_round_sent(&run_id, crate::runtime_llm_runs::RunRoundSent::Final);
                }
                let sent_now = ctx.now
                    + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
                append_session_sent_marker(queue, ctx.item_id, sent_now).await;
                return TurnResolution {
                    outcome: TurnOutcome::Sent {
                        parts: sent.total_count,
                        side_effect_tickets: ticket_ids(&side_effect_tickets),
                    },
                    disposition: JobDisposition::Complete,
                };
            }
            let (duplicate_message_id, duplicate) =
                should_suppress_duplicate_bot_reply(&duplicate_guard_history, &sanitized);
            if duplicate {
                if regenerations < ctx.max_regenerations.max(0)
                    && budget.remaining(failure_now) >= MIN_REGENERATION_BUDGET
                {
                    regenerations += 1;
                    report.regenerations = regenerations;
                    anti_loop = true;
                    append_session_regeneration_event(
                        queue,
                        ctx.item_id,
                        regenerations,
                        duplicate_message_id,
                        failure_now,
                    )
                    .await;
                    continue;
                }
                report.suppressed_duplicate_message_id = Some(duplicate_message_id);
                let error = format!(
                    "dialog answer duplicated bot message {duplicate_message_id} after {regenerations} regeneration(s)"
                );
                if sent.any() {
                    return TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "duplicate_exhausted_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    };
                }
                return TurnResolution {
                    outcome: TurnOutcome::TerminalFailed {
                        reason: "duplicate_exhausted",
                        error: error.clone(),
                        user_signal: UserSignalPlan::React,
                    },
                    disposition: JobDisposition::Fail(error),
                };
            }

            if let Err(validation) = validate_dialog_answer_deliverable(&sanitized) {
                let error = format!("dialog answer rejected by outbound validation: {validation}");
                report.empty_answer_error = Some(error.clone());
                if sent.any() {
                    return TurnResolution {
                        outcome: TurnOutcome::TerminalFailed {
                            reason: "undeliverable_after_partial",
                            error: error.clone(),
                            user_signal: UserSignalPlan::React,
                        },
                        disposition: JobDisposition::Fail(error),
                    };
                }
                return handle_retryable_dialog_provider_error(
                    queue,
                    ctx.item,
                    &active_params,
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
                .await;
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
                    &active_params,
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

            return match send_result {
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
                    sent.record(&final_answer, false);
                    report.sent_answer = true;
                    if let Some(runs) = ctx.llm_runs {
                        runs.mark_round_sent(&run_id, crate::runtime_llm_runs::RunRoundSent::Final);
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
            };
        }

        // ---- Tool iteration: record the assistant step, deliver its visible
        // text once, execute every call in order, then let the typed batch
        // semantics decide whether tool results require another model step.
        transcript.push(SessionMessage::Assistant {
            text: step.text.clone(),
            tool_calls: step
                .tool_calls
                .iter()
                .map(|call| SessionToolCall {
                    id: call.id.clone(),
                    name: call.step.step.clone(),
                    arguments: serde_json::to_value(&call.step).unwrap_or(Value::Null),
                })
                .collect(),
        });
        let announcement = prepare_dialog_chat_response(&step.text);
        let step_text_accounted_for = if announcement.trim().is_empty() {
            false
        } else {
            let delivery = try_send_intermediate(
                &active_params,
                effects,
                queue,
                ctx.item_id,
                ctx.item.latest_update_id,
                &mut sent,
                cfg.max_messages,
                &announcement,
                failure_now,
            )
            .await;
            let delivered = delivery.status == openplotva_dialog::TOOL_RESULT_STATUS_OK;
            if delivered && let Some(runs) = ctx.llm_runs {
                runs.mark_round_sent(&run_id, crate::runtime_llm_runs::RunRoundSent::Intermediate);
            }
            delivered
                || delivery.error.as_ref().is_some_and(|error| {
                    error.code == "duplicate_message" && sent.matches_delivery(&announcement)
                })
        };

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
                && !parallel_media_results.contains_key(&call_index)
            {
                let batch_len =
                    consecutive_understand_media_call_count(&step.tool_calls, call_index);
                parallel_media_results.extend(
                    execute_parallel_understand_media_calls(
                        &step.tool_calls[call_index..call_index + batch_len],
                        call_index,
                        cfg,
                        &meta,
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
            let (result, tool_duration_ms, executed) = if let Some(prepared) =
                parallel_media_results.remove(&call_index)
            {
                (prepared.result, prepared.duration_ms, true)
            } else if let Some((original_call_id, cached)) = tool_result_cache.get(&semantic_key) {
                (
                    reused_tool_result(cached, original_call_id),
                    tool_started.elapsed().as_millis(),
                    false,
                )
            } else {
                let result = execute_session_tool(
                    SessionToolExecution {
                        call,
                        cfg,
                        meta: &meta,
                        params: &active_params,
                        causation_update_id: ctx.item.latest_update_id,
                        budget: &mut budget,
                        sent: &mut sent,
                        draws_scheduled: &mut draws_scheduled,
                        songs_scheduled: &mut songs_scheduled,
                        reacted_message_ids: &mut reacted_message_ids,
                        processing_started,
                        now: ctx.now,
                    },
                    effects,
                    queue,
                    ctx.item_id,
                )
                .await;
                (result, tool_started.elapsed().as_millis(), true)
            };
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
            if call.step.step == STEP_WEB_SEARCH
                && result
                    .status
                    .eq_ignore_ascii_case(openplotva_dialog::TOOL_RESULT_STATUS_OK)
            {
                successful_web_search = true;
                collect_web_source_urls(&result, &mut web_source_urls);
            }
            append_session_tool_event(
                queue,
                ctx.item_id,
                &call.step.step,
                &result.status,
                tool_duration_ms,
                ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default(),
            )
            .await;
            if let Some(runs) = ctx.llm_runs {
                runs.record_tool_result(
                    &run_id,
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
                        &run_id,
                        crate::runtime_llm_runs::RunRoundSent::Intermediate,
                    );
                }
            }
            recorded_tool_calls.push(recorded_session_tool_call(
                &call.step, &result, &call.id, iteration,
            ));
            batch_results.push(result.clone());
            transcript.push(SessionMessage::ToolResult {
                tool_call_id: call.id.clone(),
                name: call.step.step.clone(),
                content: serde_json::to_string(&result)
                    .unwrap_or_else(|_| "{\"status\":\"failed\"}".to_owned()),
            });
        }

        match persist_dialog_tool_calls(tool_history, &active_params, &recorded_tool_calls).await {
            Ok(persisted) => report.persisted_tool_call_history = persisted,
            Err(error) => {
                report.tool_call_history_error = Some(error.to_string());
            }
        }

        let disposition = if !announcement.trim().is_empty() && !step_text_accounted_for {
            SessionBatchDisposition::ContinueForResults
        } else {
            session_batch_disposition(&step.tool_calls, &batch_results, step_text_accounted_for)
        };
        let disposition_now =
            ctx.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();
        append_session_batch_event(
            queue,
            ctx.item_id,
            iteration,
            disposition,
            step_text_accounted_for,
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
                return session_delegated(&sent, &side_effect_tickets);
            }
            SessionBatchDisposition::CompleteAfterSidecars => {
                report.sent_answer = true;
                if let Some(runs) = ctx.llm_runs {
                    runs.mark_round_sent(&run_id, crate::runtime_llm_runs::RunRoundSent::Final);
                }
                append_session_sent_marker(queue, ctx.item_id, disposition_now).await;
                return TurnResolution {
                    outcome: TurnOutcome::Sent {
                        parts: sent.total_count,
                        side_effect_tickets: ticket_ids(&side_effect_tickets),
                    },
                    disposition: JobDisposition::Complete,
                };
            }
        }
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
    step_had_text: bool,
) -> SessionBatchDisposition {
    let mut only_sidecars = !calls.is_empty();
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
                only_sidecars = false;
                let Some(result) = results.get(index) else {
                    return SessionBatchDisposition::ContinueForResults;
                };
                if queued_generation_side_effect(result).is_some() {
                    queued_generation = true;
                } else {
                    return SessionBatchDisposition::ContinueForResults;
                }
            }
            ToolContinuation::ExplicitIntermediate => {
                only_sidecars = false;
            }
        }
    }

    if queued_generation {
        SessionBatchDisposition::CompleteWithSideEffect
    } else if only_sidecars && step_had_text {
        SessionBatchDisposition::CompleteAfterSidecars
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
    cfg: &SessionTurnConfig<'_>,
    round_now: OffsetDateTime,
) -> std::time::Duration {
    let slice = (budget.remaining(round_now) - TOOL_RESERVE)
        .min(TimeDuration::seconds(i64::from(
            cfg.tool_extension_secs.max(1),
        )))
        .max(TimeDuration::seconds(1));
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
    params: &'a openplotva_taskman::DialogJobParams,
    causation_update_id: Option<i64>,
    budget: &'a mut SessionBudget,
    sent: &'a mut SentLog,
    draws_scheduled: &'a mut i32,
    songs_scheduled: &'a mut i32,
    reacted_message_ids: &'a mut BTreeSet<i64>,
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
    match step.step.as_str() {
        STEP_SEND_MESSAGE => {
            let sanitized = prepare_dialog_chat_response(&step.text);
            if sanitized.trim().is_empty() {
                return ToolResult::failed("empty_text", "message text is empty after sanitizing");
            }
            let round_now = exec.now
                + TimeDuration::try_from(exec.processing_started.elapsed()).unwrap_or_default();
            try_send_intermediate(
                exec.params,
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
            let round_now = exec.now
                + TimeDuration::try_from(exec.processing_started.elapsed()).unwrap_or_default();
            let slice = session_tool_slice(exec.budget, exec.cfg, round_now);
            let result =
                dispatch_session_tool_with_timeout(exec.cfg.toolbox, exec.meta, step, slice).await;
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
    if i64::from(sent.intermediate_count) >= i64::from(max_messages.max(0)) {
        return ToolResult::failed(
            "message_limit",
            "per-turn message limit reached; write your final answer",
        );
    }
    if sent.matches_delivery(sanitized) {
        return ToolResult::failed("duplicate_message", "this text was already sent this turn");
    }
    if let Err(validation) = validate_dialog_answer_deliverable(sanitized) {
        return ToolResult::failed("undeliverable", validation.to_string());
    }
    let first_send = !sent.any();
    let seq = sent.intermediate_count + 1;
    match effects
        .send_dialog_intermediate(
            item_id,
            causation_update_id,
            params,
            sanitized,
            seq,
            first_send,
        )
        .await
    {
        Ok(()) => {
            sent.record(sanitized, true);
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
    at: OffsetDateTime,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let mut data = BTreeMap::new();
    data.insert("tool".to_owned(), name.to_owned());
    data.insert("status".to_owned(), status.to_owned());
    data.insert("duration_ms".to_owned(), duration_ms.to_string());
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

/// Drive the session loop for the admin console: the toolbox is an argument
/// (SAFE/REAL is the caller's choice per message, not a provider property),
/// send_message and text-next-to-tools are captured instead of dispatched,
/// and errors surface directly — no retries, durable markers, or durable ledger.
pub async fn run_captured_session(
    step_provider: &dyn ChatStepProvider,
    toolbox: &dyn DialogToolbox,
    base_input: DialogInput,
    max_iterations: i32,
) -> Result<CapturedSessionOutput, String> {
    let meta = dialog_tool_context(&base_input);
    let native_tools = session_native_tools().map_err(|error| error.to_string())?;
    let mut transcript: Vec<SessionMessage> = Vec::new();
    let mut messages: Vec<String> = Vec::new();
    let mut sent = SentLog::new();
    let mut recorded: Vec<ToolCall> = Vec::new();
    let mut provider = String::new();
    let mut web_source_urls = BTreeSet::new();
    let mut search_citation_repairs: i32 = 0;
    let max_iterations = max_iterations.max(1);

    for iteration in 1..=max_iterations {
        let force_final = iteration == max_iterations;
        let tools = if base_input.disable_tools {
            ToolsMode::Disabled
        } else if force_final || search_citation_repairs > 0 {
            ToolsMode::FinalOnly
        } else {
            ToolsMode::Native(native_tools.clone())
        };
        let mut input = base_input.clone();
        if search_citation_repairs > 0 {
            input
                .reference_context
                .push(SEARCH_CITATION_REPAIR_HINT.to_owned());
        }
        let step = step_provider
            .run_chat_step(ChatStepRequest {
                input,
                transcript: transcript.clone(),
                tools,
                iteration: usize::try_from(iteration).unwrap_or(1),
            })
            .await
            .map_err(|error| error.to_string())?;
        provider = step.provider.clone();

        if step.tool_calls.is_empty() || force_final || search_citation_repairs > 0 {
            let sanitized = prepare_dialog_chat_response(&step.text);
            if !web_source_urls.is_empty() && !answer_cites_web_source(&sanitized, &web_source_urls)
            {
                if search_citation_repairs < MAX_SEARCH_CITATION_REPAIRS
                    && iteration < max_iterations
                {
                    search_citation_repairs += 1;
                    continue;
                }
                tracing::warn!(
                    attempts = search_citation_repairs,
                    sources = web_source_urls.len(),
                    "sending captured searched answer without a source citation after repair attempts"
                );
            }
            if !sanitized.trim().is_empty() && !sent.matches_delivery(&sanitized) {
                sent.record(&sanitized, false);
                messages.push(sanitized);
            }
            return Ok(CapturedSessionOutput {
                messages,
                tool_calls: recorded,
                provider,
            });
        }

        transcript.push(SessionMessage::Assistant {
            text: step.text.clone(),
            tool_calls: step
                .tool_calls
                .iter()
                .map(|call| SessionToolCall {
                    id: call.id.clone(),
                    name: call.step.step.clone(),
                    arguments: serde_json::to_value(&call.step).unwrap_or(Value::Null),
                })
                .collect(),
        });
        let announcement = prepare_dialog_chat_response(&step.text);
        let step_text_accounted_for = if announcement.trim().is_empty() {
            false
        } else if sent.matches_delivery(&announcement) {
            true
        } else {
            sent.record(&announcement, true);
            messages.push(announcement);
            true
        };

        let mut batch_results = Vec::with_capacity(step.tool_calls.len());
        for call in &step.tool_calls {
            let step_def = &call.step;
            let result = match step_def.step.as_str() {
                STEP_SEND_MESSAGE => {
                    let sanitized = prepare_dialog_chat_response(&step_def.text);
                    if sanitized.trim().is_empty() {
                        ToolResult::failed("empty_text", "message text is empty after sanitizing")
                    } else if sent.matches_delivery(&sanitized) {
                        ToolResult::failed(
                            "duplicate_message",
                            "this text was already sent this turn",
                        )
                    } else {
                        sent.record(&sanitized, true);
                        messages.push(sanitized);
                        ToolResult {
                            status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
                            message: "message sent".to_owned(),
                            ..ToolResult::default()
                        }
                    }
                }
                STEP_REACT_TO_MESSAGE => ToolResult {
                    status: openplotva_dialog::TOOL_RESULT_STATUS_OK.to_owned(),
                    message: format!("reaction captured: {}", step_def.emoji),
                    ..ToolResult::default()
                },
                _ => match dispatch_dialog_tool(toolbox, &meta, step_def).await {
                    Ok(result) => result,
                    Err(error) => ToolResult::failed("tool_error", error.to_string()),
                },
            };
            if step_def.step == STEP_WEB_SEARCH
                && result
                    .status
                    .eq_ignore_ascii_case(openplotva_dialog::TOOL_RESULT_STATUS_OK)
            {
                collect_web_source_urls(&result, &mut web_source_urls);
            }
            recorded.push(recorded_session_tool_call(
                step_def, &result, &call.id, iteration,
            ));
            batch_results.push(result.clone());
            transcript.push(SessionMessage::ToolResult {
                tool_call_id: call.id.clone(),
                name: step_def.step.clone(),
                content: serde_json::to_string(&result)
                    .unwrap_or_else(|_| "{\"status\":\"failed\"}".to_owned()),
            });
        }
        if matches!(
            session_batch_disposition(&step.tool_calls, &batch_results, step_text_accounted_for),
            SessionBatchDisposition::CompleteWithSideEffect
                | SessionBatchDisposition::CompleteAfterSidecars
        ) {
            return Ok(CapturedSessionOutput {
                messages,
                tool_calls: recorded,
                provider,
            });
        }
    }
    Ok(CapturedSessionOutput {
        messages,
        tool_calls: recorded,
        provider,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use openplotva_dialog::{ToolboxFuture, VisionRequest};

    use super::*;

    #[test]
    fn sent_log_matches_html_equivalent_and_aggregate_replays() {
        let mut sent = SentLog::new();
        sent.record("Ну и <b>юмор</b>", true);

        assert!(sent.matches_delivery("<p>Ну и юмор</p>"));

        sent.record("Ещё реплика", true);
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
            session_batch_disposition(&[call(STEP_REACT_TO_MESSAGE)], &[ok()], true),
            SessionBatchDisposition::CompleteAfterSidecars
        );
        assert_eq!(
            session_batch_disposition(&[call(STEP_REACT_TO_MESSAGE)], &[ok()], false),
            SessionBatchDisposition::ContinueWithoutFinal
        );
        assert_eq!(
            session_batch_disposition(&[call(STEP_DRAW_IMAGE)], &[queued()], true),
            SessionBatchDisposition::CompleteWithSideEffect
        );
        assert_eq!(
            session_batch_disposition(
                &[call(STEP_DRAW_IMAGE)],
                &[ToolResult::failed("draw_failed", "draw failed")],
                true,
            ),
            SessionBatchDisposition::ContinueForResults
        );
        assert_eq!(
            session_batch_disposition(
                &[call(STEP_DRAW_IMAGE), call(STEP_WEB_SEARCH)],
                &[queued(), ok()],
                true,
            ),
            SessionBatchDisposition::ContinueForResults
        );
        assert_eq!(
            session_batch_disposition(&[call(STEP_SEND_MESSAGE)], &[ok()], false),
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
        let tools = session_native_tools().expect("native tool schemas");
        let names = tools
            .iter()
            .filter_map(|tool| tool.pointer("/function/name").and_then(Value::as_str))
            .collect::<Vec<_>>();

        assert!(!names.contains(&openplotva_dialog::STEP_MEMORY_SEARCH));
        assert!(names.contains(&STEP_SEND_MESSAGE));
        assert!(names.contains(&STEP_REACT_TO_MESSAGE));
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
