//! Worker loops, per-job processing entry points, and the router trigger
//! poller.

use std::{future::Future, sync::Arc, time::Duration};

use openplotva_llm::ChatProvider;
use openplotva_taskman::{
    DIALOG_AIFARM_QUEUE_NAME, LOWEST_PRIORITY, dialog_job_params_from_stateless_job,
};
use time::{Duration as TimeDuration, OffsetDateTime};

use crate::telegram_activity::{TelegramActivityAction, TelegramActivityPulse};

use super::{
    DialogInputMaterializer, DialogJobEffects, DialogJobWorkerQueue, DialogJobWorkerReport,
    DialogJobWorkerRunReport, DialogToolCallHistoryStore, trace_dialog_job_tick,
};

/// Shared loop controls for dialog taskman workers.
#[derive(Clone, Copy)]
pub struct DialogJobWorkerLoopOptions<'a, Materializer: ?Sized, ToolHistory: ?Sized> {
    pub materializer: &'a Materializer,
    pub tool_history: &'a ToolHistory,
    pub routing_events: Option<&'a crate::runtime_routing::RoutingEventReporter>,
    /// Reply-outcome ledger observer; every handled job records one outcome.
    pub turn_outcomes: Option<&'a crate::dialog_turn::DialogTurnObserver>,
    /// Queue names to poll in order.
    pub queue_names: &'static [&'static str],
    /// Poll interval.
    pub interval: Duration,
    pub max_llm_job_attempts: i32,
    /// Per-turn wall-clock budget in seconds, anchored at first processing start.
    pub turn_budget_secs: i32,
    /// Pending age in seconds beyond which never-processed jobs are dropped.
    pub turn_max_queue_age_secs: i32,
    /// In-process duplicate-answer regenerations per turn.
    pub max_regenerations: i32,
    /// Terminal-failure user signal wiring (reaction with text fallback).
    pub terminal_signal: crate::dialog_turn::TurnSignalPolicy<'a>,
    /// Delivery-obligation annotator: finalize backfills the dialog job id on
    /// obligations recorded by the schedulers (annotation only, never creates).
    pub obligations: Option<&'a dyn crate::dialog_turn::DeliveryObligationAnnotator>,
    /// Session-engine wiring: toolbox, reactor, registry, and session caps.
    pub session: &'a crate::dialog_turn::SessionWorkerWiring,
    /// Agent-run buffer for the admin LLM Dialogs section.
    pub llm_runs: Option<&'a crate::runtime_llm_runs::RuntimeLlmRunBuffer>,
    /// Best-effort Telegram activity indicator while the active turn runs.
    pub activity_pulse: Option<&'a TelegramActivityPulse>,
}

/// True when the current UTC time is inside the daily `[start, end)` minute window,
/// supporting windows that wrap past midnight (`start > end`).
fn in_utc_minute_window(start_minute: u32, end_minute: u32) -> bool {
    let now = OffsetDateTime::now_utc();
    let minute = u32::from(now.hour()) * 60 + u32::from(now.minute());
    if start_minute <= end_minute {
        minute >= start_minute && minute < end_minute
    } else {
        minute >= start_minute || minute < end_minute
    }
}

/// Evaluate every workflow's triggers once and publish their engagement state.
/// queue_depth applies hysteresis (engage at `high`, disengage below `low`);
/// error_rate compares the breaker's windowed ratio; time_of_day checks the UTC
/// window. Only the known dialog queue is polled for depth in this version.
async fn evaluate_router_triggers_once<Queue>(
    handle: &openplotva_llm::router::RouterHandle,
    triggers: &openplotva_llm::router::TriggerState,
    breakers: &openplotva_llm::router::BreakerSet,
    queue: &Queue,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
{
    let table = handle.snapshot();
    let keys: Vec<String> = table.workflow_keys().map(str::to_owned).collect();
    for key in keys {
        let Some(route) = table.resolve(&key, false) else {
            continue;
        };
        for spec in &route.triggers {
            let engaged = match &spec.condition {
                openplotva_llm::router::TriggerCondition::QueueDepth {
                    queue: queue_name,
                    high,
                    low,
                } => {
                    if queue_name != DIALOG_AIFARM_QUEUE_NAME {
                        continue;
                    }
                    let Ok(depth) = queue
                        .pending_dialog_job_depth(DIALOG_AIFARM_QUEUE_NAME, LOWEST_PRIORITY)
                        .await
                    else {
                        continue;
                    };
                    if triggers.is_engaged(spec.id) {
                        depth >= *low
                    } else {
                        depth >= *high
                    }
                }
                openplotva_llm::router::TriggerCondition::ErrorRate {
                    provider,
                    model,
                    threshold,
                    window,
                } => {
                    breakers.error_rate(*provider, *model, *window, std::time::Instant::now())
                        >= *threshold
                }
                openplotva_llm::router::TriggerCondition::TimeOfDay {
                    start_minute,
                    end_minute,
                } => in_utc_minute_window(*start_minute, *end_minute),
                openplotva_llm::router::TriggerCondition::ProviderCapacity {
                    provider,
                    model,
                    ..
                } => triggers.provider_capacity_unavailable(*provider, *model),
            };
            triggers.set_engaged(spec.id, engaged);
        }
    }
}

/// Background task that keeps the router's trigger-engagement state current,
/// generalizing the dialog-aifarm watermark gate to all DB-configured triggers.
pub async fn run_router_trigger_poller<Queue, Stop>(
    handle: Arc<openplotva_llm::router::RouterHandle>,
    triggers: Arc<openplotva_llm::router::TriggerState>,
    breakers: Arc<openplotva_llm::router::BreakerSet>,
    queue: Arc<Queue>,
    interval: Duration,
    stop: Stop,
) where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    let interval = if interval.is_zero() {
        Duration::from_secs(1)
    } else {
        interval
    };
    let mut stop = std::pin::pin!(stop);
    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                evaluate_router_triggers_once(&handle, &triggers, &breakers, queue.as_ref()).await;
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DialogJobProcessOptions<'a> {
    pub(crate) queue_name: &'static str,
    pub(crate) max_llm_job_attempts: i32,
    pub(crate) turn_budget_secs: i32,
    pub(crate) turn_max_queue_age_secs: i32,
    pub(crate) max_regenerations: i32,
    pub(crate) now: OffsetDateTime,
    pub(crate) routing_events: Option<&'a crate::runtime_routing::RoutingEventReporter>,
    pub(crate) turn_outcomes: Option<&'a crate::dialog_turn::DialogTurnObserver>,
    pub(crate) terminal_signal: crate::dialog_turn::TurnSignalPolicy<'a>,
    pub(crate) obligations: Option<&'a dyn crate::dialog_turn::DeliveryObligationAnnotator>,
    pub(crate) session: &'a crate::dialog_turn::SessionWorkerWiring,
    pub(crate) llm_runs: Option<&'a crate::runtime_llm_runs::RuntimeLlmRunBuffer>,
    pub(crate) activity_pulse: Option<&'a TelegramActivityPulse>,
}

pub(crate) async fn process_dialog_job_once_in_queue_with_materializer_history_and_retry_at<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    options: DialogJobProcessOptions<'_>,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    let mut report = DialogJobWorkerReport {
        queue_name: options.queue_name.to_owned(),
        ..DialogJobWorkerReport::default()
    };
    let item = match queue.dequeue_dialog_job(options.queue_name).await {
        Ok(item) => item,
        Err(error) => {
            report.dequeue_error = Some(error.to_string());
            return report;
        }
    };

    let Some(item) = item else {
        return report;
    };
    report.dequeued = true;
    report.job_id = Some(item.id);
    report.provider = Some(provider.provider_name().to_owned());

    let budget = crate::dialog_turn::TurnBudget::from_events(
        &item.events,
        options.turn_budget_secs,
        options.now,
    );

    let params = match dialog_job_params_from_stateless_job(&item.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            report.decode_error = Some(error.clone());
            let resolution = crate::dialog_turn::TurnResolution {
                outcome: crate::dialog_turn::TurnOutcome::SkippedDecodeError {
                    error: error.clone(),
                },
                disposition: crate::dialog_turn::JobDisposition::Fail(error),
            };
            crate::dialog_turn::finalize_turn(
                queue,
                &item,
                resolution,
                &budget,
                options.now,
                options.turn_outcomes,
                options.terminal_signal,
                options.obligations,
                &mut report,
            )
            .await;
            return report;
        }
    };
    report.chat_id = (params.chat_id != 0).then_some(params.chat_id);
    report.thread_id = params.thread_id;
    report.user_id = (params.user_id != 0).then_some(params.user_id);
    report.message_id = (params.message_id != 0).then_some(params.message_id);

    // Per-(chat, thread) serialization: while a session runs, an initiator
    // message merges into it and a third party's turn parks behind it; both
    // complete here with their own ledger rows (single-exit holds). A release
    // race (claim finds Busy but inject/park misses) retries and, as bug
    // containment, degrades to today's parallel turn.
    let session_registry = &options.session.registry;
    let session_key = crate::dialog_turn::SessionKey::new(params.chat_id, params.thread_id);
    let mut session_claimed = false;
    let mut session_inbox: Option<std::sync::Arc<crate::dialog_turn::SessionInbox>> = None;
    {
        let registry = session_registry.as_ref();
        for _containment in 0..3 {
            match registry.claim(session_key, item.id, params.user_id) {
                crate::dialog_turn::ClaimOutcome::Claimed(inbox) => {
                    session_claimed = true;
                    session_inbox = Some(inbox);
                    break;
                }
                // The same job id re-delivered while its original run is
                // still registered: proceed without self-injecting; the
                // engine's sent-marker re-entry guard resolves it.
                crate::dialog_turn::ClaimOutcome::AlreadyOwned => break,
                crate::dialog_turn::ClaimOutcome::Busy {
                    session_job_id,
                    initiator_user_id,
                } => {
                    let absorbed = if params.user_id == initiator_user_id {
                        registry.inject(
                            session_key,
                            session_job_id,
                            crate::dialog_turn::InjectedMessage {
                                params: params.clone(),
                            },
                        )
                    } else {
                        registry.park(
                            session_key,
                            session_job_id,
                            crate::dialog_turn::ParkedJob {
                                queue_name: options.queue_name.to_owned(),
                                job: item.job.clone(),
                            },
                        )
                    };
                    if absorbed {
                        let outcome = if params.user_id == initiator_user_id {
                            report.merged_into_session = Some(session_job_id);
                            crate::dialog_turn::TurnOutcome::MergedIntoSession { session_job_id }
                        } else {
                            report.deferred_after_session = Some(session_job_id);
                            crate::dialog_turn::TurnOutcome::DeferredAfterSession { session_job_id }
                        };
                        let resolution = crate::dialog_turn::TurnResolution {
                            outcome,
                            disposition: crate::dialog_turn::JobDisposition::Complete,
                        };
                        crate::dialog_turn::finalize_turn(
                            queue,
                            &item,
                            resolution,
                            &budget,
                            options.now,
                            options.turn_outcomes,
                            options.terminal_signal,
                            options.obligations,
                            &mut report,
                        )
                        .await;
                        return report;
                    }
                    // Release race: the session just ended; retry the claim.
                }
            }
        }
    }

    // Generation is the only long-running step of a tick: fold its real wall
    // time into finalize's `now` so the ledger's elapsed_ms reflects the true
    // turn duration (terminal failures no longer record elapsed_ms=0).
    let processing_started = tokio::time::Instant::now();
    let run_id = format!("job-{}", item.id);
    if let Some(runs) = options.llm_runs {
        runs.begin_run(
            run_id.clone(),
            "dialog",
            crate::runtime_llm_runs::RunOrigin {
                chat_id: params.chat_id,
                thread_id: params.thread_id,
                chat_title: None,
                user_id: params.user_id,
                user_full_name: (!params.user_full_name.trim().is_empty())
                    .then(|| params.user_full_name.clone()),
                trigger_message_id: params.message_id,
                trigger_preview: (!params.message_text.trim().is_empty())
                    .then(|| params.message_text.chars().take(120).collect::<String>()),
                queue_name: Some(options.queue_name.to_owned()),
                job_id: Some(item.id),
            },
            options.now,
        );
    }
    let activity_guard = options.activity_pulse.map(|pulse| {
        pulse.start(
            params.chat_id,
            params.thread_id,
            TelegramActivityAction::Typing,
        )
    });
    let turn = crate::dialog_turn::execute_dialog_turn(
        crate::dialog_turn::TurnContext {
            item: &item,
            params: &params,
            queue_name: options.queue_name,
            max_llm_job_attempts: options.max_llm_job_attempts,
            max_queue_age: TimeDuration::seconds(i64::from(options.turn_max_queue_age_secs.max(1))),
            max_regenerations: options.max_regenerations,
            budget,
            now: options.now,
            routing_events: options.routing_events,
            session: options.session.turn_config(),
            session_inbox,
            llm_runs: options.llm_runs,
        },
        queue,
        provider,
        effects,
        materializer,
        tool_history,
        &mut report,
    );
    let resolution = openplotva_llm::with_run_scope(
        openplotva_llm::LlmRunScope {
            run_id,
            run_kind: "dialog".to_owned(),
        },
        turn,
    )
    .await;
    if let Some(guard) = activity_guard.as_ref() {
        report.activity = guard.snapshot();
    }
    drop(activity_guard);
    let finalize_now =
        options.now + TimeDuration::try_from(processing_started.elapsed()).unwrap_or_default();

    crate::dialog_turn::finalize_turn(
        queue,
        &item,
        resolution,
        &budget,
        finalize_now,
        options.turn_outcomes,
        options.terminal_signal,
        options.obligations,
        &mut report,
    )
    .await;

    // Release the session key AFTER finalize so a Busy observer can never
    // start a parallel turn while this one still owns the outcome. Everything
    // the session left behind gets its own turn now.
    if session_claimed {
        let registry = session_registry.as_ref();
        let release = registry.release(session_key, item.id);
        if let Some(newest) = release.leftover_injected.last() {
            // Older leftovers are already persisted chat history and will
            // materialize as context for this follow-up.
            let job = openplotva_taskman::new_dialog_job_at(newest.params.clone(), finalize_now);
            match queue.respawn_dialog_job(options.queue_name, job).await {
                Ok(follow_up_id) => report.followup_respawned = Some(follow_up_id),
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        chat_id = params.chat_id,
                        "failed to respawn the follow-up turn for leftover injected messages"
                    );
                }
            }
        }
        for parked in release.parked {
            let crate::dialog_turn::ParkedJob { queue_name, job } = parked;
            if let Err(error) = queue.respawn_dialog_job(&queue_name, job).await {
                tracing::warn!(
                    error = %error,
                    chat_id = params.chat_id,
                    "failed to respawn a deferred third-party turn"
                );
            }
        }
    }
    report
}

/// Run dialog taskman workers with custom input materialization and tool-call history storage.
pub async fn run_dialog_job_worker_with_materializer_and_history_every_until<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
    Stop,
>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    options: DialogJobWorkerLoopOptions<'_, Materializer, ToolHistory>,
    stop: Stop,
) -> DialogJobWorkerRunReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
    Stop: Future<Output = ()>,
{
    let mut report = DialogJobWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(options.interval) => {
                for queue_name in options.queue_names {
                    let tick = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
                        queue,
                        provider,
                        effects,
                        options.materializer,
                        options.tool_history,
                        DialogJobProcessOptions {
                            queue_name,
                            max_llm_job_attempts: options.max_llm_job_attempts,
                            turn_budget_secs: options.turn_budget_secs,
                            turn_max_queue_age_secs: options.turn_max_queue_age_secs,
                            max_regenerations: options.max_regenerations,
                            now: OffsetDateTime::now_utc(),
                            routing_events: options.routing_events,
                            turn_outcomes: options.turn_outcomes,
                            terminal_signal: options.terminal_signal,
                            obligations: options.obligations,
                            session: options.session,
                            llm_runs: options.llm_runs,
                            activity_pulse: options.activity_pulse,
                        },
                    ).await;
                    trace_dialog_job_tick(&tick);
                    report.record_tick(&tick);
                }
            }
        }
    }

    report
}
