use std::{
    collections::VecDeque,
    error::Error,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

use openplotva_core::{ChatAttachment, ChatMessageMeta, ChatSettings, ToolCall};
use openplotva_dialog::{DialogOutput, HistoryMessage, MESSAGE_KIND_TEXT, ROLE_MODEL, ROLE_USER};
use openplotva_llm::{
    ChatProvider, ChatProviderError, ContentBlockedError,
    retry::{FailureReason, ProviderError},
};
use openplotva_taskman::{
    DEFAULT_LLM_JOB_MAX_ATTEMPTS, DIALOG_AIFARM_QUEUE_NAME, DialogJobParams, HIGH_PRIORITY,
    InMemoryTaskQueue, JobStatus, LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE, Priority,
    TEXT_QUEUE_NAME, TaskQueueError, TaskQueueJobEvent, new_dialog_job_at,
};
use openplotva_telegram::DispatcherQueue;
use time::{Duration as TimeDuration, OffsetDateTime};

use super::*;

#[derive(Debug)]
struct RecordingActivityEffects {
    report: crate::telegram_activity::TelegramActivityReport,
    calls: Mutex<
        Vec<(
            i64,
            Option<i32>,
            crate::telegram_activity::TelegramActivityAction,
        )>,
    >,
}

impl RecordingActivityEffects {
    fn new(report: crate::telegram_activity::TelegramActivityReport) -> Self {
        Self {
            report,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(
        &self,
    ) -> Vec<(
        i64,
        Option<i32>,
        crate::telegram_activity::TelegramActivityAction,
    )> {
        self.calls.lock().expect("activity calls").clone()
    }
}

impl crate::telegram_activity::TelegramActivityEffects for RecordingActivityEffects {
    fn send_activity_action<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
        action: crate::telegram_activity::TelegramActivityAction,
    ) -> crate::telegram_activity::TelegramActivityFuture<'a> {
        Box::pin(async move {
            self.calls
                .lock()
                .expect("activity calls")
                .push((chat_id, thread_id, action));
            self.report.clone()
        })
    }
}

fn activity_pulse(
    effects: Arc<RecordingActivityEffects>,
) -> crate::telegram_activity::TelegramActivityPulse {
    crate::telegram_activity::TelegramActivityPulse::new(
        crate::telegram_activity::TelegramActivityPulseSettings::from_millis(true, 1_000, 0),
        effects,
    )
}

#[tokio::test]
async fn dialog_worker_runs_provider_sends_answer_and_completes_job() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        TEXT_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now).with_priority(HIGH_PRIORITY),
    );
    let provider = ProviderStub::returning(DialogOutput {
        provider: "stub".to_owned(),
        answer: "  pong  ".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report =
        process_dialog_job_once_in_queue_at(&queue, TEXT_QUEUE_NAME, &provider, &effects, now)
            .await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(report.sent_answer);
    assert!(report.completed);
    assert!(!report.failed);
    assert_eq!(
        effects.sent(),
        vec![("hello".to_owned(), "pong".to_owned())]
    );
    assert_eq!(
        effects.answer_options(),
        vec![DialogAnswerSendOptions::default()],
        "ordinary answers must preserve Telegram's default preview behavior"
    );
    let input = provider.inputs().pop().expect("provider input");
    assert_eq!(input.context.chat_id, 42);
    assert_eq!(input.context.thread_id, Some(9));
    assert_eq!(input.message.text, "hello");
    assert_eq!(input.message.normalized, "hello");
    assert_eq!(input.max_output_tokens, 512);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let record = queue.record(job_id).expect("job record");
    assert!(
        record
            .events
            .iter()
            .any(|event| event.stage == crate::dialog_turn::SESSION_MESSAGE_SENT_STAGE),
        "successful send must append the session sent marker"
    );
    Ok(())
}

#[tokio::test]
async fn existing_unresolved_outbox_batch_skips_provider_and_waits_for_delivery()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        TEXT_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now).with_priority(HIGH_PRIORITY),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "must not run".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::with_queued_answer(QueuedBatchReceipt::durable_outbox(
        format!("dialog-answer:v1:99:{job_id}"),
        vec!["tgop:v1:one".to_owned(), "tgop:v1:two".to_owned()],
        false,
    ));

    let report =
        process_dialog_job_once_in_queue_at(&queue, TEXT_QUEUE_NAME, &provider, &effects, now)
            .await;

    assert!(
        provider.inputs().is_empty(),
        "preflight must run before LLM"
    );
    assert!(effects.sent().is_empty());
    assert!(report.queued_answer);
    assert!(report.waiting_delivery);
    assert!(!report.sent_answer);
    assert!(!report.completed);
    assert_eq!(record_status(&queue, job_id), JobStatus::WaitingDelivery);
    Ok(())
}

#[tokio::test]
async fn existing_delivered_outbox_batch_skips_provider_and_completes_job()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        TEXT_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now).with_priority(HIGH_PRIORITY),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "must not run".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::with_queued_answer(QueuedBatchReceipt::durable_outbox(
        format!("dialog-answer:v1:99:{job_id}"),
        vec!["tgop:v1:one".to_owned()],
        true,
    ));

    let report =
        process_dialog_job_once_in_queue_at(&queue, TEXT_QUEUE_NAME, &provider, &effects, now)
            .await;

    assert!(
        provider.inputs().is_empty(),
        "preflight must run before LLM"
    );
    assert!(effects.sent().is_empty());
    assert!(report.sent_answer);
    assert!(report.completed);
    assert!(!report.waiting_delivery);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dialog_worker_pulses_typing_during_active_turn() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("долго подумай"), now),
    );
    let provider = SlowProviderStub::new(
        DialogOutput {
            answer: "готово".to_owned(),
            ..DialogOutput::default()
        },
        Duration::from_millis(2_500),
    );
    let effects = EffectsStub::default();
    let activity_effects = Arc::new(RecordingActivityEffects::new(
        crate::telegram_activity::TelegramActivityReport::Sent,
    ));
    let pulse = activity_pulse(Arc::clone(&activity_effects));
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            activity_pulse: Some(&pulse),
            ..session_options(now, &outcomes, leaked_session_wiring())
        },
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    assert!(
        report.activity.sent >= 2,
        "long active turns must refresh typing, got {report:?}"
    );
    assert!(
        activity_effects.calls().iter().all(|call| {
            *call
                == (
                    42,
                    Some(9),
                    crate::telegram_activity::TelegramActivityAction::Typing,
                )
        }),
        "typing pulse must target the dialog chat/thread"
    );
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dialog_worker_ignores_activity_error() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("ответь даже если typing упал"), now),
    );
    let provider = SlowProviderStub::new(
        DialogOutput {
            answer: "ответ".to_owned(),
            ..DialogOutput::default()
        },
        Duration::from_millis(1_000),
    );
    let effects = EffectsStub::default();
    let activity_effects = Arc::new(RecordingActivityEffects::new(
        crate::telegram_activity::TelegramActivityReport::SendFailed {
            message: "telegram unavailable".to_owned(),
        },
    ));
    let pulse = activity_pulse(Arc::clone(&activity_effects));
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            activity_pulse: Some(&pulse),
            ..session_options(now, &outcomes, leaked_session_wiring())
        },
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    assert!(
        report.activity.failed >= 1,
        "activity failures must be recorded without failing the dialog"
    );
    assert_eq!(effects.sent()[0].1, "ответ");
    Ok(())
}

#[tokio::test]
async fn dialog_worker_skips_resend_when_answer_sent_marker_present() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now),
    );
    // Simulate a crash between the sent answer and the status write: the
    // marker survives on the job record while the job returns to the queue.
    queue.append_job_event(
        job_id,
        TaskQueueJobEvent {
            stage: crate::dialog_turn::ANSWER_SENT_STAGE.to_owned(),
            ..TaskQueueJobEvent::default()
        },
        now - TimeDuration::seconds(1),
    )?;
    let provider = ProviderStub::returning(DialogOutput {
        answer: "pong".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.sent_answer);
    assert!(report.resent_skipped);
    assert!(report.completed);
    assert!(
        effects.sent().is_empty(),
        "the answer_sent marker must suppress the resend"
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "sent");
    assert_eq!(rows[0].detail["resent_skipped"], serde_json::json!(true));
    Ok(())
}

#[tokio::test]
async fn dialog_dispatcher_effects_allow_sending_without_deleted_reply_target()
-> Result<(), Box<dyn Error>> {
    let queue = Arc::new(DispatcherQueue::new(
        openplotva_telegram::DispatcherConfig::default(),
    ));
    let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
        .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));

    effects
        .send_dialog_answer(
            1,
            None,
            &dialog_params("hello"),
            "pong",
            DialogAnswerSendOptions::default(),
        )
        .await?;

    let item = queue.dequeue_immediate().expect("queued dialog answer");
    assert_eq!(
        item.method_kind(),
        Some(openplotva_telegram::TelegramOutboundMethodKind::SendMessage)
    );
    assert!(
        item.metadata().protected,
        "dialog answers must survive queue trims"
    );
    assert!(
        item.metadata().fingerprint_key.ends_with(":r100"),
        "debounce key is reply-scoped, got {}",
        item.metadata().fingerprint_key
    );
    let (_, method) = item.into_parts();
    let Some(openplotva_telegram::TelegramOutboundMethod::SendMessage(method)) = method else {
        return Err("expected sendMessage".into());
    };
    let value = serde_json::to_value(method.as_ref())?;
    assert_eq!(value["chat_id"], 42);
    assert_eq!(value["text"], "pong");
    assert_eq!(value["reply_parameters"]["message_id"], 100);
    assert_eq!(
        value["reply_parameters"]["allow_sending_without_reply"],
        true
    );
    assert!(value.get("link_preview_options").is_none());
    Ok(())
}

#[tokio::test]
async fn dialog_dispatcher_effects_disable_preview_for_search_grounded_final_text()
-> Result<(), Box<dyn Error>> {
    let queue = Arc::new(DispatcherQueue::new(
        openplotva_telegram::DispatcherConfig::default(),
    ));
    let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
        .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));

    effects
        .send_dialog_answer(
            1,
            None,
            &dialog_params("latest"),
            "<a href=\"https://example.com/current\">current fact</a>",
            DialogAnswerSendOptions {
                disable_link_preview: true,
            },
        )
        .await?;

    let item = queue.dequeue_immediate().expect("queued dialog answer");
    let (_, method) = item.into_parts();
    let Some(openplotva_telegram::TelegramOutboundMethod::SendMessage(method)) = method else {
        return Err("expected sendMessage".into());
    };
    let value = serde_json::to_value(method.as_ref())?;
    assert_eq!(
        value["link_preview_options"]["is_disabled"],
        serde_json::json!(true)
    );
    Ok(())
}

#[tokio::test]
async fn dialog_dispatcher_effects_routes_rich_only_html_to_rich_queue()
-> Result<(), Box<dyn Error>> {
    let queue = Arc::new(DispatcherQueue::new(
        openplotva_telegram::DispatcherConfig::default(),
    ));
    let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
        .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));

    effects
        .send_dialog_answer(
            1,
            None,
            &dialog_params("hello"),
            "<h2>pong</h2>",
            DialogAnswerSendOptions {
                disable_link_preview: true,
            },
        )
        .await?;

    let item = queue.dequeue_immediate().expect("queued dialog answer");
    assert_eq!(
        item.method_kind(),
        Some(openplotva_telegram::TelegramOutboundMethodKind::SendRichMessage)
    );
    assert!(
        item.metadata().protected,
        "rich dialog answers must survive queue trims"
    );
    assert!(
        item.metadata().fingerprint_key.ends_with(":r100"),
        "debounce key is reply-scoped, got {}",
        item.metadata().fingerprint_key
    );
    Ok(())
}

#[tokio::test]
async fn dialog_worker_persists_executed_tool_calls_before_answer() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("когда солнцестояние?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "солнцестояние".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("21 июня")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let history = ToolHistoryStub::default();

    let report = process_dialog_job_once_in_queue_with_materializer_and_history_at_with_session(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &history,
        &wiring,
        now,
    )
    .await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(report.persisted_tool_call_history);
    let calls = history.calls();
    assert!(!calls.is_empty());
    assert_eq!(calls[0].0, 42);
    assert_eq!(calls[0].2[0].name, openplotva_dialog::STEP_WEB_SEARCH);
    assert!(report.sent_answer);
    assert!(report.completed);
    assert!(!report.failed);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn dialog_session_persists_cumulative_tool_history_across_iterations()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("сравни два источника"), now),
    );
    let search = |id: &str, query: &str| {
        step_tools(
            "",
            vec![(
                id,
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: query.to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )
    };
    let provider = StepProviderStub::with_steps(vec![
        Ok(search("call-1", "первый")),
        Ok(search("call-2", "второй")),
        Ok(step_text("готово")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let history = ToolHistoryStub::default();

    let report = process_dialog_job_once_in_queue_with_materializer_and_history_at_with_session(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &history,
        &wiring,
        now,
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    let snapshots = history.calls();
    assert_eq!(snapshots.len(), 2);
    assert_eq!(snapshots[0].2.len(), 1);
    assert_eq!(snapshots[1].2.len(), 2);
    assert_eq!(snapshots[1].2[0].r#ref, "call-1");
    assert_eq!(snapshots[1].2[1].r#ref, "call-2");
    Ok(())
}

#[tokio::test]
async fn dialog_worker_completes_delegated_queued_song_without_sending_text()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("спой про кота"), now),
    );
    let provider = StepProviderStub::with_steps(vec![Ok(step_tools(
        "",
        vec![(
            "call-1",
            openplotva_dialog::ToolStep {
                step: openplotva_dialog::STEP_GENERATE_SONG.to_owned(),
                topic: "песня про кота".to_owned(),
                ..openplotva_dialog::ToolStep::default()
            },
        )],
    ))]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::with_queued_song("9001"));
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let history = ToolHistoryStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &history,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            session: &wiring,
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(report.persisted_tool_call_history);
    assert!(
        !report.sent_answer,
        "queued schedule is silent: the artifact is the reply"
    );
    assert!(report.completed);
    assert!(!report.failed);
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "side_effect_delegated");
    assert_eq!(rows[0].side_effect_ticket_id, Some(9001));
    Ok(())
}

#[tokio::test]
async fn dialog_worker_keeps_tool_call_history_failures_nonfatal() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("когда солнцестояние?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "солнцестояние".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("21 июня")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let history = ToolHistoryStub::failing();

    let report = process_dialog_job_once_in_queue_with_materializer_and_history_at_with_session(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &history,
        &wiring,
        now,
    )
    .await;

    assert_eq!(
        report.tool_call_history_error,
        Some("history down".to_owned())
    );
    assert!(report.sent_answer);
    assert!(report.completed);
    assert!(!report.failed);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

fn duplicate_guard_history_fixture() -> Vec<HistoryMessage> {
    vec![
        HistoryMessage {
            message_id: 30,
            role: ROLE_USER.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "latest user message".to_owned(),
            ..HistoryMessage::default()
        },
        HistoryMessage {
            message_id: 29,
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "fallback".to_owned(),
            original_text: "<b>Привет, мир &amp; день!</b>".to_owned(),
            ..HistoryMessage::default()
        },
        HistoryMessage {
            message_id: 28,
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "older bot reply".to_owned(),
            ..HistoryMessage::default()
        },
    ]
}

fn duplicate_answer_output() -> DialogOutput {
    DialogOutput {
        answer: "  <i>Привет,   мир &amp; день!</i>  ".to_owned(),
        ..DialogOutput::default()
    }
}

#[tokio::test]
async fn dialog_worker_regenerates_duplicate_answer_with_anti_loop_hint()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что нового"), now),
    );
    let provider = SequenceProviderStub::returning(vec![
        duplicate_answer_output(),
        DialogOutput {
            answer: "Свежий ответ".to_owned(),
            ..DialogOutput::default()
        },
    ]);
    let effects = EffectsStub::default();
    let materializer = MaterializerStub::with_history(duplicate_guard_history_fixture());
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &materializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert_eq!(report.job_id, Some(job_id));
    assert_eq!(report.regenerations, 1);
    assert_eq!(report.suppressed_duplicate_message_id, None);
    assert!(report.sent_answer);
    assert!(report.completed);
    assert!(!report.failed);
    assert_eq!(
        effects.sent(),
        vec![("что нового".to_owned(), "Свежий ответ".to_owned())]
    );
    let inputs = provider.inputs();
    assert_eq!(inputs.len(), 2);
    assert!(inputs[0].reference_context.is_empty());
    assert_eq!(
        inputs[1].reference_context,
        vec![openplotva_dialog::turn::ANTI_LOOP_HINT.to_owned()]
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let record = queue.record(job_id).expect("job record");
    let regen_events: Vec<_> = record
        .events
        .iter()
        .filter(|event| event.stage == crate::dialog_turn::DIALOG_TURN_REGENERATE_STAGE)
        .collect();
    assert_eq!(regen_events.len(), 1);
    assert_eq!(regen_events[0].attempt, 1);
    assert_eq!(regen_events[0].data["reason"], "dedup_regenerate");
    assert_eq!(regen_events[0].data["duplicate_message_id"], "29");
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "sent");
    assert_eq!(rows[0].detail["regenerations"], 1);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_exhausts_regenerations_on_permanent_duplicate_and_signals()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что нового"), now),
    );
    let provider = SequenceProviderStub::returning(vec![duplicate_answer_output()]);
    let effects = EffectsStub::default();
    let materializer = MaterializerStub::with_history(duplicate_guard_history_fixture());
    let signal = FakeSignal::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &materializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(Some(&signal), "🤔", 600),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert_eq!(report.regenerations, 2);
    assert_eq!(report.suppressed_duplicate_message_id, Some(29));
    assert!(report.failed);
    assert!(!report.completed);
    assert!(!report.sent_answer);
    assert!(effects.sent().is_empty());
    assert_eq!(
        provider.inputs().len(),
        3,
        "initial round + 2 regenerations"
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    let calls = signal.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].emoji, "🤔");
    assert!(calls[0].text_fallback_allowed);
    let record = queue.record(job_id).expect("job record");
    let regen_events: Vec<_> = record
        .events
        .iter()
        .filter(|event| event.stage == crate::dialog_turn::DIALOG_TURN_REGENERATE_STAGE)
        .collect();
    assert_eq!(regen_events.len(), 2);
    assert!(
        record
            .events
            .iter()
            .any(|event| event.stage == crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
    );
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "terminal_failed");
    assert_eq!(rows[0].reason.as_deref(), Some("duplicate_exhausted"));
    assert_eq!(rows[0].user_signal.as_deref(), Some("reaction_sent"));
    assert_eq!(rows[0].detail["regenerations"], 2);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn dialog_worker_skips_regeneration_when_budget_nearly_exhausted()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что нового"), now),
    );
    // Generation eats all but ~9s of the 120s budget, below the 10s
    // regeneration floor: the duplicate goes terminal without a retry.
    let provider = SlowProviderStub::new(duplicate_answer_output(), Duration::from_secs(111));
    let effects = EffectsStub::default();
    let materializer = MaterializerStub::with_history(duplicate_guard_history_fixture());
    let signal = FakeSignal::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &materializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(Some(&signal), "🤔", 600),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert_eq!(report.regenerations, 0);
    assert!(report.failed);
    assert_eq!(provider.call_count(), 1);
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    assert_eq!(signal.calls().len(), 1);
    let record = queue.record(job_id).expect("job record");
    assert!(
        record
            .events
            .iter()
            .all(|event| event.stage != crate::dialog_turn::DIALOG_TURN_REGENERATE_STAGE)
    );
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].reason.as_deref(), Some("duplicate_exhausted"));
    assert_eq!(rows[0].user_signal.as_deref(), Some("reaction_sent"));
    // Finalize folds real generation wall time into `now`: the terminal row
    // records the 111s generation instead of elapsed 0.
    assert_eq!(rows[0].elapsed_ms, Some(111_000));
    Ok(())
}

#[tokio::test]
async fn dialog_worker_signals_terminal_failure_with_configured_emoji() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now),
    );
    let provider = ProviderStub::failing(io::Error::other("provider down"));
    let effects = EffectsStub::default();
    let signal = FakeSignal::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(Some(&signal), "🤔", 600),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.failed);
    let calls = signal.calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].chat_id, 42);
    assert_eq!(calls[0].thread_id, Some(9));
    assert_eq!(calls[0].message_id, 100);
    assert_eq!(calls[0].emoji, "🤔");
    assert!(calls[0].text_fallback_allowed);
    let record = queue.record(job_id).expect("job record");
    let marker = record
        .events
        .iter()
        .find(|event| event.stage == crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
        .expect("terminal user signal marker");
    assert_eq!(marker.data["result"], "reaction_sent");
    assert_eq!(marker.data["emoji"], "🤔");
    let outcome_event = record
        .events
        .iter()
        .find(|event| event.stage == crate::dialog_turn::TURN_OUTCOME_STAGE)
        .expect("turn outcome event");
    assert_eq!(outcome_event.data["user_signal"], "reaction_sent");
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "terminal_failed");
    assert_eq!(rows[0].user_signal.as_deref(), Some("reaction_sent"));
    Ok(())
}

#[tokio::test]
async fn dialog_worker_skips_signal_for_job_older_than_max_signal_age() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    // Old enough to be past the signal age gate, with a recent processing
    // anchor so the turn itself still runs (post-downtime backlog shape).
    let created = now - TimeDuration::seconds(700);
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), created),
    );
    queue.append_job_event(
        job_id,
        TaskQueueJobEvent {
            stage: crate::dialog_turn::TURN_STARTED_STAGE.to_owned(),
            ..TaskQueueJobEvent::default()
        },
        now - TimeDuration::seconds(30),
    )?;
    let provider = ProviderStub::failing(io::Error::other("provider down"));
    let effects = EffectsStub::default();
    let signal = FakeSignal::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(Some(&signal), "🤔", 600),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.failed);
    assert!(signal.calls().is_empty(), "no reaction on stale backlog");
    let record = queue.record(job_id).expect("job record");
    assert!(
        record
            .events
            .iter()
            .all(|event| event.stage != crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
    );
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].user_signal.as_deref(), Some("skipped_too_old"));
    Ok(())
}

#[tokio::test]
async fn dialog_worker_still_reacts_but_gates_text_fallback_after_prior_signal()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now),
    );
    // A previous run delivered the signal before its status write failed.
    queue.append_job_event(
        job_id,
        TaskQueueJobEvent {
            stage: crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE.to_owned(),
            ..TaskQueueJobEvent::default()
        },
        now - TimeDuration::seconds(5),
    )?;
    let provider = ProviderStub::failing(io::Error::other("provider down"));
    let effects = EffectsStub::default();
    let signal = FakeSignal::default();

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::new(Some(&signal), "🤔", 600),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: None,
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.failed);
    let calls = signal.calls();
    assert_eq!(calls.len(), 1, "reaction is re-attempted (idempotent)");
    assert!(
        !calls[0].text_fallback_allowed,
        "marker gates the non-idempotent text fallback"
    );
    let record = queue.record(job_id).expect("job record");
    let markers = record
        .events
        .iter()
        .filter(|event| event.stage == crate::dialog_turn::TERMINAL_USER_SIGNAL_STAGE)
        .count();
    assert_eq!(markers, 1, "marker is not duplicated");
    Ok(())
}

#[tokio::test]
async fn dialog_worker_fails_when_answer_sanitizes_to_empty_after_attempts_exhausted()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("найди сообщения"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "<tool_calls><tool_call name=\"history_search\"></tool_call></tool_calls>"
            .to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );
    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: 1,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.failed);
    assert!(report.retry_exhausted);
    assert!(!report.completed);
    assert!(!report.sent_answer);
    assert!(effects.sent().is_empty());
    assert_eq!(
        report.empty_answer_error,
        Some("dialog answer became empty after sanitization".to_owned())
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "terminal_failed");
    assert_eq!(rows[0].reason.as_deref(), Some("sanitized_empty_exhausted"));
    assert_eq!(rows[0].user_signal.as_deref(), Some("none"));
    Ok(())
}

#[tokio::test]
async fn dialog_worker_requeues_when_provider_answer_sanitizes_to_empty()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("найди сообщения"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "<tool_calls><tool_call name=\"history_search\"></tool_call></tool_calls>"
            .to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

    assert!(report.retry_requeued);
    assert!(!report.failed);
    assert!(!report.sent_answer);
    assert!(effects.sent().is_empty());
    assert_eq!(report.retry_attempt, Some(1));
    assert_eq!(record_status(&queue, job_id), JobStatus::Pending);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_skips_empty_payload_without_provider_call() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(empty_params(), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "unused".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(report.skipped_empty_payload);
    assert!(report.completed);
    assert!(provider.inputs().is_empty());
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_fails_expired_queue_backlog_job_without_provider_call()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let created = now - TimeDuration::seconds(11 * 60);
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("old hello"), created),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "unused".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(report.skipped_queue_backlog);
    assert!(report.failed);
    assert!(!report.completed);
    assert!(provider.inputs().is_empty());
    assert!(effects.sent().is_empty());
    let record = queue.record(job_id).expect("job record");
    assert_eq!(record.status, JobStatus::Failed);
    // The turn never started: no anchor event, only the outcome event.
    assert!(
        record
            .events
            .iter()
            .all(|event| event.stage != crate::dialog_turn::TURN_STARTED_STAGE)
    );
    let outcome_event = record
        .events
        .iter()
        .find(|event| event.stage == crate::dialog_turn::TURN_OUTCOME_STAGE)
        .expect("turn outcome event");
    assert_eq!(outcome_event.data["outcome"], "skipped");
    assert_eq!(outcome_event.data["reason"], "queue_backlog_expired");
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "skipped");
    assert_eq!(rows[0].reason.as_deref(), Some("queue_backlog_expired"));
    Ok(())
}

#[tokio::test]
async fn dialog_worker_processes_old_job_that_already_started_processing()
-> Result<(), Box<dyn Error>> {
    // A requeued retry older than the old 180s stale gate must keep answering:
    // the budget is anchored at the recorded turn_started event, not job.created.
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let created = now - TimeDuration::seconds(11 * 60);
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("late retry"), created),
    );
    queue.append_job_event(
        job_id,
        TaskQueueJobEvent {
            stage: crate::dialog_turn::TURN_STARTED_STAGE.to_owned(),
            ..TaskQueueJobEvent::default()
        },
        now - TimeDuration::seconds(30),
    )?;
    let provider = ProviderStub::returning(DialogOutput {
        answer: "pong".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(!report.skipped_queue_backlog);
    assert!(report.sent_answer);
    assert!(report.completed);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_fails_turn_budget_exhausted_before_provider_call()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now - TimeDuration::seconds(200)),
    );
    queue.append_job_event(
        job_id,
        TaskQueueJobEvent {
            stage: crate::dialog_turn::TURN_STARTED_STAGE.to_owned(),
            ..TaskQueueJobEvent::default()
        },
        now - TimeDuration::seconds(115),
    )?;
    let provider = ProviderStub::returning(DialogOutput {
        answer: "unused".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.failed);
    assert!(provider.inputs().is_empty());
    assert!(effects.sent().is_empty());
    let record = queue.record(job_id).expect("job record");
    assert_eq!(record.status, JobStatus::Failed);
    assert!(
        record
            .error
            .as_deref()
            .is_some_and(|error| error.contains("budget exhausted"))
    );
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].outcome, "terminal_failed");
    assert_eq!(rows[0].reason.as_deref(), Some("budget_exhausted"));
    assert_eq!(rows[0].user_signal.as_deref(), Some("none"));
    assert_eq!(rows[0].budget_ms, Some(120_000));
    // Finalize adds the tick's real (unpaused) wall time on top of the 115s
    // anchored elapsed, so allow sub-second slack.
    let elapsed_ms = rows[0].elapsed_ms.expect("elapsed_ms");
    assert!(
        (115_000..116_000).contains(&elapsed_ms),
        "unexpected elapsed_ms: {elapsed_ms}"
    );
    Ok(())
}

#[tokio::test]
async fn dialog_worker_retries_provider_empty_output_and_fails_on_exhaustion()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now),
    );
    let provider = ProviderStub::returning(DialogOutput::default());
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );
    let options = DialogJobProcessOptions {
        queue_name: DIALOG_AIFARM_QUEUE_NAME,
        max_llm_job_attempts: 2,
        turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
        turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
        max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
        terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
        obligations: None,
        now,
        routing_events: None,
        turn_outcomes: Some(&outcomes),
        session: leaked_session_wiring(),
        llm_runs: None,
        activity_pulse: None,
    };

    let first = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        options,
    )
    .await;

    assert!(first.retry_requeued);
    assert!(first.empty_answer_error.is_some());
    assert!(!first.completed);
    assert!(!first.failed);
    assert_eq!(first.retry_attempt, Some(1));
    assert_eq!(record_status(&queue, job_id), JobStatus::Pending);

    let second = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        options,
    )
    .await;

    assert!(second.retry_exhausted);
    assert!(second.failed);
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].outcome, "retry_scheduled");
    assert_eq!(rows[1].reason.as_deref(), Some("provider_empty"));
    assert_eq!(rows[1].attempt, 1);
    assert_eq!(rows[0].outcome, "terminal_failed");
    assert_eq!(rows[0].reason.as_deref(), Some("provider_empty_exhausted"));
    assert_eq!(rows[0].attempt, 2);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_requeues_undeliverable_answer_instead_of_failing_at_send()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("напиши статью"), now),
    );
    let oversized_rich = format!("<p>{}</p>", "a".repeat(40_000));
    let provider = ProviderStub::returning(DialogOutput {
        answer: oversized_rich,
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );
    let options = DialogJobProcessOptions {
        queue_name: DIALOG_AIFARM_QUEUE_NAME,
        max_llm_job_attempts: 2,
        turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
        turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
        max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
        terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
        obligations: None,
        now,
        routing_events: None,
        turn_outcomes: Some(&outcomes),
        session: leaked_session_wiring(),
        llm_runs: None,
        activity_pulse: None,
    };

    let first = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        options,
    )
    .await;

    assert!(first.retry_requeued);
    assert!(!first.failed);
    assert!(!first.sent_answer);
    assert!(effects.sent().is_empty());
    assert!(
        first
            .empty_answer_error
            .as_deref()
            .is_some_and(|error| error.contains("rejected by outbound validation"))
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Pending);

    let second = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        options,
    )
    .await;

    assert!(second.retry_exhausted);
    assert!(second.failed);
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[1].reason.as_deref(), Some("undeliverable_answer"));
    assert_eq!(
        rows[0].reason.as_deref(),
        Some("undeliverable_answer_exhausted")
    );
    Ok(())
}

#[tokio::test]
async fn retry_handler_exhausts_on_budget_deadline_despite_remaining_attempts()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now),
    );
    let item = queue
        .dequeue_dialog_job(DIALOG_AIFARM_QUEUE_NAME)
        .await?
        .expect("work item");
    let params = dialog_params("hello");
    let mut report = DialogJobWorkerReport::default();

    let resolution = handle_retryable_dialog_provider_error(
        &queue,
        &item,
        &params,
        None,
        RetryableDialogProviderFailure {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            provider_name: "aifarm",
            reason: FailureReason::ProviderUnavailable,
            codes: PROVIDER_ERROR_RETRY_CODES,
            error: "status 503",
            max_attempts: 5,
            now: now + TimeDuration::seconds(130),
            budget_deadline: now + TimeDuration::seconds(120),
        },
        &mut report,
    )
    .await;

    assert!(report.retry_exhausted);
    assert_eq!(report.retry_attempt, Some(1));
    assert_eq!(
        resolution.outcome,
        crate::dialog_turn::TurnOutcome::TerminalFailed {
            reason: "budget_exhausted",
            error: "status 503".to_owned(),
            user_signal: crate::dialog_turn::UserSignalPlan::React,
        }
    );
    assert_eq!(
        resolution.disposition,
        crate::dialog_turn::JobDisposition::Fail("status 503".to_owned())
    );
    let record = queue.record(job_id).expect("job record");
    let event = record.events.last().expect("retry event");
    assert_eq!(event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
    assert_eq!(
        event.message,
        "retryable LLM provider error exhausted the turn budget"
    );
    Ok(())
}

#[tokio::test]
async fn event_append_failure_does_not_leave_job_processing() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = FailingEventQueue::new();
    let job_id = queue.inner.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    let provider =
        RetryableProviderStub::new("aifarm", FailureReason::ProviderUnavailable, "status 503");
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_in_queue_at(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        now,
    )
    .await;

    assert!(report.retry_requeued, "status write must win over events");
    assert!(report.status_error.is_some(), "append failure is recorded");
    assert!(!report.failed);
    let record = queue
        .inner
        .records()
        .into_iter()
        .find(|record| record.id == job_id)
        .expect("record");
    assert_eq!(record.status, JobStatus::Pending);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_records_turn_started_anchor_on_first_attempt() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now - TimeDuration::seconds(30)),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "pong".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

    assert!(report.sent_answer);
    let record = queue.record(job_id).expect("job record");
    let anchor = record
        .events
        .iter()
        .find(|event| event.stage == crate::dialog_turn::TURN_STARTED_STAGE)
        .expect("turn_started event");
    assert_eq!(anchor.at, "2026-05-19T12:30:00Z");
    let outcome_event = record
        .events
        .iter()
        .find(|event| event.stage == crate::dialog_turn::TURN_OUTCOME_STAGE)
        .expect("turn outcome event");
    assert_eq!(outcome_event.data["outcome"], "sent");
    Ok(())
}

#[tokio::test]
async fn dialog_worker_completes_content_blocked_provider_error() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    let provider = ProviderStub::failing(ContentBlockedError::new("safety"));
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_at(&queue, &provider, &effects, now).await;

    assert!(report.content_blocked);
    assert!(report.completed);
    assert!(!report.failed);
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_silently_completes_departed_member_without_provider_call()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("spam"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "must not run".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &DepartedMemberMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: None,
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.skipped_sender_not_member);
    assert!(report.completed);
    assert!(!report.failed);
    assert!(provider.inputs().is_empty());
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_marks_provider_and_send_errors_failed() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let provider_queue = InMemoryTaskQueue::new();
    let provider_job = provider_queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    let provider = ProviderStub::failing(io::Error::other("provider down"));
    let effects = EffectsStub::default();

    let provider_report =
        process_dialog_job_once_at(&provider_queue, &provider, &effects, now).await;

    assert!(provider_report.failed);
    assert_eq!(
        provider_report.provider_error,
        Some("provider down".to_owned())
    );
    assert_eq!(
        record_status(&provider_queue, provider_job),
        JobStatus::Failed
    );

    let send_queue = InMemoryTaskQueue::new();
    let send_job = send_queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "answer".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::failing();

    let send_report = process_dialog_job_once_at(&send_queue, &provider, &effects, now).await;

    assert!(send_report.failed);
    assert_eq!(send_report.send_error, Some("send down".to_owned()));
    assert_eq!(record_status(&send_queue, send_job), JobStatus::Failed);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_requeues_retryable_provider_error_like_go() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    let provider =
        RetryableProviderStub::new("gemini", FailureReason::ProviderOverloaded, "high demand");
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_in_queue_at(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        now,
    )
    .await;

    assert!(report.retry_requeued);
    assert!(!report.failed);
    assert_eq!(
        report.retryable_provider_error,
        Some("provider_overloaded".to_owned())
    );
    assert_eq!(report.retry_attempt, Some(1));
    assert_eq!(
        report.retry_target_queue,
        Some(DIALOG_AIFARM_QUEUE_NAME.to_owned())
    );
    let record = queue.record(job_id).expect("job");
    assert_eq!(record.status, JobStatus::Pending);
    assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
    assert_eq!(record.error, None);
    assert_eq!(record.worker_id, None);
    assert_eq!(record.started_at, None);
    assert_eq!(record.completed_at, None);
    let event = record
        .events
        .iter()
        .find(|event| event.stage == LLM_JOB_RETRY_STAGE)
        .expect("retry event");
    // `at` carries the real generation wall time on top of the tick time.
    assert!(
        event.at.starts_with("2026-05-19T12:30:00"),
        "unexpected retry event time: {}",
        event.at
    );
    assert_eq!(event.level, "warn");
    assert_eq!(event.stage, LLM_JOB_RETRY_STAGE);
    assert_eq!(event.attempt, 1);
    assert_eq!(event.provider, "gemini");
    assert_eq!(
        event.message,
        "retryable LLM provider error, requeueing job"
    );
    assert_eq!(
        event.error,
        "gemini provider provider_overloaded: high demand"
    );
    assert_eq!(event.data["fallback_reason"], "provider_overloaded");
    assert_eq!(event.data["max_attempts"], "5");
    assert_eq!(event.data["target_queue"], DIALOG_AIFARM_QUEUE_NAME);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_exhausts_retryable_provider_error_at_default_limit()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    for attempt in 1..DEFAULT_LLM_JOB_MAX_ATTEMPTS {
        queue.append_job_event(
            job_id,
            TaskQueueJobEvent {
                stage: LLM_JOB_RETRY_STAGE.to_owned(),
                attempt,
                ..TaskQueueJobEvent::default()
            },
            now,
        )?;
    }
    let provider =
        RetryableProviderStub::new("aifarm", FailureReason::ProviderUnavailable, "status 503");
    let effects = EffectsStub::default();
    let reporter = crate::runtime_routing::RoutingEventReporter::new(
        crate::runtime_routing::RoutingEventBuffer::new(8),
        None,
        None,
        Duration::from_secs(600),
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: Some(&reporter),
            turn_outcomes: None,
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;

    assert!(report.retry_exhausted);
    assert!(report.failed);
    assert_eq!(report.retry_attempt, Some(DEFAULT_LLM_JOB_MAX_ATTEMPTS));
    let record = queue.record(job_id).expect("job");
    assert_eq!(record.status, JobStatus::Failed);
    assert_eq!(
        record.error,
        Some("aifarm provider provider_unavailable: status 503".to_owned())
    );
    let retry_events: Vec<_> = record
        .events
        .iter()
        .filter(|event| {
            event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
        })
        .collect();
    assert_eq!(retry_events.len(), DEFAULT_LLM_JOB_MAX_ATTEMPTS as usize);
    let event = retry_events.last().expect("exhausted event");
    assert_eq!(event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
    assert_eq!(event.attempt, DEFAULT_LLM_JOB_MAX_ATTEMPTS);
    assert_eq!(
        event.message,
        "retryable LLM provider error exhausted job attempts"
    );
    assert_eq!(event.data["fallback_reason"], "provider_unavailable");
    let routing_events = reporter.buffer().routing_events(10);
    assert_eq!(routing_events.len(), 1);
    assert_eq!(routing_events[0].event_type, "all_attempts_exhausted");
    assert_eq!(routing_events[0].workflow_key, "dialog");
    assert_eq!(
        routing_events[0].queue_name.as_deref(),
        Some(DIALOG_AIFARM_QUEUE_NAME)
    );
    assert_eq!(routing_events[0].job_id, Some(job_id));
    assert_eq!(routing_events[0].chat_id, Some(42));
    assert_eq!(routing_events[0].message_id, Some(100));
    assert_eq!(
        routing_events[0].detail["last_retryable_reason"],
        "provider_unavailable"
    );
    Ok(())
}

#[tokio::test]
async fn dialog_worker_uses_configured_retry_attempt_limit() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("x"), now),
    );
    let provider =
        RetryableProviderStub::new("aifarm", FailureReason::ProviderUnavailable, "status 503");
    let effects = EffectsStub::default();

    let first = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: 2,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: None,
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;
    assert!(first.retry_requeued);
    assert_eq!(first.retry_max_attempts, Some(2));

    let second = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            queue_name: DIALOG_AIFARM_QUEUE_NAME,
            max_llm_job_attempts: 2,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: None,
            session: leaked_session_wiring(),
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await;
    assert!(second.retry_exhausted);
    assert!(second.failed);
    assert_eq!(second.retry_attempt, Some(2));

    let record = queue.record(job_id).expect("job");
    assert_eq!(record.status, JobStatus::Failed);
    let retry_events: Vec<_> = record
        .events
        .iter()
        .filter(|event| {
            event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
        })
        .collect();
    assert_eq!(retry_events.len(), 2);
    assert_eq!(retry_events[1].stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
    assert_eq!(retry_events[1].data["max_attempts"], "2");
    Ok(())
}

#[test]
fn dialog_job_has_payload_matches_go_text_meta_and_attachments() {
    assert!(!dialog_job_has_payload(&empty_params()));
    let mut original = empty_params();
    original.original_text = " original ".to_owned();
    assert!(dialog_job_has_payload(&original));

    let mut meta = empty_params();
    meta.meta = serde_json::json!({"vision_description":" image "});
    assert!(dialog_job_has_payload(&meta));

    let mut attachment = empty_params();
    attachment.meta = serde_json::to_value(ChatMessageMeta {
        attachments: vec![ChatAttachment {
            kind: "image".to_owned(),
            ..ChatAttachment::default()
        }],
        ..ChatMessageMeta::default()
    })
    .expect("meta json");
    assert!(dialog_job_has_payload(&attachment));
}

#[test]
fn dialog_job_answer_prefers_answer_then_response() {
    assert_eq!(
        dialog_job_answer(&DialogOutput {
            answer: " answer ".to_owned(),
            response: "response".to_owned(),
            ..DialogOutput::default()
        }),
        "answer"
    );
    assert_eq!(
        dialog_job_answer(&DialogOutput {
            response: " response ".to_owned(),
            ..DialogOutput::default()
        }),
        "response"
    );
}

#[test]
fn prepare_dialog_chat_response_matches_go_dialog_send_sanitizer() {
    assert_eq!(
        prepare_dialog_chat_response(r#"Он сказал "Привет" и ушел"#),
        r#"Он сказал "Привет" и ушел"#
    );
    assert_eq!(
        prepare_dialog_chat_response(r#"<b>Он сказал "Привет"</b><div> и ушел</div>"#),
        r#"<b>Он сказал "Привет"</b> и ушел"#
    );
    assert_eq!(prepare_dialog_chat_response("```\nhello\n```"), "hello");
    assert_eq!(
        prepare_dialog_chat_response("```json\n{\"ok\":true}\n```"),
        "{\"ok\":true}"
    );
    assert_eq!(
        prepare_dialog_chat_response("```go!\nprintln()\n```"),
        "go!\nprintln()"
    );
    assert_eq!(
        prepare_dialog_chat_response("```very-long-language-name\nbody\n```"),
        "very-long-language-name\nbody"
    );
    assert_eq!(prepare_dialog_chat_response("`hello`"), "hello");
    assert_eq!(
        prepare_dialog_chat_response(
            r#"<reply to_id="6877"><to_user>WaveCut</to_user></reply>
<assistant id="6878">Плотва</assistant>
<message_type>text</message_type>
<text>Только этот текст должен дойти.</text>"#
        ),
        "Только этот текст должен дойти."
    );
    assert_eq!(prepare_dialog_chat_response("thought\n<channel|>"), "");
    assert_eq!(
        prepare_dialog_chat_response(
            r#"<attach_id>171363</attach_id>
<message id="171363" thread_id="171360" timestamp="2026-07-13T14:49:25Z">
  <user username="Плотва" type="bot"></user>
  <message_type>text</message_type>
  <text>*шепотом* ...они уже начали ГОВОРИТЬ в эфире...</text>
</message>"#
        ),
        ""
    );
}

#[test]
fn dialog_rich_router_keeps_classic_telegram_html_classic() {
    assert!(!dialog_response_requires_rich("<pre>code</pre>"));
    assert!(!dialog_response_requires_rich(
        "<a href='https://example.test'>link</a>"
    ));
    assert!(dialog_response_requires_rich("<p>paragraph</p>"));
    assert!(dialog_response_requires_rich("<h2>Status</h2>"));
    assert!(dialog_response_requires_rich(
        "<table><tr><td>1</td></tr></table>"
    ));
    assert!(dialog_response_requires_rich("H<sub>2</sub>O"));
    assert!(dialog_response_requires_rich("x<sup>2</sup>"));
    assert!(dialog_response_requires_rich("<mark>note</mark>"));
}

#[test]
fn prepare_dialog_chat_response_keeps_routing_and_sanitizer_consistent() {
    use openplotva_telegram::is_valid_telegram_html;

    // A stray rich-only close tag selects the rich sanitizer, which then
    // drops it; surviving rich-only inline markup must not leak onto the
    // plain send path (prod send failure 2026-07-02: raw `<sub>` reached
    // sendMessage and Telegram rejected the reply).
    let degenerate = "Ответ. <b><i></b><i><sub></i></i><i><sub></li></code>";
    let out = prepare_dialog_chat_response(degenerate);
    assert!(
        dialog_response_requires_rich(&out) || is_valid_telegram_html(&out),
        "plain-routed answer must satisfy the plain sanitizer: {out}"
    );

    // `<br>` renders in rich messages but is not a rich trigger: once the
    // stray `</ul>` is dropped the answer re-routes to plain and must be
    // re-sanitized for the plain tag set.
    let flipped = "line1<br>line2</ul>";
    let out = prepare_dialog_chat_response(flipped);
    assert!(!dialog_response_requires_rich(&out));
    assert!(is_valid_telegram_html(&out), "{out}");
}

#[test]
fn duplicate_guard_matches_go_normalization_and_recent_model_window() {
    // Multi-message turns interleave several bot messages, so the guard scans
    // the last THREE model messages (not just the newest one)...
    let history = vec![
        HistoryMessage {
            message_id: 40,
            role: ROLE_USER.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "latest user message".to_owned(),
            ..HistoryMessage::default()
        },
        HistoryMessage {
            message_id: 39,
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "fresh answer".to_owned(),
            ..HistoryMessage::default()
        },
        HistoryMessage {
            message_id: 38,
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "same as candidate".to_owned(),
            ..HistoryMessage::default()
        },
    ];

    assert_eq!(
        should_suppress_duplicate_bot_reply(&history, "same as candidate"),
        (38, true)
    );

    // ...but a match beyond that window stays invisible.
    let mut deep_history = vec![HistoryMessage {
        message_id: 44,
        role: ROLE_USER.to_owned(),
        kind: MESSAGE_KIND_TEXT.to_owned(),
        text: "latest user message".to_owned(),
        ..HistoryMessage::default()
    }];
    for (offset, text) in ["one", "two", "three"].iter().enumerate() {
        deep_history.push(HistoryMessage {
            message_id: 43 - i32::try_from(offset).expect("offset"),
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: (*text).to_owned(),
            ..HistoryMessage::default()
        });
    }
    deep_history.push(HistoryMessage {
        message_id: 40,
        role: ROLE_MODEL.to_owned(),
        kind: MESSAGE_KIND_TEXT.to_owned(),
        text: "same as candidate".to_owned(),
        ..HistoryMessage::default()
    });
    assert_eq!(
        should_suppress_duplicate_bot_reply(&deep_history, "same as candidate"),
        (0, false)
    );

    let latest = vec![HistoryMessage {
        message_id: 29,
        role: ROLE_MODEL.to_owned(),
        kind: MESSAGE_KIND_TEXT.to_owned(),
        text: "Привет, мир & день!".to_owned(),
        original_text: "<b>Привет, мир &amp; день!</b>".to_owned(),
        ..HistoryMessage::default()
    }];

    assert_eq!(
        should_suppress_duplicate_bot_reply(&latest, "  <i>Привет,   мир &amp; день!</i>  "),
        (29, true)
    );
}

#[test]
fn duplicate_guard_ignores_foreign_bot_user_role() {
    let history = vec![
        HistoryMessage {
            message_id: 11,
            role: ROLE_USER.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "чужой бот".to_owned(),
            ..HistoryMessage::default()
        },
        HistoryMessage {
            message_id: 10,
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            text: "наша реплика".to_owned(),
            ..HistoryMessage::default()
        },
    ];

    assert_eq!(
        should_suppress_duplicate_bot_reply(&history, "чужой бот"),
        (0, false)
    );
}

#[test]
fn materialized_dialog_input_fills_go_context_settings_history_and_meta()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let mut params = dialog_params(" hello ");
    params.meta = serde_json::to_value(ChatMessageMeta {
        attachments: vec![ChatAttachment {
            kind: "image".to_owned(),
            source: "message".to_owned(),
            ..ChatAttachment::default()
        }],
        tool_calls: vec![openplotva_core::ToolCall {
            name: "final_response".to_owned(),
            ..openplotva_core::ToolCall::default()
        }],
        ..ChatMessageMeta::default()
    })?;
    let settings = ChatSettings {
        mood_alignment: Some(" warm ".to_owned()),
        custom_persona: Some("  custom voice  ".to_owned()),
        reactivity_percentage: 9,
        proactivity_percentage: 2,
        enable_obscenifier: false,
        enable_profanity: false,
        ..ChatSettings::defaults(params.chat_id)
    };
    let canonical_message_at = now - TimeDuration::seconds(5);
    let history = vec![HistoryMessage {
        message_id: params.message_id,
        kind: MESSAGE_KIND_TEXT.to_owned(),
        timestamp: Some(canonical_message_at),
        reply_to_id: 55,
        reply_to_name: "Bob".to_owned(),
        ..HistoryMessage::default()
    }];

    let input = dialog_input_from_materialized_context(
        &params,
        now,
        &DialogBotIdentity::new(99, "Plotvabot"),
        Some(&settings),
        Some(openplotva_core::ChatState::new(
            params.chat_id,
            "supergroup",
            Some(" Team ".to_owned()),
            None,
            None,
            None,
            Some(true),
        )),
        Some(openplotva_core::UserState::new(
            params.user_id,
            "Ada",
            None,
            Some("ada".to_owned()),
            Some(" uk ".to_owned()),
            Some(true),
        )),
        history,
    );

    assert_eq!(input.context.bot_name, "Plotvabot");
    assert_eq!(input.context.chat_title, "Team");
    assert_eq!(input.context.locale, "uk");
    assert_eq!(input.persona.mood, "warm");
    assert_eq!(input.persona.custom_persona, "custom voice");
    assert_eq!(input.persona.persona, None);
    assert!(!input.persona.profanity);
    assert!(!input.persona.obscenifier);
    assert_eq!(input.persona.reactivity, 9);
    assert_eq!(input.persona.proactivity, 2);
    assert_eq!(input.message.reply_to_id, 55);
    assert_eq!(input.message.reply_to_name, "Bob");
    assert_eq!(input.message.timestamp, Some(canonical_message_at));
    assert_eq!(input.message.meta.message_type, "image");
    assert_eq!(input.message.meta.sender_id, params.user_id);
    assert_eq!(input.message.meta.sender_name, "Ada");
    assert!(input.message.meta.tool_calls.is_empty());
    Ok(())
}

#[test]
fn dialog_history_payload_merge_matches_go_dedupe_order_and_reply_context()
-> Result<(), Box<dyn Error>> {
    let older = serde_json::json!({
        "entry_id": "msg:100",
        "kind": "text",
        "timestamp": "2024-03-09T16:00:00Z",
        "message_id": 100,
        "message_thread_id": 9,
        "text": "current",
        "from": {"id": 7, "first_name": "Ada", "username": "ada"},
        "reply_to_message": {
            "message_id": 55,
            "from": {"id": 8, "first_name": "Bob"}
        },
        "meta": {"sender_id": 7}
    });
    let duplicate_thread = serde_json::json!({
        "entry_id": "msg:100",
        "kind": "text",
        "timestamp": "2024-03-09T16:00:01Z",
        "message_id": 100,
        "text": "duplicate"
    });
    let newer_bot = serde_json::json!({
        "entry_id": "msg:101",
        "kind": "text",
        "timestamp": "2024-03-09T16:00:02Z",
        "message_id": 101,
        "text": "model",
        "from": {"id": 99, "first_name": "Plotva"},
        "meta": {"sender_id": 99}
    });

    let history = merge_dialog_history_payloads(
        &[serde_json::to_vec(&older)?, serde_json::to_vec(&newer_bot)?],
        &[serde_json::to_vec(&duplicate_thread)?],
        99,
    );

    assert_eq!(history.len(), 2);
    assert_eq!(history[0].entry_id, "msg:101");
    assert_eq!(history[0].role, ROLE_MODEL);
    assert_eq!(history[1].entry_id, "msg:100");
    assert_eq!(history[1].role, ROLE_USER);
    assert_eq!(history[1].reply_to_id, 55);
    assert_eq!(history[1].reply_to_name, "Bob");
    assert_eq!(history[1].meta.sender_name, "Ada");
    Ok(())
}

#[test]
fn dialog_history_context_matches_confirmed_telegram_message_order() -> Result<(), Box<dyn Error>> {
    let payload = |id: i32, sender_id: i64, role: &str, text: &str| {
        serde_json::to_vec(&serde_json::json!({
            "entry_id": format!("msg:{id}"),
            "kind": "text",
            "role": role,
            "timestamp": "2026-07-12T19:42:00Z",
            "message_id": id,
            "text": text,
            "from": {"id": sender_id, "first_name": if sender_id == 99 {"Plotva"} else {"Ada"}},
            "meta": {"sender_id": sender_id}
        }))
    };
    let history = merge_dialog_history_payloads(
        &[
            payload(1110915, 7, "user", "посмотри видео")?,
            payload(1110914, 99, "model", "что именно?")?,
            payload(1110913, 7, "user", "и шо?")?,
            payload(1110912, 99, "model", "сейчас посмотрю")?,
            payload(1110911, 7, "user", "опиши")?,
        ],
        &[],
        99,
    );

    let selected =
        openplotva_dialog::select_llm_history_messages_for_context(&history, 10, 1110915, 0);

    assert_eq!(
        selected
            .iter()
            .map(|message| (message.message_id, message.role.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (1110911, ROLE_USER),
            (1110912, ROLE_MODEL),
            (1110913, ROLE_USER),
            (1110914, ROLE_MODEL),
        ]
    );
    Ok(())
}

#[test]
fn dialog_persona_uses_go_daily_catalogue_without_custom_persona() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(12_345 * 86_400)?;
    let settings = ChatSettings {
        custom_persona: None,
        ..ChatSettings::defaults(42)
    };

    let persona = dialog_persona(42, now, Some(&settings), "Plotvabot");

    assert_eq!(
        persona.persona,
        Some(openplotva_dialog::daily_persona_for_day(42, 12_345))
    );
    assert!(persona.custom_persona.is_empty());
    Ok(())
}

#[derive(Clone)]
struct ProviderStub {
    state: Arc<Mutex<ProviderState>>,
}

struct ProviderState {
    result: Result<DialogOutput, Box<dyn Error + Send + Sync + 'static>>,
    inputs: Vec<DialogInput>,
}

impl ProviderStub {
    fn returning(output: DialogOutput) -> Self {
        Self {
            state: Arc::new(Mutex::new(ProviderState {
                result: Ok(output),
                inputs: Vec::new(),
            })),
        }
    }

    fn failing<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            state: Arc::new(Mutex::new(ProviderState {
                result: Err(Box::new(error)),
                inputs: Vec::new(),
            })),
        }
    }

    fn inputs(&self) -> Vec<DialogInput> {
        self.state.lock().expect("provider state").inputs.clone()
    }
}

/// Legacy canned outputs adapt to the step seam as one text-only final step.
fn step_output_from_dialog(output: &DialogOutput) -> openplotva_dialog::ChatStepOutput {
    openplotva_dialog::ChatStepOutput {
        provider: output.provider.clone(),
        text: dialog_job_answer(output),
        ..openplotva_dialog::ChatStepOutput::default()
    }
}

impl ChatProvider for ProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn as_chat_step(&self) -> Option<&dyn openplotva_llm::ChatStepProvider> {
        Some(self)
    }
}

impl openplotva_llm::ChatStepProvider for ProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(
        &'a self,
        request: openplotva_dialog::ChatStepRequest,
    ) -> openplotva_llm::ChatStepFuture<'a> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("provider state");
            state.inputs.push(request.input);
            match &state.result {
                Ok(output) => Ok(step_output_from_dialog(output)),
                Err(error) => {
                    let error: Box<dyn Error + Send + Sync + 'static> =
                        Box::new(io::Error::other(error.to_string()));
                    Err(error)
                }
            }
        })
    }
}

/// Provider returning scripted outputs in order, repeating the last one.
#[derive(Clone)]
struct SequenceProviderStub {
    state: Arc<Mutex<SequenceProviderState>>,
}

struct SequenceProviderState {
    outputs: Vec<DialogOutput>,
    inputs: Vec<DialogInput>,
}

impl SequenceProviderStub {
    fn returning(outputs: Vec<DialogOutput>) -> Self {
        assert!(!outputs.is_empty(), "sequence provider needs outputs");
        Self {
            state: Arc::new(Mutex::new(SequenceProviderState {
                outputs,
                inputs: Vec::new(),
            })),
        }
    }

    fn inputs(&self) -> Vec<DialogInput> {
        self.state.lock().expect("provider state").inputs.clone()
    }
}

impl ChatProvider for SequenceProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn as_chat_step(&self) -> Option<&dyn openplotva_llm::ChatStepProvider> {
        Some(self)
    }
}

impl openplotva_llm::ChatStepProvider for SequenceProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(
        &'a self,
        request: openplotva_dialog::ChatStepRequest,
    ) -> openplotva_llm::ChatStepFuture<'a> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("provider state");
            let round = state.inputs.len();
            state.inputs.push(request.input);
            let index = round.min(state.outputs.len() - 1);
            Ok(step_output_from_dialog(&state.outputs[index]))
        })
    }
}

/// Provider that consumes (paused) tokio time before answering, so tests
/// can drive the budget without real waiting.
struct SlowProviderStub {
    output: DialogOutput,
    delay: Duration,
    calls: Arc<Mutex<usize>>,
}

impl SlowProviderStub {
    fn new(output: DialogOutput, delay: Duration) -> Self {
        Self {
            output,
            delay,
            calls: Arc::new(Mutex::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        *self.calls.lock().expect("slow provider calls")
    }
}

impl ChatProvider for SlowProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn as_chat_step(&self) -> Option<&dyn openplotva_llm::ChatStepProvider> {
        Some(self)
    }
}

impl openplotva_llm::ChatStepProvider for SlowProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(
        &'a self,
        _request: openplotva_dialog::ChatStepRequest,
    ) -> openplotva_llm::ChatStepFuture<'a> {
        Box::pin(async move {
            *self.calls.lock().expect("slow provider calls") += 1;
            tokio::time::sleep(self.delay).await;
            Ok(step_output_from_dialog(&self.output))
        })
    }
}

/// Recording terminal user signal with a scripted result.
#[derive(Clone)]
struct FakeSignal {
    state: Arc<Mutex<FakeSignalState>>,
}

struct FakeSignalState {
    calls: Vec<crate::dialog_turn::SignalTarget>,
    result: crate::dialog_turn::UserSignalResult,
}

impl Default for FakeSignal {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeSignalState {
                calls: Vec::new(),
                result: crate::dialog_turn::UserSignalResult::ReactionSent,
            })),
        }
    }
}

impl FakeSignal {
    fn calls(&self) -> Vec<crate::dialog_turn::SignalTarget> {
        self.state.lock().expect("signal state").calls.clone()
    }
}

impl crate::dialog_turn::TerminalUserSignal for FakeSignal {
    fn signal_turn_failure<'a>(
        &'a self,
        target: crate::dialog_turn::SignalTarget,
    ) -> crate::dialog_turn::UserSignalFuture<'a> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("signal state");
            state.calls.push(target);
            state.result.clone()
        })
    }
}

struct RetryableProviderStub {
    provider: &'static str,
    reason: FailureReason,
    message: &'static str,
}

impl RetryableProviderStub {
    fn new(provider: &'static str, reason: FailureReason, message: &'static str) -> Self {
        Self {
            provider,
            reason,
            message,
        }
    }
}

impl ChatProvider for RetryableProviderStub {
    fn provider_name(&self) -> &str {
        self.provider
    }

    fn as_chat_step(&self) -> Option<&dyn openplotva_llm::ChatStepProvider> {
        Some(self)
    }
}

impl openplotva_llm::ChatStepProvider for RetryableProviderStub {
    fn provider_name(&self) -> &str {
        self.provider
    }

    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(
        &'a self,
        _request: openplotva_dialog::ChatStepRequest,
    ) -> openplotva_llm::ChatStepFuture<'a> {
        Box::pin(async move {
            let error: Box<dyn Error + Send + Sync + 'static> =
                Box::new(ProviderError::new(self.provider, self.reason, self.message));
            Err(error)
        })
    }
}

#[derive(Clone, Default)]
struct EffectsStub {
    state: Arc<Mutex<EffectsState>>,
}

#[derive(Default)]
struct EffectsState {
    sent: Vec<(String, String)>,
    answer_options: Vec<DialogAnswerSendOptions>,
    intermediates: Vec<(String, u32, bool)>,
    queued_answer: Option<QueuedBatchReceipt>,
    fail: bool,
}

impl EffectsStub {
    fn failing() -> Self {
        let this = Self::default();
        this.state.lock().expect("effects state").fail = true;
        this
    }

    fn sent(&self) -> Vec<(String, String)> {
        self.state.lock().expect("effects state").sent.clone()
    }

    fn answer_options(&self) -> Vec<DialogAnswerSendOptions> {
        self.state
            .lock()
            .expect("effects state")
            .answer_options
            .clone()
    }

    fn with_queued_answer(receipt: QueuedBatchReceipt) -> Self {
        let this = Self::default();
        this.state.lock().expect("effects state").queued_answer = Some(receipt);
        this
    }

    #[allow(dead_code)]
    fn intermediates(&self) -> Vec<(String, u32, bool)> {
        self.state
            .lock()
            .expect("effects state")
            .intermediates
            .clone()
    }
}

impl DialogJobEffects for EffectsStub {
    type Error = io::Error;

    fn send_dialog_answer<'a>(
        &'a self,
        dialog_job_id: i64,
        _causation_update_id: Option<i64>,
        params: &'a DialogJobParams,
        answer: &'a str,
        options: DialogAnswerSendOptions,
    ) -> DialogJobReceiptFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("effects state");
            if state.fail {
                return Err(io::Error::other("send down"));
            }
            state
                .sent
                .push((params.message_text.clone(), answer.to_owned()));
            state.answer_options.push(options);
            Ok(QueuedBatchReceipt::dispatcher(
                format!("test-dialog-answer:{dialog_job_id}"),
                vec![format!("test-operation:{dialog_job_id}")],
            ))
        })
    }

    fn send_dialog_intermediate<'a>(
        &'a self,
        _dialog_job_id: i64,
        _causation_update_id: Option<i64>,
        _params: &'a DialogJobParams,
        text: &'a str,
        seq: u32,
        reply_to_trigger: bool,
    ) -> DialogJobEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("effects state");
            if state.fail {
                return Err(io::Error::other("send down"));
            }
            state
                .intermediates
                .push((text.to_owned(), seq, reply_to_trigger));
            Ok(())
        })
    }

    fn queued_dialog_answer<'a>(
        &'a self,
        _dialog_job_id: i64,
    ) -> DialogJobReceiptLookupFuture<'a, Self::Error> {
        Box::pin(async move {
            Ok(self
                .state
                .lock()
                .expect("effects state")
                .queued_answer
                .clone())
        })
    }
}

#[derive(Clone, Default)]
struct ToolHistoryStub {
    state: Arc<Mutex<ToolHistoryState>>,
}

#[derive(Default)]
struct ToolHistoryState {
    calls: Vec<(i64, i32, Vec<ToolCall>)>,
    fail: bool,
}

impl ToolHistoryStub {
    fn failing() -> Self {
        let this = Self::default();
        this.state.lock().expect("tool history state").fail = true;
        this
    }

    fn calls(&self) -> Vec<(i64, i32, Vec<ToolCall>)> {
        self.state.lock().expect("tool history state").calls.clone()
    }
}

impl DialogToolCallHistoryStore for ToolHistoryStub {
    type Error = io::Error;

    fn upsert_dialog_tool_calls<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        tool_calls: &'a [ToolCall],
    ) -> DialogToolCallHistoryFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("tool history state");
            if state.fail {
                return Err(io::Error::other("history down"));
            }
            state.calls.push((chat_id, message_id, tool_calls.to_vec()));
            Ok(true)
        })
    }
}

#[derive(Clone, Default)]
struct MaterializerStub {
    history: Vec<HistoryMessage>,
}

#[derive(Clone, Copy, Default)]
struct DepartedMemberMaterializer;

impl DialogInputMaterializer for DepartedMemberMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        _now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move {
            Err(DialogInputMaterializationError::SenderNotMember {
                chat_id: params.chat_id,
                user_id: params.user_id,
            })
        })
    }
}

#[derive(Clone, Default)]
struct FailingInjectedMaterializer {
    calls: Arc<Mutex<usize>>,
}

impl DialogInputMaterializer for FailingInjectedMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move {
            let mut calls = self.calls.lock().expect("materializer calls");
            *calls += 1;
            if *calls > 1 {
                return Err(DialogInputMaterializationError::History {
                    message: "temporary history failure".to_owned(),
                });
            }
            Ok(dialog_input_from_job_params_at(params, now))
        })
    }
}

impl MaterializerStub {
    fn with_history(history: Vec<HistoryMessage>) -> Self {
        Self { history }
    }
}

impl DialogInputMaterializer for MaterializerStub {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a DialogJobParams,
        now: OffsetDateTime,
    ) -> DialogInputMaterializerFuture<'a> {
        Box::pin(async move {
            let mut input = dialog_input_from_job_params_at(params, now);
            input.history = self.history.clone();
            Ok(input)
        })
    }
}

#[tokio::test]
async fn deliverable_validation_matches_outbound_acceptance() -> Result<(), Box<dyn Error>> {
    let queue = Arc::new(DispatcherQueue::new(
        openplotva_telegram::DispatcherConfig::default(),
    ));
    let effects = DialogDispatcherEffects::new(Arc::clone(&queue))
        .with_virtual_id_factory(Arc::new(|| "dialog-v1".to_owned()));
    let params = dialog_params("hello");
    let corpus: Vec<String> = vec![
        "hello".to_owned(),
        "<b>hello</b>".to_owned(),
        "<p>rich paragraph</p>".to_owned(),
        "<b></b>".to_owned(),
        "<b> </b>".to_owned(),
        "\u{200b}\u{200c}".to_owned(),
        "&#x200b;".to_owned(),
        format!("<p>{}</p>", "a".repeat(40_000)),
        format!("<h2>{}</h2>", "б".repeat(100)),
    ];

    for answer in corpus {
        let validated = validate_dialog_answer_deliverable(&answer).is_ok();
        let accepted = effects
            .send_dialog_answer(
                1,
                None,
                &params,
                &answer,
                DialogAnswerSendOptions::default(),
            )
            .await
            .is_ok();
        assert_eq!(
            validated,
            accepted,
            "validator and outbound boundary disagree on {:?}...",
            answer.chars().take(40).collect::<String>()
        );
    }
    Ok(())
}

fn ledger_rows(
    outcomes: &crate::dialog_turn::DialogTurnObserver,
) -> Vec<openplotva_server::RuntimeTurnOutcomeData> {
    use openplotva_server::{RuntimeTurnOutcomeInspector, RuntimeTurnOutcomesFilter};
    outcomes
        .buffer()
        .turn_outcomes(RuntimeTurnOutcomesFilter {
            limit: 32,
            ..RuntimeTurnOutcomesFilter::default()
        })
        .expect("turn outcomes")
}

/// Queue whose event appends always fail while status writes succeed,
/// probing the S13 invariant.
struct FailingEventQueue {
    inner: InMemoryTaskQueue,
}

impl FailingEventQueue {
    fn new() -> Self {
        Self {
            inner: InMemoryTaskQueue::new(),
        }
    }
}

impl DialogJobWorkerQueue for FailingEventQueue {
    type Error = TaskQueueError;

    fn dequeue_dialog_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> DialogJobWorkerFuture<'a, Option<DialogJobWorkItem>, Self::Error> {
        self.inner.dequeue_dialog_job(queue_name)
    }

    fn respawn_dialog_job<'a>(
        &'a self,
        queue_name: &'a str,
        job: openplotva_taskman::StatelessJobItem,
    ) -> DialogJobWorkerFuture<'a, i64, Self::Error> {
        self.inner.respawn_dialog_job(queue_name, job)
    }

    fn pending_dialog_job_depth<'a>(
        &'a self,
        queue_name: &'static str,
        priority: Priority,
    ) -> DialogJobWorkerFuture<'a, usize, Self::Error> {
        self.inner.pending_dialog_job_depth(queue_name, priority)
    }

    fn complete_dialog_job<'a>(
        &'a self,
        job_id: i64,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        self.inner.complete_dialog_job(job_id)
    }

    fn wait_for_dialog_delivery<'a>(
        &'a self,
        job_id: i64,
    ) -> DialogJobWorkerFuture<'a, bool, Self::Error> {
        self.inner.wait_for_dialog_delivery(job_id)
    }

    fn fail_dialog_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        self.inner.fail_dialog_job(job_id, error)
    }

    fn append_dialog_job_event<'a>(
        &'a self,
        _job_id: i64,
        _event: TaskQueueJobEvent,
        _at: OffsetDateTime,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        Box::pin(async { Err(TaskQueueError::QueueNameRequired) })
    }

    fn requeue_retryable_dialog_job<'a>(
        &'a self,
        job_id: i64,
        target_queue: &'a str,
    ) -> DialogJobWorkerFuture<'a, (), Self::Error> {
        self.inner
            .requeue_retryable_dialog_job(job_id, target_queue)
    }
}

fn dialog_params(text: &str) -> DialogJobParams {
    DialogJobParams {
        chat_id: 42,
        message_id: 100,
        user_id: 7,
        user_full_name: "Ada".to_owned(),
        message_text: text.to_owned(),
        original_text: String::new(),
        meta: serde_json::Value::Null,
        max_output_tokens: 512,
        thread_id: Some(9),
    }
}

fn empty_params() -> DialogJobParams {
    DialogJobParams {
        message_text: String::new(),
        max_output_tokens: 0,
        thread_id: None,
        ..dialog_params("")
    }
}

fn record_status(queue: &InMemoryTaskQueue, job_id: i64) -> JobStatus {
    queue
        .records()
        .into_iter()
        .find(|record| record.id == job_id)
        .expect("record")
        .status
}

// ---- Dialog session engine ----

type StepHook = Box<dyn FnMut(usize) + Send>;

struct StepProviderStub {
    steps: Mutex<VecDeque<Result<openplotva_dialog::ChatStepOutput, String>>>,
    requests: Mutex<Vec<openplotva_dialog::ChatStepRequest>>,
    retryable_errors: bool,
    on_call: Mutex<Option<StepHook>>,
}

impl StepProviderStub {
    fn with_steps(steps: Vec<Result<openplotva_dialog::ChatStepOutput, String>>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            retryable_errors: true,
            on_call: Mutex::new(None),
        }
    }

    fn with_on_call(self, hook: StepHook) -> Self {
        *self.on_call.lock().expect("on_call") = Some(hook);
        self
    }

    fn requests(&self) -> Vec<openplotva_dialog::ChatStepRequest> {
        self.requests.lock().expect("step requests").clone()
    }

    fn calls(&self) -> usize {
        self.requests.lock().expect("step requests").len()
    }
}

fn step_text(text: &str) -> openplotva_dialog::ChatStepOutput {
    openplotva_dialog::ChatStepOutput {
        provider: "step-stub".to_owned(),
        model: "test-model".to_owned(),
        text: text.to_owned(),
        ..openplotva_dialog::ChatStepOutput::default()
    }
}

fn step_tools(
    text: &str,
    calls: Vec<(&str, openplotva_dialog::ToolStep)>,
) -> openplotva_dialog::ChatStepOutput {
    openplotva_dialog::ChatStepOutput {
        provider: "step-stub".to_owned(),
        model: "test-model".to_owned(),
        text: text.to_owned(),
        tool_calls: calls
            .into_iter()
            .map(|(id, step)| openplotva_dialog::ChatStepToolCall {
                id: id.to_owned(),
                step,
                salvaged: false,
            })
            .collect(),
        ..openplotva_dialog::ChatStepOutput::default()
    }
}

impl ChatProvider for StepProviderStub {
    fn provider_name(&self) -> &str {
        "step-stub"
    }

    fn as_chat_step(&self) -> Option<&dyn openplotva_llm::ChatStepProvider> {
        Some(self)
    }
}

impl openplotva_llm::ChatStepProvider for StepProviderStub {
    fn provider_name(&self) -> &str {
        "step-stub"
    }

    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(
        &'a self,
        request: openplotva_dialog::ChatStepRequest,
    ) -> openplotva_llm::ChatStepFuture<'a> {
        Box::pin(async move {
            let call_index = {
                let mut requests = self.requests.lock().expect("step requests");
                requests.push(request);
                requests.len()
            };
            if let Some(hook) = self.on_call.lock().expect("on_call").as_mut() {
                hook(call_index);
            }
            match self.steps.lock().expect("steps").pop_front() {
                Some(Ok(step)) => Ok(step),
                Some(Err(message)) if self.retryable_errors => Err(Box::new(ProviderError::new(
                    "step-stub",
                    FailureReason::ProviderUnavailable,
                    message,
                ))
                    as ChatProviderError),
                Some(Err(message)) => Err(message.into()),
                None => panic!("step stub ran out of scripted steps"),
            }
        })
    }
}

#[derive(Default)]
struct SessionToolboxStub {
    web_search_queries: Mutex<Vec<String>>,
    web_search_result: Mutex<Option<openplotva_dialog::ToolResult>>,
    understand_media_refs: Mutex<Vec<String>>,
    draw_requests: Mutex<Vec<openplotva_dialog::DrawRequest>>,
    draw_result: Mutex<Option<openplotva_dialog::ToolResult>>,
    song_result: Mutex<Option<openplotva_dialog::ToolResult>>,
}

impl SessionToolboxStub {
    fn with_web_search_result(result: openplotva_dialog::ToolResult) -> Self {
        let stub = Self::default();
        *stub.web_search_result.lock().expect("web search result") = Some(result);
        stub
    }

    fn with_queued_draw(ticket: &str) -> Self {
        let stub = Self::default();
        *stub.draw_result.lock().expect("draw result") = Some(openplotva_dialog::ToolResult {
            status: "queued".to_owned(),
            side_effect: Some(openplotva_dialog::ToolSideEffect {
                kind: openplotva_dialog::turn::SIDE_EFFECT_KIND_IMAGE.to_owned(),
                ticket_id: ticket.to_owned(),
                eta: "~1 min".to_owned(),
                state: "queued".to_owned(),
            }),
            ..openplotva_dialog::ToolResult::default()
        });
        stub
    }

    fn with_queued_song(ticket: &str) -> Self {
        let stub = Self::default();
        *stub.song_result.lock().expect("song result") = Some(openplotva_dialog::ToolResult {
            status: "queued".to_owned(),
            side_effect: Some(openplotva_dialog::ToolSideEffect {
                kind: openplotva_dialog::turn::SIDE_EFFECT_KIND_MUSIC.to_owned(),
                ticket_id: ticket.to_owned(),
                eta: "~3 min".to_owned(),
                state: "queued".to_owned(),
            }),
            ..openplotva_dialog::ToolResult::default()
        });
        stub
    }
}

impl openplotva_dialog::DialogToolbox for SessionToolboxStub {
    fn web_search<'a>(&'a self, query: String) -> openplotva_dialog::ToolboxFuture<'a> {
        self.web_search_queries.lock().expect("queries").push(query);
        let result = self
            .web_search_result
            .lock()
            .expect("web search result")
            .clone()
            .unwrap_or_else(|| openplotva_dialog::ToolResult {
                status: "ok".to_owned(),
                message: "results: solstice is on June 21".to_owned(),
                ..openplotva_dialog::ToolResult::default()
            });
        Box::pin(async move { Ok(result) })
    }

    fn understand_media<'a>(
        &'a self,
        request: openplotva_dialog::VisionRequest,
    ) -> openplotva_dialog::ToolboxFuture<'a> {
        self.understand_media_refs
            .lock()
            .expect("media refs")
            .push(request.file_id);
        Box::pin(async {
            Ok(openplotva_dialog::ToolResult {
                status: "ok".to_owned(),
                message: "media understood".to_owned(),
                data: Some(serde_json::json!({"file_unique_id": "media-unique"})),
                ..openplotva_dialog::ToolResult::default()
            })
        })
    }

    fn draw_image<'a>(
        &'a self,
        req: openplotva_dialog::DrawRequest,
    ) -> openplotva_dialog::ToolboxFuture<'a> {
        self.draw_requests.lock().expect("draws").push(req);
        let result = self
            .draw_result
            .lock()
            .expect("draw result")
            .clone()
            .unwrap_or_else(|| openplotva_dialog::ToolResult::failed("draw_down", "no drawing"));
        Box::pin(async move { Ok(result) })
    }

    fn generate_song<'a>(
        &'a self,
        _req: openplotva_dialog::SongRequest,
    ) -> openplotva_dialog::ToolboxFuture<'a> {
        let result = self
            .song_result
            .lock()
            .expect("song result")
            .clone()
            .unwrap_or_else(|| openplotva_dialog::ToolResult::failed("song_down", "no singing"));
        Box::pin(async move { Ok(result) })
    }
}

#[derive(Default)]
struct ReactorStub {
    reactions: Mutex<Vec<(i64, i64, String)>>,
}

impl crate::dialog_turn::SessionReactor for ReactorStub {
    fn react<'a>(
        &'a self,
        chat_id: i64,
        message_id: i64,
        emoji: &'a str,
    ) -> crate::dialog_turn::SessionReactionFuture<'a> {
        self.reactions
            .lock()
            .expect("reactions")
            .push((chat_id, message_id, emoji.to_owned()));
        Box::pin(async { Ok(()) })
    }
}

fn session_wiring(
    toolbox: Arc<dyn openplotva_dialog::DialogToolbox>,
    reactor: Option<Arc<dyn crate::dialog_turn::SessionReactor>>,
) -> crate::dialog_turn::SessionWorkerWiring {
    crate::dialog_turn::SessionWorkerWiring {
        toolbox,
        reactor,
        registry: std::sync::Arc::new(crate::dialog_turn::DialogSessionRegistry::new()),
        max_iterations: 8,
        max_messages: 4,
        tool_extension_secs: 60,
        hard_cap_secs: 300,
        max_draws: 1,
        max_songs: 1,
    }
}

/// Test-only default wiring: leaked so inline `DialogJobProcessOptions`
/// literals can borrow it for 'static.
fn leaked_session_wiring() -> &'static crate::dialog_turn::SessionWorkerWiring {
    Box::leak(Box::new(session_wiring(
        Arc::new(SessionToolboxStub::default()),
        None,
    )))
}

async fn process_dialog_job_once_in_queue_at<Queue, Provider, Effects>(
    queue: &Queue,
    queue_name: &'static str,
    provider: &Provider,
    effects: &Effects,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_with_materializer_and_history_at(
        queue,
        queue_name,
        provider,
        effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        now,
    )
    .await
}

async fn process_dialog_job_once_at<Queue, Provider, Effects>(
    queue: &Queue,
    provider: &Provider,
    effects: &Effects,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_at(queue, DIALOG_AIFARM_QUEUE_NAME, provider, effects, now)
        .await
}

async fn process_dialog_job_once_in_queue_with_materializer_and_history_at<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
>(
    queue: &Queue,
    queue_name: &'static str,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_with_materializer_and_history_at_with_session(
        queue,
        queue_name,
        provider,
        effects,
        materializer,
        tool_history,
        leaked_session_wiring(),
        now,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn process_dialog_job_once_in_queue_with_materializer_and_history_at_with_session<
    Queue,
    Provider,
    Effects,
    Materializer,
    ToolHistory,
>(
    queue: &Queue,
    queue_name: &'static str,
    provider: &Provider,
    effects: &Effects,
    materializer: &Materializer,
    tool_history: &ToolHistory,
    session: &crate::dialog_turn::SessionWorkerWiring,
    now: OffsetDateTime,
) -> DialogJobWorkerReport
where
    Queue: DialogJobWorkerQueue + Sync + ?Sized,
    Provider: ChatProvider + Sync + ?Sized,
    Effects: DialogJobEffects + Sync + ?Sized,
    Materializer: DialogInputMaterializer + Sync + ?Sized,
    ToolHistory: DialogToolCallHistoryStore + Sync + ?Sized,
{
    process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        queue,
        provider,
        effects,
        materializer,
        tool_history,
        DialogJobProcessOptions {
            queue_name,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            now,
            routing_events: None,
            turn_outcomes: None,
            session,
            llm_runs: None,
            activity_pulse: None,
        },
    )
    .await
}

fn session_options<'a>(
    now: OffsetDateTime,
    outcomes: &'a crate::dialog_turn::DialogTurnObserver,
    wiring: &'a crate::dialog_turn::SessionWorkerWiring,
) -> DialogJobProcessOptions<'a> {
    DialogJobProcessOptions {
        queue_name: DIALOG_AIFARM_QUEUE_NAME,
        max_llm_job_attempts: 2,
        turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
        turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
        max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
        terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
        obligations: None,
        now,
        routing_events: None,
        turn_outcomes: Some(outcomes),
        session: wiring,
        llm_runs: None,
        activity_pulse: None,
    }
}

#[tokio::test]
async fn session_runs_tool_round_then_final_text() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("когда солнцестояние?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "солнцестояние 2026".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("21 июня, лови")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(report.session_iterations, 2);
    assert_eq!(effects.sent().len(), 1);
    assert_eq!(effects.sent()[0].1, "21 июня, лови");
    assert_eq!(
        effects.answer_options(),
        vec![DialogAnswerSendOptions {
            disable_link_preview: true,
        }],
        "a successful web_search disables previews only on the final answer"
    );
    assert!(effects.intermediates().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    // The second step saw the first round's tool result in the transcript.
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].transcript.is_empty());
    assert!(matches!(
        &requests[1].transcript[..],
        [
            openplotva_dialog::SessionMessage::Assistant { .. },
            openplotva_dialog::SessionMessage::ToolResult { tool_call_id, .. },
        ] if tool_call_id == "call-1"
    ));
    // The engine advertises send_message/react_to_message on non-final steps.
    let openplotva_dialog::ToolsMode::Native(tools) = &requests[0].tools else {
        panic!("first step must carry native tools");
    };
    let rendered = serde_json::to_string(tools)?;
    assert!(rendered.contains("send_message"));
    assert!(rendered.contains("react_to_message"));
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "sent");
    Ok(())
}

#[tokio::test]
async fn failed_web_search_keeps_final_answer_link_previews_unchanged() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что сейчас?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "что сейчас".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("не смогла проверить свежие данные")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::with_web_search_result(
            openplotva_dialog::ToolResult::failed("search_down", "search unavailable"),
        ));
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(
        effects.answer_options(),
        vec![DialogAnswerSendOptions::default()]
    );
    Ok(())
}

/// The production entry point is the worker LOOP, not the single-shot process
/// call the other session tests use; this pins the loop forwarding
/// `LoopOptions.session` into every tick (a dropped option silently reverts
/// prod to the legacy provider loop — the stub's `run_dialog` panics on that).
#[tokio::test]
async fn worker_loop_forwards_session_wiring_to_turns() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::now_utc();
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("когда солнцестояние?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "солнцестояние 2026".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("21 июня, лови")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let stop = async {
        for _ in 0..500 {
            if record_status(&queue, job_id) == JobStatus::Completed {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };
    run_dialog_job_worker_with_materializer_and_history_every_until(
        &queue,
        &provider,
        &effects,
        DialogJobWorkerLoopOptions {
            materializer: &BasicDialogInputMaterializer,
            tool_history: &NoopDialogToolCallHistoryStore,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            queue_names: &DIALOG_JOB_WORKER_QUEUES,
            interval: std::time::Duration::from_millis(5),
            max_llm_job_attempts: 2,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            session: &wiring,
            llm_runs: None,
            activity_pulse: None,
        },
        stop,
    )
    .await;

    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    assert_eq!(effects.sent().len(), 1);
    assert_eq!(effects.sent()[0].1, "21 июня, лови");
    assert_eq!(provider.requests().len(), 2);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "sent");
    assert_eq!(rows[0].detail["iterations"], serde_json::json!(2));
    Ok(())
}

#[tokio::test]
async fn turn_opens_and_closes_an_agent_run_with_origin_tools_and_outcome()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что там в мире?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "новости".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("вот что нашлось")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let runs = crate::runtime_llm_runs::RuntimeLlmRunBuffer::new(8);
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    )
    .with_run_buffer(runs.clone());

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            llm_runs: Some(&runs),
            activity_pulse: None,
            ..session_options(now, &outcomes, &wiring)
        },
    )
    .await;
    assert!(report.sent_answer);

    let records = runs.list(&crate::runtime_llm_runs::RunListFilter::default(), now);
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.run_id, format!("job-{job_id}"));
    assert_eq!(record.kind, "dialog");
    assert_eq!(record.status, crate::runtime_llm_runs::RunStatus::Completed);
    assert_eq!(record.origin.chat_id, 42);
    assert_eq!(record.origin.thread_id, Some(9));
    assert_eq!(record.origin.user_id, 7);
    assert_eq!(record.origin.user_full_name.as_deref(), Some("Ada"));
    assert_eq!(record.origin.trigger_message_id, 100);
    assert_eq!(
        record.origin.trigger_preview.as_deref(),
        Some("что там в мире?")
    );
    assert_eq!(
        record.origin.queue_name.as_deref(),
        Some(DIALOG_AIFARM_QUEUE_NAME)
    );
    assert_eq!(record.origin.job_id, Some(job_id));
    let outcome = record.outcome.as_ref().expect("ledger outcome on the run");
    assert_eq!(outcome.outcome, "sent");
    assert_eq!(record.totals.tool_calls, 1, "session tool hook recorded");
    assert!(record.ended_at.is_some());
    Ok(())
}

/// A materializer that injects a captured context artifact, to prove the turn
/// engine lifts it onto the run under the exact `job-{id}` run id.
struct ContextStubMaterializer;

impl crate::dialog_jobs::DialogInputMaterializer for ContextStubMaterializer {
    fn materialize_dialog_input<'a>(
        &'a self,
        params: &'a openplotva_taskman::DialogJobParams,
        now: OffsetDateTime,
    ) -> crate::dialog_jobs::DialogInputMaterializerFuture<'a> {
        Box::pin(async move {
            let mut input = crate::dialog_jobs::BasicDialogInputMaterializer
                .materialize_dialog_input(params, now)
                .await?;
            input.context_capture = Some(openplotva_dialog::TurnContextArtifact {
                memories: vec![openplotva_dialog::CapturedMemory {
                    card_id: 7,
                    salience: 0.8,
                    preview: "ada likes tea".to_owned(),
                    ..openplotva_dialog::CapturedMemory::default()
                }],
                persona: Some(openplotva_dialog::PersonaSnapshot {
                    mood: "playful".to_owned(),
                    ..openplotva_dialog::PersonaSnapshot::default()
                }),
                ..openplotva_dialog::TurnContextArtifact::default()
            });
            Ok(input)
        })
    }
}

#[tokio::test]
async fn turn_lifts_the_context_artifact_onto_the_run_detail() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что ты помнишь?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![Ok(step_text("помню чай"))]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let runs = crate::runtime_llm_runs::RuntimeLlmRunBuffer::new(8);
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    )
    .with_run_buffer(runs.clone());

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &ContextStubMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            llm_runs: Some(&runs),
            activity_pulse: None,
            ..session_options(now, &outcomes, &wiring)
        },
    )
    .await;
    assert!(report.sent_answer);

    let record = runs
        .list(&crate::runtime_llm_runs::RunListFilter::default(), now)
        .into_iter()
        .find(|record| record.run_id == format!("job-{job_id}"))
        .expect("dialog run present");
    let detail = record.detail_json();
    assert_eq!(detail["context"]["memories"][0]["card_id"], 7);
    assert_eq!(detail["context"]["memories"][0]["preview"], "ada likes tea");
    assert_eq!(detail["context"]["persona"]["mood"], "playful");
    Ok(())
}

/// The loop must forward `LoopOptions.llm_runs` into every tick — a dropped
/// option silently ships an empty LLM Dialogs section (same failure mode the
/// `session` option had before it got its loop test).
#[tokio::test]
async fn worker_loop_forwards_llm_runs_into_turns() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::now_utc();
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("сколько время?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![Ok(step_text("полдень"))]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let runs = crate::runtime_llm_runs::RuntimeLlmRunBuffer::new(8);
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    )
    .with_run_buffer(runs.clone());

    let stop = async {
        for _ in 0..500 {
            if record_status(&queue, job_id) == JobStatus::Completed {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };
    run_dialog_job_worker_with_materializer_and_history_every_until(
        &queue,
        &provider,
        &effects,
        DialogJobWorkerLoopOptions {
            materializer: &BasicDialogInputMaterializer,
            tool_history: &NoopDialogToolCallHistoryStore,
            routing_events: None,
            turn_outcomes: Some(&outcomes),
            queue_names: &DIALOG_JOB_WORKER_QUEUES,
            interval: std::time::Duration::from_millis(5),
            max_llm_job_attempts: 2,
            turn_budget_secs: DEFAULT_DIALOG_TURN_BUDGET_SECS,
            turn_max_queue_age_secs: DEFAULT_DIALOG_TURN_MAX_QUEUE_AGE_SECS,
            max_regenerations: DEFAULT_DIALOG_TURN_MAX_REGENERATIONS,
            terminal_signal: crate::dialog_turn::TurnSignalPolicy::default(),
            obligations: None,
            session: &wiring,
            llm_runs: Some(&runs),
            activity_pulse: None,
        },
        stop,
    )
    .await;

    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let records = runs.list(
        &crate::runtime_llm_runs::RunListFilter::default(),
        OffsetDateTime::now_utc(),
    );
    assert_eq!(
        records.len(),
        1,
        "the loop forwarded llm_runs into the turn"
    );
    assert_eq!(records[0].run_id, format!("job-{job_id}"));
    assert_eq!(
        records[0].status,
        crate::runtime_llm_runs::RunStatus::Completed
    );
    Ok(())
}

#[tokio::test]
async fn merged_and_deferred_turns_do_not_open_agent_runs() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let registry = Arc::new(crate::dialog_turn::DialogSessionRegistry::new());
    let key = crate::dialog_turn::SessionKey::new(42, Some(9));
    let crate::dialog_turn::ClaimOutcome::Claimed(_inbox) = registry.claim(key, 999, 7) else {
        panic!("pre-claimed");
    };
    let wiring = wiring_with_registry(Arc::clone(&registry));
    let runs = crate::runtime_llm_runs::RuntimeLlmRunBuffer::new(8);
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    )
    .with_run_buffer(runs.clone());

    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(params_from(7, "и ещё вот что"), now),
    );
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(params_from(8, "а можно мне тоже"), now),
    );
    let provider = StepProviderStub::with_steps(Vec::new());
    let effects = EffectsStub::default();
    for _ in 0..2 {
        process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
            &queue,
            &provider,
            &effects,
            &BasicDialogInputMaterializer,
            &NoopDialogToolCallHistoryStore,
            DialogJobProcessOptions {
                llm_runs: Some(&runs),
                activity_pulse: None,
                ..session_options(now, &outcomes, &wiring)
            },
        )
        .await;
    }

    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "deferred_after_session");
    assert_eq!(rows[1].outcome, "merged_into_session");
    assert!(
        runs.list(&crate::runtime_llm_runs::RunListFilter::default(), now)
            .is_empty(),
        "absorbed turns must not create run records"
    );
    Ok(())
}

#[tokio::test]
async fn session_sends_announcement_next_to_tool_calls() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что там в мире?"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "щас гляну",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "новости".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("вот что нашлось: всё спокойно")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer);
    assert_eq!(
        effects.intermediates(),
        vec![("щас гляну".to_owned(), 1, true)],
        "text next to tool calls goes to the chat as the first send (reply-to)"
    );
    assert_eq!(effects.sent().len(), 1, "final answer still lands");
    assert_eq!(
        effects.answer_options(),
        vec![DialogAnswerSendOptions {
            disable_link_preview: true,
        }],
        "the intermediate send is unchanged while the final records successful search"
    );
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "sent");
    assert_eq!(rows[0].sent_message_parts, Some(2));
    Ok(())
}

#[tokio::test]
async fn session_reuses_semantically_identical_tool_result_within_trigger()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что там в мире?"), now),
    );
    let search = |id: &str, query: &str| {
        step_tools(
            "",
            vec![(
                id,
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: query.to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )
    };
    let provider = StepProviderStub::with_steps(vec![
        Ok(search("call-1", "новости")),
        Ok(search("call-2", "  новости  ")),
        Ok(step_text("всё спокойно")),
    ]);
    let toolbox = Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox.clone(), None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(provider.calls(), 3);
    assert_eq!(
        toolbox
            .web_search_queries
            .lock()
            .expect("queries")
            .as_slice(),
        ["новости"],
        "the second semantic call must reuse the first result"
    );
    Ok(())
}

#[tokio::test]
async fn session_reuses_understand_media_result_by_resolved_unique_id() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("что на видео?"), now),
    );
    let media = |id: &str, file_id: &str| {
        step_tools(
            "",
            vec![(
                id,
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_UNDERSTAND_MEDIA.to_owned(),
                    file_id: file_id.to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )
    };
    let provider = StepProviderStub::with_steps(vec![
        Ok(media("call-1", "message\\_77\\_video\\_1")),
        Ok(media("call-2", "media-unique")),
        Ok(step_text("готово")),
    ]);
    let toolbox = Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox.clone(), None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(provider.calls(), 3);
    assert_eq!(
        toolbox
            .understand_media_refs
            .lock()
            .expect("media refs")
            .as_slice(),
        ["message\\_77\\_video\\_1"]
    );
    Ok(())
}

#[tokio::test]
async fn session_accepts_final_text_already_delivered_by_send_message() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("посмотри видео"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "send-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_SEND_MESSAGE.to_owned(),
                    text: "в видео говорят о контексте".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("в видео говорят о контексте")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert!(!report.failed, "{report:?}");
    assert_eq!(report.regenerations, 0);
    assert_eq!(provider.calls(), 2, "the tool result reached the model");
    assert_eq!(effects.intermediates().len(), 1);
    assert!(
        effects.sent().is_empty(),
        "the final must not be sent twice"
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let record = queue.record(job_id).expect("job record");
    assert!(
        record
            .events
            .iter()
            .any(|event| event.stage == crate::dialog_turn::SESSION_MESSAGE_SENT_STAGE)
    );
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "sent");
    assert_eq!(rows[0].sent_message_parts, Some(1));
    Ok(())
}

#[tokio::test]
async fn session_queued_draw_terminates_without_confirmation_text() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("нарисуй кота"), now),
    );
    let provider = StepProviderStub::with_steps(vec![Ok(step_tools(
        "",
        vec![(
            "call-1",
            openplotva_dialog::ToolStep {
                step: openplotva_dialog::STEP_DRAW_IMAGE.to_owned(),
                prompt: "кот".to_owned(),
                ..openplotva_dialog::ToolStep::default()
            },
        )],
    ))]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::with_queued_draw("77"));
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    // Side-effect terminal: exactly one LLM call, no post-factum status text.
    assert_eq!(provider.calls(), 1);
    assert!(effects.sent().is_empty());
    assert!(effects.intermediates().is_empty());
    assert!(!report.sent_answer);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "side_effect_delegated");
    assert_eq!(rows[0].side_effect_ticket_id, Some(77));
    Ok(())
}

#[tokio::test]
async fn session_failed_draw_feeds_back_and_loop_continues() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("нарисуй кота"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "call-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_DRAW_IMAGE.to_owned(),
                    prompt: "кот".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("рисовалка прилегла, потом попробуем")),
    ]);
    // Default stub: draw fails without a side effect.
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    assert_eq!(provider.calls(), 2, "the error came back to the model");
    let requests = provider.requests();
    let openplotva_dialog::SessionMessage::ToolResult { content, .. } = &requests[1].transcript[1]
    else {
        panic!("second request carries the tool result");
    };
    assert!(content.contains("draw_down"));
    assert_eq!(effects.sent()[0].1, "рисовалка прилегла, потом попробуем");
    Ok(())
}

#[tokio::test]
async fn session_send_message_tool_respects_cap_and_dedup() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("расскажи длинно"), now),
    );
    let send = |text: &str, id: &str| {
        step_tools(
            "",
            vec![(
                id,
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_SEND_MESSAGE.to_owned(),
                    text: text.to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )
    };
    let provider = StepProviderStub::with_steps(vec![
        Ok(send("часть один", "c1")),
        Ok(send("часть один", "c2")), // duplicate → cached success result
        Ok(send("часть два", "c3")),
        Ok(send("часть три", "c4")),
        Ok(send("часть четыре", "c5")),
        Ok(send("часть пять", "c6")), // over the cap of 4 → error result
        Ok(step_text("и финал")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer);
    let intermediates = effects.intermediates();
    assert_eq!(intermediates.len(), 4, "cap holds: {intermediates:?}");
    assert_eq!(intermediates[0], ("часть один".to_owned(), 1, true));
    assert!(
        intermediates[1..].iter().all(|(_, _, reply)| !reply),
        "only the first send replies to the trigger"
    );
    // The duplicate reuses the first result; the over-cap call remains an error.
    let requests = provider.requests();
    let transcript_json = format!("{:?}", requests.last().expect("final request").transcript);
    assert!(transcript_json.contains("reused_from_call_id"));
    assert!(transcript_json.contains("message_limit"));
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].sent_message_parts, Some(5));
    Ok(())
}

#[tokio::test]
async fn session_intermediate_without_final_answer_is_terminal_failure()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("посмотри видео"), now),
    );
    let send = || {
        step_tools(
            "",
            vec![(
                "send-1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_SEND_MESSAGE.to_owned(),
                    text: "сейчас посмотрю".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )
    };
    let provider = StepProviderStub::with_steps(vec![Ok(send()), Ok(send())]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let mut wiring = session_wiring(toolbox, None);
    wiring.max_iterations = 2;
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.failed, "{report:?}");
    assert!(
        !report.sent_answer,
        "intermediate text is not a final answer"
    );
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    let record = queue.record(job_id).expect("job record");
    assert!(
        record
            .events
            .iter()
            .any(|event| { event.stage == crate::dialog_turn::SESSION_INTERMEDIATE_QUEUED_STAGE })
    );
    assert!(
        !record
            .events
            .iter()
            .any(|event| { event.stage == crate::dialog_turn::SESSION_MESSAGE_SENT_STAGE })
    );
    Ok(())
}

#[tokio::test]
async fn session_react_to_message_validates_emoji_and_repeats() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("смешно же"), now),
    );
    let react = |emoji: &str, id: &str| {
        step_tools(
            "",
            vec![(
                id,
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_REACT_TO_MESSAGE.to_owned(),
                    emoji: emoji.to_owned(),
                    target_chat_id: -999, // engine clamps to the session chat
                    target_message_id: 100,
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )
    };
    let provider = StepProviderStub::with_steps(vec![
        Ok(react("🎨", "c1")), // not in the allowed list
        Ok(react("🤣", "c2")),
        Ok(react("🔥", "c3")), // same message again → already_reacted
        Ok(step_text("ну да")),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let reactor = Arc::new(ReactorStub::default());
    let wiring = session_wiring(
        toolbox,
        Some(Arc::clone(&reactor) as Arc<dyn crate::dialog_turn::SessionReactor>),
    );
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer);
    assert_eq!(
        reactor.reactions.lock().expect("reactions").clone(),
        vec![(42, 100, "🤣".to_owned())],
        "one reaction landed, clamped to the session chat"
    );
    let transcript_json = format!(
        "{:?}",
        provider.requests().last().expect("final").transcript
    );
    assert!(transcript_json.contains("emoji_not_allowed"));
    assert!(transcript_json.contains("already_reacted"));
    Ok(())
}

#[tokio::test]
async fn session_retry_semantics_depend_on_first_send() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

    // Before any send: a retryable step error requeues the job as today.
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("привет"), now),
    );
    let provider = StepProviderStub::with_steps(vec![Err("aifarm down".to_owned())]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );
    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;
    assert!(report.retry_requeued, "{report:?}");
    assert_eq!(record_status(&queue, job_id), JobStatus::Pending);

    // After an intermediate went out: the same error is terminal, never a
    // replay of a partially delivered session.
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("привет"), now),
    );
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "щас",
            vec![(
                "c1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "x".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Err("aifarm down".to_owned()),
    ]);
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );
    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;
    assert!(!report.retry_requeued);
    assert!(report.failed, "{report:?}");
    assert_eq!(record_status(&queue, job_id), JobStatus::Failed);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "terminal_failed");
    assert_eq!(rows[0].reason.as_deref(), Some("llm_failed_after_partial"));
    Ok(())
}

#[tokio::test]
async fn session_reentry_marker_skips_replay() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("привет"), now),
    );
    queue.append_job_event(
        job_id,
        TaskQueueJobEvent {
            stage: crate::dialog_turn::SESSION_MESSAGE_SENT_STAGE.to_owned(),
            ..TaskQueueJobEvent::default()
        },
        now,
    )?;
    let provider = StepProviderStub::with_steps(Vec::new());
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    let wiring = session_wiring(toolbox, None);
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.resent_skipped, "{report:?}");
    assert_eq!(provider.calls(), 0, "no LLM call on re-entry");
    assert!(effects.sent().is_empty());
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

fn wiring_with_registry(
    registry: Arc<crate::dialog_turn::DialogSessionRegistry>,
) -> crate::dialog_turn::SessionWorkerWiring {
    let toolbox: Arc<dyn openplotva_dialog::DialogToolbox> =
        Arc::new(SessionToolboxStub::default());
    crate::dialog_turn::SessionWorkerWiring {
        registry,
        ..session_wiring(toolbox, None)
    }
}

fn params_from(user_id: i64, text: &str) -> DialogJobParams {
    DialogJobParams {
        user_id,
        ..dialog_params(text)
    }
}

#[tokio::test]
async fn busy_session_absorbs_initiator_and_defers_third_party() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let registry = Arc::new(crate::dialog_turn::DialogSessionRegistry::new());
    let key = crate::dialog_turn::SessionKey::new(42, Some(9));
    // A running session owned by fake job 999, initiated by user 7.
    let crate::dialog_turn::ClaimOutcome::Claimed(inbox) = registry.claim(key, 999, 7) else {
        panic!("pre-claimed");
    };
    let wiring = wiring_with_registry(Arc::clone(&registry));
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    // Initiator message → merged, no LLM work, job completes.
    let queue = InMemoryTaskQueue::new();
    let merged_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(params_from(7, "и ещё вот что"), now),
    );
    let provider = StepProviderStub::with_steps(Vec::new());
    let effects = EffectsStub::default();
    let activity_effects = Arc::new(RecordingActivityEffects::new(
        crate::telegram_activity::TelegramActivityReport::Sent,
    ));
    let pulse = activity_pulse(Arc::clone(&activity_effects));
    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            activity_pulse: Some(&pulse),
            ..session_options(now, &outcomes, &wiring)
        },
    )
    .await;
    assert_eq!(report.merged_into_session, Some(999));
    assert_eq!(report.activity.sent, 0);
    assert_eq!(provider.calls(), 0);
    assert_eq!(record_status(&queue, merged_id), JobStatus::Completed);
    assert_eq!(inbox.drain_open().len(), 1, "message reached the inbox");

    // Third-party message → deferred, parked for release.
    let deferred_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(params_from(8, "а можно мне тоже"), now),
    );
    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        DialogJobProcessOptions {
            activity_pulse: Some(&pulse),
            ..session_options(now, &outcomes, &wiring)
        },
    )
    .await;
    assert_eq!(report.deferred_after_session, Some(999));
    assert_eq!(report.activity.sent, 0);
    assert_eq!(record_status(&queue, deferred_id), JobStatus::Completed);
    assert!(
        activity_effects.calls().is_empty(),
        "absorbed/deferred jobs must not start an independent activity pulse"
    );

    let release = registry.release(key, 999, true);
    assert_eq!(release.parked.len(), 1);
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "deferred_after_session");
    assert_eq!(rows[1].outcome, "merged_into_session");
    Ok(())
}

#[tokio::test]
async fn injected_message_reaches_the_next_iteration_and_leftovers_respawn()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let registry = Arc::new(crate::dialog_turn::DialogSessionRegistry::new());
    let key = crate::dialog_turn::SessionKey::new(42, Some(9));
    let wiring = wiring_with_registry(Arc::clone(&registry));
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(params_from(7, "посчитай мне"), now),
    );
    // Iteration 1: the initiator sends a follow-up while the tool runs — the
    // engine drains it before iteration 2. During the FINAL iteration another
    // follow-up arrives; it stays in the inbox and respawns after release.
    let inject_registry = Arc::clone(&registry);
    let provider = StepProviderStub::with_steps(vec![
        Ok(step_tools(
            "",
            vec![(
                "c1",
                openplotva_dialog::ToolStep {
                    step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                    query: "числа".to_owned(),
                    ..openplotva_dialog::ToolStep::default()
                },
            )],
        )),
        Ok(step_text("готово: 4")),
    ])
    .with_on_call(Box::new(move |call_index| {
        let text = if call_index == 1 {
            "кстати умножь на два"
        } else {
            "и ещё раз на три"
        };
        assert!(inject_registry.inject(
            key,
            job_id,
            crate::dialog_turn::InjectedMessage {
                params: params_from(7, text),
            },
        ));
    }));
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.sent_answer, "{report:?}");
    // The first injected message is the rematerialized current input and is
    // not repeated in the session transcript.
    let requests = provider.requests();
    let injected = requests[1]
        .transcript
        .iter()
        .filter_map(|entry| match entry {
            openplotva_dialog::SessionMessage::InjectedUser { rendered } => Some(rendered.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(injected.is_empty(), "{:?}", requests[1].transcript);
    assert_eq!(
        requests[1].input.message.text, "кстати умножь на два",
        "the newest injected message is fully rematerialized"
    );
    assert_eq!(
        effects.sent()[0].0,
        "кстати умножь на два",
        "the final answer targets the newest injected trigger"
    );
    assert_eq!(
        effects.answer_options(),
        vec![DialogAnswerSendOptions::default()],
        "absorbing a new user turn must reset successful search state"
    );
    // The second injected message was left in the inbox → a follow-up job.
    assert!(report.followup_respawned.is_some(), "{report:?}");
    assert_eq!(registry.active_count(), 0, "session released its key");
    let follow_up = queue
        .records()
        .into_iter()
        .find(|record| Some(record.id) == report.followup_respawned)
        .expect("follow-up job");
    assert_eq!(follow_up.status, JobStatus::Pending);
    Ok(())
}

#[tokio::test]
async fn injected_materialization_failure_respawns_consumed_message() -> Result<(), Box<dyn Error>>
{
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let registry = Arc::new(crate::dialog_turn::DialogSessionRegistry::new());
    let key = crate::dialog_turn::SessionKey::new(42, Some(9));
    let wiring = wiring_with_registry(Arc::clone(&registry));
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(params_from(7, "first"), now),
    );
    let inject_registry = Arc::clone(&registry);
    let provider = StepProviderStub::with_steps(vec![Ok(step_tools(
        "",
        vec![(
            "c1",
            openplotva_dialog::ToolStep {
                step: openplotva_dialog::STEP_WEB_SEARCH.to_owned(),
                query: "first".to_owned(),
                ..openplotva_dialog::ToolStep::default()
            },
        )],
    ))])
    .with_on_call(Box::new(move |_| {
        assert!(inject_registry.inject(
            key,
            job_id,
            crate::dialog_turn::InjectedMessage {
                params: params_from(7, "must survive"),
            },
        ));
    }));
    let effects = EffectsStub::default();
    let outcomes = crate::dialog_turn::DialogTurnObserver::new(
        crate::dialog_turn::RuntimeTurnOutcomeBuffer::new(8),
        None,
    );

    let report = process_dialog_job_once_in_queue_with_materializer_history_and_retry_at(
        &queue,
        &provider,
        &effects,
        &FailingInjectedMaterializer::default(),
        &NoopDialogToolCallHistoryStore,
        session_options(now, &outcomes, &wiring),
    )
    .await;

    assert!(report.failed, "{report:?}");
    let follow_up_id = report.followup_respawned.expect("follow-up job");
    let follow_up = queue
        .records()
        .into_iter()
        .find(|record| record.id == follow_up_id)
        .expect("follow-up record");
    assert_eq!(
        follow_up
            .job
            .data
            .dialog_data
            .expect("dialog params")
            .message_text,
        "must survive"
    );
    Ok(())
}
