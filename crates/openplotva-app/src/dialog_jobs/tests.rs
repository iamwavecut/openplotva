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
    ChatProvider, ChatProviderError, ChatProviderFuture, ContentBlockedError,
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
            .any(|event| event.stage == crate::dialog_turn::ANSWER_SENT_STAGE),
        "successful send must append the answer_sent marker"
    );
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
            session: None,
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
        .send_dialog_answer(&dialog_params("hello"), "pong")
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
        .send_dialog_answer(&dialog_params("hello"), "<h2>pong</h2>")
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
async fn dialog_worker_records_go_fallback_event_before_completion() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("hello"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        provider: "genkit".to_owned(),
        fallback_from: "aifarm".to_owned(),
        fallback_error: "aifarm provider provider_unavailable: status 503".to_owned(),
        answer: "pong".to_owned(),
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();

    let report = process_dialog_job_once_in_queue_at(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        now,
    )
    .await;

    assert!(report.recorded_dialog_fallback_event);
    assert_eq!(report.dialog_fallback_event_error, None);
    assert!(report.completed);
    let record = queue.record(job_id).expect("job record");
    let event = record
        .events
        .iter()
        .find(|event| event.stage == "dialog_fallback")
        .expect("dialog fallback event");
    assert_eq!(event.at, "2026-05-19T12:30:00Z");
    assert_eq!(event.level, "warn");
    assert_eq!(event.stage, "dialog_fallback");
    assert_eq!(event.provider, "aifarm");
    assert_eq!(
        event.message,
        "primary dialog backend failed, trying fallback"
    );
    assert_eq!(
        event.error,
        "aifarm provider provider_unavailable: status 503"
    );
    assert_eq!(event.data["fallback_provider"], "genkit");
    assert_eq!(event.data["fallback_reason"], "provider_unavailable");
    Ok(())
}

#[tokio::test]
async fn dialog_worker_persists_filtered_tool_call_history_before_answer()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("нарисуй кота"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        provider: "stub".to_owned(),
        answer: "готово".to_owned(),
        tool_calls: vec![
            ToolCall {
                name: "draw_image".to_owned(),
                r#ref: "req-1".to_owned(),
                input: Some(serde_json::json!({"prompt":"cat"})),
                output: Some(serde_json::json!({"status":"queued"})),
                ..ToolCall::default()
            },
            ToolCall {
                name: "chat_history_summary".to_owned(),
                ..ToolCall::default()
            },
            ToolCall {
                name: "final_response".to_owned(),
                ..ToolCall::default()
            },
        ],
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();
    let history = ToolHistoryStub::default();

    let report = process_dialog_job_once_in_queue_with_materializer_and_history_at(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &history,
        now,
    )
    .await;

    assert_eq!(report.job_id, Some(job_id));
    assert!(report.persisted_tool_call_history);
    assert_eq!(history.calls().len(), 1);
    assert_eq!(history.calls()[0].0, 42);
    assert_eq!(history.calls()[0].1, 100);
    assert_eq!(history.calls()[0].2.len(), 1);
    assert_eq!(history.calls()[0].2[0].name, "draw_image");
    assert!(report.sent_answer);
    assert!(report.completed);
    assert!(!report.failed);
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn dialog_worker_completes_delegated_queued_song_without_sending_text()
-> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("напиши песню"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        provider: "stub".to_owned(),
        tool_calls: vec![ToolCall {
            name: "generate_song".to_owned(),
            r#ref: "generate_song-1".to_owned(),
            output: Some(serde_json::json!({
                "status": "queued",
                "side_effect": {
                    "kind": "music_generation_job",
                    "ticket_id": "9001",
                    "state": "queued"
                }
            })),
            ..ToolCall::default()
        }],
        ..DialogOutput::default()
    });
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
            session: None,
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
    assert!(effects.sent().is_empty(), "no confirmation text is sent");
    assert_eq!(record_status(&queue, job_id), JobStatus::Completed);
    use openplotva_server::{RuntimeTurnOutcomeInspector, RuntimeTurnOutcomesFilter};
    let recorded = outcomes
        .buffer()
        .turn_outcomes(RuntimeTurnOutcomesFilter {
            limit: 10,
            ..RuntimeTurnOutcomesFilter::default()
        })
        .expect("turn outcomes");
    assert_eq!(recorded.len(), 1);
    assert_eq!(
        recorded[0].outcome,
        crate::dialog_turn::TURN_OUTCOME_SIDE_EFFECT_DELEGATED
    );
    assert_eq!(
        recorded[0].side_effect_ticket_id,
        Some(9001),
        "delegated turn records the generation ticket"
    );
    Ok(())
}

#[tokio::test]
async fn dialog_worker_keeps_tool_call_history_failures_nonfatal() -> Result<(), Box<dyn Error>> {
    let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
    let queue = InMemoryTaskQueue::new();
    let job_id = queue.assign(
        DIALOG_AIFARM_QUEUE_NAME,
        new_dialog_job_at(dialog_params("нарисуй кота"), now),
    );
    let provider = ProviderStub::returning(DialogOutput {
        answer: "готово".to_owned(),
        tool_calls: vec![ToolCall {
            name: "draw_image".to_owned(),
            ..ToolCall::default()
        }],
        ..DialogOutput::default()
    });
    let effects = EffectsStub::default();
    let history = ToolHistoryStub::failing();

    let report = process_dialog_job_once_in_queue_with_materializer_and_history_at(
        &queue,
        DIALOG_AIFARM_QUEUE_NAME,
        &provider,
        &effects,
        &BasicDialogInputMaterializer,
        &history,
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
            session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
        session: None,
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
    assert!(first.answer_empty_all_sources);
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
        session: None,
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
            session: None,
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
            session: None,
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
            session: None,
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
    let history = vec![HistoryMessage {
        message_id: params.message_id,
        kind: MESSAGE_KIND_TEXT.to_owned(),
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

impl ChatProvider for ProviderStub {
    fn provider_name(&self) -> &str {
        "stub"
    }

    fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("provider state");
            state.inputs.push(input);
            match &state.result {
                Ok(output) => Ok(output.clone()),
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

    fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("provider state");
            let round = state.inputs.len();
            state.inputs.push(input);
            let index = round.min(state.outputs.len() - 1);
            Ok(state.outputs[index].clone())
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

    fn run_dialog<'a>(&'a self, _input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move {
            *self.calls.lock().expect("slow provider calls") += 1;
            tokio::time::sleep(self.delay).await;
            Ok(self.output.clone())
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

    fn run_dialog<'a>(&'a self, _input: DialogInput) -> ChatProviderFuture<'a> {
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
    intermediates: Vec<(String, u32, bool)>,
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
        params: &'a DialogJobParams,
        answer: &'a str,
    ) -> DialogJobEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut state = self.state.lock().expect("effects state");
            if state.fail {
                return Err(io::Error::other("send down"));
            }
            state
                .sent
                .push((params.message_text.clone(), answer.to_owned()));
            Ok(())
        })
    }

    fn send_dialog_intermediate<'a>(
        &'a self,
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
            input
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
        let accepted = effects.send_dialog_answer(&params, &answer).await.is_ok();
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

// ---- Dialog session engine (DIALOG_AGENT_LOOP_ENABLED) ----

struct StepProviderStub {
    steps: Mutex<VecDeque<Result<openplotva_dialog::ChatStepOutput, String>>>,
    requests: Mutex<Vec<openplotva_dialog::ChatStepRequest>>,
    retryable_errors: bool,
}

impl StepProviderStub {
    fn with_steps(steps: Vec<Result<openplotva_dialog::ChatStepOutput, String>>) -> Self {
        Self {
            steps: Mutex::new(steps.into_iter().collect()),
            requests: Mutex::new(Vec::new()),
            retryable_errors: true,
        }
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

    fn run_dialog<'a>(&'a self, _input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move { panic!("session tests must go through the step seam") })
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
            self.requests.lock().expect("step requests").push(request);
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
    draw_requests: Mutex<Vec<openplotva_dialog::DrawRequest>>,
    draw_result: Mutex<Option<openplotva_dialog::ToolResult>>,
}

impl SessionToolboxStub {
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
}

impl openplotva_dialog::DialogToolbox for SessionToolboxStub {
    fn web_search<'a>(&'a self, query: String) -> openplotva_dialog::ToolboxFuture<'a> {
        self.web_search_queries.lock().expect("queries").push(query);
        Box::pin(async {
            Ok(openplotva_dialog::ToolResult {
                status: "ok".to_owned(),
                message: "results: solstice is on June 21".to_owned(),
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
        enabled_chats: Vec::new(),
        max_iterations: 8,
        max_messages: 4,
        tool_extension_secs: 60,
        hard_cap_secs: 300,
        max_draws: 1,
        max_songs: 1,
    }
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
        session: Some(wiring),
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
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].outcome, "sent");
    assert_eq!(rows[0].sent_message_parts, Some(2));
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
        Ok(send("часть один", "c2")), // duplicate → error result
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
    // The duplicate and the over-cap calls came back as error results.
    let requests = provider.requests();
    let transcript_json = format!("{:?}", requests.last().expect("final request").transcript);
    assert!(transcript_json.contains("duplicate_message"));
    assert!(transcript_json.contains("message_limit"));
    let rows = ledger_rows(&outcomes);
    assert_eq!(rows[0].sent_message_parts, Some(5));
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
