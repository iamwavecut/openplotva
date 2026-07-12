//! Durable Telegram outbox transport worker.
//!
//! The worker deliberately separates the durable request-start marker from the
//! network call. A create operation that loses its worker after that marker is
//! never replayed automatically: lease recovery moves it to `ambiguous` and an
//! idempotent terminal reaction is queued instead.

use std::{
    convert::Infallible,
    fmt,
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use carapax::{api::ExecuteError, types::EditMessageResult};
use futures_util::{StreamExt, stream};
use openplotva_storage::{
    ClaimedTelegramOutboxOperation, PostgresHistoryStore, PostgresTelegramOutboxStore,
    TelegramDeliveryPolicy, TelegramOutboxBatchInput, TelegramOutboxPartInput,
    TelegramReceiptHistoryEntry,
};
use openplotva_telegram::{
    OutboundSendErrorClass, RichApiClient, RichApiError, TelegramClient,
    TelegramOutboundExecuteError, TelegramOutboundMethod, TelegramOutboundMethodKind,
    TelegramOutboundResponse, build_message_reaction_method, execute_telegram_method_with_rich,
    replay_outbound_method, replay_outbound_method_without_reply, snapshot_outbound_method,
};
use serde_json::{Value, json};
use sqlx::Row;
use time::OffsetDateTime;

use crate::task_queue::SharedTaskQueueRuntime;
use openplotva_updates::{build_fetcher_message_context, build_history_text_entry};

pub const TELEGRAM_OUTBOX_WORKER_DEFAULT_CLAIM_LIMIT: usize = 32;
pub const TELEGRAM_OUTBOX_WORKER_DEFAULT_MAX_ATTEMPTS: i32 = 8;
pub const TELEGRAM_OUTBOX_WORKER_DEFAULT_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(20);
pub const TELEGRAM_OUTBOX_WORKER_DEFAULT_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(200);
pub const TELEGRAM_OUTBOX_WORKER_DEFAULT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);

const OUTBOX_MAINTENANCE_LIMIT: usize = 1_000;
const DIALOG_DELIVERY_RECONCILE_LIMIT: i64 = 250;
const AMBIGUITY_REACTION_EMOJI: &str = "🤔";
const AMBIGUITY_REACTION_BATCH_PREFIX: &str = "tgamb:v1:";

pub type TelegramOutboxTransportFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<TelegramOutboundResponse, TelegramOutboundExecuteError>>
            + Send
            + 'a,
    >,
>;

/// Injectable network boundary used by the durable worker.
pub trait TelegramOutboxTransport: Send + Sync {
    fn execute<'a>(&'a self, method: TelegramOutboundMethod) -> TelegramOutboxTransportFuture<'a>;
}

/// Real Telegram transport, including the rich-message endpoint.
#[derive(Clone)]
pub struct TelegramApiOutboxTransport {
    telegram: TelegramClient,
    rich: RichApiClient,
}

impl TelegramApiOutboxTransport {
    #[must_use]
    pub fn new(telegram: TelegramClient, rich: RichApiClient) -> Self {
        Self { telegram, rich }
    }
}

impl TelegramOutboxTransport for TelegramApiOutboxTransport {
    fn execute<'a>(&'a self, method: TelegramOutboundMethod) -> TelegramOutboxTransportFuture<'a> {
        Box::pin(async move {
            execute_telegram_method_with_rich(&self.telegram, &self.rich, method).await
        })
    }
}

pub type TelegramOutboxJobFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boundary that resolves the taskman job linked to a complete outbox batch.
pub trait TelegramOutboxJobResolver: Send + Sync {
    type Error: fmt::Display + Send + Sync + 'static;

    fn complete_job<'a>(&'a self, job_id: i64) -> TelegramOutboxJobFuture<'a, Self::Error>;

    fn fail_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> TelegramOutboxJobFuture<'a, Self::Error>;
}

impl TelegramOutboxJobResolver for SharedTaskQueueRuntime {
    type Error = String;

    fn complete_job<'a>(&'a self, job_id: i64) -> TelegramOutboxJobFuture<'a, Self::Error> {
        Box::pin(async move {
            self.queue()
                .complete(job_id, OffsetDateTime::now_utc())
                .map_err(|error| error.to_string())?;
            self.durability_barrier(job_id).await
        })
    }

    fn fail_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> TelegramOutboxJobFuture<'a, Self::Error> {
        Box::pin(async move {
            self.queue()
                .fail(job_id, error, OffsetDateTime::now_utc())
                .map_err(|error| error.to_string())?;
            self.durability_barrier(job_id).await
        })
    }
}

#[derive(Clone)]
pub struct RunAwareTelegramOutboxJobResolver {
    jobs: SharedTaskQueueRuntime,
    runs: crate::runtime_llm_runs::RuntimeLlmRunBuffer,
}

impl RunAwareTelegramOutboxJobResolver {
    #[must_use]
    pub fn new(
        jobs: SharedTaskQueueRuntime,
        runs: crate::runtime_llm_runs::RuntimeLlmRunBuffer,
    ) -> Self {
        Self { jobs, runs }
    }
}

impl TelegramOutboxJobResolver for RunAwareTelegramOutboxJobResolver {
    type Error = String;

    fn complete_job<'a>(&'a self, job_id: i64) -> TelegramOutboxJobFuture<'a, Self::Error> {
        Box::pin(async move {
            self.jobs.complete_job(job_id).await?;
            self.runs.mark_job_delivered(job_id);
            Ok(())
        })
    }

    fn fail_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> TelegramOutboxJobFuture<'a, Self::Error> {
        self.jobs.fail_job(job_id, error)
    }
}

/// Resolver for deployments that do not attach outbox rows to taskman jobs.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTelegramOutboxJobResolver;

impl TelegramOutboxJobResolver for NoopTelegramOutboxJobResolver {
    type Error = Infallible;

    fn complete_job<'a>(&'a self, _job_id: i64) -> TelegramOutboxJobFuture<'a, Self::Error> {
        Box::pin(async { Ok(()) })
    }

    fn fail_job<'a>(
        &'a self,
        _job_id: i64,
        _error: &'a str,
    ) -> TelegramOutboxJobFuture<'a, Self::Error> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug)]
pub struct TelegramOutboxWorkerConfig {
    pub claim_limit: usize,
    pub max_attempts: i32,
    pub lease_renew_interval: Duration,
    pub idle_poll_interval: Duration,
    pub maintenance_interval: Duration,
}

impl Default for TelegramOutboxWorkerConfig {
    fn default() -> Self {
        Self {
            claim_limit: TELEGRAM_OUTBOX_WORKER_DEFAULT_CLAIM_LIMIT,
            max_attempts: TELEGRAM_OUTBOX_WORKER_DEFAULT_MAX_ATTEMPTS,
            lease_renew_interval: TELEGRAM_OUTBOX_WORKER_DEFAULT_LEASE_RENEW_INTERVAL,
            idle_poll_interval: TELEGRAM_OUTBOX_WORKER_DEFAULT_IDLE_POLL_INTERVAL,
            maintenance_interval: TELEGRAM_OUTBOX_WORKER_DEFAULT_MAINTENANCE_INTERVAL,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramOutboxWorkerReport {
    pub maintenance_passes: u64,
    pub recovered_retry_wait: u64,
    pub recovered_ambiguous: u64,
    pub recovered_expired: u64,
    pub expired: u64,
    pub claimed: u64,
    pub delivered: u64,
    pub retried: u64,
    pub ambiguous: u64,
    pub dead_lettered: u64,
    pub lease_lost: u64,
    pub ambiguity_reactions_queued: u64,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub delivery_outcomes_reconciled: u64,
    pub errors: u64,
    pub last_error: Option<String>,
}

/// Run a crash-recovering outbox worker until shutdown is requested.
///
/// Shutdown is observed between operations. Once `request_started_at` is
/// durable, the current network request is allowed to reach a durable outcome;
/// cancelling it at that point would manufacture an avoidable ambiguity.
#[allow(clippy::too_many_arguments)]
pub async fn run_telegram_outbox_worker_until<Transport, Jobs>(
    store: PostgresTelegramOutboxStore,
    history: PostgresHistoryStore,
    transport: Transport,
    jobs: Jobs,
    worker_id: &str,
    expected_bot_id: i64,
    config: TelegramOutboxWorkerConfig,
    stop: impl Future<Output = ()>,
) -> TelegramOutboxWorkerReport
where
    Transport: TelegramOutboxTransport,
    Jobs: TelegramOutboxJobResolver,
{
    let mut report = TelegramOutboxWorkerReport::default();
    let mut last_maintenance = None;
    tokio::pin!(stop);

    loop {
        if last_maintenance
            .is_none_or(|last: Instant| last.elapsed() >= config.maintenance_interval)
        {
            run_outbox_maintenance(&store, &jobs, &mut report).await;
            last_maintenance = Some(Instant::now());
        }

        if stop_requested_now(&mut stop).await {
            break;
        }
        // Claiming commits fenced leases in PostgreSQL. Await the mutation so
        // shutdown cannot discard successfully claimed rows and hide them
        // until lease expiry.
        let claimed = store.claim_operations(worker_id, config.claim_limit).await;
        let operations = match claimed {
            Ok(operations) => operations,
            Err(error) => {
                record_worker_error(&mut report, format!("claim Telegram outbox: {error}"));
                tokio::select! {
                    () = &mut stop => break,
                    () = tokio::time::sleep(config.idle_poll_interval) => {}
                }
                continue;
            }
        };

        if operations.is_empty() {
            tokio::select! {
                () = &mut stop => break,
                () = tokio::time::sleep(config.idle_poll_interval) => {}
            }
            continue;
        }

        report.claimed = report
            .claimed
            .saturating_add(u64::try_from(operations.len()).unwrap_or(u64::MAX));
        let operation_reports = stream::iter(operations)
            .map(|operation| {
                let store = &store;
                let transport = &transport;
                let jobs = &jobs;
                let config = &config;
                let history = history.clone();
                async move {
                    let mut operation_report = TelegramOutboxWorkerReport::default();
                    if operation.bot_id != expected_bot_id {
                        let available_at = OffsetDateTime::now_utc() + time::Duration::seconds(30);
                        match store
                            .retry_operation(
                                operation.id,
                                operation.lease_token,
                                available_at,
                                "wrong_bot_worker",
                                "outbox operation was claimed by a worker for another bot",
                                None,
                                None,
                            )
                            .await
                        {
                            Ok(true) => {
                                operation_report.retried =
                                    operation_report.retried.saturating_add(1);
                            }
                            Ok(false) => {
                                operation_report.lease_lost =
                                    operation_report.lease_lost.saturating_add(1);
                            }
                            Err(error) => record_worker_error(
                                &mut operation_report,
                                format!("release wrong-bot outbox operation: {error}"),
                            ),
                        }
                    } else {
                        process_claimed_operation(
                            store,
                            &history,
                            transport,
                            jobs,
                            operation,
                            config,
                            &mut operation_report,
                        )
                        .await;
                    }
                    operation_report
                }
            })
            .buffer_unordered(config.claim_limit.max(1))
            .collect::<Vec<_>>()
            .await;
        for operation_report in operation_reports {
            merge_worker_report(&mut report, operation_report);
        }
    }

    report
}

async fn process_claimed_operation<Transport, Jobs>(
    store: &PostgresTelegramOutboxStore,
    history: &PostgresHistoryStore,
    transport: &Transport,
    jobs: &Jobs,
    operation: ClaimedTelegramOutboxOperation,
    config: &TelegramOutboxWorkerConfig,
    report: &mut TelegramOutboxWorkerReport,
) where
    Transport: TelegramOutboxTransport,
    Jobs: TelegramOutboxJobResolver,
{
    let method = match replay_claimed_operation(&operation) {
        Ok(method) => method,
        Err(error) => {
            match store
                .dead_letter_operation(
                    operation.id,
                    operation.lease_token,
                    "payload_not_replayable",
                    &error,
                    None,
                )
                .await
            {
                Ok(true) => {
                    report.dead_lettered = report.dead_lettered.saturating_add(1);
                    resolve_batch(store, jobs, &operation.batch_id, report).await;
                }
                Ok(false) => report.lease_lost = report.lease_lost.saturating_add(1),
                Err(store_error) => record_worker_error(
                    report,
                    format!("dead-letter unreplayable outbox operation: {store_error}"),
                ),
            }
            return;
        }
    };

    let response_kind = response_kind_name(method.response_kind());
    let reply_missing_replacement = reply_missing_replacement_payload(&method, &operation.payload);
    match store
        .mark_request_started(operation.id, operation.lease_token)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            report.lease_lost = report.lease_lost.saturating_add(1);
            return;
        }
        Err(error) => {
            record_worker_error(report, format!("mark Telegram request started: {error}"));
            return;
        }
    }

    let response = execute_with_lease_renewal(
        store,
        transport,
        &operation,
        method,
        config.lease_renew_interval,
        report,
    )
    .await;

    match response {
        LeasedExecution::LeaseLost => {
            report.lease_lost = report.lease_lost.saturating_add(1);
        }
        LeasedExecution::Response(Ok(response)) => {
            let history_entries = telegram_response_history_entries(&response, operation.bot_id);
            let receipt = telegram_response_receipt(response);
            match store
                .mark_delivered_with_history(
                    operation.id,
                    operation.lease_token,
                    response_kind,
                    &receipt.message_ids,
                    &receipt.value,
                    &history_entries,
                )
                .await
            {
                Ok(true) => {
                    let mut invalidated_chats = Vec::new();
                    for entry in &history_entries {
                        if invalidated_chats.contains(&entry.chat_id) {
                            continue;
                        }
                        invalidated_chats.push(entry.chat_id);
                        if let Err(error) =
                            history.invalidate_chat_history_cache(entry.chat_id).await
                        {
                            tracing::warn!(
                                %error,
                                chat_id = entry.chat_id,
                                outbox_id = operation.id,
                                "Telegram receipt history committed but cache invalidation failed"
                            );
                        }
                    }
                    report.delivered = report.delivered.saturating_add(1);
                    resolve_batch(store, jobs, &operation.batch_id, report).await;
                }
                Ok(false) => report.lease_lost = report.lease_lost.saturating_add(1),
                Err(error) => {
                    record_worker_error(report, format!("record Telegram receipt: {error}"));
                }
            }
        }
        LeasedExecution::Response(Err(error)) => {
            handle_send_error(
                store,
                jobs,
                &operation,
                error,
                reply_missing_replacement.as_ref(),
                config.max_attempts,
                report,
            )
            .await;
        }
    }
}

enum LeasedExecution {
    Response(Result<TelegramOutboundResponse, TelegramOutboundExecuteError>),
    LeaseLost,
}

async fn execute_with_lease_renewal<Transport>(
    store: &PostgresTelegramOutboxStore,
    transport: &Transport,
    operation: &ClaimedTelegramOutboxOperation,
    method: TelegramOutboundMethod,
    renew_interval: Duration,
    report: &mut TelegramOutboxWorkerReport,
) -> LeasedExecution
where
    Transport: TelegramOutboxTransport,
{
    let response = transport.execute(method);
    tokio::pin!(response);
    let mut ticker = tokio::time::interval(renew_interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            response = &mut response => return LeasedExecution::Response(response),
            _ = ticker.tick() => {
                match store
                    .renew_operation_lease(operation.id, operation.lease_token)
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => return LeasedExecution::LeaseLost,
                    Err(error) => record_worker_error(
                        report,
                        format!("renew Telegram outbox lease: {error}"),
                    ),
                }
            }
        }
    }
}

async fn handle_send_error<Jobs>(
    store: &PostgresTelegramOutboxStore,
    jobs: &Jobs,
    operation: &ClaimedTelegramOutboxOperation,
    error: TelegramOutboundExecuteError,
    reply_missing_replacement: Option<&Value>,
    max_attempts: i32,
    report: &mut TelegramOutboxWorkerReport,
) where
    Jobs: TelegramOutboxJobResolver,
{
    let http_status = telegram_error_status(&error);
    let error_text = error.diagnostic_message();

    if error.is_reply_missing()
        && let Some(replacement) = reply_missing_replacement
    {
        let transitioned = store
            .retry_operation(
                operation.id,
                operation.lease_token,
                OffsetDateTime::now_utc(),
                "reply_missing_without_reply",
                &error_text,
                http_status,
                Some(replacement),
            )
            .await;
        record_retry_transition(transitioned, report, "persist reply-missing retry");
        return;
    }

    let class = error.classification();
    let retryable = matches!(
        class,
        OutboundSendErrorClass::RetryableRateLimited { .. }
            | OutboundSendErrorClass::RetryableTransient
    ) || (matches!(
        operation.delivery_policy.as_str(),
        "target_idempotent" | "ephemeral"
    ) && matches!(class, OutboundSendErrorClass::TerminalOther));

    if retryable && operation.attempt < max_attempts.max(1) {
        let available_at = retry_available_at(operation, class);
        let transitioned = store
            .retry_operation(
                operation.id,
                operation.lease_token,
                available_at,
                class.as_str(),
                &error_text,
                http_status,
                None,
            )
            .await;
        record_retry_transition(transitioned, report, "persist Telegram outbox retry");
        return;
    }

    if matches!(operation.delivery_policy.as_str(), "create" | "financial")
        && !matches!(
            class,
            OutboundSendErrorClass::RetryableRateLimited { .. }
                | OutboundSendErrorClass::RetryableTransient
        )
    {
        if operation.delivery_policy == "create" {
            match enqueue_ambiguity_reaction(store, operation).await {
                Ok(true) => {
                    report.ambiguity_reactions_queued =
                        report.ambiguity_reactions_queued.saturating_add(1);
                }
                Ok(false) => {}
                Err(enqueue_error) => {
                    record_worker_error(
                        report,
                        format!("enqueue ambiguous-create reaction: {enqueue_error}"),
                    );
                    return;
                }
            }
        }
        match store
            .mark_ambiguous(
                operation.id,
                operation.lease_token,
                class.as_str(),
                &error_text,
            )
            .await
        {
            Ok(true) => {
                report.ambiguous = report.ambiguous.saturating_add(1);
                resolve_batch(store, jobs, &operation.batch_id, report).await;
            }
            Ok(false) => report.lease_lost = report.lease_lost.saturating_add(1),
            Err(store_error) => record_worker_error(
                report,
                format!("mark Telegram create ambiguous: {store_error}"),
            ),
        }
        return;
    }

    match store
        .dead_letter_operation(
            operation.id,
            operation.lease_token,
            class.as_str(),
            &error_text,
            http_status,
        )
        .await
    {
        Ok(true) => {
            report.dead_lettered = report.dead_lettered.saturating_add(1);
            resolve_batch(store, jobs, &operation.batch_id, report).await;
        }
        Ok(false) => report.lease_lost = report.lease_lost.saturating_add(1),
        Err(store_error) => record_worker_error(
            report,
            format!("dead-letter Telegram outbox operation: {store_error}"),
        ),
    }
}

fn record_retry_transition(
    result: Result<bool, openplotva_storage::StorageError>,
    report: &mut TelegramOutboxWorkerReport,
    context: &str,
) {
    match result {
        Ok(true) => report.retried = report.retried.saturating_add(1),
        Ok(false) => report.lease_lost = report.lease_lost.saturating_add(1),
        Err(error) => record_worker_error(report, format!("{context}: {error}")),
    }
}

fn retry_available_at(
    operation: &ClaimedTelegramOutboxOperation,
    class: OutboundSendErrorClass,
) -> OffsetDateTime {
    let delay = match class {
        OutboundSendErrorClass::RetryableRateLimited { retry_after_secs } => {
            Duration::from_secs(retry_after_secs.max(1))
        }
        _ => {
            let exponent =
                u32::try_from(operation.attempt.saturating_sub(1).clamp(0, 6)).unwrap_or(0);
            let base = Duration::from_millis(500_u64.saturating_mul(1_u64 << exponent));
            let jitter = deterministic_retry_jitter_ms(&operation.operation_id);
            base.saturating_add(Duration::from_millis(jitter))
        }
    };
    OffsetDateTime::now_utc()
        + time::Duration::try_from(delay).unwrap_or(time::Duration::seconds(60))
}

fn deterministic_retry_jitter_ms(operation_id: &str) -> u64 {
    operation_id.bytes().fold(0_u64, |hash, byte| {
        hash.wrapping_mul(16_777_619) ^ u64::from(byte)
    }) % 250
}

struct TelegramReceipt {
    message_ids: Vec<i64>,
    value: Value,
}

fn telegram_response_history_entries(
    response: &TelegramOutboundResponse,
    bot_id: i64,
) -> Vec<TelegramReceiptHistoryEntry> {
    let messages: Vec<_> = match response {
        TelegramOutboundResponse::Message(message) => vec![message.as_ref()],
        TelegramOutboundResponse::Messages(messages) => messages.iter().collect(),
        TelegramOutboundResponse::EditMessage(_)
        | TelegramOutboundResponse::Boolean(_)
        | TelegramOutboundResponse::SentGuestMessage(_)
        | TelegramOutboundResponse::String(_) => Vec::new(),
    };
    messages
        .into_iter()
        .filter_map(|message| {
            let context = build_fetcher_message_context(message);
            build_history_text_entry(message, &context.original_text, context.meta, bot_id)
        })
        .map(|entry| TelegramReceiptHistoryEntry {
            bucket_day: entry.occurred_at.date(),
            chat_id: entry.chat_id,
            thread_id: entry.thread_id,
            message_id: entry.message_id,
            entry_id: entry.entry_id,
            kind: entry.kind,
            role: entry.role,
            occurred_at: entry.occurred_at,
            sender_id: entry.sender_id,
            payload: serde_json::from_slice(&entry.payload)
                .expect("history text entry payload is serialized JSON"),
        })
        .collect()
}

fn telegram_response_receipt(response: TelegramOutboundResponse) -> TelegramReceipt {
    match response {
        TelegramOutboundResponse::Message(message) => TelegramReceipt {
            message_ids: vec![message.id],
            value: json!({"kind": "message", "response": message}),
        },
        TelegramOutboundResponse::Messages(messages) => TelegramReceipt {
            message_ids: messages.iter().map(|message| message.id).collect(),
            value: json!({"kind": "messages", "response": messages}),
        },
        TelegramOutboundResponse::EditMessage(result) => {
            let message_ids = match &result {
                EditMessageResult::Message(message) => vec![message.id],
                EditMessageResult::Bool(_) => Vec::new(),
            };
            TelegramReceipt {
                message_ids,
                value: json!({"kind": "edit_message", "response": result}),
            }
        }
        TelegramOutboundResponse::Boolean(value) => TelegramReceipt {
            message_ids: Vec::new(),
            value: json!({"kind": "boolean", "response": value}),
        },
        TelegramOutboundResponse::SentGuestMessage(message) => TelegramReceipt {
            message_ids: Vec::new(),
            value: json!({"kind": "sent_guest_message", "response": message}),
        },
        TelegramOutboundResponse::String(value) => TelegramReceipt {
            message_ids: Vec::new(),
            value: json!({"kind": "string", "response": value}),
        },
    }
}

fn response_kind_name(kind: openplotva_telegram::TelegramOutboundResponseKind) -> &'static str {
    match kind {
        openplotva_telegram::TelegramOutboundResponseKind::Message => "message",
        openplotva_telegram::TelegramOutboundResponseKind::Messages => "messages",
        openplotva_telegram::TelegramOutboundResponseKind::EditMessage => "edit_message",
        openplotva_telegram::TelegramOutboundResponseKind::Boolean => "boolean",
        openplotva_telegram::TelegramOutboundResponseKind::SentGuestMessage => "sent_guest_message",
        openplotva_telegram::TelegramOutboundResponseKind::String => "string",
    }
}

fn telegram_error_status(error: &TelegramOutboundExecuteError) -> Option<i32> {
    match error {
        TelegramOutboundExecuteError::Telegram(ExecuteError::Response(response)) => response
            .error_code()
            .and_then(|code| i32::try_from(code).ok()),
        TelegramOutboundExecuteError::Rich(RichApiError::Api { code, .. }) => {
            i32::try_from(*code).ok()
        }
        TelegramOutboundExecuteError::Telegram(
            ExecuteError::Http(_) | ExecuteError::Payload(_),
        )
        | TelegramOutboundExecuteError::Rich(RichApiError::Http(_) | RichApiError::Decode(_)) => {
            None
        }
    }
}

fn replay_claimed_operation(
    operation: &ClaimedTelegramOutboxOperation,
) -> Result<TelegramOutboundMethod, String> {
    if operation.payload_version != 1 {
        return Err(format!(
            "unsupported Telegram outbox payload version {}",
            operation.payload_version
        ));
    }
    let kind = telegram_method_kind_from_storage(&operation.method_kind)
        .ok_or_else(|| format!("unsupported Telegram method kind {}", operation.method_kind))?;
    let payload = payload_with_blob(operation)?;
    let bytes = serde_json::to_vec(&payload)
        .map_err(|error| format!("serialize Telegram outbox payload: {error}"))?;
    replay_outbound_method(kind, &bytes).ok_or_else(|| {
        format!(
            "Telegram payload cannot replay as {}",
            operation.method_kind
        )
    })
}

/// Decode both Bot API names and the existing debug-style discriminator.
#[must_use]
pub fn telegram_method_kind_from_storage(value: &str) -> Option<TelegramOutboundMethodKind> {
    match value {
        "sendMessage" | "SendMessage" => Some(TelegramOutboundMethodKind::SendMessage),
        "sendRichMessage" | "SendRichMessage" => Some(TelegramOutboundMethodKind::SendRichMessage),
        "sendSticker" | "SendSticker" => Some(TelegramOutboundMethodKind::SendSticker),
        "sendPhoto" | "SendPhoto" => Some(TelegramOutboundMethodKind::SendPhoto),
        "sendAudio" | "SendAudio" => Some(TelegramOutboundMethodKind::SendAudio),
        "sendMediaGroup" | "SendMediaGroup" => Some(TelegramOutboundMethodKind::SendMediaGroup),
        "sendChatAction" | "SendChatAction" => Some(TelegramOutboundMethodKind::SendChatAction),
        "answerCallbackQuery" | "AnswerCallbackQuery" => {
            Some(TelegramOutboundMethodKind::AnswerCallbackQuery)
        }
        "answerInlineQuery" | "AnswerInlineQuery" => {
            Some(TelegramOutboundMethodKind::AnswerInlineQuery)
        }
        "answerGuestQuery" | "AnswerGuestQuery" => {
            Some(TelegramOutboundMethodKind::AnswerGuestQuery)
        }
        "answerPreCheckoutQuery" | "AnswerPreCheckoutQuery" => {
            Some(TelegramOutboundMethodKind::AnswerPreCheckoutQuery)
        }
        "createInvoiceLink" | "CreateInvoiceLink" => {
            Some(TelegramOutboundMethodKind::CreateInvoiceLink)
        }
        "refundStarPayment" | "RefundStarPayment" => {
            Some(TelegramOutboundMethodKind::RefundStarPayment)
        }
        "editUserStarSubscription" | "EditUserStarSubscription" => {
            Some(TelegramOutboundMethodKind::EditUserStarSubscription)
        }
        "editMessageText" | "EditMessageText" => Some(TelegramOutboundMethodKind::EditMessageText),
        "editMessageCaption" | "EditMessageCaption" => {
            Some(TelegramOutboundMethodKind::EditMessageCaption)
        }
        "editMessageReplyMarkup" | "EditMessageReplyMarkup" => {
            Some(TelegramOutboundMethodKind::EditMessageReplyMarkup)
        }
        "editMessageMedia" | "EditMessageMedia" => {
            Some(TelegramOutboundMethodKind::EditMessageMedia)
        }
        "deleteMessage" | "DeleteMessage" => Some(TelegramOutboundMethodKind::DeleteMessage),
        "setMessageReaction" | "SetMessageReaction" => {
            Some(TelegramOutboundMethodKind::SetMessageReaction)
        }
        _ => None,
    }
}

fn payload_with_blob(operation: &ClaimedTelegramOutboxOperation) -> Result<Value, String> {
    let Some(blob) = operation.blob.as_ref() else {
        return Ok(operation.payload.clone());
    };
    let mut payload = operation.payload.clone();
    let file_name = blob.file_name.clone().unwrap_or_else(|| {
        format!(
            "{}.bin",
            hex::encode(&blob.sha256[..blob.sha256.len().min(8)])
        )
    });
    let upload = json!({
        "Name": file_name,
        "Bytes": BASE64_STANDARD.encode(&blob.bytes),
    });
    if let Some(pointer) = blob.metadata.get("payload_pointer").and_then(Value::as_str) {
        let target = payload
            .pointer_mut(pointer)
            .ok_or_else(|| format!("outbox blob payload pointer does not exist: {pointer}"))?;
        *target = upload;
        return Ok(payload);
    }

    let object = payload
        .as_object_mut()
        .ok_or_else(|| "outbox payload with a blob must be a JSON object".to_owned())?;
    let field = match telegram_method_kind_from_storage(&operation.method_kind) {
        Some(TelegramOutboundMethodKind::SendPhoto) => {
            existing_field(object, &["File", "photo"]).unwrap_or("photo")
        }
        Some(TelegramOutboundMethodKind::SendAudio) => {
            existing_field(object, &["File", "audio"]).unwrap_or("audio")
        }
        Some(TelegramOutboundMethodKind::SendSticker) => {
            existing_field(object, &["File", "sticker"]).unwrap_or("sticker")
        }
        _ => {
            return Err(
                "outbox blob requires metadata.payload_pointer for this Telegram method".to_owned(),
            );
        }
    };
    object.insert(field.to_owned(), upload);
    Ok(payload)
}

fn existing_field<'a>(
    object: &serde_json::Map<String, Value>,
    names: &[&'a str],
) -> Option<&'a str> {
    names
        .iter()
        .copied()
        .find(|name| object.contains_key(*name))
}

fn reply_missing_replacement_payload(
    method: &TelegramOutboundMethod,
    current_payload: &Value,
) -> Option<Value> {
    if let Some(replacement) = replay_outbound_method_without_reply(method)
        && let Some((_, bytes)) = snapshot_outbound_method(&replacement)
        && let Ok(replacement) = serde_json::from_slice::<Value>(&bytes)
        && replacement != *current_payload
    {
        return Some(replacement);
    }

    let mut replacement = current_payload.clone();
    let object = replacement.as_object_mut()?;
    let mut changed = object.remove("ReplyParameters").is_some();
    changed |= object.remove("reply_parameters").is_some();
    changed |= object.remove("ReplyToMessageID").is_some();
    changed |= object.remove("reply_to_message_id").is_some();
    if let Some(options) = object.get_mut("options").and_then(Value::as_object_mut) {
        changed |= options.remove("reply_to_message_id").is_some();
        changed |= options.remove("reply_parameters").is_some();
        if options.contains_key("allow_sending_without_reply") {
            options.insert("allow_sending_without_reply".to_owned(), Value::Bool(false));
        }
    }
    changed.then_some(replacement)
}

async fn enqueue_ambiguity_reaction(
    store: &PostgresTelegramOutboxStore,
    operation: &ClaimedTelegramOutboxOperation,
) -> Result<bool, String> {
    let (Some(chat_id), Some(message_id)) = (operation.chat_id, operation.trigger_message_id)
    else {
        return Ok(false);
    };
    enqueue_ambiguity_reaction_fields(
        store,
        &operation.operation_id,
        operation.bot_id,
        chat_id,
        operation.thread_id,
        &operation.ordering_key,
        operation.causation_update_id,
        message_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_ambiguity_reaction_fields(
    store: &PostgresTelegramOutboxStore,
    source_operation_id: &str,
    bot_id: i64,
    chat_id: i64,
    thread_id: Option<i32>,
    ordering_key: &str,
    causation_update_id: Option<i64>,
    trigger_message_id: i64,
) -> Result<bool, String> {
    let method =
        build_message_reaction_method(chat_id, trigger_message_id, AMBIGUITY_REACTION_EMOJI);
    let (_, bytes) = snapshot_outbound_method(&method)
        .ok_or_else(|| "setMessageReaction is not snapshot-replayable".to_owned())?;
    let payload: Value = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode setMessageReaction snapshot: {error}"))?;
    let batch_id = format!("{AMBIGUITY_REACTION_BATCH_PREFIX}{source_operation_id}");
    let queued = store
        .enqueue_batch(&TelegramOutboxBatchInput {
            batch_id,
            bot_id,
            chat_id: Some(chat_id),
            thread_id,
            ordering_key: ordering_key.to_owned(),
            causation_update_id,
            dialog_job_id: None,
            trigger_message_id: Some(trigger_message_id),
            delivery_policy: TelegramDeliveryPolicy::TargetIdempotent,
            protected: true,
            priority: i32::MAX,
            parts: vec![TelegramOutboxPartInput {
                method_kind: method.method_name().to_owned(),
                payload_version: 1,
                payload,
                blob: None,
                available_at: OffsetDateTime::now_utc(),
                expires_at: None,
            }],
        })
        .await
        .map_err(|error| error.to_string())?;
    Ok(queued.parts.first().is_some_and(|part| part.inserted))
}

async fn run_outbox_maintenance<Jobs>(
    store: &PostgresTelegramOutboxStore,
    jobs: &Jobs,
    report: &mut TelegramOutboxWorkerReport,
) where
    Jobs: TelegramOutboxJobResolver,
{
    report.maintenance_passes = report.maintenance_passes.saturating_add(1);
    loop {
        match store.recover_expired_leases(OUTBOX_MAINTENANCE_LIMIT).await {
            Ok(recovered) => {
                report.recovered_retry_wait = report
                    .recovered_retry_wait
                    .saturating_add(recovered.retry_wait);
                report.recovered_ambiguous = report
                    .recovered_ambiguous
                    .saturating_add(recovered.ambiguous);
                report.recovered_expired =
                    report.recovered_expired.saturating_add(recovered.expired);
                let total = recovered
                    .retry_wait
                    .saturating_add(recovered.ambiguous)
                    .saturating_add(recovered.expired);
                if total < OUTBOX_MAINTENANCE_LIMIT as u64 {
                    break;
                }
            }
            Err(error) => {
                record_worker_error(report, format!("recover Telegram outbox leases: {error}"));
                break;
            }
        }
    }

    loop {
        match store.expire_due_operations(OUTBOX_MAINTENANCE_LIMIT).await {
            Ok(expired) => {
                report.expired = report.expired.saturating_add(expired);
                if expired < OUTBOX_MAINTENANCE_LIMIT as u64 {
                    break;
                }
            }
            Err(error) => {
                record_worker_error(report, format!("expire Telegram outbox rows: {error}"));
                break;
            }
        }
    }

    reconcile_ambiguity_reactions(store, report).await;
    reconcile_dialog_turn_delivery_states(store, report).await;
    reconcile_resolved_taskman_batches(store, jobs, report).await;
}

async fn reconcile_ambiguity_reactions(
    store: &PostgresTelegramOutboxStore,
    report: &mut TelegramOutboxWorkerReport,
) {
    let rows = match sqlx::query(
        "SELECT operation_id, bot_id, chat_id, thread_id, ordering_key, \
                causation_update_id, trigger_message_id \
         FROM telegram_outbox AS source \
         WHERE source.state = 'ambiguous' \
           AND source.delivery_policy = 'create' \
           AND source.chat_id IS NOT NULL \
           AND source.trigger_message_id IS NOT NULL \
           AND NOT EXISTS ( \
               SELECT 1 FROM telegram_outbox AS reaction \
               WHERE reaction.batch_id = $1 || source.operation_id \
           ) \
         ORDER BY source.id \
         LIMIT $2",
    )
    .bind(AMBIGUITY_REACTION_BATCH_PREFIX)
    .bind(i64::try_from(OUTBOX_MAINTENANCE_LIMIT).unwrap_or(1_000))
    .fetch_all(store.pool())
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            record_worker_error(
                report,
                format!("scan missing ambiguous-create reactions: {error}"),
            );
            return;
        }
    };

    for row in rows {
        let result = enqueue_ambiguity_reaction_fields(
            store,
            &row.get::<String, _>("operation_id"),
            row.get("bot_id"),
            row.get("chat_id"),
            row.get("thread_id"),
            &row.get::<String, _>("ordering_key"),
            row.get("causation_update_id"),
            row.get("trigger_message_id"),
        )
        .await;
        match result {
            Ok(true) => {
                report.ambiguity_reactions_queued =
                    report.ambiguity_reactions_queued.saturating_add(1);
            }
            Ok(false) => {}
            Err(error) => record_worker_error(
                report,
                format!("reconcile ambiguous-create reaction: {error}"),
            ),
        }
    }
}

async fn reconcile_dialog_turn_delivery_states(
    store: &PostgresTelegramOutboxStore,
    report: &mut TelegramOutboxWorkerReport,
) {
    let reconciled = sqlx::query(
        "WITH candidates AS ( \
             SELECT outcome.id, outcome.job_id, \
                    count(*) AS operation_count, \
                    count(*) FILTER (WHERE operation.state = 'delivered') = count(*) \
                        AS all_delivered, \
                    count(*) FILTER (WHERE operation.state = 'delivered') > 0 \
                        AS any_delivered, \
                    bool_or(operation.state = 'ambiguous') AS any_ambiguous, \
                    max(operation.confirmed_at) FILTER (WHERE operation.state = 'delivered') \
                        AS delivered_at, \
                    string_agg(DISTINCT operation.state, ',' ORDER BY operation.state) AS states \
             FROM dialog_turn_outcomes AS outcome \
             JOIN telegram_outbox AS operation ON operation.dialog_job_id = outcome.job_id \
             WHERE outcome.outcome = 'queued_for_delivery' \
               AND outcome.delivery_state IN ('legacy_unverified', 'queued') \
               AND outcome.created_at >= statement_timestamp() - interval '1 day' \
               AND NOT EXISTS ( \
                   SELECT 1 FROM telegram_outbox AS unresolved \
                   WHERE unresolved.dialog_job_id = outcome.job_id \
                     AND unresolved.state IN ('pending', 'leased', 'retry_wait') \
               ) \
             GROUP BY outcome.id, outcome.job_id \
             ORDER BY outcome.id DESC \
             LIMIT $1 \
         ) \
         UPDATE dialog_turn_outcomes AS outcome \
         SET delivery_state = CASE \
                 WHEN candidates.all_delivered THEN 'delivered' \
                 WHEN candidates.any_ambiguous THEN 'ambiguous' \
                 WHEN candidates.any_delivered THEN 'partial' \
                 ELSE 'dead_letter' \
             END, \
             outbox_operation_ids = ARRAY( \
                 SELECT operation.operation_id \
                 FROM telegram_outbox AS operation \
                 WHERE operation.dialog_job_id = candidates.job_id \
                 ORDER BY operation.batch_id, operation.part_index \
             ), \
             telegram_message_ids = ARRAY( \
                 SELECT DISTINCT message.message_id \
                 FROM telegram_outbox AS operation \
                 CROSS JOIN LATERAL unnest(operation.telegram_message_ids) AS message(message_id) \
                 WHERE operation.dialog_job_id = candidates.job_id \
                 ORDER BY message.message_id \
             ), \
             sent_message_parts = CASE \
                 WHEN candidates.all_delivered \
                 THEN LEAST(candidates.operation_count, 2147483647)::integer \
                 ELSE outcome.sent_message_parts \
             END, \
             delivered_at = CASE \
                 WHEN candidates.all_delivered THEN candidates.delivered_at \
                 ELSE outcome.delivered_at \
             END, \
             delivery_error_class = CASE \
                 WHEN candidates.all_delivered THEN NULL \
                 WHEN candidates.any_ambiguous THEN 'ambiguous' \
                 WHEN candidates.any_delivered THEN 'partial' \
                 ELSE 'dead_letter' \
             END, \
             delivery_error = CASE \
                 WHEN candidates.all_delivered THEN NULL \
                 ELSE 'Telegram outbox reached terminal state(s): ' || candidates.states \
             END \
         FROM candidates \
         WHERE outcome.id = candidates.id",
    )
    .bind(DIALOG_DELIVERY_RECONCILE_LIMIT)
    .execute(store.pool())
    .await;
    match reconciled {
        Ok(result) => {
            report.delivery_outcomes_reconciled = report
                .delivery_outcomes_reconciled
                .saturating_add(result.rows_affected());
        }
        Err(error) => record_worker_error(
            report,
            format!("reconcile dialog turn delivery states: {error}"),
        ),
    }
}

async fn reconcile_resolved_taskman_batches<Jobs>(
    store: &PostgresTelegramOutboxStore,
    jobs: &Jobs,
    report: &mut TelegramOutboxWorkerReport,
) where
    Jobs: TelegramOutboxJobResolver,
{
    let batch_ids = match sqlx::query_scalar::<_, String>(
        "SELECT DISTINCT operation.batch_id \
         FROM telegram_outbox AS operation \
         JOIN taskman_jobs AS job ON job.id = operation.dialog_job_id \
         WHERE job.status = 'waiting_delivery' \
           AND NOT EXISTS ( \
               SELECT 1 FROM telegram_outbox AS unresolved \
               WHERE unresolved.batch_id = operation.batch_id \
                 AND unresolved.state IN ('pending', 'leased', 'retry_wait') \
           ) \
         ORDER BY operation.batch_id \
         LIMIT $1",
    )
    .bind(i64::try_from(OUTBOX_MAINTENANCE_LIMIT).unwrap_or(1_000))
    .fetch_all(store.pool())
    .await
    {
        Ok(batch_ids) => batch_ids,
        Err(error) => {
            record_worker_error(report, format!("scan resolved outbox batches: {error}"));
            return;
        }
    };
    for batch_id in batch_ids {
        resolve_batch(store, jobs, &batch_id, report).await;
    }
}

enum BatchResolution {
    Pending,
    Delivered { job_id: Option<i64> },
    Terminal { job_id: Option<i64>, error: String },
}

async fn batch_resolution(
    store: &PostgresTelegramOutboxStore,
    batch_id: &str,
) -> Result<BatchResolution, sqlx::Error> {
    let row = sqlx::query(
        "SELECT max(dialog_job_id) AS dialog_job_id, \
                count(*) FILTER (WHERE state = 'delivered') = count(*) AS delivered, \
                count(*) FILTER (WHERE state IN ('pending', 'leased', 'retry_wait')) > 0 \
                    AS unresolved, \
                string_agg(DISTINCT state, ',' ORDER BY state) AS states \
         FROM telegram_outbox \
         WHERE batch_id = $1",
    )
    .bind(batch_id)
    .fetch_one(store.pool())
    .await?;
    let job_id: Option<i64> = row.try_get("dialog_job_id")?;
    let delivered: bool = row.try_get("delivered")?;
    let unresolved: bool = row.try_get("unresolved")?;
    let states: Option<String> = row.try_get("states")?;
    if unresolved || states.is_none() {
        return Ok(BatchResolution::Pending);
    }
    if delivered {
        return Ok(BatchResolution::Delivered { job_id });
    }
    Ok(BatchResolution::Terminal {
        job_id,
        error: format!(
            "Telegram outbox batch {batch_id} reached terminal state(s): {}",
            states.unwrap_or_else(|| "unknown".to_owned())
        ),
    })
}

async fn resolve_batch<Jobs>(
    store: &PostgresTelegramOutboxStore,
    jobs: &Jobs,
    batch_id: &str,
    report: &mut TelegramOutboxWorkerReport,
) where
    Jobs: TelegramOutboxJobResolver,
{
    match batch_resolution(store, batch_id).await {
        Ok(BatchResolution::Pending) => {}
        Ok(BatchResolution::Delivered { job_id: None })
        | Ok(BatchResolution::Terminal { job_id: None, .. }) => {}
        Ok(BatchResolution::Delivered {
            job_id: Some(job_id),
        }) => match jobs.complete_job(job_id).await {
            Ok(()) => report.jobs_completed = report.jobs_completed.saturating_add(1),
            Err(error) => record_worker_error(
                report,
                format!("complete taskman job after Telegram delivery: {error}"),
            ),
        },
        Ok(BatchResolution::Terminal {
            job_id: Some(job_id),
            error,
        }) => match jobs.fail_job(job_id, &error).await {
            Ok(()) => report.jobs_failed = report.jobs_failed.saturating_add(1),
            Err(job_error) => record_worker_error(
                report,
                format!("fail taskman job after terminal Telegram delivery: {job_error}"),
            ),
        },
        Err(error) => {
            record_worker_error(report, format!("resolve Telegram outbox batch: {error}"));
        }
    }
}

fn record_worker_error(report: &mut TelegramOutboxWorkerReport, error: String) {
    report.errors = report.errors.saturating_add(1);
    report.last_error = Some(error.clone());
    tracing::warn!(%error, "Telegram outbox worker error");
}

async fn stop_requested_now<Stop>(stop: &mut Pin<&mut Stop>) -> bool
where
    Stop: Future<Output = ()>,
{
    tokio::select! {
        biased;
        () = stop.as_mut() => true,
        () = std::future::ready(()) => false,
    }
}

fn merge_worker_report(
    aggregate: &mut TelegramOutboxWorkerReport,
    report: TelegramOutboxWorkerReport,
) {
    aggregate.maintenance_passes = aggregate
        .maintenance_passes
        .saturating_add(report.maintenance_passes);
    aggregate.recovered_retry_wait = aggregate
        .recovered_retry_wait
        .saturating_add(report.recovered_retry_wait);
    aggregate.recovered_ambiguous = aggregate
        .recovered_ambiguous
        .saturating_add(report.recovered_ambiguous);
    aggregate.recovered_expired = aggregate
        .recovered_expired
        .saturating_add(report.recovered_expired);
    aggregate.expired = aggregate.expired.saturating_add(report.expired);
    aggregate.claimed = aggregate.claimed.saturating_add(report.claimed);
    aggregate.delivered = aggregate.delivered.saturating_add(report.delivered);
    aggregate.retried = aggregate.retried.saturating_add(report.retried);
    aggregate.ambiguous = aggregate.ambiguous.saturating_add(report.ambiguous);
    aggregate.dead_lettered = aggregate.dead_lettered.saturating_add(report.dead_lettered);
    aggregate.lease_lost = aggregate.lease_lost.saturating_add(report.lease_lost);
    aggregate.ambiguity_reactions_queued = aggregate
        .ambiguity_reactions_queued
        .saturating_add(report.ambiguity_reactions_queued);
    aggregate.jobs_completed = aggregate
        .jobs_completed
        .saturating_add(report.jobs_completed);
    aggregate.jobs_failed = aggregate.jobs_failed.saturating_add(report.jobs_failed);
    aggregate.delivery_outcomes_reconciled = aggregate
        .delivery_outcomes_reconciled
        .saturating_add(report.delivery_outcomes_reconciled);
    aggregate.errors = aggregate.errors.saturating_add(report.errors);
    if report.last_error.is_some() {
        aggregate.last_error = report.last_error;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_kind_parser_accepts_storage_and_bot_api_labels() {
        assert_eq!(
            telegram_method_kind_from_storage("sendMessage"),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        assert_eq!(
            telegram_method_kind_from_storage("SetMessageReaction"),
            Some(TelegramOutboundMethodKind::SetMessageReaction)
        );
        assert_eq!(telegram_method_kind_from_storage("unknown"), None);
    }

    #[test]
    fn reply_missing_replacement_removes_reply_parameters() {
        let method = TelegramOutboundMethod::from(
            carapax::types::SendMessage::new(42, "hello")
                .with_reply_parameters(carapax::types::ReplyParameters::new(7).with_chat_id(42)),
        );
        let (_, bytes) = snapshot_outbound_method(&method).expect("snapshot");
        let original: Value = serde_json::from_slice(&bytes).expect("payload");

        let replacement =
            reply_missing_replacement_payload(&method, &original).expect("replacement payload");

        assert_ne!(replacement, original);
        assert!(replay_outbound_method_without_reply(&method).is_some());
        assert!(
            replacement.get("ReplyParameters").is_none()
                && replacement.get("reply_parameters").is_none()
        );
    }

    #[test]
    fn reply_missing_replacement_handles_form_method_payloads() {
        let method = TelegramOutboundMethod::from(carapax::types::SendPhoto::new(
            42,
            carapax::types::InputFile::file_id("photo-id"),
        ));
        let original = json!({
            "chat_id": 42,
            "photo": "photo-id",
            "reply_parameters": {"message_id": 7}
        });

        let replacement =
            reply_missing_replacement_payload(&method, &original).expect("replacement payload");

        assert!(replacement.get("reply_parameters").is_none());
        assert_eq!(replacement["photo"], "photo-id");
    }

    #[test]
    fn retry_jitter_is_stable_and_bounded() {
        let first = deterministic_retry_jitter_ms("tgop:v1:abc");
        assert_eq!(first, deterministic_retry_jitter_ms("tgop:v1:abc"));
        assert!(first < 250);
    }

    #[test]
    fn message_receipt_preserves_real_message_id() {
        let message: carapax::types::Message = serde_json::from_value(json!({
            "message_id": 91,
            "date": 1,
            "chat": {"id": 42, "type": "private", "first_name": "Test"}
        }))
        .expect("message");

        let receipt =
            telegram_response_receipt(TelegramOutboundResponse::Message(Box::new(message)));

        assert_eq!(receipt.message_ids, vec![91]);
        assert_eq!(receipt.value["kind"], "message");
    }

    #[test]
    fn message_receipt_builds_model_history_from_confirmed_telegram_message() {
        let message: carapax::types::Message = serde_json::from_value(json!({
            "message_id": 92,
            "from": {"id": 7, "is_bot": true, "first_name": "Plotva"},
            "date": 1,
            "chat": {"id": 42, "type": "private", "first_name": "Test"},
            "text": "confirmed text"
        }))
        .expect("message");
        let response = TelegramOutboundResponse::Message(Box::new(message));

        let entries = telegram_response_history_entries(&response, 7);

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].chat_id, 42);
        assert_eq!(entries[0].message_id, 92);
        assert_eq!(entries[0].entry_id, "msg:92");
        assert_eq!(entries[0].role, "model");
        assert_eq!(entries[0].payload["text"], "confirmed text");
    }
}
