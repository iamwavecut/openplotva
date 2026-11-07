use crate::{
    RuntimeWorkerGroup, RuntimeWorkerPhase, RuntimeWorkerStops, maintenance_api, permissions,
    rate_limits, routing_admin_reports, virtual_messages, wait_for_runtime_stop,
};
use openplotva_storage::{
    PostgresChatSettingsStore, PostgresHistoryStore, RedisEphemeralMessageStore,
    RedisRateLimitStore,
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use std::{future::Future, time::Duration};

use openplotva_server::{
    RuntimeDispatchFailureData, RuntimeDispatcherFailureInspector, RuntimeDispatcherInspector,
    RuntimeDispatcherStatsData,
};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Default)]
pub(crate) struct RuntimeDispatcherInspectorHandle {
    queue: Arc<Mutex<Option<Arc<openplotva_telegram::DispatcherQueue>>>>,
}

impl RuntimeDispatcherInspectorHandle {
    pub(crate) fn set_queue(&self, queue: Arc<openplotva_telegram::DispatcherQueue>) {
        *self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(queue);
    }
}

impl RuntimeDispatcherInspector for RuntimeDispatcherInspectorHandle {
    fn stats(&self) -> RuntimeDispatcherStatsData {
        let Some(queue) = self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return RuntimeDispatcherStatsData::default();
        };
        let stats = queue.stats();
        RuntimeDispatcherStatsData {
            regular_queue_size: stats.regular_queue_size.min(i32::MAX as usize) as i32,
            immediate_queue_size: stats.immediate_queue_size.min(i32::MAX as usize) as i32,
            processed_total: stats.processed_total,
            deduped_total: stats.deduped_total,
            oldest_regular_age_ms: stats.oldest_regular_age.as_millis().min(i32::MAX as u128)
                as i32,
            oldest_immediate_age_ms: stats.oldest_immediate_age.as_millis().min(i32::MAX as u128)
                as i32,
        }
    }
}

/// Ring class recorded when a chat-level persisted Telegram rate limit drops the item.
pub const DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED: &str = "chat_rate_limited";
/// Ring class recorded when a queued item carries no sendable method (programmer error).
pub const DISPATCH_FAILURE_CLASS_MISSING_METHOD: &str = "missing_method";

/// Maximum retained terminal-send-failure records.
const DISPATCH_FAILURE_RING_CAP: usize = 1024;

/// One terminal outbound dispatcher send failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchFailureRecord {
    pub at: OffsetDateTime,
    pub virtual_id: String,
    pub chat_id: i64,
    pub method_kind: String,
    pub error: String,
    /// `OutboundSendErrorClass::as_str` value or a dispatcher-local skip class.
    pub class: &'static str,
    /// Whether the failed item was protected (dialog answer / watcher notice).
    pub protected: bool,
    /// Trigger message id recovered from the reply-scoped `r{message_id}`
    /// debounce key, when the producer set one.
    pub reply_to_message_id: Option<i64>,
}

/// Bounded in-memory ring of terminal outbound send failures. Producers are
/// the dispatcher workers; consumers are the delivery-obligation watcher
/// (protected failures, via the sequence cursor) and the runtime GraphQL
/// `dispatcherSendFailures` query (snapshots, so watcher consumption never
/// hides records from diagnostics).
#[derive(Default)]
pub struct DispatchFailureRing {
    state: Mutex<DispatchFailureRingState>,
}

#[derive(Default)]
struct DispatchFailureRingState {
    records: VecDeque<(u64, DispatchFailureRecord)>,
    next_seq: u64,
}

impl DispatchFailureRing {
    pub fn record(&self, record: DispatchFailureRecord) {
        let mut state = self.lock();
        let seq = state.next_seq;
        state.next_seq += 1;
        state.records.push_back((seq, record));
        while state.records.len() > DISPATCH_FAILURE_RING_CAP {
            state.records.pop_front();
        }
    }

    /// Most recent failures, newest first.
    pub fn snapshot(&self, limit: usize) -> Vec<DispatchFailureRecord> {
        let state = self.lock();
        state
            .records
            .iter()
            .rev()
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect()
    }

    /// Protected failures recorded after `cursor`, oldest first, plus the new
    /// cursor value to persist for the next scan.
    pub fn protected_failures_since(&self, cursor: u64) -> (Vec<DispatchFailureRecord>, u64) {
        let state = self.lock();
        let next_cursor = state.next_seq;
        let records = state
            .records
            .iter()
            .filter(|(seq, record)| *seq >= cursor && record.protected)
            .map(|(_, record)| record.clone())
            .collect();
        (records, next_cursor)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DispatchFailureRingState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl RuntimeDispatcherFailureInspector for DispatchFailureRing {
    fn send_failures(&self, limit: i32) -> Vec<RuntimeDispatchFailureData> {
        self.snapshot(usize::try_from(limit).unwrap_or(0))
            .into_iter()
            .map(|record| RuntimeDispatchFailureData {
                at: record
                    .at
                    .format(&Rfc3339)
                    .unwrap_or_else(|_| record.at.to_string()),
                virtual_id: record.virtual_id,
                chat_id: record.chat_id,
                method_kind: record.method_kind,
                error: record.error,
                class: record.class.to_owned(),
                protected: record.protected,
                reply_to_message_id: record.reply_to_message_id,
            })
            .collect()
    }
}

/// Recover the trigger message id from a reply-scoped `r{message_id}` debounce
/// key, which producers append as the fourth `:`-separated fingerprint
/// segment. Chosen over threading `reply_to_message_id` through the dispatcher
/// metadata chain because the key already encodes it losslessly and the
/// metadata shape is persisted (Redis snapshot/restore) — least invasive.
pub fn reply_message_id_from_fingerprint_key(fingerprint_key: &str) -> Option<i64> {
    let debounce_key = fingerprint_key.splitn(4, ':').nth(3)?;
    debounce_key.strip_prefix('r')?.parse().ok()
}

pub(crate) fn build_dispatcher_worker_group(
    inputs: DispatcherWorkerGroupInputs,
    stops: &RuntimeWorkerStops,
) -> RuntimeWorkerGroup {
    let DispatcherWorkerGroupInputs {
        history,
        telegram,
        rich,
        ephemeral,
        rate_limits,
        permissions,
        queue,
        limiters,
        failure_ring,
        routing_admin_reports,
    } = inputs;
    let mut workers = RuntimeWorkerGroup::new(RuntimeWorkerPhase::Outbound);

    let immediate_history = history.clone();
    let immediate_telegram = telegram.clone();
    let immediate_rich = rich.clone();
    let immediate_ephemeral = ephemeral.clone();
    let immediate_rate_limits = Arc::clone(&rate_limits);
    let immediate_permissions = Arc::clone(&permissions);
    let immediate_queue = Arc::clone(&queue);
    let immediate_failure_ring = Arc::clone(&failure_ring);
    let immediate_routing_admin_reports = routing_admin_reports.clone();
    let immediate_stop = stops.subscribe(RuntimeWorkerPhase::Outbound);
    workers.register(
        "dispatcher-immediate",
        tokio::spawn(async move {
            let outcome = immediate_queue
                .run_immediate_worker_until(wait_for_runtime_stop(immediate_stop), |item| {
                    send_dispatcher_work_item(
                        immediate_history.clone(),
                        immediate_telegram.clone(),
                        immediate_rich.clone(),
                        immediate_ephemeral.clone(),
                        Arc::clone(&immediate_rate_limits),
                        Arc::clone(&immediate_permissions),
                        Some(Arc::clone(&immediate_failure_ring)),
                        Some(immediate_routing_admin_reports.clone()),
                        item,
                    )
                })
                .await;

            tracing::info!(?outcome, "outbound immediate dispatcher worker stopped");
        }),
    );

    let regular_queue = queue;
    let regular_limiters = Arc::clone(&limiters);
    let regular_stop = stops.subscribe(RuntimeWorkerPhase::Outbound);
    workers.register(
        "dispatcher-regular",
        tokio::spawn(async move {
            let outcome = regular_queue
                .run_regular_worker_until(
                    &regular_limiters,
                    wait_for_runtime_stop(regular_stop),
                    |item| {
                        send_dispatcher_work_item(
                            history.clone(),
                            telegram.clone(),
                            rich.clone(),
                            ephemeral.clone(),
                            Arc::clone(&rate_limits),
                            Arc::clone(&permissions),
                            Some(Arc::clone(&failure_ring)),
                            Some(routing_admin_reports.clone()),
                            item,
                        )
                    },
                )
                .await;

            tracing::info!(?outcome, "outbound regular dispatcher worker stopped");
        }),
    );

    let cleanup_stop = stops.subscribe(RuntimeWorkerPhase::Outbound);
    workers.register(
        "dispatcher-limiter-cleanup",
        tokio::spawn(async move {
            let outcome = openplotva_telegram::run_limiter_cleanup_until(
                &limiters,
                openplotva_telegram::DispatcherRuntimeConfig::default(),
                wait_for_runtime_stop(cleanup_stop),
            )
            .await;

            tracing::info!(
                ?outcome,
                "outbound dispatcher limiter cleanup worker stopped"
            );
        }),
    );

    workers
}

#[allow(clippy::too_many_arguments)]
async fn send_dispatcher_work_item(
    history: PostgresHistoryStore,
    telegram: openplotva_telegram::TelegramClient,
    rich: openplotva_telegram::RichApiClient,
    ephemeral: RedisEphemeralMessageStore,
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<RedisRateLimitStore>>,
    permissions: Arc<permissions::ChatPermissionPolicy<PostgresChatSettingsStore>>,
    failure_ring: Option<Arc<DispatchFailureRing>>,
    routing_admin_reports: Option<openplotva_storage::llm_routing::PostgresRoutingAdminReportStore>,
    item: openplotva_telegram::DispatcherWorkItem,
) -> openplotva_telegram::DispatcherSendStatus {
    send_dispatcher_work_item_with_transport_and_history(
        history,
        ephemeral,
        rate_limits,
        permissions,
        failure_ring,
        routing_admin_reports,
        item,
        |method| {
            let telegram = telegram.clone();
            let rich = rich.clone();
            async move {
                openplotva_telegram::execute_telegram_method_with_rich(&telegram, &rich, method)
                    .await
            }
        },
    )
    .await
}

#[cfg(test)]
pub(crate) async fn send_dispatcher_work_item_with_transport<R, P, SendFn, SendFuture>(
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<R>>,
    permissions: Arc<permissions::ChatPermissionPolicy<P>>,
    item: openplotva_telegram::DispatcherWorkItem,
    send: SendFn,
) -> openplotva_telegram::DispatcherSendStatus
where
    R: rate_limits::RateLimitStore + Send + Sync,
    P: permissions::ChatPermissionStore + Send + Sync,
    SendFn: Fn(openplotva_telegram::TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<
        Output = Result<
            openplotva_telegram::TelegramOutboundResponse,
            openplotva_telegram::TelegramOutboundExecuteError,
        >,
    >,
{
    send_dispatcher_work_item_with_transport_and_history(
        virtual_messages::NoopEditHistorySink,
        virtual_messages::NoopEphemeralMessageTracker,
        rate_limits,
        permissions,
        None,
        None,
        item,
        send,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_dispatcher_work_item_with_transport_and_history<
    H,
    E,
    R,
    P,
    SendFn,
    SendFuture,
>(
    history: H,
    ephemeral: E,
    rate_limits: Arc<rate_limits::ChatRateLimitPolicy<R>>,
    permissions: Arc<permissions::ChatPermissionPolicy<P>>,
    failure_ring: Option<Arc<DispatchFailureRing>>,
    routing_admin_reports: Option<openplotva_storage::llm_routing::PostgresRoutingAdminReportStore>,
    item: openplotva_telegram::DispatcherWorkItem,
    send: SendFn,
) -> openplotva_telegram::DispatcherSendStatus
where
    H: virtual_messages::EditHistorySink,
    E: virtual_messages::EphemeralMessageTracker + Sync,
    R: rate_limits::RateLimitStore + Send + Sync,
    P: permissions::ChatPermissionStore + Send + Sync,
    SendFn: Fn(openplotva_telegram::TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<
        Output = Result<
            openplotva_telegram::TelegramOutboundResponse,
            openplotva_telegram::TelegramOutboundExecuteError,
        >,
    >,
{
    let chat_id = item.metadata().chat_id;
    let bypass_chat_restrictions = item.bypasses_chat_restrictions();
    let virtual_id = item.metadata().virtual_id.clone();
    let protected = item.metadata().protected;
    if virtual_id.starts_with(openplotva_storage::maintenance_notifications::NOTIFICATION_PREFIX) {
        let Some(store) = routing_admin_reports.as_ref() else {
            return openplotva_telegram::DispatcherSendStatus::Failed;
        };
        match maintenance_api::begin_notification(store.pool(), &virtual_id).await {
            Ok(true) => {}
            Ok(false) => return openplotva_telegram::DispatcherSendStatus::Sent,
            Err(error) => {
                tracing::warn!(%error, "failed to claim maintenance notification send");
                return openplotva_telegram::DispatcherSendStatus::Failed;
            }
        }
    }
    let reply_to_message_id =
        reply_message_id_from_fingerprint_key(&item.metadata().fingerprint_key);
    let method_kind_label = item
        .method_kind()
        .map(|kind| format!("{kind:?}"))
        .unwrap_or_else(|| "none".to_owned());
    let record_failure = |error: String, class: &'static str| {
        if let Some(ring) = failure_ring.as_deref() {
            ring.record(DispatchFailureRecord {
                at: OffsetDateTime::now_utc(),
                virtual_id: virtual_id.clone(),
                chat_id,
                method_kind: method_kind_label.clone(),
                error,
                class,
                protected,
                reply_to_message_id,
            });
        }
    };
    if chat_id != 0 {
        let check = rate_limits
            .is_rate_limited_at(chat_id, OffsetDateTime::now_utc())
            .await;
        if let Some(load_error) = check.load_error.as_deref() {
            tracing::debug!(
                chat_id,
                %load_error,
                "failed to load persisted Telegram rate-limit state"
            );
        }
        if check.rate_limited {
            tracing::debug!(chat_id, "skipping Telegram send for rate-limited chat");
            record_failure(
                "skipped send for rate-limited chat".to_owned(),
                DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED,
            );
            if let Some(store) = routing_admin_reports.as_ref()
                && let Err(error) = maintenance_api::finish_notification(
                    store.pool(),
                    &virtual_id,
                    None,
                    true,
                    false,
                )
                .await
            {
                tracing::warn!(%error, "failed to defer rate-limited maintenance notification");
            }
            if let Some(store) = routing_admin_reports.as_ref()
                && let Err(error) = routing_admin_reports::record_routing_admin_dispatch_failure(
                    store,
                    &virtual_id,
                    DISPATCH_FAILURE_CLASS_CHAT_RATE_LIMITED,
                    OffsetDateTime::now_utc(),
                )
                .await
            {
                tracing::warn!(%error, %virtual_id, "failed to record rate-limited LLM admin report");
            }
            return openplotva_telegram::DispatcherSendStatus::Failed;
        }
    }
    if chat_id != 0
        && !bypass_chat_restrictions
        && let Some(method_kind) = item.method_kind()
    {
        for action in permissions::dispatcher_required_actions(method_kind) {
            let report = permissions
                .can_perform_action_at(chat_id, None, action, OffsetDateTime::now_utc())
                .await;
            if let Some(load_error) = report.load_error.as_deref() {
                tracing::debug!(
                    chat_id,
                    action,
                    %load_error,
                    "failed to load Telegram permission state"
                );
            }
            if !report.allowed {
                tracing::debug!(
                    chat_id,
                    action,
                    "skipping Telegram send for chat permission settings"
                );
                record_failure(
                    format!("skipped send: chat permission settings deny {action}"),
                    openplotva_telegram::OutboundSendErrorClass::TerminalPermission.as_str(),
                );
                if let Some(store) = routing_admin_reports.as_ref()
                    && let Err(error) = maintenance_api::finish_notification(
                        store.pool(),
                        &virtual_id,
                        None,
                        true,
                        true,
                    )
                    .await
                {
                    tracing::warn!(%error, "failed to defer denied maintenance notification");
                }
                if let Some(store) = routing_admin_reports.as_ref()
                    && let Err(error) =
                        routing_admin_reports::record_routing_admin_dispatch_failure(
                            store,
                            &virtual_id,
                            openplotva_telegram::OutboundSendErrorClass::TerminalPermission
                                .as_str(),
                            OffsetDateTime::now_utc(),
                        )
                        .await
                {
                    tracing::warn!(%error, %virtual_id, "failed to record denied LLM admin report");
                }
                return openplotva_telegram::DispatcherSendStatus::Failed;
            }
        }
    }

    let retry_virtual_id = virtual_id.clone();
    let report = virtual_messages::send_work_item_with_history_and_ephemeral(
        &history,
        &ephemeral,
        item,
        OffsetDateTime::now_utc(),
        |method| {
            let rate_limits = Arc::clone(&rate_limits);
            let permissions = Arc::clone(&permissions);
            async move {
                let method_kind = method.kind();
                let reply_missing_fallback =
                    openplotva_telegram::replay_outbound_method_without_reply(&method);
                match openplotva_telegram::send_outbound_method_with_bounded_retry(
                    &send,
                    method,
                    &retry_virtual_id,
                    chat_id,
                )
                .await
                {
                    Ok(response) => Ok(response),
                    Err(mut error) => {
                        if matches!(
                            method_kind,
                            openplotva_telegram::TelegramOutboundMethodKind::SendMessage
                                | openplotva_telegram::TelegramOutboundMethodKind::SendRichMessage
                        ) && error.is_reply_missing()
                        {
                            tracing::warn!(
                                chat_id,
                                "reply target missing; retrying Telegram send without reply parameters"
                            );
                            if let Some(fallback) = reply_missing_fallback {
                                match openplotva_telegram::send_outbound_method_with_bounded_retry(
                                    &send,
                                    fallback,
                                    &retry_virtual_id,
                                    chat_id,
                                )
                                .await
                                {
                                    Ok(response) => return Ok(response),
                                    Err(fallback_error) => error = fallback_error,
                                }
                            }
                        }
                        if let Some(retry_after) =
                            rate_limits::telegram_retry_after_from_outbound_error(&error)
                        {
                            let report = rate_limits
                                .set_rate_limit_at(chat_id, retry_after, OffsetDateTime::now_utc())
                                .await;
                            if let Some(save_error) = report.save_error.as_deref() {
                                tracing::warn!(
                                    chat_id,
                                    retry_after_seconds = retry_after.as_secs(),
                                    %save_error,
                                    "failed to persist Telegram rate-limit state"
                                );
                            }
                        }
                        if chat_id != 0
                            && error.is_permission_error()
                        {
                            let report = permissions
                                .record_send_permission_error(chat_id, method_kind)
                                .await;
                            if let Some(load_error) = report.load_error.as_deref() {
                                tracing::warn!(
                                    chat_id,
                                    method = ?method_kind,
                                    %load_error,
                                    "failed to load chat permission state after Telegram permission error"
                                );
                            }
                            if let Some(save_error) = report.save_error.as_deref() {
                                tracing::warn!(
                                    chat_id,
                                    method = ?method_kind,
                                    %save_error,
                                    "failed to persist chat permission state after Telegram permission error"
                                );
                            }
                        }
                        Err(error)
                    }
                }
            }
        },
    )
    .await;
    if report.ephemeral_track_error.is_some() {
        tracing::warn!(
            virtual_id = report.virtual_id,
            real_message_id = ?report.sent_message_id,
            ephemeral_track_error = ?report.ephemeral_track_error,
            "failed to track outbound ephemeral message"
        );
    }
    if let Some(store) = routing_admin_reports.as_ref()
        && let Err(error) = maintenance_api::record_dispatch(store.pool(), &report).await
    {
        tracing::warn!(%error, "failed to record maintenance notification delivery receipt");
    }
    if matches!(
        report.status,
        openplotva_telegram::DispatcherSendStatus::Failed
    ) {
        let error_class = report
            .error_class
            .unwrap_or(DISPATCH_FAILURE_CLASS_MISSING_METHOD);
        let send_error = report.send_error.clone().unwrap_or_default();
        tracing::warn!(
            virtual_id = report.virtual_id,
            chat_id,
            error_class,
            send_error,
            method_kind = method_kind_label,
            "outbound dispatcher send failed"
        );
        record_failure(send_error, error_class);
    }
    if let Some(store) = routing_admin_reports.as_ref()
        && let Err(error) = routing_admin_reports::record_routing_admin_dispatch_result(
            store,
            &report,
            OffsetDateTime::now_utc(),
        )
        .await
    {
        tracing::warn!(%error, virtual_id = report.virtual_id, "failed to record LLM admin report delivery");
    }
    report.status
}

pub(crate) fn dispatcher_config() -> openplotva_telegram::DispatcherConfig {
    openplotva_telegram::DispatcherConfig {
        max_queue_size: DISPATCHER_MAX_QUEUE_SIZE,
        dedupe_config: openplotva_telegram::DebouncerConfig {
            enabled: true,
            default_window: DISPATCHER_DEBOUNCE_WINDOW,
            max_cache_size: DISPATCHER_DEBOUNCE_CACHE_SIZE,
            per_chat_settings: Default::default(),
        },
        ..openplotva_telegram::DispatcherConfig::default()
    }
}

pub(crate) struct DispatcherWorkerGroupInputs {
    pub(crate) history: PostgresHistoryStore,
    pub(crate) telegram: openplotva_telegram::TelegramClient,
    pub(crate) rich: openplotva_telegram::RichApiClient,
    pub(crate) ephemeral: RedisEphemeralMessageStore,
    pub(crate) rate_limits: Arc<rate_limits::ChatRateLimitPolicy<RedisRateLimitStore>>,
    pub(crate) permissions: Arc<permissions::ChatPermissionPolicy<PostgresChatSettingsStore>>,
    pub(crate) queue: Arc<openplotva_telegram::DispatcherQueue>,
    pub(crate) limiters: Arc<openplotva_telegram::ChatLimiters>,
    pub(crate) failure_ring: Arc<DispatchFailureRing>,
    pub(crate) routing_admin_reports:
        openplotva_storage::llm_routing::PostgresRoutingAdminReportStore,
}

pub(crate) const DISPATCHER_MAX_QUEUE_SIZE: usize = 10_000;
pub(crate) const DISPATCHER_DEBOUNCE_WINDOW: Duration = Duration::from_secs(3);
pub(crate) const DISPATCHER_DEBOUNCE_CACHE_SIZE: usize = 1_000;
#[cfg(test)]
mod tests {
    use super::*;

    fn failure(virtual_id: &str, protected: bool) -> DispatchFailureRecord {
        DispatchFailureRecord {
            at: OffsetDateTime::from_unix_timestamp(1_782_993_600).expect("timestamp"),
            virtual_id: virtual_id.to_owned(),
            chat_id: -100,
            method_kind: "SendMessage".to_owned(),
            error: "Forbidden: bot was blocked by the user".to_owned(),
            class: "terminal_permission",
            protected,
            reply_to_message_id: Some(77),
        }
    }

    #[test]
    fn ring_records_and_snapshots_newest_first_within_cap() {
        let ring = DispatchFailureRing::default();
        for index in 0..(DISPATCH_FAILURE_RING_CAP + 2) {
            ring.record(failure(&format!("v{index}"), false));
        }

        let snapshot = ring.snapshot(2);
        assert_eq!(snapshot.len(), 2);
        assert_eq!(
            snapshot[0].virtual_id,
            format!("v{}", DISPATCH_FAILURE_RING_CAP + 1)
        );
        assert_eq!(
            snapshot[1].virtual_id,
            format!("v{}", DISPATCH_FAILURE_RING_CAP)
        );
        assert_eq!(ring.snapshot(usize::MAX).len(), DISPATCH_FAILURE_RING_CAP);
        let oldest = ring
            .snapshot(usize::MAX)
            .pop()
            .expect("oldest retained record");
        assert_eq!(oldest.virtual_id, "v2", "oldest records were dropped");
    }

    #[test]
    fn protected_scan_returns_only_new_protected_records_and_advances_cursor() {
        let ring = DispatchFailureRing::default();
        ring.record(failure("v0", true));
        ring.record(failure("v1", false));

        let (first, cursor) = ring.protected_failures_since(0);
        assert_eq!(
            first
                .iter()
                .map(|record| record.virtual_id.as_str())
                .collect::<Vec<_>>(),
            vec!["v0"]
        );

        let (empty, cursor) = ring.protected_failures_since(cursor);
        assert!(
            empty.is_empty(),
            "already-consumed records are not rescanned"
        );

        ring.record(failure("v2", true));
        let (second, _) = ring.protected_failures_since(cursor);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].virtual_id, "v2");
    }

    #[test]
    fn inspector_serves_ring_records_for_graphql() {
        let ring = DispatchFailureRing::default();
        ring.record(failure("v0", true));

        let data = ring.send_failures(10);
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].at, "2026-07-02T12:00:00Z");
        assert_eq!(data[0].virtual_id, "v0");
        assert_eq!(data[0].chat_id, -100);
        assert_eq!(data[0].class, "terminal_permission");
        assert!(data[0].protected);
        assert_eq!(data[0].reply_to_message_id, Some(77));
    }

    #[test]
    fn reply_message_id_parses_reply_scoped_debounce_keys_only() {
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123:r100"),
            Some(100)
        );
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123"),
            None
        );
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123:custom"),
            None
        );
        assert_eq!(
            reply_message_id_from_fingerprint_key("42:text:abc123:rnot-a-number"),
            None
        );
    }
}
