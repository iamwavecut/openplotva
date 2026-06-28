use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use sqlx::PgPool;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

const ROUTING_EVENT_BUFFER_CAPACITY: usize = 1_000;
const ROUTING_EVENT_WRITER_CHANNEL_CAPACITY: usize = 10_000;
const ROUTING_EVENT_WRITER_BATCH_SIZE: usize = 100;
const ROUTING_EVENT_WRITER_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
pub const LLM_ROUTING_EVENTS_CLEANUP_BATCH_SIZE: i64 = 10_000;
pub const LLM_ROUTING_EVENTS_CLEANUP_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
pub const DEFAULT_ROUTING_ADMIN_REPORT_COOLDOWN: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoutingEvent {
    pub severity: String,
    pub event_type: String,
    pub workflow_key: String,
    pub provider_id: Option<i64>,
    pub model_id: Option<i64>,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub dedupe_key: String,
    pub summary: String,
    pub detail: Value,
}

pub fn routing_backfill_failed_event(operation: &str, error: &str) -> RoutingEvent {
    routing_failure_event("routing_backfill_failed", operation, error)
}

pub fn router_reload_failed_event(operation: &str, error: &str) -> RoutingEvent {
    routing_failure_event("router_reload_failed", operation, error)
}

fn routing_failure_event(event_type: &str, operation: &str, error: &str) -> RoutingEvent {
    let operation = compact(operation);
    RoutingEvent {
        severity: "error".to_owned(),
        event_type: event_type.to_owned(),
        workflow_key: "routing".to_owned(),
        provider_id: None,
        model_id: None,
        queue_name: None,
        job_id: None,
        chat_id: None,
        thread_id: None,
        message_id: None,
        dedupe_key: format!("{event_type}:{operation}"),
        summary: format!("{operation} failed"),
        detail: json!({
            "operation": operation,
            "error": compact(error),
        }),
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RoutingEventData {
    pub id: i64,
    pub at_millis: i64,
    pub severity: String,
    pub event_type: String,
    pub workflow_key: String,
    pub provider_id: Option<i64>,
    pub model_id: Option<i64>,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub dedupe_key: String,
    pub summary: String,
    pub detail: Value,
}

impl RoutingEventData {
    fn from_event(event: RoutingEvent, id: i64, at_millis: i64) -> Self {
        Self {
            id,
            at_millis,
            severity: event.severity,
            event_type: event.event_type,
            workflow_key: event.workflow_key,
            provider_id: event.provider_id,
            model_id: event.model_id,
            queue_name: event.queue_name,
            job_id: event.job_id,
            chat_id: event.chat_id,
            thread_id: event.thread_id,
            message_id: event.message_id,
            dedupe_key: event.dedupe_key,
            summary: event.summary,
            detail: event.detail,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RoutingEventBuffer {
    inner: Arc<Mutex<RoutingEventBufferInner>>,
    next_id: Arc<AtomicI64>,
}

#[derive(Debug)]
struct RoutingEventBufferInner {
    ring: Vec<Option<RoutingEventData>>,
    write: usize,
    count: usize,
}

impl Default for RoutingEventBuffer {
    fn default() -> Self {
        Self::new(ROUTING_EVENT_BUFFER_CAPACITY)
    }
}

impl RoutingEventBuffer {
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 {
            ROUTING_EVENT_BUFFER_CAPACITY
        } else {
            capacity
        };
        Self {
            inner: Arc::new(Mutex::new(RoutingEventBufferInner {
                ring: vec![None; capacity],
                write: 0,
                count: 0,
            })),
            next_id: Arc::new(AtomicI64::new(0)),
        }
    }

    fn record(&self, event: RoutingEvent, at_millis: i64) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let data = RoutingEventData::from_event(event, id, at_millis);
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.ring.is_empty() {
            return;
        }
        let write = inner.write;
        inner.ring[write] = Some(data);
        inner.write = (write + 1) % inner.ring.len();
        inner.count = inner.count.saturating_add(1).min(inner.ring.len());
    }

    pub fn routing_events(&self, limit: usize) -> Vec<RoutingEventData> {
        let limit = limit.max(1);
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if inner.count == 0 || inner.ring.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(inner.count.min(limit));
        let mut idx = inner.write.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        for _ in 0..inner.count {
            if let Some(event) = inner.ring[idx].clone() {
                out.push(event);
                if out.len() >= limit {
                    break;
                }
            }
            idx = idx.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        }
        out
    }
}

impl openplotva_server::RuntimeRoutingEventInspector for RoutingEventBuffer {
    fn routing_events(
        &self,
        filter: openplotva_server::RuntimeRoutingEventsFilter,
    ) -> Result<Vec<openplotva_server::RuntimeRoutingEventData>, String> {
        let limit = filter.limit.max(1) as usize;
        let mut out = Vec::with_capacity(limit.min(ROUTING_EVENT_BUFFER_CAPACITY));
        for event in self.routing_events(ROUTING_EVENT_BUFFER_CAPACITY) {
            if !routing_event_matches_filter(&event, &filter) {
                continue;
            }
            out.push(server_event_from_buffer_event(event));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }
}

pub trait RoutingAdminNotifier: Send + Sync {
    fn notify_admins(&self, text: String) -> Result<(), String>;
}

#[derive(Clone)]
pub struct PostgresRoutingEventRecorder {
    sender: mpsc::Sender<openplotva_storage::llm_routing::RoutingEventInput>,
}

impl PostgresRoutingEventRecorder {
    pub fn spawn(pool: PgPool, stop: watch::Receiver<bool>) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(ROUTING_EVENT_WRITER_CHANNEL_CAPACITY);
        let handle = tokio::spawn(run_routing_event_writer(
            pool,
            receiver,
            stop,
            ROUTING_EVENT_WRITER_BATCH_SIZE,
            ROUTING_EVENT_WRITER_FLUSH_INTERVAL,
        ));
        (Self { sender }, handle)
    }

    fn enqueue(&self, event: openplotva_storage::llm_routing::RoutingEventInput) {
        if let Err(error) = self.sender.try_send(event) {
            match error {
                mpsc::error::TrySendError::Full(event) => {
                    tracing::warn!(
                        workflow = %event.workflow_key,
                        event_type = %event.event_type,
                        "dropping llm routing event because writer channel is full"
                    );
                }
                mpsc::error::TrySendError::Closed(event) => {
                    tracing::debug!(
                        workflow = %event.workflow_key,
                        event_type = %event.event_type,
                        "dropping llm routing event because writer is stopped"
                    );
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct RoutingEventReporter {
    buffer: RoutingEventBuffer,
    recorder: Option<PostgresRoutingEventRecorder>,
    admin_notifier: Option<Arc<dyn RoutingAdminNotifier>>,
    suppression: Arc<Mutex<HashMap<String, SuppressionState>>>,
    cooldown: Duration,
}

#[derive(Clone, Copy, Debug, Default)]
struct SuppressionState {
    next_allowed_millis: i64,
    suppressed: u64,
}

impl RoutingEventReporter {
    pub fn new(
        buffer: RoutingEventBuffer,
        recorder: Option<PostgresRoutingEventRecorder>,
        admin_notifier: Option<Arc<dyn RoutingAdminNotifier>>,
        cooldown: Duration,
    ) -> Self {
        Self {
            buffer,
            recorder,
            admin_notifier,
            suppression: Arc::new(Mutex::new(HashMap::new())),
            cooldown,
        }
    }

    pub fn buffer(&self) -> RoutingEventBuffer {
        self.buffer.clone()
    }

    pub fn record(&self, event: RoutingEvent) {
        self.record_at_millis(event, current_unix_millis());
    }

    pub fn record_at_millis(&self, event: RoutingEvent, now_millis: i64) {
        let page = self.admin_page_decision(&event, now_millis);
        let mut event = event;
        event.detail = enrich_detail_for_admin_page(event.detail, &page);

        self.buffer.record(event.clone(), now_millis);
        if let Some(recorder) = &self.recorder {
            recorder.enqueue(storage_input_from_event(&event));
        }
        if let AdminPageDecision::Send { suppressed } = page
            && let Some(notifier) = &self.admin_notifier
        {
            let text = format_admin_report(&event, suppressed);
            if let Err(error) = notifier.notify_admins(text) {
                tracing::warn!(
                    %error,
                    workflow = %event.workflow_key,
                    event_type = %event.event_type,
                    "failed to enqueue llm routing admin report"
                );
            }
        }
    }

    fn admin_page_decision(&self, event: &RoutingEvent, now_millis: i64) -> AdminPageDecision {
        if self.admin_notifier.is_none() {
            return AdminPageDecision::NotConfigured;
        }
        if event
            .detail
            .get("admin_actionable")
            .and_then(Value::as_bool)
            == Some(false)
        {
            return AdminPageDecision::NotActionable;
        }
        if !is_actionable_event(&event.event_type) {
            return AdminPageDecision::NotActionable;
        }
        let mut suppression = self
            .suppression
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = suppression.entry(event.dedupe_key.clone()).or_default();
        if now_millis < state.next_allowed_millis {
            state.suppressed = state.suppressed.saturating_add(1);
            return AdminPageDecision::Suppressed {
                suppressed: state.suppressed,
            };
        }
        let suppressed = state.suppressed;
        state.suppressed = 0;
        state.next_allowed_millis = now_millis.saturating_add(self.cooldown.as_millis() as i64);
        AdminPageDecision::Send { suppressed }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdminPageDecision {
    NotConfigured,
    NotActionable,
    Send { suppressed: u64 },
    Suppressed { suppressed: u64 },
}

#[derive(Debug)]
pub struct DispatcherRoutingAdminNotifier {
    admin_ids: Arc<[i64]>,
    queue: Arc<openplotva_telegram::DispatcherQueue>,
    sequence: AtomicU64,
}

impl DispatcherRoutingAdminNotifier {
    pub fn new(
        admin_ids: impl Into<Arc<[i64]>>,
        queue: Arc<openplotva_telegram::DispatcherQueue>,
    ) -> Self {
        Self {
            admin_ids: admin_ids.into(),
            queue,
            sequence: AtomicU64::new(0),
        }
    }
}

impl RoutingAdminNotifier for DispatcherRoutingAdminNotifier {
    fn notify_admins(&self, text: String) -> Result<(), String> {
        for admin_id in self.admin_ids.iter().copied() {
            let request = openplotva_telegram::TextMessageRequest {
                chat: Some(openplotva_telegram::ChatRef {
                    id: admin_id,
                    is_forum: false,
                }),
                message_thread_id: 0,
                disable_notification: false,
                allow_sending_without_reply: None,
                text: text.clone(),
                render_as: String::new(),
                reply_markup: None,
            };
            let methods = openplotva_telegram::build_text_message_methods(&request, None)
                .map_err(|error| error.to_string())?;
            for method in methods {
                let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
                let fingerprint =
                    openplotva_telegram::fingerprint_text_message_part(admin_id, &text);
                let message = openplotva_telegram::DispatcherMessage::new(
                    fingerprint,
                    format!("routing-admin-{admin_id}-{sequence}"),
                )
                .with_method(openplotva_telegram::TelegramOutboundMethod::from(method));
                let _ = self.queue.enqueue(message, true);
            }
        }
        Ok(())
    }
}

fn storage_input_from_event(
    event: &RoutingEvent,
) -> openplotva_storage::llm_routing::RoutingEventInput {
    openplotva_storage::llm_routing::RoutingEventInput {
        severity: event.severity.clone(),
        event_type: event.event_type.clone(),
        workflow_key: event.workflow_key.clone(),
        provider_id: event.provider_id,
        model_id: event.model_id,
        queue_name: event.queue_name.clone(),
        job_id: event.job_id,
        chat_id: event.chat_id,
        thread_id: event.thread_id,
        message_id: event.message_id,
        dedupe_key: event.dedupe_key.clone(),
        summary: event.summary.clone(),
        detail: event.detail.clone(),
    }
}

fn is_actionable_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "route_unavailable"
            | "no_candidates"
            | "all_attempts_exhausted"
            | "circuit_open_exhaustion"
            | "capacity_unavailable"
            | "router_reload_failed"
            | "routing_backfill_failed"
    )
}

fn enrich_detail_for_admin_page(detail: Value, page: &AdminPageDecision) -> Value {
    let mut object = detail.as_object().cloned().unwrap_or_default();
    let page_json = match page {
        AdminPageDecision::NotConfigured => json!({"action": "not_configured"}),
        AdminPageDecision::NotActionable => json!({"action": "none"}),
        AdminPageDecision::Send { suppressed } => {
            json!({"action": "sent", "suppressed_repeats": suppressed})
        }
        AdminPageDecision::Suppressed { suppressed } => {
            json!({"action": "suppressed", "suppressed_repeats": suppressed})
        }
    };
    object.insert("admin_report".to_owned(), page_json);
    Value::Object(object)
}

fn format_admin_report(event: &RoutingEvent, suppressed: u64) -> String {
    let mut lines = vec![
        "LLM routing incident".to_owned(),
        format!("severity: {}", compact(&event.severity)),
        format!("workflow: {}", compact(&event.workflow_key)),
        format!("failure: {}", compact(&event.event_type)),
    ];
    if event.provider_id.is_some() || event.model_id.is_some() {
        lines.push(format!(
            "provider/model: {}/{}",
            event
                .provider_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            event
                .model_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_owned())
        ));
    }
    let context = report_context(event);
    if !context.is_empty() {
        lines.push(format!("context: {context}"));
    }
    if suppressed > 0 {
        lines.push(format!("suppressed repeats: {suppressed}"));
    }
    if !event.summary.trim().is_empty() {
        lines.push(format!("summary: {}", compact(&event.summary)));
    }
    lines.push("inspect: runtime API llmRequests and routing events".to_owned());
    lines.join("\n")
}

fn report_context(event: &RoutingEvent) -> String {
    let mut parts = Vec::new();
    if let Some(queue) = event
        .queue_name
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("queue={}", compact(queue)));
    }
    if let Some(job_id) = event.job_id {
        parts.push(format!("job={job_id}"));
    }
    if let Some(chat_id) = event.chat_id {
        parts.push(format!("chat={chat_id}"));
    }
    if let Some(thread_id) = event.thread_id {
        parts.push(format!("thread={thread_id}"));
    }
    if let Some(message_id) = event.message_id {
        parts.push(format!("message={message_id}"));
    }
    parts.join(" ")
}

fn compact(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn current_unix_millis() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn routing_event_matches_filter(
    event: &RoutingEventData,
    filter: &openplotva_server::RuntimeRoutingEventsFilter,
) -> bool {
    if !filter.workflow_key.is_empty() && event.workflow_key != filter.workflow_key {
        return false;
    }
    if !filter.event_type.is_empty() && event.event_type != filter.event_type {
        return false;
    }
    if filter.q.is_empty() {
        return true;
    }
    let q = filter.q.to_lowercase();
    event.summary.to_lowercase().contains(&q)
        || event.event_type.to_lowercase().contains(&q)
        || event.workflow_key.to_lowercase().contains(&q)
        || event.dedupe_key.to_lowercase().contains(&q)
        || event
            .queue_name
            .as_deref()
            .is_some_and(|queue| queue.to_lowercase().contains(&q))
}

fn server_event_from_buffer_event(
    event: RoutingEventData,
) -> openplotva_server::RuntimeRoutingEventData {
    openplotva_server::RuntimeRoutingEventData {
        id: event.id,
        at: format_unix_millis(event.at_millis),
        severity: event.severity,
        event_type: event.event_type,
        workflow_key: event.workflow_key,
        provider_id: event.provider_id,
        model_id: event.model_id,
        queue_name: event.queue_name,
        job_id: event.job_id,
        chat_id: event.chat_id,
        thread_id: event.thread_id,
        message_id: event.message_id,
        dedupe_key: event.dedupe_key,
        summary: event.summary,
        detail: event.detail,
    }
}

fn format_unix_millis(millis: i64) -> String {
    let nanos = i128::from(millis).saturating_mul(1_000_000);
    match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
        Ok(at) => at.format(&Rfc3339).unwrap_or_default(),
        Err(_) => String::new(),
    }
}

async fn run_routing_event_writer(
    pool: PgPool,
    mut receiver: mpsc::Receiver<openplotva_storage::llm_routing::RoutingEventInput>,
    mut stop: watch::Receiver<bool>,
    batch_size: usize,
    flush_interval: Duration,
) {
    let batch_size = batch_size.max(1);
    let mut pending = Vec::with_capacity(batch_size);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if *stop.borrow() {
            drain_and_flush_routing_event_batches(&pool, &mut receiver, &mut pending, batch_size)
                .await;
            break;
        }

        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    drain_and_flush_routing_event_batches(
                        &pool,
                        &mut receiver,
                        &mut pending,
                        batch_size,
                    )
                    .await;
                    break;
                }
            }
            maybe_event = receiver.recv() => {
                let Some(event) = maybe_event else {
                    flush_routing_event_batch(&pool, &mut pending).await;
                    break;
                };
                pending.push(event);
                if pending.len() >= batch_size {
                    flush_routing_event_batch(&pool, &mut pending).await;
                }
            }
            _ = interval.tick() => {
                flush_routing_event_batch(&pool, &mut pending).await;
            }
        }
    }
}

async fn drain_and_flush_routing_event_batches(
    pool: &PgPool,
    receiver: &mut mpsc::Receiver<openplotva_storage::llm_routing::RoutingEventInput>,
    pending: &mut Vec<openplotva_storage::llm_routing::RoutingEventInput>,
    batch_size: usize,
) {
    loop {
        drain_routing_event_channel(receiver, pending, batch_size);
        if pending.is_empty() {
            break;
        }
        flush_routing_event_batch(pool, pending).await;
    }
}

fn drain_routing_event_channel(
    receiver: &mut mpsc::Receiver<openplotva_storage::llm_routing::RoutingEventInput>,
    pending: &mut Vec<openplotva_storage::llm_routing::RoutingEventInput>,
    batch_size: usize,
) {
    while pending.len() < batch_size {
        match receiver.try_recv() {
            Ok(event) => pending.push(event),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

async fn flush_routing_event_batch(
    pool: &PgPool,
    pending: &mut Vec<openplotva_storage::llm_routing::RoutingEventInput>,
) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    if let Err(error) = openplotva_storage::llm_routing::insert_routing_events(pool, &batch).await {
        tracing::warn!(
            %error,
            count = batch.len(),
            "failed to insert llm routing event batch"
        );
    }
}

pub async fn run_llm_routing_event_cleanup_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    stop: Stop,
) -> crate::runtime_llm::RuntimeLlmRequestEventCleanupReport
where
    Stop: std::future::Future<Output = ()>,
{
    let mut report = crate::runtime_llm::RuntimeLlmRequestEventCleanupReport {
        enabled: retention_days > 0,
        ..crate::runtime_llm::RuntimeLlmRequestEventCleanupReport::default()
    };
    if !report.enabled {
        return report;
    }

    let stop = stop;
    tokio::pin!(stop);
    loop {
        match openplotva_storage::llm_routing::delete_old_llm_routing_events_batch(
            &pool,
            retention_days,
            batch_size,
        )
        .await
        {
            Ok(deleted) => {
                report.deleted += deleted;
                tracing::debug!(
                    deleted,
                    retention_days,
                    "deleted old llm_routing_events batch"
                );
            }
            Err(error) => {
                report.errors += 1;
                tracing::warn!(%error, retention_days, "failed to delete old llm_routing_events batch");
            }
        }
        report.ticks += 1;

        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct CapturingAdminNotifier {
        messages: Mutex<Vec<String>>,
    }

    impl RoutingAdminNotifier for CapturingAdminNotifier {
        fn notify_admins(&self, text: String) -> Result<(), String> {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(text);
            Ok(())
        }
    }

    impl CapturingAdminNotifier {
        fn messages(&self) -> Vec<String> {
            self.messages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    fn event(event_type: &str) -> RoutingEvent {
        RoutingEvent {
            severity: "error".to_owned(),
            event_type: event_type.to_owned(),
            workflow_key: "dialog".to_owned(),
            provider_id: Some(10),
            model_id: Some(20),
            queue_name: Some("text".to_owned()),
            job_id: Some(30),
            chat_id: Some(-100),
            thread_id: Some(5),
            message_id: Some(77),
            dedupe_key: "route:dialog".to_owned(),
            summary: "dialog route is unavailable".to_owned(),
            detail: json!({
                "raw_prompt": "must not be in telegram report",
                "redis_value": "must not be in telegram report"
            }),
        }
    }

    #[test]
    fn routing_reporter_suppresses_duplicate_admin_pages_until_cooldown() {
        let notifier = Arc::new(CapturingAdminNotifier::default());
        let reporter = RoutingEventReporter::new(
            RoutingEventBuffer::new(8),
            None,
            Some(notifier.clone()),
            Duration::from_millis(600_000),
        );

        reporter.record_at_millis(event("route_unavailable"), 0);
        reporter.record_at_millis(event("route_unavailable"), 1_000);
        reporter.record_at_millis(event("route_unavailable"), 600_001);

        let messages = notifier.messages();
        assert_eq!(messages.len(), 2);
        assert!(messages[0].contains("severity: error"));
        assert!(messages[0].contains("workflow: dialog"));
        assert!(messages[0].contains("failure: route_unavailable"));
        assert!(messages[0].contains("provider/model: 10/20"));
        assert!(messages[0].contains("queue=text"));
        assert!(messages[0].contains("job=30"));
        assert!(messages[0].contains("chat=-100"));
        assert!(messages[0].contains("message=77"));
        assert!(messages[0].contains("llmRequests"));
        assert!(messages[0].contains("routing events"));
        assert!(!messages[0].contains("raw_prompt"));
        assert!(!messages[0].contains("redis_value"));
        assert!(messages[1].contains("suppressed repeats: 1"));
    }

    #[test]
    fn routing_reporter_records_every_suppressed_event_in_buffer() {
        let reporter = RoutingEventReporter::new(
            RoutingEventBuffer::new(8),
            None,
            None,
            Duration::from_millis(600_000),
        );

        reporter.record_at_millis(event("all_attempts_exhausted"), 0);
        reporter.record_at_millis(event("all_attempts_exhausted"), 1_000);

        let events = reporter.buffer().routing_events(10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, "all_attempts_exhausted");
        assert_eq!(events[1].event_type, "all_attempts_exhausted");
    }

    #[test]
    fn routing_reporter_without_notifier_does_not_consume_admin_cooldown() {
        let buffer = RoutingEventBuffer::new(8);
        let reporter_without_notifier =
            RoutingEventReporter::new(buffer.clone(), None, None, Duration::from_millis(600_000));

        reporter_without_notifier.record_at_millis(event("router_reload_failed"), 0);

        let first = buffer.routing_events(1).pop().expect("buffered event");
        assert_eq!(
            first.detail["admin_report"]["action"].as_str(),
            Some("not_configured")
        );

        let notifier = Arc::new(CapturingAdminNotifier::default());
        let reporter_with_notifier = RoutingEventReporter::new(
            buffer,
            None,
            Some(notifier.clone()),
            Duration::from_millis(600_000),
        );
        reporter_with_notifier.record_at_millis(event("router_reload_failed"), 1_000);

        assert_eq!(notifier.messages().len(), 1);
    }

    #[test]
    fn retryable_provider_error_is_recorded_but_does_not_page_admins() {
        let notifier = Arc::new(CapturingAdminNotifier::default());
        let reporter = RoutingEventReporter::new(
            RoutingEventBuffer::new(8),
            None,
            Some(notifier.clone()),
            Duration::from_millis(600_000),
        );

        reporter.record_at_millis(event("provider_retryable_error"), 0);

        assert!(notifier.messages().is_empty());
        assert_eq!(reporter.buffer().routing_events(10).len(), 1);
    }

    #[test]
    fn non_actionable_detail_suppresses_admin_page_for_actionable_event_type() {
        let notifier = Arc::new(CapturingAdminNotifier::default());
        let reporter = RoutingEventReporter::new(
            RoutingEventBuffer::new(8),
            None,
            Some(notifier.clone()),
            Duration::from_millis(600_000),
        );
        let mut event = event("all_attempts_exhausted");
        event.detail = json!({
            "admin_actionable": false,
            "admin_actionable_reason": "handled_by_job_retry_budget"
        });

        reporter.record_at_millis(event, 0);

        assert!(notifier.messages().is_empty());
        let events = reporter.buffer().routing_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].detail["admin_report"]["action"], "none");
    }

    #[test]
    fn routing_event_buffer_filters_runtime_api_events() {
        let buffer = RoutingEventBuffer::new(8);
        buffer.record(event("route_unavailable"), 1_780_000_000_000);
        buffer.record(
            RoutingEvent {
                workflow_key: "vision".to_owned(),
                event_type: "all_attempts_exhausted".to_owned(),
                summary: "vision attempts exhausted".to_owned(),
                ..event("all_attempts_exhausted")
            },
            1_780_000_001_000,
        );

        let events = openplotva_server::RuntimeRoutingEventInspector::routing_events(
            &buffer,
            openplotva_server::RuntimeRoutingEventsFilter {
                q: "unavailable".to_owned(),
                workflow_key: "dialog".to_owned(),
                event_type: "route_unavailable".to_owned(),
                limit: 10,
            },
        )
        .expect("routing events");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].workflow_key, "dialog");
        assert_eq!(events[0].event_type, "route_unavailable");
        assert_eq!(events[0].at, "2026-05-28T20:26:40Z");
    }
}
