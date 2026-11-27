//! Telegram update startup method builders and sources.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use carapax::{
    api::{Method, Payload, PayloadError},
    types::{AllowedUpdate, DeleteWebhook, GetUpdates, SetWebhook, Update as TelegramUpdate},
};
use openplotva_updates::{
    RedisUpdateStream, UpdateIngressDecision, UpdateIngressGuard, UpdateProducerSource,
    UpdateProducerSourceFuture, UpdateStreamAppend, UpdateStreamSource,
};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};

pub const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(60);

pub const LONG_POLL_RETRY_DELAY: Duration = Duration::from_secs(3);

pub const TELEGRAM_WEBHOOK_PATH: &str = "/telegram/webhook";

pub const TELEGRAM_WEBHOOK_SECRET_HEADER: &str = "X-Telegram-Bot-Api-Secret-Token";

pub const WEBHOOK_STREAM_APPEND_TIMEOUT: Duration = Duration::from_secs(3);

const LONG_POLL_STREAM_MAX_RECORDED_ERRORS: usize = 64;

/// Boxed future returned by Telegram `getUpdates` executors.
pub type GetUpdatesFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Vec<serde_json::Value>, E>> + Send + 'a>>;

/// Minimal capability needed by the long-poll update source.
pub trait GetUpdatesExecutor {
    /// Error returned by the concrete Telegram client.
    type Error: fmt::Display + Send;

    /// Execute one Telegram `getUpdates` request. Each update stays the raw
    /// Bot API JSON object, the same shape a webhook request body carries.
    fn get_updates<'a>(&'a self, method: GetUpdates) -> GetUpdatesFuture<'a, Self::Error>;
}

impl GetUpdatesExecutor for carapax::api::Client {
    type Error = carapax::api::ExecuteError;

    fn get_updates<'a>(&'a self, method: GetUpdates) -> GetUpdatesFuture<'a, Self::Error> {
        Box::pin(self.execute(RawGetUpdates(method)))
    }
}

/// `getUpdates` with an untyped result. The typed carapax result is lossy:
/// it reads a GIF `animation` with its `document` mirror as a plain document.
struct RawGetUpdates(GetUpdates);

impl Method for RawGetUpdates {
    type Response = Vec<serde_json::Value>;

    fn into_payload(self) -> Result<Payload, PayloadError> {
        self.0.into_payload()
    }
}

/// One raw Bot API update as webhook and long-poll ingress see it. The raw
/// JSON goes to the stream unchanged; the shared decoder reads it here only
/// for the update ID and the ingress guard, as every consumer does later.
struct RawIngressUpdate {
    update_id: Option<i64>,
    decoded: Option<TelegramUpdate>,
}

impl RawIngressUpdate {
    fn from_slice(body: &[u8]) -> Self {
        serde_json::from_slice(body).map_or(
            Self {
                update_id: None,
                decoded: None,
            },
            Self::from_value,
        )
    }

    fn from_value(value: serde_json::Value) -> Self {
        let raw_update_id = value.get("update_id").and_then(serde_json::Value::as_i64);
        let decoded = openplotva_updates::decode_telegram_update_value(value).ok();
        Self {
            update_id: decoded.as_ref().map(|update| update.id).or(raw_update_id),
            decoded,
        }
    }

    fn dropped_by(&self, ingress_guard: Option<&UpdateIngressGuard>) -> bool {
        match (ingress_guard, &self.decoded) {
            (Some(ingress_guard), Some(update)) => drop_by_ingress_guard(ingress_guard, update),
            _ => false,
        }
    }
}

fn drop_by_ingress_guard(ingress_guard: &UpdateIngressGuard, update: &TelegramUpdate) -> bool {
    let decision = ingress_guard.check_update(update);
    match decision {
        UpdateIngressDecision::Allowed { .. } => return false,
        UpdateIngressDecision::DroppedFlood { .. } => {
            tracing::warn!(
                chat_id = decision.chat_id(),
                update_id = update.id,
                update_name = openplotva_updates::producer_update_name(update),
                "blocked flooded Telegram chat at ingress"
            );
        }
        UpdateIngressDecision::DroppedBlocked { .. } => {
            tracing::debug!(
                chat_id = decision.chat_id(),
                update_id = update.id,
                update_name = openplotva_updates::producer_update_name(update),
                "dropped Telegram update from an ingress-blocked chat"
            );
        }
    }
    true
}

/// Concrete long-polling producer source for Telegram updates.
pub struct LongPollUpdateSource<C> {
    client: C,
    retry_delay: Duration,
    state: Mutex<LongPollState>,
}

#[derive(Debug, Default)]
struct LongPollState {
    offset: i64,
    buffered: VecDeque<TelegramUpdate>,
}

impl<C> LongPollUpdateSource<C> {
    pub fn new(client: C) -> Self {
        Self {
            client,
            retry_delay: LONG_POLL_RETRY_DELAY,
            state: Mutex::new(LongPollState::default()),
        }
    }

    /// Override the retry delay after failed polls.
    pub fn with_retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }
}

impl<C> LongPollUpdateSource<C>
where
    C: GetUpdatesExecutor + Sync,
{
    async fn next_update_inner(&self) -> Option<TelegramUpdate> {
        loop {
            if let Some(update) = self.pop_buffered_update().await {
                return Some(update);
            }

            let offset = self.state.lock().await.offset;
            match self
                .client
                .get_updates(build_get_updates_method_with_offset(offset))
                .await
            {
                Ok(updates) => {
                    self.push_polled_updates(updates).await;
                }
                Err(error) => {
                    let message = error.to_string();
                    drop(error);
                    tracing::warn!(error = %message, "failed to get Telegram updates");
                    sleep(self.retry_delay).await;
                }
            }
        }
    }

    async fn pop_buffered_update(&self) -> Option<TelegramUpdate> {
        let mut state = self.state.lock().await;
        state.buffered.pop_front()
    }

    async fn push_polled_updates(&self, updates: Vec<serde_json::Value>) {
        let mut state = self.state.lock().await;
        for raw_update in updates {
            let update = RawIngressUpdate::from_value(raw_update);
            let Some(update_id) = update
                .update_id
                .filter(|update_id| *update_id >= state.offset)
            else {
                continue;
            };
            state.offset = update_id + 1;
            match update.decoded {
                Some(decoded) => state.buffered.push_back(decoded),
                None => tracing::warn!(update_id, "skipped undecodable Telegram update"),
            }
        }
    }
}

impl<C> UpdateProducerSource for LongPollUpdateSource<C>
where
    C: GetUpdatesExecutor + Send + Sync,
{
    fn next_update<'a>(&'a self) -> UpdateProducerSourceFuture<'a> {
        Box::pin(self.next_update_inner())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LongPollStreamRunReport {
    pub polled_batches: usize,
    pub received: usize,
    pub appended: usize,
    pub dropped_by_ingress_guard: usize,
    pub poll_errors: Vec<String>,
    pub append_errors: Vec<String>,
    pub serialization_errors: Vec<String>,
}

/// Poll Telegram directly into the Redis Stream. The persisted poll cursor is
/// advanced in the same Redis transaction as all XADDs from one response.
pub async fn run_long_poll_stream_producer_until<C, Stop>(
    client: &C,
    stream: &RedisUpdateStream,
    bot_id: i64,
    stop: Stop,
) -> LongPollStreamRunReport
where
    C: GetUpdatesExecutor + Sync,
    Stop: Future<Output = ()>,
{
    run_long_poll_stream_producer_with_optional_ingress_guard_until(
        client, stream, bot_id, None, stop,
    )
    .await
}

pub async fn run_long_poll_stream_producer_with_ingress_guard_until<C, Stop>(
    client: &C,
    stream: &RedisUpdateStream,
    bot_id: i64,
    ingress_guard: &openplotva_updates::UpdateIngressGuard,
    stop: Stop,
) -> LongPollStreamRunReport
where
    C: GetUpdatesExecutor + Sync,
    Stop: Future<Output = ()>,
{
    run_long_poll_stream_producer_with_optional_ingress_guard_until(
        client,
        stream,
        bot_id,
        Some(ingress_guard),
        stop,
    )
    .await
}

async fn run_long_poll_stream_producer_with_optional_ingress_guard_until<C, Stop>(
    client: &C,
    stream: &RedisUpdateStream,
    bot_id: i64,
    ingress_guard: Option<&openplotva_updates::UpdateIngressGuard>,
    stop: Stop,
) -> LongPollStreamRunReport
where
    C: GetUpdatesExecutor + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = LongPollStreamRunReport::default();
    tokio::pin!(stop);
    let mut cursor = loop {
        tokio::select! {
            _ = &mut stop => return report,
            result = stream.long_poll_cursor() => match result {
                Ok(cursor) => break cursor,
                Err(error) => {
                    record_limited_error(&mut report.append_errors, error.to_string());
                    tokio::select! {
                        _ = &mut stop => return report,
                        () = sleep(LONG_POLL_RETRY_DELAY) => {}
                    }
                }
            }
        }
    };

    loop {
        let polled = tokio::select! {
            _ = &mut stop => break,
            result = client.get_updates(build_get_updates_method_with_offset(cursor)) => result,
        };
        let updates = match polled {
            Ok(updates) => updates,
            Err(error) => {
                record_limited_error(&mut report.poll_errors, error.to_string());
                tokio::select! {
                    _ = &mut stop => break,
                    () = sleep(LONG_POLL_RETRY_DELAY) => {}
                }
                continue;
            }
        };
        report.polled_batches = report.polled_batches.saturating_add(1);
        let batch = long_poll_batch(updates, cursor, bot_id, ingress_guard, unix_millis_now());
        report.received = report.received.saturating_add(batch.fresh);
        report.dropped_by_ingress_guard = report
            .dropped_by_ingress_guard
            .saturating_add(batch.dropped_by_ingress_guard);
        if !batch.serialization_errors.is_empty() {
            for error in batch.serialization_errors {
                record_limited_error(&mut report.serialization_errors, error);
            }
            tokio::select! {
                _ = &mut stop => break,
                () = sleep(LONG_POLL_RETRY_DELAY) => {}
            }
            continue;
        }
        if batch.fresh == 0 {
            continue;
        }

        loop {
            let appended = tokio::select! {
                _ = &mut stop => return report,
                result = stream.append_long_poll_batch(&batch.appends, batch.next_cursor) => result,
            };
            match appended {
                Ok(ids) => {
                    report.appended = report.appended.saturating_add(ids.len());
                    cursor = batch.next_cursor;
                    break;
                }
                Err(error) => {
                    record_limited_error(&mut report.append_errors, error.to_string());
                    tokio::select! {
                        _ = &mut stop => return report,
                        () = sleep(LONG_POLL_RETRY_DELAY) => {}
                    }
                }
            }
        }
    }
    report
}

/// Stream entries for one `getUpdates` response.
#[derive(Debug, Default)]
struct LongPollBatch {
    appends: Vec<UpdateStreamAppend>,
    next_cursor: i64,
    /// Updates at or past the cursor, including ones the ingress guard dropped.
    fresh: usize,
    dropped_by_ingress_guard: usize,
    serialization_errors: Vec<String>,
}

/// Turn raw `getUpdates` results into stream entries with the same raw JSON
/// and the same ingress checks a webhook request gets.
fn long_poll_batch(
    updates: Vec<serde_json::Value>,
    cursor: i64,
    bot_id: i64,
    ingress_guard: Option<&UpdateIngressGuard>,
    received_at_unix_ms: i64,
) -> LongPollBatch {
    let mut batch = LongPollBatch {
        appends: Vec::with_capacity(updates.len()),
        next_cursor: cursor,
        ..LongPollBatch::default()
    };
    for raw_update in updates {
        let raw_payload = match serde_json::to_vec(&raw_update) {
            Ok(raw_payload) => raw_payload,
            Err(error) => {
                batch.serialization_errors.push(error.to_string());
                continue;
            }
        };
        let update = RawIngressUpdate::from_value(raw_update);
        if let Some(update_id) = update.update_id {
            if update_id < cursor {
                continue;
            }
            batch.next_cursor = batch.next_cursor.max(update_id.saturating_add(1));
        }
        batch.fresh = batch.fresh.saturating_add(1);
        if update.dropped_by(ingress_guard) {
            batch.dropped_by_ingress_guard = batch.dropped_by_ingress_guard.saturating_add(1);
            continue;
        }
        batch.appends.push(UpdateStreamAppend {
            bot_id,
            update_id: update.update_id,
            source: UpdateStreamSource::LongPoll,
            received_at_unix_ms,
            raw_payload,
        });
    }
    batch
}

fn record_limited_error(errors: &mut Vec<String>, error: String) {
    if errors.len() < LONG_POLL_STREAM_MAX_RECORDED_ERRORS {
        errors.push(error);
    }
}

fn unix_millis_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[derive(Clone, Debug)]
pub struct WebhookUpdateSender {
    stream: RedisUpdateStream,
    bot_id: i64,
    send_timeout: Duration,
    ingress_guard: Option<Arc<openplotva_updates::UpdateIngressGuard>>,
    metrics: Arc<WebhookIngressMetrics>,
}

#[derive(Debug, Default)]
struct WebhookIngressMetrics {
    append_attempts: AtomicU64,
    append_succeeded: AtomicU64,
    append_failed: AtomicU64,
    append_timed_out: AtomicU64,
    append_in_flight: AtomicU64,
    last_append_latency_ms: AtomicU64,
    last_failure_unix_ms: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WebhookIngressSnapshot {
    pub append_attempts: u64,
    pub append_succeeded: u64,
    pub append_failed: u64,
    pub append_timed_out: u64,
    pub append_in_flight: u64,
    pub last_append_latency_ms: u64,
    pub last_failure_unix_ms: Option<u64>,
}

/// Error returned while durably appending a webhook update to Redis Streams.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WebhookUpdateSendError {
    #[error("webhook update stream append timed out")]
    Timeout,
    #[error("webhook update stream append failed")]
    Stream,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WebhookUpdateRequestError {
    /// Only POST is accepted.
    #[error("method not allowed")]
    MethodNotAllowed,
    /// The Telegram secret-token header did not match configured webhook secret.
    #[error("unauthorized")]
    Unauthorized,
    #[error("service unavailable")]
    ServiceUnavailable,
}

impl WebhookUpdateRequestError {
    pub const fn http_status(self) -> u16 {
        match self {
            Self::MethodNotAllowed => 405,
            Self::Unauthorized => 401,
            Self::ServiceUnavailable => 503,
        }
    }
}

/// Build a webhook sink that durably appends raw requests to Redis Streams.
#[must_use]
pub fn webhook_update_stream(stream: RedisUpdateStream, bot_id: i64) -> WebhookUpdateSender {
    WebhookUpdateSender {
        stream,
        bot_id,
        send_timeout: WEBHOOK_STREAM_APPEND_TIMEOUT,
        ingress_guard: None,
        metrics: Arc::new(WebhookIngressMetrics::default()),
    }
}

impl WebhookUpdateSender {
    #[must_use]
    pub fn ingress_snapshot(&self) -> WebhookIngressSnapshot {
        let last_failure_unix_ms = self.metrics.last_failure_unix_ms.load(Ordering::Relaxed);
        WebhookIngressSnapshot {
            append_attempts: self.metrics.append_attempts.load(Ordering::Relaxed),
            append_succeeded: self.metrics.append_succeeded.load(Ordering::Relaxed),
            append_failed: self.metrics.append_failed.load(Ordering::Relaxed),
            append_timed_out: self.metrics.append_timed_out.load(Ordering::Relaxed),
            append_in_flight: self.metrics.append_in_flight.load(Ordering::Relaxed),
            last_append_latency_ms: self.metrics.last_append_latency_ms.load(Ordering::Relaxed),
            last_failure_unix_ms: (last_failure_unix_ms != 0).then_some(last_failure_unix_ms),
        }
    }

    pub fn with_send_timeout(mut self, send_timeout: Duration) -> Self {
        self.send_timeout = send_timeout;
        self
    }

    #[must_use]
    pub fn with_ingress_guard(
        mut self,
        ingress_guard: Arc<openplotva_updates::UpdateIngressGuard>,
    ) -> Self {
        self.ingress_guard = Some(ingress_guard);
        self
    }

    pub async fn handle_webhook_request(
        &self,
        method: &str,
        provided_secret: Option<&str>,
        secret_token: &str,
        body: &[u8],
    ) -> Result<(), WebhookUpdateRequestError> {
        if method != "POST" {
            return Err(WebhookUpdateRequestError::MethodNotAllowed);
        }
        let Some(provided_secret) = provided_secret else {
            return Err(WebhookUpdateRequestError::Unauthorized);
        };
        if secret_token.is_empty()
            || provided_secret.is_empty()
            || !constant_time_secret_eq(provided_secret.as_bytes(), secret_token.as_bytes())
        {
            return Err(WebhookUpdateRequestError::Unauthorized);
        }

        let update = RawIngressUpdate::from_slice(body);
        if update.dropped_by(self.ingress_guard.as_deref()) {
            return Ok(());
        }
        self.append_raw_stream_update(update.update_id, body)
            .await
            .map_err(|_| WebhookUpdateRequestError::ServiceUnavailable)
    }

    async fn append_raw_stream_update(
        &self,
        update_id: Option<i64>,
        body: &[u8],
    ) -> Result<(), WebhookUpdateSendError> {
        let started = Instant::now();
        self.metrics.append_attempts.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .append_in_flight
            .fetch_add(1, Ordering::Relaxed);
        let received_at_unix_ms = unix_millis_now();
        let append = UpdateStreamAppend {
            bot_id: self.bot_id,
            update_id,
            source: UpdateStreamSource::Webhook,
            received_at_unix_ms,
            raw_payload: body.to_vec(),
        };
        let result = match timeout(self.send_timeout, self.stream.append(&append)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => {
                tracing::warn!(
                    bot_id = self.bot_id,
                    update_id,
                    error = %error,
                    "failed to append Telegram webhook update to Redis Stream"
                );
                Err(WebhookUpdateSendError::Stream)
            }
            Err(_) => {
                tracing::warn!(
                    bot_id = self.bot_id,
                    update_id,
                    timeout_ms = self.send_timeout.as_millis(),
                    "timed out appending Telegram webhook update to Redis Stream"
                );
                self.metrics
                    .append_timed_out
                    .fetch_add(1, Ordering::Relaxed);
                Err(WebhookUpdateSendError::Timeout)
            }
        };
        self.metrics
            .append_in_flight
            .fetch_sub(1, Ordering::Relaxed);
        self.metrics.last_append_latency_ms.store(
            started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if result.is_ok() {
            self.metrics
                .append_succeeded
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.append_failed.fetch_add(1, Ordering::Relaxed);
            self.metrics.last_failure_unix_ms.store(
                unix_millis_now().try_into().unwrap_or_default(),
                Ordering::Relaxed,
            );
        }
        result
    }
}

fn constant_time_secret_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebhookSetup {
    /// Public Telegram webhook URL.
    pub url: String,
    /// Optional `X-Telegram-Bot-Api-Secret-Token` value.
    pub secret_token: Option<String>,
    pub certificate: Option<WebhookCertificate>,
}

/// Custom Telegram webhook certificate payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebhookCertificate {
    /// Multipart filename sent to Telegram.
    pub name: String,
    /// Certificate bytes read from `BOT_WEBHOOK_CERT_FILE`.
    pub bytes: Vec<u8>,
}

impl WebhookCertificate {
    /// Build a custom webhook certificate payload.
    pub fn new(name: impl Into<String>, bytes: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            bytes,
        }
    }
}

impl WebhookSetup {
    /// Build setup inputs from a URL and optional secret token.
    pub fn new(url: impl Into<String>, secret_token: Option<String>) -> Self {
        Self {
            url: url.into(),
            secret_token,
            certificate: None,
        }
    }

    /// Attach a custom certificate payload.
    pub fn with_certificate(mut self, certificate: WebhookCertificate) -> Self {
        self.certificate = Some(certificate);
        self
    }
}

pub fn product_allowed_update_set() -> HashSet<AllowedUpdate> {
    [
        AllowedUpdate::BotStatus,
        AllowedUpdate::CallbackQuery,
        AllowedUpdate::ChannelPost,
        AllowedUpdate::EditedChannelPost,
        AllowedUpdate::EditedMessage,
        AllowedUpdate::GuestMessage,
        AllowedUpdate::InlineQuery,
        AllowedUpdate::Message,
        AllowedUpdate::PreCheckoutQuery,
        AllowedUpdate::UserStatus,
    ]
    .into_iter()
    .collect()
}

pub fn build_get_updates_method() -> GetUpdates {
    build_get_updates_method_with_offset(0)
}

pub fn build_get_updates_method_with_offset(offset: i64) -> GetUpdates {
    GetUpdates::default()
        .with_offset(offset)
        .with_timeout(LONG_POLL_TIMEOUT)
        .with_allowed_updates(product_allowed_update_set())
}

pub fn build_delete_webhook_method() -> DeleteWebhook {
    DeleteWebhook::default()
}

pub fn build_set_webhook_method(setup: &WebhookSetup) -> SetWebhook {
    let mut method =
        SetWebhook::new(setup.url.clone()).with_allowed_updates(product_allowed_update_set());

    if let Some(secret_token) = setup
        .secret_token
        .as_deref()
        .filter(|secret_token| !secret_token.is_empty())
    {
        method = method.with_secret_token(secret_token);
    }

    method
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, VecDeque},
        error::Error,
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use carapax::types::{MessageData, Update as TelegramUpdate};
    use openplotva_updates::{
        RedisUpdateStream, UpdateIngressGuard, UpdateIngressGuardConfig, UpdateProducerSource,
        UpdateStreamSource, decode_telegram_update_json_slice,
    };
    use serde_json::{Value, json};

    use super::{
        GetUpdatesExecutor, LONG_POLL_TIMEOUT, LongPollUpdateSource, TELEGRAM_WEBHOOK_PATH,
        TELEGRAM_WEBHOOK_SECRET_HEADER, WebhookSetup, WebhookUpdateRequestError,
        build_delete_webhook_method, build_get_updates_method,
        build_get_updates_method_with_offset, build_set_webhook_method, long_poll_batch,
    };

    #[test]
    fn get_updates_method_requests_only_product_updates() {
        let payload = serde_json::to_value(build_get_updates_method()).expect("getUpdates JSON");

        assert_eq!(payload.get("offset"), Some(&json!(0)));
        assert_eq!(payload.get("timeout"), Some(&json!(60)));
        assert_eq!(
            allowed_update_names(&payload),
            expected_product_allowed_update_names()
        );
        assert!(
            payload.get("limit").is_none(),
            "Zero update limit stays unset"
        );
        assert_eq!(LONG_POLL_TIMEOUT.as_secs(), 60);
    }

    #[test]
    fn delete_webhook_method_matches_go_long_poll_startup_contract() {
        let payload =
            serde_json::to_value(build_delete_webhook_method()).expect("deleteWebhook JSON");

        assert_eq!(payload.get("drop_pending_updates"), Some(&Value::Null));
    }

    #[tokio::test]
    async fn set_webhook_method_requests_only_product_updates() {
        let setup = WebhookSetup::new("https://plotva.example/tg", Some("secret-token".to_owned()));
        let payload = crate::test_api::bot_api_payload(build_set_webhook_method(&setup))
            .await
            .expect("setWebhook payload");

        assert_eq!(
            payload.get("url"),
            Some(&json!("https://plotva.example/tg"))
        );
        assert_eq!(payload.get("secret_token"), Some(&json!("secret-token")));
        assert_eq!(
            allowed_update_names(&payload),
            expected_product_allowed_update_names()
        );
        assert_eq!(TELEGRAM_WEBHOOK_PATH, "/telegram/webhook");
    }

    #[tokio::test]
    async fn set_webhook_method_omits_empty_secret_like_go_add_non_empty() {
        let setup = WebhookSetup::new("https://plotva.example/tg", Some(String::new()));
        let payload = crate::test_api::bot_api_payload(build_set_webhook_method(&setup))
            .await
            .expect("setWebhook payload");

        assert!(payload.get("secret_token").is_none());
    }

    #[tokio::test]
    async fn long_poll_source_uses_go_offsets_and_yields_buffered_updates()
    -> Result<(), Box<dyn Error>> {
        let client = PollClientStub::new(vec![
            PollAction::Updates(vec![sample_message_update(10), sample_message_update(11)]),
            PollAction::Updates(vec![sample_message_update(12)]),
        ]);
        let source = LongPollUpdateSource::new(client.clone()).with_retry_delay(Duration::ZERO);

        let first = source.next_update().await.ok_or("expected first update")?;
        let second = source
            .next_update()
            .await
            .ok_or("expected buffered update")?;
        let third = source
            .next_update()
            .await
            .ok_or("expected next polled update")?;

        assert_eq!([first.id, second.id, third.id], [10, 11, 12]);
        assert_eq!(request_offsets(&client.requests()), vec![0, 12]);
        Ok(())
    }

    #[tokio::test]
    async fn long_poll_source_skips_updates_below_current_offset_like_go()
    -> Result<(), Box<dyn Error>> {
        let client = PollClientStub::new(vec![
            PollAction::Updates(vec![sample_message_update(10)]),
            PollAction::Updates(vec![sample_message_update(9), sample_message_update(11)]),
        ]);
        let source = LongPollUpdateSource::new(client.clone()).with_retry_delay(Duration::ZERO);

        let first = source.next_update().await.ok_or("expected first update")?;
        let second = source
            .next_update()
            .await
            .ok_or("expected non-stale update")?;

        assert_eq!([first.id, second.id], [10, 11]);
        assert_eq!(request_offsets(&client.requests()), vec![0, 11]);
        Ok(())
    }

    #[tokio::test]
    async fn long_poll_source_retries_after_poll_errors_like_go() -> Result<(), Box<dyn Error>> {
        let client = PollClientStub::new(vec![
            PollAction::Error("temporary telegram error"),
            PollAction::Updates(vec![sample_message_update(10)]),
        ]);
        let source = LongPollUpdateSource::new(client.clone()).with_retry_delay(Duration::ZERO);

        let update = source
            .next_update()
            .await
            .ok_or("expected retried update")?;

        assert_eq!(update.id, 10);
        assert_eq!(request_offsets(&client.requests()), vec![0, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn long_poll_source_decodes_with_the_shared_decoder() -> Result<(), Box<dyn Error>> {
        let client = PollClientStub::new(vec![
            PollAction::Updates(vec![gif_animation_update(11)]),
            PollAction::Updates(vec![sample_message_update(12)]),
        ]);
        let source = LongPollUpdateSource::new(client.clone()).with_retry_delay(Duration::ZERO);

        let gif = source.next_update().await.ok_or("expected GIF update")?;
        let text = source.next_update().await.ok_or("expected text update")?;

        assert_eq!([gif.id, text.id], [11, 12]);
        assert!(is_animation(&gif));
        assert_eq!(request_offsets(&client.requests()), vec![0, 12]);
        Ok(())
    }

    #[tokio::test]
    async fn carapax_get_updates_returns_raw_bot_api_json() -> Result<(), Box<dyn Error>> {
        let gif = gif_animation_update(21);
        let response = json!({"ok": true, "result": [gif.clone()]}).to_string();
        let (host, server) = crate::test_api::serve_one_bot_api_response("200 OK", response)?;
        let client = carapax::api::Client::new("fixture")?
            .with_host(host)
            .with_max_retries(0);

        let updates = client
            .get_updates(build_get_updates_method_with_offset(21))
            .await?;
        let request = server.join().map_err(|_| "fixture server panicked")??;

        assert_eq!(updates, vec![gif]);
        assert!(String::from_utf8_lossy(&request).starts_with("POST /botfixture/getUpdates "));
        Ok(())
    }

    #[test]
    fn long_poll_batch_keeps_raw_bot_api_json_for_the_shared_decoder() -> Result<(), Box<dyn Error>>
    {
        let gif = gif_animation_update(21);

        let batch = long_poll_batch(
            vec![
                sample_message_update(20),
                gif.clone(),
                sample_message_update(22),
            ],
            21,
            77,
            None,
            1_710_000_000_000,
        );

        assert_eq!(batch.fresh, 2);
        assert_eq!(batch.next_cursor, 23);
        assert!(batch.serialization_errors.is_empty());
        let [gif_entry, text_entry] = batch.appends.as_slice() else {
            return Err(format!("expected 2 stream entries, got {}", batch.appends.len()).into());
        };
        assert_eq!(gif_entry.bot_id, 77);
        assert_eq!(gif_entry.update_id, Some(21));
        assert_eq!(gif_entry.source, UpdateStreamSource::LongPoll);
        assert_eq!(gif_entry.received_at_unix_ms, 1_710_000_000_000);
        assert_eq!(
            serde_json::from_slice::<Value>(&gif_entry.raw_payload)?,
            gif
        );
        assert!(is_animation(&decode_telegram_update_json_slice(
            &gif_entry.raw_payload
        )?));
        assert_eq!(text_entry.update_id, Some(22));
        Ok(())
    }

    #[test]
    fn long_poll_batch_skips_stale_updates_before_the_ingress_guard() {
        let guard = UpdateIngressGuard::new(UpdateIngressGuardConfig {
            short_window: Duration::from_secs(10),
            short_limit: 2,
            long_window: Duration::from_secs(60),
            long_limit: 100,
            block_duration: Duration::from_secs(300),
        });

        let batch = long_poll_batch(
            vec![
                sample_message_update(9),
                sample_message_update(10),
                sample_message_update(11),
            ],
            10,
            77,
            Some(&guard),
            0,
        );

        assert_eq!(batch.fresh, 2);
        assert_eq!(batch.dropped_by_ingress_guard, 1);
        assert_eq!(batch.next_cursor, 12);
        assert_eq!(
            batch
                .appends
                .iter()
                .map(|append| append.update_id)
                .collect::<Vec<_>>(),
            vec![Some(10)]
        );
    }

    #[tokio::test]
    async fn webhook_request_rejects_method_and_secret_before_redis_io()
    -> Result<(), Box<dyn Error>> {
        let client = redis::Client::open("redis://127.0.0.1:1/0")?;
        let stream = openplotva_updates::RedisUpdateStream::with_key(
            client,
            "openplotva:webhook-security-test",
        );
        let sender = super::webhook_update_stream(stream, 77);

        let wrong_method = sender
            .handle_webhook_request("GET", Some("secret"), "secret", b"{}")
            .await
            .expect_err("method rejected");
        let wrong_secret = sender
            .handle_webhook_request("POST", Some("wrong"), "secret", b"{}")
            .await
            .expect_err("secret rejected");
        let missing_secret = sender
            .handle_webhook_request("POST", None, "secret", b"{}")
            .await
            .expect_err("missing secret rejected");
        let empty_secret = sender
            .handle_webhook_request("POST", Some(""), "", b"{}")
            .await
            .expect_err("empty configured secret rejected");

        assert_eq!(wrong_method, WebhookUpdateRequestError::MethodNotAllowed);
        assert_eq!(wrong_method.http_status(), 405);
        assert_eq!(wrong_secret, WebhookUpdateRequestError::Unauthorized);
        assert_eq!(wrong_secret.http_status(), 401);
        assert_eq!(missing_secret, WebhookUpdateRequestError::Unauthorized);
        assert_eq!(empty_secret, WebhookUpdateRequestError::Unauthorized);
        assert_eq!(
            TELEGRAM_WEBHOOK_SECRET_HEADER,
            "X-Telegram-Bot-Api-Secret-Token"
        );
        assert_eq!(sender.ingress_snapshot(), Default::default());
        Ok(())
    }

    #[tokio::test]
    async fn webhook_ingress_metrics_record_append_timeouts() -> Result<(), Box<dyn Error>> {
        let client = redis::Client::open("redis://127.0.0.1:1/0")?;
        let stream = openplotva_updates::RedisUpdateStream::with_key(
            client,
            "openplotva:webhook-metrics-test",
        );
        let sender = super::webhook_update_stream(stream, 77).with_send_timeout(Duration::ZERO);

        let error = sender
            .handle_webhook_request("POST", Some("secret"), "secret", b"{}")
            .await
            .expect_err("zero timeout must fail");

        assert_eq!(error, WebhookUpdateRequestError::ServiceUnavailable);
        let snapshot = sender.ingress_snapshot();
        assert_eq!(snapshot.append_attempts, 1);
        assert_eq!(snapshot.append_succeeded, 0);
        assert_eq!(snapshot.append_failed, 1);
        assert_eq!(snapshot.append_timed_out, 1);
        assert_eq!(snapshot.append_in_flight, 0);
        assert!(snapshot.last_failure_unix_ms.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn webhook_flood_drop_returns_success_without_touching_redis()
    -> Result<(), Box<dyn Error>> {
        let client = redis::Client::open("redis://127.0.0.1:1/0")?;
        let stream = openplotva_updates::RedisUpdateStream::with_key(
            client,
            "openplotva:webhook-flood-test",
        );
        let guard = Arc::new(openplotva_updates::UpdateIngressGuard::new(
            openplotva_updates::UpdateIngressGuardConfig {
                short_window: Duration::from_secs(10),
                short_limit: 1,
                long_window: Duration::from_secs(60),
                long_limit: 100,
                block_duration: Duration::from_secs(300),
            },
        ));
        let sender = super::webhook_update_stream(stream, 77).with_ingress_guard(guard);
        let body = serde_json::to_vec(&sample_message_update(10))?;

        sender
            .handle_webhook_request("POST", Some("secret"), "secret", &body)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_webhook_stream_acknowledges_only_after_untrimmed_xadd()
    -> Result<(), Box<dyn Error>> {
        let Ok(url) = std::env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let key = format!("openplotva:webhook-stream-test:{}", std::process::id());
        let stream = openplotva_updates::RedisUpdateStream::with_key(client.clone(), &key);
        let sender = super::webhook_update_stream(stream.clone(), 77);

        sender
            .handle_webhook_request("POST", Some("secret"), "secret", b"not-json")
            .await?;
        assert_eq!(stream.stats().await?.length, 1);

        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_webhook_and_long_poll_store_the_same_raw_update() -> Result<(), Box<dyn Error>> {
        let Ok(url) = std::env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let prefix = format!("openplotva:ingress-parity-test:{}", std::process::id());
        let webhook_key = format!("{prefix}:webhook");
        let long_poll_key = format!("{prefix}:long-poll");
        let webhook_stream = RedisUpdateStream::with_key(client.clone(), &webhook_key);
        let long_poll_stream = RedisUpdateStream::with_key(client.clone(), &long_poll_key);
        webhook_stream.ensure_consumer_group().await?;
        long_poll_stream.ensure_consumer_group().await?;
        let gif = gif_animation_update(31);

        super::webhook_update_stream(webhook_stream.clone(), 77)
            .handle_webhook_request("POST", Some("secret"), "secret", &serde_json::to_vec(&gif)?)
            .await?;
        let poll = PollClientStub::new(vec![
            PollAction::Updates(vec![gif.clone()]),
            PollAction::Pending,
        ]);
        let report =
            super::run_long_poll_stream_producer_until(&poll, &long_poll_stream, 77, async {
                while long_poll_stream
                    .stats()
                    .await
                    .map_or(true, |stats| stats.length == 0)
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;

        let webhook_entries = webhook_stream
            .read_group("parity", Duration::ZERO, 10)
            .await?;
        let long_poll_entries = long_poll_stream
            .read_group("parity", Duration::ZERO, 10)
            .await?;
        let cursor = long_poll_stream.long_poll_cursor().await?;
        let mut connection = client.get_multiplexed_async_connection().await?;
        let _: usize = redis::cmd("DEL")
            .arg(&webhook_key)
            .arg(&long_poll_key)
            .arg(format!("{long_poll_key}:long-poll-cursor"))
            .query_async(&mut connection)
            .await?;

        assert_eq!(report.appended, 1);
        assert_eq!(cursor, 32);
        let ([webhook_entry], [long_poll_entry]) =
            (webhook_entries.as_slice(), long_poll_entries.as_slice())
        else {
            return Err("expected one entry in each stream".into());
        };
        let webhook_entry = webhook_entry.decode()?;
        let long_poll_entry = long_poll_entry.decode()?;
        assert_eq!(webhook_entry.source, UpdateStreamSource::Webhook);
        assert_eq!(long_poll_entry.source, UpdateStreamSource::LongPoll);
        assert_eq!(webhook_entry.update_id, long_poll_entry.update_id);
        assert_eq!(
            serde_json::from_slice::<Value>(&webhook_entry.raw_payload)?,
            serde_json::from_slice::<Value>(&long_poll_entry.raw_payload)?
        );
        assert!(is_animation(&decode_telegram_update_json_slice(
            &long_poll_entry.raw_payload
        )?));
        Ok(())
    }

    fn allowed_update_names(payload: &Value) -> BTreeSet<&str> {
        payload
            .get("allowed_updates")
            .and_then(Value::as_array)
            .expect("allowed_updates array")
            .iter()
            .map(|value| value.as_str().expect("allowed update name"))
            .collect()
    }

    fn expected_product_allowed_update_names() -> BTreeSet<&'static str> {
        BTreeSet::from([
            "callback_query",
            "channel_post",
            "chat_member",
            "edited_channel_post",
            "edited_message",
            "guest_message",
            "inline_query",
            "message",
            "my_chat_member",
            "pre_checkout_query",
        ])
    }

    #[derive(Clone)]
    struct PollClientStub {
        actions: Arc<Mutex<VecDeque<PollAction>>>,
        requests: Arc<Mutex<Vec<Value>>>,
    }

    impl PollClientStub {
        fn new(actions: Vec<PollAction>) -> Self {
            Self {
                actions: Arc::new(Mutex::new(actions.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<Value> {
            self.requests.lock().expect("poll requests").clone()
        }
    }

    enum PollAction {
        Updates(Vec<Value>),
        Error(&'static str),
        /// Hold the poll open, like Telegram with no new updates.
        Pending,
    }

    #[derive(Debug)]
    struct PollError(&'static str);

    impl fmt::Display for PollError {
        fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
            out.write_str(self.0)
        }
    }

    impl Error for PollError {}

    impl super::GetUpdatesExecutor for PollClientStub {
        type Error = PollError;

        fn get_updates<'a>(
            &'a self,
            method: super::GetUpdates,
        ) -> super::GetUpdatesFuture<'a, Self::Error> {
            self.requests
                .lock()
                .expect("poll requests")
                .push(serde_json::to_value(&method).expect("getUpdates request JSON"));
            let action = self
                .actions
                .lock()
                .expect("poll actions")
                .pop_front()
                .expect("scripted poll action");

            Box::pin(async move {
                match action {
                    PollAction::Updates(updates) => Ok(updates),
                    PollAction::Error(error) => Err(PollError(error)),
                    PollAction::Pending => std::future::pending().await,
                }
            })
        }
    }

    fn request_offsets(requests: &[Value]) -> Vec<i64> {
        requests
            .iter()
            .map(|request| {
                request
                    .get("offset")
                    .and_then(Value::as_i64)
                    .expect("request offset")
            })
            .collect()
    }

    fn sample_message_update(update_id: i64) -> Value {
        json!({
            "update_id": update_id,
            "message": {
                "message_id": 77,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": "/start hello"
            }
        })
    }

    /// GIF message as the Bot API sends it: `animation` plus a `document`
    /// mirror for old clients.
    fn gif_animation_update(update_id: i64) -> Value {
        json!({
            "update_id": update_id,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "from": {"id": 99, "is_bot": false, "first_name": "Ada"},
                "animation": {
                    "file_id": "gif-file",
                    "file_unique_id": "gif-unique",
                    "width": 320,
                    "height": 240,
                    "duration": 3,
                    "file_name": "clip.gif.mp4",
                    "mime_type": "video/mp4",
                    "file_size": 1024
                },
                "document": {
                    "file_id": "gif-file",
                    "file_unique_id": "gif-unique",
                    "file_name": "clip.gif.mp4",
                    "mime_type": "video/mp4",
                    "file_size": 1024
                }
            }
        })
    }

    fn is_animation(update: &TelegramUpdate) -> bool {
        matches!(
            update.get_message().map(|message| &message.data),
            Some(MessageData::Animation(_))
        )
    }
}
