//! Telegram update startup method builders and sources.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
    future::Future,
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use carapax::types::{
    AllowedUpdate, DeleteWebhook, GetUpdates, SetWebhook, Update as TelegramUpdate,
};
use openplotva_updates::{
    RedisUpdateStream, UpdateProducerSource, UpdateProducerSourceFuture, UpdateStreamAppend,
    UpdateStreamSource,
};
use thiserror::Error;
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};

pub const GO_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(60);

pub const GO_LONG_POLL_RETRY_DELAY: Duration = Duration::from_secs(3);

pub const TELEGRAM_WEBHOOK_PATH: &str = "/telegram/webhook";

pub const TELEGRAM_WEBHOOK_SECRET_HEADER: &str = "X-Telegram-Bot-Api-Secret-Token";

pub const WEBHOOK_STREAM_APPEND_TIMEOUT: Duration = Duration::from_secs(3);

const LONG_POLL_STREAM_MAX_RECORDED_ERRORS: usize = 64;

/// Boxed future returned by Telegram `getUpdates` executors.
pub type GetUpdatesFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Vec<TelegramUpdate>, E>> + Send + 'a>>;

/// Minimal capability needed by the long-poll update source.
pub trait GetUpdatesExecutor {
    /// Error returned by the concrete Telegram client.
    type Error: fmt::Display + Send;

    /// Execute one Telegram `getUpdates` request.
    fn get_updates<'a>(&'a self, method: GetUpdates) -> GetUpdatesFuture<'a, Self::Error>;
}

impl GetUpdatesExecutor for carapax::api::Client {
    type Error = carapax::api::ExecuteError;

    fn get_updates<'a>(&'a self, method: GetUpdates) -> GetUpdatesFuture<'a, Self::Error> {
        Box::pin(self.execute(method))
    }
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
            retry_delay: GO_LONG_POLL_RETRY_DELAY,
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

    async fn push_polled_updates(&self, updates: Vec<TelegramUpdate>) {
        let mut state = self.state.lock().await;
        for update in updates {
            if update.id >= state.offset {
                state.offset = update.id + 1;
                state.buffered.push_back(update);
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
                        () = sleep(GO_LONG_POLL_RETRY_DELAY) => {}
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
                    () = sleep(GO_LONG_POLL_RETRY_DELAY) => {}
                }
                continue;
            }
        };
        report.polled_batches = report.polled_batches.saturating_add(1);
        let updates = updates
            .into_iter()
            .filter(|update| update.id >= cursor)
            .collect::<Vec<_>>();
        if updates.is_empty() {
            continue;
        }
        report.received = report.received.saturating_add(updates.len());
        let received_at_unix_ms = unix_millis_now();
        let mut appends = Vec::with_capacity(updates.len());
        let mut next_cursor = cursor;
        let mut serialization_failed = false;
        for update in &updates {
            match serde_json::to_vec(update) {
                Ok(raw_payload) => {
                    appends.push(UpdateStreamAppend {
                        bot_id,
                        update_id: Some(update.id),
                        source: UpdateStreamSource::LongPoll,
                        received_at_unix_ms,
                        raw_payload,
                    });
                    next_cursor = next_cursor.max(update.id.saturating_add(1));
                }
                Err(error) => {
                    serialization_failed = true;
                    record_limited_error(
                        &mut report.serialization_errors,
                        format!("update {}: {error}", update.id),
                    );
                }
            }
        }
        if serialization_failed {
            tokio::select! {
                _ = &mut stop => break,
                () = sleep(GO_LONG_POLL_RETRY_DELAY) => {}
            }
            continue;
        }

        loop {
            let appended = tokio::select! {
                _ = &mut stop => return report,
                result = stream.append_long_poll_batch(&appends, next_cursor) => result,
            };
            match appended {
                Ok(ids) => {
                    report.appended = report.appended.saturating_add(ids.len());
                    cursor = next_cursor;
                    break;
                }
                Err(error) => {
                    record_limited_error(&mut report.append_errors, error.to_string());
                    tokio::select! {
                        _ = &mut stop => return report,
                        () = sleep(GO_LONG_POLL_RETRY_DELAY) => {}
                    }
                }
            }
        }
    }
    report
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
    }
}

impl WebhookUpdateSender {
    pub fn with_send_timeout(mut self, send_timeout: Duration) -> Self {
        self.send_timeout = send_timeout;
        self
    }

    /// Append one parsed Telegram update to the durable ingress Stream.
    pub async fn accept_update(
        &self,
        update: TelegramUpdate,
    ) -> Result<(), WebhookUpdateSendError> {
        let raw_payload =
            serde_json::to_vec(&update).map_err(|_| WebhookUpdateSendError::Stream)?;
        self.append_raw_stream_update(Some(update.id), &raw_payload)
            .await
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
        if provided_secret.unwrap_or_default() != secret_token {
            return Err(WebhookUpdateRequestError::Unauthorized);
        }

        self.append_raw_stream_update(raw_update_id(body), body)
            .await
            .map_err(|_| WebhookUpdateRequestError::ServiceUnavailable)
    }

    async fn append_raw_stream_update(
        &self,
        update_id: Option<i64>,
        body: &[u8],
    ) -> Result<(), WebhookUpdateSendError> {
        let received_at_unix_ms = unix_millis_now();
        let append = UpdateStreamAppend {
            bot_id: self.bot_id,
            update_id,
            source: UpdateStreamSource::Webhook,
            received_at_unix_ms,
            raw_payload: body.to_vec(),
        };
        timeout(self.send_timeout, self.stream.append(&append))
            .await
            .map_err(|_| WebhookUpdateSendError::Timeout)?
            .map(|_| ())
            .map_err(|error| {
                tracing::warn!(
                    bot_id = self.bot_id,
                    update_id,
                    error = %error,
                    "failed to append Telegram webhook update to Redis Stream"
                );
                WebhookUpdateSendError::Stream
            })
    }
}

fn raw_update_id(body: &[u8]) -> Option<i64> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("update_id")?
        .as_i64()
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

pub fn go_allowed_update_set() -> HashSet<AllowedUpdate> {
    [
        AllowedUpdate::BotStatus,
        AllowedUpdate::BusinessConnection,
        AllowedUpdate::BusinessMessage,
        AllowedUpdate::CallbackQuery,
        AllowedUpdate::ChannelPost,
        AllowedUpdate::ChatBoostRemoved,
        AllowedUpdate::ChatBoostUpdated,
        AllowedUpdate::ChatJoinRequest,
        AllowedUpdate::ChosenInlineResult,
        AllowedUpdate::DeletedBusinessMessages,
        AllowedUpdate::EditedBusinessMessage,
        AllowedUpdate::EditedChannelPost,
        AllowedUpdate::EditedMessage,
        AllowedUpdate::GuestMessage,
        AllowedUpdate::InlineQuery,
        AllowedUpdate::Message,
        AllowedUpdate::MessageReaction,
        AllowedUpdate::MessageReactionCount,
        AllowedUpdate::Poll,
        AllowedUpdate::PollAnswer,
        AllowedUpdate::PreCheckoutQuery,
        AllowedUpdate::PurchasedPaidMedia,
        AllowedUpdate::ShippingQuery,
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
        .with_timeout(GO_LONG_POLL_TIMEOUT)
        .with_allowed_updates(go_allowed_update_set())
}

pub fn build_delete_webhook_method() -> DeleteWebhook {
    DeleteWebhook::default()
}

pub fn build_set_webhook_method(setup: &WebhookSetup) -> SetWebhook {
    let mut method =
        SetWebhook::new(setup.url.clone()).with_allowed_updates(go_allowed_update_set());

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

    use carapax::types::Update as TelegramUpdate;
    use openplotva_updates::UpdateProducerSource;
    use serde_json::{Value, json};

    use super::{
        GO_LONG_POLL_TIMEOUT, LongPollUpdateSource, TELEGRAM_WEBHOOK_PATH,
        TELEGRAM_WEBHOOK_SECRET_HEADER, WebhookSetup, WebhookUpdateRequestError,
        build_delete_webhook_method, build_get_updates_method, build_set_webhook_method,
    };

    #[test]
    fn get_updates_method_matches_go_long_poll_startup_contract() {
        let payload = serde_json::to_value(build_get_updates_method()).expect("getUpdates JSON");

        assert_eq!(payload.get("offset"), Some(&json!(0)));
        assert_eq!(payload.get("timeout"), Some(&json!(60)));
        let allowed = allowed_update_names(&payload);
        assert!(allowed.contains("message"));
        assert!(allowed.contains("channel_post"));
        assert!(allowed.contains("message_reaction"));
        assert!(allowed.contains("shipping_query"));
        assert!(
            payload.get("limit").is_none(),
            "Zero update limit stays unset"
        );
        assert_eq!(GO_LONG_POLL_TIMEOUT.as_secs(), 60);
    }

    #[test]
    fn delete_webhook_method_matches_go_long_poll_startup_contract() {
        let payload =
            serde_json::to_value(build_delete_webhook_method()).expect("deleteWebhook JSON");

        assert_eq!(payload.get("drop_pending_updates"), Some(&Value::Null));
    }

    #[test]
    fn set_webhook_method_matches_go_webhook_startup_contract() {
        let setup = WebhookSetup::new("https://plotva.example/tg", Some("secret-token".to_owned()));
        let payload =
            serde_json::to_value(build_set_webhook_method(&setup)).expect("setWebhook JSON");

        assert_eq!(
            payload.get("url"),
            Some(&json!("https://plotva.example/tg"))
        );
        assert_eq!(payload.get("secret_token"), Some(&json!("secret-token")));
        let allowed = allowed_update_names(&payload);
        assert!(allowed.contains("message"));
        assert!(allowed.contains("business_connection"));
        assert!(allowed.contains("purchased_paid_media"));
        assert_eq!(TELEGRAM_WEBHOOK_PATH, "/telegram/webhook");
    }

    #[test]
    fn set_webhook_method_omits_empty_secret_like_go_add_non_empty() {
        let setup = WebhookSetup::new("https://plotva.example/tg", Some(String::new()));
        let payload =
            serde_json::to_value(build_set_webhook_method(&setup)).expect("setWebhook JSON");

        assert!(payload.get("secret_token").is_none());
    }

    #[tokio::test]
    async fn long_poll_source_uses_go_offsets_and_yields_buffered_updates()
    -> Result<(), Box<dyn Error>> {
        let client = PollClientStub::new(vec![
            PollAction::Updates(vec![sample_message_update(10)?, sample_message_update(11)?]),
            PollAction::Updates(vec![sample_message_update(12)?]),
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
            PollAction::Updates(vec![sample_message_update(10)?]),
            PollAction::Updates(vec![sample_message_update(9)?, sample_message_update(11)?]),
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
            PollAction::Updates(vec![sample_message_update(10)?]),
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

        assert_eq!(wrong_method, WebhookUpdateRequestError::MethodNotAllowed);
        assert_eq!(wrong_method.http_status(), 405);
        assert_eq!(wrong_secret, WebhookUpdateRequestError::Unauthorized);
        assert_eq!(wrong_secret.http_status(), 401);
        assert_eq!(
            TELEGRAM_WEBHOOK_SECRET_HEADER,
            "X-Telegram-Bot-Api-Secret-Token"
        );
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

    fn allowed_update_names(payload: &Value) -> BTreeSet<&str> {
        payload
            .get("allowed_updates")
            .and_then(Value::as_array)
            .expect("allowed_updates array")
            .iter()
            .map(|value| value.as_str().expect("allowed update name"))
            .collect()
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
        Updates(Vec<TelegramUpdate>),
        Error(&'static str),
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

    fn sample_message_update(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
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
        }))
    }
}
