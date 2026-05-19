//! Telegram update startup method builders and sources.

use std::{
    collections::{HashSet, VecDeque},
    fmt,
    future::Future,
    pin::Pin,
    time::Duration,
};

use carapax::types::{
    AllowedUpdate, DeleteWebhook, GetUpdates, SetWebhook, Update as TelegramUpdate,
};
use openplotva_updates::{UpdateProducerSource, UpdateProducerSourceFuture};
use thiserror::Error;
use tokio::{
    sync::{Mutex, mpsc},
    time::{sleep, timeout},
};

/// Go `StartUpdateLoop` long-poll timeout.
pub const GO_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(60);

/// Go `GetUpdatesChan` retry delay after a failed poll.
pub const GO_LONG_POLL_RETRY_DELAY: Duration = Duration::from_secs(3);

/// Go webhook route registered by `ListenForUpdates`.
pub const TELEGRAM_WEBHOOK_PATH: &str = "/telegram/webhook";

/// Go webhook secret-token header checked by `ListenForUpdates`.
pub const TELEGRAM_WEBHOOK_SECRET_HEADER: &str = "X-Telegram-Bot-Api-Secret-Token";

/// Go `botAPI.Buffer` value set in `cmd/main.go`.
pub const GO_WEBHOOK_UPDATE_BUFFER_SIZE: usize = 1_000_000;

/// Go timeout for accepting one webhook update into the update channel.
pub const GO_WEBHOOK_UPDATE_SEND_TIMEOUT: Duration = Duration::from_millis(1_500);

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
    /// Create a long-polling source using Go's retry delay.
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

/// Source side of Go's webhook update channel.
pub struct WebhookUpdateSource {
    receiver: Mutex<mpsc::Receiver<TelegramUpdate>>,
}

/// Sender side of Go's webhook update channel.
#[derive(Clone, Debug)]
pub struct WebhookUpdateSender {
    sender: mpsc::Sender<TelegramUpdate>,
    send_timeout: Duration,
}

/// Error returned while accepting a webhook update into the in-memory channel.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WebhookUpdateSendError {
    /// The receiver side has been closed.
    #[error("webhook update receiver is closed")]
    Closed,
    /// The update channel stayed full until the Go timeout elapsed.
    #[error("webhook update channel is full")]
    Timeout,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum WebhookUpdateRequestError {
    /// Only POST is accepted.
    #[error("method not allowed")]
    MethodNotAllowed,
    /// The Telegram secret-token header did not match configured webhook secret.
    #[error("unauthorized")]
    Unauthorized,
    /// Request body could not be parsed as a Telegram update.
    #[error("invalid update")]
    InvalidUpdate,
    /// The update channel stayed full until the Go timeout elapsed.
    #[error("service unavailable")]
    ServiceUnavailable,
}

impl WebhookUpdateRequestError {
    /// HTTP status code returned by Go `ListenForUpdates`.
    pub const fn http_status(self) -> u16 {
        match self {
            Self::MethodNotAllowed => 405,
            Self::Unauthorized => 401,
            Self::InvalidUpdate => 400,
            Self::ServiceUnavailable => 503,
        }
    }

    /// Error body returned by Go for invalid update payloads.
    pub const fn error_body(self) -> Option<&'static str> {
        match self {
            Self::InvalidUpdate => Some(r#"{"error":"invalid update"}"#),
            Self::MethodNotAllowed | Self::Unauthorized | Self::ServiceUnavailable => None,
        }
    }
}

/// Build Go's webhook update channel pair.
pub fn webhook_update_channel(buffer_size: usize) -> (WebhookUpdateSender, WebhookUpdateSource) {
    let (sender, receiver) = mpsc::channel(buffer_size);
    (
        WebhookUpdateSender {
            sender,
            send_timeout: GO_WEBHOOK_UPDATE_SEND_TIMEOUT,
        },
        WebhookUpdateSource {
            receiver: Mutex::new(receiver),
        },
    )
}

impl WebhookUpdateSender {
    pub fn with_send_timeout(mut self, send_timeout: Duration) -> Self {
        self.send_timeout = send_timeout;
        self
    }

    /// Accept one parsed Telegram update into the webhook channel.
    pub async fn accept_update(
        &self,
        update: TelegramUpdate,
    ) -> Result<(), WebhookUpdateSendError> {
        timeout(self.send_timeout, self.sender.send(update))
            .await
            .map_err(|_| WebhookUpdateSendError::Timeout)?
            .map_err(|_| WebhookUpdateSendError::Closed)
    }

    /// Validate and accept one Telegram webhook request like Go `ListenForUpdates`.
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

        let update: TelegramUpdate =
            serde_json::from_slice(body).map_err(|_| WebhookUpdateRequestError::InvalidUpdate)?;
        self.accept_update(update)
            .await
            .map_err(|_| WebhookUpdateRequestError::ServiceUnavailable)
    }
}

impl WebhookUpdateSource {
    async fn next_update_inner(&self) -> Option<TelegramUpdate> {
        self.receiver.lock().await.recv().await
    }

    /// Return the next buffered update without waiting.
    pub async fn next_update_now(&self) -> Option<TelegramUpdate> {
        self.receiver.lock().await.try_recv().ok()
    }
}

impl UpdateProducerSource for WebhookUpdateSource {
    fn next_update<'a>(&'a self) -> UpdateProducerSourceFuture<'a> {
        Box::pin(self.next_update_inner())
    }
}

/// Minimal webhook setup inputs used by Go `StartWebhookServer`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WebhookSetup {
    /// Public Telegram webhook URL.
    pub url: String,
    /// Optional `X-Telegram-Bot-Api-Secret-Token` value.
    pub secret_token: Option<String>,
}

impl WebhookSetup {
    /// Build setup inputs from a URL and optional secret token.
    pub fn new(url: impl Into<String>, secret_token: Option<String>) -> Self {
        Self {
            url: url.into(),
            secret_token,
        }
    }
}

/// Return the Go allowed-update list as a native `carapax` set.
pub fn go_allowed_update_set() -> HashSet<AllowedUpdate> {
    openplotva_updates::GO_ALLOWED_UPDATES
        .iter()
        .copied()
        .collect()
}

/// Build Go's long-polling `getUpdates` method.
pub fn build_get_updates_method() -> GetUpdates {
    build_get_updates_method_with_offset(0)
}

/// Build Go's long-polling `getUpdates` method with an explicit offset.
pub fn build_get_updates_method_with_offset(offset: i64) -> GetUpdates {
    GetUpdates::default()
        .with_offset(offset)
        .with_timeout(GO_LONG_POLL_TIMEOUT)
        .with_allowed_updates(go_allowed_update_set())
}

/// Build Go's webhook deletion method used before long polling.
pub fn build_delete_webhook_method() -> DeleteWebhook {
    DeleteWebhook::default()
}

/// Build Go's webhook setup method.
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
        GO_LONG_POLL_TIMEOUT, GO_WEBHOOK_UPDATE_BUFFER_SIZE, GO_WEBHOOK_UPDATE_SEND_TIMEOUT,
        LongPollUpdateSource, TELEGRAM_WEBHOOK_PATH, TELEGRAM_WEBHOOK_SECRET_HEADER, WebhookSetup,
        WebhookUpdateRequestError, build_delete_webhook_method, build_get_updates_method,
        build_set_webhook_method, webhook_update_channel,
    };

    #[test]
    fn get_updates_method_matches_go_long_poll_startup_contract() {
        let payload = serde_json::to_value(build_get_updates_method()).expect("getUpdates JSON");

        assert_eq!(payload.get("offset"), Some(&json!(0)));
        assert_eq!(payload.get("timeout"), Some(&json!(60)));
        assert_eq!(
            allowed_update_names(&payload),
            openplotva_updates::GO_ALLOWED_UPDATE_NAMES
                .iter()
                .copied()
                .collect()
        );
        assert!(
            payload.get("limit").is_none(),
            "Go NewUpdate(0) leaves limit unset"
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
        assert_eq!(
            allowed_update_names(&payload),
            openplotva_updates::GO_ALLOWED_UPDATE_NAMES
                .iter()
                .copied()
                .collect()
        );
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
    async fn webhook_update_source_yields_accepted_updates_in_fifo_order()
    -> Result<(), Box<dyn Error>> {
        let (sender, source) = webhook_update_channel(2);

        sender.accept_update(sample_message_update(10)?).await?;
        sender.accept_update(sample_message_update(11)?).await?;

        let first = source.next_update().await.ok_or("expected first update")?;
        let second = source.next_update().await.ok_or("expected second update")?;

        assert_eq!([first.id, second.id], [10, 11]);
        assert_eq!(GO_WEBHOOK_UPDATE_BUFFER_SIZE, 1_000_000);
        assert_eq!(GO_WEBHOOK_UPDATE_SEND_TIMEOUT.as_millis(), 1_500);
        Ok(())
    }

    #[tokio::test]
    async fn webhook_request_rejects_non_post_and_wrong_secret_like_go()
    -> Result<(), Box<dyn Error>> {
        let (sender, source) = webhook_update_channel(1);
        let body = serde_json::to_vec(&sample_message_update(10)?)?;

        let wrong_method = sender
            .handle_webhook_request("GET", Some("secret"), "secret", &body)
            .await
            .expect_err("method rejected");
        let wrong_secret = sender
            .handle_webhook_request("POST", Some("wrong"), "secret", &body)
            .await
            .expect_err("secret rejected");

        assert_eq!(wrong_method, WebhookUpdateRequestError::MethodNotAllowed);
        assert_eq!(wrong_method.http_status(), 405);
        assert_eq!(wrong_secret, WebhookUpdateRequestError::Unauthorized);
        assert_eq!(wrong_secret.http_status(), 401);
        assert!(source.next_update_now().await.is_none());
        assert_eq!(
            TELEGRAM_WEBHOOK_SECRET_HEADER,
            "X-Telegram-Bot-Api-Secret-Token"
        );
        Ok(())
    }

    #[tokio::test]
    async fn webhook_request_accepts_empty_secret_and_valid_json_like_go()
    -> Result<(), Box<dyn Error>> {
        let (sender, source) = webhook_update_channel(1);
        let body = serde_json::to_vec(&sample_message_update(10)?)?;

        sender
            .handle_webhook_request("POST", None, "", &body)
            .await?;

        let update = source.next_update().await.ok_or("expected update")?;
        assert_eq!(update.id, 10);
        Ok(())
    }

    #[tokio::test]
    async fn webhook_request_reports_invalid_update_and_full_channel_like_go()
    -> Result<(), Box<dyn Error>> {
        let (sender, source) = webhook_update_channel(1);
        let sender = sender.with_send_timeout(Duration::from_millis(1));
        let body = serde_json::to_vec(&sample_message_update(10)?)?;

        sender
            .handle_webhook_request("POST", Some("secret"), "secret", &body)
            .await?;
        let invalid = sender
            .handle_webhook_request("POST", Some("secret"), "secret", b"not-json")
            .await
            .expect_err("invalid update rejected");
        let full = sender
            .handle_webhook_request("POST", Some("secret"), "secret", &body)
            .await
            .expect_err("full channel rejected");

        assert_eq!(invalid.http_status(), 400);
        assert_eq!(invalid.error_body(), Some(r#"{"error":"invalid update"}"#));
        assert_eq!(full, WebhookUpdateRequestError::ServiceUnavailable);
        assert_eq!(full.http_status(), 503);
        assert_eq!(source.next_update().await.ok_or("queued update")?.id, 10);
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
