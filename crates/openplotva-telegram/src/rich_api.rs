//! Raw Bot API client for **Rich Messages** (Bot API 10.1).
//!
//! `carapax`/`tgbot` (0.46) do not know `sendRichMessage`/`sendRichMessageDraft`/the
//! `rich_message` parameter of `editMessageText`, and `tgbot::Payload` cannot be built
//! downstream (its constructors are `pub(crate)`), so the `Method` trait is not usable
//! for rich calls. This module issues the calls as raw JSON POSTs through `reqwest` and
//! parses the Telegram response envelope itself, surfacing `retry_after` for the
//! streaming throttle.

use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{TELEGRAM_HTTP_CONNECT_TIMEOUT, TELEGRAM_HTTP_REQUEST_TIMEOUT};

/// Telegram type returned by `sendRichMessage`.
pub type RichMessage = carapax::types::Message;

const DEFAULT_BASE_URL: &str = "https://api.telegram.org";

/// Client for the rich-message Bot API methods that `carapax` does not expose.
#[derive(Clone)]
pub struct RichApiClient {
    http: reqwest::Client,
    base_url: String,
    token: String,
}

impl fmt::Debug for RichApiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RichApiClient")
            .field("base_url", &self.base_url)
            .field("token", &format_args!("..."))
            .finish()
    }
}

/// Optional parameters shared by rich-message sends.
#[derive(Debug, Default, Clone, Deserialize, PartialEq, Serialize)]
pub struct RichSendOptions {
    pub message_thread_id: Option<i64>,
    pub reply_to_message_id: Option<i64>,
    pub allow_sending_without_reply: bool,
    pub disable_notification: bool,
    /// A serialized `InlineKeyboardMarkup` (or compatible) object.
    pub reply_markup: Option<Value>,
}

/// Concrete raw Bot API payload for queued `sendRichMessage`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SendRichMessage {
    pub chat_id: i64,
    pub html: String,
    #[serde(default)]
    pub options: RichSendOptions,
}

/// Errors raised while calling the rich-message Bot API.
#[derive(Debug, thiserror::Error)]
pub enum RichApiError {
    #[error("rich api transport error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("rich api response decode error: {0}")]
    Decode(String),
    #[error("rich api error {code}: {description}")]
    Api {
        code: i64,
        description: String,
        retry_after: Option<u64>,
    },
}

impl RichApiError {
    /// Flood-control wait hint from Telegram (`parameters.retry_after`), when present.
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            Self::Api { retry_after, .. } => *retry_after,
            _ => None,
        }
    }

    /// True when Telegram rejected an edit as a no-op ("message is not modified").
    pub fn is_not_modified(&self) -> bool {
        matches!(self, Self::Api { description, .. } if description.to_ascii_lowercase().contains("message is not modified"))
    }
}

impl RichApiClient {
    /// Build a client targeting the public Bot API host.
    pub fn new(token: impl Into<String>) -> Result<Self, RichApiError> {
        Self::with_base_url(token, "")
    }

    /// Build a client, optionally targeting a loopback/local Bot API host.
    pub fn with_base_url(
        token: impl Into<String>,
        base_url: impl AsRef<str>,
    ) -> Result<Self, RichApiError> {
        let http = reqwest::Client::builder()
            .tls_backend_rustls()
            .no_proxy()
            .connect_timeout(TELEGRAM_HTTP_CONNECT_TIMEOUT)
            .timeout(TELEGRAM_HTTP_REQUEST_TIMEOUT)
            .build()?;
        let trimmed = base_url.as_ref().trim().trim_end_matches('/');
        let base_url = if trimmed.is_empty() {
            DEFAULT_BASE_URL.to_owned()
        } else {
            trimmed.to_owned()
        };
        Ok(Self {
            http,
            base_url,
            token: token.into(),
        })
    }

    /// Send a rich message; returns the persisted [`RichMessage`].
    pub async fn send_rich_message(
        &self,
        chat_id: i64,
        html: &str,
        options: &RichSendOptions,
    ) -> Result<RichMessage, RichApiError> {
        self.call("sendRichMessage", build_send_body(chat_id, html, options))
            .await
    }

    /// Send a pre-built queued rich message.
    pub async fn send_rich_message_request(
        &self,
        request: &SendRichMessage,
    ) -> Result<RichMessage, RichApiError> {
        self.send_rich_message(request.chat_id, &request.html, &request.options)
            .await
    }

    /// Stream a partial rich message (private chats only). `draft_id` must be non-zero;
    /// reusing it animates the transition. The draft is ephemeral — finalize with
    /// [`send_rich_message`](Self::send_rich_message).
    pub async fn send_rich_message_draft(
        &self,
        chat_id: i64,
        draft_id: i64,
        html: &str,
        message_thread_id: Option<i64>,
    ) -> Result<(), RichApiError> {
        let _: Value = self
            .call(
                "sendRichMessageDraft",
                build_draft_body(chat_id, draft_id, html, message_thread_id),
            )
            .await?;
        Ok(())
    }

    /// Replace a message's content with new rich HTML in place.
    pub async fn edit_message_text_rich(
        &self,
        chat_id: i64,
        message_id: i64,
        html: &str,
        reply_markup: Option<Value>,
    ) -> Result<(), RichApiError> {
        let _: Value = self
            .call(
                "editMessageText",
                build_edit_body(chat_id, message_id, html, reply_markup),
            )
            .await?;
        Ok(())
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        body: Value,
    ) -> Result<T, RichApiError> {
        let url = format!("{}/bot{}/{}", self.base_url, self.token, method);
        let response = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|error| RichApiError::Http(error.without_url()))?;
        let text = response
            .text()
            .await
            .map_err(|error| RichApiError::Http(error.without_url()))?;
        parse_api_response::<T>(&text)
    }
}

fn build_send_body(chat_id: i64, html: &str, options: &RichSendOptions) -> Value {
    let mut body = Map::new();
    body.insert("chat_id".to_owned(), json!(chat_id));
    body.insert("rich_message".to_owned(), json!({ "html": html }));
    if let Some(thread) = options.message_thread_id {
        body.insert("message_thread_id".to_owned(), json!(thread));
    }
    if options.disable_notification {
        body.insert("disable_notification".to_owned(), json!(true));
    }
    if let Some(reply_to) = options.reply_to_message_id {
        let mut reply = Map::new();
        reply.insert("message_id".to_owned(), json!(reply_to));
        if options.allow_sending_without_reply {
            reply.insert("allow_sending_without_reply".to_owned(), json!(true));
        }
        body.insert("reply_parameters".to_owned(), Value::Object(reply));
    }
    if let Some(markup) = &options.reply_markup {
        body.insert("reply_markup".to_owned(), markup.clone());
    }
    Value::Object(body)
}

fn build_draft_body(
    chat_id: i64,
    draft_id: i64,
    html: &str,
    message_thread_id: Option<i64>,
) -> Value {
    let mut body = Map::new();
    body.insert("chat_id".to_owned(), json!(chat_id));
    body.insert("draft_id".to_owned(), json!(draft_id));
    body.insert("rich_message".to_owned(), json!({ "html": html }));
    if let Some(thread) = message_thread_id {
        body.insert("message_thread_id".to_owned(), json!(thread));
    }
    Value::Object(body)
}

fn build_edit_body(
    chat_id: i64,
    message_id: i64,
    html: &str,
    reply_markup: Option<Value>,
) -> Value {
    let mut body = Map::new();
    body.insert("chat_id".to_owned(), json!(chat_id));
    body.insert("message_id".to_owned(), json!(message_id));
    body.insert("rich_message".to_owned(), json!({ "html": html }));
    if let Some(markup) = reply_markup {
        body.insert("reply_markup".to_owned(), markup);
    }
    Value::Object(body)
}

#[derive(Deserialize)]
struct ApiResponse<T> {
    #[serde(default)]
    ok: bool,
    result: Option<T>,
    description: Option<String>,
    error_code: Option<i64>,
    parameters: Option<ApiResponseParameters>,
}

#[derive(Deserialize)]
struct ApiResponseParameters {
    retry_after: Option<u64>,
}

fn parse_api_response<T: DeserializeOwned>(text: &str) -> Result<T, RichApiError> {
    let envelope: ApiResponse<T> =
        serde_json::from_str(text).map_err(|err| RichApiError::Decode(err.to_string()))?;
    if envelope.ok {
        envelope
            .result
            .ok_or_else(|| RichApiError::Decode("missing result in ok response".to_owned()))
    } else {
        Err(RichApiError::Api {
            code: envelope.error_code.unwrap_or_default(),
            description: envelope.description.unwrap_or_default(),
            retry_after: envelope.parameters.and_then(|params| params.retry_after),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_body_minimal() {
        let body = build_send_body(42, "<b>x</b>", &RichSendOptions::default());
        assert_eq!(
            body,
            json!({"chat_id": 42, "rich_message": {"html": "<b>x</b>"}})
        );
    }

    #[test]
    fn send_body_with_reply_thread_and_markup() {
        let options = RichSendOptions {
            message_thread_id: Some(7),
            reply_to_message_id: Some(11),
            allow_sending_without_reply: true,
            disable_notification: true,
            reply_markup: Some(json!({"inline_keyboard": []})),
        };
        let body = build_send_body(1, "hi", &options);
        assert_eq!(
            body,
            json!({
                "chat_id": 1,
                "rich_message": {"html": "hi"},
                "message_thread_id": 7,
                "disable_notification": true,
                "reply_parameters": {"message_id": 11, "allow_sending_without_reply": true},
                "reply_markup": {"inline_keyboard": []}
            })
        );
    }

    #[test]
    fn draft_body_shape() {
        assert_eq!(
            build_draft_body(5, 70101, "<tg-thinking>…</tg-thinking>", None),
            json!({"chat_id": 5, "draft_id": 70101, "rich_message": {"html": "<tg-thinking>…</tg-thinking>"}})
        );
    }

    #[test]
    fn edit_body_with_and_without_markup() {
        assert_eq!(
            build_edit_body(5, 9, "<p>x</p>", None),
            json!({"chat_id": 5, "message_id": 9, "rich_message": {"html": "<p>x</p>"}})
        );
        assert_eq!(
            build_edit_body(5, 9, "<p>x</p>", Some(json!({"inline_keyboard": []}))),
            json!({"chat_id": 5, "message_id": 9, "rich_message": {"html": "<p>x</p>"}, "reply_markup": {"inline_keyboard": []}})
        );
    }

    #[test]
    fn parse_ok_value_and_bool() {
        let value: Value = parse_api_response(r#"{"ok":true,"result":{"message_id":3}}"#)
            .expect("ok API object response should parse result");
        assert_eq!(value, json!({"message_id": 3}));
        let flag: bool = parse_api_response(r#"{"ok":true,"result":true}"#)
            .expect("ok API bool response should parse result");
        assert!(flag);
    }

    #[test]
    fn parse_error_surfaces_retry_after() {
        let err = parse_api_response::<Value>(
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 5","parameters":{"retry_after":5}}"#,
        )
        .expect_err("API retry-after error should be surfaced");
        match &err {
            RichApiError::Api {
                code,
                retry_after,
                description,
            } => {
                assert_eq!(*code, 429);
                assert_eq!(*retry_after, Some(5));
                assert!(description.contains("Too Many Requests"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        assert_eq!(err.retry_after(), Some(5));
    }

    #[test]
    fn detects_not_modified() {
        let err = parse_api_response::<Value>(
            r#"{"ok":false,"error_code":400,"description":"Bad Request: message is not modified"}"#,
        )
        .expect_err("not-modified API error should be surfaced");
        assert!(err.is_not_modified());
        assert_eq!(err.retry_after(), None);
    }

    #[tokio::test]
    async fn transport_error_never_contains_bot_token_url() {
        const SECRET: &str = "123456:super-secret-token";
        let client = RichApiClient::with_base_url(SECRET, "http://127.0.0.1:1")
            .expect("client construction");

        let error = client
            .send_rich_message(42, "hello", &RichSendOptions::default())
            .await
            .expect_err("closed loopback port must fail");
        let rendered = error.to_string();

        assert!(!rendered.contains(SECRET), "secret leaked: {rendered}");
        assert!(
            !rendered.contains("/bot"),
            "credential URL leaked: {rendered}"
        );
    }
}
