use std::{future::Future, time::Duration};

use carapax::{
    api::{Client, ExecuteError},
    types::{
        AnswerCallbackQuery, AnswerGuestQuery, AnswerInlineQuery, AnswerPreCheckoutQuery,
        CreateInvoiceLink, DeleteMessage, EditMessageCaption, EditMessageMedia,
        EditMessageReplyMarkup, EditMessageResult, EditMessageText, EditUserStarSubscription,
        Message, RefundStarPayment, SendAudio, SendChatAction, SendMediaGroup, SendMessage,
        SendPhoto, SendSticker, SentGuestMessage, SetMessageReaction,
    },
};

use crate::{
    DispatcherSendStatus, RichApiClient, RichApiError, SendRichMessage, format_rich_html,
    replay_outbound_method, snapshot_outbound_method,
};

/// Maximum attempts (including the first) made by [`send_outbound_method_with_bounded_retry`].
pub const OUTBOUND_SEND_MAX_ATTEMPTS: u32 = 3;

/// Longest Telegram `retry_after` hint honored by the inline bounded retry.
pub const OUTBOUND_RETRY_AFTER_INLINE_CAP_SECS: u64 = 5;

/// Concrete outbound Telegram methods currently queued by the Rust dispatcher.
#[derive(Debug)]
pub enum TelegramOutboundMethod {
    /// Telegram `sendMessage`.
    SendMessage(Box<SendMessage>),
    /// Telegram `sendRichMessage`.
    SendRichMessage(Box<SendRichMessage>),
    /// Telegram `sendSticker`.
    SendSticker(Box<SendSticker>),
    /// Telegram `sendPhoto`.
    SendPhoto(Box<SendPhoto>),
    /// Telegram `sendAudio`.
    SendAudio(Box<SendAudio>),
    /// Telegram `sendMediaGroup`.
    SendMediaGroup(Box<SendMediaGroup>),
    /// Telegram `sendChatAction`.
    SendChatAction(Box<SendChatAction>),
    /// Telegram `answerCallbackQuery`.
    AnswerCallbackQuery(Box<AnswerCallbackQuery>),
    /// Telegram `answerInlineQuery`.
    AnswerInlineQuery(Box<AnswerInlineQuery>),
    /// Telegram `answerGuestQuery`.
    AnswerGuestQuery(Box<AnswerGuestQuery>),
    /// Telegram `answerPreCheckoutQuery`.
    AnswerPreCheckoutQuery(Box<AnswerPreCheckoutQuery>),
    /// Telegram `createInvoiceLink`.
    CreateInvoiceLink(Box<CreateInvoiceLink>),
    /// Telegram `refundStarPayment`.
    RefundStarPayment(Box<RefundStarPayment>),
    /// Telegram `editUserStarSubscription`.
    EditUserStarSubscription(Box<EditUserStarSubscription>),
    /// Telegram `editMessageText`.
    EditMessageText(Box<EditMessageText>),
    /// Telegram `editMessageCaption`.
    EditMessageCaption(Box<EditMessageCaption>),
    /// Telegram `editMessageReplyMarkup`.
    EditMessageReplyMarkup(Box<EditMessageReplyMarkup>),
    /// Telegram `editMessageMedia`.
    EditMessageMedia(Box<EditMessageMedia>),
    /// Telegram `deleteMessage`.
    DeleteMessage(Box<DeleteMessage>),
    /// Telegram `setMessageReaction`.
    SetMessageReaction(Box<SetMessageReaction>),
}

/// Stable method discriminator for tests, metrics, and future persistence metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramOutboundMethodKind {
    /// Telegram `sendMessage`.
    SendMessage,
    /// Telegram `sendRichMessage`.
    SendRichMessage,
    /// Telegram `sendSticker`.
    SendSticker,
    /// Telegram `sendPhoto`.
    SendPhoto,
    /// Telegram `sendAudio`.
    SendAudio,
    /// Telegram `sendMediaGroup`.
    SendMediaGroup,
    /// Telegram `sendChatAction`.
    SendChatAction,
    /// Telegram `answerCallbackQuery`.
    AnswerCallbackQuery,
    /// Telegram `answerInlineQuery`.
    AnswerInlineQuery,
    /// Telegram `answerGuestQuery`.
    AnswerGuestQuery,
    /// Telegram `answerPreCheckoutQuery`.
    AnswerPreCheckoutQuery,
    /// Telegram `createInvoiceLink`.
    CreateInvoiceLink,
    /// Telegram `refundStarPayment`.
    RefundStarPayment,
    /// Telegram `editUserStarSubscription`.
    EditUserStarSubscription,
    /// Telegram `editMessageText`.
    EditMessageText,
    /// Telegram `editMessageCaption`.
    EditMessageCaption,
    /// Telegram `editMessageReplyMarkup`.
    EditMessageReplyMarkup,
    /// Telegram `editMessageMedia`.
    EditMessageMedia,
    /// Telegram `deleteMessage`.
    DeleteMessage,
    /// Telegram `setMessageReaction`.
    SetMessageReaction,
}

/// Response shape returned by a concrete Telegram outbound method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramOutboundResponseKind {
    /// Methods returning one Telegram message.
    Message,
    /// Methods returning multiple Telegram messages.
    Messages,
    /// Telegram edit methods returning either a message or `true`.
    EditMessage,
    /// Methods returning a boolean success flag.
    Boolean,
    /// Telegram `answerGuestQuery` sent-message result.
    SentGuestMessage,
    /// Methods returning a string value.
    String,
}

/// Concrete successful response from executing an outbound Telegram method.
#[derive(Clone, Debug)]
pub enum TelegramOutboundResponse {
    /// One Telegram message.
    Message(Box<Message>),
    /// Multiple Telegram messages from an album send.
    Messages(Vec<Message>),
    /// Edit result from `editMessage*`.
    EditMessage(EditMessageResult),
    /// Boolean success flag.
    Boolean(bool),
    /// Inline guest message response from `answerGuestQuery`.
    SentGuestMessage(Box<SentGuestMessage>),
    /// String response, such as `createInvoiceLink`.
    String(String),
}

#[derive(Debug, thiserror::Error)]
pub enum TelegramOutboundExecuteError {
    #[error("{0}")]
    Telegram(#[from] ExecuteError),
    #[error("{0}")]
    Rich(#[from] RichApiError),
}

/// Retry/terminal classification of one outbound Telegram send failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundSendErrorClass {
    /// Telegram flood control (429) with its wait hint.
    RetryableRateLimited { retry_after_secs: u64 },
    /// Failure that provably happened before the request reached Telegram.
    RetryableTransient,
    /// 403 or permission-shaped 400: the bot cannot post in this chat.
    TerminalPermission,
    /// Other 400s, including empty-text rejections.
    TerminalBadRequest,
    /// Everything else, including mid-response transport failures that may double-send.
    TerminalOther,
}

impl OutboundSendErrorClass {
    /// Stable string form for logs and diagnostics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RetryableRateLimited { .. } => "retryable_rate_limited",
            Self::RetryableTransient => "retryable_transient",
            Self::TerminalPermission => "terminal_permission",
            Self::TerminalBadRequest => "terminal_bad_request",
            Self::TerminalOther => "terminal_other",
        }
    }
}

impl TelegramOutboundExecuteError {
    /// Diagnostic text safe for persistence and operator APIs. Reqwest errors
    /// may include the Bot API URL, whose `/bot<token>/` path embeds the token.
    #[must_use]
    pub fn diagnostic_message(&self) -> String {
        sanitize_telegram_transport_diagnostic(&self.to_string())
    }

    pub fn classification(&self) -> OutboundSendErrorClass {
        match self {
            Self::Telegram(ExecuteError::Response(response))
                if response.error_code() == Some(429) =>
            {
                OutboundSendErrorClass::RetryableRateLimited {
                    retry_after_secs: response.retry_after().unwrap_or(60),
                }
            }
            Self::Rich(RichApiError::Api {
                code: 429,
                retry_after,
                ..
            }) => OutboundSendErrorClass::RetryableRateLimited {
                retry_after_secs: retry_after.unwrap_or(60),
            },
            _ if self.is_permission_error() => OutboundSendErrorClass::TerminalPermission,
            Self::Telegram(ExecuteError::Response(response))
                if response.error_code() == Some(400) =>
            {
                OutboundSendErrorClass::TerminalBadRequest
            }
            Self::Rich(RichApiError::Api { code: 400, .. }) => {
                OutboundSendErrorClass::TerminalBadRequest
            }
            // Only connect-phase failures are provably pre-request; mid-response
            // failures ("connection closed before message completed" style) may have
            // been accepted by Telegram, so replaying them could double-send.
            Self::Telegram(ExecuteError::Http(error)) if error.is_connect() => {
                OutboundSendErrorClass::RetryableTransient
            }
            Self::Rich(RichApiError::Http(error)) if error.is_connect() => {
                OutboundSendErrorClass::RetryableTransient
            }
            _ => OutboundSendErrorClass::TerminalOther,
        }
    }

    pub fn retry_after(&self) -> Option<u64> {
        match self {
            Self::Telegram(ExecuteError::Response(response))
                if response.error_code() == Some(429) =>
            {
                Some(response.retry_after().unwrap_or(60))
            }
            Self::Telegram(_) => None,
            Self::Rich(error) => error.retry_after(),
        }
    }

    pub fn is_reply_missing(&self) -> bool {
        match self {
            Self::Telegram(error) => telegram_execute_error_is_reply_missing(error),
            Self::Rich(RichApiError::Api { description, .. }) => {
                contains_any_ascii_fold(description, REPLY_MISSING_ERROR_FRAGMENTS)
            }
            Self::Rich(_) => false,
        }
    }

    pub fn is_permission_error(&self) -> bool {
        match self {
            Self::Telegram(error) => classify_telegram_send_error(error).permission_error,
            Self::Rich(RichApiError::Api {
                code, description, ..
            }) => {
                *code == 403
                    || (*code == 400
                        && (is_permission_send_error(description)
                            || description.contains("CHAT_WRITE_FORBIDDEN")))
            }
            Self::Rich(_) => false,
        }
    }
}

/// Remove Bot API tokens embedded in request URLs before a message reaches
/// logs, Postgres diagnostics, or runtime GraphQL.
#[must_use]
pub fn sanitize_telegram_transport_diagnostic(message: &str) -> String {
    const MARKER: &str = "/bot";
    const REDACTED: &str = "<redacted>";

    let mut sanitized = message.to_owned();
    let mut offset = 0;
    while let Some(relative) = sanitized[offset..].find(MARKER) {
        let token_start = offset + relative + MARKER.len();
        let Some(token_end_relative) = sanitized[token_start..].find('/') else {
            break;
        };
        let token_end = token_start + token_end_relative;
        if sanitized[token_start..token_end].contains(':') {
            sanitized.replace_range(token_start..token_end, REDACTED);
            offset = token_start + REDACTED.len();
        } else {
            offset = token_start;
        }
    }
    sanitized
}

impl TelegramOutboundMethod {
    /// Return the stable method discriminator.
    pub fn kind(&self) -> TelegramOutboundMethodKind {
        match self {
            Self::SendMessage(_) => TelegramOutboundMethodKind::SendMessage,
            Self::SendRichMessage(_) => TelegramOutboundMethodKind::SendRichMessage,
            Self::SendSticker(_) => TelegramOutboundMethodKind::SendSticker,
            Self::SendPhoto(_) => TelegramOutboundMethodKind::SendPhoto,
            Self::SendAudio(_) => TelegramOutboundMethodKind::SendAudio,
            Self::SendMediaGroup(_) => TelegramOutboundMethodKind::SendMediaGroup,
            Self::SendChatAction(_) => TelegramOutboundMethodKind::SendChatAction,
            Self::AnswerCallbackQuery(_) => TelegramOutboundMethodKind::AnswerCallbackQuery,
            Self::AnswerInlineQuery(_) => TelegramOutboundMethodKind::AnswerInlineQuery,
            Self::AnswerGuestQuery(_) => TelegramOutboundMethodKind::AnswerGuestQuery,
            Self::AnswerPreCheckoutQuery(_) => TelegramOutboundMethodKind::AnswerPreCheckoutQuery,
            Self::CreateInvoiceLink(_) => TelegramOutboundMethodKind::CreateInvoiceLink,
            Self::RefundStarPayment(_) => TelegramOutboundMethodKind::RefundStarPayment,
            Self::EditUserStarSubscription(_) => {
                TelegramOutboundMethodKind::EditUserStarSubscription
            }
            Self::EditMessageText(_) => TelegramOutboundMethodKind::EditMessageText,
            Self::EditMessageCaption(_) => TelegramOutboundMethodKind::EditMessageCaption,
            Self::EditMessageReplyMarkup(_) => TelegramOutboundMethodKind::EditMessageReplyMarkup,
            Self::EditMessageMedia(_) => TelegramOutboundMethodKind::EditMessageMedia,
            Self::DeleteMessage(_) => TelegramOutboundMethodKind::DeleteMessage,
            Self::SetMessageReaction(_) => TelegramOutboundMethodKind::SetMessageReaction,
        }
    }

    /// Return the Bot API method name used by `tgbot::api::Method::into_payload`.
    pub fn method_name(&self) -> &'static str {
        match self {
            Self::SendMessage(_) => "sendMessage",
            Self::SendRichMessage(_) => "sendRichMessage",
            Self::SendSticker(_) => "sendSticker",
            Self::SendPhoto(_) => "sendPhoto",
            Self::SendAudio(_) => "sendAudio",
            Self::SendMediaGroup(_) => "sendMediaGroup",
            Self::SendChatAction(_) => "sendChatAction",
            Self::AnswerCallbackQuery(_) => "answerCallbackQuery",
            Self::AnswerInlineQuery(_) => "answerInlineQuery",
            Self::AnswerGuestQuery(_) => "answerGuestQuery",
            Self::AnswerPreCheckoutQuery(_) => "answerPreCheckoutQuery",
            Self::CreateInvoiceLink(_) => "createInvoiceLink",
            Self::RefundStarPayment(_) => "refundStarPayment",
            Self::EditUserStarSubscription(_) => "editUserStarSubscription",
            Self::EditMessageText(_) => "editMessageText",
            Self::EditMessageCaption(_) => "editMessageCaption",
            Self::EditMessageReplyMarkup(_) => "editMessageReplyMarkup",
            Self::EditMessageMedia(_) => "editMessageMedia",
            Self::DeleteMessage(_) => "deleteMessage",
            Self::SetMessageReaction(_) => "setMessageReaction",
        }
    }

    /// Return the expected successful response shape.
    pub fn response_kind(&self) -> TelegramOutboundResponseKind {
        match self {
            Self::SendMessage(_)
            | Self::SendRichMessage(_)
            | Self::SendSticker(_)
            | Self::SendPhoto(_)
            | Self::SendAudio(_) => TelegramOutboundResponseKind::Message,
            Self::SendMediaGroup(_) => TelegramOutboundResponseKind::Messages,
            Self::SendChatAction(_)
            | Self::AnswerCallbackQuery(_)
            | Self::AnswerInlineQuery(_)
            | Self::AnswerPreCheckoutQuery(_)
            | Self::RefundStarPayment(_)
            | Self::EditUserStarSubscription(_) => TelegramOutboundResponseKind::Boolean,
            Self::CreateInvoiceLink(_) => TelegramOutboundResponseKind::String,
            Self::AnswerGuestQuery(_) => TelegramOutboundResponseKind::SentGuestMessage,
            Self::EditMessageText(_)
            | Self::EditMessageCaption(_)
            | Self::EditMessageReplyMarkup(_)
            | Self::EditMessageMedia(_) => TelegramOutboundResponseKind::EditMessage,
            Self::DeleteMessage(_) | Self::SetMessageReaction(_) => {
                TelegramOutboundResponseKind::Boolean
            }
        }
    }

    /// Execute this method with a `carapax` API client.
    pub async fn execute_with(
        self,
        client: &Client,
    ) -> Result<TelegramOutboundResponse, ExecuteError> {
        execute_telegram_method(client, self).await
    }
}

/// Execute one concrete outbound Telegram method through `carapax`.
pub async fn execute_telegram_method(
    client: &Client,
    method: TelegramOutboundMethod,
) -> Result<TelegramOutboundResponse, ExecuteError> {
    match method {
        TelegramOutboundMethod::SendMessage(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendRichMessage(_) => {
            panic!("sendRichMessage requires execute_telegram_method_with_rich")
        }
        TelegramOutboundMethod::SendSticker(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendPhoto(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendAudio(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::Message(Box::new(message))),
        TelegramOutboundMethod::SendMediaGroup(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Messages),
        TelegramOutboundMethod::SendChatAction(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::AnswerCallbackQuery(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::AnswerInlineQuery(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::AnswerGuestQuery(method) => client
            .execute(*method)
            .await
            .map(|message| TelegramOutboundResponse::SentGuestMessage(Box::new(message))),
        TelegramOutboundMethod::AnswerPreCheckoutQuery(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::CreateInvoiceLink(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::String),
        TelegramOutboundMethod::RefundStarPayment(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::EditUserStarSubscription(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::EditMessageText(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageCaption(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageReplyMarkup(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::EditMessageMedia(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::EditMessage),
        TelegramOutboundMethod::DeleteMessage(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
        TelegramOutboundMethod::SetMessageReaction(method) => client
            .execute(*method)
            .await
            .map(TelegramOutboundResponse::Boolean),
    }
}

pub async fn execute_telegram_method_with_rich(
    client: &Client,
    rich: &RichApiClient,
    method: TelegramOutboundMethod,
) -> Result<TelegramOutboundResponse, TelegramOutboundExecuteError> {
    match method {
        TelegramOutboundMethod::SendRichMessage(mut method) => {
            method.html = format_rich_html(&method.html);
            rich.send_rich_message_request(&method)
                .await
                .map(|message| TelegramOutboundResponse::Message(Box::new(message)))
                .map_err(TelegramOutboundExecuteError::from)
        }
        method => execute_telegram_method(client, method)
            .await
            .map_err(TelegramOutboundExecuteError::from),
    }
}

/// Execute one outbound method and collapse the result into dispatcher accounting status.
pub async fn send_telegram_method_status(
    client: &Client,
    method: TelegramOutboundMethod,
) -> DispatcherSendStatus {
    match execute_telegram_method(client, method).await {
        Ok(_) => DispatcherSendStatus::Sent,
        Err(_) => DispatcherSendStatus::Failed,
    }
}

pub async fn send_telegram_method_status_with_rich(
    client: &Client,
    rich: &RichApiClient,
    method: TelegramOutboundMethod,
) -> DispatcherSendStatus {
    match execute_telegram_method_with_rich(client, rich, method).await {
        Ok(_) => DispatcherSendStatus::Sent,
        Err(_) => DispatcherSendStatus::Failed,
    }
}

/// Send one outbound method, replaying it from a serialized snapshot on short 429s
/// and pre-request connect failures, up to [`OUTBOUND_SEND_MAX_ATTEMPTS`] attempts.
///
/// Any other failure — including mid-response transport errors that may have already
/// been accepted by Telegram — returns immediately, as does a method whose payload
/// cannot be snapshot/replayed (photo/audio/media-group and other form-backed sends).
pub async fn send_outbound_method_with_bounded_retry<Send, Fut>(
    send: Send,
    method: TelegramOutboundMethod,
    virtual_id: &str,
    chat_id: i64,
) -> Result<TelegramOutboundResponse, TelegramOutboundExecuteError>
where
    Send: Fn(TelegramOutboundMethod) -> Fut,
    Fut: Future<Output = Result<TelegramOutboundResponse, TelegramOutboundExecuteError>>,
{
    let snapshot = snapshot_outbound_method(&method);
    let mut current = method;
    let mut attempt: u32 = 1;
    loop {
        let error = match send(current).await {
            Ok(response) => return Ok(response),
            Err(error) => error,
        };
        let class = error.classification();
        let delay = match class {
            OutboundSendErrorClass::RetryableRateLimited { retry_after_secs }
                if retry_after_secs <= OUTBOUND_RETRY_AFTER_INLINE_CAP_SECS =>
            {
                Duration::from_secs(retry_after_secs)
            }
            OutboundSendErrorClass::RetryableTransient => {
                Duration::from_millis(500).saturating_mul(attempt)
            }
            _ => return Err(error),
        };
        if attempt >= OUTBOUND_SEND_MAX_ATTEMPTS {
            return Err(error);
        }
        let Some(replayed) = snapshot
            .as_ref()
            .and_then(|(kind, bytes)| replay_outbound_method(*kind, bytes))
        else {
            return Err(error);
        };
        tracing::warn!(
            virtual_id,
            chat_id,
            attempt,
            class = class.as_str(),
            error = %error,
            "retrying outbound Telegram send"
        );
        tokio::time::sleep(delay).await;
        current = replayed;
        attempt += 1;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramSendErrorClassification {
    /// Operator-visible formatted error prefix/body.
    pub message: String,
    pub permission_error: bool,
}

pub fn classify_telegram_send_error(error: &ExecuteError) -> TelegramSendErrorClassification {
    let ExecuteError::Response(response) = error else {
        return TelegramSendErrorClassification {
            message: format!("failed to send message: {error}"),
            permission_error: false,
        };
    };

    let message = response.description();
    match response.error_code() {
        Some(403) => TelegramSendErrorClassification {
            message: format!("bot was blocked or kicked: {error}"),
            permission_error: true,
        },
        Some(400) if is_empty_text_send_error(message) => TelegramSendErrorClassification {
            message: "bad request: text must be non-empty".to_owned(),
            permission_error: false,
        },
        Some(400) if is_permission_send_error(message) => TelegramSendErrorClassification {
            message: format!("insufficient rights: {error}"),
            permission_error: true,
        },
        Some(400) if message.contains("message thread not found") => {
            TelegramSendErrorClassification {
                message: format!("forum thread not found: {error}"),
                permission_error: false,
            }
        }
        Some(400) => TelegramSendErrorClassification {
            message: format!("bad request: {error}"),
            permission_error: false,
        },
        _ => TelegramSendErrorClassification {
            message: format!("telegram API error: {error}"),
            permission_error: false,
        },
    }
}

fn is_empty_text_send_error(message: &str) -> bool {
    message.contains("text must be non-empty") || message.contains("message text is empty")
}

fn is_permission_send_error(message: &str) -> bool {
    message.contains("not enough rights")
        || message.contains("CHAT_WRITE_FORBIDDEN")
        || (message.contains("CHAT_SEND_") && message.contains("_FORBIDDEN"))
        || message.contains("have no rights to send a message")
}

pub fn telegram_execute_error_is_reply_missing(error: &ExecuteError) -> bool {
    let ExecuteError::Response(response) = error else {
        return false;
    };
    response.error_code() == Some(400)
        && contains_any_ascii_fold(response.description(), REPLY_MISSING_ERROR_FRAGMENTS)
}

const REPLY_MISSING_ERROR_FRAGMENTS: &[&str] = &[
    "reply message not found",
    "message to reply not found",
    "message to be replied not found",
    "reply_to_message not found",
];

fn contains_any_ascii_fold(haystack: &str, needles: &[&str]) -> bool {
    needles
        .iter()
        .any(|needle| contains_ascii_fold(haystack, needle))
}

fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| ascii_eq_fold(window, needle))
}

fn ascii_eq_fold(left: &[u8], right: &[u8]) -> bool {
    left.iter()
        .zip(right)
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

impl From<SendMessage> for TelegramOutboundMethod {
    fn from(value: SendMessage) -> Self {
        Self::SendMessage(Box::new(value))
    }
}

impl From<SendRichMessage> for TelegramOutboundMethod {
    fn from(value: SendRichMessage) -> Self {
        Self::SendRichMessage(Box::new(value))
    }
}

impl From<SendSticker> for TelegramOutboundMethod {
    fn from(value: SendSticker) -> Self {
        Self::SendSticker(Box::new(value))
    }
}

impl From<SendPhoto> for TelegramOutboundMethod {
    fn from(value: SendPhoto) -> Self {
        Self::SendPhoto(Box::new(value))
    }
}

impl From<SendAudio> for TelegramOutboundMethod {
    fn from(value: SendAudio) -> Self {
        Self::SendAudio(Box::new(value))
    }
}

impl From<SendMediaGroup> for TelegramOutboundMethod {
    fn from(value: SendMediaGroup) -> Self {
        Self::SendMediaGroup(Box::new(value))
    }
}

impl From<SendChatAction> for TelegramOutboundMethod {
    fn from(value: SendChatAction) -> Self {
        Self::SendChatAction(Box::new(value))
    }
}

impl From<AnswerCallbackQuery> for TelegramOutboundMethod {
    fn from(value: AnswerCallbackQuery) -> Self {
        Self::AnswerCallbackQuery(Box::new(value))
    }
}

impl From<AnswerInlineQuery> for TelegramOutboundMethod {
    fn from(value: AnswerInlineQuery) -> Self {
        Self::AnswerInlineQuery(Box::new(value))
    }
}

impl From<AnswerGuestQuery> for TelegramOutboundMethod {
    fn from(value: AnswerGuestQuery) -> Self {
        Self::AnswerGuestQuery(Box::new(value))
    }
}

impl From<AnswerPreCheckoutQuery> for TelegramOutboundMethod {
    fn from(value: AnswerPreCheckoutQuery) -> Self {
        Self::AnswerPreCheckoutQuery(Box::new(value))
    }
}

impl From<CreateInvoiceLink> for TelegramOutboundMethod {
    fn from(value: CreateInvoiceLink) -> Self {
        Self::CreateInvoiceLink(Box::new(value))
    }
}

impl From<RefundStarPayment> for TelegramOutboundMethod {
    fn from(value: RefundStarPayment) -> Self {
        Self::RefundStarPayment(Box::new(value))
    }
}

impl From<EditUserStarSubscription> for TelegramOutboundMethod {
    fn from(value: EditUserStarSubscription) -> Self {
        Self::EditUserStarSubscription(Box::new(value))
    }
}

impl From<EditMessageText> for TelegramOutboundMethod {
    fn from(value: EditMessageText) -> Self {
        Self::EditMessageText(Box::new(value))
    }
}

impl From<EditMessageCaption> for TelegramOutboundMethod {
    fn from(value: EditMessageCaption) -> Self {
        Self::EditMessageCaption(Box::new(value))
    }
}

impl From<EditMessageReplyMarkup> for TelegramOutboundMethod {
    fn from(value: EditMessageReplyMarkup) -> Self {
        Self::EditMessageReplyMarkup(Box::new(value))
    }
}

impl From<EditMessageMedia> for TelegramOutboundMethod {
    fn from(value: EditMessageMedia) -> Self {
        Self::EditMessageMedia(Box::new(value))
    }
}

impl From<DeleteMessage> for TelegramOutboundMethod {
    fn from(value: DeleteMessage) -> Self {
        Self::DeleteMessage(Box::new(value))
    }
}

impl From<SetMessageReaction> for TelegramOutboundMethod {
    fn from(value: SetMessageReaction) -> Self {
        Self::SetMessageReaction(Box::new(value))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use serde_json::json;

    use super::{
        OutboundSendErrorClass, TelegramOutboundExecuteError, TelegramOutboundMethod,
        TelegramOutboundMethodKind, TelegramOutboundResponse, TelegramOutboundResponseKind,
        classify_telegram_send_error, sanitize_telegram_transport_diagnostic,
        send_outbound_method_with_bounded_retry, telegram_execute_error_is_reply_missing,
    };
    use crate::{
        AudioMessageRequest, AudioSource, CallbackAnswerRequest, ChatActionRequest, ChatRef,
        DonationInvoiceLinkRequest, EditCaptionMessageRequest, EditMediaMessageRequest,
        EditReplyMarkupMessageRequest, EditTextMessageRequest, GuestQueryAnswerRequest,
        InlineArticleRequest, InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryAnswerRequest,
        MediaGroupMessageRequest, MediaGroupPhotoItem, PhotoMessageRequest, PhotoSource,
        RichApiError, StickerMessageRequest, SubscriptionInvoiceLinkRequest,
        TELEGRAM_PARSE_MODE_HTML, TextMessageRequest, build_audio_message_method,
        build_callback_answer_method, build_cancel_star_subscription_method,
        build_chat_action_method, build_delete_message_method, build_donation_invoice_link_method,
        build_edit_caption_message_method, build_edit_media_message_method,
        build_edit_reply_markup_message_method, build_edit_text_message_method,
        build_guest_query_answer_method, build_inline_query_answer_method,
        build_inline_query_result_article, build_media_group_message_method,
        build_message_reaction_method, build_photo_message_method, build_pre_checkout_ok_method,
        build_refund_star_payment_method, build_sticker_message_method,
        build_subscription_invoice_link_method, build_text_message_method,
    };

    fn chat(id: i64) -> ChatRef {
        ChatRef {
            id,
            is_forum: false,
        }
    }

    #[test]
    fn transport_diagnostic_redacts_bot_token_url_segments() {
        let sentinel = "123456:SECRET_SENTINEL";
        let diagnostic =
            format!("request failed for https://api.telegram.org/bot{sentinel}/sendMessage");

        let sanitized = sanitize_telegram_transport_diagnostic(&diagnostic);

        assert!(!sanitized.contains(sentinel));
        assert_eq!(
            sanitized,
            "request failed for https://api.telegram.org/bot<redacted>/sendMessage"
        );
    }

    fn photo_item(file_id: &str) -> MediaGroupPhotoItem {
        MediaGroupPhotoItem {
            photo: PhotoSource::FileId(file_id.to_owned()),
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
        }
    }

    #[test]
    fn outbound_method_metadata_names_the_real_bot_api_methods_and_response_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let text = TelegramOutboundMethod::from(
            build_text_message_method(
                &TextMessageRequest {
                    chat: Some(chat(42)),
                    message_thread_id: 0,
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    text: "hello".to_owned(),
                    render_as: String::new(),
                    reply_markup: None,
                },
                chat(42),
                None,
                "hello",
                true,
            )
            .expect("text method"),
        );
        let sticker = TelegramOutboundMethod::from(
            build_sticker_message_method(
                &StickerMessageRequest {
                    chat: Some(chat(42)),
                    message_thread_id: 0,
                    disable_notification: false,
                    file_id: "sticker-id".to_owned(),
                },
                None,
            )
            .expect("sticker method"),
        );
        let photo = TelegramOutboundMethod::from(
            build_photo_message_method(&PhotoMessageRequest {
                chat: chat(42),
                message_thread_id: 0,
                disable_notification: false,
                photo: PhotoSource::FileId("photo-id".to_owned()),
                caption: String::new(),
                render_as: String::new(),
                has_spoiler: false,
                reply_parameters: None,
            })
            .expect("photo method"),
        );
        let audio = TelegramOutboundMethod::from(
            build_audio_message_method(&AudioMessageRequest {
                chat: chat(42),
                message_thread_id: 0,
                disable_notification: false,
                audio: AudioSource::FileId("audio-id".to_owned()),
                caption: String::new(),
                render_as: String::new(),
                reply_parameters: None,
            })
            .expect("audio method"),
        );
        let media_group = TelegramOutboundMethod::from(
            build_media_group_message_method(&MediaGroupMessageRequest {
                chat: chat(42),
                message_thread_id: 0,
                disable_notification: false,
                items: vec![photo_item("photo-1"), photo_item("photo-2")],
                reply_parameters: None,
            })
            .expect("media group method"),
        );
        let edit_text = TelegramOutboundMethod::from(
            build_edit_text_message_method(&EditTextMessageRequest {
                chat: chat(42),
                message_id: 7,
                text: "edited".to_owned(),
                render_as: String::new(),
                reply_markup: None,
            })
            .expect("edit text method"),
        );
        let edit_media = TelegramOutboundMethod::from(
            build_edit_media_message_method(&EditMediaMessageRequest {
                chat: chat(42),
                message_id: 7,
                media: photo_item("replacement-photo"),
            })
            .expect("edit media method"),
        );
        let edit_caption = TelegramOutboundMethod::from(
            build_edit_caption_message_method(&EditCaptionMessageRequest {
                chat: chat(42),
                message_id: 7,
                caption: "<b>caption</b>".to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                reply_markup: None,
            })
            .expect("edit caption method"),
        );
        let edit_reply_markup = TelegramOutboundMethod::from(
            build_edit_reply_markup_message_method(&EditReplyMarkupMessageRequest {
                chat: chat(42),
                message_id: 7,
                reply_markup: InlineKeyboardMarkup::from([[
                    InlineKeyboardButton::for_callback_data("ok", "ok"),
                ]]),
            })
            .expect("edit reply markup method"),
        );
        let chat_action = TelegramOutboundMethod::from(
            build_chat_action_method(&ChatActionRequest {
                chat: chat(42),
                message_thread_id: 0,
                action: "typing".to_owned(),
            })
            .expect("chat action method"),
        );
        let callback_answer =
            TelegramOutboundMethod::from(build_callback_answer_method(&CallbackAnswerRequest {
                callback_query_id: "query-id".to_owned(),
                text: "ack".to_owned(),
                show_alert: false,
                url: String::new(),
                cache_time: 0,
            }));
        let inline_article = build_inline_query_result_article(&InlineArticleRequest {
            id: "inline-id".to_owned(),
            title: "Шевелись, Плотва!".to_owned(),
            message_text: "raw query".to_owned(),
            render_as: String::new(),
            description: String::new(),
            reply_markup: None,
        })
        .expect("inline article");
        let inline_answer = TelegramOutboundMethod::from(build_inline_query_answer_method(
            &InlineQueryAnswerRequest {
                inline_query_id: "inline-id".to_owned(),
                results: vec![inline_article.clone()],
                cache_time: 1,
                is_personal: true,
                next_offset: String::new(),
            },
        ));
        let guest_answer = TelegramOutboundMethod::from(build_guest_query_answer_method(
            &GuestQueryAnswerRequest {
                guest_query_id: "guest-query".to_owned(),
                result: inline_article,
            },
        ));
        let pre_checkout = TelegramOutboundMethod::from(build_pre_checkout_ok_method("pcq-id"));
        let subscription_invoice = TelegramOutboundMethod::from(
            build_subscription_invoice_link_method(&SubscriptionInvoiceLinkRequest {
                user_id: 42,
                user_name: String::new(),
                amount_stars: 300,
            }),
        );
        let donation_invoice = TelegramOutboundMethod::from(build_donation_invoice_link_method(
            &DonationInvoiceLinkRequest {
                user_id: 42,
                amount_stars: 600,
            },
        ));
        let refund = TelegramOutboundMethod::from(build_refund_star_payment_method(42, "charge"));
        let cancel_subscription =
            TelegramOutboundMethod::from(build_cancel_star_subscription_method(42, "charge"));
        let delete_message = TelegramOutboundMethod::from(build_delete_message_method(
            &crate::DeleteMessageRequest {
                chat_id: 42,
                message_id: 7,
            },
        )?);
        let message_reaction = build_message_reaction_method(42, 7, "🤔");

        let cases = [
            (
                text,
                TelegramOutboundMethodKind::SendMessage,
                "sendMessage",
                TelegramOutboundResponseKind::Message,
            ),
            (
                sticker,
                TelegramOutboundMethodKind::SendSticker,
                "sendSticker",
                TelegramOutboundResponseKind::Message,
            ),
            (
                photo,
                TelegramOutboundMethodKind::SendPhoto,
                "sendPhoto",
                TelegramOutboundResponseKind::Message,
            ),
            (
                audio,
                TelegramOutboundMethodKind::SendAudio,
                "sendAudio",
                TelegramOutboundResponseKind::Message,
            ),
            (
                media_group,
                TelegramOutboundMethodKind::SendMediaGroup,
                "sendMediaGroup",
                TelegramOutboundResponseKind::Messages,
            ),
            (
                edit_text,
                TelegramOutboundMethodKind::EditMessageText,
                "editMessageText",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                edit_media,
                TelegramOutboundMethodKind::EditMessageMedia,
                "editMessageMedia",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                edit_caption,
                TelegramOutboundMethodKind::EditMessageCaption,
                "editMessageCaption",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                edit_reply_markup,
                TelegramOutboundMethodKind::EditMessageReplyMarkup,
                "editMessageReplyMarkup",
                TelegramOutboundResponseKind::EditMessage,
            ),
            (
                chat_action,
                TelegramOutboundMethodKind::SendChatAction,
                "sendChatAction",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                callback_answer,
                TelegramOutboundMethodKind::AnswerCallbackQuery,
                "answerCallbackQuery",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                inline_answer,
                TelegramOutboundMethodKind::AnswerInlineQuery,
                "answerInlineQuery",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                guest_answer,
                TelegramOutboundMethodKind::AnswerGuestQuery,
                "answerGuestQuery",
                TelegramOutboundResponseKind::SentGuestMessage,
            ),
            (
                pre_checkout,
                TelegramOutboundMethodKind::AnswerPreCheckoutQuery,
                "answerPreCheckoutQuery",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                subscription_invoice,
                TelegramOutboundMethodKind::CreateInvoiceLink,
                "createInvoiceLink",
                TelegramOutboundResponseKind::String,
            ),
            (
                donation_invoice,
                TelegramOutboundMethodKind::CreateInvoiceLink,
                "createInvoiceLink",
                TelegramOutboundResponseKind::String,
            ),
            (
                refund,
                TelegramOutboundMethodKind::RefundStarPayment,
                "refundStarPayment",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                cancel_subscription,
                TelegramOutboundMethodKind::EditUserStarSubscription,
                "editUserStarSubscription",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                delete_message,
                TelegramOutboundMethodKind::DeleteMessage,
                "deleteMessage",
                TelegramOutboundResponseKind::Boolean,
            ),
            (
                message_reaction,
                TelegramOutboundMethodKind::SetMessageReaction,
                "setMessageReaction",
                TelegramOutboundResponseKind::Boolean,
            ),
        ];

        for (method, kind, name, response_kind) in cases {
            assert_eq!(method.kind(), kind);
            assert_eq!(method.method_name(), name);
            assert_eq!(method.response_kind(), response_kind);
        }

        Ok(())
    }

    #[test]
    fn reply_missing_classifier_matches_go_fragments() -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            ("Bad Request: reply message not found", true),
            ("message to reply not found", true),
            ("Bad Request: message to be replied not found", true),
            ("reply_to_message not found", true),
            ("Bad Request: chat not found", false),
        ];

        for (description, expected) in cases {
            let error = response_error(400, description)?;
            assert_eq!(
                telegram_execute_error_is_reply_missing(&error),
                expected,
                "{description}"
            );
        }
        let forbidden = response_error(403, "Forbidden")?;
        assert!(!telegram_execute_error_is_reply_missing(&forbidden));

        Ok(())
    }

    #[test]
    fn reply_missing_classifier_matches_mixed_case() -> Result<(), Box<dyn std::error::Error>> {
        let error = response_error(400, "Bad Request: REPLY MESSAGE NOT FOUND")?;

        assert!(telegram_execute_error_is_reply_missing(&error));

        Ok(())
    }

    #[test]
    fn send_error_classifier_matches_go_server_policy() -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            (
                response_error(403, "Forbidden: bot was kicked from the group chat")?,
                "bot was blocked or kicked",
                true,
            ),
            (
                response_error(400, "Bad Request: not enough rights to send text messages")?,
                "insufficient rights",
                true,
            ),
            (
                response_error(400, "Bad Request: CHAT_SEND_PHOTOS_FORBIDDEN")?,
                "insufficient rights",
                true,
            ),
            (
                response_error(400, "Bad Request: message text is empty")?,
                "bad request: text must be non-empty",
                false,
            ),
            (
                response_error(400, "Bad Request: message thread not found")?,
                "forum thread not found",
                false,
            ),
            (
                response_error(400, "Bad Request: chat not found")?,
                "bad request",
                false,
            ),
            (
                response_error(500, "Internal Server Error")?,
                "telegram API error",
                false,
            ),
        ];

        for (error, expected_message, expected_permission) in cases {
            let classified = classify_telegram_send_error(&error);
            assert!(
                classified.message.starts_with(expected_message),
                "{} did not start with {expected_message}",
                classified.message
            );
            assert_eq!(classified.permission_error, expected_permission);
        }

        Ok(())
    }

    #[test]
    fn message_reaction_builder_serializes_set_message_reaction_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let method = build_message_reaction_method(42, 7, "🤔");

        assert_eq!(
            method.kind(),
            TelegramOutboundMethodKind::SetMessageReaction
        );
        assert_eq!(method.method_name(), "setMessageReaction");
        assert_eq!(
            method.response_kind(),
            TelegramOutboundResponseKind::Boolean
        );

        let TelegramOutboundMethod::SetMessageReaction(method) = method else {
            panic!("expected setMessageReaction method");
        };
        let payload = serde_json::to_value(method.as_ref())?;
        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(7));
        assert_eq!(
            payload["reaction"],
            json!([{"type": "emoji", "emoji": "🤔"}])
        );

        Ok(())
    }

    #[test]
    fn classification_maps_429_403_400_and_transport_errors()
    -> Result<(), Box<dyn std::error::Error>> {
        let cases: Vec<(TelegramOutboundExecuteError, OutboundSendErrorClass)> = vec![
            (
                rate_limited_error(2)?.into(),
                OutboundSendErrorClass::RetryableRateLimited {
                    retry_after_secs: 2,
                },
            ),
            (
                response_error(429, "Too Many Requests")?.into(),
                OutboundSendErrorClass::RetryableRateLimited {
                    retry_after_secs: 60,
                },
            ),
            (
                response_error(403, "Forbidden: bot was kicked from the group chat")?.into(),
                OutboundSendErrorClass::TerminalPermission,
            ),
            (
                response_error(400, "Bad Request: not enough rights to send text messages")?.into(),
                OutboundSendErrorClass::TerminalPermission,
            ),
            (
                response_error(400, "Bad Request: message text is empty")?.into(),
                OutboundSendErrorClass::TerminalBadRequest,
            ),
            (
                response_error(400, "Bad Request: chat not found")?.into(),
                OutboundSendErrorClass::TerminalBadRequest,
            ),
            (
                response_error(500, "Internal Server Error")?.into(),
                OutboundSendErrorClass::TerminalOther,
            ),
            (
                RichApiError::Api {
                    code: 429,
                    description: "Too Many Requests: retry after 3".to_owned(),
                    retry_after: Some(3),
                }
                .into(),
                OutboundSendErrorClass::RetryableRateLimited {
                    retry_after_secs: 3,
                },
            ),
            (
                RichApiError::Api {
                    code: 403,
                    description: "Forbidden: bot was blocked by the user".to_owned(),
                    retry_after: None,
                }
                .into(),
                OutboundSendErrorClass::TerminalPermission,
            ),
            (
                RichApiError::Api {
                    code: 400,
                    description: "Bad Request: CHAT_SEND_PHOTOS_FORBIDDEN".to_owned(),
                    retry_after: None,
                }
                .into(),
                OutboundSendErrorClass::TerminalPermission,
            ),
            (
                RichApiError::Api {
                    code: 400,
                    description: "Bad Request: message is too long".to_owned(),
                    retry_after: None,
                }
                .into(),
                OutboundSendErrorClass::TerminalBadRequest,
            ),
            (
                RichApiError::Decode("connection closed before message completed".to_owned())
                    .into(),
                OutboundSendErrorClass::TerminalOther,
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.classification(), expected, "{error}");
        }

        Ok(())
    }

    #[tokio::test]
    async fn classification_marks_connect_failures_retryable_and_mid_response_terminal() {
        let connect_error = reqwest::Client::new()
            .get("http://127.0.0.1:1/")
            .send()
            .await
            .expect_err("connecting to a closed port must fail");
        assert!(connect_error.is_connect());
        let error =
            TelegramOutboundExecuteError::from(carapax::api::ExecuteError::Http(connect_error));
        assert_eq!(
            error.classification(),
            OutboundSendErrorClass::RetryableTransient
        );

        // A listener that never answers: the connection succeeds (kernel backlog),
        // so the timeout fires after the request left the process — mid-response.
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("bind local hung listener");
        let addr = listener.local_addr().expect("hung listener address");
        let mid_response_error = reqwest::Client::new()
            .get(format!("http://{addr}/"))
            .timeout(Duration::from_millis(200))
            .send()
            .await
            .expect_err("request against a hung listener must time out");
        assert!(!mid_response_error.is_connect());
        let error = TelegramOutboundExecuteError::from(carapax::api::ExecuteError::Http(
            mid_response_error,
        ));
        assert_eq!(
            error.classification(),
            OutboundSendErrorClass::TerminalOther
        );
        drop(listener);
    }

    fn text_send_method(chat_id: i64, text: &str) -> TelegramOutboundMethod {
        TelegramOutboundMethod::from(carapax::types::SendMessage::new(chat_id, text))
    }

    fn rich_rate_limited(retry_after_secs: u64) -> TelegramOutboundExecuteError {
        RichApiError::Api {
            code: 429,
            description: format!("Too Many Requests: retry after {retry_after_secs}"),
            retry_after: Some(retry_after_secs),
        }
        .into()
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_retry_retries_short_429_then_succeeds() {
        let sent_texts = Arc::new(Mutex::new(Vec::new()));

        let result = send_outbound_method_with_bounded_retry(
            {
                let sent_texts = Arc::clone(&sent_texts);
                move |method: TelegramOutboundMethod| {
                    let sent_texts = Arc::clone(&sent_texts);
                    async move {
                        let TelegramOutboundMethod::SendMessage(send) = &method else {
                            panic!("expected sendMessage method");
                        };
                        let payload =
                            serde_json::to_value(send.as_ref()).expect("payload serializes");
                        let attempts = {
                            let mut sent_texts = sent_texts.lock().expect("sent texts lock");
                            sent_texts.push(payload["text"].clone());
                            sent_texts.len()
                        };
                        if attempts == 1 {
                            Err(rich_rate_limited(2))
                        } else {
                            Ok(TelegramOutboundResponse::Boolean(true))
                        }
                    }
                }
            },
            text_send_method(42, "hello"),
            "vmsg-retry",
            42,
        )
        .await;

        assert!(matches!(
            result,
            Ok(TelegramOutboundResponse::Boolean(true))
        ));
        assert_eq!(
            *sent_texts.lock().expect("sent texts lock"),
            vec![json!("hello"), json!("hello")]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_retry_stops_after_max_attempts() {
        let attempts = Arc::new(Mutex::new(0u32));

        let result = send_outbound_method_with_bounded_retry(
            {
                let attempts = Arc::clone(&attempts);
                move |_method: TelegramOutboundMethod| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        *attempts.lock().expect("attempts lock") += 1;
                        Err::<TelegramOutboundResponse, _>(rich_rate_limited(1))
                    }
                }
            },
            text_send_method(42, "hello"),
            "vmsg-exhausted",
            42,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            *attempts.lock().expect("attempts lock"),
            super::OUTBOUND_SEND_MAX_ATTEMPTS
        );
    }

    #[tokio::test]
    async fn bounded_retry_gives_up_on_permission_error() {
        let attempts = Arc::new(Mutex::new(0u32));

        let result = send_outbound_method_with_bounded_retry(
            {
                let attempts = Arc::clone(&attempts);
                move |_method: TelegramOutboundMethod| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        *attempts.lock().expect("attempts lock") += 1;
                        Err::<TelegramOutboundResponse, _>(
                            RichApiError::Api {
                                code: 403,
                                description: "Forbidden: bot was blocked by the user".to_owned(),
                                retry_after: None,
                            }
                            .into(),
                        )
                    }
                }
            },
            text_send_method(42, "hello"),
            "vmsg-permission",
            42,
        )
        .await;

        let error = result.expect_err("permission error is terminal");
        assert_eq!(
            error.classification(),
            OutboundSendErrorClass::TerminalPermission
        );
        assert_eq!(*attempts.lock().expect("attempts lock"), 1);
    }

    #[tokio::test]
    async fn bounded_retry_does_not_replay_mid_response_transport_failures() {
        let attempts = Arc::new(Mutex::new(0u32));

        let result = send_outbound_method_with_bounded_retry(
            {
                let attempts = Arc::clone(&attempts);
                move |_method: TelegramOutboundMethod| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        *attempts.lock().expect("attempts lock") += 1;
                        Err::<TelegramOutboundResponse, _>(
                            RichApiError::Decode(
                                "connection closed before message completed".to_owned(),
                            )
                            .into(),
                        )
                    }
                }
            },
            text_send_method(42, "hello"),
            "vmsg-mid-response",
            42,
        )
        .await;

        let error = result.expect_err("mid-response failure is terminal");
        assert_eq!(
            error.classification(),
            OutboundSendErrorClass::TerminalOther
        );
        assert_eq!(*attempts.lock().expect("attempts lock"), 1);
    }

    #[tokio::test]
    async fn bounded_retry_does_not_honor_long_retry_after() {
        let attempts = Arc::new(Mutex::new(0u32));

        let result = send_outbound_method_with_bounded_retry(
            {
                let attempts = Arc::clone(&attempts);
                move |_method: TelegramOutboundMethod| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        *attempts.lock().expect("attempts lock") += 1;
                        Err::<TelegramOutboundResponse, _>(rich_rate_limited(30))
                    }
                }
            },
            text_send_method(42, "hello"),
            "vmsg-long-wait",
            42,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(*attempts.lock().expect("attempts lock"), 1);
    }

    #[tokio::test]
    async fn bounded_retry_returns_error_when_method_cannot_be_replayed()
    -> Result<(), Box<dyn std::error::Error>> {
        let sticker = TelegramOutboundMethod::from(
            build_sticker_message_method(
                &StickerMessageRequest {
                    chat: Some(chat(42)),
                    message_thread_id: 0,
                    disable_notification: false,
                    file_id: "sticker-id".to_owned(),
                },
                None,
            )
            .expect("sticker method"),
        );
        let attempts = Arc::new(Mutex::new(0u32));

        let result = send_outbound_method_with_bounded_retry(
            {
                let attempts = Arc::clone(&attempts);
                move |_method: TelegramOutboundMethod| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        *attempts.lock().expect("attempts lock") += 1;
                        Err::<TelegramOutboundResponse, _>(rich_rate_limited(1))
                    }
                }
            },
            sticker,
            "vmsg-no-replay",
            42,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(*attempts.lock().expect("attempts lock"), 1);
        Ok(())
    }

    fn response_error(
        code: i64,
        description: &str,
    ) -> Result<carapax::api::ExecuteError, Box<dyn std::error::Error>> {
        let response: carapax::types::Response<serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "ok": false,
                "error_code": code,
                "description": description,
            }))?;
        match response.into_result() {
            Ok(_) => panic!("test response unexpectedly succeeded"),
            Err(error) => Ok(carapax::api::ExecuteError::Response(error)),
        }
    }

    fn rate_limited_error(
        retry_after: u64,
    ) -> Result<carapax::api::ExecuteError, Box<dyn std::error::Error>> {
        let response: carapax::types::Response<serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "ok": false,
                "error_code": 429,
                "description": format!("Too Many Requests: retry after {retry_after}"),
                "parameters": {"retry_after": retry_after},
            }))?;
        match response.into_result() {
            Ok(_) => panic!("test response unexpectedly succeeded"),
            Err(error) => Ok(carapax::api::ExecuteError::Response(error)),
        }
    }
}
