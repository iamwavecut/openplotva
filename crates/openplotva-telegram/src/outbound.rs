//! Outbound Telegram request builders ported from the Go server send path.

use carapax::types::{
    EditMessageText, InlineKeyboardMarkup, InputFile, ParseMode, ReplyMarkup, ReplyParameters,
    ReplyParametersError, SendMessage, SendSticker,
};
use thiserror::Error;

use crate::{TELEGRAM_PARSE_MODE_HTML, extract_visible_text, split_telegram_text};

/// Telegram text message limit used by the Go outbound server.
pub const TELEGRAM_TEXT_MAX_BYTES: usize = 4096;

/// Minimal chat fields needed by the outbound builder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChatRef {
    /// Telegram chat ID.
    pub id: i64,
    /// Whether the chat is a forum supergroup.
    pub is_forum: bool,
}

/// Minimal replied-to message fields used by the Go reply helpers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyMessageRef {
    /// Replied-to Telegram message ID.
    pub message_id: i64,
    /// Replied-to message chat.
    pub chat: ChatRef,
    /// Whether the replied-to message belongs to a forum topic.
    pub is_topic_message: bool,
    /// Replied-to message topic ID.
    pub message_thread_id: i64,
}

/// Text send request fields used by the Go `TextMessage` builder.
#[derive(Clone, Debug, PartialEq)]
pub struct TextMessageRequest {
    /// Target chat when not replying to an existing message.
    pub chat: Option<ChatRef>,
    /// Target topic ID for forum chats.
    pub message_thread_id: i64,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Go pointer semantics: `None` means default true.
    pub allow_sending_without_reply: Option<bool>,
    /// Message text.
    pub text: String,
    /// Go `RenderAs` parse mode string.
    pub render_as: String,
    /// Markup attached only to the last split text part.
    pub reply_markup: Option<ReplyMarkup>,
}

/// Text edit request fields used by the Go `TextMessage` builder.
#[derive(Clone, Debug, PartialEq)]
pub struct EditTextMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Message ID to edit.
    pub message_id: i64,
    /// New message text.
    pub text: String,
    /// Go `RenderAs` parse mode string.
    pub render_as: String,
    /// Go edit path only applies inline keyboard markup.
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

/// Sticker send request fields used by the Go `StickerMessage` builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StickerMessageRequest {
    /// Target chat when not replying to an existing message.
    pub chat: Option<ChatRef>,
    /// Target topic ID for forum chats.
    pub message_thread_id: i64,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Telegram file ID for the sticker.
    pub file_id: String,
}

/// Public reply-parameter mirror for asserting form-only `carapax` methods.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyParametersPlan {
    /// Message ID being replied to.
    pub message_id: i64,
    /// Chat ID of the replied-to message.
    pub chat_id: i64,
    /// Whether Telegram may send even if the reply target is gone.
    pub allow_sending_without_reply: bool,
}

/// Public sticker payload mirror for asserting form-only `carapax` methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StickerMessagePlan {
    /// Telegram target chat ID.
    pub chat_id: i64,
    /// Telegram sticker file ID.
    pub file_id: String,
    /// Forum topic ID, when Go would set it on the outbound config.
    pub message_thread_id: Option<i64>,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Reply parameters, when sending as a reply.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Outbound builder error matching Go validation failures where possible.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum OutboundBuildError {
    /// Go: `text is empty`.
    #[error("text is empty")]
    EmptyText,
    /// Go: `text is empty after formatting`.
    #[error("text is empty after formatting")]
    EmptyTextAfterFormatting,
    /// Go: `chat is not set`.
    #[error("chat is not set")]
    ChatNotSet,
    /// Go: `message ID is required for editing`.
    #[error("message ID is required for editing")]
    MessageIdRequired,
    /// `carapax` does not accept raw parse-mode strings.
    #[error("unsupported Telegram parse mode: {0}")]
    UnsupportedParseMode(String),
    /// Split text unexpectedly produced no outbound parts.
    #[error("no parts to send")]
    NoParts,
    /// `carapax` failed to serialize reply parameters for a form method.
    #[error("failed to serialize Telegram reply parameters: {0}")]
    ReplyParameters(String),
}

/// Build all outbound `sendMessage` methods for a text request.
pub fn build_text_message_methods(
    req: &TextMessageRequest,
    reply_to: Option<&ReplyMessageRef>,
) -> Result<Vec<SendMessage>, OutboundBuildError> {
    validate_text_message_text(&req.text, &req.render_as)?;
    let chat = message_target_chat(req.chat.as_ref(), reply_to)?;
    let parts = split_telegram_text(&req.text, &req.render_as, TELEGRAM_TEXT_MAX_BYTES);
    let total = parts.len();
    if total == 0 {
        return Err(OutboundBuildError::NoParts);
    }

    parts
        .into_iter()
        .enumerate()
        .map(|(idx, part)| build_text_message_method(req, chat, reply_to, part, idx + 1 == total))
        .collect()
}

/// Build one outbound `sendMessage` method for an already split text part.
pub fn build_text_message_method(
    req: &TextMessageRequest,
    chat: ChatRef,
    reply_to: Option<&ReplyMessageRef>,
    part: impl Into<String>,
    is_last_part: bool,
) -> Result<SendMessage, OutboundBuildError> {
    let mut method = SendMessage::new(chat.id, part);
    if req.disable_notification {
        method = method.with_disable_notification(true);
    }
    if let Some(parse_mode) = parse_mode_from_go(&req.render_as)? {
        method = method.with_parse_mode(parse_mode);
    }

    if let Some(reply) = reply_to {
        method = apply_reply_parameters(method, reply, req.allow_sending_without_reply);
        if let Some(thread_id) = reply_thread_id(reply).filter(|thread_id| *thread_id != 0) {
            method = method.with_message_thread_id(thread_id);
        }
    } else if chat.is_forum && req.message_thread_id != 0 {
        method = method.with_message_thread_id(req.message_thread_id);
    }

    if is_last_part && let Some(markup) = req.reply_markup.clone() {
        method = method.with_reply_markup(markup);
    }

    Ok(method)
}

/// Build an outbound `editMessageText` method.
pub fn build_edit_text_message_method(
    req: &EditTextMessageRequest,
) -> Result<EditMessageText, OutboundBuildError> {
    validate_text_message_text(&req.text, &req.render_as)?;
    if req.message_id == 0 {
        return Err(OutboundBuildError::MessageIdRequired);
    }

    let mut method =
        EditMessageText::for_chat_message(req.chat.id, req.message_id, req.text.clone());
    if let Some(parse_mode) = parse_mode_from_go(&req.render_as)? {
        method = method.with_parse_mode(parse_mode);
    }
    if let Some(markup) = req.reply_markup.clone() {
        method = method.with_reply_markup(markup);
    }
    Ok(method)
}

/// Build an outbound `sendSticker` method.
pub fn build_sticker_message_method(
    req: &StickerMessageRequest,
    reply_to: Option<&ReplyMessageRef>,
) -> Result<SendSticker, OutboundBuildError> {
    build_sticker_message_plan(req, reply_to)?.to_carapax()
}

/// Build an inspectable sticker payload plan matching Go `buildStickerMessageConfig`.
pub fn build_sticker_message_plan(
    req: &StickerMessageRequest,
    reply_to: Option<&ReplyMessageRef>,
) -> Result<StickerMessagePlan, OutboundBuildError> {
    let chat = message_target_chat(req.chat.as_ref(), reply_to)?;
    let (message_thread_id, reply_parameters) = if let Some(reply) = reply_to {
        (
            reply_thread_id(reply).filter(|thread_id| *thread_id != 0),
            Some(ReplyParametersPlan {
                message_id: reply.message_id,
                chat_id: reply.chat.id,
                allow_sending_without_reply: true,
            }),
        )
    } else {
        (
            chat.is_forum
                .then_some(req.message_thread_id)
                .filter(|thread_id| *thread_id != 0),
            None,
        )
    };

    Ok(StickerMessagePlan {
        chat_id: chat.id,
        file_id: req.file_id.clone(),
        message_thread_id,
        disable_notification: req.disable_notification,
        reply_parameters,
    })
}

/// Validate text exactly like Go before send/edit.
pub fn validate_text_message_text(text: &str, render_as: &str) -> Result<(), OutboundBuildError> {
    if text.is_empty() {
        return Err(OutboundBuildError::EmptyText);
    }
    if extract_visible_text(text, render_as).is_empty() {
        return Err(OutboundBuildError::EmptyTextAfterFormatting);
    }
    Ok(())
}

/// Choose the target chat using Go `messageTargetChat` precedence.
pub fn message_target_chat(
    req_chat: Option<&ChatRef>,
    reply_to: Option<&ReplyMessageRef>,
) -> Result<ChatRef, OutboundBuildError> {
    if let Some(reply) = reply_to {
        return Ok(reply.chat);
    }
    req_chat.copied().ok_or(OutboundBuildError::ChatNotSet)
}

/// Return the virtual-message topic ID following the Go helper.
pub fn forum_thread_id(chat: ChatRef, message_thread_id: i64) -> Option<i64> {
    chat.is_forum.then_some(message_thread_id)
}

/// Go pointer default for `AllowSendingWithoutReply`.
pub fn allow_sending_without_reply(value: Option<bool>) -> bool {
    value.unwrap_or(true)
}

/// Convert Go parse mode strings into `carapax` parse modes.
pub fn parse_mode_from_go(value: &str) -> Result<Option<ParseMode>, OutboundBuildError> {
    match value {
        "" => Ok(None),
        TELEGRAM_PARSE_MODE_HTML => Ok(Some(ParseMode::Html)),
        "Markdown" => Ok(Some(ParseMode::Markdown)),
        "MarkdownV2" => Ok(Some(ParseMode::MarkdownV2)),
        other => Err(OutboundBuildError::UnsupportedParseMode(other.to_owned())),
    }
}

impl StickerMessagePlan {
    /// Convert the inspectable plan into the `carapax` form-backed method.
    pub fn to_carapax(&self) -> Result<SendSticker, OutboundBuildError> {
        let mut method = SendSticker::new(self.chat_id, InputFile::file_id(self.file_id.clone()));
        if let Some(thread_id) = self.message_thread_id {
            method = method.with_message_thread_id(thread_id);
        }
        if self.disable_notification {
            method = method.with_disable_notification(true);
        }
        if let Some(reply) = self.reply_parameters {
            method = method.with_reply_parameters(reply.into_carapax())?;
        }
        Ok(method)
    }
}

impl ReplyParametersPlan {
    fn into_carapax(self) -> ReplyParameters {
        let mut params = ReplyParameters::new(self.message_id).with_chat_id(self.chat_id);
        if self.allow_sending_without_reply {
            params = params.with_allow_sending_without_reply(true);
        }
        params
    }
}

impl From<ReplyParametersError> for OutboundBuildError {
    fn from(value: ReplyParametersError) -> Self {
        Self::ReplyParameters(value.to_string())
    }
}

fn apply_reply_parameters(
    method: SendMessage,
    reply: &ReplyMessageRef,
    allow_without_reply: Option<bool>,
) -> SendMessage {
    let mut params = ReplyParameters::new(reply.message_id).with_chat_id(reply.chat.id);
    if allow_sending_without_reply(allow_without_reply) {
        params = params.with_allow_sending_without_reply(true);
    }
    method.with_reply_parameters(params)
}

fn reply_thread_id(reply: &ReplyMessageRef) -> Option<i64> {
    (reply.chat.is_forum && reply.is_topic_message).then_some(reply.message_thread_id)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ChatRef, EditTextMessageRequest, OutboundBuildError, ReplyMessageRef, ReplyParametersPlan,
        StickerMessageRequest, TextMessageRequest, allow_sending_without_reply,
        build_edit_text_message_method, build_sticker_message_method, build_sticker_message_plan,
        build_text_message_method, build_text_message_methods, forum_thread_id,
        message_target_chat, validate_text_message_text,
    };
    use crate::{
        InlineKeyboardButton, InlineKeyboardMarkup, ReplyMarkup, TELEGRAM_PARSE_MODE_HTML,
        TELEGRAM_TEXT_MAX_BYTES,
    };

    fn private_chat(id: i64) -> ChatRef {
        ChatRef {
            id,
            is_forum: false,
        }
    }

    fn forum_chat(id: i64) -> ChatRef {
        ChatRef { id, is_forum: true }
    }

    fn base_text_request(text: &str) -> TextMessageRequest {
        TextMessageRequest {
            chat: Some(private_chat(1)),
            message_thread_id: 0,
            disable_notification: false,
            allow_sending_without_reply: None,
            text: text.to_owned(),
            render_as: String::new(),
            reply_markup: None,
        }
    }

    #[test]
    fn validate_text_message_text_matches_go_errors() {
        assert_eq!(
            validate_text_message_text("", ""),
            Err(OutboundBuildError::EmptyText)
        );
        assert_eq!(
            validate_text_message_text("<b></b>", TELEGRAM_PARSE_MODE_HTML),
            Err(OutboundBuildError::EmptyTextAfterFormatting)
        );
        assert!(validate_text_message_text("<b>hello</b>", TELEGRAM_PARSE_MODE_HTML).is_ok());
    }

    #[test]
    fn message_target_chat_prefers_reply_chat() {
        let req_chat = private_chat(1);
        let reply = ReplyMessageRef {
            message_id: 9,
            chat: private_chat(2),
            is_topic_message: false,
            message_thread_id: 0,
        };

        assert_eq!(
            message_target_chat(Some(&req_chat), Some(&reply)),
            Ok(private_chat(2))
        );
        assert_eq!(message_target_chat(Some(&req_chat), None), Ok(req_chat));
        assert_eq!(
            message_target_chat(None, None),
            Err(OutboundBuildError::ChatNotSet)
        );
    }

    #[test]
    fn forum_thread_id_returns_value_only_for_forum_chats() {
        assert_eq!(forum_thread_id(forum_chat(42), 55), Some(55));
        assert_eq!(forum_thread_id(private_chat(42), 55), None);
    }

    #[test]
    fn allow_sending_without_reply_defaults_to_true() {
        assert!(allow_sending_without_reply(None));
        assert!(!allow_sending_without_reply(Some(false)));
    }

    #[test]
    fn build_text_message_method_uses_reply_message() -> Result<(), Box<dyn std::error::Error>> {
        let mut req = base_text_request("<b>part</b>");
        req.render_as = TELEGRAM_PARSE_MODE_HTML.to_owned();
        req.allow_sending_without_reply = Some(false);
        let reply = ReplyMessageRef {
            message_id: 9,
            chat: private_chat(42),
            is_topic_message: false,
            message_thread_id: 0,
        };

        let method =
            build_text_message_method(&req, reply.chat, Some(&reply), "<b>part</b>", false)?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["text"], json!("<b>part</b>"));
        assert_eq!(payload["parse_mode"], json!("HTML"));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(9));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(42));
        assert!(
            payload["reply_parameters"]
                .get("allow_sending_without_reply")
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn build_text_message_method_defaults_reply_to_allow_true()
    -> Result<(), Box<dyn std::error::Error>> {
        let req = base_text_request("part");
        let reply = ReplyMessageRef {
            message_id: 9,
            chat: private_chat(42),
            is_topic_message: false,
            message_thread_id: 0,
        };

        let method = build_text_message_method(&req, reply.chat, Some(&reply), "part", false)?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );

        Ok(())
    }

    #[test]
    fn build_text_message_method_uses_forum_thread_and_last_markup()
    -> Result<(), Box<dyn std::error::Error>> {
        let markup = ReplyMarkup::from([[InlineKeyboardButton::for_callback_data("ok", "ok")]]);
        let req = TextMessageRequest {
            chat: Some(forum_chat(42)),
            message_thread_id: 55,
            disable_notification: true,
            allow_sending_without_reply: None,
            text: "part".to_owned(),
            render_as: String::new(),
            reply_markup: Some(markup),
        };

        let method = build_text_message_method(&req, forum_chat(42), None, "part", true)?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_thread_id"], json!(55));
        assert_eq!(payload["disable_notification"], json!(true));
        assert!(payload.get("reply_markup").is_some());

        Ok(())
    }

    #[test]
    fn build_text_message_methods_puts_markup_on_last_part_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let markup = ReplyMarkup::from([[InlineKeyboardButton::for_callback_data("ok", "ok")]]);
        let req = TextMessageRequest {
            reply_markup: Some(markup),
            text: format!("{} {}", "a".repeat(TELEGRAM_TEXT_MAX_BYTES), "tail"),
            ..base_text_request("unused")
        };

        let methods = build_text_message_methods(&req, None)?;
        assert!(methods.len() > 1);

        let first = serde_json::to_value(&methods[0])?;
        let last = serde_json::to_value(&methods[methods.len() - 1])?;

        assert!(first.get("reply_markup").is_none());
        assert!(last.get("reply_markup").is_some());

        Ok(())
    }

    #[test]
    fn build_text_message_method_uses_reply_topic_thread() -> Result<(), Box<dyn std::error::Error>>
    {
        let req = base_text_request("part");
        let reply = ReplyMessageRef {
            message_id: 9,
            chat: forum_chat(42),
            is_topic_message: true,
            message_thread_id: 77,
        };

        let method = build_text_message_method(&req, reply.chat, Some(&reply), "part", false)?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["message_thread_id"], json!(77));

        Ok(())
    }

    #[test]
    fn build_sticker_message_plan_uses_reply_message() -> Result<(), Box<dyn std::error::Error>> {
        let req = StickerMessageRequest {
            chat: Some(private_chat(1)),
            message_thread_id: 0,
            disable_notification: true,
            file_id: "sticker-file".to_owned(),
        };
        let reply = ReplyMessageRef {
            message_id: 9,
            chat: private_chat(42),
            is_topic_message: false,
            message_thread_id: 0,
        };

        let plan = build_sticker_message_plan(&req, Some(&reply))?;

        assert_eq!(plan.chat_id, 42);
        assert_eq!(plan.file_id, "sticker-file");
        assert!(plan.disable_notification);
        assert_eq!(plan.message_thread_id, None);
        assert_eq!(
            plan.reply_parameters,
            Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            })
        );

        Ok(())
    }

    #[test]
    fn build_sticker_message_plan_uses_forum_thread_without_reply()
    -> Result<(), Box<dyn std::error::Error>> {
        let req = StickerMessageRequest {
            chat: Some(forum_chat(42)),
            message_thread_id: 55,
            disable_notification: false,
            file_id: "sticker-file".to_owned(),
        };

        let plan = build_sticker_message_plan(&req, None)?;

        assert_eq!(plan.chat_id, 42);
        assert_eq!(plan.file_id, "sticker-file");
        assert_eq!(plan.message_thread_id, Some(55));
        assert_eq!(plan.reply_parameters, None);

        Ok(())
    }

    #[test]
    fn build_sticker_message_plan_uses_reply_topic_thread() -> Result<(), Box<dyn std::error::Error>>
    {
        let req = StickerMessageRequest {
            chat: Some(private_chat(1)),
            message_thread_id: 0,
            disable_notification: false,
            file_id: "sticker-file".to_owned(),
        };
        let reply = ReplyMessageRef {
            message_id: 9,
            chat: forum_chat(42),
            is_topic_message: true,
            message_thread_id: 77,
        };

        let plan = build_sticker_message_plan(&req, Some(&reply))?;

        assert_eq!(plan.chat_id, 42);
        assert_eq!(plan.message_thread_id, Some(77));
        assert_eq!(
            plan.reply_parameters,
            Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            })
        );

        Ok(())
    }

    #[test]
    fn build_sticker_message_method_builds_carapax_method() -> Result<(), Box<dyn std::error::Error>>
    {
        let req = StickerMessageRequest {
            chat: Some(private_chat(42)),
            message_thread_id: 0,
            disable_notification: false,
            file_id: "sticker-file".to_owned(),
        };

        let _method = build_sticker_message_method(&req, None)?;

        Ok(())
    }

    #[test]
    fn build_edit_text_message_method_keeps_parse_mode_and_inline_markup()
    -> Result<(), Box<dyn std::error::Error>> {
        let markup =
            InlineKeyboardMarkup::from([[InlineKeyboardButton::for_callback_data("ok", "ok")]]);
        let req = EditTextMessageRequest {
            chat: private_chat(42),
            message_id: 77,
            text: "<b>edited</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: Some(markup),
        };

        let method = build_edit_text_message_method(&req)?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(77));
        assert_eq!(payload["text"], json!("<b>edited</b>"));
        assert_eq!(payload["parse_mode"], json!("HTML"));
        assert!(payload.get("reply_markup").is_some());

        Ok(())
    }

    #[test]
    fn build_edit_text_message_method_requires_message_id() {
        let req = EditTextMessageRequest {
            chat: private_chat(42),
            message_id: 0,
            text: "edited".to_owned(),
            render_as: String::new(),
            reply_markup: None,
        };

        assert_eq!(
            build_edit_text_message_method(&req).err(),
            Some(OutboundBuildError::MessageIdRequired)
        );
    }
}
