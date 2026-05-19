//! Outbound Telegram request builders ported from the Go server send path.

use std::{borrow::Cow, fmt, io::Cursor};

use carapax::types::{
    EditMessageMedia, EditMessageText, InlineKeyboardMarkup, InputFile, InputFileReader,
    InputMedia, InputMediaError, InputMediaPhoto, MediaGroup, MediaGroupError, MediaGroupItem,
    ParseMode, ReplyMarkup, ReplyParameters, ReplyParametersError, SendAudio, SendMediaGroup,
    SendMessage, SendPhoto, SendSticker,
};
use crc::{CRC_32_ISCSI, Crc};
use thiserror::Error;

use crate::{TELEGRAM_PARSE_MODE_HTML, extract_visible_text, split_telegram_text};

/// Telegram text message limit used by the Go outbound server.
pub const TELEGRAM_TEXT_MAX_BYTES: usize = 4096;

/// Go message type string for outbound text fingerprints.
pub const MESSAGE_TYPE_TEXT: &str = "text";

const MESSAGE_TYPE_STICKER: &str = "sticker";
const MESSAGE_TYPE_PHOTO: &str = "photo";
const MESSAGE_TYPE_AUDIO: &str = "audio";
const CRC32_CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// Deduplication fingerprint key material used by the Go dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MessageFingerprint {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Go message type string, such as `text`, `photo`, or `audio`.
    pub message_type: String,
    /// CRC32-Castagnoli hash of the outbound content.
    pub content_hash: u32,
    /// Optional debounce namespace appended to the key.
    pub debounce_key: Option<String>,
}

impl fmt::Display for MessageFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{:x}",
            self.chat_id, self.message_type, self.content_hash
        )?;
        if let Some(debounce_key) = &self.debounce_key {
            write!(f, ":{debounce_key}")?;
        }
        Ok(())
    }
}

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

/// Telegram photo source variants used by Go `api.NewPhoto` call sites.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhotoSource {
    /// Existing Telegram file ID.
    FileId(String),
    /// Public URL for Telegram to fetch.
    Url(String),
    /// Uploaded file bytes with the multipart file name Go would attach.
    Bytes {
        /// Multipart file name.
        file_name: String,
        /// File bytes.
        bytes: Vec<u8>,
    },
}

/// Photo send request fields assembled by Go draw/fetcher paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhotoMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Target topic ID for forum chats.
    pub message_thread_id: i64,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Photo file, URL, or upload bytes.
    pub photo: PhotoSource,
    /// Optional Telegram caption.
    pub caption: String,
    /// Go `ParseMode` string for the caption.
    pub render_as: String,
    /// Whether Telegram should cover the media with a spoiler.
    pub has_spoiler: bool,
    /// Explicit reply parameters overlaid by the Go caller.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Public photo payload mirror for asserting form-only `carapax` methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhotoMessagePlan {
    /// Telegram target chat ID.
    pub chat_id: i64,
    /// Photo file, URL, or upload bytes.
    pub photo: PhotoSource,
    /// Forum topic ID, when Go would set it on the outbound config.
    pub message_thread_id: Option<i64>,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Optional Telegram caption.
    pub caption: String,
    /// Go `ParseMode` string for the caption.
    pub render_as: String,
    /// Whether Telegram should cover the media with a spoiler.
    pub has_spoiler: bool,
    /// Reply parameters, when sending as a reply.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Telegram audio source variants used by Go `api.NewAudio` call sites.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AudioSource {
    /// Existing Telegram file ID.
    FileId(String),
    /// Public URL for Telegram to fetch.
    Url(String),
    /// Uploaded audio bytes with the multipart file name Go attaches.
    Bytes {
        /// Multipart file name.
        file_name: String,
        /// File bytes.
        bytes: Vec<u8>,
    },
}

/// Audio send request fields assembled by the Go song generation path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AudioMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Target topic ID when present in the music job.
    pub message_thread_id: i64,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Audio file, URL, or upload bytes.
    pub audio: AudioSource,
    /// Optional Telegram caption.
    pub caption: String,
    /// Go `ParseMode` string for the caption.
    pub render_as: String,
    /// Explicit reply parameters overlaid by the Go caller.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Public audio payload mirror for asserting form-only `carapax` methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AudioMessagePlan {
    /// Telegram target chat ID.
    pub chat_id: i64,
    /// Audio file, URL, or upload bytes.
    pub audio: AudioSource,
    /// Forum topic ID, when Go would set it on the outbound config.
    pub message_thread_id: Option<i64>,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Optional Telegram caption.
    pub caption: String,
    /// Go `ParseMode` string for the caption.
    pub render_as: String,
    /// Reply parameters, when sending as a reply.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Photo item in a Telegram media group, matching Go `api.InputMediaPhoto`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaGroupPhotoItem {
    /// Photo file, URL, or upload bytes.
    pub photo: PhotoSource,
    /// Optional Telegram caption.
    pub caption: String,
    /// Go `ParseMode` string for the caption.
    pub render_as: String,
    /// Whether Telegram should cover the media with a spoiler.
    pub has_spoiler: bool,
}

/// Media group send request fields assembled by the Go image workflow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaGroupMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Target topic ID when present in the draw request.
    pub message_thread_id: i64,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Ordered album items.
    pub items: Vec<MediaGroupPhotoItem>,
    /// Explicit reply parameters overlaid by the Go caller.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Public media-group payload mirror for asserting form-only `carapax` methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MediaGroupMessagePlan {
    /// Telegram target chat ID.
    pub chat_id: i64,
    /// Forum topic ID, when Go would set it on the outbound config.
    pub message_thread_id: Option<i64>,
    /// Whether Telegram should suppress user notification sound.
    pub disable_notification: bool,
    /// Ordered album items.
    pub items: Vec<MediaGroupPhotoItem>,
    /// Reply parameters, when sending as a reply.
    pub reply_parameters: Option<ReplyParametersPlan>,
}

/// Edit-media request fields used by the Go generated-image placeholder replacement path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditMediaMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Existing placeholder message ID.
    pub message_id: i64,
    /// Prepared replacement media.
    pub media: MediaGroupPhotoItem,
}

/// Public edit-media payload mirror for asserting form-only `carapax` methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EditMediaMessagePlan {
    /// Telegram target chat ID.
    pub chat_id: i64,
    /// Existing placeholder message ID.
    pub message_id: i64,
    /// Prepared replacement media.
    pub media: MediaGroupPhotoItem,
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
    /// `carapax` rejected a Telegram media group.
    #[error("failed to build Telegram media group: {0}")]
    MediaGroup(String),
    /// `carapax` rejected Telegram input media.
    #[error("failed to build Telegram input media: {0}")]
    InputMedia(String),
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

/// Build an outbound `sendPhoto` method.
pub fn build_photo_message_method(
    req: &PhotoMessageRequest,
) -> Result<SendPhoto, OutboundBuildError> {
    build_photo_message_plan(req)?.to_carapax()
}

/// Build an inspectable photo payload plan matching Go `api.NewPhoto` overlays.
pub fn build_photo_message_plan(
    req: &PhotoMessageRequest,
) -> Result<PhotoMessagePlan, OutboundBuildError> {
    Ok(PhotoMessagePlan {
        chat_id: req.chat.id,
        photo: req.photo.clone(),
        message_thread_id: (req.message_thread_id != 0).then_some(req.message_thread_id),
        disable_notification: req.disable_notification,
        caption: req.caption.clone(),
        render_as: req.render_as.clone(),
        has_spoiler: req.has_spoiler,
        reply_parameters: req.reply_parameters,
    })
}

/// Build an outbound `sendAudio` method.
pub fn build_audio_message_method(
    req: &AudioMessageRequest,
) -> Result<SendAudio, OutboundBuildError> {
    build_audio_message_plan(req).to_carapax()
}

/// Build an inspectable audio payload plan matching Go `api.NewAudio` overlays.
pub fn build_audio_message_plan(req: &AudioMessageRequest) -> AudioMessagePlan {
    AudioMessagePlan {
        chat_id: req.chat.id,
        audio: req.audio.clone(),
        message_thread_id: (req.message_thread_id != 0).then_some(req.message_thread_id),
        disable_notification: req.disable_notification,
        caption: req.caption.clone(),
        render_as: req.render_as.clone(),
        reply_parameters: req.reply_parameters,
    }
}

/// Build an outbound `sendMediaGroup` method.
pub fn build_media_group_message_method(
    req: &MediaGroupMessageRequest,
) -> Result<SendMediaGroup, OutboundBuildError> {
    build_media_group_message_plan(req).to_carapax()
}

/// Build an inspectable media-group payload plan matching Go `api.NewMediaGroup` overlays.
pub fn build_media_group_message_plan(req: &MediaGroupMessageRequest) -> MediaGroupMessagePlan {
    MediaGroupMessagePlan {
        chat_id: req.chat.id,
        message_thread_id: (req.message_thread_id != 0).then_some(req.message_thread_id),
        disable_notification: req.disable_notification,
        items: req.items.clone(),
        reply_parameters: req.reply_parameters,
    }
}

/// Build an outbound `editMessageMedia` method.
pub fn build_edit_media_message_method(
    req: &EditMediaMessageRequest,
) -> Result<EditMessageMedia, OutboundBuildError> {
    build_edit_media_message_plan(req).to_carapax()
}

/// Build an inspectable edit-media payload plan matching Go `editMessageMediaConfig`.
pub fn build_edit_media_message_plan(req: &EditMediaMessageRequest) -> EditMediaMessagePlan {
    EditMediaMessagePlan {
        chat_id: req.chat.id,
        message_id: req.message_id,
        media: req.media.clone(),
    }
}

/// Hash outbound content with Go's CRC32-Castagnoli deduplication algorithm.
pub fn hash_content(content: &str) -> u32 {
    CRC32_CASTAGNOLI.checksum(content.as_bytes())
}

/// Build the Go-equivalent fingerprint for a sticker send plan.
pub fn fingerprint_sticker_message_plan(plan: &StickerMessagePlan) -> MessageFingerprint {
    message_fingerprint(
        plan.chat_id,
        MESSAGE_TYPE_STICKER,
        hash_content(&plan.file_id),
    )
}

/// Build the Go-equivalent fingerprint for a photo send plan.
pub fn fingerprint_photo_message_plan(plan: &PhotoMessagePlan) -> MessageFingerprint {
    let content = plan.photo.fingerprint_content();
    message_fingerprint(
        plan.chat_id,
        MESSAGE_TYPE_PHOTO,
        hash_content(content.as_ref()),
    )
}

/// Build the Go-equivalent fingerprint for an audio send plan.
pub fn fingerprint_audio_message_plan(plan: &AudioMessagePlan) -> MessageFingerprint {
    let content = plan.audio.fingerprint_content();
    message_fingerprint(
        plan.chat_id,
        MESSAGE_TYPE_AUDIO,
        hash_content(content.as_ref()),
    )
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

impl PhotoMessagePlan {
    /// Convert the inspectable plan into the `carapax` form-backed method.
    pub fn to_carapax(&self) -> Result<SendPhoto, OutboundBuildError> {
        let mut method = SendPhoto::new(self.chat_id, self.photo.to_input_file());
        if let Some(thread_id) = self.message_thread_id {
            method = method.with_message_thread_id(thread_id);
        }
        if self.disable_notification {
            method = method.with_disable_notification(true);
        }
        if !self.caption.is_empty() {
            method = method.with_caption(self.caption.clone());
        }
        if let Some(parse_mode) = parse_mode_from_go(&self.render_as)? {
            method = method.with_caption_parse_mode(parse_mode);
        }
        if self.has_spoiler {
            method = method.with_has_spoiler(true);
        }
        if let Some(reply) = self.reply_parameters {
            method = method.with_reply_parameters(reply.into_carapax())?;
        }
        Ok(method)
    }
}

impl PhotoSource {
    fn to_input_file(&self) -> InputFile {
        match self {
            Self::FileId(file_id) => InputFile::file_id(file_id.clone()),
            Self::Url(url) => InputFile::url(url.clone()),
            Self::Bytes { file_name, bytes } => InputFileReader::new(Cursor::new(bytes.clone()))
                .with_file_name(file_name.clone())
                .into(),
        }
    }

    fn fingerprint_content(&self) -> Cow<'_, str> {
        match self {
            Self::FileId(file_id) | Self::Url(file_id) => Cow::Borrowed(file_id),
            Self::Bytes { file_name, bytes } => Cow::Owned(format_go_file_bytes(file_name, bytes)),
        }
    }
}

impl AudioMessagePlan {
    /// Convert the inspectable plan into the `carapax` form-backed method.
    pub fn to_carapax(&self) -> Result<SendAudio, OutboundBuildError> {
        let mut method = SendAudio::new(self.chat_id, self.audio.to_input_file());
        if let Some(thread_id) = self.message_thread_id {
            method = method.with_message_thread_id(thread_id);
        }
        if self.disable_notification {
            method = method.with_disable_notification(true);
        }
        if !self.caption.is_empty() {
            method = method.with_caption(self.caption.clone());
        }
        if let Some(parse_mode) = parse_mode_from_go(&self.render_as)? {
            method = method.with_caption_parse_mode(parse_mode);
        }
        if let Some(reply) = self.reply_parameters {
            method = method.with_reply_parameters(reply.into_carapax())?;
        }
        Ok(method)
    }
}

impl AudioSource {
    fn to_input_file(&self) -> InputFile {
        match self {
            Self::FileId(file_id) => InputFile::file_id(file_id.clone()),
            Self::Url(url) => InputFile::url(url.clone()),
            Self::Bytes { file_name, bytes } => InputFileReader::new(Cursor::new(bytes.clone()))
                .with_file_name(file_name.clone())
                .into(),
        }
    }

    fn fingerprint_content(&self) -> Cow<'_, str> {
        match self {
            Self::FileId(file_id) | Self::Url(file_id) => Cow::Borrowed(file_id),
            Self::Bytes { file_name, bytes } => Cow::Owned(format_go_file_bytes(file_name, bytes)),
        }
    }
}

impl MediaGroupMessagePlan {
    /// Convert the inspectable plan into the `carapax` form-backed method.
    pub fn to_carapax(&self) -> Result<SendMediaGroup, OutboundBuildError> {
        let items = self
            .items
            .iter()
            .map(MediaGroupPhotoItem::to_carapax)
            .collect::<Result<Vec<_>, _>>()?;
        let group = MediaGroup::new(items)?;
        let mut method = SendMediaGroup::new(self.chat_id, group);
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

impl MediaGroupPhotoItem {
    fn to_carapax(&self) -> Result<MediaGroupItem, OutboundBuildError> {
        Ok(MediaGroupItem::for_photo(
            self.photo.to_input_file(),
            self.photo_metadata()?,
        ))
    }

    fn to_input_media(&self) -> Result<InputMedia, OutboundBuildError> {
        Ok(InputMedia::for_photo(
            self.photo.to_input_file(),
            self.photo_metadata()?,
        ))
    }

    fn photo_metadata(&self) -> Result<InputMediaPhoto, OutboundBuildError> {
        let mut metadata = InputMediaPhoto::default();
        if !self.caption.is_empty() {
            metadata = metadata.with_caption(self.caption.clone());
        }
        if let Some(parse_mode) = parse_mode_from_go(&self.render_as)? {
            metadata = metadata.with_caption_parse_mode(parse_mode);
        }
        if self.has_spoiler {
            metadata = metadata.with_has_spoiler(true);
        }
        Ok(metadata)
    }
}

impl EditMediaMessagePlan {
    /// Convert the inspectable plan into the `carapax` form-backed method.
    pub fn to_carapax(&self) -> Result<EditMessageMedia, OutboundBuildError> {
        Ok(EditMessageMedia::for_chat_message(
            self.chat_id,
            self.message_id,
            self.media.to_input_media()?,
        )?)
    }
}

impl From<ReplyParametersError> for OutboundBuildError {
    fn from(value: ReplyParametersError) -> Self {
        Self::ReplyParameters(value.to_string())
    }
}

impl From<MediaGroupError> for OutboundBuildError {
    fn from(value: MediaGroupError) -> Self {
        Self::MediaGroup(value.to_string())
    }
}

impl From<InputMediaError> for OutboundBuildError {
    fn from(value: InputMediaError) -> Self {
        Self::InputMedia(value.to_string())
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

fn message_fingerprint(chat_id: i64, message_type: &str, content_hash: u32) -> MessageFingerprint {
    MessageFingerprint {
        chat_id,
        message_type: message_type.to_owned(),
        content_hash,
        debounce_key: None,
    }
}

fn format_go_file_bytes(file_name: &str, bytes: &[u8]) -> String {
    let mut formatted = String::new();
    formatted.push('{');
    formatted.push_str(file_name);
    formatted.push_str(" [");
    for (idx, byte) in bytes.iter().enumerate() {
        if idx > 0 {
            formatted.push(' ');
        }
        formatted.push_str(&byte.to_string());
    }
    formatted.push_str("]}");
    formatted
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        AudioMessagePlan, AudioMessageRequest, AudioSource, ChatRef, EditMediaMessageRequest,
        EditTextMessageRequest, MESSAGE_TYPE_TEXT, MediaGroupMessageRequest, MediaGroupPhotoItem,
        MessageFingerprint, OutboundBuildError, PhotoMessagePlan, PhotoMessageRequest, PhotoSource,
        ReplyMessageRef, ReplyParametersPlan, StickerMessagePlan, StickerMessageRequest,
        TextMessageRequest, allow_sending_without_reply, build_audio_message_method,
        build_audio_message_plan, build_edit_media_message_method, build_edit_media_message_plan,
        build_edit_text_message_method, build_media_group_message_method,
        build_media_group_message_plan, build_photo_message_method, build_photo_message_plan,
        build_sticker_message_method, build_sticker_message_plan, build_text_message_method,
        build_text_message_methods, fingerprint_audio_message_plan, fingerprint_photo_message_plan,
        fingerprint_sticker_message_plan, forum_thread_id, hash_content, message_target_chat,
        validate_text_message_text,
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
    fn message_fingerprint_key_matches_go_hot_path_format() {
        let fp = MessageFingerprint {
            chat_id: -100123,
            message_type: MESSAGE_TYPE_TEXT.to_owned(),
            content_hash: 0x1a2b3c,
            debounce_key: Some("reply".to_owned()),
        };

        assert_eq!(fp.to_string(), "-100123:text:1a2b3c:reply");
    }

    #[test]
    fn hash_content_matches_go_castagnoli_crc32() {
        assert_eq!(hash_content("same outbound payload"), 0x32c39d97);
        assert_eq!(hash_content(""), 0);
    }

    #[test]
    fn fingerprint_photo_plan_hashes_reusable_file_id_without_formatting() {
        let plan = PhotoMessagePlan {
            chat_id: 42,
            photo: PhotoSource::FileId("photo-file-id".to_owned()),
            message_thread_id: None,
            disable_notification: false,
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
            reply_parameters: None,
        };

        let fingerprint = fingerprint_photo_message_plan(&plan);

        assert_eq!(fingerprint.chat_id, 42);
        assert_eq!(fingerprint.message_type, "photo");
        assert_eq!(fingerprint.content_hash, 0xa2fb5546);
        assert_eq!(fingerprint.debounce_key, None);
    }

    #[test]
    fn fingerprint_photo_plan_hashes_uploaded_file_like_go_file_bytes() {
        let plan = PhotoMessagePlan {
            chat_id: 42,
            photo: PhotoSource::Bytes {
                file_name: "plotva_image_provider_1.jpg".to_owned(),
                bytes: vec![1, 2, 3],
            },
            message_thread_id: None,
            disable_notification: false,
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
            reply_parameters: None,
        };

        let fingerprint = fingerprint_photo_message_plan(&plan);

        assert_eq!(fingerprint.message_type, "photo");
        assert_eq!(fingerprint.content_hash, 0x23e4d9b6);
    }

    #[test]
    fn fingerprint_media_plans_use_go_message_type_names() {
        let sticker = StickerMessagePlan {
            chat_id: 42,
            file_id: "sticker-file".to_owned(),
            message_thread_id: None,
            disable_notification: false,
            reply_parameters: None,
        };
        let audio = AudioMessagePlan {
            chat_id: 7,
            audio: AudioSource::FileId("song-file-id".to_owned()),
            message_thread_id: None,
            disable_notification: false,
            caption: String::new(),
            render_as: String::new(),
            reply_parameters: None,
        };

        let sticker = fingerprint_sticker_message_plan(&sticker);
        let audio = fingerprint_audio_message_plan(&audio);

        assert_eq!(sticker.message_type, "sticker");
        assert_eq!(sticker.content_hash, 0xee082665);
        assert_eq!(audio.chat_id, 7);
        assert_eq!(audio.message_type, "audio");
        assert_eq!(audio.content_hash, 0x18202a3a);
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
    fn build_photo_message_plan_uses_caption_spoiler_reply_and_thread()
    -> Result<(), Box<dyn std::error::Error>> {
        let req = PhotoMessageRequest {
            chat: forum_chat(42),
            message_thread_id: 77,
            disable_notification: true,
            photo: PhotoSource::Url("https://example.test/image.png".to_owned()),
            caption: "<b>caption</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            has_spoiler: true,
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let plan = build_photo_message_plan(&req)?;

        assert_eq!(plan.chat_id, 42);
        assert_eq!(
            plan.photo,
            PhotoSource::Url("https://example.test/image.png".to_owned())
        );
        assert_eq!(plan.message_thread_id, Some(77));
        assert!(plan.disable_notification);
        assert_eq!(plan.caption, "<b>caption</b>");
        assert_eq!(plan.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert!(plan.has_spoiler);
        assert_eq!(plan.reply_parameters, req.reply_parameters);

        Ok(())
    }

    #[test]
    fn build_photo_message_plan_omits_zero_thread_and_keeps_bytes_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let source = PhotoSource::Bytes {
            file_name: "image.png".to_owned(),
            bytes: vec![1, 2, 3],
        };
        let req = PhotoMessageRequest {
            chat: private_chat(42),
            message_thread_id: 0,
            disable_notification: false,
            photo: source.clone(),
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
            reply_parameters: None,
        };

        let plan = build_photo_message_plan(&req)?;

        assert_eq!(plan.message_thread_id, None);
        assert_eq!(plan.photo, source);
        assert_eq!(plan.caption, "");
        assert_eq!(plan.reply_parameters, None);

        Ok(())
    }

    #[test]
    fn build_photo_message_method_builds_carapax_method() -> Result<(), Box<dyn std::error::Error>>
    {
        let req = PhotoMessageRequest {
            chat: private_chat(42),
            message_thread_id: 0,
            disable_notification: false,
            photo: PhotoSource::FileId("photo-file".to_owned()),
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
            reply_parameters: None,
        };

        let _method = build_photo_message_method(&req)?;

        Ok(())
    }

    #[test]
    fn build_media_group_message_plan_keeps_placeholder_reply_thread_and_first_caption() {
        let first = MediaGroupPhotoItem {
            photo: PhotoSource::FileId("placeholder-file".to_owned()),
            caption: "<b>caption</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            has_spoiler: false,
        };
        let second = MediaGroupPhotoItem {
            photo: PhotoSource::FileId("placeholder-file".to_owned()),
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
        };
        let req = MediaGroupMessageRequest {
            chat: private_chat(42),
            message_thread_id: 77,
            disable_notification: false,
            items: vec![first.clone(), second.clone()],
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let plan = build_media_group_message_plan(&req);

        assert_eq!(plan.chat_id, 42);
        assert_eq!(plan.message_thread_id, Some(77));
        assert_eq!(plan.items, vec![first, second]);
        assert_eq!(plan.reply_parameters, req.reply_parameters);
    }

    #[test]
    fn build_media_group_message_method_builds_carapax_method()
    -> Result<(), Box<dyn std::error::Error>> {
        let req = MediaGroupMessageRequest {
            chat: private_chat(42),
            message_thread_id: 0,
            disable_notification: false,
            items: vec![
                MediaGroupPhotoItem {
                    photo: PhotoSource::FileId("first-photo".to_owned()),
                    caption: String::new(),
                    render_as: String::new(),
                    has_spoiler: false,
                },
                MediaGroupPhotoItem {
                    photo: PhotoSource::Url("https://example.test/second.png".to_owned()),
                    caption: String::new(),
                    render_as: String::new(),
                    has_spoiler: false,
                },
            ],
            reply_parameters: None,
        };

        let _method = build_media_group_message_method(&req)?;

        Ok(())
    }

    #[test]
    fn build_media_group_message_method_rejects_single_photo_album() {
        let req = MediaGroupMessageRequest {
            chat: private_chat(42),
            message_thread_id: 0,
            disable_notification: false,
            items: vec![MediaGroupPhotoItem {
                photo: PhotoSource::FileId("only-photo".to_owned()),
                caption: String::new(),
                render_as: String::new(),
                has_spoiler: false,
            }],
            reply_parameters: None,
        };

        let err = build_media_group_message_method(&req).err();

        assert!(matches!(err, Some(OutboundBuildError::MediaGroup(_))));
    }

    #[test]
    fn build_audio_message_plan_keeps_song_caption_reply_and_thread() {
        let source = AudioSource::Bytes {
            file_name: "song.mp3".to_owned(),
            bytes: vec![1, 2, 3],
        };
        let req = AudioMessageRequest {
            chat: private_chat(42),
            message_thread_id: 77,
            disable_notification: false,
            audio: source.clone(),
            caption: "<code>song</code>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let plan = build_audio_message_plan(&req);

        assert_eq!(plan.chat_id, 42);
        assert_eq!(plan.message_thread_id, Some(77));
        assert_eq!(plan.audio, source);
        assert_eq!(plan.caption, "<code>song</code>");
        assert_eq!(plan.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(plan.reply_parameters, req.reply_parameters);
    }

    #[test]
    fn build_audio_message_method_builds_carapax_method() -> Result<(), Box<dyn std::error::Error>>
    {
        let req = AudioMessageRequest {
            chat: private_chat(42),
            message_thread_id: 0,
            disable_notification: false,
            audio: AudioSource::Bytes {
                file_name: "song.mp3".to_owned(),
                bytes: vec![1, 2, 3],
            },
            caption: String::new(),
            render_as: String::new(),
            reply_parameters: None,
        };

        let _method = build_audio_message_method(&req)?;

        Ok(())
    }

    #[test]
    fn build_edit_media_message_plan_keeps_placeholder_target_and_prepared_photo() {
        let media = MediaGroupPhotoItem {
            photo: PhotoSource::Bytes {
                file_name: "plotva_image_provider_1.jpg".to_owned(),
                bytes: vec![1, 2, 3],
            },
            caption: "<b>caption</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            has_spoiler: true,
        };
        let req = EditMediaMessageRequest {
            chat: private_chat(42),
            message_id: 77,
            media: media.clone(),
        };

        let plan = build_edit_media_message_plan(&req);

        assert_eq!(plan.chat_id, 42);
        assert_eq!(plan.message_id, 77);
        assert_eq!(plan.media, media);
    }

    #[test]
    fn build_edit_media_message_method_builds_carapax_method()
    -> Result<(), Box<dyn std::error::Error>> {
        let req = EditMediaMessageRequest {
            chat: private_chat(42),
            message_id: 77,
            media: MediaGroupPhotoItem {
                photo: PhotoSource::Url("https://example.test/image.png".to_owned()),
                caption: String::new(),
                render_as: String::new(),
                has_spoiler: false,
            },
        };

        let _method = build_edit_media_message_method(&req)?;

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
