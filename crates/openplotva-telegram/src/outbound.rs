//! Outbound Telegram request builders ported from the Go server send path.

use std::{borrow::Cow, fmt, io::Cursor};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use carapax::types::{
    AnswerCallbackQuery, AnswerGuestQuery, AnswerInlineQuery, AnswerPreCheckoutQuery, ChatAction,
    ChatMember, CreateInvoiceLink, DeleteMessage, EditMessageCaption, EditMessageMedia,
    EditMessageReplyMarkup, EditMessageText, EditUserStarSubscription, GetChatAdministrators,
    GetChatMember, InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResult,
    InlineQueryResultArticle, InputFile, InputFileReader, InputMedia, InputMediaError,
    InputMediaPhoto, InputMessageContentText, InvoiceParameters, LabeledPrice, MediaGroup,
    MediaGroupError, MediaGroupItem, ParseMode, RefundStarPayment, ReplyMarkup, ReplyParameters,
    ReplyParametersError, SendAudio, SendChatAction, SendMediaGroup, SendMessage, SendPhoto,
    SendSticker, WebAppInfo,
};
use crc::{CRC_32_ISCSI, Crc};
use serde_json::{Map, Value, json};
use sha1::{Digest, Sha1};
use thiserror::Error;

use crate::{
    DispatcherPersistencePayload, TELEGRAM_PARSE_MODE_HTML, escape_telegram_html_text,
    extract_visible_text, sanitize_telegram_html, split_telegram_text, strip_telegram_html,
};

/// Telegram text message limit used by the Go outbound server.
pub const TELEGRAM_TEXT_MAX_BYTES: usize = 4096;

/// Go message type string for outbound text fingerprints.
pub const MESSAGE_TYPE_TEXT: &str = "text";

/// Button text used by Go settings WebApp keyboards.
pub const SETTINGS_BUTTON_TEXT: &str = "⚙️ Настройки";

/// Telegram inline message text limit used by Go guest answers.
pub const GUEST_INLINE_TEXT_LIMIT: usize = 4096;

/// Plain-text truncate limit used when sanitized guest HTML exceeds Telegram's inline limit.
pub const GUEST_INLINE_TRUNCATE_LIMIT: usize = 3900;

/// Payload used by Go guest add-to-chat links.
pub const GUEST_ADD_TO_CHAT_PAYLOAD: &str = "guest";

/// Fallback bot username used by Go guest helpers.
pub const DEFAULT_GUEST_BOT_USERNAME: &str = "PlotvoBot";

/// Telegram Stars currency used by Go payment requests.
pub const TELEGRAM_STARS_CURRENCY: &str = "XTR";

/// Go default VIP subscription price in Telegram Stars.
pub const SUBSCRIPTION_PRICE_STARS: i64 = 300;

/// Go lower donation command bound in Telegram Stars.
pub const MIN_DONATION_STARS: i64 = 10;

/// Go upper donation command bound in Telegram Stars.
pub const MAX_DONATION_STARS: i64 = 10000;

/// Go VIP subscription duration in days.
pub const SUBSCRIPTION_DURATION_DAYS: i64 = 30;

/// Bot API subscription period used by Go `createInvoiceLink` requests.
pub const SUBSCRIPTION_PERIOD_SECONDS: i64 = 2_592_000;

/// Go VIP invoice title and price label.
pub const VIP_SUBSCRIPTION_TITLE: &str = "VIP Подписка на 30 дней";

/// Go VIP invoice description.
pub const VIP_SUBSCRIPTION_DESCRIPTION: &str = "Подписка на VIP статус в боте на 30 дней. Включает приоритетную обработку запросов, двойные лимиты на рисование, генерацию музыки, изображения лучшего качества и ранний доступ к новым фичам!";

/// Go donation invoice title.
pub const DONATION_TITLE: &str = "Донат разработчику";

/// Go donation invoice description.
pub const DONATION_DESCRIPTION: &str = "Поддержите разработку бота! Ваш донат согреет сердце разработчика и поможет создавать новые функции.";

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

/// Caption edit request fields used by Go `api.NewEditMessageCaption` call sites.
#[derive(Clone, Debug, PartialEq)]
pub struct EditCaptionMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Message ID to edit.
    pub message_id: i64,
    /// New caption text.
    pub caption: String,
    /// Go `ParseMode` string for the caption.
    pub render_as: String,
    /// Optional inline keyboard markup.
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

/// Reply-markup edit request fields used by Go `api.NewEditMessageReplyMarkup` call sites.
#[derive(Clone, Debug, PartialEq)]
pub struct EditReplyMarkupMessageRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Message ID to edit.
    pub message_id: i64,
    /// Replacement inline keyboard markup, including Go's explicit empty markup.
    pub reply_markup: InlineKeyboardMarkup,
}

/// Chat action request fields used by Go `api.NewChatAction` call sites.
#[derive(Clone, Debug, PartialEq)]
pub struct ChatActionRequest {
    /// Target chat.
    pub chat: ChatRef,
    /// Target topic ID; Go omits zero values.
    pub message_thread_id: i64,
    /// Telegram chat action string, such as `typing` or `upload_photo`.
    pub action: String,
}

/// Callback query answer fields used by Go `api.NewCallback*` call sites.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallbackAnswerRequest {
    /// Telegram callback query ID.
    pub callback_query_id: String,
    /// Optional notification text; Go omits empty values.
    pub text: String,
    /// Whether to show an alert; Go omits false values.
    pub show_alert: bool,
    /// Optional URL; Go omits empty values.
    pub url: String,
    /// Optional cache time in seconds; Go omits zero values.
    pub cache_time: i64,
}

/// Inline article fields used by Go `api.NewInlineQueryResultArticle*` call sites.
#[derive(Clone, Debug, PartialEq)]
pub struct InlineArticleRequest {
    /// Inline result ID.
    pub id: String,
    /// Inline result title.
    pub title: String,
    /// Text sent when the result is selected.
    pub message_text: String,
    /// Go parse mode string; empty means plain text.
    pub render_as: String,
    /// Optional result description; Go omits empty values.
    pub description: String,
    /// Optional inline keyboard markup attached to the result.
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

/// Inline query answer fields used by Go `api.InlineConfig` call sites.
#[derive(Clone, Debug, PartialEq)]
pub struct InlineQueryAnswerRequest {
    /// Telegram inline query ID.
    pub inline_query_id: String,
    /// Results to return to Telegram.
    pub results: Vec<InlineQueryResult>,
    /// Optional cache time in seconds; Go omits zero values.
    pub cache_time: i64,
    /// Whether results are personal; Go omits false values.
    pub is_personal: bool,
    /// Optional pagination offset; Go omits empty values.
    pub next_offset: String,
}

/// Guest query answer fields used by Go `api.NewAnswerGuestQuery` call sites.
#[derive(Clone, Debug, PartialEq)]
pub struct GuestQueryAnswerRequest {
    /// Telegram guest query ID.
    pub guest_query_id: String,
    /// Inline result to send on behalf of the guest bot.
    pub result: InlineQueryResult,
}

/// HTML guest answer fields used by Go `Fetcher.answerGuestHTML`.
#[derive(Clone, Debug, PartialEq)]
pub struct GuestHtmlAnswerRequest {
    /// Telegram guest query ID.
    pub guest_query_id: String,
    /// Source message ID used as a fallback result ID seed.
    pub message_id: i64,
    /// Inline article title.
    pub title: String,
    /// Candidate HTML answer text.
    pub html_text: String,
    /// Bot username used for fallback and add-to-chat links.
    pub bot_username: String,
    /// Optional inline keyboard markup attached to the article.
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

/// Go payment payload classes used after Telegram reports a successful payment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PaymentPayloadKind {
    Unknown,
    Subscription,
    Donation,
}

/// VIP invoice link fields used by Go `executeVIPInvoiceControlJob`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionInvoiceLinkRequest {
    /// Telegram user ID that becomes the payment payload suffix.
    pub user_id: i64,
    /// Telegram username; Go discounts exactly `WaveCut`.
    pub user_name: String,
    /// Control-job override amount. Zero means Go's default subscription price.
    pub amount_stars: i64,
}

/// Donation invoice link fields used by Go `executeDonateInvoiceControlJob`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DonationInvoiceLinkRequest {
    /// Telegram user ID that becomes the payment payload suffix.
    pub user_id: i64,
    /// Donation amount in Telegram Stars.
    pub amount_stars: i64,
}

/// Delete request fields used by Go `api.NewDeleteMessage` call sites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteMessageRequest {
    /// Target chat ID.
    pub chat_id: i64,
    /// Message ID to delete.
    pub message_id: i64,
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
    /// Go drops blank chat action requests before sending.
    #[error("chat action is required")]
    ChatActionRequired,
    /// `carapax` models Telegram chat actions as a closed enum.
    #[error("unsupported Telegram chat action: {0}")]
    UnsupportedChatAction(String),
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
    #[error("failed to serialize Telegram persistence payload: {0}")]
    PersistencePayload(String),
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

/// Build an outbound `editMessageCaption` method.
pub fn build_edit_caption_message_method(
    req: &EditCaptionMessageRequest,
) -> Result<EditMessageCaption, OutboundBuildError> {
    if req.message_id == 0 {
        return Err(OutboundBuildError::MessageIdRequired);
    }

    let mut method = EditMessageCaption::for_chat_message(req.chat.id, req.message_id)
        .with_caption(req.caption.clone());
    if let Some(parse_mode) = parse_mode_from_go(&req.render_as)? {
        method = method.with_caption_parse_mode(parse_mode);
    }
    if let Some(markup) = req.reply_markup.clone() {
        method = method.with_reply_markup(markup);
    }
    Ok(method)
}

/// Build an outbound `editMessageReplyMarkup` method.
pub fn build_edit_reply_markup_message_method(
    req: &EditReplyMarkupMessageRequest,
) -> Result<EditMessageReplyMarkup, OutboundBuildError> {
    if req.message_id == 0 {
        return Err(OutboundBuildError::MessageIdRequired);
    }

    Ok(
        EditMessageReplyMarkup::for_chat_message(req.chat.id, req.message_id)
            .with_reply_markup(req.reply_markup.clone()),
    )
}

/// Build an outbound `sendChatAction` method.
pub fn build_chat_action_method(
    req: &ChatActionRequest,
) -> Result<SendChatAction, OutboundBuildError> {
    if req.chat.id == 0 {
        return Err(OutboundBuildError::ChatNotSet);
    }

    let action = chat_action_from_go(&req.action)?;
    let mut method = SendChatAction::new(req.chat.id, action);
    if req.message_thread_id != 0 {
        method = method.with_message_thread_id(req.message_thread_id);
    }
    Ok(method)
}

/// Build Go's `getChatMember` permission probe request.
#[must_use]
pub fn build_get_chat_member_method(chat_id: i64, user_id: i64) -> GetChatMember {
    GetChatMember::new(chat_id, user_id)
}

/// Build Go's `getChatAdministrators` admin-sync request.
#[must_use]
pub fn build_get_chat_administrators_method(chat_id: i64) -> GetChatAdministrators {
    GetChatAdministrators::new(chat_id)
}

/// Go `telegramMemberCanOpenGroupSettings`.
#[must_use]
pub fn telegram_member_can_open_group_settings(member: &ChatMember) -> bool {
    match member {
        ChatMember::Creator(_) => true,
        ChatMember::Administrator(admin) => admin.can_promote_members,
        ChatMember::Kicked(_)
        | ChatMember::Left(_)
        | ChatMember::Member { .. }
        | ChatMember::Restricted(_) => false,
    }
}

/// Build an outbound `answerCallbackQuery` method.
pub fn build_callback_answer_method(req: &CallbackAnswerRequest) -> AnswerCallbackQuery {
    let mut method = AnswerCallbackQuery::new(req.callback_query_id.clone());
    if !req.text.is_empty() {
        method = method.with_text(req.text.clone());
    }
    if req.show_alert {
        method = method.with_show_alert(true);
    }
    if !req.url.is_empty() {
        method = method.with_url(req.url.clone());
    }
    if req.cache_time != 0 {
        method = method.with_cache_time(req.cache_time);
    }
    method
}

/// Build an inline article result using Go `api.NewInlineQueryResultArticle*` semantics.
pub fn build_inline_query_result_article(
    req: &InlineArticleRequest,
) -> Result<InlineQueryResult, OutboundBuildError> {
    let mut content = InputMessageContentText::new(req.message_text.clone());
    if let Some(parse_mode) = parse_mode_from_go(&req.render_as)? {
        content = content.with_parse_mode(parse_mode);
    }

    let mut article = InlineQueryResultArticle::new(req.id.clone(), content, req.title.clone());
    if !req.description.is_empty() {
        article = article.with_description(req.description.clone());
    }
    if let Some(markup) = req.reply_markup.clone() {
        article = article.with_reply_markup(markup);
    }
    Ok(article.into())
}

/// Build an outbound `answerInlineQuery` method.
pub fn build_inline_query_answer_method(req: &InlineQueryAnswerRequest) -> AnswerInlineQuery {
    let mut method = AnswerInlineQuery::new(req.inline_query_id.clone(), req.results.clone());
    if req.cache_time != 0 {
        method = method.with_cache_time(req.cache_time);
    }
    if req.is_personal {
        method = method.with_is_personal(true);
    }
    if !req.next_offset.is_empty() {
        method = method.with_next_offset(req.next_offset.clone());
    }
    method
}

/// Build an outbound `answerGuestQuery` method.
pub fn build_guest_query_answer_method(req: &GuestQueryAnswerRequest) -> AnswerGuestQuery {
    AnswerGuestQuery::new(req.guest_query_id.clone(), req.result.clone())
}

/// Build the Go successful pre-checkout answer.
#[must_use]
pub fn build_pre_checkout_ok_method(
    pre_checkout_query_id: impl Into<String>,
) -> AnswerPreCheckoutQuery {
    AnswerPreCheckoutQuery::ok(pre_checkout_query_id)
}

/// Build Go `subscription_<user_id>` invoice payload text.
#[must_use]
pub fn subscription_invoice_payload(user_id: i64) -> String {
    format!("subscription_{user_id}")
}

/// Build Go `donation_<user_id>_<amount>` invoice payload text.
#[must_use]
pub fn donation_invoice_payload(user_id: i64, amount_stars: i64) -> String {
    format!("donation_{user_id}_{amount_stars}")
}

/// Return the Go payment payload class using prefix semantics.
#[must_use]
pub fn classify_payment_payload(payload: &str) -> PaymentPayloadKind {
    if payload.starts_with("subscription") {
        PaymentPayloadKind::Subscription
    } else if payload.starts_with("donation") {
        PaymentPayloadKind::Donation
    } else {
        PaymentPayloadKind::Unknown
    }
}

/// Return the Go VIP invoice price for one control-job request.
#[must_use]
pub fn subscription_invoice_price_stars(req: &SubscriptionInvoiceLinkRequest) -> i64 {
    if req.user_name == "WaveCut" {
        return 1;
    }
    if req.amount_stars > 0 {
        req.amount_stars
    } else {
        SUBSCRIPTION_PRICE_STARS
    }
}

/// Build Go `createInvoiceLink` for a VIP subscription invoice.
#[must_use]
pub fn build_subscription_invoice_link_method(
    req: &SubscriptionInvoiceLinkRequest,
) -> CreateInvoiceLink {
    CreateInvoiceLink::new(
        VIP_SUBSCRIPTION_TITLE,
        VIP_SUBSCRIPTION_DESCRIPTION,
        subscription_invoice_payload(req.user_id),
        TELEGRAM_STARS_CURRENCY,
        [LabeledPrice::new(
            subscription_invoice_price_stars(req),
            VIP_SUBSCRIPTION_TITLE,
        )],
    )
    .with_parameters(InvoiceParameters::default().with_provider_token(""))
    .with_subscription_period(SUBSCRIPTION_PERIOD_SECONDS)
}

/// Build Go `createInvoiceLink` for a donation invoice.
#[must_use]
pub fn build_donation_invoice_link_method(req: &DonationInvoiceLinkRequest) -> CreateInvoiceLink {
    CreateInvoiceLink::new(
        DONATION_TITLE,
        DONATION_DESCRIPTION,
        donation_invoice_payload(req.user_id, req.amount_stars),
        TELEGRAM_STARS_CURRENCY,
        [LabeledPrice::new(req.amount_stars, "Донат")],
    )
    .with_parameters(InvoiceParameters::default().with_provider_token(""))
}

/// Build Go `refundStarPayment` request params through `carapax`.
#[must_use]
pub fn build_refund_star_payment_method(
    user_id: i64,
    telegram_payment_charge_id: impl Into<String>,
) -> RefundStarPayment {
    RefundStarPayment::new(user_id, telegram_payment_charge_id)
}

/// Build Go cancellation request for a Telegram Stars subscription.
#[must_use]
pub fn build_cancel_star_subscription_method(
    user_id: i64,
    telegram_payment_charge_id: impl Into<String>,
) -> EditUserStarSubscription {
    EditUserStarSubscription::new(user_id, telegram_payment_charge_id, true)
}

/// Prepare guest HTML with Go `prepareGuestHTML` semantics.
#[must_use]
pub fn prepare_guest_html(text: &str) -> String {
    let sanitized = sanitize_telegram_html(text);
    if strip_telegram_html(&sanitized).trim().is_empty() {
        return String::new();
    }
    if sanitized.chars().count() <= GUEST_INLINE_TEXT_LIMIT {
        return sanitized;
    }

    let plain = strip_telegram_html(&sanitized);
    let truncated = if plain.chars().count() > GUEST_INLINE_TRUNCATE_LIMIT {
        let mut text = plain
            .chars()
            .take(GUEST_INLINE_TRUNCATE_LIMIT)
            .collect::<String>();
        text.push_str("...");
        text
    } else {
        plain
    };
    escape_telegram_html_text(&truncated)
}

/// Build Go `guestInlineDescription` from prepared guest HTML.
#[must_use]
pub fn guest_inline_description(html_text: &str) -> String {
    let plain = strip_telegram_html(html_text);
    if plain.chars().count() <= 120 {
        return plain;
    }
    let mut description = plain.chars().take(117).collect::<String>();
    description.push_str("...");
    description
}

/// Build Go `guestInlineResultID` from optional guest query and message ID.
#[must_use]
pub fn guest_inline_result_id(guest_query_id: Option<&str>, message_id: i64) -> String {
    let Some(query_id) = guest_query_id else {
        return "guest".to_owned();
    };
    let mut base = query_id.trim().to_owned();
    if base.is_empty() {
        base = format!("message-{message_id}");
    }
    if base.len() <= 58 {
        return format!("guest-{base}");
    }
    let digest = Sha1::digest(base.as_bytes());
    let hash = format!("{digest:x}");
    format!("guest-{}", &hash[..24])
}

/// Build Go `guestAddToChatURL`.
#[must_use]
pub fn guest_add_to_chat_url(bot_username: &str) -> String {
    let username = bot_username.trim().trim_start_matches('@');
    let username = if username.is_empty() {
        DEFAULT_GUEST_BOT_USERNAME
    } else {
        username
    };
    format!("https://t.me/{username}?startgroup={GUEST_ADD_TO_CHAT_PAYLOAD}")
}

/// Build Go `guestUnsupportedFeatureHTML`.
#[must_use]
pub fn guest_unsupported_feature_html(bot_username: &str) -> String {
    let add_url = guest_add_to_chat_url(bot_username);
    format!(
        "Некоторые функции Плотвы работают только в чате, куда её добавили: картинки, песни, настройки и длинные фоновые задачи.\n\n<a href=\"{add_url}\">Добавить Плотву в чат</a>"
    )
}

/// Build Go `guestDialogFallbackHTML`.
#[must_use]
pub fn guest_dialog_fallback_html(bot_username: &str) -> String {
    let add_url = guest_add_to_chat_url(bot_username);
    format!(
        "Не успела ответить в гостевом режиме. <a href=\"{add_url}\">Добавьте Плотву в чат</a>, если нужна длинная задача."
    )
}

/// Build Go `guestAddToChatMarkup`.
#[must_use]
pub fn build_guest_add_to_chat_markup(bot_username: &str) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_url(
            "Добавить Плотву в чат",
            guest_add_to_chat_url(bot_username),
        ),
    ])])
}

/// Build Go `Fetcher.answerGuestHTML`'s direct `answerGuestQuery` method.
#[must_use]
pub fn build_guest_html_answer_method(req: &GuestHtmlAnswerRequest) -> Option<AnswerGuestQuery> {
    let guest_query_id = req.guest_query_id.trim();
    if guest_query_id.is_empty() {
        return None;
    }

    let mut prepared = prepare_guest_html(&req.html_text);
    if prepared.is_empty() {
        prepared = prepare_guest_html(&guest_dialog_fallback_html(&req.bot_username));
    }

    let article = build_inline_query_result_article(&InlineArticleRequest {
        id: guest_inline_result_id(Some(guest_query_id), req.message_id),
        title: req.title.clone(),
        message_text: prepared.clone(),
        render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
        description: guest_inline_description(&prepared),
        reply_markup: req.reply_markup.clone(),
    })
    .expect("Telegram HTML parse mode is supported");

    Some(build_guest_query_answer_method(&GuestQueryAnswerRequest {
        guest_query_id: guest_query_id.to_owned(),
        result: article,
    }))
}

/// Build a callback-data inline keyboard button matching Go `api.NewInlineKeyboardButtonData`.
pub fn build_inline_keyboard_button_data(
    text: impl Into<String>,
    data: impl Into<String>,
) -> InlineKeyboardButton {
    InlineKeyboardButton::for_callback_data(text, data)
}

/// Build a URL inline keyboard button matching Go `api.NewInlineKeyboardButtonURL`.
pub fn build_inline_keyboard_button_url(
    text: impl Into<String>,
    url: impl Into<String>,
) -> InlineKeyboardButton {
    InlineKeyboardButton::for_url(text, url)
}

/// Build a WebApp inline keyboard button matching Go `InlineKeyboardButton{WebApp: ...}`.
pub fn build_inline_keyboard_button_web_app(
    text: impl Into<String>,
    url: impl Into<String>,
) -> InlineKeyboardButton {
    InlineKeyboardButton::for_web_app(text, WebAppInfo::from(url.into()))
}

/// Build an inline keyboard row matching Go `api.NewInlineKeyboardRow`.
pub fn build_inline_keyboard_row(
    buttons: impl IntoIterator<Item = InlineKeyboardButton>,
) -> Vec<InlineKeyboardButton> {
    buttons.into_iter().collect()
}

/// Build an inline keyboard markup matching Go `api.NewInlineKeyboardMarkup`.
pub fn build_inline_keyboard_markup(
    rows: impl IntoIterator<Item = impl IntoIterator<Item = InlineKeyboardButton>>,
) -> InlineKeyboardMarkup {
    InlineKeyboardMarkup::from(rows)
}

/// Build the one-button settings keyboard used by Go private settings entrypoints.
pub fn build_private_settings_keyboard(url: impl Into<String>) -> InlineKeyboardMarkup {
    build_inline_keyboard_markup([build_inline_keyboard_row([
        build_inline_keyboard_button_web_app(SETTINGS_BUTTON_TEXT, url),
    ])])
}

/// Build an outbound `deleteMessage` method.
pub fn build_delete_message_method(
    req: &DeleteMessageRequest,
) -> Result<DeleteMessage, OutboundBuildError> {
    if req.message_id == 0 {
        return Err(OutboundBuildError::MessageIdRequired);
    }
    Ok(DeleteMessage::new(req.chat_id, req.message_id))
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

/// Build the Go-equivalent fingerprint for one outbound text message part.
pub fn fingerprint_text_message_part(chat_id: i64, part: &str) -> MessageFingerprint {
    message_fingerprint(chat_id, MESSAGE_TYPE_TEXT, hash_content(part))
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

fn chat_action_from_go(action: &str) -> Result<ChatAction, OutboundBuildError> {
    let action = action.trim();
    if action.is_empty() {
        return Err(OutboundBuildError::ChatActionRequired);
    }

    match action {
        "choose_sticker" => Ok(ChatAction::ChooseSticker),
        "find_location" => Ok(ChatAction::FindLocation),
        "record_video" => Ok(ChatAction::RecordVideo),
        "record_voice" => Ok(ChatAction::RecordVoice),
        "record_video_note" => Ok(ChatAction::RecordVideoNote),
        "typing" => Ok(ChatAction::Typing),
        "upload_document" => Ok(ChatAction::UploadDocument),
        "upload_photo" => Ok(ChatAction::UploadPhoto),
        "upload_video" => Ok(ChatAction::UploadVideo),
        "upload_video_note" => Ok(ChatAction::UploadVideoNote),
        "upload_voice" => Ok(ChatAction::UploadVoice),
        other => Err(OutboundBuildError::UnsupportedChatAction(other.to_owned())),
    }
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
    /// Build the Go `api.StickerConfig` JSON payload used by queue persistence.
    pub fn to_persistence_payload(
        &self,
    ) -> Result<DispatcherPersistencePayload, serde_json::Error> {
        let mut value = go_base_file_config_json(
            self.chat_id,
            self.message_thread_id,
            self.disable_notification,
            self.reply_parameters,
            json!(self.file_id),
        );
        if let Value::Object(fields) = &mut value {
            fields.insert("Emoji".to_owned(), json!(""));
        }
        go_persistence_payload("*api.StickerConfig", value)
    }

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
    /// Build the Go `api.PhotoConfig` JSON payload used by queue persistence.
    pub fn to_persistence_payload(
        &self,
    ) -> Result<DispatcherPersistencePayload, serde_json::Error> {
        let mut value = go_base_file_config_json(
            self.chat_id,
            self.message_thread_id,
            self.disable_notification,
            self.reply_parameters,
            self.photo.go_file_value(),
        );
        if let Value::Object(fields) = &mut value {
            fields.insert("HasSpoiler".to_owned(), json!(self.has_spoiler));
            fields.insert("Thumb".to_owned(), Value::Null);
            fields.insert("Caption".to_owned(), json!(self.caption));
            fields.insert("ParseMode".to_owned(), json!(self.render_as));
            fields.insert("CaptionEntities".to_owned(), Value::Null);
            fields.insert("ShowCaptionAboveMedia".to_owned(), json!(false));
        }
        go_persistence_payload("*api.PhotoConfig", value)
    }

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
    fn go_file_value(&self) -> Value {
        match self {
            Self::FileId(file_id) | Self::Url(file_id) => json!(file_id),
            Self::Bytes { file_name, bytes } => json!({
                "Name": file_name,
                "Bytes": BASE64_STANDARD.encode(bytes),
            }),
        }
    }

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
    /// Build the Go `api.AudioConfig` JSON payload used by queue persistence.
    pub fn to_persistence_payload(
        &self,
    ) -> Result<DispatcherPersistencePayload, serde_json::Error> {
        let mut value = go_base_file_config_json(
            self.chat_id,
            self.message_thread_id,
            self.disable_notification,
            self.reply_parameters,
            self.audio.go_file_value(),
        );
        if let Value::Object(fields) = &mut value {
            fields.insert("Thumb".to_owned(), Value::Null);
            fields.insert("Caption".to_owned(), json!(self.caption));
            fields.insert("ParseMode".to_owned(), json!(self.render_as));
            fields.insert("CaptionEntities".to_owned(), Value::Null);
            fields.insert("Duration".to_owned(), json!(0));
            fields.insert("Performer".to_owned(), json!(""));
            fields.insert("Title".to_owned(), json!(""));
        }
        go_persistence_payload("*api.AudioConfig", value)
    }

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
    fn go_file_value(&self) -> Value {
        match self {
            Self::FileId(file_id) | Self::Url(file_id) => json!(file_id),
            Self::Bytes { file_name, bytes } => json!({
                "Name": file_name,
                "Bytes": BASE64_STANDARD.encode(bytes),
            }),
        }
    }

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
    /// Build the Go `api.MediaGroupConfig` JSON payload used by queue persistence.
    pub fn to_persistence_payload(
        &self,
    ) -> Result<DispatcherPersistencePayload, serde_json::Error> {
        let mut fields = go_base_chat_config_fields(
            self.chat_id,
            self.message_thread_id,
            self.disable_notification,
            self.reply_parameters,
        );
        fields.insert(
            "Media".to_owned(),
            Value::Array(
                self.items
                    .iter()
                    .map(MediaGroupPhotoItem::go_input_media_photo_value)
                    .collect(),
            ),
        );
        go_persistence_payload("*api.MediaGroupConfig", Value::Object(fields))
    }

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
    fn go_input_media_photo_value(&self) -> Value {
        let mut fields = Map::new();
        fields.insert("type".to_owned(), json!("photo"));
        fields.insert("media".to_owned(), self.photo.go_file_value());
        if !self.caption.is_empty() {
            fields.insert("caption".to_owned(), json!(self.caption));
        }
        if !self.render_as.is_empty() {
            fields.insert("parse_mode".to_owned(), json!(self.render_as));
        }
        if self.has_spoiler {
            fields.insert("has_spoiler".to_owned(), json!(true));
        }
        Value::Object(fields)
    }

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
    /// Build the Go `api.EditMessageMediaConfig` JSON payload used by queue persistence.
    pub fn to_persistence_payload(
        &self,
    ) -> Result<DispatcherPersistencePayload, serde_json::Error> {
        let mut fields = go_base_edit_config_fields(self.chat_id, self.message_id);
        fields.insert("Media".to_owned(), self.media.go_input_media_photo_value());
        go_persistence_payload("*api.EditMessageMediaConfig", Value::Object(fields))
    }

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

fn go_persistence_payload(
    message_type: &'static str,
    value: Value,
) -> Result<DispatcherPersistencePayload, serde_json::Error> {
    let json = serde_json::to_string(&value)?;
    Ok(DispatcherPersistencePayload::new(
        message_type,
        escape_go_json_html(&json).into_bytes(),
    ))
}

fn go_base_file_config_json(
    chat_id: i64,
    message_thread_id: Option<i64>,
    disable_notification: bool,
    reply_parameters: Option<ReplyParametersPlan>,
    file: Value,
) -> Value {
    let mut fields = go_base_chat_config_fields(
        chat_id,
        message_thread_id,
        disable_notification,
        reply_parameters,
    );
    fields.insert("File".to_owned(), file);
    Value::Object(fields)
}

fn go_base_chat_config_fields(
    chat_id: i64,
    message_thread_id: Option<i64>,
    disable_notification: bool,
    reply_parameters: Option<ReplyParametersPlan>,
) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("ChatID".to_owned(), json!(chat_id));
    fields.insert("ChannelUsername".to_owned(), json!(""));
    fields.insert("SuperGroupUsername".to_owned(), json!(""));
    fields.insert("BusinessConnectionID".to_owned(), json!(""));
    fields.insert(
        "MessageThreadID".to_owned(),
        json!(message_thread_id.unwrap_or_default()),
    );
    fields.insert("DirectMessagesTopicID".to_owned(), json!(0));
    fields.insert("ProtectContent".to_owned(), json!(false));
    fields.insert("ReplyMarkup".to_owned(), Value::Null);
    fields.insert(
        "DisableNotification".to_owned(),
        json!(disable_notification),
    );
    fields.insert("AllowPaidBroadcast".to_owned(), json!(false));
    fields.insert("MessageEffectID".to_owned(), json!(""));
    fields.insert(
        "ReplyParameters".to_owned(),
        go_reply_parameters_value(reply_parameters),
    );
    fields.insert("SuggestedPostParameters".to_owned(), Value::Null);
    fields
}

fn go_base_edit_config_fields(chat_id: i64, message_id: i64) -> Map<String, Value> {
    let mut fields = Map::new();
    fields.insert("ChatID".to_owned(), json!(chat_id));
    fields.insert("ChannelUsername".to_owned(), json!(""));
    fields.insert("SuperGroupUsername".to_owned(), json!(""));
    fields.insert("MessageID".to_owned(), json!(message_id));
    fields.insert("BusinessConnectionID".to_owned(), json!(""));
    fields.insert("InlineMessageID".to_owned(), json!(""));
    fields.insert("ReplyMarkup".to_owned(), Value::Null);
    fields
}

fn go_reply_parameters_value(reply_parameters: Option<ReplyParametersPlan>) -> Value {
    let mut fields = Map::new();
    let Some(reply_parameters) = reply_parameters else {
        fields.insert("message_id".to_owned(), json!(0));
        return Value::Object(fields);
    };

    fields.insert("message_id".to_owned(), json!(reply_parameters.message_id));
    fields.insert("chat_id".to_owned(), json!(reply_parameters.chat_id));
    if reply_parameters.allow_sending_without_reply {
        fields.insert("allow_sending_without_reply".to_owned(), json!(true));
    }
    Value::Object(fields)
}

fn escape_go_json_html(json: &str) -> String {
    let mut escaped = String::with_capacity(json.len());
    for ch in json.chars() {
        match ch {
            '<' => escaped.push_str("\\u003c"),
            '>' => escaped.push_str("\\u003e"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            _ => escaped.push(ch),
        }
    }
    escaped
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
    use carapax::types::{ChatMember, ChatMemberAdministrator, ChatMemberCreator, User};
    use serde_json::{Value, json};

    use super::{
        AudioMessagePlan, AudioMessageRequest, AudioSource, CallbackAnswerRequest,
        ChatActionRequest, ChatRef, DeleteMessageRequest, DonationInvoiceLinkRequest,
        EditCaptionMessageRequest, EditMediaMessagePlan, EditMediaMessageRequest,
        EditReplyMarkupMessageRequest, EditTextMessageRequest, GuestHtmlAnswerRequest,
        GuestQueryAnswerRequest, InlineArticleRequest, InlineQueryAnswerRequest, MESSAGE_TYPE_TEXT,
        MediaGroupMessagePlan, MediaGroupMessageRequest, MediaGroupPhotoItem, MessageFingerprint,
        OutboundBuildError, PaymentPayloadKind, PhotoMessagePlan, PhotoMessageRequest, PhotoSource,
        ReplyMessageRef, ReplyParametersPlan, StickerMessagePlan, StickerMessageRequest,
        SubscriptionInvoiceLinkRequest, TextMessageRequest, allow_sending_without_reply,
        build_audio_message_method, build_audio_message_plan, build_callback_answer_method,
        build_cancel_star_subscription_method, build_chat_action_method,
        build_delete_message_method, build_donation_invoice_link_method,
        build_edit_caption_message_method, build_edit_media_message_method,
        build_edit_media_message_plan, build_edit_reply_markup_message_method,
        build_edit_text_message_method, build_get_chat_administrators_method,
        build_get_chat_member_method, build_guest_add_to_chat_markup,
        build_guest_html_answer_method, build_guest_query_answer_method,
        build_inline_keyboard_button_data, build_inline_keyboard_button_url,
        build_inline_keyboard_button_web_app, build_inline_keyboard_markup,
        build_inline_keyboard_row, build_inline_query_answer_method,
        build_inline_query_result_article, build_media_group_message_method,
        build_media_group_message_plan, build_photo_message_method, build_photo_message_plan,
        build_pre_checkout_ok_method, build_private_settings_keyboard,
        build_refund_star_payment_method, build_sticker_message_method, build_sticker_message_plan,
        build_subscription_invoice_link_method, build_text_message_method,
        build_text_message_methods, classify_payment_payload, donation_invoice_payload,
        fingerprint_audio_message_plan, fingerprint_photo_message_plan,
        fingerprint_sticker_message_plan, fingerprint_text_message_part, forum_thread_id,
        guest_add_to_chat_url, guest_dialog_fallback_html, guest_inline_description,
        guest_inline_result_id, guest_unsupported_feature_html, hash_content, message_target_chat,
        prepare_guest_html, subscription_invoice_payload, telegram_member_can_open_group_settings,
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

    fn persistence_payload_value(
        payload: &crate::DispatcherPersistencePayload,
    ) -> Result<Value, serde_json::Error> {
        serde_json::from_slice(&payload.message)
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
    fn fingerprint_text_part_hashes_split_part_like_go_message_config() {
        let fingerprint = fingerprint_text_message_part(42, "hello");

        assert_eq!(fingerprint.chat_id, 42);
        assert_eq!(fingerprint.message_type, MESSAGE_TYPE_TEXT);
        assert_eq!(fingerprint.content_hash, 0x9a71bb4c);
        assert_eq!(fingerprint.debounce_key, None);
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
    fn sticker_plan_persistence_payload_matches_go_sticker_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = StickerMessagePlan {
            chat_id: 42,
            file_id: "sticker-file".to_owned(),
            message_thread_id: Some(77),
            disable_notification: true,
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let payload = plan.to_persistence_payload()?;
        let value = persistence_payload_value(&payload)?;

        assert_eq!(payload.message_type, "*api.StickerConfig");
        assert_eq!(value["ChatID"], json!(42));
        assert_eq!(value["MessageThreadID"], json!(77));
        assert_eq!(value["DisableNotification"], json!(true));
        assert_eq!(value["File"], json!("sticker-file"));
        assert_eq!(value["ReplyParameters"]["message_id"], json!(9));
        assert_eq!(value["ReplyParameters"]["chat_id"], json!(42));
        assert_eq!(
            value["ReplyParameters"]["allow_sending_without_reply"],
            json!(true)
        );

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
    fn photo_plan_persistence_payload_matches_go_photo_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = PhotoMessagePlan {
            chat_id: 42,
            photo: PhotoSource::Url("https://example.test/image.png".to_owned()),
            message_thread_id: Some(77),
            disable_notification: true,
            caption: "<b>caption</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            has_spoiler: true,
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let payload = plan.to_persistence_payload()?;
        let value = persistence_payload_value(&payload)?;

        assert_eq!(payload.message_type, "*api.PhotoConfig");
        assert_eq!(value["ChatID"], json!(42));
        assert_eq!(value["MessageThreadID"], json!(77));
        assert_eq!(value["DisableNotification"], json!(true));
        assert_eq!(value["File"], json!("https://example.test/image.png"));
        assert_eq!(value["Caption"], json!("<b>caption</b>"));
        assert_eq!(value["ParseMode"], json!("HTML"));
        assert_eq!(value["HasSpoiler"], json!(true));
        assert!(
            String::from_utf8(payload.message)?
                .contains(r#""Caption":"\u003cb\u003ecaption\u003c/b\u003e""#)
        );

        Ok(())
    }

    #[test]
    fn photo_plan_persistence_payload_encodes_uploaded_bytes_like_go_file_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = PhotoMessagePlan {
            chat_id: 42,
            photo: PhotoSource::Bytes {
                file_name: "image.png".to_owned(),
                bytes: vec![1, 2, 3],
            },
            message_thread_id: None,
            disable_notification: false,
            caption: String::new(),
            render_as: String::new(),
            has_spoiler: false,
            reply_parameters: None,
        };

        let payload = plan.to_persistence_payload()?;
        let value = persistence_payload_value(&payload)?;

        assert_eq!(value["File"]["Name"], json!("image.png"));
        assert_eq!(value["File"]["Bytes"], json!("AQID"));

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
    fn media_group_plan_persistence_payload_matches_go_media_group_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = MediaGroupMessagePlan {
            chat_id: 42,
            message_thread_id: Some(77),
            disable_notification: true,
            items: vec![MediaGroupPhotoItem {
                photo: PhotoSource::FileId("first-photo".to_owned()),
                caption: "<b>caption</b>".to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                has_spoiler: true,
            }],
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let payload = plan.to_persistence_payload()?;
        let value = persistence_payload_value(&payload)?;

        assert_eq!(payload.message_type, "*api.MediaGroupConfig");
        assert_eq!(value["ChatID"], json!(42));
        assert_eq!(value["MessageThreadID"], json!(77));
        assert_eq!(value["DisableNotification"], json!(true));
        assert_eq!(value["Media"][0]["type"], json!("photo"));
        assert_eq!(value["Media"][0]["media"], json!("first-photo"));
        assert_eq!(value["Media"][0]["caption"], json!("<b>caption</b>"));
        assert_eq!(value["Media"][0]["parse_mode"], json!("HTML"));
        assert_eq!(value["Media"][0]["has_spoiler"], json!(true));

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
    fn audio_plan_persistence_payload_matches_go_audio_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = AudioMessagePlan {
            chat_id: 42,
            audio: AudioSource::FileId("song-file-id".to_owned()),
            message_thread_id: Some(77),
            disable_notification: true,
            caption: "song".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_parameters: Some(ReplyParametersPlan {
                message_id: 9,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        };

        let payload = plan.to_persistence_payload()?;
        let value = persistence_payload_value(&payload)?;

        assert_eq!(payload.message_type, "*api.AudioConfig");
        assert_eq!(value["ChatID"], json!(42));
        assert_eq!(value["MessageThreadID"], json!(77));
        assert_eq!(value["DisableNotification"], json!(true));
        assert_eq!(value["File"], json!("song-file-id"));
        assert_eq!(value["Caption"], json!("song"));
        assert_eq!(value["ParseMode"], json!("HTML"));

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
    fn edit_media_plan_persistence_payload_matches_go_edit_message_media_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let plan = EditMediaMessagePlan {
            chat_id: 42,
            message_id: 7,
            media: MediaGroupPhotoItem {
                photo: PhotoSource::FileId("first-photo".to_owned()),
                caption: "<b>caption</b>".to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                has_spoiler: true,
            },
        };

        let payload = plan.to_persistence_payload()?;
        let value = persistence_payload_value(&payload)?;

        assert_eq!(payload.message_type, "*api.EditMessageMediaConfig");
        assert_eq!(value["ChatID"], json!(42));
        assert_eq!(value["MessageID"], json!(7));
        assert_eq!(value["Media"]["type"], json!("photo"));
        assert_eq!(value["Media"]["media"], json!("first-photo"));
        assert_eq!(value["Media"]["caption"], json!("<b>caption</b>"));
        assert_eq!(value["Media"]["parse_mode"], json!("HTML"));
        assert_eq!(value["Media"]["has_spoiler"], json!(true));

        Ok(())
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

    #[test]
    fn build_edit_caption_message_method_matches_go_caption_transfer_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let req = EditCaptionMessageRequest {
            chat: private_chat(42),
            message_id: 88,
            caption: "<b>kept caption</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };

        let method = build_edit_caption_message_method(&req)?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(88));
        assert_eq!(payload["caption"], json!("<b>kept caption</b>"));
        assert_eq!(payload["parse_mode"], json!("HTML"));

        Ok(())
    }

    #[test]
    fn build_edit_reply_markup_message_method_keeps_empty_and_nonempty_markup()
    -> Result<(), Box<dyn std::error::Error>> {
        let empty = build_edit_reply_markup_message_method(&EditReplyMarkupMessageRequest {
            chat: private_chat(42),
            message_id: 77,
            reply_markup: InlineKeyboardMarkup::default(),
        })?;
        let empty_payload = serde_json::to_value(empty)?;
        assert_eq!(empty_payload["chat_id"], json!(42));
        assert_eq!(empty_payload["message_id"], json!(77));
        assert_eq!(empty_payload["reply_markup"]["inline_keyboard"], json!([]),);

        let markup =
            InlineKeyboardMarkup::from([[InlineKeyboardButton::for_callback_data("ok", "ok")]]);
        let method = build_edit_reply_markup_message_method(&EditReplyMarkupMessageRequest {
            chat: private_chat(43),
            message_id: 78,
            reply_markup: markup,
        })?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(43));
        assert_eq!(payload["message_id"], json!(78));
        assert_eq!(
            payload["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("ok"),
        );

        Ok(())
    }

    #[test]
    fn build_delete_message_method_builds_carapax_method() -> Result<(), Box<dyn std::error::Error>>
    {
        let method = build_delete_message_method(&DeleteMessageRequest {
            chat_id: 42,
            message_id: 77,
        })?;
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(42));
        assert_eq!(payload["message_id"], json!(77));

        Ok(())
    }

    #[test]
    fn build_chat_action_method_matches_go_request_validation_and_thread_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            build_chat_action_method(&ChatActionRequest {
                chat: private_chat(0),
                message_thread_id: 0,
                action: "typing".to_owned(),
            })
            .err(),
            Some(OutboundBuildError::ChatNotSet),
        );
        assert_eq!(
            build_chat_action_method(&ChatActionRequest {
                chat: private_chat(42),
                message_thread_id: 0,
                action: " ".to_owned(),
            })
            .err(),
            Some(OutboundBuildError::ChatActionRequired),
        );

        let typing = build_chat_action_method(&ChatActionRequest {
            chat: private_chat(42),
            message_thread_id: 5,
            action: "typing".to_owned(),
        })?;
        let typing_payload = serde_json::to_value(typing)?;
        assert_eq!(typing_payload["chat_id"], json!(42));
        assert_eq!(typing_payload["message_thread_id"], json!(5));
        assert_eq!(typing_payload["action"], json!("typing"));

        let upload_voice = build_chat_action_method(&ChatActionRequest {
            chat: private_chat(43),
            message_thread_id: 0,
            action: "upload_voice".to_owned(),
        })?;
        let upload_voice_payload = serde_json::to_value(upload_voice)?;
        assert_eq!(upload_voice_payload["chat_id"], json!(43));
        assert!(upload_voice_payload.get("message_thread_id").is_none());
        assert_eq!(upload_voice_payload["action"], json!("upload_voice"));

        assert_eq!(
            build_chat_action_method(&ChatActionRequest {
                chat: private_chat(44),
                message_thread_id: 0,
                action: "non_telegram_action".to_owned(),
            })
            .err(),
            Some(OutboundBuildError::UnsupportedChatAction(
                "non_telegram_action".to_owned()
            )),
        );

        Ok(())
    }

    #[test]
    fn build_get_chat_member_method_matches_go_permission_probe_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let method = build_get_chat_member_method(-10042, 42);
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(payload["user_id"], json!(42));
        Ok(())
    }

    #[test]
    fn build_get_chat_administrators_method_matches_go_admin_sync_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let method = build_get_chat_administrators_method(-10042);
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["chat_id"], json!(-10042));
        assert!(payload.get("return_bots").is_none());
        Ok(())
    }

    #[test]
    fn telegram_member_permission_matches_go_group_settings_rule() {
        let creator = ChatMember::Creator(ChatMemberCreator::new(User::new(42, "Ada", false)));
        let promoting_admin = ChatMember::Administrator(
            ChatMemberAdministrator::new(User::new(43, "Grace", false))
                .with_can_promote_members(true),
        );
        let non_promoting_admin = ChatMember::Administrator(
            ChatMemberAdministrator::new(User::new(44, "Alan", false))
                .with_can_promote_members(false),
        );
        let member = ChatMember::Member {
            user: User::new(45, "Linus", false),
            tag: None,
            until_date: None,
        };

        assert!(telegram_member_can_open_group_settings(&creator));
        assert!(telegram_member_can_open_group_settings(&promoting_admin));
        assert!(!telegram_member_can_open_group_settings(
            &non_promoting_admin
        ));
        assert!(!telegram_member_can_open_group_settings(&member));
    }

    #[test]
    fn build_callback_answer_method_matches_go_callback_params()
    -> Result<(), Box<dyn std::error::Error>> {
        let ack = build_callback_answer_method(&CallbackAnswerRequest {
            callback_query_id: "query-1".to_owned(),
            text: String::new(),
            show_alert: false,
            url: String::new(),
            cache_time: 0,
        });
        let ack_payload = serde_json::to_value(ack)?;
        assert_eq!(ack_payload["callback_query_id"], json!("query-1"));
        assert!(ack_payload.get("text").is_none());
        assert!(ack_payload.get("show_alert").is_none());
        assert!(ack_payload.get("url").is_none());
        assert!(ack_payload.get("cache_time").is_none());

        let alert = build_callback_answer_method(&CallbackAnswerRequest {
            callback_query_id: "query-2".to_owned(),
            text: "Генерация не найдена".to_owned(),
            show_alert: true,
            url: "https://example.invalid/plotva".to_owned(),
            cache_time: 10,
        });
        let alert_payload = serde_json::to_value(alert)?;
        assert_eq!(alert_payload["callback_query_id"], json!("query-2"));
        assert_eq!(alert_payload["text"], json!("Генерация не найдена"));
        assert_eq!(alert_payload["show_alert"], json!(true));
        assert_eq!(
            alert_payload["url"],
            json!("https://example.invalid/plotva")
        );
        assert_eq!(alert_payload["cache_time"], json!(10));

        Ok(())
    }

    #[test]
    fn payment_payload_helpers_match_go_prefix_and_payload_rules() {
        assert_eq!(subscription_invoice_payload(42), "subscription_42");
        assert_eq!(donation_invoice_payload(42, 600), "donation_42_600");

        assert_eq!(
            classify_payment_payload("subscription_42"),
            PaymentPayloadKind::Subscription
        );
        assert_eq!(
            classify_payment_payload("donation_42_100"),
            PaymentPayloadKind::Donation
        );
        assert_eq!(
            classify_payment_payload("donation"),
            PaymentPayloadKind::Donation
        );
        assert_eq!(
            classify_payment_payload("other"),
            PaymentPayloadKind::Unknown
        );
    }

    #[test]
    fn build_payment_methods_match_go_stars_payloads() -> Result<(), Box<dyn std::error::Error>> {
        let subscription =
            build_subscription_invoice_link_method(&SubscriptionInvoiceLinkRequest {
                user_id: 42,
                user_name: "Alice".to_owned(),
                amount_stars: 0,
            });
        let subscription_payload = serde_json::to_value(subscription)?;
        assert_eq!(
            subscription_payload["title"],
            json!("VIP Подписка на 30 дней")
        );
        assert_eq!(subscription_payload["payload"], json!("subscription_42"));
        assert_eq!(subscription_payload["provider_token"], json!(""));
        assert_eq!(subscription_payload["currency"], json!("XTR"));
        assert_eq!(
            subscription_payload["subscription_period"],
            json!(2_592_000)
        );
        assert_eq!(
            subscription_payload["prices"][0]["label"],
            json!("VIP Подписка на 30 дней")
        );
        assert_eq!(subscription_payload["prices"][0]["amount"], json!(300));

        let wavecut_subscription =
            build_subscription_invoice_link_method(&SubscriptionInvoiceLinkRequest {
                user_id: 1717359759,
                user_name: "WaveCut".to_owned(),
                amount_stars: 300,
            });
        let wavecut_payload = serde_json::to_value(wavecut_subscription)?;
        assert_eq!(wavecut_payload["prices"][0]["amount"], json!(1));

        let donation = build_donation_invoice_link_method(&DonationInvoiceLinkRequest {
            user_id: 42,
            amount_stars: 600,
        });
        let donation_payload = serde_json::to_value(donation)?;
        assert_eq!(donation_payload["title"], json!("Донат разработчику"));
        assert_eq!(donation_payload["payload"], json!("donation_42_600"));
        assert_eq!(donation_payload["provider_token"], json!(""));
        assert_eq!(donation_payload["currency"], json!("XTR"));
        assert!(donation_payload.get("subscription_period").is_none());
        assert_eq!(donation_payload["prices"][0]["label"], json!("Донат"));
        assert_eq!(donation_payload["prices"][0]["amount"], json!(600));

        let pre_checkout = build_pre_checkout_ok_method("pre-checkout-id");
        let pre_checkout_payload = serde_json::to_value(pre_checkout)?;
        assert_eq!(
            pre_checkout_payload["pre_checkout_query_id"],
            json!("pre-checkout-id")
        );
        assert_eq!(pre_checkout_payload["ok"], json!(true));
        assert!(pre_checkout_payload.get("error_message").is_none());

        let refund = build_refund_star_payment_method(42, "charge-id");
        let refund_payload = serde_json::to_value(refund)?;
        assert_eq!(refund_payload["user_id"], json!(42));
        assert_eq!(
            refund_payload["telegram_payment_charge_id"],
            json!("charge-id")
        );

        let cancel = build_cancel_star_subscription_method(42, "charge-id");
        let cancel_payload = serde_json::to_value(cancel)?;
        assert_eq!(cancel_payload["user_id"], json!(42));
        assert_eq!(
            cancel_payload["telegram_payment_charge_id"],
            json!("charge-id")
        );
        assert_eq!(cancel_payload["is_canceled"], json!(true));

        Ok(())
    }

    #[test]
    fn build_inline_query_answer_method_matches_go_inline_config()
    -> Result<(), Box<dyn std::error::Error>> {
        let article = build_inline_query_result_article(&InlineArticleRequest {
            id: "inline-id".to_owned(),
            title: "Шевелись, Плотва!".to_owned(),
            message_text: "raw query".to_owned(),
            render_as: String::new(),
            description: String::new(),
            reply_markup: None,
        })?;
        let article_payload = serde_json::to_value(article.clone())?;
        assert_eq!(article_payload["type"], json!("article"));
        assert_eq!(article_payload["id"], json!("inline-id"));
        assert_eq!(article_payload["title"], json!("Шевелись, Плотва!"));
        assert_eq!(
            article_payload["input_message_content"]["message_text"],
            json!("raw query")
        );
        assert!(
            article_payload["input_message_content"]
                .get("parse_mode")
                .is_none()
        );
        assert!(article_payload.get("description").is_none());
        assert!(article_payload.get("reply_markup").is_none());

        let empty_options = build_inline_query_answer_method(&InlineQueryAnswerRequest {
            inline_query_id: "inline-empty".to_owned(),
            results: vec![article.clone()],
            cache_time: 0,
            is_personal: false,
            next_offset: String::new(),
        });
        let empty_payload = serde_json::to_value(empty_options)?;
        assert_eq!(empty_payload["inline_query_id"], json!("inline-empty"));
        assert!(empty_payload.get("cache_time").is_none());
        assert!(empty_payload.get("is_personal").is_none());
        assert!(empty_payload.get("next_offset").is_none());

        let method = build_inline_query_answer_method(&InlineQueryAnswerRequest {
            inline_query_id: "inline-id".to_owned(),
            results: vec![article],
            cache_time: 1,
            is_personal: true,
            next_offset: String::new(),
        });
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["inline_query_id"], json!("inline-id"));
        assert_eq!(payload["cache_time"], json!(1));
        assert_eq!(payload["is_personal"], json!(true));
        assert!(payload.get("next_offset").is_none());
        assert!(payload.get("button").is_none());
        assert_eq!(payload["results"][0]["type"], json!("article"));
        assert_eq!(
            payload["results"][0]["input_message_content"]["message_text"],
            json!("raw query")
        );

        Ok(())
    }

    #[test]
    fn build_guest_query_answer_method_matches_go_guest_article()
    -> Result<(), Box<dyn std::error::Error>> {
        let markup =
            InlineKeyboardMarkup::from([[InlineKeyboardButton::for_callback_data("ok", "ok")]]);
        let article = build_inline_query_result_article(&InlineArticleRequest {
            id: "guest-42".to_owned(),
            title: "Добавьте Плотву в чат".to_owned(),
            message_text: "<b>Готово</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            description: "Готово".to_owned(),
            reply_markup: Some(markup),
        })?;
        let method = build_guest_query_answer_method(&GuestQueryAnswerRequest {
            guest_query_id: "guest-query".to_owned(),
            result: article,
        });
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["guest_query_id"], json!("guest-query"));
        assert_eq!(payload["result"]["type"], json!("article"));
        assert_eq!(payload["result"]["id"], json!("guest-42"));
        assert_eq!(payload["result"]["title"], json!("Добавьте Плотву в чат"));
        assert_eq!(payload["result"]["description"], json!("Готово"));
        assert_eq!(
            payload["result"]["input_message_content"]["message_text"],
            json!("<b>Готово</b>")
        );
        assert_eq!(
            payload["result"]["input_message_content"]["parse_mode"],
            json!("HTML")
        );
        assert_eq!(
            payload["result"]["reply_markup"]["inline_keyboard"][0][0]["callback_data"],
            json!("ok")
        );

        Ok(())
    }

    #[test]
    fn guest_html_preparation_matches_go_sanitizer_and_truncation() {
        assert_eq!(
            prepare_guest_html("<script>bad()</script><b>Готово</b>"),
            "<b>Готово</b>"
        );
        assert_eq!(prepare_guest_html("<b>   </b>"), "");

        let long = format!("<b>{}</b>", "ж".repeat(4100));
        let prepared = prepare_guest_html(&long);
        assert_eq!(prepared.chars().count(), 3903);
        assert!(prepared.starts_with(&"ж".repeat(3900)));
        assert!(prepared.ends_with("..."));
        assert!(!prepared.contains("<b>"));
    }

    #[test]
    fn guest_inline_description_and_ids_match_go_helpers() {
        assert_eq!(
            guest_inline_description("<b>Готово</b> &amp; спокойно"),
            "Готово & спокойно"
        );
        let long_description = format!("{}{}", "а".repeat(120), "б");
        assert_eq!(
            guest_inline_description(&long_description),
            format!("{}...", "а".repeat(117))
        );

        assert_eq!(guest_inline_result_id(None, 77), "guest");
        assert_eq!(guest_inline_result_id(Some("   "), 77), "guest-message-77");
        assert_eq!(
            guest_inline_result_id(Some("short-query"), 77),
            "guest-short-query"
        );
        assert_eq!(
            guest_inline_result_id(
                Some(
                    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz"
                ),
                77
            ),
            "guest-f2090afe4177d6f288072a47"
        );
    }

    #[test]
    fn guest_add_to_chat_helpers_match_go_text_and_markup() -> Result<(), Box<dyn std::error::Error>>
    {
        assert_eq!(
            guest_add_to_chat_url("@PlotvoBot"),
            "https://t.me/PlotvoBot?startgroup=guest"
        );
        assert_eq!(
            guest_add_to_chat_url("  "),
            "https://t.me/PlotvoBot?startgroup=guest"
        );

        let unsupported = guest_unsupported_feature_html("@PlotvoBot");
        assert!(unsupported.contains("Некоторые функции Плотвы работают только в чате"));
        assert!(unsupported.contains("https://t.me/PlotvoBot?startgroup=guest"));

        let fallback = guest_dialog_fallback_html("PlotvoBot");
        assert!(fallback.contains("Не успела ответить в гостевом режиме."));
        assert!(fallback.contains("https://t.me/PlotvoBot?startgroup=guest"));

        let markup = build_guest_add_to_chat_markup("@PlotvoBot");
        let payload = serde_json::to_value(markup)?;
        assert_eq!(
            payload["inline_keyboard"][0][0]["text"],
            json!("Добавить Плотву в чат")
        );
        assert_eq!(
            payload["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvoBot?startgroup=guest")
        );

        Ok(())
    }

    #[test]
    fn build_guest_html_answer_method_matches_go_answer_guest_html()
    -> Result<(), Box<dyn std::error::Error>> {
        let method = build_guest_html_answer_method(&GuestHtmlAnswerRequest {
            guest_query_id: "guest-query".to_owned(),
            message_id: 55,
            title: "Плотва отвечает".to_owned(),
            html_text: "<script>bad()</script><b>Готово</b>".to_owned(),
            bot_username: "@PlotvoBot".to_owned(),
            reply_markup: Some(build_guest_add_to_chat_markup("@PlotvoBot")),
        })
        .expect("non-empty guest query id builds a method");
        let payload = serde_json::to_value(method)?;

        assert_eq!(payload["guest_query_id"], json!("guest-query"));
        assert_eq!(payload["result"]["id"], json!("guest-guest-query"));
        assert_eq!(payload["result"]["title"], json!("Плотва отвечает"));
        assert_eq!(payload["result"]["description"], json!("Готово"));
        assert_eq!(
            payload["result"]["input_message_content"]["message_text"],
            json!("<b>Готово</b>")
        );
        assert_eq!(
            payload["result"]["input_message_content"]["parse_mode"],
            json!("HTML")
        );
        assert_eq!(
            payload["result"]["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvoBot?startgroup=guest")
        );

        let fallback = build_guest_html_answer_method(&GuestHtmlAnswerRequest {
            guest_query_id: "fallback-query".to_owned(),
            message_id: 56,
            title: "Плотва отвечает".to_owned(),
            html_text: "<b>   </b>".to_owned(),
            bot_username: "PlotvoBot".to_owned(),
            reply_markup: None,
        })
        .expect("fallback still builds a method");
        let fallback_payload = serde_json::to_value(fallback)?;
        assert!(
            fallback_payload["result"]["input_message_content"]["message_text"]
                .as_str()
                .unwrap_or_default()
                .contains("Не успела ответить в гостевом режиме.")
        );
        assert!(fallback_payload["result"].get("reply_markup").is_none());

        assert!(
            build_guest_html_answer_method(&GuestHtmlAnswerRequest {
                guest_query_id: "  ".to_owned(),
                message_id: 57,
                title: "Плотва отвечает".to_owned(),
                html_text: "Готово".to_owned(),
                bot_username: "PlotvoBot".to_owned(),
                reply_markup: None,
            })
            .is_none()
        );

        Ok(())
    }

    #[test]
    fn build_inline_keyboard_helpers_match_go_constructor_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let delete_button = build_inline_keyboard_button_data("🗑 Удалить текст", "dl_i:22:-100");
        let url_button =
            build_inline_keyboard_button_url("📖 Помощь", "https://t.me/PlotvoBot?start=help");
        let row = build_inline_keyboard_row([delete_button.clone(), url_button.clone()]);
        let markup = build_inline_keyboard_markup([row]);
        let payload = serde_json::to_value(markup)?;

        assert_eq!(
            payload["inline_keyboard"][0][0]["text"],
            json!("🗑 Удалить текст")
        );
        assert_eq!(
            payload["inline_keyboard"][0][0]["callback_data"],
            json!("dl_i:22:-100")
        );
        assert_eq!(payload["inline_keyboard"][0][1]["text"], json!("📖 Помощь"));
        assert_eq!(
            payload["inline_keyboard"][0][1]["url"],
            json!("https://t.me/PlotvoBot?start=help")
        );
        assert!(payload["inline_keyboard"][0][0].get("url").is_none());
        assert!(
            payload["inline_keyboard"][0][1]
                .get("callback_data")
                .is_none()
        );

        Ok(())
    }

    #[test]
    fn build_inline_keyboard_web_app_button_matches_go_settings_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let button = build_inline_keyboard_button_web_app(
            "⚙️ Настройки",
            "https://plotva.example/settings/?chat_id=42&signature=780e28cf",
        );
        let payload = serde_json::to_value(button)?;

        assert_eq!(payload["text"], json!("⚙️ Настройки"));
        assert_eq!(
            payload["web_app"]["url"],
            json!("https://plotva.example/settings/?chat_id=42&signature=780e28cf")
        );
        assert!(payload.get("url").is_none());
        assert!(payload.get("callback_data").is_none());

        Ok(())
    }

    #[test]
    fn build_private_settings_keyboard_matches_go_single_web_app_row()
    -> Result<(), Box<dyn std::error::Error>> {
        let markup = build_private_settings_keyboard(
            "https://plotva.example/settings/index.html?signature=780e28cf",
        );
        let payload = serde_json::to_value(markup)?;

        let rows = payload["inline_keyboard"]
            .as_array()
            .ok_or("inline_keyboard must be an array")?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            payload["inline_keyboard"][0][0]["text"],
            json!("⚙️ Настройки")
        );
        assert_eq!(
            payload["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/index.html?signature=780e28cf")
        );

        Ok(())
    }

    #[test]
    fn build_inline_keyboard_helpers_preserve_empty_and_copied_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        let empty_payload = serde_json::to_value(build_inline_keyboard_markup(Vec::<
            Vec<InlineKeyboardButton>,
        >::new()))?;
        assert_eq!(empty_payload["inline_keyboard"], json!([]));

        let source = [build_inline_keyboard_button_data("Да", "confirm")];
        let copied = build_inline_keyboard_row(source.iter().cloned());
        let markup = build_inline_keyboard_markup([copied]);
        let payload = serde_json::to_value(markup)?;

        assert_eq!(payload["inline_keyboard"][0][0]["text"], json!("Да"));
        assert_eq!(
            payload["inline_keyboard"][0][0]["callback_data"],
            json!("confirm")
        );

        Ok(())
    }

    #[test]
    fn build_delete_message_method_requires_message_id() {
        assert_eq!(
            build_delete_message_method(&DeleteMessageRequest {
                chat_id: 42,
                message_id: 0,
            })
            .err(),
            Some(OutboundBuildError::MessageIdRequired)
        );
    }
}
