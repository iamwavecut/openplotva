//! Telegram Bot API boundary for OpenPlotva.

mod callback;
mod dedup;
mod dispatcher;
mod html;
mod outbound;
mod pending_ops;
mod persistence;
mod rate_limit;
mod transport;
mod update_startup;

pub use callback::{
    CallbackActionData, CallbackActionParse, CallbackHandlerKind, CallbackQueryRoute,
    DELETE_DRAWING_ACTION_CLOSE, DELETE_DRAWING_ACTION_CONFIRM,
    DELETE_DRAWING_ACTION_FRAME_CONFIRM, DELETE_DRAWING_ACTION_FRAME_PICK,
    DELETE_DRAWING_ACTION_INIT, DELETE_LYRICS_ACTION_CLOSE, DELETE_LYRICS_ACTION_CONFIRM,
    DELETE_LYRICS_ACTION_INIT, build_checkin_theme_selection_keyboard,
    build_delete_drawing_confirm_keyboard, build_delete_drawing_frame_confirm_keyboard,
    build_delete_drawing_frame_picker_keyboard, build_delete_drawing_initial_keyboard,
    build_lyrics_delete_confirm_keyboard, build_lyrics_delete_keyboard,
    callback_handler_for_action, callback_query_ack_method, callback_query_ack_request,
    callback_query_route, checkin_theme_callback_init, checkin_theme_callback_theme,
    checkin_theme_selection_ack_method, checkin_theme_selection_alert,
    delete_drawing_callback_data, delete_lyrics_callback_data, parse_callback_action,
    parse_callback_i64, settings_callback_ack_method,
};
pub use dedup::{DEFAULT_DEBOUNCE_CACHE_SIZE, DEFAULT_DEBOUNCE_WINDOW, Debouncer, DebouncerConfig};
pub use dispatcher::{
    DEFAULT_DISPATCHER_CLEANUP_INTERVAL, DispatcherConfig, DispatcherDrain, DispatcherMessage,
    DispatcherPersistencePayload, DispatcherQueue, DispatcherQueuedMessage,
    DispatcherRestoredMessage, DispatcherRuntimeConfig, DispatcherSendStatus, DispatcherStats,
    DispatcherWorkItem, DispatcherWorkerLoopOutcome, DispatcherWorkerOutcome, EnqueueOutcome,
    QueueSnapshot, RegularDequeueOutcome, run_limiter_cleanup_until,
};
pub use html::{
    TELEGRAM_PARSE_MODE_HTML, clean_unicode_non_printables, decode_html_entities,
    ensure_telegram_safe_text, escape_telegram_html_text, extract_visible_text,
    is_valid_telegram_html, sanitize_telegram_html, split_telegram_text, strip_telegram_html,
};
pub use outbound::{
    AudioMessagePlan, AudioMessageRequest, AudioSource, CallbackAnswerRequest, ChatActionRequest,
    ChatRef, DEFAULT_GUEST_BOT_USERNAME, DONATION_DESCRIPTION, DONATION_TITLE,
    DeleteMessageRequest, DonationInvoiceLinkRequest, EditCaptionMessageRequest,
    EditMediaMessagePlan, EditMediaMessageRequest, EditReplyMarkupMessageRequest,
    EditTextMessageRequest, GUEST_ADD_TO_CHAT_PAYLOAD, GUEST_INLINE_TEXT_LIMIT,
    GUEST_INLINE_TRUNCATE_LIMIT, GuestHtmlAnswerRequest, GuestQueryAnswerRequest,
    InlineArticleRequest, InlineQueryAnswerRequest, MAX_DONATION_STARS, MESSAGE_TYPE_TEXT,
    MIN_DONATION_STARS, MediaGroupMessagePlan, MediaGroupMessageRequest, MediaGroupPhotoItem,
    MessageFingerprint, OutboundBuildError, PaymentPayloadKind, PhotoMessagePlan,
    PhotoMessageRequest, PhotoSource, ReplyMessageRef, ReplyParametersPlan, SETTINGS_BUTTON_TEXT,
    SUBSCRIPTION_DURATION_DAYS, SUBSCRIPTION_PERIOD_SECONDS, SUBSCRIPTION_PRICE_STARS,
    StickerMessagePlan, StickerMessageRequest, SubscriptionInvoiceLinkRequest,
    TELEGRAM_STARS_CURRENCY, TELEGRAM_TEXT_MAX_BYTES, TextMessageRequest,
    VIP_SUBSCRIPTION_DESCRIPTION, VIP_SUBSCRIPTION_TITLE, allow_sending_without_reply,
    build_audio_message_method, build_audio_message_plan, build_callback_answer_method,
    build_cancel_star_subscription_method, build_chat_action_method, build_delete_message_method,
    build_donation_invoice_link_method, build_edit_caption_message_method,
    build_edit_media_message_method, build_edit_media_message_plan,
    build_edit_reply_markup_message_method, build_edit_text_message_method,
    build_get_chat_administrators_method, build_get_chat_member_method, build_get_chat_method,
    build_guest_add_to_chat_markup, build_guest_html_answer_method,
    build_guest_query_answer_method, build_inline_keyboard_button_data,
    build_inline_keyboard_button_url, build_inline_keyboard_button_web_app,
    build_inline_keyboard_markup, build_inline_keyboard_row, build_inline_query_answer_method,
    build_inline_query_result_article, build_media_group_message_method,
    build_media_group_message_plan, build_photo_message_method, build_photo_message_plan,
    build_pre_checkout_ok_method, build_private_settings_keyboard,
    build_refund_star_payment_method, build_sticker_message_method, build_sticker_message_plan,
    build_subscription_invoice_link_method, build_text_message_method, build_text_message_methods,
    classify_payment_payload, donation_invoice_payload, fingerprint_audio_message_plan,
    fingerprint_photo_message_plan, fingerprint_sticker_message_plan,
    fingerprint_text_message_part, forum_thread_id, guest_add_to_chat_url,
    guest_dialog_fallback_html, guest_inline_description, guest_inline_result_id,
    guest_unsupported_feature_html, hash_content, message_target_chat, parse_mode_from_go,
    prepare_guest_html, subscription_invoice_payload, subscription_invoice_price_stars,
    telegram_member_can_open_group_settings, validate_text_message_text,
};
pub use pending_ops::{
    PENDING_OP_DELETE, PENDING_OP_EDIT, PendingOpBuildError, build_pending_op_method,
};
pub use persistence::{
    DEFAULT_DISPATCHER_QUEUE_KEY, DEFAULT_DISPATCHER_SHUTDOWN_TIMEOUT, DispatcherPersistenceError,
    PersistentDispatcherItem, PersistentDispatcherQueue, PersistentDispatcherReplay,
    PersistentDispatcherRestoreReport, RedisDispatcherQueueStore, persistent_queue_from_drain,
    persistent_queue_redis_value_from_items, persistent_queue_replay_from_items,
    persistent_queue_replay_from_json, persistent_queue_replay_from_redis_value,
    restore_persistent_queue_replay,
};
pub use rate_limit::{ChatLimiters, DEFAULT_DISPATCH_INTERVAL, DEFAULT_RATE_LIMITER_MAX_IDLE};
pub use transport::{
    TelegramOutboundMethod, TelegramOutboundMethodKind, TelegramOutboundResponse,
    TelegramOutboundResponseKind, TelegramSendErrorClassification, classify_telegram_send_error,
    execute_telegram_method, send_telegram_method_status, telegram_execute_error_is_reply_missing,
};
pub use update_startup::{
    GO_LONG_POLL_RETRY_DELAY, GO_LONG_POLL_TIMEOUT, GO_WEBHOOK_UPDATE_BUFFER_SIZE,
    GO_WEBHOOK_UPDATE_SEND_TIMEOUT, GetUpdatesExecutor, GetUpdatesFuture, LongPollUpdateSource,
    TELEGRAM_WEBHOOK_PATH, TELEGRAM_WEBHOOK_SECRET_HEADER, WebhookCertificate, WebhookSetup,
    WebhookUpdateRequestError, WebhookUpdateSendError, WebhookUpdateSender, WebhookUpdateSource,
    build_delete_webhook_method, build_get_updates_method, build_get_updates_method_with_offset,
    build_set_webhook_method, go_allowed_update_set, webhook_update_channel,
};

pub const INTEGRATION_CRATE: &str = "carapax";

pub const HELP_COMMAND: &str = "help";

pub const DONATE_COMMAND: &str = "donate";

/// Re-exported `carapax` context type used to anchor the Telegram boundary.
pub type CarapaxContext = carapax::Context;

/// Telegram bot command type from `carapax`.
pub type BotCommand = carapax::types::BotCommand;

/// Telegram command scope type from `carapax`.
pub type BotCommandScope = carapax::types::BotCommandScope;

/// Telegram method that deletes configured bot commands.
pub type DeleteBotCommands = carapax::types::DeleteBotCommands;

/// Telegram method that sets configured bot commands.
pub type SetBotCommands = carapax::types::SetBotCommands;

/// Telegram getMe method from `carapax`/`tgbot`.
pub type GetBot = carapax::types::GetBot;

/// Telegram bot identity returned by getMe.
pub type Bot = carapax::types::Bot;

/// Telegram getUpdates method from `carapax`.
pub type GetUpdates = carapax::types::GetUpdates;

/// Telegram getChat method from `carapax`.
pub type GetChat = carapax::types::GetChat;

/// Telegram full-chat information returned by getChat.
pub type ChatFullInfo = carapax::types::ChatFullInfo;

/// Telegram getChatMember method from `carapax`.
pub type GetChatMember = carapax::types::GetChatMember;

/// Telegram getChatAdministrators method from `carapax`.
pub type GetChatAdministrators = carapax::types::GetChatAdministrators;

/// Telegram setWebhook method from `carapax`.
pub type SetWebhook = carapax::types::SetWebhook;

/// Telegram deleteWebhook method from `carapax`.
pub type DeleteWebhook = carapax::types::DeleteWebhook;

/// Error returned by `carapax` when a Bot API command is invalid.
pub type BotCommandError = carapax::types::BotCommandError;

/// Telegram sendMessage method from `carapax`.
pub type SendMessage = carapax::types::SendMessage;

/// Telegram sendSticker method from `carapax`.
pub type SendSticker = carapax::types::SendSticker;

/// Telegram sendPhoto method from `carapax`.
pub type SendPhoto = carapax::types::SendPhoto;

/// Telegram sendMediaGroup method from `carapax`.
pub type SendMediaGroup = carapax::types::SendMediaGroup;

/// Telegram sendAudio method from `carapax`.
pub type SendAudio = carapax::types::SendAudio;

/// Telegram editMessageText method from `carapax`.
pub type EditMessageText = carapax::types::EditMessageText;

/// Telegram editMessageMedia method from `carapax`.
pub type EditMessageMedia = carapax::types::EditMessageMedia;

/// Telegram deleteMessage method from `carapax`.
pub type DeleteMessage = carapax::types::DeleteMessage;

/// Telegram answerPreCheckoutQuery method from `carapax`.
pub type AnswerPreCheckoutQuery = carapax::types::AnswerPreCheckoutQuery;

/// Telegram createInvoiceLink method from `carapax`.
pub type CreateInvoiceLink = carapax::types::CreateInvoiceLink;

/// Telegram refundStarPayment method from `carapax`.
pub type RefundStarPayment = carapax::types::RefundStarPayment;

/// Telegram editUserStarSubscription method from `carapax`.
pub type EditUserStarSubscription = carapax::types::EditUserStarSubscription;

/// Telegram reply markup type from `carapax`.
pub type ReplyMarkup = carapax::types::ReplyMarkup;

/// Telegram inline keyboard markup type from `carapax`.
pub type InlineKeyboardMarkup = carapax::types::InlineKeyboardMarkup;

/// Telegram inline keyboard button type from `carapax`.
pub type InlineKeyboardButton = carapax::types::InlineKeyboardButton;

/// Telegram WebApp info type from `carapax`.
pub type WebAppInfo = carapax::types::WebAppInfo;

/// Telegram parse mode type from `carapax`.
pub type ParseMode = carapax::types::ParseMode;

/// Telegram input-file type from `carapax`.
pub type InputFile = carapax::types::InputFile;

/// Telegram input-file reader type from `carapax`.
pub type InputFileReader = carapax::types::InputFileReader;

/// Telegram media group type from `carapax`.
pub type MediaGroup = carapax::types::MediaGroup;

/// Telegram media group item type from `carapax`.
pub type MediaGroupItem = carapax::types::MediaGroupItem;

/// Telegram input media photo metadata type from `carapax`.
pub type InputMediaPhoto = carapax::types::InputMediaPhoto;

/// Telegram input media type from `carapax`.
pub type InputMedia = carapax::types::InputMedia;

/// Telegram Bot API client from `carapax`.
pub type TelegramClient = carapax::api::Client;

/// Telegram message type returned by outbound send methods.
pub type TelegramMessage = carapax::types::Message;

/// Telegram Bot API client construction error from `carapax`.
pub type TelegramClientError = carapax::api::ClientError;

/// Create an empty Telegram integration context.
pub fn empty_context() -> CarapaxContext {
    CarapaxContext::default()
}

/// Create a Telegram Bot API client through the mandated `carapax` integration.
pub fn telegram_client(token: impl Into<String>) -> Result<TelegramClient, TelegramClientError> {
    telegram_client_with_base_url(token, "")
}

/// Create a Telegram Bot API client, optionally targeting a loopback/local Bot API host.
pub fn telegram_client_with_base_url(
    token: impl Into<String>,
    base_url: impl AsRef<str>,
) -> Result<TelegramClient, TelegramClientError> {
    let client = TelegramClient::new(token)?;
    let base_url = base_url.as_ref().trim().trim_end_matches('/');
    if base_url.is_empty() {
        Ok(client)
    } else {
        Ok(client.with_host(base_url.to_owned()))
    }
}

#[must_use]
pub const fn build_get_bot_method() -> GetBot {
    carapax::types::GetBot
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandScope {
    /// All private chats.
    Private,
    /// All group chats.
    Group,
    /// All group chat administrators.
    GroupAdmin,
}

impl CommandScope {
    pub fn inventory_name(self) -> &'static str {
        match self {
            Self::Private => "privateCommands",
            Self::Group => "groupCommands",
            Self::GroupAdmin => "groupAdminCommands",
        }
    }

    /// Convert to the `carapax` Bot API scope.
    pub fn carapax_scope(self) -> BotCommandScope {
        match self {
            Self::Private => BotCommandScope::AllPrivateChats,
            Self::Group => BotCommandScope::AllGroupChats,
            Self::GroupAdmin => BotCommandScope::AllChatAdministrators,
        }
    }
}

/// Static command definition before conversion into `carapax` types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandSpec {
    /// Command name without a leading slash.
    pub command: &'static str,
    /// Telegram command description.
    pub description: &'static str,
}

impl CommandSpec {
    /// Convert to a validated `carapax` command.
    pub fn to_carapax(self) -> Result<BotCommand, BotCommandError> {
        BotCommand::new(self.command, self.description)
    }
}

pub const PRIVATE_COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        command: "reset",
        description: "Сбросить контекст диалога",
    },
    CommandSpec {
        command: HELP_COMMAND,
        description: "Краткая справка о возможностях",
    },
    CommandSpec {
        command: "settings",
        description: "Настройки бота",
    },
    CommandSpec {
        command: "vip",
        description: "Оформить VIP-подписку",
    },
    CommandSpec {
        command: "song",
        description: "Сгенерировать песню (VIP)",
    },
    CommandSpec {
        command: DONATE_COMMAND,
        description: "Поддержать разработку",
    },
];

pub const GROUP_COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        command: "reset",
        description: "Сбросить контекст диалога",
    },
    CommandSpec {
        command: HELP_COMMAND,
        description: "Краткая справка о возможностях",
    },
    CommandSpec {
        command: "vip",
        description: "Оформить VIP-подписку",
    },
    CommandSpec {
        command: "song",
        description: "Сгенерировать песню (VIP)",
    },
    CommandSpec {
        command: DONATE_COMMAND,
        description: "Поддержать разработку",
    },
    CommandSpec {
        command: "checkin",
        description: "Игра дня",
    },
    CommandSpec {
        command: "delete_drawing",
        description: "Удалить последнюю генерацию",
    },
];

/// Commands for group administrators. Order matches `append(groupCommands, settings)`.
pub const GROUP_ADMIN_COMMANDS: &[CommandSpec] = &[
    CommandSpec {
        command: "reset",
        description: "Сбросить контекст диалога",
    },
    CommandSpec {
        command: HELP_COMMAND,
        description: "Краткая справка о возможностях",
    },
    CommandSpec {
        command: "vip",
        description: "Оформить VIP-подписку",
    },
    CommandSpec {
        command: "song",
        description: "Сгенерировать песню (VIP)",
    },
    CommandSpec {
        command: DONATE_COMMAND,
        description: "Поддержать разработку",
    },
    CommandSpec {
        command: "checkin",
        description: "Игра дня",
    },
    CommandSpec {
        command: "delete_drawing",
        description: "Удалить последнюю генерацию",
    },
    CommandSpec {
        command: "settings",
        description: "Настройки бота",
    },
];

/// Scoped command set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandSet {
    /// Telegram command scope.
    pub scope: CommandScope,
    /// Ordered commands for the scope.
    pub commands: &'static [CommandSpec],
}

pub const COMMAND_SETS: &[CommandSet] = &[
    CommandSet {
        scope: CommandScope::Private,
        commands: PRIVATE_COMMANDS,
    },
    CommandSet {
        scope: CommandScope::Group,
        commands: GROUP_COMMANDS,
    },
    CommandSet {
        scope: CommandScope::GroupAdmin,
        commands: GROUP_ADMIN_COMMANDS,
    },
];

/// Build the `deleteMyCommands` method used before scoped command registration.
pub fn delete_my_commands_method() -> DeleteBotCommands {
    DeleteBotCommands::default()
}

pub fn set_my_commands_methods() -> Result<Vec<SetBotCommands>, BotCommandError> {
    COMMAND_SETS
        .iter()
        .map(|set| {
            let commands = set
                .commands
                .iter()
                .map(|command| command.to_carapax())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SetBotCommands::new(commands).with_scope(set.scope.carapax_scope()))
        })
        .collect()
}

/// Command alias group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandAliasGroup {
    /// Canonical alias group name.
    pub name: &'static str,
    /// Accepted aliases.
    pub aliases: &'static [&'static str],
}

pub const DRAW_ALIASES: &[&str] = &["нарисуй", "draw", "рисуй"];

pub const EDIT_ALIASES: &[&str] = &[
    "изменить",
    "измени",
    "измените",
    "отредактируй",
    "отредактируйте",
    "редактируй",
    "редактируйте",
    "поправь",
    "поправьте",
    "исправь",
    "исправьте",
    "переделай",
    "переделайте",
    "перерисуй",
    "перерисуйте",
    "замени",
    "замените",
    "убери",
    "уберите",
    "добавь",
    "добавьте",
    "убавь",
    "убавьте",
    "усиль",
    "усильте",
    "edit",
    "modify",
    "change",
    "alter",
    "retouch",
    "fix",
    "replace",
    "remove",
    "delete",
    "add",
    "adjust",
    "tweak",
];

pub const SONG_ALIASES: &[&str] = &["song", "песня", "!song", "!песня"];

pub const TRANSLATE_ALIASES: &[&str] = &["переведи", "перевод", "translate"];

pub const COMMAND_ALIAS_GROUPS: &[CommandAliasGroup] = &[
    CommandAliasGroup {
        name: "draw",
        aliases: DRAW_ALIASES,
    },
    CommandAliasGroup {
        name: "edit",
        aliases: EDIT_ALIASES,
    },
    CommandAliasGroup {
        name: "song",
        aliases: SONG_ALIASES,
    },
    CommandAliasGroup {
        name: "translate",
        aliases: TRANSLATE_ALIASES,
    },
];

pub const CALLBACK_ACTIONS: &[&str] = &[
    "back_to_vip_status",
    "cancel_vip",
    "checkin_theme_select",
    "confirm_cancel_vip",
    "cts",
    "del_c",
    "del_fc",
    "del_fp",
    "del_i",
    "del_x",
    "delete",
    "dl_c",
    "dl_i",
    "dl_x",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApiConstructorUsage {
    pub name: &'static str,
    pub count: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawApiMethodUsage {
    /// Raw Bot API method name.
    pub method: &'static str,
    pub count: usize,
}

pub const API_CONSTRUCTOR_USAGES: &[ApiConstructorUsage] = &[
    ApiConstructorUsage {
        name: "AnswerGuestQuery",
        count: 1,
    },
    ApiConstructorUsage {
        name: "Audio",
        count: 1,
    },
    ApiConstructorUsage {
        name: "BotAPIWithAPIEndpoint",
        count: 1,
    },
    ApiConstructorUsage {
        name: "BotAPIWithClient",
        count: 2,
    },
    ApiConstructorUsage {
        name: "BotCommandScopeAllChatAdministrators",
        count: 1,
    },
    ApiConstructorUsage {
        name: "BotCommandScopeAllGroupChats",
        count: 1,
    },
    ApiConstructorUsage {
        name: "BotCommandScopeAllPrivateChats",
        count: 1,
    },
    ApiConstructorUsage {
        name: "Callback",
        count: 28,
    },
    ApiConstructorUsage {
        name: "CallbackWithAlert",
        count: 10,
    },
    ApiConstructorUsage {
        name: "ChatAction",
        count: 1,
    },
    ApiConstructorUsage {
        name: "DeleteMessage",
        count: 2,
    },
    ApiConstructorUsage {
        name: "DeleteMyCommands",
        count: 1,
    },
    ApiConstructorUsage {
        name: "EditMessageCaption",
        count: 1,
    },
    ApiConstructorUsage {
        name: "EditMessageReplyMarkup",
        count: 5,
    },
    ApiConstructorUsage {
        name: "EditMessageText",
        count: 5,
    },
    ApiConstructorUsage {
        name: "InlineKeyboardButtonData",
        count: 16,
    },
    ApiConstructorUsage {
        name: "InlineKeyboardButtonURL",
        count: 8,
    },
    ApiConstructorUsage {
        name: "InlineKeyboardMarkup",
        count: 14,
    },
    ApiConstructorUsage {
        name: "InlineKeyboardRow",
        count: 12,
    },
    ApiConstructorUsage {
        name: "InlineQueryResultArticle",
        count: 1,
    },
    ApiConstructorUsage {
        name: "InlineQueryResultArticleHTML",
        count: 1,
    },
    ApiConstructorUsage {
        name: "MediaGroup",
        count: 1,
    },
    ApiConstructorUsage {
        name: "Message",
        count: 17,
    },
    ApiConstructorUsage {
        name: "Photo",
        count: 5,
    },
    ApiConstructorUsage {
        name: "SetMyCommandsWithScope",
        count: 3,
    },
    ApiConstructorUsage {
        name: "Sticker",
        count: 3,
    },
    ApiConstructorUsage {
        name: "Update",
        count: 1,
    },
];

pub const RAW_API_METHOD_USAGES: &[RawApiMethodUsage] = &[
    RawApiMethodUsage {
        method: "createInvoiceLink",
        count: 2,
    },
    RawApiMethodUsage {
        method: "editUserStarSubscription",
        count: 1,
    },
    RawApiMethodUsage {
        method: "refundStarPayment",
        count: 2,
    },
];

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        BotCommandError, BotCommandScope, CallbackActionParse, CallbackHandlerKind,
        CallbackQueryRoute, CommandScope, DELETE_DRAWING_ACTION_CLOSE,
        DELETE_DRAWING_ACTION_CONFIRM, DELETE_DRAWING_ACTION_FRAME_CONFIRM,
        DELETE_DRAWING_ACTION_FRAME_PICK, DELETE_DRAWING_ACTION_INIT, DELETE_LYRICS_ACTION_CLOSE,
        DELETE_LYRICS_ACTION_CONFIRM, DELETE_LYRICS_ACTION_INIT, GROUP_ADMIN_COMMANDS,
        GROUP_COMMANDS, PRIVATE_COMMANDS, build_delete_drawing_confirm_keyboard,
        build_delete_drawing_frame_confirm_keyboard, build_delete_drawing_frame_picker_keyboard,
        build_delete_drawing_initial_keyboard, build_lyrics_delete_confirm_keyboard,
        build_lyrics_delete_keyboard, callback_handler_for_action, callback_query_ack_method,
        callback_query_ack_request, callback_query_route, checkin_theme_callback_init,
        checkin_theme_callback_theme, checkin_theme_selection_ack_method,
        checkin_theme_selection_alert, delete_drawing_callback_data, delete_lyrics_callback_data,
        delete_my_commands_method, empty_context, parse_callback_action, parse_callback_i64,
        set_my_commands_methods, settings_callback_ack_method,
    };

    #[test]
    fn telegram_boundary_uses_carapax() {
        assert_eq!(super::INTEGRATION_CRATE, "carapax");
        let _ = empty_context();
        let _ = delete_my_commands_method();
    }

    #[test]
    fn command_order_matches_go_startup() {
        assert_eq!(
            PRIVATE_COMMANDS
                .iter()
                .map(|command| command.command)
                .collect::<Vec<_>>(),
            ["reset", "help", "settings", "vip", "song", "donate"]
        );
        assert_eq!(
            GROUP_COMMANDS
                .iter()
                .map(|command| command.command)
                .collect::<Vec<_>>(),
            [
                "reset",
                "help",
                "vip",
                "song",
                "donate",
                "checkin",
                "delete_drawing"
            ]
        );
        assert_eq!(
            GROUP_ADMIN_COMMANDS
                .iter()
                .map(|command| command.command)
                .collect::<Vec<_>>(),
            [
                "reset",
                "help",
                "vip",
                "song",
                "donate",
                "checkin",
                "delete_drawing",
                "settings"
            ]
        );
    }

    #[test]
    fn parse_callback_action_uses_long_and_short_keys() {
        let CallbackActionParse::Action { data, action } =
            parse_callback_action(r#"{"action":"delete"}"#)
        else {
            panic!("expected action callback data");
        };
        assert_eq!(action, "delete");
        assert_eq!(data.get("action").map(String::as_str), Some("delete"));

        let CallbackActionParse::Action { data, action } =
            parse_callback_action(r#"{"a":"del_i","u":"1"}"#)
        else {
            panic!("expected short action callback data");
        };
        assert_eq!(action, "del_i");
        assert_eq!(data.get("u").map(String::as_str), Some("1"));
    }

    #[test]
    fn parse_callback_action_rejects_legacy_or_actionless_data() {
        assert_eq!(
            parse_callback_action("old-format"),
            CallbackActionParse::Invalid
        );

        let CallbackActionParse::Actionless(data) = parse_callback_action(r#"{"u":"1"}"#) else {
            panic!("expected json callback data without action");
        };
        assert_eq!(data.get("u").map(String::as_str), Some("1"));
    }

    #[test]
    fn callback_handler_for_action_covers_known_actions() {
        for action in [
            "delete",
            "cancel_vip",
            "confirm_cancel_vip",
            "back_to_vip_status",
            "checkin_theme_select",
            "cts",
            "del_i",
            "dl_i",
        ] {
            assert!(
                callback_handler_for_action(action).is_some(),
                "expected callback handler for {action}"
            );
        }

        assert_eq!(callback_handler_for_action("unknown"), None);
        assert_eq!(
            callback_handler_for_action("checkin_theme_select"),
            Some(CallbackHandlerKind::CheckinThemeSelect)
        );
    }

    #[test]
    fn delete_drawing_callback_data_matches_go_json_shapes() {
        assert_eq!(
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_INIT, 42, -1001234567890, 0),
            r#"{"a":"del_i","u":"42","c":"-1001234567890"}"#
        );
        assert_eq!(
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_FRAME_PICK, 42, -1001234567890, 3),
            r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"3"}"#
        );
        assert_eq!(
            delete_drawing_callback_data(
                DELETE_DRAWING_ACTION_FRAME_CONFIRM,
                42,
                -1001234567890,
                -1
            ),
            r#"{"a":"del_fc","u":"42","c":"-1001234567890"}"#
        );
        assert_eq!(
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_CONFIRM, 42, -1001234567890, 0),
            r#"{"a":"del_c","u":"42","c":"-1001234567890"}"#
        );
        assert_eq!(
            delete_drawing_callback_data(DELETE_DRAWING_ACTION_CLOSE, 42, -1001234567890, 0),
            r#"{"a":"del_x","u":"42","c":"-1001234567890"}"#
        );
    }

    #[test]
    fn delete_lyrics_callback_data_matches_go_json_shapes() {
        assert_eq!(
            delete_lyrics_callback_data(DELETE_LYRICS_ACTION_INIT, 42, -1001234567890),
            r#"{"a":"dl_i","u":"42","c":"-1001234567890"}"#
        );
        assert_eq!(
            delete_lyrics_callback_data(DELETE_LYRICS_ACTION_CONFIRM, 42, -1001234567890),
            r#"{"a":"dl_c","u":"42","c":"-1001234567890"}"#
        );
        assert_eq!(
            delete_lyrics_callback_data(DELETE_LYRICS_ACTION_CLOSE, 42, -1001234567890),
            r#"{"a":"dl_x","u":"42","c":"-1001234567890"}"#
        );
        assert_eq!(
            callback_handler_for_action(DELETE_LYRICS_ACTION_CLOSE),
            None
        );
    }

    #[test]
    fn parse_callback_i64_matches_go_zero_fallback() {
        assert_eq!(parse_callback_i64("12345"), 12345);
        assert_eq!(parse_callback_i64("-1001234567890"), -1001234567890);
        assert_eq!(parse_callback_i64(" 42 "), 42);
        assert_eq!(parse_callback_i64("12x45"), 0);
        assert_eq!(parse_callback_i64(""), 0);
        assert_eq!(parse_callback_i64("9223372036854775808"), 0);
    }

    #[test]
    fn delete_drawing_keyboards_match_go_layouts() -> Result<(), Box<dyn std::error::Error>> {
        let initial =
            serde_json::to_value(build_delete_drawing_initial_keyboard(42, -1001234567890))?;
        assert_eq!(
            initial["inline_keyboard"],
            json!([
                [
                    {
                        "text": "🗑️ Удалить",
                        "callback_data": r#"{"a":"del_i","u":"42","c":"-1001234567890"}"#
                    },
                    {
                        "text": "✕",
                        "callback_data": r#"{"a":"del_x","u":"42","c":"-1001234567890"}"#
                    }
                ]
            ])
        );

        let confirm =
            serde_json::to_value(build_delete_drawing_confirm_keyboard(42, -1001234567890))?;
        assert_eq!(
            confirm["inline_keyboard"],
            json!([
                [
                    {
                        "text": "Да, удалить? ❌",
                        "callback_data": r#"{"a":"del_c","u":"42","c":"-1001234567890"}"#
                    },
                    {
                        "text": "✕",
                        "callback_data": r#"{"a":"del_x","u":"42","c":"-1001234567890"}"#
                    }
                ]
            ])
        );

        let frame_confirm = serde_json::to_value(build_delete_drawing_frame_confirm_keyboard(
            42,
            -1001234567890,
            3,
        ))?;
        assert_eq!(
            frame_confirm["inline_keyboard"],
            json!([
                [
                    {
                        "text": "Удалить #3? ❌",
                        "callback_data": r#"{"a":"del_fc","u":"42","c":"-1001234567890","n":"3"}"#
                    },
                    {
                        "text": "✕",
                        "callback_data": r#"{"a":"del_x","u":"42","c":"-1001234567890"}"#
                    }
                ]
            ])
        );

        Ok(())
    }

    #[test]
    fn delete_drawing_frame_picker_chunks_buttons_like_go() -> Result<(), Box<dyn std::error::Error>>
    {
        let picker = serde_json::to_value(build_delete_drawing_frame_picker_keyboard(
            42,
            -1001234567890,
            6,
        ))?;
        assert_eq!(
            picker["inline_keyboard"],
            json!([
                [
                    {
                        "text": "#1",
                        "callback_data": r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"1"}"#
                    },
                    {
                        "text": "#2",
                        "callback_data": r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"2"}"#
                    },
                    {
                        "text": "#3",
                        "callback_data": r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"3"}"#
                    },
                    {
                        "text": "#4",
                        "callback_data": r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"4"}"#
                    },
                    {
                        "text": "#5",
                        "callback_data": r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"5"}"#
                    }
                ],
                [
                    {
                        "text": "#6",
                        "callback_data": r#"{"a":"del_fp","u":"42","c":"-1001234567890","n":"6"}"#
                    },
                    {
                        "text": "Всё",
                        "callback_data": r#"{"a":"del_c","u":"42","c":"-1001234567890"}"#
                    },
                    {
                        "text": "✕",
                        "callback_data": r#"{"a":"del_x","u":"42","c":"-1001234567890"}"#
                    }
                ]
            ])
        );

        Ok(())
    }

    #[test]
    fn lyrics_delete_keyboards_match_go_layouts() -> Result<(), Box<dyn std::error::Error>> {
        let initial = serde_json::to_value(build_lyrics_delete_keyboard(42, -1001234567890))?;
        assert_eq!(
            initial["inline_keyboard"],
            json!([
                [
                    {
                        "text": "🗑 Удалить текст",
                        "callback_data": r#"{"a":"dl_i","u":"42","c":"-1001234567890"}"#
                    }
                ]
            ])
        );

        let confirm =
            serde_json::to_value(build_lyrics_delete_confirm_keyboard(42, -1001234567890))?;
        assert_eq!(
            confirm["inline_keyboard"],
            json!([
                [
                    {
                        "text": "Да, удалить",
                        "callback_data": r#"{"a":"dl_c","u":"42","c":"-1001234567890"}"#
                    },
                    {
                        "text": "✕",
                        "callback_data": r#"{"a":"dl_x","u":"42","c":"-1001234567890"}"#
                    }
                ]
            ])
        );

        Ok(())
    }

    #[test]
    fn checkin_theme_callback_data_uses_long_and_short_keys() {
        let long = parse_callback_action(r#"{"init":"10","i":"20","theme":"classic","t":"short"}"#)
            .into_data()
            .expect("callback data");
        assert_eq!(checkin_theme_callback_init(&long), Some("10"));
        assert_eq!(checkin_theme_callback_theme(&long), "classic");

        let short = parse_callback_action(r#"{"i":"20","t":"short"}"#)
            .into_data()
            .expect("callback data");
        assert_eq!(checkin_theme_callback_init(&short), Some("20"));
        assert_eq!(checkin_theme_callback_theme(&short), "short");
    }

    #[test]
    fn checkin_theme_selection_alert_matches_go_blocking() {
        let own = parse_callback_action(r#"{"init":"10"}"#)
            .into_data()
            .expect("callback data");
        assert_eq!(checkin_theme_selection_alert(10, &own), ("", false));

        let foreign = parse_callback_action(r#"{"init":"20"}"#)
            .into_data()
            .expect("callback data");
        assert_eq!(
            checkin_theme_selection_alert(10, &foreign),
            ("Только инициатор может выбрать тему", true)
        );

        let missing = parse_callback_action(r#"{"theme":"classic"}"#)
            .into_data()
            .expect("callback data");
        assert_eq!(checkin_theme_selection_alert(10, &missing), ("", true));
    }

    #[test]
    fn checkin_theme_selection_ack_method_matches_go_allowed_ack() {
        let data = parse_callback_action(r#"{"a":"cts","i":"10","t":"classic"}"#)
            .into_data()
            .expect("callback data");
        let (method, blocked) = checkin_theme_selection_ack_method("query-id", 10, &data);

        assert!(!blocked);
        assert_eq!(
            method.kind(),
            super::TelegramOutboundMethodKind::AnswerCallbackQuery
        );
        let super::TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
            panic!("expected answerCallbackQuery method");
        };
        let payload = serde_json::to_value(method.as_ref()).expect("callback ack JSON");
        assert_eq!(payload["callback_query_id"], "query-id");
        assert!(payload.get("text").is_none());
        assert!(payload.get("show_alert").is_none());
        assert!(payload.get("url").is_none());
        assert!(payload.get("cache_time").is_none());
    }

    #[test]
    fn checkin_theme_selection_ack_method_matches_go_blocking_alerts() {
        let foreign = parse_callback_action(r#"{"a":"cts","i":"20"}"#)
            .into_data()
            .expect("callback data");
        let (method, blocked) = checkin_theme_selection_ack_method("query-id", 10, &foreign);
        assert!(blocked);

        let super::TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
            panic!("expected answerCallbackQuery method");
        };
        let payload = serde_json::to_value(method.as_ref()).expect("callback ack JSON");
        assert_eq!(payload["callback_query_id"], "query-id");
        assert_eq!(payload["text"], "Только инициатор может выбрать тему");
        assert_eq!(payload["show_alert"], true);
        assert!(payload.get("url").is_none());
        assert!(payload.get("cache_time").is_none());

        let missing_init = parse_callback_action(r#"{"a":"cts","t":"classic"}"#)
            .into_data()
            .expect("callback data");
        let (method, blocked) = checkin_theme_selection_ack_method("query-id", 10, &missing_init);
        assert!(blocked);
        let super::TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
            panic!("expected answerCallbackQuery method");
        };
        let payload = serde_json::to_value(method.as_ref()).expect("callback ack JSON");
        assert_eq!(payload["callback_query_id"], "query-id");
        assert!(payload.get("text").is_none());
        assert_eq!(payload["show_alert"], true);
    }

    #[test]
    fn callback_query_route_preserves_go_pre_handler_order() {
        assert_eq!(
            callback_query_route(false, true, r#"{"a":"del_i"}"#),
            CallbackQueryRoute::AckOrphan
        );
        assert_eq!(
            callback_query_route(true, true, r#"{"a":"del_i"}"#),
            CallbackQueryRoute::SkipRateLimited
        );
        assert_eq!(
            callback_query_route(true, false, ""),
            CallbackQueryRoute::AckEmptyData
        );
        assert_eq!(
            callback_query_route(true, false, "settings:enable_global_text_reply=true"),
            CallbackQueryRoute::Settings {
                data: "settings:enable_global_text_reply=true".to_owned()
            }
        );
    }

    #[test]
    fn callback_query_route_splits_ack_fallbacks_and_known_handlers() {
        assert_eq!(
            callback_query_route(true, false, "old-format"),
            CallbackQueryRoute::AckLegacyData
        );
        assert_eq!(
            callback_query_route(true, false, r#"{"u":"1"}"#),
            CallbackQueryRoute::AckActionlessJson {
                data: parse_callback_action(r#"{"u":"1"}"#)
                    .into_data()
                    .expect("callback data")
            }
        );
        assert_eq!(
            callback_query_route(true, false, r#"{"action":"unknown"}"#),
            CallbackQueryRoute::AckUnknownAction {
                action: "unknown".to_owned()
            }
        );

        let route = callback_query_route(true, false, r#"{"a":"cts","i":"42"}"#);
        let CallbackQueryRoute::Handle {
            handler,
            action,
            data,
        } = route
        else {
            panic!("expected known handler route");
        };
        assert_eq!(handler, CallbackHandlerKind::CheckinThemeSelect);
        assert_eq!(action, "cts");
        assert_eq!(data.get("i").map(String::as_str), Some("42"));
    }

    #[test]
    fn callback_query_ack_request_matches_go_empty_ack_routes() {
        for route in [
            CallbackQueryRoute::AckOrphan,
            CallbackQueryRoute::AckEmptyData,
            CallbackQueryRoute::AckLegacyData,
            CallbackQueryRoute::AckActionlessJson {
                data: parse_callback_action(r#"{"u":"1"}"#)
                    .into_data()
                    .expect("callback data"),
            },
            CallbackQueryRoute::AckUnknownAction {
                action: "unknown".to_owned(),
            },
        ] {
            let ack = callback_query_ack_request("query-id", &route).expect("empty ack");
            assert_eq!(ack.callback_query_id, "query-id");
            assert!(ack.text.is_empty());
            assert!(!ack.show_alert);
            assert!(ack.url.is_empty());
            assert_eq!(ack.cache_time, 0);
        }
    }

    #[test]
    fn callback_query_ack_request_skips_delegated_or_rate_limited_routes() {
        assert!(
            callback_query_ack_request("query-id", &CallbackQueryRoute::SkipRateLimited).is_none()
        );
        assert!(
            callback_query_ack_request(
                "query-id",
                &CallbackQueryRoute::Settings {
                    data: "settings:x".to_owned()
                }
            )
            .is_none()
        );
        assert!(
            callback_query_ack_request(
                "query-id",
                &callback_query_route(true, false, r#"{"a":"cts","i":"42"}"#)
            )
            .is_none()
        );
    }

    #[test]
    fn callback_query_ack_method_builds_go_empty_answer_callback_query() {
        let method = callback_query_ack_method("query-id", &CallbackQueryRoute::AckEmptyData)
            .expect("empty callback ack method");

        assert_eq!(
            method.kind(),
            super::TelegramOutboundMethodKind::AnswerCallbackQuery
        );
        assert_eq!(method.method_name(), "answerCallbackQuery");

        let super::TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
            panic!("expected answerCallbackQuery method");
        };
        let payload = serde_json::to_value(method.as_ref()).expect("callback ack JSON");
        assert_eq!(payload["callback_query_id"], "query-id");
        assert!(payload.get("text").is_none());
        assert!(payload.get("show_alert").is_none());
        assert!(payload.get("url").is_none());
        assert!(payload.get("cache_time").is_none());
    }

    #[test]
    fn callback_query_ack_method_skips_delegated_or_rate_limited_routes() {
        assert!(
            callback_query_ack_method("query-id", &CallbackQueryRoute::SkipRateLimited).is_none()
        );
        assert!(
            callback_query_ack_method(
                "query-id",
                &CallbackQueryRoute::Settings {
                    data: "settings:x".to_owned()
                }
            )
            .is_none()
        );
        assert!(
            callback_query_ack_method(
                "query-id",
                &callback_query_route(true, false, r#"{"a":"delete"}"#)
            )
            .is_none()
        );
    }

    #[test]
    fn settings_callback_ack_method_matches_go_cached_empty_ack() {
        let method = settings_callback_ack_method("query-id");

        assert_eq!(
            method.kind(),
            super::TelegramOutboundMethodKind::AnswerCallbackQuery
        );
        assert_eq!(method.method_name(), "answerCallbackQuery");

        let super::TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
            panic!("expected answerCallbackQuery method");
        };
        let payload = serde_json::to_value(method.as_ref()).expect("settings callback ack JSON");
        assert_eq!(payload["callback_query_id"], "query-id");
        assert!(payload.get("text").is_none());
        assert!(payload.get("show_alert").is_none());
        assert!(payload.get("url").is_none());
        assert_eq!(payload["cache_time"], 10);
    }

    #[test]
    fn scoped_commands_build_carapax_methods() -> Result<(), BotCommandError> {
        let methods = set_my_commands_methods()?;

        assert_eq!(methods.len(), 3);
        assert_eq!(
            CommandScope::Private.carapax_scope(),
            BotCommandScope::AllPrivateChats
        );
        assert_eq!(
            CommandScope::Group.carapax_scope(),
            BotCommandScope::AllGroupChats
        );
        assert_eq!(
            CommandScope::GroupAdmin.carapax_scope(),
            BotCommandScope::AllChatAdministrators
        );

        Ok(())
    }
}
