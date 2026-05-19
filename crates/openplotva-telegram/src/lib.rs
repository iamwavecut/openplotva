//! Telegram Bot API boundary for OpenPlotva.

mod dedup;
mod dispatcher;
mod html;
mod outbound;
mod persistence;
mod rate_limit;
mod transport;

pub use dedup::{DEFAULT_DEBOUNCE_CACHE_SIZE, DEFAULT_DEBOUNCE_WINDOW, Debouncer, DebouncerConfig};
pub use dispatcher::{
    DEFAULT_DISPATCHER_CLEANUP_INTERVAL, DispatcherConfig, DispatcherDrain, DispatcherMessage,
    DispatcherPersistencePayload, DispatcherQueue, DispatcherQueuedMessage,
    DispatcherRestoredMessage, DispatcherRuntimeConfig, DispatcherSendStatus, DispatcherStats,
    DispatcherWorkItem, DispatcherWorkerLoopOutcome, DispatcherWorkerOutcome, EnqueueOutcome,
    QueueSnapshot, RegularDequeueOutcome, run_limiter_cleanup_until,
};
pub use html::{
    TELEGRAM_PARSE_MODE_HTML, clean_unicode_non_printables, ensure_telegram_safe_text,
    escape_telegram_html_text, extract_visible_text, is_valid_telegram_html,
    sanitize_telegram_html, split_telegram_text, strip_telegram_html,
};
pub use outbound::{
    AudioMessagePlan, AudioMessageRequest, AudioSource, ChatRef, EditMediaMessagePlan,
    EditMediaMessageRequest, EditTextMessageRequest, MESSAGE_TYPE_TEXT, MediaGroupMessagePlan,
    MediaGroupMessageRequest, MediaGroupPhotoItem, MessageFingerprint, OutboundBuildError,
    PhotoMessagePlan, PhotoMessageRequest, PhotoSource, ReplyMessageRef, ReplyParametersPlan,
    StickerMessagePlan, StickerMessageRequest, TELEGRAM_TEXT_MAX_BYTES, TextMessageRequest,
    allow_sending_without_reply, build_audio_message_method, build_audio_message_plan,
    build_edit_media_message_method, build_edit_media_message_plan, build_edit_text_message_method,
    build_media_group_message_method, build_media_group_message_plan, build_photo_message_method,
    build_photo_message_plan, build_sticker_message_method, build_sticker_message_plan,
    build_text_message_method, build_text_message_methods, fingerprint_audio_message_plan,
    fingerprint_photo_message_plan, fingerprint_sticker_message_plan, forum_thread_id,
    hash_content, message_target_chat, parse_mode_from_go, validate_text_message_text,
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
    TelegramOutboundResponseKind, execute_telegram_method, send_telegram_method_status,
};

pub const INTEGRATION_CRATE: &str = "carapax";

/// `/help` command constant from the Go runtime.
pub const HELP_COMMAND: &str = "help";

/// `/donate` command constant from the Go runtime.
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

/// Telegram reply markup type from `carapax`.
pub type ReplyMarkup = carapax::types::ReplyMarkup;

/// Telegram inline keyboard markup type from `carapax`.
pub type InlineKeyboardMarkup = carapax::types::InlineKeyboardMarkup;

/// Telegram inline keyboard button type from `carapax`.
pub type InlineKeyboardButton = carapax::types::InlineKeyboardButton;

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

/// Create an empty Telegram integration context.
pub fn empty_context() -> CarapaxContext {
    CarapaxContext::default()
}

/// Bot command scope used by the Go command setup.
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

/// Commands for all private chats. Order matches `cmd/main.go`.
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

/// Commands for all group chats. Order matches `cmd/main.go`.
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

/// All command sets applied during Go bot initialization.
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

/// Build the three `setMyCommands` methods used by Go startup.
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

/// Draw command aliases from the Go fetcher.
pub const DRAW_ALIASES: &[&str] = &["нарисуй", "draw", "рисуй"];

/// Edit command aliases from the Go fetcher.
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

/// Song command aliases from the Go fetcher.
pub const SONG_ALIASES: &[&str] = &["song", "песня", "!song", "!песня"];

/// Translation command aliases from the Go fetcher.
pub const TRANSLATE_ALIASES: &[&str] = &["переведи", "перевод", "translate"];

/// All command alias groups currently found in Go.
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

/// Callback data action prefixes currently found in Go.
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

/// Go Telegram API constructor inventory entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApiConstructorUsage {
    /// Go constructor name without the `api.New` prefix.
    pub name: &'static str,
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use serde::Deserialize;

    use super::{
        API_CONSTRUCTOR_USAGES, BotCommandError, BotCommandScope, CALLBACK_ACTIONS,
        COMMAND_ALIAS_GROUPS, COMMAND_SETS, CommandScope, DONATE_COMMAND, GROUP_ADMIN_COMMANDS,
        GROUP_COMMANDS, HELP_COMMAND, PRIVATE_COMMANDS, delete_my_commands_method, empty_context,
        set_my_commands_methods,
    };

    #[derive(Debug, Deserialize)]
    struct TelegramInventory {
        bot_commands: Vec<InventoryCommand>,
        command_constants: Vec<String>,
        command_aliases: BTreeMap<String, Vec<String>>,
        callback_actions: Vec<String>,
        api_constructors: Vec<InventoryApiConstructor>,
    }

    #[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
    struct InventoryCommand {
        scope: String,
        command: String,
        description: String,
    }

    #[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
    struct InventoryApiConstructor {
        name: String,
        count: usize,
    }

    fn inventory() -> Result<TelegramInventory, serde_json::Error> {
        serde_json::from_str(include_str!("../../../docs/contract/generated/telegram.json"))
    }

    fn rust_inventory_commands() -> BTreeSet<InventoryCommand> {
        COMMAND_SETS
            .iter()
            .flat_map(|set| {
                set.commands.iter().map(move |command| InventoryCommand {
                    scope: set.scope.inventory_name().to_owned(),
                    command: command.command.to_owned(),
                    description: command.description.to_owned(),
                })
            })
            .collect()
    }

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
    fn command_sets_match_generated_go_inventory() -> Result<(), serde_json::Error> {
        let expected = inventory()?
            .bot_commands
            .into_iter()
            .collect::<BTreeSet<_>>();

        assert_eq!(rust_inventory_commands(), expected);

        Ok(())
    }

    #[test]
    fn command_constants_match_generated_go_inventory() -> Result<(), serde_json::Error> {
        let mut expected = inventory()?.command_constants;
        expected.sort();

        let mut actual = vec![DONATE_COMMAND.to_owned(), HELP_COMMAND.to_owned()];
        actual.sort();

        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn command_aliases_match_generated_go_inventory() -> Result<(), serde_json::Error> {
        let expected = inventory()?.command_aliases;
        let actual = COMMAND_ALIAS_GROUPS
            .iter()
            .map(|group| {
                (
                    group.name.to_owned(),
                    group
                        .aliases
                        .iter()
                        .map(|alias| (*alias).to_owned())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn callback_actions_match_generated_go_inventory() -> Result<(), serde_json::Error> {
        let expected = inventory()?
            .callback_actions
            .into_iter()
            .collect::<BTreeSet<_>>();
        let actual = CALLBACK_ACTIONS
            .iter()
            .map(|action| (*action).to_owned())
            .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn api_constructor_usage_matches_generated_go_inventory() -> Result<(), serde_json::Error> {
        let expected = inventory()?
            .api_constructors
            .into_iter()
            .collect::<BTreeSet<_>>();
        let actual = API_CONSTRUCTOR_USAGES
            .iter()
            .map(|usage| InventoryApiConstructor {
                name: usage.name.to_owned(),
                count: usage.count,
            })
            .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);

        Ok(())
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
