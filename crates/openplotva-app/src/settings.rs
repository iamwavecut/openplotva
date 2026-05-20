//! App-level fetcher settings command behavior.

use std::{fmt, future::Future, pin::Pin, time::Duration};

use carapax::types::{
    Chat as TelegramChat, ChatMember as TelegramChatMember, ChatMemberRestricted,
    Message as TelegramMessage, MessageData as TelegramMessageData,
    TextEntity as TelegramTextEntity, Update as TelegramUpdate, UpdateType, User as TelegramUser,
};
use openplotva_core::{SENDER_TYPE_CHANNEL, SENDER_TYPE_SAME_CHAT, UserState};
use openplotva_storage::{ChatMemberRecord, ChatMemberUpsert};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY,
    StatelessJobItem, new_control_job_at,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMarkup, ReplyMessageRef, TextMessageRequest,
};
use thiserror::Error;
use time::OffsetDateTime;

use crate::virtual_messages::{
    QueueTextReport, QueueTextRequest, VirtualMessageStore, queue_text_message_parts,
};

const GROUP_SETTINGS_CONTROL_JOB_TITLE: &str = "group settings";
const NEW_MEMBERS_FOLLOWUP_CONTROL_JOB_TITLE: &str = "new members followup";
const GROUP_SETTINGS_WAIT_NOTICE_TEXT: &str = "⏳ Проверяю права и готовлю ссылку на настройки...";
const SETTINGS_SAME_CHAT_DECLINE_TEXT: &str = "❌ Невозможно подтвердить права владельца чата при отправке от имени чата.\n\nДля доступа к настройкам отправьте команду от имени владельца чата (не анонимно).";
const SETTINGS_CHANNEL_DECLINE_TEXT: &str = "❌ Сообщения от имени канала не могут быть проверены как владелец чата.\n\nДля доступа к настройкам отправьте команду от имени владельца чата (не анонимно).";
const SETTINGS_QUEUE_ERROR_TEXT: &str = "❌ Не удалось поставить задачу в очередь.";
const GROUP_SETTINGS_CHECK_FAILED_TEXT: &str = "❌ Не удалось проверить права. Попробуйте позже.";
const GROUP_SETTINGS_PERMISSION_DENIED_TEXT: &str =
    "❌ У вас нет прав для управления настройками этого чата.";
const GROUP_SETTINGS_OPEN_PRIVATE_TEXT: &str =
    "Откройте личный чат со мной, чтобы выбрать чат для настройки:";
const GROUP_SETTINGS_OPEN_BUTTON_TEXT: &str = "⚙️ Открыть настройки";
const NEW_MEMBERS_FOLLOWUP_UNBLOCK_TEXT: &str = "🚫 Мои сообщения были отключены в этом чате из-за предыдущих ограничений доступа.\n\nНажмите на кнопку ниже и откройте настройки, где можно будет включить мою отправку сообщений:";
const NEW_MEMBERS_FOLLOWUP_SETTINGS_BUTTON_TEXT: &str = "⚙️ Настройки";
const ADMIN_CHAT_SETTINGS_USAGE_TEXT: &str = "Usage: /admin_chat_settings [chat_id или @username]";
const ADMIN_CHAT_SETTINGS_WEBAPP_MISSING_TEXT: &str = "WebApp URL is not configured.";
const CHAT_ADMINS_CACHE_TTL: Duration = Duration::from_secs(30 * 60);

/// Boxed future returned by settings taskman assignment calls.
pub type SettingsControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by group settings executor permission checks.
pub type GroupSettingsControlJobFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by Go `syncChatAdmins` equivalents.
pub type GroupSettingsSyncFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Boxed future returned by new-members follow-up side effects.
pub type NewMembersFollowupFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed future returned by group settings member storage/API calls.
pub type GroupSettingsMemberFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by `/admin_chat_settings` target lookups.
pub type AdminChatTargetFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<AdminChatSettingsTarget, E>> + Send + 'a>>;

/// Queue boundary for Go settings-owned taskman control jobs.
pub trait SettingsControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Assign a control job to a named taskman queue.
    fn assign_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> SettingsControlJobQueueFuture<'a, Self::Error>;
}

/// Side-effect boundary for Go group settings control jobs.
pub trait GroupSettingsControlJobEffects {
    /// Error returned by permission checks.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Check Go `canOpenGroupSettings`.
    fn can_open_group_settings<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsControlJobFuture<'a, bool, Self::Error>;

    /// Run Go `syncChatAdmins`; it logs internally and does not affect the job result.
    fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> GroupSettingsSyncFuture<'a>;
}

/// Side-effect boundary for Go new-members follow-up control jobs.
pub trait NewMembersFollowupControlJobEffects {
    /// Run Go `enableChatCommunication`.
    fn enable_chat_communication<'a>(&'a self, chat_id: i64) -> NewMembersFollowupFuture<'a, ()>;

    /// Check Go `ai.IsChatBlocked` after communication is re-enabled.
    fn is_chat_blocked<'a>(&'a self, chat_id: i64) -> NewMembersFollowupFuture<'a, bool>;

    /// Run Go `trySendGreetingForJoinWave`.
    fn try_send_greeting_for_join_wave<'a>(
        &'a self,
        greeting: NewMembersGreeting,
    ) -> NewMembersFollowupFuture<'a, ()>;
}

/// Storage boundary for Go new-chat-member persistence.
pub trait NewMembersFollowupMemberStore {
    /// Error returned by concrete member storage.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Persist one Go `ChatMemberStatusMember` row for a newly joined member.
    fn upsert_new_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error>;
}

/// Storage boundary for Go `canOpenGroupSettings`.
pub trait GroupSettingsMemberStore {
    /// Error returned by concrete member storage.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load a cached chat-member row.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Option<ChatMemberRecord>, Self::Error>;

    /// Persist a freshly fetched chat-member row.
    fn upsert_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error>;
}

/// Telegram boundary for Go `getChatMember` permission probes.
pub trait GroupSettingsMemberApi {
    /// Error returned by concrete Telegram calls.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Fetch one chat-member from Telegram.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsMemberFuture<'a, TelegramChatMember, Self::Error>;
}

/// Storage boundary for Go `syncChatAdmins`.
pub trait GroupSettingsAdminSyncStore {
    /// Error returned by concrete admin-sync storage.
    type Error: fmt::Display + Send + Sync + 'static;

    /// List cached chat-member rows for Telegram API fallback.
    fn list_chat_members<'a>(
        &'a self,
        chat_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Vec<ChatMemberRecord>, Self::Error>;

    /// Persist a freshly fetched admin membership.
    fn upsert_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error>;

    /// Persist the admin user, matching Go `ensureUserPersistence`.
    fn upsert_user_state<'a>(
        &'a self,
        user: UserState,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error>;
}

/// Redis cache boundary for Go `syncChatAdmins`.
pub trait GroupSettingsAdminCache {
    /// Error returned by concrete admin-cache storage.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Persist the latest successful Telegram admin ID list.
    fn save_chat_admin_ids<'a>(
        &'a self,
        chat_id: i64,
        admin_ids: Vec<i64>,
        ttl: Duration,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error>;
}

/// Telegram boundary for Go `getChatAdministrators`.
pub trait GroupSettingsAdminsApi {
    /// Error returned by concrete Telegram calls.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Fetch chat administrators from Telegram.
    fn get_chat_administrators<'a>(
        &'a self,
        chat_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Vec<TelegramChatMember>, Self::Error>;
}

/// Telegram chat data needed by Go `/admin_chat_settings`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AdminChatSettingsTarget {
    /// Telegram chat ID.
    pub id: i64,
    /// Chat title, when Telegram returned one.
    pub title: String,
    /// Chat username without `@`, when Telegram returned one.
    pub username: String,
    /// First name for private-chat targets.
    pub first_name: String,
    /// Last name for private-chat targets.
    pub last_name: String,
}

/// Telegram boundary for Go `resolveChatTarget`.
pub trait AdminChatTargetResolver {
    /// Error returned by concrete Telegram calls.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Resolve a raw command target into Telegram chat metadata.
    fn resolve_admin_chat_target<'a>(
        &'a self,
        target_identifier: &'a str,
    ) -> AdminChatTargetFuture<'a, Self::Error>;
}

/// Error returned by the concrete Go `canOpenGroupSettings` port.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GroupSettingsPermissionCheckError {
    /// Go returns an error when the caller user ID is missing.
    #[error("missing caller user ID")]
    MissingCaller,
    /// Telegram permission probe failed.
    #[error("{0}")]
    Telegram(String),
}

/// Source used by a group-settings admin sync attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupSettingsAdminSyncSource {
    /// Go skips zero chat IDs before IO.
    #[default]
    Skipped,
    /// Administrators came from Telegram `getChatAdministrators`.
    Telegram,
    /// Telegram failed and stored admin rows were used.
    StoredFallback,
}

/// Testable report for Go `syncChatAdmins` side effects.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupSettingsAdminSyncReport {
    /// Source used for admin rows.
    pub source: GroupSettingsAdminSyncSource,
    /// Number of admin rows processed.
    pub admin_count: usize,
    /// Best-effort membership upsert failures.
    pub member_upsert_errors: usize,
    /// Best-effort user upsert failures.
    pub user_upsert_errors: usize,
    /// Best-effort Redis admin-list cache write failures.
    pub cache_errors: usize,
}

/// Error returned by the concrete Go `syncChatAdmins` port.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GroupSettingsAdminSyncError {
    /// Telegram failed and no stored admin fallback was available.
    #[error("{0}")]
    Telegram(String),
    /// Stored fallback failed after Telegram failed.
    #[error("failed to get chat members: {0}")]
    Storage(String),
}

/// Error returned by the concrete Go `resolveChatTarget` port.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum AdminChatTargetResolveError {
    /// Telegram `getChat` failed for a numeric chat ID.
    #[error("{0}")]
    Telegram(String),
    /// Telegram could not resolve a username target.
    #[error("unable to resolve chat {0}")]
    UnableToResolveChat(String),
}

/// Result of handling a decoded `/settings` update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SettingsCommandOutcome {
    /// The update was not a Telegram message carrying Go's `/settings` command.
    NotSettingsCommand,
    /// The command was not in a private chat; group handling is a later taskman slice.
    NonPrivateChat,
    /// Go logs and returns when `WEBAPP_URL` is blank.
    WebAppUrlMissing,
    /// The private settings link was queued through Go's text-send path.
    Queued(QueueTextReport),
}

/// Result of building Go's group `/settings` control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsControlJobBuild {
    /// The update was not a Telegram message carrying Go's `/settings` command.
    NotSettingsCommand,
    /// Private chats are handled by `handlePrivateSettingsCommand`.
    PrivateChat,
    /// Go declines anonymous admins sent as the chat itself.
    UnsupportedSameChatSender,
    /// Go declines linked-channel senders because owner rights cannot be checked.
    UnsupportedChannelSender,
    /// Go logs and returns when the caller user ID is absent.
    MissingCaller,
    /// Telegram identifiers did not fit the Go-shaped taskman payload.
    InvalidMessage,
    /// Go-shaped taskman job for the control queue.
    Job(Box<StatelessJobItem>),
}

/// Result of building Go's new-members follow-up control job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewMembersFollowupControlJobBuild {
    /// The update was not a Telegram message.
    NotMessage,
    /// The message had no `new_chat_members` service payload.
    NoNewChatMembers,
    /// Telegram identifiers did not fit the Go-shaped taskman payload.
    InvalidMessage,
    /// Go-shaped taskman job for the control queue.
    Job(Box<StatelessJobItem>),
}

/// Result of handling Go's group `/settings` command path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsCommandOutcome {
    /// The update was not a Telegram message carrying Go's `/settings` command.
    NotSettingsCommand,
    /// Private chats are handled by `handlePrivateSettingsCommand`.
    PrivateChat,
    /// Same-chat sender decline text was queued.
    UnsupportedSameChatSender(QueueTextReport),
    /// Channel sender decline text was queued.
    UnsupportedChannelSender(QueueTextReport),
    /// Go logs and returns when the caller user ID is absent.
    MissingCaller,
    /// Telegram identifiers did not fit the Go-shaped taskman payload.
    InvalidMessage,
    /// Assigning the control job failed; Go queues the failure text.
    QueueError(QueueTextReport),
    /// The control job was assigned and the wait notice was queued.
    Queued { notice: QueueTextReport },
}

/// Result of handling Go's new-members follow-up producer path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewMembersFollowupUpdateOutcome {
    /// The update was not a Telegram message.
    NotMessage,
    /// The message had no new-chat-member service payload.
    NoNewChatMembers,
    /// Telegram identifiers did not fit the Go-shaped taskman payload.
    InvalidMessage,
    /// Assigning the control job failed; Go queues the generic failure notice.
    QueueError {
        notice: QueueTextReport,
        member_upsert_errors: usize,
    },
    /// The control job was assigned. Go sends no success notice for this producer.
    Queued { member_upsert_errors: usize },
}

/// Inputs that identify one Go new-members producer pass.
#[derive(Clone, Copy, Debug)]
pub struct NewMembersFollowupUpdateInput<'a> {
    /// Decoded Telegram update.
    pub update: &'a TelegramUpdate,
    /// Current bot Telegram user ID, or zero when unavailable.
    pub bot_id: i64,
    /// Task creation time.
    pub created: OffsetDateTime,
}

/// Result of executing Go's group-settings control-job slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsControlJobOutcome {
    /// The job was not the group-settings control kind.
    UnsupportedKind(ControlKind),
    /// Permission check failed; Go sends a retry-later notice and returns the check error.
    PermissionCheckFailed(String),
    /// The caller is not allowed to manage group settings.
    PermissionDenied,
    /// Go returns an error before sync/send when the bot username is unavailable.
    BotUsernameMissing,
    /// Admin sync ran and the settings deep-link notice was queued.
    SentLink,
}

/// Control-message shape passed to the future Go greeting implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewMembersGreeting {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Trigger message ID.
    pub message_id: i32,
    /// Thread ID carried by taskman data, even though Go's reply helper omits it for control messages.
    pub thread_id: Option<i32>,
    pub new_chat_member_ids: Vec<i64>,
}

/// Result of executing Go's new-members follow-up control-job slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewMembersFollowupControlJobOutcome {
    /// The job was not the new-members follow-up control kind.
    UnsupportedKind(ControlKind),
    /// Greeting was attempted and the optional blocked-chat notice state is recorded.
    Completed { unblock_notice_queued: bool },
}

/// Result of handling Go's `/admin_chat_settings` command path after admin auth.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminChatSettingsCommandOutcome {
    /// The update was not a Telegram message carrying Go's admin chat settings command.
    NotAdminChatSettingsCommand,
    /// Go sent the usage text because no target chat was provided.
    Usage(QueueTextReport),
    /// Go sent the target resolution error text.
    ResolveError(QueueTextReport),
    /// Go sent the missing WebApp URL text.
    WebAppUrlMissing(QueueTextReport),
    /// The target settings button was queued.
    Queued(QueueTextReport),
}

/// Handle Go's private `/settings` command path.
pub async fn handle_private_settings_command_update<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    update: &TelegramUpdate,
    bot_username: &str,
    web_app_url: &str,
    next_virtual_id: NextId,
) -> Result<SettingsCommandOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let UpdateType::Message(message) = &update.update_type else {
        return Ok(SettingsCommandOutcome::NotSettingsCommand);
    };
    if !openplotva_updates::is_settings_command_message(message, bot_username) {
        return Ok(SettingsCommandOutcome::NotSettingsCommand);
    }
    if !matches!(message.chat, TelegramChat::Private(_)) {
        return Ok(SettingsCommandOutcome::NonPrivateChat);
    }
    if web_app_url.is_empty() {
        return Ok(SettingsCommandOutcome::WebAppUrlMissing);
    }

    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: false,
    };
    let user_id = message
        .sender
        .get_user()
        .map(|user| user.id.into())
        .unwrap_or_default();
    let url = openplotva_web::private_settings_web_app_url(web_app_url, user_id);
    let keyboard = openplotva_telegram::build_private_settings_keyboard(url);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: "Откройте настройки бота:".to_owned(),
        render_as: String::new(),
        reply_markup: Some(ReplyMarkup::InlineKeyboardMarkup(keyboard)),
    };
    let reply_to = ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: false,
        message_thread_id: 0,
    };
    let report = queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions: true,
            ephemeral_delete_after: None,
        },
        next_virtual_id,
    )
    .await?;

    Ok(SettingsCommandOutcome::Queued(report))
}

/// Handle Go's `/admin_chat_settings` command path after admin authorization.
pub async fn handle_admin_chat_settings_command_update<S, Resolver, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    resolver: &Resolver,
    update: &TelegramUpdate,
    web_app_url: &str,
    next_virtual_id: NextId,
) -> Result<AdminChatSettingsCommandOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Resolver: AdminChatTargetResolver + Sync,
    NextId: FnMut() -> String,
{
    let UpdateType::Message(message) = &update.update_type else {
        return Ok(AdminChatSettingsCommandOutcome::NotAdminChatSettingsCommand);
    };
    let Some(command) = admin_chat_settings_command_from_message(message) else {
        return Ok(AdminChatSettingsCommandOutcome::NotAdminChatSettingsCommand);
    };
    if !command.name.eq_ignore_ascii_case("admin_chat_settings") {
        return Ok(AdminChatSettingsCommandOutcome::NotAdminChatSettingsCommand);
    }

    let target_identifier = command.arguments.trim();
    if target_identifier.is_empty() {
        let report = queue_admin_chat_settings_text(
            store,
            dispatcher_queue,
            message,
            AdminChatSettingsText {
                text: ADMIN_CHAT_SETTINGS_USAGE_TEXT,
                reply_markup: None,
                bypass_chat_restrictions: false,
                immediate_first: false,
            },
            next_virtual_id,
        )
        .await?;
        return Ok(AdminChatSettingsCommandOutcome::Usage(report));
    }

    let target = match resolver.resolve_admin_chat_target(target_identifier).await {
        Ok(target) => target,
        Err(error) => {
            let text =
                format!("Could not find or access chat: {target_identifier}. Error: {error}");
            let report = queue_admin_chat_settings_text(
                store,
                dispatcher_queue,
                message,
                AdminChatSettingsText {
                    text: &text,
                    reply_markup: None,
                    bypass_chat_restrictions: false,
                    immediate_first: false,
                },
                next_virtual_id,
            )
            .await?;
            return Ok(AdminChatSettingsCommandOutcome::ResolveError(report));
        }
    };

    let Some(button_url) = openplotva_web::settings_button_url(web_app_url, target.id) else {
        let report = queue_admin_chat_settings_text(
            store,
            dispatcher_queue,
            message,
            AdminChatSettingsText {
                text: ADMIN_CHAT_SETTINGS_WEBAPP_MISSING_TEXT,
                reply_markup: None,
                bypass_chat_restrictions: false,
                immediate_first: false,
            },
            next_virtual_id,
        )
        .await?;
        return Ok(AdminChatSettingsCommandOutcome::WebAppUrlMissing(report));
    };

    let text = format!(
        "Откройте настройки для чата \"{}\" (ID: {}):",
        admin_chat_settings_display_title(&target),
        target.id
    );
    let keyboard = openplotva_telegram::build_private_settings_keyboard(button_url);
    let report = queue_admin_chat_settings_text(
        store,
        dispatcher_queue,
        message,
        AdminChatSettingsText {
            text: &text,
            reply_markup: Some(ReplyMarkup::InlineKeyboardMarkup(keyboard)),
            bypass_chat_restrictions: true,
            immediate_first: true,
        },
        next_virtual_id,
    )
    .await?;
    Ok(AdminChatSettingsCommandOutcome::Queued(report))
}

/// Execute Go's group-settings control-job behavior up to Telegram dispatch queueing.
pub async fn execute_group_settings_control_job_at<S, Effects, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    effects: &Effects,
    params: &ControlJobParams,
    bot_username: &str,
    next_virtual_id: NextId,
) -> Result<GroupSettingsControlJobOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Effects: GroupSettingsControlJobEffects + Sync,
    NextId: FnMut() -> String,
{
    if params.data.kind != ControlKind::GroupSettings {
        return Ok(GroupSettingsControlJobOutcome::UnsupportedKind(
            params.data.kind,
        ));
    }

    match effects
        .can_open_group_settings(params.chat_id, params.user_id)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            queue_group_settings_control_text(
                store,
                dispatcher_queue,
                params,
                GROUP_SETTINGS_PERMISSION_DENIED_TEXT,
                None,
                true,
                next_virtual_id,
            )
            .await?;
            return Ok(GroupSettingsControlJobOutcome::PermissionDenied);
        }
        Err(error) => {
            let error = error.to_string();
            queue_group_settings_control_text(
                store,
                dispatcher_queue,
                params,
                GROUP_SETTINGS_CHECK_FAILED_TEXT,
                None,
                true,
                next_virtual_id,
            )
            .await?;
            return Ok(GroupSettingsControlJobOutcome::PermissionCheckFailed(error));
        }
    }

    if bot_username.is_empty() {
        return Ok(GroupSettingsControlJobOutcome::BotUsernameMissing);
    }

    effects.sync_chat_admins(params.chat_id).await;

    let deep_link = format!("https://t.me/{bot_username}?start=settings");
    let button = openplotva_telegram::build_inline_keyboard_button_url(
        GROUP_SETTINGS_OPEN_BUTTON_TEXT,
        &deep_link,
    );
    queue_group_settings_control_text(
        store,
        dispatcher_queue,
        params,
        GROUP_SETTINGS_OPEN_PRIVATE_TEXT,
        Some(ReplyMarkup::from([[button]])),
        true,
        next_virtual_id,
    )
    .await?;
    Ok(GroupSettingsControlJobOutcome::SentLink)
}

/// Execute Go's new-members follow-up control-job behavior up to Telegram dispatch queueing.
pub async fn execute_new_members_followup_control_job_at<S, Effects, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    effects: &Effects,
    params: &ControlJobParams,
    bot_username: &str,
    next_virtual_id: NextId,
) -> Result<NewMembersFollowupControlJobOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Effects: NewMembersFollowupControlJobEffects + Sync,
    NextId: FnMut() -> String,
{
    if params.data.kind != ControlKind::NewMembersFollowup {
        return Ok(NewMembersFollowupControlJobOutcome::UnsupportedKind(
            params.data.kind,
        ));
    }

    let mut unblock_notice_queued = false;
    if params.data.bot_was_added {
        effects.enable_chat_communication(params.chat_id).await;
        if effects.is_chat_blocked(params.chat_id).await {
            let bot_url = if bot_username.is_empty() {
                String::new()
            } else {
                format!("https://t.me/{bot_username}?start=settings")
            };
            let button = openplotva_telegram::build_inline_keyboard_button_url(
                NEW_MEMBERS_FOLLOWUP_SETTINGS_BUTTON_TEXT,
                bot_url,
            );
            queue_new_members_followup_control_text(
                store,
                dispatcher_queue,
                params,
                Some(ReplyMarkup::from([[button]])),
                next_virtual_id,
            )
            .await?;
            unblock_notice_queued = true;
        }
    }

    effects
        .try_send_greeting_for_join_wave(new_members_greeting_from_control_params(params))
        .await;
    Ok(NewMembersFollowupControlJobOutcome::Completed {
        unblock_notice_queued,
    })
}

/// Build the Go taskman control job produced by group `/settings`.
#[must_use]
pub fn group_settings_control_job_from_update_at(
    update: &TelegramUpdate,
    bot_username: &str,
    created: OffsetDateTime,
) -> GroupSettingsControlJobBuild {
    let UpdateType::Message(message) = &update.update_type else {
        return GroupSettingsControlJobBuild::NotSettingsCommand;
    };
    group_settings_control_job_from_message_at(message, bot_username, created)
}

/// Build the Go taskman control job produced by `processNewChatMembers`.
#[must_use]
pub fn new_members_followup_control_job_from_update_at(
    update: &TelegramUpdate,
    bot_id: i64,
    created: OffsetDateTime,
) -> NewMembersFollowupControlJobBuild {
    let UpdateType::Message(message) = &update.update_type else {
        return NewMembersFollowupControlJobBuild::NotMessage;
    };
    new_members_followup_control_job_from_message_at(message, bot_id, created)
}

/// Handle Go's group `/settings` command path up to taskman assignment.
pub async fn handle_group_settings_command_update_at<S, Queue, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    control_queue: &Queue,
    update: &TelegramUpdate,
    bot_username: &str,
    created: OffsetDateTime,
    next_virtual_id: NextId,
) -> Result<GroupSettingsCommandOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Queue: SettingsControlJobQueue + Sync,
    NextId: FnMut() -> String,
{
    let UpdateType::Message(message) = &update.update_type else {
        return Ok(GroupSettingsCommandOutcome::NotSettingsCommand);
    };

    match group_settings_control_job_from_message_at(message, bot_username, created) {
        GroupSettingsControlJobBuild::NotSettingsCommand => {
            Ok(GroupSettingsCommandOutcome::NotSettingsCommand)
        }
        GroupSettingsControlJobBuild::PrivateChat => Ok(GroupSettingsCommandOutcome::PrivateChat),
        GroupSettingsControlJobBuild::UnsupportedSameChatSender => {
            let report = queue_group_settings_notice(
                store,
                dispatcher_queue,
                message,
                SETTINGS_SAME_CHAT_DECLINE_TEXT,
                true,
                None,
                next_virtual_id,
            )
            .await?;
            Ok(GroupSettingsCommandOutcome::UnsupportedSameChatSender(
                report,
            ))
        }
        GroupSettingsControlJobBuild::UnsupportedChannelSender => {
            let report = queue_group_settings_notice(
                store,
                dispatcher_queue,
                message,
                SETTINGS_CHANNEL_DECLINE_TEXT,
                true,
                None,
                next_virtual_id,
            )
            .await?;
            Ok(GroupSettingsCommandOutcome::UnsupportedChannelSender(
                report,
            ))
        }
        GroupSettingsControlJobBuild::MissingCaller => {
            Ok(GroupSettingsCommandOutcome::MissingCaller)
        }
        GroupSettingsControlJobBuild::InvalidMessage => {
            Ok(GroupSettingsCommandOutcome::InvalidMessage)
        }
        GroupSettingsControlJobBuild::Job(job) => {
            match control_queue
                .assign_settings_control_job(CONTROL_QUEUE_NAME, *job)
                .await
            {
                Ok(()) => {
                    let notice = queue_group_settings_notice(
                        store,
                        dispatcher_queue,
                        message,
                        GROUP_SETTINGS_WAIT_NOTICE_TEXT,
                        true,
                        Some(Duration::from_secs(60)),
                        next_virtual_id,
                    )
                    .await?;
                    Ok(GroupSettingsCommandOutcome::Queued { notice })
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to assign group settings control job");
                    let report = queue_group_settings_notice(
                        store,
                        dispatcher_queue,
                        message,
                        SETTINGS_QUEUE_ERROR_TEXT,
                        false,
                        Some(Duration::from_secs(60)),
                        next_virtual_id,
                    )
                    .await?;
                    Ok(GroupSettingsCommandOutcome::QueueError(report))
                }
            }
        }
    }
}

/// Handle Go `processNewChatMembers` up to taskman assignment.
pub async fn handle_new_members_followup_update_at<S, Members, Queue, NextId>(
    store: &S,
    dispatcher_queue: &DispatcherQueue,
    member_store: &Members,
    control_queue: &Queue,
    input: NewMembersFollowupUpdateInput<'_>,
    next_virtual_id: NextId,
) -> Result<NewMembersFollowupUpdateOutcome, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    Members: NewMembersFollowupMemberStore + Sync,
    Queue: SettingsControlJobQueue + Sync,
    NextId: FnMut() -> String,
{
    let UpdateType::Message(message) = &input.update.update_type else {
        return Ok(NewMembersFollowupUpdateOutcome::NotMessage);
    };

    match new_members_followup_control_job_from_message_at(message, input.bot_id, input.created) {
        NewMembersFollowupControlJobBuild::NotMessage => {
            Ok(NewMembersFollowupUpdateOutcome::NotMessage)
        }
        NewMembersFollowupControlJobBuild::NoNewChatMembers => {
            Ok(NewMembersFollowupUpdateOutcome::NoNewChatMembers)
        }
        NewMembersFollowupControlJobBuild::InvalidMessage => {
            Ok(NewMembersFollowupUpdateOutcome::InvalidMessage)
        }
        NewMembersFollowupControlJobBuild::Job(job) => {
            let member_upsert_errors =
                upsert_new_chat_members_from_message(member_store, message).await;
            match control_queue
                .assign_settings_control_job(CONTROL_QUEUE_NAME, *job)
                .await
            {
                Ok(()) => Ok(NewMembersFollowupUpdateOutcome::Queued {
                    member_upsert_errors,
                }),
                Err(error) => {
                    tracing::warn!(%error, "failed to assign new-members follow-up control job");
                    let notice = queue_group_settings_notice(
                        store,
                        dispatcher_queue,
                        message,
                        SETTINGS_QUEUE_ERROR_TEXT,
                        false,
                        Some(Duration::from_secs(60)),
                        next_virtual_id,
                    )
                    .await?;
                    Ok(NewMembersFollowupUpdateOutcome::QueueError {
                        notice,
                        member_upsert_errors,
                    })
                }
            }
        }
    }
}

fn group_settings_control_job_from_message_at(
    message: &TelegramMessage,
    bot_username: &str,
    created: OffsetDateTime,
) -> GroupSettingsControlJobBuild {
    if !openplotva_updates::is_settings_command_message(message, bot_username) {
        return GroupSettingsControlJobBuild::NotSettingsCommand;
    }
    if matches!(message.chat, TelegramChat::Private(_)) {
        return GroupSettingsControlJobBuild::PrivateChat;
    }

    let sender = openplotva_updates::resolve_message_sender(Some(message));
    match sender.sender_type.as_str() {
        SENDER_TYPE_SAME_CHAT => return GroupSettingsControlJobBuild::UnsupportedSameChatSender,
        SENDER_TYPE_CHANNEL => return GroupSettingsControlJobBuild::UnsupportedChannelSender,
        _ => {}
    }

    let Some(user) = message.sender.get_user() else {
        return GroupSettingsControlJobBuild::MissingCaller;
    };
    let user_id = i64::from(user.id);
    if user_id == 0 {
        return GroupSettingsControlJobBuild::MissingCaller;
    }

    let Ok(message_id) = i32::try_from(message.id) else {
        return GroupSettingsControlJobBuild::InvalidMessage;
    };
    let thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let mut data = control_data_from_settings_message(message);
    data.kind = ControlKind::GroupSettings;

    let user_full_name = user_full_name(user);
    let user_full_name = if user_full_name.trim().is_empty() {
        sender.display_name()
    } else {
        user_full_name
    };
    let params = ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id,
        user_id,
        user_full_name,
        thread_id,
        data,
    };
    GroupSettingsControlJobBuild::Job(Box::new(
        new_control_job_at(params, created)
            .with_name(GROUP_SETTINGS_CONTROL_JOB_TITLE)
            .with_priority(HIGH_PRIORITY),
    ))
}

fn new_members_followup_control_job_from_message_at(
    message: &TelegramMessage,
    bot_id: i64,
    created: OffsetDateTime,
) -> NewMembersFollowupControlJobBuild {
    let TelegramMessageData::NewChatMembers(members) = &message.data else {
        return NewMembersFollowupControlJobBuild::NoNewChatMembers;
    };
    if members.is_empty() {
        return NewMembersFollowupControlJobBuild::NoNewChatMembers;
    }

    let Ok(message_id) = i32::try_from(message.id) else {
        return NewMembersFollowupControlJobBuild::InvalidMessage;
    };
    let thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let sender = openplotva_updates::resolve_message_sender(Some(message));
    let user = message.sender.get_user();
    let user_id = if sender.id != 0 {
        sender.id
    } else {
        user.map(|user| i64::from(user.id)).unwrap_or_default()
    };
    let user_full_name = user.map(user_full_name).unwrap_or_default();
    let user_full_name = if user_full_name.trim().is_empty() {
        sender.display_name()
    } else {
        user_full_name
    };
    let mut data = control_data_from_settings_message(message);
    data.kind = ControlKind::NewMembersFollowup;
    data.bot_was_added = bot_id != 0 && members.iter().any(|member| i64::from(member.id) == bot_id);
    data.new_chat_member_ids = members.iter().map(|member| i64::from(member.id)).collect();

    let params = ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id,
        user_id,
        user_full_name,
        thread_id,
        data,
    };
    NewMembersFollowupControlJobBuild::Job(Box::new(
        new_control_job_at(params, created)
            .with_name(NEW_MEMBERS_FOLLOWUP_CONTROL_JOB_TITLE)
            .with_priority(HIGH_PRIORITY),
    ))
}

fn control_data_from_settings_message(message: &TelegramMessage) -> ControlJobData {
    let mut data = ControlJobData {
        chat_type: chat_type_name(&message.chat).to_owned(),
        ..ControlJobData::default()
    };
    if let Some(user) = message.sender.get_user() {
        data.user_name = user
            .username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        data.first_name = user.first_name.clone();
        data.last_name = user.last_name.clone().unwrap_or_default();
        data.language_code = user.language_code.clone().unwrap_or_default();
        data.is_premium = user.is_premium.unwrap_or_default();
    }
    data
}

async fn upsert_new_chat_members_from_message<Store>(
    store: &Store,
    message: &TelegramMessage,
) -> usize
where
    Store: NewMembersFollowupMemberStore + Sync,
{
    let TelegramMessageData::NewChatMembers(members) = &message.data else {
        return 0;
    };
    let chat_id = message.chat.get_id().into();
    let mut errors = 0;
    for member in members {
        let upsert = ChatMemberUpsert {
            chat_id,
            user_id: member.id.into(),
            status: openplotva_storage::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
            is_anonymous: Some(false),
            can_be_edited: Some(false),
            ..ChatMemberUpsert::default()
        };
        if store.upsert_new_chat_member(upsert).await.is_err() {
            errors += 1;
        }
    }
    errors
}

async fn queue_group_settings_notice<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    message: &TelegramMessage,
    text: &str,
    bypass_chat_restrictions: bool,
    ephemeral_delete_after: Option<Duration>,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = message_chat_ref(message);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as: String::new(),
        reply_markup: None,
    };
    let reply_to = ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions,
            ephemeral_delete_after,
        },
        next_virtual_id,
    )
    .await
}

async fn queue_group_settings_control_text<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    params: &ControlJobParams,
    text: &str,
    reply_markup: Option<ReplyMarkup>,
    bypass_chat_restrictions: bool,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = ChatRef {
        id: params.chat_id,
        is_forum: false,
    };
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.to_owned(),
        render_as: String::new(),
        reply_markup,
    };
    let reply_to = ReplyMessageRef {
        message_id: i64::from(params.message_id),
        chat,
        is_topic_message: false,
        message_thread_id: 0,
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions,
            ephemeral_delete_after: None,
        },
        next_virtual_id,
    )
    .await
}

async fn queue_new_members_followup_control_text<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    params: &ControlJobParams,
    reply_markup: Option<ReplyMarkup>,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = ChatRef {
        id: params.chat_id,
        is_forum: false,
    };
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: NEW_MEMBERS_FOLLOWUP_UNBLOCK_TEXT.to_owned(),
        render_as: String::new(),
        reply_markup,
    };
    let reply_to = ReplyMessageRef {
        message_id: i64::from(params.message_id),
        chat,
        is_topic_message: false,
        message_thread_id: 0,
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions: true,
            ephemeral_delete_after: None,
        },
        next_virtual_id,
    )
    .await
}

async fn queue_admin_chat_settings_text<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    message: &TelegramMessage,
    text: AdminChatSettingsText<'_>,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = message_chat_ref(message);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: text.text.to_owned(),
        render_as: String::new(),
        reply_markup: text.reply_markup,
    };
    let reply_to = ReplyMessageRef {
        message_id: message.id,
        chat,
        is_topic_message: message.message_thread_id.is_some(),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: text.immediate_first,
            bypass_chat_restrictions: text.bypass_chat_restrictions,
            ephemeral_delete_after: None,
        },
        next_virtual_id,
    )
    .await
}

struct AdminChatSettingsText<'a> {
    text: &'a str,
    reply_markup: Option<ReplyMarkup>,
    bypass_chat_restrictions: bool,
    immediate_first: bool,
}

fn new_members_greeting_from_control_params(params: &ControlJobParams) -> NewMembersGreeting {
    NewMembersGreeting {
        chat_id: params.chat_id,
        message_id: params.message_id,
        thread_id: params.thread_id,
        new_chat_member_ids: params.data.new_chat_member_ids.clone(),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdminChatSettingsCommand<'a> {
    name: &'a str,
    arguments: &'a str,
}

fn admin_chat_settings_command_from_message(
    message: &TelegramMessage,
) -> Option<AdminChatSettingsCommand<'_>> {
    let TelegramMessageData::Text(text) = &message.data else {
        return None;
    };
    let first = text.entities.as_ref()?.into_iter().next()?;
    let TelegramTextEntity::BotCommand(position) = first else {
        return None;
    };
    if position.offset != 0 {
        return None;
    }

    let command_end = utf16_index_to_byte_index(&text.data, position.length)?;
    let command_with_slash = text.data.get(..command_end)?;
    let command_with_at = command_with_slash.strip_prefix('/')?;
    let name = command_with_at
        .split_once('@')
        .map_or(command_with_at, |(name, _)| name);
    let arguments = command_arguments_after_command(&text.data, command_end)?;
    Some(AdminChatSettingsCommand { name, arguments })
}

fn command_arguments_after_command(text: &str, command_end: usize) -> Option<&str> {
    let after_command = text.get(command_end..)?;
    let Some(separator) = after_command.chars().next() else {
        return Some("");
    };
    after_command.get(separator.len_utf8()..)
}

fn utf16_index_to_byte_index(text: &str, utf16_units: u32) -> Option<usize> {
    let mut consumed = 0_u32;
    for (byte_index, ch) in text.char_indices() {
        if consumed == utf16_units {
            return Some(byte_index);
        }
        consumed = consumed.checked_add(u32::try_from(ch.len_utf16()).ok()?)?;
        if consumed > utf16_units {
            return None;
        }
    }
    if consumed == utf16_units {
        Some(text.len())
    } else {
        None
    }
}

fn admin_chat_settings_display_title(target: &AdminChatSettingsTarget) -> String {
    if !target.title.is_empty() {
        return target.title.clone();
    }
    if !target.username.is_empty() {
        return format!("@{}", target.username);
    }
    if !target.first_name.is_empty() {
        if !target.last_name.is_empty() {
            return format!("{} {}", target.first_name, target.last_name);
        }
        return target.first_name.clone();
    }
    target.id.to_string()
}

/// Go `canOpenGroupSettings` port using injected storage and Telegram boundaries.
pub async fn can_open_group_settings_with_sources<Store, Api>(
    store: &Store,
    telegram: &Api,
    chat_id: i64,
    user_id: i64,
) -> Result<bool, GroupSettingsPermissionCheckError>
where
    Store: GroupSettingsMemberStore + Sync,
    Api: GroupSettingsMemberApi + Sync,
{
    if user_id == 0 {
        return Err(GroupSettingsPermissionCheckError::MissingCaller);
    }

    match store.get_chat_member(chat_id, user_id).await {
        Ok(member)
            if openplotva_storage::stored_member_can_open_group_settings(member.as_ref()) =>
        {
            return Ok(true);
        }
        Ok(_) => {}
        Err(error) => {
            tracing::debug!(
                %error,
                chat_id,
                user_id,
                "failed to load cached caller membership; falling back to Telegram"
            );
        }
    }

    let member = telegram
        .get_chat_member(chat_id, user_id)
        .await
        .map_err(|error| GroupSettingsPermissionCheckError::Telegram(error.to_string()))?;
    let upsert = chat_member_upsert_from_telegram(chat_id, user_id, &member);
    if let Err(error) = store.upsert_chat_member(upsert).await {
        tracing::debug!(
            %error,
            chat_id,
            user_id,
            "failed to upsert caller membership from API"
        );
    }

    Ok(openplotva_telegram::telegram_member_can_open_group_settings(&member))
}

/// Build Go `chatMemberUpsertParams` from a `carapax` Telegram member.
#[must_use]
pub fn chat_member_upsert_from_telegram(
    chat_id: i64,
    user_id: i64,
    member: &TelegramChatMember,
) -> ChatMemberUpsert {
    let mut params = ChatMemberUpsert {
        chat_id,
        user_id,
        status: telegram_chat_member_status(member).to_owned(),
        is_anonymous: Some(telegram_chat_member_is_anonymous(member)),
        can_be_edited: Some(telegram_chat_member_can_be_edited(member)),
        ..ChatMemberUpsert::default()
    };
    apply_chat_member_role_permissions(&mut params, member);
    apply_chat_member_send_permissions(&mut params, member);
    params
}

/// Go `syncChatAdmins` and `getChatAdministrators` port with injectable boundaries.
pub async fn sync_chat_admins_with_sources<Store, Api>(
    store: &Store,
    telegram: &Api,
    chat_id: i64,
) -> Result<GroupSettingsAdminSyncReport, GroupSettingsAdminSyncError>
where
    Store: GroupSettingsAdminSyncStore + Sync,
    Api: GroupSettingsAdminsApi + Sync,
{
    sync_chat_admins_with_cache(store, telegram, &NoopGroupSettingsAdminCache, chat_id).await
}

/// Go `syncChatAdmins` and `getChatAdministrators` port with injectable Redis cache.
pub async fn sync_chat_admins_with_cache<Store, Api, Cache>(
    store: &Store,
    telegram: &Api,
    cache: &Cache,
    chat_id: i64,
) -> Result<GroupSettingsAdminSyncReport, GroupSettingsAdminSyncError>
where
    Store: GroupSettingsAdminSyncStore + Sync,
    Api: GroupSettingsAdminsApi + Sync,
    Cache: GroupSettingsAdminCache + Sync,
{
    if chat_id == 0 {
        return Ok(GroupSettingsAdminSyncReport {
            source: GroupSettingsAdminSyncSource::Skipped,
            ..GroupSettingsAdminSyncReport::default()
        });
    }

    match telegram.get_chat_administrators(chat_id).await {
        Ok(admins) => sync_api_admins(store, cache, chat_id, admins).await,
        Err(api_error) => {
            let api_error = api_error.to_string();
            let members = store
                .list_chat_members(chat_id)
                .await
                .map_err(|error| GroupSettingsAdminSyncError::Storage(error.to_string()))?;
            let admins = members
                .iter()
                .filter_map(openplotva_storage::stored_admin_chat_member)
                .collect::<Vec<_>>();
            if admins.is_empty() {
                return Err(GroupSettingsAdminSyncError::Telegram(api_error));
            }
            sync_stored_admins(store, admins).await
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct NoopGroupSettingsAdminCache;

impl GroupSettingsAdminCache for NoopGroupSettingsAdminCache {
    type Error = std::convert::Infallible;

    fn save_chat_admin_ids<'a>(
        &'a self,
        _chat_id: i64,
        _admin_ids: Vec<i64>,
        _ttl: Duration,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }
}

/// Build Go `adminChatMemberUpsertParams` from a `carapax` admin member.
#[must_use]
pub fn admin_chat_member_upsert_from_telegram(
    chat_id: i64,
    admin: &TelegramChatMember,
) -> Option<ChatMemberUpsert> {
    let (user, status, custom_title, is_anonymous, permissions) = match admin {
        TelegramChatMember::Administrator(admin) => (
            &admin.user,
            openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR,
            admin.custom_title.clone().unwrap_or_default(),
            admin.is_anonymous,
            AdminSyncPermissions {
                can_delete_messages: admin.can_delete_messages,
                can_manage_video_chats: admin.can_manage_video_chats,
                can_restrict_members: admin.can_restrict_members,
                can_promote_members: admin.can_promote_members,
                can_change_info: admin.can_change_info,
                can_invite_users: admin.can_invite_users,
                can_post_messages: admin.can_post_messages.unwrap_or_default(),
                can_edit_messages: admin.can_edit_messages.unwrap_or_default(),
                can_pin_messages: admin.can_pin_messages.unwrap_or_default(),
            },
        ),
        TelegramChatMember::Creator(creator) => (
            &creator.user,
            openplotva_storage::CHAT_MEMBER_STATUS_CREATOR,
            creator.custom_title.clone().unwrap_or_default(),
            false,
            AdminSyncPermissions::default(),
        ),
        TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => return None,
    };

    Some(ChatMemberUpsert {
        chat_id,
        user_id: i64::from(user.id),
        status: status.to_owned(),
        is_anonymous: Some(is_anonymous),
        custom_title: Some(custom_title),
        can_be_edited: Some(false),
        can_manage_chat: Some(true),
        can_delete_messages: Some(permissions.can_delete_messages),
        can_manage_video_chats: Some(permissions.can_manage_video_chats),
        can_restrict_members: Some(permissions.can_restrict_members),
        can_promote_members: Some(permissions.can_promote_members),
        can_change_info: Some(permissions.can_change_info),
        can_invite_users: Some(permissions.can_invite_users),
        can_post_messages: Some(permissions.can_post_messages),
        can_edit_messages: Some(permissions.can_edit_messages),
        can_pin_messages: Some(permissions.can_pin_messages),
        can_manage_topics: Some(false),
        ..ChatMemberUpsert::default()
    })
}

fn message_chat_ref(message: &TelegramMessage) -> ChatRef {
    ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    }
}

impl GroupSettingsMemberStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Option<ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.get_chat_member(chat_id, user_id).await })
    }

    fn upsert_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_chat_member(&member).await })
    }
}

impl NewMembersFollowupMemberStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_new_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_chat_member(&member).await })
    }
}

impl GroupSettingsMemberApi for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsMemberFuture<'a, TelegramChatMember, Self::Error> {
        Box::pin(async move {
            self.execute(openplotva_telegram::build_get_chat_member_method(
                chat_id, user_id,
            ))
            .await
        })
    }
}

impl GroupSettingsAdminSyncStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn list_chat_members<'a>(
        &'a self,
        chat_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Vec<ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.list_chat_members(chat_id).await })
    }

    fn upsert_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_chat_member(&member).await })
    }

    fn upsert_user_state<'a>(
        &'a self,
        user: UserState,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_user_state(&user).await })
    }
}

impl GroupSettingsAdminCache for openplotva_storage::RedisChatAdminCacheStore {
    type Error = openplotva_storage::StorageError;

    fn save_chat_admin_ids<'a>(
        &'a self,
        chat_id: i64,
        admin_ids: Vec<i64>,
        ttl: Duration,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error> {
        Box::pin(async move { self.set_chat_admin_ids(chat_id, &admin_ids, ttl).await })
    }
}

impl GroupSettingsAdminsApi for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn get_chat_administrators<'a>(
        &'a self,
        chat_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Vec<TelegramChatMember>, Self::Error> {
        Box::pin(async move {
            self.execute(openplotva_telegram::build_get_chat_administrators_method(
                chat_id,
            ))
            .await
        })
    }
}

impl AdminChatTargetResolver for openplotva_telegram::TelegramClient {
    type Error = AdminChatTargetResolveError;

    fn resolve_admin_chat_target<'a>(
        &'a self,
        target_identifier: &'a str,
    ) -> AdminChatTargetFuture<'a, Self::Error> {
        Box::pin(async move {
            let target_identifier = target_identifier.trim();
            if let Ok(chat_id) = target_identifier.parse::<i64>() {
                return self
                    .execute(openplotva_telegram::build_get_chat_method(chat_id))
                    .await
                    .map(admin_chat_settings_target_from_full_info)
                    .map_err(|error| AdminChatTargetResolveError::Telegram(error.to_string()));
            }

            let username = if target_identifier.starts_with('@') {
                target_identifier.to_owned()
            } else {
                format!("@{target_identifier}")
            };
            match self
                .execute(openplotva_telegram::build_get_chat_method(username.clone()))
                .await
            {
                Ok(chat) => Ok(admin_chat_settings_target_from_full_info(chat)),
                Err(_) => self
                    .execute(openplotva_telegram::build_get_chat_method(username))
                    .await
                    .map(admin_chat_settings_target_from_full_info)
                    .map_err(|_| {
                        AdminChatTargetResolveError::UnableToResolveChat(
                            target_identifier.to_owned(),
                        )
                    }),
            }
        })
    }
}

fn admin_chat_settings_target_from_full_info(
    chat: openplotva_telegram::ChatFullInfo,
) -> AdminChatSettingsTarget {
    AdminChatSettingsTarget {
        id: chat.id,
        title: chat.title.unwrap_or_default(),
        username: chat.username.unwrap_or_default(),
        first_name: chat.first_name.unwrap_or_default(),
        last_name: chat.last_name.unwrap_or_default(),
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AdminSyncPermissions {
    can_delete_messages: bool,
    can_manage_video_chats: bool,
    can_restrict_members: bool,
    can_promote_members: bool,
    can_change_info: bool,
    can_invite_users: bool,
    can_post_messages: bool,
    can_edit_messages: bool,
    can_pin_messages: bool,
}

async fn sync_api_admins<Store, Cache>(
    store: &Store,
    cache: &Cache,
    chat_id: i64,
    admins: Vec<TelegramChatMember>,
) -> Result<GroupSettingsAdminSyncReport, GroupSettingsAdminSyncError>
where
    Store: GroupSettingsAdminSyncStore + Sync,
    Cache: GroupSettingsAdminCache + Sync,
{
    let admin_ids = admins
        .iter()
        .map(|admin| i64::from(admin.get_user().id))
        .collect::<Vec<_>>();
    let mut report = GroupSettingsAdminSyncReport {
        source: GroupSettingsAdminSyncSource::Telegram,
        admin_count: admins.len(),
        ..GroupSettingsAdminSyncReport::default()
    };

    for admin in admins {
        if let Some(upsert) = admin_chat_member_upsert_from_telegram(chat_id, &admin)
            && GroupSettingsAdminSyncStore::upsert_chat_member(store, upsert)
                .await
                .is_err()
        {
            report.member_upsert_errors += 1;
        }
        if GroupSettingsAdminSyncStore::upsert_user_state(
            store,
            user_state_from_telegram_user(admin.get_user()),
        )
        .await
        .is_err()
        {
            report.user_upsert_errors += 1;
        }
    }
    if cache
        .save_chat_admin_ids(chat_id, admin_ids, CHAT_ADMINS_CACHE_TTL)
        .await
        .is_err()
    {
        report.cache_errors += 1;
    }

    Ok(report)
}

async fn sync_stored_admins<Store>(
    store: &Store,
    admins: Vec<openplotva_storage::StoredAdminChatMember>,
) -> Result<GroupSettingsAdminSyncReport, GroupSettingsAdminSyncError>
where
    Store: GroupSettingsAdminSyncStore + Sync,
{
    let mut report = GroupSettingsAdminSyncReport {
        source: GroupSettingsAdminSyncSource::StoredFallback,
        admin_count: admins.len(),
        ..GroupSettingsAdminSyncReport::default()
    };

    for admin in admins {
        if GroupSettingsAdminSyncStore::upsert_user_state(
            store,
            UserState::new(admin.user_id, "", None, None, None, None),
        )
        .await
        .is_err()
        {
            report.user_upsert_errors += 1;
        }
    }

    Ok(report)
}

fn user_state_from_telegram_user(user: &TelegramUser) -> UserState {
    UserState::new(
        i64::from(user.id),
        user.first_name.clone(),
        user.last_name.clone(),
        user.username.as_ref().map(ToString::to_string),
        user.language_code.clone(),
        user.is_premium,
    )
}

fn telegram_chat_member_status(member: &TelegramChatMember) -> &'static str {
    match member {
        TelegramChatMember::Administrator(_) => {
            openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
        }
        TelegramChatMember::Creator(_) => openplotva_storage::CHAT_MEMBER_STATUS_CREATOR,
        TelegramChatMember::Kicked(_) => openplotva_storage::CHAT_MEMBER_STATUS_KICKED,
        TelegramChatMember::Left(_) => openplotva_storage::CHAT_MEMBER_STATUS_LEFT,
        TelegramChatMember::Member { .. } => openplotva_storage::CHAT_MEMBER_STATUS_MEMBER,
        TelegramChatMember::Restricted(_) => "restricted",
    }
}

fn telegram_chat_member_is_anonymous(member: &TelegramChatMember) -> bool {
    match member {
        TelegramChatMember::Administrator(admin) => admin.is_anonymous,
        TelegramChatMember::Creator(creator) => creator.is_anonymous,
        TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => false,
    }
}

fn telegram_chat_member_can_be_edited(member: &TelegramChatMember) -> bool {
    match member {
        TelegramChatMember::Administrator(admin) => admin.can_be_edited,
        TelegramChatMember::Creator(_)
        | TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => false,
    }
}

fn apply_chat_member_role_permissions(params: &mut ChatMemberUpsert, member: &TelegramChatMember) {
    match member {
        TelegramChatMember::Creator(_) => apply_creator_chat_member_permissions(params),
        TelegramChatMember::Administrator(admin) => {
            params.can_delete_messages = Some(admin.can_delete_messages);
            params.can_manage_video_chats = Some(admin.can_manage_video_chats);
            params.can_restrict_members = Some(admin.can_restrict_members);
            params.can_promote_members = Some(admin.can_promote_members);
            params.can_change_info = Some(admin.can_change_info);
            params.can_invite_users = Some(admin.can_invite_users);
            params.can_post_messages = admin.can_post_messages;
            params.can_edit_messages = admin.can_edit_messages;
            params.can_pin_messages = admin.can_pin_messages;
            params.can_manage_topics = admin.can_manage_topics;
        }
        TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => {}
    }
}

fn apply_creator_chat_member_permissions(params: &mut ChatMemberUpsert) {
    params.can_promote_members = Some(true);
    params.can_delete_messages = Some(true);
    params.can_manage_video_chats = Some(true);
    params.can_restrict_members = Some(true);
    params.can_change_info = Some(true);
    params.can_invite_users = Some(true);
    params.can_post_messages = Some(true);
    params.can_edit_messages = Some(true);
    params.can_pin_messages = Some(true);
}

fn apply_chat_member_send_permissions(params: &mut ChatMemberUpsert, member: &TelegramChatMember) {
    let TelegramChatMember::Restricted(restricted) = member else {
        return;
    };
    set_bool_if_true(&mut params.can_send_messages, restricted.can_send_messages);
    set_bool_if_true(
        &mut params.can_send_media_messages,
        restricted_can_send_media_messages(restricted),
    );
    set_bool_if_true(&mut params.can_send_polls, restricted.can_send_polls);
    set_bool_if_true(
        &mut params.can_send_other_messages,
        restricted.can_send_other_messages,
    );
    set_bool_if_true(
        &mut params.can_add_web_page_previews,
        restricted.can_add_web_page_previews,
    );
}

fn restricted_can_send_media_messages(member: &ChatMemberRestricted) -> bool {
    member.can_send_audios
        && member.can_send_documents
        && member.can_send_photos
        && member.can_send_videos
        && member.can_send_video_notes
        && member.can_send_voice_notes
}

fn set_bool_if_true(target: &mut Option<bool>, value: bool) {
    if value {
        *target = Some(true);
    }
}

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Channel(_) => "channel",
        TelegramChat::Group(_) => "group",
        TelegramChat::Private(_) => "private",
        TelegramChat::Supergroup(_) => "supergroup",
    }
}

fn user_full_name(user: &TelegramUser) -> String {
    let name = format!(
        "{} {}",
        user.first_name,
        user.last_name.as_deref().unwrap_or_default()
    )
    .trim()
    .to_owned();
    if name.is_empty() {
        user.username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        future::Future,
        io,
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use carapax::types::{
        ChatMember, ChatMemberAdministrator, ChatMemberCreator, Update as TelegramUpdate, User,
    };
    use openplotva_core::{MessageIdMapping, UserState};
    use openplotva_storage::{ChatMemberRecord, ChatMemberUpsert};
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY, JobType,
        StatelessJobItem,
    };
    use openplotva_telegram::{DispatcherConfig, DispatcherQueue, TelegramOutboundMethodKind};
    use serde_json::{Value, json};
    use time::OffsetDateTime;

    use crate::virtual_messages::VirtualMessageStore;

    use super::{
        GroupSettingsAdminSyncSource, GroupSettingsCommandOutcome, GroupSettingsControlJobBuild,
        SettingsCommandOutcome, SettingsControlJobQueue, SettingsControlJobQueueFuture,
        admin_chat_member_upsert_from_telegram, can_open_group_settings_with_sources,
        chat_member_upsert_from_telegram, execute_group_settings_control_job_at,
        execute_new_members_followup_control_job_at, group_settings_control_job_from_update_at,
        handle_group_settings_command_update_at, handle_new_members_followup_update_at,
        handle_private_settings_command_update, new_members_followup_control_job_from_update_at,
        sync_chat_admins_with_cache, sync_chat_admins_with_sources,
    };

    #[tokio::test]
    async fn private_settings_command_queues_go_webapp_button_with_bypass_and_immediate()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = private_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &store,
            &queue,
            &update,
            "PlotvaBot",
            "https://plotva.example",
            || "settings-v1".to_owned(),
        )
        .await?;

        let SettingsCommandOutcome::Queued(report) = outcome else {
            return Err(format!("expected queued settings command, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(report.parts[0].virtual_id, "settings-v1");
        assert!(report.parts[0].immediate);
        assert_eq!(store.inserted(), vec![("settings-v1".to_owned(), 42, None)]);

        let item = queue
            .dequeue_immediate()
            .expect("private settings command should enqueue immediate text");
        assert!(item.bypasses_chat_restrictions());
        assert_eq!(item.ephemeral_delete_after(), None);
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        let method = item.into_method().expect("queued settings method");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(42));
        assert_eq!(value["text"], json!("Откройте настройки бота:"));
        assert_eq!(value["reply_parameters"]["message_id"], json!(77));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(42));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("⚙️ Настройки")
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/index.html?signature=780e28cf")
        );

        Ok(())
    }

    #[tokio::test]
    async fn private_settings_command_skips_blank_webapp_url_like_go() -> Result<(), Box<dyn Error>>
    {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = private_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &store,
            &queue,
            &update,
            "PlotvaBot",
            "",
            || "settings-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, SettingsCommandOutcome::WebAppUrlMissing);
        assert!(queue.snapshot().immediate.is_empty());
        assert!(store.inserted().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn private_settings_handler_leaves_group_settings_to_group_path()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = group_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &store,
            &queue,
            &update,
            "PlotvaBot",
            "https://plotva.example",
            || "settings-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, SettingsCommandOutcome::NonPrivateChat);
        assert!(queue.snapshot().immediate.is_empty());
        assert!(store.inserted().is_empty());
        Ok(())
    }

    #[test]
    fn group_settings_command_builds_go_control_job_payload() -> Result<(), Box<dyn Error>> {
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let GroupSettingsControlJobBuild::Job(job) =
            group_settings_control_job_from_update_at(&update, "PlotvaBot", created)
        else {
            return Err("expected group settings control job".into());
        };

        assert_eq!(job.title, "group settings");
        assert_eq!(job.created, created);
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.data.job_type, JobType::Control);

        let telegram_data = job.data.telegram_data.as_ref().expect("telegram metadata");
        assert_eq!(telegram_data.chat_id, -10042);
        assert_eq!(telegram_data.message_id, 78);
        assert_eq!(telegram_data.user_id, 42);
        assert_eq!(telegram_data.user_full_name, "Ada Lovelace");
        assert_eq!(telegram_data.thread_message_id, Some(99));
        assert_eq!(telegram_data.chat_title, "");

        let control_data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(control_data.kind, ControlKind::GroupSettings);
        assert_eq!(control_data.chat_type, "supergroup");
        assert_eq!(control_data.user_name, "ada_l");
        assert_eq!(control_data.first_name, "Ada");
        assert_eq!(control_data.last_name, "Lovelace");
        assert_eq!(control_data.language_code, "en");
        assert!(control_data.is_premium);

        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_assigns_control_job_and_sends_wait_notice()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
            &store,
            &dispatcher,
            &control_queue,
            &update,
            "PlotvaBot",
            created,
            || "group-settings-v1".to_owned(),
        )
        .await?;

        let GroupSettingsCommandOutcome::Queued { notice } = outcome else {
            return Err(format!("expected queued group settings command, got {outcome:?}").into());
        };
        assert_eq!(notice.enqueued_count(), 1);

        let assigned = control_queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "group settings");

        assert_eq!(
            store.inserted(),
            vec![("group-settings-v1".to_owned(), -10042, Some(0))]
        );
        let item = dispatcher
            .dequeue_immediate()
            .expect("group settings wait notice should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        assert_eq!(item.ephemeral_delete_after(), Some(Duration::from_secs(60)));
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        let method = item.into_method().expect("queued wait notice");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!("⏳ Проверяю права и готовлю ссылку на настройки...")
        );
        assert_eq!(value["message_thread_id"], json!(99));
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );

        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_declines_same_chat_sender_without_queueing_job()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = same_chat_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
            &store,
            &dispatcher,
            &control_queue,
            &update,
            "PlotvaBot",
            created,
            || "settings-decline-v1".to_owned(),
        )
        .await?;

        let GroupSettingsCommandOutcome::UnsupportedSameChatSender(report) = outcome else {
            return Err(format!("expected same-chat decline, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert!(control_queue.assigned().is_empty());

        let item = dispatcher
            .dequeue_immediate()
            .expect("same-chat decline should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        assert_eq!(item.ephemeral_delete_after(), None);
        let method = item.into_method().expect("queued same-chat decline");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!(
                "❌ Невозможно подтвердить права владельца чата при отправке от имени чата.\n\nДля доступа к настройкам отправьте команду от имени владельца чата (не анонимно)."
            )
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        assert!(store.inserted().len() == 1);
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_queue_error_sends_go_failure_notice()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::failing();
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
            &store,
            &dispatcher,
            &control_queue,
            &update,
            "PlotvaBot",
            created,
            || "settings-error-v1".to_owned(),
        )
        .await?;

        let GroupSettingsCommandOutcome::QueueError(report) = outcome else {
            return Err(format!("expected queue failure notice, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(control_queue.assigned().len(), 1);

        let item = dispatcher
            .dequeue_immediate()
            .expect("queue failure notice should enqueue immediately");
        assert!(!item.bypasses_chat_restrictions());
        assert_eq!(item.ephemeral_delete_after(), Some(Duration::from_secs(60)));
        let method = item.into_method().expect("queued failure notice");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!("❌ Не удалось поставить задачу в очередь.")
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        Ok(())
    }

    #[test]
    fn group_settings_command_without_caller_is_not_queued_like_go() -> Result<(), Box<dyn Error>> {
        let update = missing_caller_group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = group_settings_control_job_from_update_at(&update, "PlotvaBot", created);

        assert_eq!(outcome, GroupSettingsControlJobBuild::MissingCaller);
        Ok(())
    }

    #[tokio::test]
    async fn admin_chat_settings_command_queues_target_settings_button_with_bypass_and_immediate()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::with_target(super::AdminChatSettingsTarget {
            id: -100777,
            title: "Target Lab".to_owned(),
            username: "target_lab".to_owned(),
            first_name: String::new(),
            last_name: String::new(),
        });
        let update = admin_chat_settings_update("/admin_chat_settings @target_lab")?;

        let outcome = super::handle_admin_chat_settings_command_update(
            &store,
            &dispatcher,
            &resolver,
            &update,
            "https://plotva.example",
            || "admin-settings-v1".to_owned(),
        )
        .await?;

        let super::AdminChatSettingsCommandOutcome::Queued(report) = outcome else {
            return Err(format!("expected queued admin settings link, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(resolver.calls(), vec!["@target_lab".to_owned()]);
        assert_eq!(
            store.inserted(),
            vec![("admin-settings-v1".to_owned(), 42, None)]
        );

        let item = dispatcher
            .dequeue_immediate()
            .expect("admin chat settings button should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        assert_eq!(item.ephemeral_delete_after(), None);
        let method = item.into_method().expect("queued admin settings method");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(42));
        assert_eq!(
            value["text"],
            json!("Откройте настройки для чата \"Target Lab\" (ID: -100777):")
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(79));
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("⚙️ Настройки")
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/?chat_id=-100777&signature=b8e86493")
        );

        Ok(())
    }

    #[tokio::test]
    async fn admin_chat_settings_command_sends_usage_without_target() -> Result<(), Box<dyn Error>>
    {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::default();
        let update = admin_chat_settings_update("/admin_chat_settings   ")?;

        let outcome = super::handle_admin_chat_settings_command_update(
            &store,
            &dispatcher,
            &resolver,
            &update,
            "https://plotva.example",
            || "admin-settings-usage-v1".to_owned(),
        )
        .await?;

        let super::AdminChatSettingsCommandOutcome::Usage(report) = outcome else {
            return Err(format!("expected usage reply, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert!(resolver.calls().is_empty());

        let item = dispatcher
            .dequeue_regular()
            .expect("admin chat settings usage should use normal SendText queueing");
        assert!(!item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued usage method");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("Usage: /admin_chat_settings [chat_id или @username]")
        );
        Ok(())
    }

    #[tokio::test]
    async fn admin_chat_settings_command_reports_resolve_error_like_go()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::failing();
        let update = admin_chat_settings_update("/admin_chat_settings missing_chat")?;

        let outcome = super::handle_admin_chat_settings_command_update(
            &store,
            &dispatcher,
            &resolver,
            &update,
            "https://plotva.example",
            || "admin-settings-error-v1".to_owned(),
        )
        .await?;

        let super::AdminChatSettingsCommandOutcome::ResolveError(report) = outcome else {
            return Err(format!("expected resolve-error reply, got {outcome:?}").into());
        };
        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(resolver.calls(), vec!["missing_chat".to_owned()]);

        let item = dispatcher
            .dequeue_regular()
            .expect("admin chat settings resolve error should use normal SendText queueing");
        assert!(!item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued resolve error method");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("Could not find or access chat: missing_chat. Error: request failed")
        );
        Ok(())
    }

    #[test]
    fn telegram_client_implements_admin_chat_settings_target_resolver() {
        fn assert_impl<T: super::AdminChatTargetResolver>() {}
        assert_impl::<openplotva_telegram::TelegramClient>();
    }

    #[tokio::test]
    async fn group_settings_executor_syncs_admins_and_sends_deep_link_when_allowed()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::allowing();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "settings-link-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, super::GroupSettingsControlJobOutcome::SentLink);
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert_eq!(effects.synced_admin_chats(), vec![-10042]);
        assert_eq!(
            store.inserted(),
            vec![("settings-link-v1".to_owned(), -10042, None)]
        );

        let item = dispatcher
            .dequeue_immediate()
            .expect("settings deep-link should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued settings deep-link");
        let value = serde_json::to_value(method_as_value(method)?)?;

        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!("Откройте личный чат со мной, чтобы выбрать чат для настройки:")
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(78));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert!(value.get("message_thread_id").is_none());
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("⚙️ Открыть настройки")
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvaBot?start=settings")
        );

        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_sends_rights_decline_when_not_allowed()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::denying();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "settings-denied-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::PermissionDenied
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());

        let item = dispatcher
            .dequeue_immediate()
            .expect("settings rights decline should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued settings rights decline");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("❌ У вас нет прав для управления настройками этого чата.")
        );
        assert!(value.get("reply_markup").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_sends_check_failure_and_reports_error()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::failing_permission_check();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "settings-check-failed-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::PermissionCheckFailed(
                "request failed".to_owned()
            )
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());

        let item = dispatcher
            .dequeue_immediate()
            .expect("settings permission-check failure should enqueue immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued settings check failure");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("❌ Не удалось проверить права. Попробуйте позже.")
        );
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_executor_rejects_blank_bot_username_before_sync_or_send()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::allowing();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "",
            || "settings-link-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::BotUsernameMissing
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert!(store.inserted().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_executor_enables_chat_and_queues_blocked_notice_when_bot_added()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = NewMembersFollowupEffectsStub::blocked();
        let params = new_members_followup_control_params(true);

        let outcome = execute_new_members_followup_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "new-members-unblock-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::NewMembersFollowupControlJobOutcome::Completed {
                unblock_notice_queued: true
            }
        );
        assert_eq!(effects.enabled_chats(), vec![-10042]);
        assert_eq!(effects.blocked_checks(), vec![-10042]);
        assert_eq!(
            effects.greetings(),
            vec![super::NewMembersGreeting {
                chat_id: -10042,
                message_id: 88,
                thread_id: Some(99),
                new_chat_member_ids: vec![7, 8],
            }]
        );
        assert_eq!(
            store.inserted(),
            vec![("new-members-unblock-v1".to_owned(), -10042, None)]
        );

        let item = dispatcher
            .dequeue_immediate()
            .expect("blocked bot-added follow-up should enqueue the bypass notice immediately");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued new-members notice");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(
            value["text"],
            json!(
                "🚫 Мои сообщения были отключены в этом чате из-за предыдущих ограничений доступа.\n\nНажмите на кнопку ниже и откройте настройки, где можно будет включить мою отправку сообщений:"
            )
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(88));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert!(value.get("message_thread_id").is_none());
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["text"],
            json!("⚙️ Настройки")
        );
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("https://t.me/PlotvaBot?start=settings")
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_executor_skips_blocked_notice_when_bot_was_not_added()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = NewMembersFollowupEffectsStub::blocked();
        let params = new_members_followup_control_params(false);

        let outcome = execute_new_members_followup_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "PlotvaBot",
            || "unused-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::NewMembersFollowupControlJobOutcome::Completed {
                unblock_notice_queued: false
            }
        );
        assert!(effects.enabled_chats().is_empty());
        assert!(effects.blocked_checks().is_empty());
        assert_eq!(
            effects.greetings(),
            vec![super::NewMembersGreeting {
                chat_id: -10042,
                message_id: 88,
                thread_id: Some(99),
                new_chat_member_ids: vec![7, 8],
            }]
        );
        assert!(store.inserted().is_empty());
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert!(dispatcher.snapshot().regular.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_executor_keeps_empty_settings_url_when_username_missing()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = NewMembersFollowupEffectsStub::blocked();
        let params = new_members_followup_control_params(true);

        let outcome = execute_new_members_followup_control_job_at(
            &store,
            &dispatcher,
            &effects,
            &params,
            "",
            || "new-members-empty-url-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::NewMembersFollowupControlJobOutcome::Completed {
                unblock_notice_queued: true
            }
        );
        let item = dispatcher
            .dequeue_immediate()
            .expect("blocked bot-added follow-up should still enqueue with an empty URL");
        assert!(item.bypasses_chat_restrictions());
        let method = item.into_method().expect("queued new-members notice");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["reply_markup"]["inline_keyboard"][0][0]["url"],
            json!("")
        );
        Ok(())
    }

    #[test]
    fn new_members_followup_update_builds_go_control_job_payload() -> Result<(), Box<dyn Error>> {
        let update = new_members_update()?;
        let created = OffsetDateTime::UNIX_EPOCH;

        let build = new_members_followup_control_job_from_update_at(&update, 999, created);

        let super::NewMembersFollowupControlJobBuild::Job(job) = build else {
            return Err(format!("expected new-members control job, got {build:?}").into());
        };
        assert_eq!(job.title, "new members followup");
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.created, created);
        assert_eq!(job.data.job_type, JobType::Control);

        let telegram = job.data.telegram_data.as_ref().expect("telegram metadata");
        assert_eq!(telegram.chat_id, -10042);
        assert_eq!(telegram.message_id, 88);
        assert_eq!(telegram.user_id, 42);
        assert_eq!(telegram.user_full_name, "Ada Lovelace");
        assert_eq!(telegram.thread_message_id, Some(99));

        let data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(data.kind, ControlKind::NewMembersFollowup);
        assert_eq!(data.chat_type, "supergroup");
        assert_eq!(data.user_name, "ada_l");
        assert_eq!(data.first_name, "Ada");
        assert_eq!(data.last_name, "Lovelace");
        assert_eq!(data.language_code, "en");
        assert!(data.is_premium);
        assert!(data.bot_was_added);
        assert_eq!(data.new_chat_member_ids, vec![999, 7]);
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_update_upserts_members_and_assigns_control_job()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let member_store = GroupSettingsMemberStoreStub::default();
        let control_queue = SettingsControlJobQueueStub::default();
        let update = new_members_update()?;

        let outcome = handle_new_members_followup_update_at(
            &store,
            &dispatcher,
            &member_store,
            &control_queue,
            super::NewMembersFollowupUpdateInput {
                update: &update,
                bot_id: 999,
                created: OffsetDateTime::UNIX_EPOCH,
            },
            || "unused-v1".to_owned(),
        )
        .await?;

        assert_eq!(
            outcome,
            super::NewMembersFollowupUpdateOutcome::Queued {
                member_upsert_errors: 0
            }
        );
        let upserts = member_store.upserts();
        assert_eq!(upserts.len(), 2);
        assert_eq!(upserts[0].chat_id, -10042);
        assert_eq!(upserts[0].user_id, 999);
        assert_eq!(
            upserts[0].status,
            openplotva_storage::CHAT_MEMBER_STATUS_MEMBER
        );
        assert_eq!(upserts[0].is_anonymous, Some(false));
        assert_eq!(upserts[0].can_be_edited, Some(false));
        assert_eq!(upserts[1].user_id, 7);

        let assigned = control_queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "new members followup");
        assert!(store.inserted().is_empty());
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert!(dispatcher.snapshot().regular.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_update_queue_error_sends_go_failure_notice()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let member_store = GroupSettingsMemberStoreStub::default();
        let control_queue = SettingsControlJobQueueStub::failing();
        let update = new_members_update()?;

        let outcome = handle_new_members_followup_update_at(
            &store,
            &dispatcher,
            &member_store,
            &control_queue,
            super::NewMembersFollowupUpdateInput {
                update: &update,
                bot_id: 999,
                created: OffsetDateTime::UNIX_EPOCH,
            },
            || "new-members-queue-error-v1".to_owned(),
        )
        .await?;

        let super::NewMembersFollowupUpdateOutcome::QueueError {
            notice,
            member_upsert_errors,
        } = outcome
        else {
            return Err(format!("expected queue-error outcome, got {outcome:?}").into());
        };
        assert_eq!(member_upsert_errors, 0);
        assert_eq!(notice.enqueued_count(), 1);
        assert_eq!(
            store.inserted(),
            vec![("new-members-queue-error-v1".to_owned(), -10042, Some(0))]
        );
        let item = dispatcher
            .dequeue_immediate()
            .expect("new-members queue failure should enqueue ephemeral failure text");
        assert!(!item.bypasses_chat_restrictions());
        assert_eq!(item.ephemeral_delete_after(), Some(Duration::from_secs(60)));
        let method = item.into_method().expect("queued failure notice");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("❌ Не удалось поставить задачу в очередь.")
        );
        assert_eq!(value["message_thread_id"], json!(99));
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_permission_uses_stored_creator_without_telegram_call()
    -> Result<(), Box<dyn Error>> {
        let store = GroupSettingsMemberStoreStub::with_member(ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: openplotva_storage::CHAT_MEMBER_STATUS_CREATOR.to_owned(),
            ..ChatMemberRecord::default()
        });
        let telegram = GroupSettingsMemberApiStub::failing();

        let allowed = can_open_group_settings_with_sources(&store, &telegram, -10042, 42).await?;

        assert!(allowed);
        assert_eq!(store.get_calls(), vec![(-10042, 42)]);
        assert!(store.upserts().is_empty());
        assert!(telegram.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_permission_refreshes_denied_store_from_telegram_and_upserts_member()
    -> Result<(), Box<dyn Error>> {
        let store = GroupSettingsMemberStoreStub::with_member(ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(false),
            ..ChatMemberRecord::default()
        });
        let telegram = GroupSettingsMemberApiStub::with_member(promoting_admin_member(42));

        let allowed = can_open_group_settings_with_sources(&store, &telegram, -10042, 42).await?;

        assert!(allowed);
        assert_eq!(telegram.calls(), vec![(-10042, 42)]);
        let upserts = store.upserts();
        assert_eq!(upserts.len(), 1);
        let upsert = &upserts[0];
        assert_eq!(upsert.chat_id, -10042);
        assert_eq!(upsert.user_id, 42);
        assert_eq!(
            upsert.status,
            openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
        );
        assert_eq!(upsert.can_promote_members, Some(true));
        assert_eq!(upsert.can_delete_messages, Some(true));
        assert_eq!(upsert.can_send_media_messages, None);
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_permission_ignores_store_errors_but_reports_telegram_errors() {
        let store = GroupSettingsMemberStoreStub::failing_get();
        let telegram = GroupSettingsMemberApiStub::failing();

        let error = can_open_group_settings_with_sources(&store, &telegram, -10042, 42)
            .await
            .expect_err("Telegram failure should be surfaced");

        assert_eq!(error.to_string(), "request failed");
        assert_eq!(store.get_calls(), vec![(-10042, 42)]);
        assert_eq!(telegram.calls(), vec![(-10042, 42)]);
    }

    #[tokio::test]
    async fn group_settings_permission_rejects_missing_caller_before_io() {
        let store = GroupSettingsMemberStoreStub::default();
        let telegram = GroupSettingsMemberApiStub::with_member(promoting_admin_member(42));

        let error = can_open_group_settings_with_sources(&store, &telegram, -10042, 0)
            .await
            .expect_err("missing caller should be rejected");

        assert_eq!(error.to_string(), "missing caller user ID");
        assert!(store.get_calls().is_empty());
        assert!(telegram.calls().is_empty());
    }

    #[test]
    fn chat_member_upsert_from_telegram_preserves_go_permission_semantics() {
        let creator = chat_member_upsert_from_telegram(
            -10042,
            42,
            &ChatMember::Creator(ChatMemberCreator::new(User::new(42, "Ada", false))),
        );
        assert_eq!(creator.can_promote_members, Some(true));
        assert_eq!(creator.can_delete_messages, Some(true));
        assert_eq!(creator.can_manage_video_chats, Some(true));
        assert_eq!(creator.can_send_media_messages, None);

        let admin = chat_member_upsert_from_telegram(-10042, 42, &promoting_admin_member(42));
        assert_eq!(
            admin.status,
            openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
        );
        assert_eq!(admin.can_promote_members, Some(true));
        assert_eq!(admin.can_delete_messages, Some(true));
        assert_eq!(admin.can_manage_topics, Some(true));
        assert_eq!(admin.can_send_messages, None);
        assert_eq!(admin.can_send_media_messages, None);

        let restricted = chat_member_upsert_from_telegram(
            -10042,
            42,
            &restricted_member_with_send_permissions(42),
        );
        assert_eq!(restricted.status, "restricted");
        assert_eq!(restricted.can_send_messages, Some(true));
        assert_eq!(restricted.can_send_media_messages, Some(true));
        assert_eq!(restricted.can_send_polls, Some(true));
        assert_eq!(restricted.can_send_other_messages, Some(true));
        assert_eq!(restricted.can_add_web_page_previews, Some(true));
    }

    #[test]
    fn admin_chat_member_upsert_from_telegram_preserves_go_admin_sync_semantics() {
        let admin = admin_chat_member_upsert_from_telegram(-10042, &promoting_admin_member(42))
            .expect("administrator should map to an admin upsert");
        assert_eq!(admin.chat_id, -10042);
        assert_eq!(admin.user_id, 42);
        assert_eq!(
            admin.status,
            openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
        );
        assert_eq!(admin.is_anonymous, Some(false));
        assert_eq!(admin.custom_title.as_deref(), Some(""));
        assert_eq!(admin.can_be_edited, Some(false));
        assert_eq!(admin.can_manage_chat, Some(true));
        assert_eq!(admin.can_promote_members, Some(true));
        assert_eq!(admin.can_manage_topics, Some(false));

        let creator = admin_chat_member_upsert_from_telegram(-10042, &creator_member(43))
            .expect("creator should map to an admin upsert");
        assert_eq!(
            creator.status,
            openplotva_storage::CHAT_MEMBER_STATUS_CREATOR
        );
        assert_eq!(creator.is_anonymous, Some(false));
        assert_eq!(creator.can_manage_chat, Some(true));
        assert_eq!(creator.can_promote_members, Some(false));

        let member = ChatMember::Member {
            user: User::new(44, "Linus", false),
            tag: None,
            until_date: None,
        };
        assert!(admin_chat_member_upsert_from_telegram(-10042, &member).is_none());
    }

    #[tokio::test]
    async fn group_settings_admin_sync_upserts_api_admins_and_users() -> Result<(), Box<dyn Error>>
    {
        let store = GroupSettingsAdminSyncStoreStub::default();
        let api = GroupSettingsAdminsApiStub::with_admins(vec![
            promoting_admin_member(42),
            creator_member(43),
        ]);

        let report = sync_chat_admins_with_sources(&store, &api, -10042).await?;

        assert_eq!(report.source, GroupSettingsAdminSyncSource::Telegram);
        assert_eq!(report.admin_count, 2);
        assert_eq!(report.member_upsert_errors, 0);
        assert_eq!(report.user_upsert_errors, 0);
        assert_eq!(api.calls(), vec![-10042]);
        assert!(store.list_calls().is_empty());

        let upserts = store.upserts();
        assert_eq!(upserts.len(), 2);
        assert_eq!(upserts[0].user_id, 42);
        assert_eq!(upserts[0].can_manage_chat, Some(true));
        assert_eq!(upserts[0].can_manage_topics, Some(false));
        assert_eq!(upserts[1].user_id, 43);

        let users = store.users();
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].id, 42);
        assert_eq!(users[0].first_name, "Ada");
        assert_eq!(users[1].id, 43);
        assert_eq!(users[1].first_name, "Grace");
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_admin_sync_caches_api_admin_ids_with_go_ttl()
    -> Result<(), Box<dyn Error>> {
        let store = GroupSettingsAdminSyncStoreStub::default();
        let api = GroupSettingsAdminsApiStub::with_admins(vec![
            promoting_admin_member(42),
            creator_member(43),
        ]);
        let cache = GroupSettingsAdminCacheStub::default();

        let report = sync_chat_admins_with_cache(&store, &api, &cache, -10042).await?;

        assert_eq!(report.source, GroupSettingsAdminSyncSource::Telegram);
        assert_eq!(report.cache_errors, 0);
        assert_eq!(
            cache.saves(),
            vec![(-10042, vec![42, 43], Duration::from_secs(30 * 60))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_admin_sync_keeps_cache_failures_nonfatal_like_go()
    -> Result<(), Box<dyn Error>> {
        let store = GroupSettingsAdminSyncStoreStub::default();
        let api = GroupSettingsAdminsApiStub::with_admins(vec![promoting_admin_member(42)]);
        let cache = GroupSettingsAdminCacheStub::failing();

        let report = sync_chat_admins_with_cache(&store, &api, &cache, -10042).await?;

        assert_eq!(report.source, GroupSettingsAdminSyncSource::Telegram);
        assert_eq!(report.admin_count, 1);
        assert_eq!(report.cache_errors, 1);
        assert_eq!(
            cache.saves(),
            vec![(-10042, vec![42], Duration::from_secs(30 * 60))]
        );
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_admin_sync_falls_back_to_stored_admins_after_api_error()
    -> Result<(), Box<dyn Error>> {
        let store = GroupSettingsAdminSyncStoreStub::with_members(vec![
            ChatMemberRecord {
                chat_id: -10042,
                user_id: 42,
                status: openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
                can_promote_members: Some(true),
                ..ChatMemberRecord::default()
            },
            ChatMemberRecord {
                chat_id: -10042,
                user_id: 44,
                status: openplotva_storage::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
                ..ChatMemberRecord::default()
            },
        ]);
        let api = GroupSettingsAdminsApiStub::failing();

        let report = sync_chat_admins_with_sources(&store, &api, -10042).await?;

        assert_eq!(report.source, GroupSettingsAdminSyncSource::StoredFallback);
        assert_eq!(report.admin_count, 1);
        assert_eq!(api.calls(), vec![-10042]);
        assert_eq!(store.list_calls(), vec![-10042]);
        assert!(store.upserts().is_empty());

        let cache_store = GroupSettingsAdminSyncStoreStub::with_members(vec![ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
            can_promote_members: Some(true),
            ..ChatMemberRecord::default()
        }]);
        let cache_api = GroupSettingsAdminsApiStub::failing();
        let cache = GroupSettingsAdminCacheStub::default();
        let _ = sync_chat_admins_with_cache(&cache_store, &cache_api, &cache, -10042).await?;
        assert!(
            cache.saves().is_empty(),
            "Go caches admin IDs only after an API success, not DB fallback"
        );

        let users = store.users();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].id, 42);
        assert_eq!(users[0].first_name, "");
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_admin_sync_returns_api_error_when_no_stored_admins() {
        let store = GroupSettingsAdminSyncStoreStub::with_members(Vec::new());
        let api = GroupSettingsAdminsApiStub::failing();

        let error = sync_chat_admins_with_sources(&store, &api, -10042)
            .await
            .expect_err("API failure with no stored admins should be reported");

        assert_eq!(error.to_string(), "request failed");
        assert_eq!(store.list_calls(), vec![-10042]);
    }

    #[tokio::test]
    async fn group_settings_admin_sync_skips_zero_chat_without_io() -> Result<(), Box<dyn Error>> {
        let store = GroupSettingsAdminSyncStoreStub::default();
        let api = GroupSettingsAdminsApiStub::with_admins(vec![promoting_admin_member(42)]);

        let report = sync_chat_admins_with_sources(&store, &api, 0).await?;

        assert_eq!(report.source, GroupSettingsAdminSyncSource::Skipped);
        assert_eq!(report.admin_count, 0);
        assert!(api.calls().is_empty());
        assert!(store.list_calls().is_empty());
        assert!(store.upserts().is_empty());
        assert!(store.users().is_empty());
        Ok(())
    }

    type RecordedVirtualInsert = (String, i64, Option<i32>);

    #[derive(Clone, Default)]
    struct StoreStub {
        inserted: Arc<Mutex<Vec<RecordedVirtualInsert>>>,
    }

    impl StoreStub {
        fn inserted(&self) -> Vec<RecordedVirtualInsert> {
            self.inserted.lock().expect("inserted virtual ids").clone()
        }
    }

    impl VirtualMessageStore for StoreStub {
        type Error = io::Error;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<MessageIdMapping>, Self::Error>> + Send + 'a>>
        {
            Box::pin(async { Ok(None) })
        }

        fn insert_virtual_message<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            thread_id: Option<i32>,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async move {
                self.inserted
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?
                    .push((vmsg_id, chat_id, thread_id));
                Ok(())
            })
        }

        fn resolve_virtual_message<'a>(
            &'a self,
            _vmsg_id: String,
            _real_message_id: i32,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn enqueue_message_op<'a>(
            &'a self,
            _vmsg_id: String,
            _chat_id: i64,
            _op: &'static str,
            _payload_json: Option<String>,
        ) -> Pin<Box<dyn Future<Output = Result<i64, Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(1) })
        }

        fn delete_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn method_as_value(method: openplotva_telegram::TelegramOutboundMethod) -> io::Result<Value> {
        match method {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => {
                serde_json::to_value(method.as_ref()).map_err(io::Error::other)
            }
            other => Err(io::Error::other(format!(
                "unexpected Telegram method: {}",
                other.method_name()
            ))),
        }
    }

    fn private_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12345,
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
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": "/settings",
                "entities": [
                    {
                        "offset": 0,
                        "length": 9,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn admin_chat_settings_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        let command_len = text
            .split_whitespace()
            .next()
            .map(|command| command.encode_utf16().count())
            .unwrap_or_default();
        serde_json::from_value(json!({
            "update_id": 12349,
            "message": {
                "message_id": 79,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": text,
                "entities": [
                    {
                        "offset": 0,
                        "length": command_len,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn group_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12346,
            "message": {
                "message_id": 78,
                "message_thread_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "username": "ada_l",
                    "language_code": "en",
                    "is_premium": true
                },
                "text": "/settings@PlotvaBot",
                "entities": [
                    {
                        "offset": 0,
                        "length": 19,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn new_members_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12350,
            "message": {
                "message_id": 88,
                "message_thread_id": 99,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "username": "ada_l",
                    "language_code": "en",
                    "is_premium": true
                },
                "new_chat_members": [
                    {
                        "id": 999,
                        "is_bot": true,
                        "first_name": "PlotvaBot"
                    },
                    {
                        "id": 7,
                        "is_bot": false,
                        "first_name": "Grace"
                    }
                ]
            }
        }))
    }

    fn same_chat_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12347,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab",
                    "username": "plotva_lab"
                },
                "sender_chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab",
                    "username": "plotva_lab"
                },
                "text": "/settings@PlotvaBot",
                "entities": [
                    {
                        "offset": 0,
                        "length": 19,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn missing_caller_group_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12348,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "text": "/settings@PlotvaBot",
                "entities": [
                    {
                        "offset": 0,
                        "length": 19,
                        "type": "bot_command"
                    }
                ]
            }
        }))
    }

    fn group_settings_control_params() -> ControlJobParams {
        ControlJobParams {
            chat_id: -10042,
            message_id: 78,
            user_id: 42,
            user_full_name: "Ada Lovelace".to_owned(),
            thread_id: Some(99),
            data: ControlJobData {
                kind: ControlKind::GroupSettings,
                chat_type: "supergroup".to_owned(),
                user_name: "ada_l".to_owned(),
                first_name: "Ada".to_owned(),
                last_name: "Lovelace".to_owned(),
                language_code: "en".to_owned(),
                is_premium: true,
                ..ControlJobData::default()
            },
        }
    }

    fn new_members_followup_control_params(bot_was_added: bool) -> ControlJobParams {
        ControlJobParams {
            chat_id: -10042,
            message_id: 88,
            user_id: 42,
            user_full_name: "Ada Lovelace".to_owned(),
            thread_id: Some(99),
            data: ControlJobData {
                kind: ControlKind::NewMembersFollowup,
                chat_type: "supergroup".to_owned(),
                new_chat_member_ids: vec![7, 8],
                bot_was_added,
                ..ControlJobData::default()
            },
        }
    }

    #[derive(Clone, Default)]
    struct AdminChatTargetResolverStub {
        state: Arc<Mutex<AdminChatTargetResolverState>>,
    }

    #[derive(Default)]
    struct AdminChatTargetResolverState {
        target: Option<super::AdminChatSettingsTarget>,
        fail: bool,
        calls: Vec<String>,
    }

    impl AdminChatTargetResolverStub {
        fn with_target(target: super::AdminChatSettingsTarget) -> Self {
            Self {
                state: Arc::new(Mutex::new(AdminChatTargetResolverState {
                    target: Some(target),
                    ..AdminChatTargetResolverState::default()
                })),
            }
        }

        fn failing() -> Self {
            Self {
                state: Arc::new(Mutex::new(AdminChatTargetResolverState {
                    fail: true,
                    ..AdminChatTargetResolverState::default()
                })),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.state
                .lock()
                .expect("admin chat target resolver state")
                .calls
                .clone()
        }
    }

    impl super::AdminChatTargetResolver for AdminChatTargetResolverStub {
        type Error = StubError;

        fn resolve_admin_chat_target<'a>(
            &'a self,
            target_identifier: &'a str,
        ) -> super::AdminChatTargetFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("admin chat target resolver state");
                state.calls.push(target_identifier.to_owned());
                if state.fail {
                    return Err(StubError);
                }
                state.target.clone().ok_or(StubError)
            })
        }
    }

    #[derive(Clone, Default)]
    struct SettingsControlJobQueueStub {
        state: Arc<Mutex<SettingsControlJobQueueState>>,
    }

    #[derive(Default)]
    struct SettingsControlJobQueueState {
        assigned: Vec<(&'static str, StatelessJobItem)>,
        error: Option<StubError>,
    }

    impl SettingsControlJobQueueStub {
        fn failing() -> Self {
            Self {
                state: Arc::new(Mutex::new(SettingsControlJobQueueState {
                    error: Some(StubError),
                    ..SettingsControlJobQueueState::default()
                })),
            }
        }

        fn assigned(&self) -> Vec<(&'static str, StatelessJobItem)> {
            self.state
                .lock()
                .expect("settings control queue state")
                .assigned
                .clone()
        }
    }

    impl SettingsControlJobQueue for SettingsControlJobQueueStub {
        type Error = StubError;

        fn assign_settings_control_job<'a>(
            &'a self,
            queue_name: &'static str,
            job: StatelessJobItem,
        ) -> SettingsControlJobQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("settings control queue state");
                state.assigned.push((queue_name, job));
                match state.error.take() {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            })
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct StubError;

    impl fmt::Display for StubError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("request failed")
        }
    }

    #[derive(Clone)]
    struct GroupSettingsEffectsStub {
        state: Arc<Mutex<GroupSettingsEffectsState>>,
    }

    #[derive(Default)]
    struct GroupSettingsEffectsState {
        allow: bool,
        fail_permission_check: bool,
        permission_checks: Vec<(i64, i64)>,
        synced_admin_chats: Vec<i64>,
    }

    impl GroupSettingsEffectsStub {
        fn allowing() -> Self {
            Self::with_state(GroupSettingsEffectsState {
                allow: true,
                ..GroupSettingsEffectsState::default()
            })
        }

        fn denying() -> Self {
            Self::with_state(GroupSettingsEffectsState::default())
        }

        fn failing_permission_check() -> Self {
            Self::with_state(GroupSettingsEffectsState {
                fail_permission_check: true,
                ..GroupSettingsEffectsState::default()
            })
        }

        fn with_state(state: GroupSettingsEffectsState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn permission_checks(&self) -> Vec<(i64, i64)> {
            self.state
                .lock()
                .expect("group settings effects state")
                .permission_checks
                .clone()
        }

        fn synced_admin_chats(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("group settings effects state")
                .synced_admin_chats
                .clone()
        }
    }

    impl super::GroupSettingsControlJobEffects for GroupSettingsEffectsStub {
        type Error = StubError;

        fn can_open_group_settings<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::GroupSettingsControlJobFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("group settings effects state");
                state.permission_checks.push((chat_id, user_id));
                if state.fail_permission_check {
                    Err(StubError)
                } else {
                    Ok(state.allow)
                }
            })
        }

        fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> super::GroupSettingsSyncFuture<'a> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("group settings effects state")
                    .synced_admin_chats
                    .push(chat_id);
            })
        }
    }

    #[derive(Clone)]
    struct NewMembersFollowupEffectsStub {
        state: Arc<Mutex<NewMembersFollowupEffectsState>>,
    }

    #[derive(Default)]
    struct NewMembersFollowupEffectsState {
        chat_blocked: bool,
        enabled_chats: Vec<i64>,
        blocked_checks: Vec<i64>,
        greetings: Vec<super::NewMembersGreeting>,
    }

    impl NewMembersFollowupEffectsStub {
        fn blocked() -> Self {
            Self {
                state: Arc::new(Mutex::new(NewMembersFollowupEffectsState {
                    chat_blocked: true,
                    ..NewMembersFollowupEffectsState::default()
                })),
            }
        }

        fn enabled_chats(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("new-members followup effects state")
                .enabled_chats
                .clone()
        }

        fn blocked_checks(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("new-members followup effects state")
                .blocked_checks
                .clone()
        }

        fn greetings(&self) -> Vec<super::NewMembersGreeting> {
            self.state
                .lock()
                .expect("new-members followup effects state")
                .greetings
                .clone()
        }
    }

    impl super::NewMembersFollowupControlJobEffects for NewMembersFollowupEffectsStub {
        fn enable_chat_communication<'a>(
            &'a self,
            chat_id: i64,
        ) -> super::NewMembersFollowupFuture<'a, ()> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("new-members followup effects state")
                    .enabled_chats
                    .push(chat_id);
            })
        }

        fn is_chat_blocked<'a>(
            &'a self,
            chat_id: i64,
        ) -> super::NewMembersFollowupFuture<'a, bool> {
            Box::pin(async move {
                let mut state = self
                    .state
                    .lock()
                    .expect("new-members followup effects state");
                state.blocked_checks.push(chat_id);
                state.chat_blocked
            })
        }

        fn try_send_greeting_for_join_wave<'a>(
            &'a self,
            greeting: super::NewMembersGreeting,
        ) -> super::NewMembersFollowupFuture<'a, ()> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("new-members followup effects state")
                    .greetings
                    .push(greeting);
            })
        }
    }

    #[derive(Clone, Default)]
    struct GroupSettingsMemberStoreStub {
        state: Arc<Mutex<GroupSettingsMemberStoreState>>,
    }

    #[derive(Default)]
    struct GroupSettingsMemberStoreState {
        member: Option<ChatMemberRecord>,
        fail_get: bool,
        get_calls: Vec<(i64, i64)>,
        upserts: Vec<ChatMemberUpsert>,
    }

    impl GroupSettingsMemberStoreStub {
        fn with_member(member: ChatMemberRecord) -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsMemberStoreState {
                    member: Some(member),
                    ..GroupSettingsMemberStoreState::default()
                })),
            }
        }

        fn failing_get() -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsMemberStoreState {
                    fail_get: true,
                    ..GroupSettingsMemberStoreState::default()
                })),
            }
        }

        fn get_calls(&self) -> Vec<(i64, i64)> {
            self.state
                .lock()
                .expect("group settings member store state")
                .get_calls
                .clone()
        }

        fn upserts(&self) -> Vec<ChatMemberUpsert> {
            self.state
                .lock()
                .expect("group settings member store state")
                .upserts
                .clone()
        }
    }

    impl super::GroupSettingsMemberStore for GroupSettingsMemberStoreStub {
        type Error = StubError;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::GroupSettingsMemberFuture<'a, Option<ChatMemberRecord>, Self::Error> {
            Box::pin(async move {
                let mut state = self
                    .state
                    .lock()
                    .expect("group settings member store state");
                state.get_calls.push((chat_id, user_id));
                if state.fail_get {
                    Err(StubError)
                } else {
                    Ok(state.member.clone())
                }
            })
        }

        fn upsert_chat_member<'a>(
            &'a self,
            member: ChatMemberUpsert,
        ) -> super::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("group settings member store state")
                    .upserts
                    .push(member);
                Ok(())
            })
        }
    }

    impl super::NewMembersFollowupMemberStore for GroupSettingsMemberStoreStub {
        type Error = StubError;

        fn upsert_new_chat_member<'a>(
            &'a self,
            member: ChatMemberUpsert,
        ) -> super::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("group settings member store state")
                    .upserts
                    .push(member);
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct GroupSettingsMemberApiStub {
        state: Arc<Mutex<GroupSettingsMemberApiState>>,
    }

    #[derive(Default)]
    struct GroupSettingsMemberApiState {
        member: Option<ChatMember>,
        fail: bool,
        calls: Vec<(i64, i64)>,
    }

    impl GroupSettingsMemberApiStub {
        fn with_member(member: ChatMember) -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsMemberApiState {
                    member: Some(member),
                    ..GroupSettingsMemberApiState::default()
                })),
            }
        }

        fn failing() -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsMemberApiState {
                    fail: true,
                    ..GroupSettingsMemberApiState::default()
                })),
            }
        }

        fn calls(&self) -> Vec<(i64, i64)> {
            self.state
                .lock()
                .expect("group settings member API state")
                .calls
                .clone()
        }
    }

    impl super::GroupSettingsMemberApi for GroupSettingsMemberApiStub {
        type Error = StubError;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::GroupSettingsMemberFuture<'a, ChatMember, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("group settings member API state");
                state.calls.push((chat_id, user_id));
                if state.fail {
                    return Err(StubError);
                }
                state.member.clone().ok_or(StubError)
            })
        }
    }

    #[derive(Clone, Default)]
    struct GroupSettingsAdminSyncStoreStub {
        state: Arc<Mutex<GroupSettingsAdminSyncStoreState>>,
    }

    #[derive(Default)]
    struct GroupSettingsAdminSyncStoreState {
        members: Vec<ChatMemberRecord>,
        fail_list: bool,
        list_calls: Vec<i64>,
        upserts: Vec<ChatMemberUpsert>,
        users: Vec<UserState>,
    }

    impl GroupSettingsAdminSyncStoreStub {
        fn with_members(members: Vec<ChatMemberRecord>) -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsAdminSyncStoreState {
                    members,
                    ..GroupSettingsAdminSyncStoreState::default()
                })),
            }
        }

        fn list_calls(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("group settings admin sync store state")
                .list_calls
                .clone()
        }

        fn upserts(&self) -> Vec<ChatMemberUpsert> {
            self.state
                .lock()
                .expect("group settings admin sync store state")
                .upserts
                .clone()
        }

        fn users(&self) -> Vec<UserState> {
            self.state
                .lock()
                .expect("group settings admin sync store state")
                .users
                .clone()
        }
    }

    impl super::GroupSettingsAdminSyncStore for GroupSettingsAdminSyncStoreStub {
        type Error = StubError;

        fn list_chat_members<'a>(
            &'a self,
            chat_id: i64,
        ) -> super::GroupSettingsMemberFuture<'a, Vec<ChatMemberRecord>, Self::Error> {
            Box::pin(async move {
                let mut state = self
                    .state
                    .lock()
                    .expect("group settings admin sync store state");
                state.list_calls.push(chat_id);
                if state.fail_list {
                    Err(StubError)
                } else {
                    Ok(state.members.clone())
                }
            })
        }

        fn upsert_chat_member<'a>(
            &'a self,
            member: ChatMemberUpsert,
        ) -> super::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("group settings admin sync store state")
                    .upserts
                    .push(member);
                Ok(())
            })
        }

        fn upsert_user_state<'a>(
            &'a self,
            user: UserState,
        ) -> super::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("group settings admin sync store state")
                    .users
                    .push(user);
                Ok(())
            })
        }
    }

    type AdminCacheSave = (i64, Vec<i64>, Duration);

    #[derive(Clone, Default)]
    struct GroupSettingsAdminCacheStub {
        state: Arc<Mutex<GroupSettingsAdminCacheState>>,
    }

    #[derive(Default)]
    struct GroupSettingsAdminCacheState {
        fail: bool,
        saves: Vec<AdminCacheSave>,
    }

    impl GroupSettingsAdminCacheStub {
        fn failing() -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsAdminCacheState {
                    fail: true,
                    ..GroupSettingsAdminCacheState::default()
                })),
            }
        }

        fn saves(&self) -> Vec<AdminCacheSave> {
            self.state
                .lock()
                .expect("group settings admin cache state")
                .saves
                .clone()
        }
    }

    impl super::GroupSettingsAdminCache for GroupSettingsAdminCacheStub {
        type Error = StubError;

        fn save_chat_admin_ids<'a>(
            &'a self,
            chat_id: i64,
            admin_ids: Vec<i64>,
            ttl: Duration,
        ) -> super::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("group settings admin cache state");
                state.saves.push((chat_id, admin_ids, ttl));
                if state.fail { Err(StubError) } else { Ok(()) }
            })
        }
    }

    #[derive(Clone)]
    struct GroupSettingsAdminsApiStub {
        state: Arc<Mutex<GroupSettingsAdminsApiState>>,
    }

    #[derive(Default)]
    struct GroupSettingsAdminsApiState {
        admins: Vec<ChatMember>,
        fail: bool,
        calls: Vec<i64>,
    }

    impl GroupSettingsAdminsApiStub {
        fn with_admins(admins: Vec<ChatMember>) -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsAdminsApiState {
                    admins,
                    ..GroupSettingsAdminsApiState::default()
                })),
            }
        }

        fn failing() -> Self {
            Self {
                state: Arc::new(Mutex::new(GroupSettingsAdminsApiState {
                    fail: true,
                    ..GroupSettingsAdminsApiState::default()
                })),
            }
        }

        fn calls(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("group settings admins API state")
                .calls
                .clone()
        }
    }

    impl super::GroupSettingsAdminsApi for GroupSettingsAdminsApiStub {
        type Error = StubError;

        fn get_chat_administrators<'a>(
            &'a self,
            chat_id: i64,
        ) -> super::GroupSettingsMemberFuture<'a, Vec<ChatMember>, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("group settings admins API state");
                state.calls.push(chat_id);
                if state.fail {
                    Err(StubError)
                } else {
                    Ok(state.admins.clone())
                }
            })
        }
    }

    fn promoting_admin_member(user_id: i64) -> ChatMember {
        ChatMember::Administrator(
            ChatMemberAdministrator::new(User::new(user_id, "Ada", false))
                .with_can_be_edited(true)
                .with_can_delete_messages(true)
                .with_can_manage_video_chats(true)
                .with_can_restrict_members(true)
                .with_can_promote_members(true)
                .with_can_change_info(true)
                .with_can_invite_users(true)
                .with_can_post_messages(true)
                .with_can_edit_messages(true)
                .with_can_pin_messages(true)
                .with_can_manage_topics(true),
        )
    }

    fn creator_member(user_id: i64) -> ChatMember {
        ChatMember::Creator(ChatMemberCreator::new(User::new(user_id, "Grace", false)))
    }

    fn restricted_member_with_send_permissions(user_id: i64) -> ChatMember {
        ChatMember::Restricted(
            carapax::types::ChatMemberRestricted::new(User::new(user_id, "Ada", false), 0)
                .with_can_send_messages(true)
                .with_can_send_audios(true)
                .with_can_send_documents(true)
                .with_can_send_photos(true)
                .with_can_send_videos(true)
                .with_can_send_video_notes(true)
                .with_can_send_voice_notes(true)
                .with_can_send_polls(true)
                .with_can_send_other_messages(true)
                .with_can_add_web_page_previews(true),
        )
    }
}
