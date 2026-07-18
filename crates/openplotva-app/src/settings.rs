//! App-level fetcher settings command behavior.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use carapax::types::{
    Chat as TelegramChat, ChatMember as TelegramChatMember, ChatMemberRestricted,
    Message as TelegramMessage, MessageData as TelegramMessageData,
    TextEntity as TelegramTextEntity, Update as TelegramUpdate, UpdateType, User as TelegramUser,
};
use openplotva_core::{ChatSettings, SENDER_TYPE_CHANNEL, SENDER_TYPE_SAME_CHAT, UserState};
use openplotva_server::ACTION_SEND_TEXT;
use openplotva_storage::{ChatMemberRecord, ChatMemberUpsert};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY,
    StatelessJobItem, control_job_params_from_stateless_job, new_control_job_at,
};
use openplotva_telegram::{
    ChatRef, DeleteMessageRequest, DispatcherConfig, DispatcherQueue, OutboundBuildError,
    ReplyMarkup, ReplyMessageRef, RichMessageRequest, TextMessageRequest,
    build_delete_message_method, execute_telegram_method,
};
use thiserror::Error;
use time::OffsetDateTime;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueRichRequest, QueueTextReport, QueueTextRequest, VirtualIdFactory,
    monotonic_virtual_id_factory, queue_rich_message, queue_text_message_parts,
    send_work_item_with_ephemeral,
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
const JOIN_GREETING_DELETE_AFTER: Duration = Duration::from_secs(10 * 60);
const JOIN_GREETING_DEBOUNCE: Duration = Duration::from_secs(30);
const ADMIN_COMMAND_UNAUTHORIZED_TEXT: &str = "❌ У вас нет прав на выполнение этой команды.";
const ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER: Duration = Duration::from_secs(60);
const ADMIN_CHAT_SETTINGS_USAGE_TEXT: &str = "Usage: /admin_chat_settings [chat_id или @username]";
const ADMIN_CHAT_SETTINGS_WEBAPP_MISSING_TEXT: &str = "WebApp URL is not configured.";
const CHAT_ADMINS_CACHE_TTL: Duration = Duration::from_secs(30 * 60);

/// Boxed future returned by settings taskman assignment calls.
pub type SettingsControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by settings taskman worker queue calls.
pub type SettingsControlJobWorkerFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by group settings executor permission checks.
pub type GroupSettingsControlJobFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub type GroupSettingsSyncFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Boxed future returned by new-members follow-up side effects.
pub type NewMembersFollowupFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed future returned by fallible new-members runtime stores.
pub type NewMembersRuntimeFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by group settings member storage/API calls.
pub type GroupSettingsMemberFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by `/admin_chat_settings` target lookups.
pub type AdminChatTargetFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<AdminChatSettingsTarget, E>> + Send + 'a>>;

pub trait SettingsControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Assign a control job to a named taskman queue.
    fn assign_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> SettingsControlJobQueueFuture<'a, Self::Error>;

    fn materializes_human_new_members_directly(&self) -> bool {
        false
    }
}

impl<T> SettingsControlJobQueue for T
where
    T: crate::payments::PaymentControlJobQueue + Sync,
{
    type Error = T::Error;

    fn assign_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> SettingsControlJobQueueFuture<'a, Self::Error> {
        self.assign_payment_control_job(queue_name, job)
    }
}

#[derive(Clone, Debug)]
pub struct ProjectionAwareSettingsControlQueue<Queue, Greeting> {
    queue: Arc<Queue>,
    greeting: Arc<Greeting>,
}

impl<Queue, Greeting> ProjectionAwareSettingsControlQueue<Queue, Greeting> {
    pub fn new(queue: Arc<Queue>, greeting: Arc<Greeting>) -> Self {
        Self { queue, greeting }
    }
}

impl<Queue, Greeting> SettingsControlJobQueue
    for ProjectionAwareSettingsControlQueue<Queue, Greeting>
where
    Queue: SettingsControlJobQueue + Send + Sync,
    Greeting: NewMembersGreetingRunner + Send + Sync,
{
    type Error = Queue::Error;

    fn assign_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> SettingsControlJobQueueFuture<'a, Self::Error> {
        Box::pin(async move {
            if let Ok(params) = control_job_params_from_stateless_job(&job)
                && params.data.kind == ControlKind::NewMembersFollowup
                && !params.data.bot_was_added
            {
                self.greeting
                    .run_join_greeting(new_members_greeting_from_control_params(&params))
                    .await;
                return Ok(());
            }
            self.queue
                .assign_settings_control_job(queue_name, job)
                .await
        })
    }

    fn materializes_human_new_members_directly(&self) -> bool {
        true
    }
}

/// Queue/status boundary for settings-owned taskman control-job workers.
pub trait SettingsControlJobWorkerQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn dequeue_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> SettingsControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    >;

    /// Finalize one settings-owned control job as completed.
    fn complete_settings_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> SettingsControlJobWorkerFuture<'a, (), Self::Error>;

    /// Finalize one settings-owned control job as failed.
    fn fail_settings_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> SettingsControlJobWorkerFuture<'a, (), Self::Error>;
}

impl<T> SettingsControlJobWorkerQueue for T
where
    T: crate::payments::SharedControlJobWorkerQueue + Sync,
{
    type Error = T::Error;

    fn dequeue_settings_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> SettingsControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    > {
        self.dequeue_shared_control_job_matching(
            queue_name,
            "settings-control",
            is_settings_control_job,
        )
    }

    fn complete_settings_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> SettingsControlJobWorkerFuture<'a, (), Self::Error> {
        self.complete_shared_control_job(job_id)
    }

    fn fail_settings_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> SettingsControlJobWorkerFuture<'a, (), Self::Error> {
        self.fail_shared_control_job(job_id, error)
    }
}

pub trait GroupSettingsControlJobEffects {
    /// Error returned by permission checks.
    type Error: fmt::Display + Send + Sync + 'static;

    fn can_open_group_settings<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsControlJobFuture<'a, bool, Self::Error>;

    fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> GroupSettingsSyncFuture<'a>;
}

pub trait NewMembersFollowupControlJobEffects {
    fn enable_chat_communication<'a>(&'a self, chat_id: i64) -> NewMembersFollowupFuture<'a, ()>;

    fn is_chat_blocked<'a>(&'a self, chat_id: i64) -> NewMembersFollowupFuture<'a, bool>;

    fn try_send_greeting_for_join_wave<'a>(
        &'a self,
        greeting: NewMembersGreeting,
    ) -> NewMembersFollowupFuture<'a, ()>;
}

pub trait NewMembersBlockedChatStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Check whether a chat is blocked at a specific instant.
    fn is_chat_blocked_at<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> NewMembersRuntimeFuture<'a, bool, Self::Error>;
}

impl NewMembersBlockedChatStore for openplotva_storage::RedisBlockedChatStore {
    type Error = openplotva_storage::StorageError;

    fn is_chat_blocked_at<'a>(
        &'a self,
        chat_id: i64,
        now: OffsetDateTime,
    ) -> NewMembersRuntimeFuture<'a, bool, Self::Error> {
        Box::pin(async move { self.is_chat_blocked_at(chat_id, now).await })
    }
}

pub trait NewMembersGreetingCache {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Record joined user IDs in `join_greet:users:{chat_id}`.
    fn record_join_member_ids<'a>(
        &'a self,
        chat_id: i64,
        user_ids: &'a [i64],
        score: i64,
        ttl: Duration,
    ) -> NewMembersRuntimeFuture<'a, (), Self::Error>;

    fn start_debounce<'a>(
        &'a self,
        chat_id: i64,
        ttl: Duration,
    ) -> NewMembersRuntimeFuture<'a, bool, Self::Error>;

    /// Load user ID strings from `join_greet:users:{chat_id}`.
    fn recent_join_member_ids<'a>(
        &'a self,
        chat_id: i64,
        min_score: i64,
    ) -> NewMembersRuntimeFuture<'a, Vec<String>, Self::Error>;

    /// Load `join_greet:msg:{chat_id}`.
    fn previous_greeting_message_id<'a>(
        &'a self,
        chat_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<i32>, Self::Error>;

    /// Save `join_greet:msg:{chat_id}`.
    fn set_previous_greeting_message_id<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        ttl: Duration,
    ) -> NewMembersRuntimeFuture<'a, (), Self::Error>;
}

impl NewMembersGreetingCache for openplotva_storage::RedisJoinGreetingStore {
    type Error = openplotva_storage::StorageError;

    fn record_join_member_ids<'a>(
        &'a self,
        chat_id: i64,
        user_ids: &'a [i64],
        score: i64,
        ttl: Duration,
    ) -> NewMembersRuntimeFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.record_join_member_ids(chat_id, user_ids, score, ttl)
                .await
        })
    }

    fn start_debounce<'a>(
        &'a self,
        chat_id: i64,
        ttl: Duration,
    ) -> NewMembersRuntimeFuture<'a, bool, Self::Error> {
        Box::pin(async move { self.start_debounce(chat_id, ttl).await })
    }

    fn recent_join_member_ids<'a>(
        &'a self,
        chat_id: i64,
        min_score: i64,
    ) -> NewMembersRuntimeFuture<'a, Vec<String>, Self::Error> {
        Box::pin(async move { self.recent_join_member_ids(chat_id, min_score).await })
    }

    fn previous_greeting_message_id<'a>(
        &'a self,
        chat_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<i32>, Self::Error> {
        Box::pin(async move { self.previous_greeting_message_id(chat_id).await })
    }

    fn set_previous_greeting_message_id<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        ttl: Duration,
    ) -> NewMembersRuntimeFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.set_previous_greeting_message_id(chat_id, message_id, ttl)
                .await
        })
    }
}

pub trait NewMembersGreetingSettingsStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load `chat_settings`.
    fn greeting_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<ChatSettings>, Self::Error>;
}

impl NewMembersGreetingSettingsStore for openplotva_storage::PostgresChatSettingsStore {
    type Error = openplotva_storage::StorageError;

    fn greeting_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<ChatSettings>, Self::Error> {
        Box::pin(async move { self.get_chat_settings(chat_id).await })
    }
}

pub trait NewMembersGreetingMemberDataStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load users by IDs.
    fn list_user_states_by_ids<'a>(
        &'a self,
        user_ids: &'a [i64],
    ) -> NewMembersRuntimeFuture<'a, Vec<UserState>, Self::Error>;

    /// Load one user by ID.
    fn get_user_state<'a>(
        &'a self,
        user_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<UserState>, Self::Error>;

    /// Load chat-member rows by candidate IDs.
    fn list_chat_members_by_user_ids<'a>(
        &'a self,
        chat_id: i64,
        user_ids: &'a [i64],
    ) -> NewMembersRuntimeFuture<'a, Vec<ChatMemberRecord>, Self::Error>;

    /// Load one chat-member row.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<ChatMemberRecord>, Self::Error>;
}

impl NewMembersGreetingMemberDataStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn list_user_states_by_ids<'a>(
        &'a self,
        user_ids: &'a [i64],
    ) -> NewMembersRuntimeFuture<'a, Vec<UserState>, Self::Error> {
        Box::pin(async move { self.list_user_states_by_ids(user_ids).await })
    }

    fn get_user_state<'a>(
        &'a self,
        user_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<UserState>, Self::Error> {
        Box::pin(async move { self.get_user_state(user_id).await })
    }

    fn list_chat_members_by_user_ids<'a>(
        &'a self,
        chat_id: i64,
        user_ids: &'a [i64],
    ) -> NewMembersRuntimeFuture<'a, Vec<ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.list_chat_members_by_user_ids(chat_id, user_ids).await })
    }

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> NewMembersRuntimeFuture<'a, Option<ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.get_chat_member(chat_id, user_id).await })
    }
}

pub trait NewMembersGreetingSender {
    /// Delete the previous greeting message. Errors are logged by the concrete sender.
    fn delete_previous_greeting_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> NewMembersFollowupFuture<'a, ()>;

    fn send_ephemeral_greeting<'a>(
        &'a self,
        message: NewMembersGreetingMessage,
    ) -> NewMembersFollowupFuture<'a, Option<i32>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewMembersGreetingMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Trigger service message ID.
    pub reply_to_message_id: i32,
    /// Optional topic ID carried by taskman data.
    pub thread_id: Option<i32>,
    /// HTML greeting text.
    pub text: String,
    pub disable_notification: bool,
    /// Ephemeral delete timing.
    pub delete_after: Duration,
}

/// Runner boundary used by concrete new-members control-job effects.
pub trait NewMembersGreetingRunner {
    fn run_join_greeting<'a>(
        &'a self,
        greeting: NewMembersGreeting,
    ) -> NewMembersFollowupFuture<'a, ()>;
}

/// Concrete new-members follow-up effects composed from communication, blocked-chat, and greeting boundaries.
#[derive(Clone, Debug)]
pub struct NewMembersFollowupRuntimeEffects<Communication, Blocked, Greeting> {
    communication: Communication,
    blocked: Blocked,
    greeting: Greeting,
}

impl<Communication, Blocked, Greeting>
    NewMembersFollowupRuntimeEffects<Communication, Blocked, Greeting>
{
    pub fn new(communication: Communication, blocked: Blocked, greeting: Greeting) -> Self {
        Self {
            communication,
            blocked,
            greeting,
        }
    }
}

impl<Communication, Blocked, Greeting> NewMembersFollowupControlJobEffects
    for NewMembersFollowupRuntimeEffects<Communication, Blocked, Greeting>
where
    Communication: crate::members::ChatCommunicationEffects + Sync,
    Blocked: NewMembersBlockedChatStore + Sync,
    Greeting: NewMembersGreetingRunner + Sync,
{
    fn enable_chat_communication<'a>(&'a self, chat_id: i64) -> NewMembersFollowupFuture<'a, ()> {
        Box::pin(async move {
            if let Err(error) = self.communication.enable_chat_communication(chat_id).await {
                tracing::warn!(%error, chat_id, "failed to enable chat communication for new-members follow-up");
            }
        })
    }

    fn is_chat_blocked<'a>(&'a self, chat_id: i64) -> NewMembersFollowupFuture<'a, bool> {
        Box::pin(async move {
            match self
                .blocked
                .is_chat_blocked_at(chat_id, OffsetDateTime::now_utc())
                .await
            {
                Ok(blocked) => blocked,
                Err(error) => {
                    tracing::warn!(%error, chat_id, "failed to check blocked-chat state");
                    false
                }
            }
        })
    }

    fn try_send_greeting_for_join_wave<'a>(
        &'a self,
        greeting: NewMembersGreeting,
    ) -> NewMembersFollowupFuture<'a, ()> {
        self.greeting.run_join_greeting(greeting)
    }
}

#[derive(Clone, Debug)]
pub struct NewMembersJoinGreetingRuntime<Cache, Settings, Members, Sender> {
    cache: Cache,
    settings: Settings,
    members: Members,
    sender: Sender,
    debounce_delay: Duration,
}

impl<Cache, Settings, Members, Sender>
    NewMembersJoinGreetingRuntime<Cache, Settings, Members, Sender>
{
    pub fn new(cache: Cache, settings: Settings, members: Members, sender: Sender) -> Self {
        Self {
            cache,
            settings,
            members,
            sender,
            debounce_delay: JOIN_GREETING_DEBOUNCE,
        }
    }

    /// Override the debounce delay, useful for deterministic tests.
    pub fn with_debounce_delay(mut self, debounce_delay: Duration) -> Self {
        self.debounce_delay = debounce_delay;
        self
    }
}

impl<Cache, Settings, Members, Sender> NewMembersGreetingRunner
    for NewMembersJoinGreetingRuntime<Cache, Settings, Members, Sender>
where
    Cache: NewMembersGreetingCache + Clone + Send + Sync + 'static,
    Settings: NewMembersGreetingSettingsStore + Clone + Send + Sync + 'static,
    Members: NewMembersGreetingMemberDataStore + Clone + Send + Sync + 'static,
    Sender: NewMembersGreetingSender + Clone + Send + Sync + 'static,
{
    fn run_join_greeting<'a>(
        &'a self,
        greeting: NewMembersGreeting,
    ) -> NewMembersFollowupFuture<'a, ()> {
        Box::pin(async move {
            let started = try_send_greeting_for_join_wave_at(
                &self.cache,
                &self.settings,
                &greeting,
                OffsetDateTime::now_utc(),
            )
            .await;
            if !started {
                return;
            }

            if self.debounce_delay.is_zero() {
                compose_and_send_greeting_at(
                    &self.cache,
                    &self.settings,
                    &self.members,
                    &self.sender,
                    &greeting,
                    OffsetDateTime::now_utc(),
                )
                .await;
                return;
            }

            let runtime = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(runtime.debounce_delay).await;
                compose_and_send_greeting_at(
                    &runtime.cache,
                    &runtime.settings,
                    &runtime.members,
                    &runtime.sender,
                    &greeting,
                    OffsetDateTime::now_utc(),
                )
                .await;
            });
        })
    }
}

#[derive(Clone, Debug)]
pub struct TelegramJoinGreetingSender {
    ephemeral_store: openplotva_storage::RedisEphemeralMessageStore,
    permissions: Arc<
        crate::permissions::ChatPermissionPolicy<openplotva_storage::PostgresChatSettingsStore>,
    >,
    telegram: openplotva_telegram::TelegramClient,
    rich: openplotva_telegram::RichApiClient,
    next_virtual_id: Arc<AtomicU64>,
}

impl TelegramJoinGreetingSender {
    /// Build a concrete join-greeting sender for runtime effects.
    pub fn new(
        ephemeral_store: openplotva_storage::RedisEphemeralMessageStore,
        permissions: Arc<
            crate::permissions::ChatPermissionPolicy<openplotva_storage::PostgresChatSettingsStore>,
        >,
        telegram: openplotva_telegram::TelegramClient,
        rich: openplotva_telegram::RichApiClient,
    ) -> Self {
        Self {
            ephemeral_store,
            permissions,
            telegram,
            rich,
            next_virtual_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_virtual_id(&self) -> String {
        let id = self.next_virtual_id.fetch_add(1, Ordering::Relaxed);
        format!("join-greeting-{id}")
    }
}

impl NewMembersGreetingSender for TelegramJoinGreetingSender {
    fn delete_previous_greeting_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> NewMembersFollowupFuture<'a, ()> {
        Box::pin(async move {
            let Ok(method) = build_delete_message_method(&DeleteMessageRequest {
                chat_id,
                message_id: i64::from(message_id),
            }) else {
                return;
            };
            if let Err(error) = execute_telegram_method(&self.telegram, method.into()).await {
                tracing::warn!(%error, chat_id, message_id, "failed to delete previous join greeting");
            }
        })
    }

    fn send_ephemeral_greeting<'a>(
        &'a self,
        message: NewMembersGreetingMessage,
    ) -> NewMembersFollowupFuture<'a, Option<i32>> {
        Box::pin(async move {
            let now = OffsetDateTime::now_utc();
            let permission = self
                .permissions
                .can_perform_action_at(message.chat_id, None, ACTION_SEND_TEXT, now)
                .await;
            if !permission.allowed {
                return None;
            }

            let queue = DispatcherQueue::new(DispatcherConfig::default());
            let chat = ChatRef {
                id: message.chat_id,
                is_forum: message.thread_id.is_some(),
            };
            let request = RichMessageRequest {
                chat: Some(chat),
                message_thread_id: message.thread_id.unwrap_or_default().into(),
                disable_notification: message.disable_notification,
                allow_sending_without_reply: None,
                html: message.text.clone(),
                reply_markup: None,
            };
            let reply_to = ReplyMessageRef {
                message_id: i64::from(message.reply_to_message_id),
                chat,
                is_topic_message: message.thread_id.is_some(),
                message_thread_id: message.thread_id.unwrap_or_default().into(),
            };
            let queued = queue_rich_message(
                &queue,
                QueueRichRequest {
                    message: &request,
                    reply_to: Some(&reply_to),
                    immediate: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(message.delete_after),
                    protected: false,
                    debounce_key: None,
                },
                || self.next_virtual_id(),
            )
            .await;
            if let Err(error) = queued {
                tracing::warn!(%error, chat_id = chat.id, "failed to queue join greeting");
                return None;
            }

            let mut first_message_id = None;
            let mut next = queue
                .dequeue_immediate()
                .or_else(|| queue.dequeue_regular());
            while let Some(item) = next {
                let report = send_work_item_with_ephemeral(
                    &self.ephemeral_store,
                    item,
                    OffsetDateTime::now_utc(),
                    |method| async move {
                        openplotva_telegram::execute_telegram_method_with_rich(
                            &self.telegram,
                            &self.rich,
                            method,
                        )
                        .await
                    },
                )
                .await;
                if first_message_id.is_none() {
                    first_message_id = report.sent_message_id;
                }
                if let Some(error) = report.send_error {
                    tracing::warn!(
                        error,
                        chat_id = chat.id,
                        "failed to send join greeting part"
                    );
                }
                next = queue
                    .dequeue_immediate()
                    .or_else(|| queue.dequeue_regular());
            }
            first_message_id
        })
    }
}

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

    fn upsert_user_state<'a>(
        &'a self,
        user: UserState,
    ) -> GroupSettingsMemberFuture<'a, (), Self::Error>;
}

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

pub trait GroupSettingsAdminsApi {
    /// Error returned by concrete Telegram calls.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Fetch chat administrators from Telegram.
    fn get_chat_administrators<'a>(
        &'a self,
        chat_id: i64,
    ) -> GroupSettingsMemberFuture<'a, Vec<TelegramChatMember>, Self::Error>;
}

#[derive(Clone, Debug)]
pub struct GroupSettingsRuntimeEffects<MemberStore, MemberApi, AdminStore, AdminApi, AdminCache> {
    member_store: MemberStore,
    member_api: MemberApi,
    admin_store: AdminStore,
    admin_api: AdminApi,
    admin_cache: AdminCache,
}

impl<MemberStore, MemberApi, AdminStore, AdminApi, AdminCache>
    GroupSettingsRuntimeEffects<MemberStore, MemberApi, AdminStore, AdminApi, AdminCache>
{
    /// Build concrete group-settings control-job effects from storage, Telegram, and cache ports.
    #[must_use]
    pub fn new(
        member_store: MemberStore,
        member_api: MemberApi,
        admin_store: AdminStore,
        admin_api: AdminApi,
        admin_cache: AdminCache,
    ) -> Self {
        Self {
            member_store,
            member_api,
            admin_store,
            admin_api,
            admin_cache,
        }
    }
}

impl<MemberStore, MemberApi, AdminStore, AdminApi, AdminCache> GroupSettingsControlJobEffects
    for GroupSettingsRuntimeEffects<MemberStore, MemberApi, AdminStore, AdminApi, AdminCache>
where
    MemberStore: GroupSettingsMemberStore + Send + Sync,
    MemberApi: GroupSettingsMemberApi + Send + Sync,
    AdminStore: GroupSettingsAdminSyncStore + Send + Sync,
    AdminApi: GroupSettingsAdminsApi + Send + Sync,
    AdminCache: GroupSettingsAdminCache + Send + Sync,
{
    type Error = GroupSettingsPermissionCheckError;

    fn can_open_group_settings<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> GroupSettingsControlJobFuture<'a, bool, Self::Error> {
        Box::pin(async move {
            can_open_group_settings_with_sources(
                &self.member_store,
                &self.member_api,
                chat_id,
                user_id,
            )
            .await
        })
    }

    fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> GroupSettingsSyncFuture<'a> {
        Box::pin(async move {
            if let Err(error) = sync_chat_admins_with_cache(
                &self.admin_store,
                &self.admin_api,
                &self.admin_cache,
                chat_id,
            )
            .await
            {
                tracing::warn!(
                    %error,
                    chat_id,
                    "failed to sync chat administrators for group settings control job"
                );
            }
        })
    }
}

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

pub trait AdminChatTargetResolver {
    /// Error returned by concrete Telegram calls.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Resolve a raw command target into Telegram chat metadata.
    fn resolve_admin_chat_target<'a>(
        &'a self,
        target_identifier: &'a str,
    ) -> AdminChatTargetFuture<'a, Self::Error>;
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GroupSettingsPermissionCheckError {
    #[error("missing caller user ID")]
    MissingCaller,
    /// Telegram permission probe failed.
    #[error("{0}")]
    Telegram(String),
}

/// Source used by a group-settings admin sync attempt.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupSettingsAdminSyncSource {
    #[default]
    Skipped,
    /// Administrators came from Telegram `getChatAdministrators`.
    Telegram,
    /// Telegram failed and stored admin rows were used.
    StoredFallback,
}

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

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GroupSettingsAdminSyncError {
    /// Telegram failed and no stored admin fallback was available.
    #[error("{0}")]
    Telegram(String),
    /// Stored fallback failed after Telegram failed.
    #[error("failed to get chat members: {0}")]
    Storage(String),
}

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
    NotSettingsCommand,
    /// The command was not in a private chat; group handling is a later taskman slice.
    NonPrivateChat,
    WebAppUrlMissing,
    Queued(QueueTextReport),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsControlJobBuild {
    NotSettingsCommand,
    /// Private chats are handled by `handlePrivateSettingsCommand`.
    PrivateChat,
    UnsupportedSameChatSender,
    UnsupportedChannelSender,
    MissingCaller,
    InvalidMessage,
    Job(Box<StatelessJobItem>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewMembersFollowupControlJobBuild {
    /// The update was not a Telegram message.
    NotMessage,
    /// The message had no `new_chat_members` service payload.
    NoNewChatMembers,
    InvalidMessage,
    Job(Box<StatelessJobItem>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsCommandOutcome {
    NotSettingsCommand,
    /// Private chats are handled by `handlePrivateSettingsCommand`.
    PrivateChat,
    /// Same-chat sender decline text was queued.
    UnsupportedSameChatSender(QueueTextReport),
    /// Channel sender decline text was queued.
    UnsupportedChannelSender(QueueTextReport),
    MissingCaller,
    InvalidMessage,
    QueueError(QueueTextReport),
    /// The control job was assigned and the wait notice was queued.
    Queued {
        notice: QueueTextReport,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewMembersFollowupUpdateOutcome {
    /// The update was not a Telegram message.
    NotMessage,
    /// The message had no new-chat-member service payload.
    NoNewChatMembers,
    InvalidMessage,
    QueueError {
        notice: QueueTextReport,
        member_upsert_errors: usize,
    },
    Queued {
        member_upsert_errors: usize,
    },
    Accumulated {
        member_upsert_errors: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct NewMembersFollowupUpdateInput<'a> {
    /// Decoded Telegram update.
    pub update: &'a TelegramUpdate,
    /// Current bot Telegram user ID, or zero when unavailable.
    pub bot_id: i64,
    /// Task creation time.
    pub created: OffsetDateTime,
}

/// Borrowed ports used by the settings-owned decoded-update slice.
#[derive(Clone, Copy, Debug)]
pub struct SettingsUpdatePorts<'a, Queue> {
    /// Dispatcher queue used for immediate/future sends.
    pub dispatcher_queue: &'a DispatcherQueue,
    /// Taskman control queue used by settings-owned jobs.
    pub control_queue: &'a Queue,
}

/// Runtime settings needed while handling one decoded update.
#[derive(Clone, Copy, Debug)]
pub struct SettingsUpdateContext<'a> {
    /// Current bot username used for command targeting.
    pub bot_username: &'a str,
    /// Current bot Telegram user ID, or zero when unavailable.
    pub bot_id: i64,
    /// WebApp base URL used by private settings buttons.
    pub web_app_url: &'a str,
    /// Task creation time.
    pub created: OffsetDateTime,
}

/// Route chosen by the settings-owned decoded-update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SettingsUpdateRoute {
    /// The update had no settings-owned behavior and was delegated.
    Delegated,
    /// A private `/settings` command was handled.
    PrivateSettings(SettingsCommandOutcome),
    /// A group `/settings` command was handled.
    GroupSettings(GroupSettingsCommandOutcome),
    NewMembersFollowup {
        /// Producer outcome.
        outcome: NewMembersFollowupUpdateOutcome,
    },
}

/// Error returned by the settings-owned decoded-update wrapper.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SettingsUpdateError {
    /// Settings side effect failed before delegation.
    #[error("settings outbound: {message}")]
    Outbound {
        /// Display form of the outbound error.
        message: String,
    },
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

/// Long-lived config for the settings-owned update handler.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettingsUpdateHandlerConfig {
    /// Current bot username used for command targeting.
    pub bot_username: String,
    /// Current bot Telegram user ID, or zero when unavailable.
    pub bot_id: i64,
    /// WebApp base URL used by private settings buttons.
    pub web_app_url: String,
}

impl SettingsUpdateHandlerConfig {
    /// Build settings update-handler config.
    pub fn new(
        bot_username: impl Into<String>,
        bot_id: i64,
        web_app_url: impl Into<String>,
    ) -> Self {
        Self {
            bot_username: bot_username.into(),
            bot_id,
            web_app_url: web_app_url.into(),
        }
    }
}

/// `UpdateHandler` adapter for settings-owned decoded update behavior.
#[derive(Clone)]
pub struct SettingsUpdateHandler<Queue, Next> {
    dispatcher_queue: Arc<DispatcherQueue>,
    control_queue: Arc<Queue>,
    bot_username: String,
    bot_id: i64,
    web_app_url: String,
    next_virtual_id: VirtualIdFactory,
    next: Arc<Next>,
}

impl<Queue, Next> SettingsUpdateHandler<Queue, Next> {
    /// Build a settings-aware handler around the real downstream update handler.
    pub fn new(
        dispatcher_queue: Arc<DispatcherQueue>,
        control_queue: Arc<Queue>,
        config: SettingsUpdateHandlerConfig,
        next: Arc<Next>,
    ) -> Self {
        Self {
            dispatcher_queue,
            control_queue,
            bot_username: config.bot_username,
            bot_id: config.bot_id,
            web_app_url: config.web_app_url,
            next_virtual_id: monotonic_virtual_id_factory("settings-vmsg"),
            next,
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Queue, Next> UpdateHandler for SettingsUpdateHandler<Queue, Next>
where
    Queue: SettingsControlJobQueue + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = SettingsUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_settings_update_or_else_at(
                SettingsUpdatePorts {
                    dispatcher_queue: self.dispatcher_queue.as_ref(),
                    control_queue: self.control_queue.as_ref(),
                },
                update,
                SettingsUpdateContext {
                    bot_username: &self.bot_username,
                    bot_id: self.bot_id,
                    web_app_url: &self.web_app_url,
                    created: OffsetDateTime::now_utc(),
                },
                || (self.next_virtual_id)(),
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupSettingsControlJobOutcome {
    /// The job was not the group-settings control kind.
    UnsupportedKind(ControlKind),
    PermissionCheckFailed(String),
    /// The caller is not allowed to manage group settings.
    PermissionDenied,
    BotUsernameMissing,
    /// Admin sync ran and the settings deep-link notice was queued.
    SentLink,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewMembersGreeting {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Trigger message ID.
    pub message_id: i32,
    pub thread_id: Option<i32>,
    pub new_chat_member_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NewMembersFollowupControlJobOutcome {
    /// The job was not the new-members follow-up control kind.
    UnsupportedKind(ControlKind),
    /// Greeting was attempted and the optional blocked-chat notice state is recorded.
    Completed { unblock_notice_queued: bool },
}

/// Settings-owned control-job execution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SettingsControlJobExecution {
    /// Group settings executor result.
    GroupSettings(GroupSettingsControlJobOutcome),
    /// New-members follow-up executor result.
    NewMembersFollowup(NewMembersFollowupControlJobOutcome),
}

/// Result of one settings-owned taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SettingsControlJobWorkerReport {
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Settings executor result, when payload decoding succeeded.
    pub execution: Option<SettingsControlJobExecution>,
    /// The job was finalized as completed.
    pub completed: bool,
    /// The job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Executor returned a direct runtime error.
    pub execution_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminChatSettingsCommandOutcome {
    NotAdminChatSettingsCommand,
    Usage(QueueTextReport),
    ResolveError(QueueTextReport),
    WebAppUrlMissing(QueueTextReport),
    /// The target settings button was queued.
    Queued(QueueTextReport),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdminChatSettingsCommandUpdateOutcome {
    /// Update was not owned by this slice and went downstream.
    Delegated,
    Unauthorized,
    UnauthorizedSendError {
        /// Queue/build error text.
        message: String,
    },
    /// The authorized command was handled by the pure command path.
    Handled(AdminChatSettingsCommandOutcome),
    /// The authorized command hit a queue/build error while sending its result.
    SendError {
        /// Queue/build error text.
        message: String,
    },
}

#[derive(Clone)]
pub struct AdminChatSettingsCommandUpdateHandler<Resolver, Next> {
    dispatcher_queue: Arc<DispatcherQueue>,
    resolver: Arc<Resolver>,
    admin_ids: Arc<[i64]>,
    bot_username: String,
    web_app_url: String,
    next_virtual_id: VirtualIdFactory,
    next: Arc<Next>,
}

impl<Resolver, Next> AdminChatSettingsCommandUpdateHandler<Resolver, Next> {
    /// Build the admin chat-settings command handler around a real downstream handler.
    #[must_use]
    pub fn new(
        dispatcher_queue: Arc<DispatcherQueue>,
        resolver: Arc<Resolver>,
        admin_ids: Vec<i64>,
        bot_username: impl Into<String>,
        web_app_url: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            dispatcher_queue,
            resolver,
            admin_ids: Arc::from(admin_ids.into_boxed_slice()),
            bot_username: bot_username.into(),
            web_app_url: web_app_url.into(),
            next_virtual_id: monotonic_virtual_id_factory("admin-chat-settings-vmsg"),
            next,
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Resolver, Next> UpdateHandler for AdminChatSettingsCommandUpdateHandler<Resolver, Next>
where
    Resolver: AdminChatTargetResolver + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = Next::Error;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_admin_chat_settings_command_update_or_else_at(
                AdminChatSettingsCommandUpdateRuntime {
                    dispatcher_queue: self.dispatcher_queue.as_ref(),
                    resolver: self.resolver.as_ref(),
                    admin_ids: &self.admin_ids,
                    bot_username: &self.bot_username,
                    web_app_url: &self.web_app_url,
                    next_virtual_id: &self.next_virtual_id,
                },
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Copy)]
pub struct AdminChatSettingsCommandUpdateRuntime<'a, Resolver> {
    pub dispatcher_queue: &'a DispatcherQueue,
    pub resolver: &'a Resolver,
    /// Authorized Telegram admin user IDs.
    pub admin_ids: &'a [i64],
    /// Current bot username from `getMe`.
    pub bot_username: &'a str,
    /// Settings WebApp base URL.
    pub web_app_url: &'a str,
    /// Virtual ID generator for queued replies.
    pub next_virtual_id: &'a VirtualIdFactory,
}

pub async fn handle_admin_chat_settings_command_update_or_else_at<
    Resolver,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    runtime: AdminChatSettingsCommandUpdateRuntime<'_, Resolver>,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<AdminChatSettingsCommandUpdateOutcome, HandleError>
where
    Resolver: AdminChatTargetResolver + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
{
    let UpdateType::Message(message) = &update.update_type else {
        handle_other(update).await?;
        return Ok(AdminChatSettingsCommandUpdateOutcome::Delegated);
    };
    let Some(command) = admin_chat_settings_command_for_message(message, runtime.bot_username)
    else {
        handle_other(update).await?;
        return Ok(AdminChatSettingsCommandUpdateOutcome::Delegated);
    };
    if !command.name.eq_ignore_ascii_case("admin_chat_settings") {
        handle_other(update).await?;
        return Ok(AdminChatSettingsCommandUpdateOutcome::Delegated);
    }

    let actor_user_id = message
        .sender
        .get_user()
        .map(|user| i64::from(user.id))
        .unwrap_or_default();
    if !runtime.admin_ids.contains(&actor_user_id) {
        return match queue_admin_chat_settings_ephemeral_text(
            runtime.dispatcher_queue,
            message,
            ADMIN_COMMAND_UNAUTHORIZED_TEXT,
            || (runtime.next_virtual_id)(),
        )
        .await
        {
            Ok(_) => Ok(AdminChatSettingsCommandUpdateOutcome::Unauthorized),
            Err(error) => {
                tracing::warn!(%error, actor_user_id, "failed to queue admin chat-settings denial");
                Ok(
                    AdminChatSettingsCommandUpdateOutcome::UnauthorizedSendError {
                        message: error.to_string(),
                    },
                )
            }
        };
    }

    let outcome = handle_admin_chat_settings_command_update(
        runtime.dispatcher_queue,
        runtime.resolver,
        &update,
        runtime.web_app_url,
        || (runtime.next_virtual_id)(),
    )
    .await;
    match outcome {
        Ok(outcome) => Ok(AdminChatSettingsCommandUpdateOutcome::Handled(outcome)),
        Err(error) => {
            tracing::warn!(%error, actor_user_id, "failed to queue admin chat-settings result");
            Ok(AdminChatSettingsCommandUpdateOutcome::SendError {
                message: error.to_string(),
            })
        }
    }
}

pub async fn handle_private_settings_command_update<NextId>(
    queue: &DispatcherQueue,
    update: &TelegramUpdate,
    bot_username: &str,
    web_app_url: &str,
    next_virtual_id: NextId,
) -> Result<SettingsCommandOutcome, OutboundBuildError>
where
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
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
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

pub async fn handle_admin_chat_settings_command_update<Resolver, NextId>(
    dispatcher_queue: &DispatcherQueue,
    resolver: &Resolver,
    update: &TelegramUpdate,
    web_app_url: &str,
    next_virtual_id: NextId,
) -> Result<AdminChatSettingsCommandOutcome, OutboundBuildError>
where
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

pub async fn execute_group_settings_control_job_at<Effects, NextId>(
    dispatcher_queue: &DispatcherQueue,
    effects: &Effects,
    params: &ControlJobParams,
    bot_username: &str,
    next_virtual_id: NextId,
) -> Result<GroupSettingsControlJobOutcome, OutboundBuildError>
where
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

pub async fn execute_new_members_followup_control_job_at<Effects, NextId>(
    dispatcher_queue: &DispatcherQueue,
    effects: &Effects,
    params: &ControlJobParams,
    bot_username: &str,
    next_virtual_id: NextId,
) -> Result<NewMembersFollowupControlJobOutcome, OutboundBuildError>
where
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

/// Process one settings-owned taskman control job, if available.
pub async fn process_settings_control_job_once_at<Queue, GroupEffects, NewEffects, NextId>(
    queue: &Queue,
    dispatcher_queue: &DispatcherQueue,
    group_effects: &GroupEffects,
    new_members_effects: &NewEffects,
    bot_username: &str,
    mut next_virtual_id: NextId,
) -> SettingsControlJobWorkerReport
where
    Queue: SettingsControlJobWorkerQueue + Sync,
    GroupEffects: GroupSettingsControlJobEffects + Sync,
    NewEffects: NewMembersFollowupControlJobEffects + Sync,
    NextId: FnMut() -> String,
{
    let mut report = SettingsControlJobWorkerReport::default();
    let item = match queue.dequeue_settings_control_job(CONTROL_QUEUE_NAME).await {
        Ok(item) => item,
        Err(error) => {
            report.dequeue_error = Some(error.to_string());
            return report;
        }
    };

    let Some(item) = item else {
        return report;
    };
    report.dequeued = true;
    report.job_id = Some(item.id);

    let params = match control_job_params_from_stateless_job(&item.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            report.decode_error = Some(error.clone());
            mark_settings_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    let execution = match params.data.kind {
        ControlKind::GroupSettings => execute_group_settings_control_job_at(
            dispatcher_queue,
            group_effects,
            &params,
            bot_username,
            &mut next_virtual_id,
        )
        .await
        .map(SettingsControlJobExecution::GroupSettings),
        ControlKind::NewMembersFollowup => execute_new_members_followup_control_job_at(
            dispatcher_queue,
            new_members_effects,
            &params,
            bot_username,
            &mut next_virtual_id,
        )
        .await
        .map(SettingsControlJobExecution::NewMembersFollowup),
        kind => Ok(SettingsControlJobExecution::GroupSettings(
            GroupSettingsControlJobOutcome::UnsupportedKind(kind),
        )),
    };

    let execution = match execution {
        Ok(execution) => execution,
        Err(error) => {
            let error = error.to_string();
            report.execution_error = Some(error.clone());
            mark_settings_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    if let Some(error) = settings_control_job_failure_message(&execution) {
        mark_settings_control_job_failed(queue, item.id, &error, &mut report).await;
    } else {
        match queue.complete_settings_control_job(item.id).await {
            Ok(()) => report.completed = true,
            Err(error) => report.status_error = Some(error.to_string()),
        }
    }
    report.execution = Some(execution);
    report
}

async fn mark_settings_control_job_failed<Queue>(
    queue: &Queue,
    job_id: i64,
    error: &str,
    report: &mut SettingsControlJobWorkerReport,
) where
    Queue: SettingsControlJobWorkerQueue + Sync,
{
    match queue.fail_settings_control_job(job_id, error).await {
        Ok(()) => report.failed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
}

pub(crate) fn settings_control_job_failure_message(
    execution: &SettingsControlJobExecution,
) -> Option<String> {
    match execution {
        SettingsControlJobExecution::GroupSettings(
            GroupSettingsControlJobOutcome::PermissionCheckFailed(error),
        ) => Some(error.clone()),
        SettingsControlJobExecution::GroupSettings(
            GroupSettingsControlJobOutcome::BotUsernameMissing,
        ) => Some("bot username is empty".to_owned()),
        SettingsControlJobExecution::GroupSettings(
            GroupSettingsControlJobOutcome::UnsupportedKind(kind),
        )
        | SettingsControlJobExecution::NewMembersFollowup(
            NewMembersFollowupControlJobOutcome::UnsupportedKind(kind),
        ) => Some(format!("unsupported settings control job kind: {kind:?}")),
        SettingsControlJobExecution::GroupSettings(
            GroupSettingsControlJobOutcome::PermissionDenied,
        )
        | SettingsControlJobExecution::GroupSettings(GroupSettingsControlJobOutcome::SentLink)
        | SettingsControlJobExecution::NewMembersFollowup(
            NewMembersFollowupControlJobOutcome::Completed { .. },
        ) => None,
    }
}

fn is_settings_control_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == openplotva_taskman::JobType::Control
        && matches!(
            job.data.control_data.as_ref().map(|data| data.kind),
            Some(ControlKind::GroupSettings | ControlKind::NewMembersFollowup)
        )
}

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

pub async fn handle_group_settings_command_update_at<Queue, NextId>(
    dispatcher_queue: &DispatcherQueue,
    control_queue: &Queue,
    update: &TelegramUpdate,
    bot_username: &str,
    created: OffsetDateTime,
    next_virtual_id: NextId,
) -> Result<GroupSettingsCommandOutcome, OutboundBuildError>
where
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

pub async fn handle_new_members_followup_update_at<Queue, NextId>(
    dispatcher_queue: &DispatcherQueue,
    control_queue: &Queue,
    input: NewMembersFollowupUpdateInput<'_>,
    next_virtual_id: NextId,
) -> Result<NewMembersFollowupUpdateOutcome, OutboundBuildError>
where
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
            let direct_accumulation = control_queue.materializes_human_new_members_directly()
                && control_job_params_from_stateless_job(&job)
                    .is_ok_and(|params| !params.data.bot_was_added);
            let member_upsert_errors = 0;
            match control_queue
                .assign_settings_control_job(CONTROL_QUEUE_NAME, *job)
                .await
            {
                Ok(()) if direct_accumulation => Ok(NewMembersFollowupUpdateOutcome::Accumulated {
                    member_upsert_errors,
                }),
                Ok(()) => Ok(NewMembersFollowupUpdateOutcome::Queued {
                    member_upsert_errors,
                }),
                Err(error) => {
                    tracing::warn!(%error, "failed to assign new-members follow-up control job");
                    let notice = queue_group_settings_notice(
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

/// Handle settings-owned decoded-update behavior, delegating the rest.
pub async fn handle_settings_update_or_else_at<Queue, NextId, HandleFn, HandleFuture, HandleError>(
    ports: SettingsUpdatePorts<'_, Queue>,
    update: TelegramUpdate,
    context: SettingsUpdateContext<'_>,
    next_virtual_id: NextId,
    handle_other: HandleFn,
) -> Result<SettingsUpdateRoute, SettingsUpdateError>
where
    Queue: SettingsControlJobQueue + Sync,
    NextId: Fn() -> String,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let mut delegated_update = Some(update);
    let update_ref = delegated_update
        .as_ref()
        .expect("delegated update should remain available until delegation");

    let new_members = handle_new_members_followup_update_at(
        ports.dispatcher_queue,
        ports.control_queue,
        NewMembersFollowupUpdateInput {
            update: update_ref,
            bot_id: context.bot_id,
            created: context.created,
        },
        &next_virtual_id,
    )
    .await
    .map_err(settings_outbound_error)?;

    let ran_new_members = matches!(
        new_members,
        NewMembersFollowupUpdateOutcome::Queued { .. }
            | NewMembersFollowupUpdateOutcome::Accumulated { .. }
            | NewMembersFollowupUpdateOutcome::QueueError { .. }
    );

    let private_settings = handle_private_settings_command_update(
        ports.dispatcher_queue,
        update_ref,
        context.bot_username,
        context.web_app_url,
        &next_virtual_id,
    )
    .await
    .map_err(settings_outbound_error)?;
    match private_settings {
        SettingsCommandOutcome::Queued(_) | SettingsCommandOutcome::WebAppUrlMissing => {
            return Ok(SettingsUpdateRoute::PrivateSettings(private_settings));
        }
        SettingsCommandOutcome::NotSettingsCommand | SettingsCommandOutcome::NonPrivateChat => {}
    }

    let group_settings = handle_group_settings_command_update_at(
        ports.dispatcher_queue,
        ports.control_queue,
        update_ref,
        context.bot_username,
        context.created,
        &next_virtual_id,
    )
    .await
    .map_err(settings_outbound_error)?;
    match group_settings {
        GroupSettingsCommandOutcome::NotSettingsCommand
        | GroupSettingsCommandOutcome::PrivateChat => {}
        GroupSettingsCommandOutcome::UnsupportedSameChatSender(_)
        | GroupSettingsCommandOutcome::UnsupportedChannelSender(_)
        | GroupSettingsCommandOutcome::MissingCaller
        | GroupSettingsCommandOutcome::InvalidMessage
        | GroupSettingsCommandOutcome::QueueError(_)
        | GroupSettingsCommandOutcome::Queued { .. } => {
            return Ok(SettingsUpdateRoute::GroupSettings(group_settings));
        }
    }

    handle_other(
        delegated_update
            .take()
            .expect("delegated update should be available"),
    )
    .await
    .map_err(|error| SettingsUpdateError::Downstream {
        message: error.to_string(),
    })?;

    if ran_new_members {
        Ok(SettingsUpdateRoute::NewMembersFollowup {
            outcome: new_members,
        })
    } else {
        Ok(SettingsUpdateRoute::Delegated)
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

fn settings_outbound_error(error: OutboundBuildError) -> SettingsUpdateError {
    SettingsUpdateError::Outbound {
        message: error.to_string(),
    }
}

async fn queue_group_settings_notice<NextId>(
    queue: &DispatcherQueue,
    message: &TelegramMessage,
    text: &str,
    bypass_chat_restrictions: bool,
    ephemeral_delete_after: Option<Duration>,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
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
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
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

async fn queue_group_settings_control_text<NextId>(
    queue: &DispatcherQueue,
    params: &ControlJobParams,
    text: &str,
    reply_markup: Option<ReplyMarkup>,
    bypass_chat_restrictions: bool,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
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
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
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

async fn queue_new_members_followup_control_text<NextId>(
    queue: &DispatcherQueue,
    params: &ControlJobParams,
    reply_markup: Option<ReplyMarkup>,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
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
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
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

async fn queue_admin_chat_settings_text<NextId>(
    queue: &DispatcherQueue,
    message: &TelegramMessage,
    text: AdminChatSettingsText<'_>,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
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
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
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

async fn queue_admin_chat_settings_ephemeral_text<NextId>(
    queue: &DispatcherQueue,
    message: &TelegramMessage,
    text: &str,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    NextId: FnMut() -> String,
{
    let chat = message_chat_ref(message);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: message.message_thread_id.unwrap_or_default(),
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
        queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions: false,
            ephemeral_delete_after: Some(ADMIN_COMMAND_UNAUTHORIZED_DELETE_AFTER),
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

pub async fn try_send_greeting_for_join_wave_at<Cache, Settings>(
    cache: &Cache,
    settings: &Settings,
    greeting: &NewMembersGreeting,
    now: OffsetDateTime,
) -> bool
where
    Cache: NewMembersGreetingCache + Sync,
    Settings: NewMembersGreetingSettingsStore + Sync,
{
    if greeting.new_chat_member_ids.is_empty() {
        return false;
    }
    let Some(settings) = load_join_greeting_settings(settings, greeting.chat_id).await else {
        return false;
    };
    if !join_greeting_enabled(&settings) {
        return false;
    }

    if let Err(error) = cache
        .record_join_member_ids(
            greeting.chat_id,
            &greeting.new_chat_member_ids,
            now.unix_timestamp(),
            openplotva_storage::JOIN_GREETING_USERS_TTL,
        )
        .await
    {
        tracing::warn!(%error, chat_id = greeting.chat_id, "failed to record join-greeting users");
    }

    match cache
        .start_debounce(
            greeting.chat_id,
            openplotva_storage::JOIN_GREETING_DEBOUNCE_TTL,
        )
        .await
    {
        Ok(started) => started,
        Err(error) => {
            tracing::warn!(%error, chat_id = greeting.chat_id, "failed to start join-greeting debounce");
            false
        }
    }
}

pub async fn compose_and_send_greeting_at<Cache, Settings, Members, Sender>(
    cache: &Cache,
    settings: &Settings,
    members: &Members,
    sender: &Sender,
    greeting: &NewMembersGreeting,
    now: OffsetDateTime,
) where
    Cache: NewMembersGreetingCache + Sync,
    Settings: NewMembersGreetingSettingsStore + Sync,
    Members: NewMembersGreetingMemberDataStore + Sync,
    Sender: NewMembersGreetingSender + Sync,
{
    let min_score = now.unix_timestamp() - JOIN_GREETING_DEBOUNCE.as_secs() as i64;
    let user_ids = match cache
        .recent_join_member_ids(greeting.chat_id, min_score)
        .await
    {
        Ok(user_ids) => user_ids,
        Err(error) => {
            tracing::warn!(%error, chat_id = greeting.chat_id, "failed to load join-greeting users");
            return;
        }
    };
    let filtered = filter_active_greeting_member_ids(members, greeting.chat_id, &user_ids).await;
    if filtered.is_empty() {
        return;
    }

    let Some(settings) = load_join_greeting_settings(settings, greeting.chat_id).await else {
        return;
    };
    let Some(html_body) = settings
        .greeting_html
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };

    let names = greeting_member_links(members, &filtered).await;
    if names.is_empty() {
        return;
    }

    let text = join_greeting_text(&names, &openplotva_telegram::sanitize_rich_html(html_body));
    if let Ok(Some(previous_id)) = cache.previous_greeting_message_id(greeting.chat_id).await {
        sender
            .delete_previous_greeting_message(greeting.chat_id, previous_id)
            .await;
    }
    let message_id = sender
        .send_ephemeral_greeting(NewMembersGreetingMessage {
            chat_id: greeting.chat_id,
            reply_to_message_id: greeting.message_id,
            thread_id: greeting.thread_id,
            text,
            disable_notification: names.len() > 1,
            delete_after: JOIN_GREETING_DELETE_AFTER,
        })
        .await;
    if let Some(message_id) = message_id
        && let Err(error) = cache
            .set_previous_greeting_message_id(
                greeting.chat_id,
                message_id,
                openplotva_storage::JOIN_GREETING_MESSAGE_TTL,
            )
            .await
    {
        tracing::warn!(%error, chat_id = greeting.chat_id, message_id, "failed to save previous join-greeting message id");
    }
}

async fn load_join_greeting_settings<Settings>(
    settings: &Settings,
    chat_id: i64,
) -> Option<ChatSettings>
where
    Settings: NewMembersGreetingSettingsStore + Sync,
{
    match settings.greeting_chat_settings(chat_id).await {
        Ok(settings) => settings,
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to load join-greeting chat settings");
            None
        }
    }
}

fn join_greeting_enabled(settings: &ChatSettings) -> bool {
    settings.enable_greet_joiners
        && settings
            .greeting_html
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

async fn filter_active_greeting_member_ids<Members>(
    members: &Members,
    chat_id: i64,
    user_ids: &[String],
) -> Vec<String>
where
    Members: NewMembersGreetingMemberDataStore + Sync,
{
    let (parsed, unique_ids) = parse_greeting_member_ids(user_ids);
    if parsed.is_empty() {
        return Vec::new();
    }

    match members
        .list_chat_members_by_user_ids(chat_id, &unique_ids)
        .await
    {
        Ok(rows) => filter_greeting_member_candidates(&parsed, &rows),
        Err(error) => {
            tracing::warn!(%error, chat_id, "failed to batch-load join-greeting members");
            filter_greeting_member_candidates_one_by_one(members, chat_id, &parsed).await
        }
    }
}

async fn filter_greeting_member_candidates_one_by_one<Members>(
    members: &Members,
    chat_id: i64,
    parsed: &[GreetingMemberId],
) -> Vec<String>
where
    Members: NewMembersGreetingMemberDataStore + Sync,
{
    let mut active_by_id = HashMap::with_capacity(parsed.len());
    for candidate in parsed {
        let active = members
            .get_chat_member(chat_id, candidate.id)
            .await
            .ok()
            .flatten()
            .is_some_and(|member| is_active_participant_status(&member.status));
        active_by_id.insert(candidate.id, active);
    }
    filter_greeting_member_candidates_by_active_ids(parsed, &active_by_id)
}

fn filter_greeting_member_candidates(
    parsed: &[GreetingMemberId],
    members: &[ChatMemberRecord],
) -> Vec<String> {
    let active_by_id = members
        .iter()
        .map(|member| (member.user_id, is_active_participant_status(&member.status)))
        .collect::<HashMap<_, _>>();
    filter_greeting_member_candidates_by_active_ids(parsed, &active_by_id)
}

fn filter_greeting_member_candidates_by_active_ids(
    parsed: &[GreetingMemberId],
    active_by_id: &HashMap<i64, bool>,
) -> Vec<String> {
    parsed
        .iter()
        .filter(|candidate| active_by_id.get(&candidate.id).copied().unwrap_or(false))
        .map(|candidate| candidate.raw.clone())
        .collect()
}

async fn greeting_member_links<Members>(members: &Members, user_ids: &[String]) -> Vec<String>
where
    Members: NewMembersGreetingMemberDataStore + Sync,
{
    let (parsed, unique_ids) = parse_greeting_member_ids(user_ids);
    if parsed.is_empty() {
        return Vec::new();
    }

    let users_by_id = members
        .list_user_states_by_ids(&unique_ids)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|user| (user.id, user))
        .collect::<HashMap<_, _>>();

    let mut names = Vec::with_capacity(parsed.len());
    for candidate in parsed {
        let display_name = match users_by_id
            .get(&candidate.id)
            .and_then(user_link_display_name)
        {
            Some(name) => name,
            None => get_greeting_user_name(members, candidate.id).await,
        };
        let safe = openplotva_telegram::escape_telegram_html_text(&display_name);
        names.push(format_user_link(candidate.id, &safe));
    }
    names
}

async fn get_greeting_user_name<Members>(members: &Members, user_id: i64) -> String
where
    Members: NewMembersGreetingMemberDataStore + Sync,
{
    members
        .get_user_state(user_id)
        .await
        .ok()
        .flatten()
        .and_then(|user| user_link_display_name(&user))
        .unwrap_or_else(|| user_id.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GreetingMemberId {
    raw: String,
    id: i64,
}

fn parse_greeting_member_ids(user_ids: &[String]) -> (Vec<GreetingMemberId>, Vec<i64>) {
    let mut parsed = Vec::with_capacity(user_ids.len());
    let mut unique_ids = Vec::with_capacity(user_ids.len());
    let mut seen = HashMap::with_capacity(user_ids.len());
    for raw in user_ids {
        let Ok(id) = raw.parse::<i64>() else {
            continue;
        };
        parsed.push(GreetingMemberId {
            raw: raw.clone(),
            id,
        });
        if seen.insert(id, ()).is_none() {
            unique_ids.push(id);
        }
    }
    (parsed, unique_ids)
}

fn user_link_display_name(user: &UserState) -> Option<String> {
    if user
        .last_name
        .as_deref()
        .is_some_and(|last| !last.is_empty())
    {
        return Some(
            format!(
                "{} {}",
                user.first_name,
                user.last_name.as_deref().unwrap_or_default()
            )
            .trim()
            .to_owned(),
        );
    }
    if !user.first_name.is_empty() {
        return Some(user.first_name.clone());
    }
    if user
        .username
        .as_deref()
        .is_some_and(|name| !name.is_empty())
    {
        return Some(format!("@{}", user.username.as_deref().unwrap_or_default()));
    }
    Some(user.first_name.clone())
}

fn format_user_link(user_id: i64, safe_name: &str) -> String {
    format!("<a href='tg://user?id={user_id}'>{safe_name}</a>")
}

fn join_greeting_text(names: &[String], html_body: &str) -> String {
    if names.len() == 1 {
        return format!(
            "<h3>Добро пожаловать</h3><p>{}, добро пожаловать!</p>{html_body}",
            names[0]
        );
    }
    format!(
        "<h3>Новые участники</h3><ol>{}</ol>{html_body}",
        names
            .iter()
            .map(|name| format!("<li>{name}</li>"))
            .collect::<Vec<_>>()
            .join("")
    )
}

fn is_active_participant_status(status: &str) -> bool {
    let status = status.trim();
    status.eq_ignore_ascii_case(openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR)
        || status.eq_ignore_ascii_case(openplotva_storage::CHAT_MEMBER_STATUS_MEMBER)
        || status.eq_ignore_ascii_case(openplotva_storage::CHAT_MEMBER_STATUS_CREATOR)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdminChatSettingsCommand<'a> {
    name: &'a str,
    target: Option<&'a str>,
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
    let (name, target) = command_with_at
        .split_once('@')
        .map_or((command_with_at, None), |(name, target)| {
            (name, Some(target))
        });
    let arguments = command_arguments_after_command(&text.data, command_end)?;
    Some(AdminChatSettingsCommand {
        name,
        target,
        arguments,
    })
}

fn admin_chat_settings_command_for_message<'a>(
    message: &'a TelegramMessage,
    bot_username: &str,
) -> Option<AdminChatSettingsCommand<'a>> {
    let command = admin_chat_settings_command_from_message(message)?;
    if matches!(message.chat, TelegramChat::Private(_)) {
        return Some(command);
    }
    (command.target == Some(bot_username)).then_some(command)
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
        is_member: Some(member.is_member()),
        is_anonymous: Some(telegram_chat_member_is_anonymous(member)),
        can_be_edited: Some(telegram_chat_member_can_be_edited(member)),
        ..ChatMemberUpsert::default()
    };
    apply_chat_member_role_permissions(&mut params, member);
    apply_chat_member_send_permissions(&mut params, member);
    params
}

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
        is_member: Some(true),
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

pub(crate) fn user_state_from_telegram_user(user: &TelegramUser) -> UserState {
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
        env,
        error::Error,
        fmt, io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::{
        ChatMember, ChatMemberAdministrator, ChatMemberCreator, Update as TelegramUpdate, User,
    };
    use openplotva_core::{ChatSettings, UserSettings, UserState};
    use openplotva_storage::{ChatMemberRecord, ChatMemberUpsert};
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind,
        DIALOG_AIFARM_QUEUE_NAME, HIGH_PRIORITY, JobType, StatelessJobItem, new_control_job_at,
    };
    use openplotva_telegram::{DispatcherConfig, DispatcherQueue, TelegramOutboundMethodKind};
    use serde_json::{Value, json};
    use time::OffsetDateTime;

    use crate::dialog_messages::{
        DialogMessageFuture, DialogMessageSchedule, DialogMessageScheduleReport,
        DialogMessageScheduler, DialogMessageUpdateConfig, DialogMessageUpdateHandler,
        DirectSongNoticeEffects, DirectSongNoticeFuture, DirectSongNoticePlan, RandomDialogEffects,
        RandomDialogRng, RandomDialogSettingsStore, RandomObscenifiedTextPlan,
    };
    use crate::help::{HelpBotIdentity, HelpCommandUpdateHandler, HelpDispatcherEffects};
    use crate::payments::{InMemoryPaymentControlJobQueue, InMemoryPaymentControlJobStatus};
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };
    use crate::virtual_messages::VirtualIdFactory;
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};

    use super::{
        AdminChatSettingsCommandUpdateHandler, AdminChatSettingsCommandUpdateOutcome,
        AdminChatSettingsCommandUpdateRuntime, GroupSettingsAdminSyncSource,
        GroupSettingsCommandOutcome, GroupSettingsControlJobBuild, GroupSettingsControlJobEffects,
        GroupSettingsRuntimeEffects, ProjectionAwareSettingsControlQueue, SettingsCommandOutcome,
        SettingsControlJobExecution, SettingsControlJobQueue, SettingsControlJobQueueFuture,
        SettingsUpdateContext, SettingsUpdateHandler, SettingsUpdateHandlerConfig,
        SettingsUpdatePorts, SettingsUpdateRoute, admin_chat_member_upsert_from_telegram,
        can_open_group_settings_with_sources, chat_member_upsert_from_telegram,
        execute_group_settings_control_job_at, execute_new_members_followup_control_job_at,
        group_settings_control_job_from_update_at,
        handle_admin_chat_settings_command_update_or_else_at,
        handle_group_settings_command_update_at, handle_new_members_followup_update_at,
        handle_private_settings_command_update, handle_settings_update_or_else_at,
        new_members_followup_control_job_from_update_at, process_settings_control_job_once_at,
        sync_chat_admins_with_cache, sync_chat_admins_with_sources,
    };

    #[tokio::test]
    async fn settings_control_jobs_can_use_shared_persistent_control_queue()
    -> Result<(), Box<dyn Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let job = new_control_job_at(
            ControlJobParams {
                chat_id: -10042,
                message_id: 77,
                user_id: 42,
                user_full_name: "Owner".to_owned(),
                thread_id: Some(5),
                data: ControlJobData {
                    kind: ControlKind::GroupSettings,
                    ..ControlJobData::default()
                },
            },
            OffsetDateTime::UNIX_EPOCH,
        );

        queue
            .assign_settings_control_job(CONTROL_QUEUE_NAME, job.clone())
            .await?;

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].queue_name, CONTROL_QUEUE_NAME);
        assert_eq!(snapshot[0].job, job);
        assert_eq!(
            snapshot[0]
                .job
                .data
                .control_data
                .as_ref()
                .map(|data| data.kind),
            Some(ControlKind::GroupSettings)
        );
        Ok(())
    }

    #[tokio::test]
    async fn settings_control_worker_skips_payment_job_and_completes_group_settings_job()
    -> Result<(), Box<dyn Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let dispatcher_queue = DispatcherQueue::new(DispatcherConfig::default());
        let group_effects = GroupSettingsEffectsStub::allowing();
        let new_members_effects = NewMembersFollowupEffectsStub::blocked();
        let payment_job = settings_test_control_job(ControlKind::VipInvoice, "vip invoice");
        let settings_job = settings_test_control_job(ControlKind::GroupSettings, "group settings");
        queue
            .assign_settings_control_job(CONTROL_QUEUE_NAME, payment_job)
            .await?;
        queue
            .assign_settings_control_job(CONTROL_QUEUE_NAME, settings_job.clone())
            .await?;

        let report = process_settings_control_job_once_at(
            &queue,
            &dispatcher_queue,
            &group_effects,
            &new_members_effects,
            "PlotvaBot",
            || "settings-worker-v1".to_owned(),
        )
        .await;

        assert!(report.dequeued);
        assert_eq!(report.job_id, Some(2));
        assert_eq!(
            report.execution,
            Some(SettingsControlJobExecution::GroupSettings(
                super::GroupSettingsControlJobOutcome::SentLink
            ))
        );
        assert!(report.completed);
        assert!(!report.failed);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot[0].status, InMemoryPaymentControlJobStatus::Pending);
        assert_eq!(
            snapshot[1].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(snapshot[1].job, settings_job);
        assert_eq!(group_effects.synced_admin_chats(), vec![-10042]);
        let item = dispatcher_queue
            .dequeue_immediate()
            .expect("settings worker should queue settings deep-link text");
        assert!(item.bypasses_chat_restrictions());
        Ok(())
    }

    #[tokio::test]
    async fn settings_control_worker_completes_new_members_followup_job()
    -> Result<(), Box<dyn Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let dispatcher_queue = DispatcherQueue::new(DispatcherConfig::default());
        let group_effects = GroupSettingsEffectsStub::allowing();
        let new_members_effects = NewMembersFollowupEffectsStub::blocked();
        let job = new_control_job_at(
            new_members_followup_control_params(true),
            OffsetDateTime::UNIX_EPOCH,
        )
        .with_name("new members followup")
        .with_priority(HIGH_PRIORITY);
        queue
            .assign_settings_control_job(CONTROL_QUEUE_NAME, job.clone())
            .await?;

        let report = process_settings_control_job_once_at(
            &queue,
            &dispatcher_queue,
            &group_effects,
            &new_members_effects,
            "PlotvaBot",
            || "new-members-worker-v1".to_owned(),
        )
        .await;

        assert_eq!(report.job_id, Some(1));
        assert_eq!(
            report.execution,
            Some(SettingsControlJobExecution::NewMembersFollowup(
                super::NewMembersFollowupControlJobOutcome::Completed {
                    unblock_notice_queued: true
                }
            ))
        );
        assert!(report.completed);
        let snapshot = queue.snapshot();
        assert_eq!(
            snapshot[0].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(snapshot[0].job, job);
        assert_eq!(new_members_effects.enabled_chats(), vec![-10042]);
        let item = dispatcher_queue
            .dequeue_immediate()
            .expect("new-members worker should queue unblock settings text");
        assert!(item.bypasses_chat_restrictions());
        Ok(())
    }

    #[tokio::test]
    async fn private_settings_command_queues_go_webapp_button_with_bypass_and_immediate()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = private_settings_update()?;

        let outcome = handle_private_settings_command_update(
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
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = private_settings_update()?;

        let outcome =
            handle_private_settings_command_update(&queue, &update, "PlotvaBot", "", || {
                "settings-v1".to_owned()
            })
            .await?;

        assert_eq!(outcome, SettingsCommandOutcome::WebAppUrlMissing);
        assert!(queue.snapshot().immediate.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn private_settings_handler_leaves_group_settings_to_group_path()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let update = group_settings_update()?;

        let outcome = handle_private_settings_command_update(
            &queue,
            &update,
            "PlotvaBot",
            "https://plotva.example",
            || "settings-v1".to_owned(),
        )
        .await?;

        assert_eq!(outcome, SettingsCommandOutcome::NonPrivateChat);
        assert!(queue.snapshot().immediate.is_empty());
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = same_chat_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
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
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_command_queue_error_sends_go_failure_notice()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::failing();
        let update = group_settings_update()?;
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_group_settings_command_update_at(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::default();
        let update = admin_chat_settings_update("/admin_chat_settings   ")?;

        let outcome = super::handle_admin_chat_settings_command_update(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::failing();
        let update = admin_chat_settings_update("/admin_chat_settings missing_chat")?;

        let outcome = super::handle_admin_chat_settings_command_update(
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

    #[tokio::test]
    async fn admin_chat_settings_update_wrapper_rejects_unauthorized_with_ephemeral_denial()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::default();
        let next = UpdateHandlerStub::default();
        let admin_ids = [7];
        let next_virtual_id: VirtualIdFactory =
            Arc::new(|| "admin-chat-settings-denial-v1".to_owned());

        let outcome = handle_admin_chat_settings_command_update_or_else_at(
            AdminChatSettingsCommandUpdateRuntime {
                dispatcher_queue: &dispatcher,
                resolver: &resolver,
                admin_ids: &admin_ids,
                bot_username: "PlotvaBot",
                web_app_url: "https://plotva.example",
                next_virtual_id: &next_virtual_id,
            },
            admin_chat_settings_update("/admin_chat_settings @target_lab")?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, AdminChatSettingsCommandUpdateOutcome::Unauthorized);
        assert!(next.calls().is_empty());
        assert!(resolver.calls().is_empty());
        let item = dispatcher
            .dequeue_immediate()
            .expect("admin chat settings denial should enqueue immediately");
        assert_eq!(item.ephemeral_delete_after(), Some(Duration::from_secs(60)));
        let method = item.into_method().expect("queued denial method");
        let value = serde_json::to_value(method_as_value(method)?)?;
        assert_eq!(
            value["text"],
            json!("❌ У вас нет прав на выполнение этой команды.")
        );
        assert_eq!(value["reply_parameters"]["message_id"], json!(79));
        Ok(())
    }

    #[tokio::test]
    async fn admin_chat_settings_update_wrapper_handles_authorized_command_before_next()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let resolver = AdminChatTargetResolverStub::with_target(super::AdminChatSettingsTarget {
            id: -100777,
            title: "Target Lab".to_owned(),
            username: "target_lab".to_owned(),
            first_name: String::new(),
            last_name: String::new(),
        });
        let next = UpdateHandlerStub::default();
        let admin_ids = [42];
        let next_virtual_id: VirtualIdFactory = Arc::new(|| "admin-chat-settings-v1".to_owned());

        let outcome = handle_admin_chat_settings_command_update_or_else_at(
            AdminChatSettingsCommandUpdateRuntime {
                dispatcher_queue: &dispatcher,
                resolver: &resolver,
                admin_ids: &admin_ids,
                bot_username: "PlotvaBot",
                web_app_url: "https://plotva.example",
                next_virtual_id: &next_virtual_id,
            },
            admin_chat_settings_update("/admin_chat_settings @target_lab")?,
            |update| next.handle_update(update),
        )
        .await?;

        assert!(matches!(
            outcome,
            AdminChatSettingsCommandUpdateOutcome::Handled(
                super::AdminChatSettingsCommandOutcome::Queued(_)
            )
        ));
        assert!(next.calls().is_empty());
        assert_eq!(resolver.calls(), vec!["@target_lab".to_owned()]);
        assert!(dispatcher.dequeue_immediate().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn admin_chat_settings_update_handler_delegates_group_command_without_bot_target()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let resolver = Arc::new(AdminChatTargetResolverStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = AdminChatSettingsCommandUpdateHandler::new(
            dispatcher,
            resolver,
            vec![42],
            "PlotvaBot",
            "https://plotva.example",
            Arc::clone(&next),
        );

        handler
            .handle_update(group_admin_chat_settings_update(
                "/admin_chat_settings @target_lab",
            )?)
            .await?;

        assert_eq!(next.calls(), vec![12351]);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_admin_chat_settings_command_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-admin-chat-settings:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&admin_chat_settings_update(
                "/admin_chat_settings @target_lab",
            )?)
            .await?;
        update_queue
            .enqueue_update(&group_admin_chat_settings_update(
                "/admin_chat_settings@PlotvaBot @target_lab",
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 2);

        let dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let resolver = Arc::new(AdminChatTargetResolverStub::with_target(
            super::AdminChatSettingsTarget {
                id: -100777,
                title: "Target Lab".to_owned(),
                username: "target_lab".to_owned(),
                first_name: String::new(),
                last_name: String::new(),
            },
        ));
        let terminal = Arc::new(UpdateHandlerStub::default());
        let next_counter = Arc::new(AtomicU64::new(1));
        let next_counter_for_factory = Arc::clone(&next_counter);
        let ids: VirtualIdFactory = Arc::new(move || {
            let id = next_counter_for_factory.fetch_add(1, Ordering::Relaxed);
            format!("admin-chat-settings-live-v{id}")
        });
        let handler = AdminChatSettingsCommandUpdateHandler::new(
            Arc::clone(&dispatcher),
            Arc::clone(&resolver),
            vec![42],
            "PlotvaBot",
            "https://plotva.example",
            Arc::clone(&terminal),
        )
        .with_virtual_id_factory(ids);
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_010);

        for expected in [
            "expected private admin-chat-settings update",
            "expected group admin-chat-settings update",
        ] {
            let update = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other(expected))?;
            let report =
                process_update_with_state_store_at(update, config, now, &state_store, |update| {
                    handler.handle_update(update)
                })
                .await;
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
        }

        assert!(terminal.calls().is_empty());
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private".to_owned(),
                "user:42:Ada".to_owned(),
                "chat:-10042:supergroup".to_owned(),
                "user:42:Ada".to_owned(),
            ]
        );
        assert_eq!(
            resolver.calls(),
            vec!["@target_lab".to_owned(), "@target_lab".to_owned()]
        );
        let private_item = dispatcher
            .dequeue_immediate()
            .expect("private admin chat settings should enqueue immediately");
        assert!(private_item.bypasses_chat_restrictions());
        assert_eq!(private_item.ephemeral_delete_after(), None);
        let private_value = serde_json::to_value(method_as_value(
            private_item
                .into_method()
                .expect("private admin chat settings method"),
        )?)?;
        assert_eq!(private_value["chat_id"], json!(42));
        assert_eq!(
            private_value["text"],
            json!("Откройте настройки для чата \"Target Lab\" (ID: -100777):")
        );
        assert_eq!(
            private_value["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/?chat_id=-100777&signature=b8e86493")
        );

        let group_item = dispatcher
            .dequeue_immediate()
            .expect("group admin chat settings should enqueue immediately");
        assert!(group_item.bypasses_chat_restrictions());
        assert_eq!(group_item.ephemeral_delete_after(), None);
        let group_value = serde_json::to_value(method_as_value(
            group_item
                .into_method()
                .expect("group admin chat settings method"),
        )?)?;
        assert_eq!(group_value["chat_id"], json!(-10042));
        assert_eq!(group_value["message_thread_id"], json!(99));
        assert_eq!(group_value["reply_parameters"]["message_id"], json!(80));
        assert_eq!(
            group_value["text"],
            json!("Откройте настройки для чата \"Target Lab\" (ID: -100777):")
        );
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::allowing();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::denying();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::failing_permission_check();
        let params = group_settings_control_params();

        let outcome = execute_group_settings_control_job_at(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = GroupSettingsEffectsStub::allowing();
        let params = group_settings_control_params();

        let outcome =
            execute_group_settings_control_job_at(&dispatcher, &effects, &params, "", || {
                "settings-link-v1".to_owned()
            })
            .await?;

        assert_eq!(
            outcome,
            super::GroupSettingsControlJobOutcome::BotUsernameMissing
        );
        assert_eq!(effects.permission_checks(), vec![(-10042, 42)]);
        assert!(effects.synced_admin_chats().is_empty());
        assert!(dispatcher.snapshot().immediate.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_executor_enables_chat_and_queues_blocked_notice_when_bot_added()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = NewMembersFollowupEffectsStub::blocked();
        let params = new_members_followup_control_params(true);

        let outcome = execute_new_members_followup_control_job_at(
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
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = NewMembersFollowupEffectsStub::blocked();
        let params = new_members_followup_control_params(false);

        let outcome = execute_new_members_followup_control_job_at(
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
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert!(dispatcher.snapshot().regular.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_executor_keeps_empty_settings_url_when_username_missing()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let effects = NewMembersFollowupEffectsStub::blocked();
        let params = new_members_followup_control_params(true);

        let outcome =
            execute_new_members_followup_control_job_at(&dispatcher, &effects, &params, "", || {
                "new-members-empty-url-v1".to_owned()
            })
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

    #[tokio::test]
    async fn join_greeting_records_users_and_composes_debounced_html_greeting()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let cache = JoinGreetingCacheStub::with_recent(vec!["7", "8"]);
        cache.set_previous(Some(55));
        let settings = JoinGreetingSettingsStub::enabled("<b>welcome</b>");
        let members = JoinGreetingMemberDataStub::default();
        members.set_users(vec![
            UserState::new(7, "Alice", None, None, None, None),
            UserState::new(8, "Bob", Some("Builder".to_owned()), None, None, None),
        ]);
        members.set_members(vec![
            ChatMemberRecord {
                chat_id: -10042,
                user_id: 7,
                status: openplotva_storage::CHAT_MEMBER_STATUS_MEMBER.to_owned(),
                ..ChatMemberRecord::default()
            },
            ChatMemberRecord {
                chat_id: -10042,
                user_id: 8,
                status: openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR.to_owned(),
                ..ChatMemberRecord::default()
            },
        ]);
        let sender = JoinGreetingSenderStub::with_next_message_id(Some(77));
        let greeting = super::NewMembersGreeting {
            chat_id: -10042,
            message_id: 88,
            thread_id: Some(99),
            new_chat_member_ids: vec![7, 8],
        };

        assert!(super::try_send_greeting_for_join_wave_at(&cache, &settings, &greeting, now).await);
        super::compose_and_send_greeting_at(&cache, &settings, &members, &sender, &greeting, now)
            .await;

        assert_eq!(cache.recorded(), vec![(-10042, vec![7, 8], 1_710_000_000)]);
        assert_eq!(cache.debounce_started(), vec![-10042]);
        assert_eq!(sender.deleted(), vec![(-10042, 55)]);
        assert_eq!(
            sender.sent(),
            vec![super::NewMembersGreetingMessage {
                chat_id: -10042,
                reply_to_message_id: 88,
                thread_id: Some(99),
                text: "<h3>Новые участники</h3><ol><li><a href='tg://user?id=7'>Alice</a></li><li><a href='tg://user?id=8'>Bob Builder</a></li></ol><b>welcome</b>".to_owned(),
                disable_notification: true,
                delete_after: Duration::from_secs(10 * 60),
            }]
        );
        assert_eq!(cache.saved_previous(), vec![(-10042, 77)]);
        Ok(())
    }

    #[tokio::test]
    async fn join_greeting_skips_redis_when_settings_disable_greetings()
    -> Result<(), Box<dyn Error>> {
        let cache = JoinGreetingCacheStub::with_recent(vec!["7"]);
        let settings = JoinGreetingSettingsStub::disabled();
        let greeting = super::NewMembersGreeting {
            chat_id: -10042,
            message_id: 88,
            thread_id: None,
            new_chat_member_ids: vec![7],
        };

        assert!(
            !super::try_send_greeting_for_join_wave_at(
                &cache,
                &settings,
                &greeting,
                OffsetDateTime::from_unix_timestamp(1_710_000_000)?,
            )
            .await
        );
        assert!(cache.recorded().is_empty());
        assert!(cache.debounce_started().is_empty());
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
    async fn new_members_followup_update_relies_on_projection_and_assigns_control_job()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = new_members_update()?;

        let outcome = handle_new_members_followup_update_at(
            &dispatcher,
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
        let assigned = control_queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "new members followup");
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert!(dispatcher.snapshot().regular.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn human_new_members_accumulate_without_member_write_or_control_job()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let persistent_queue = Arc::new(SettingsControlJobQueueStub::default());
        let greeting = Arc::new(NewMembersFollowupEffectsStub::blocked());
        let control_queue = ProjectionAwareSettingsControlQueue::new(
            Arc::clone(&persistent_queue),
            Arc::clone(&greeting),
        );
        let update = human_new_members_update()?;

        let outcome = handle_new_members_followup_update_at(
            &dispatcher,
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
            super::NewMembersFollowupUpdateOutcome::Accumulated {
                member_upsert_errors: 0
            }
        );
        assert!(persistent_queue.assigned().is_empty());
        assert_eq!(
            greeting.greetings(),
            vec![super::NewMembersGreeting {
                chat_id: -10042,
                message_id: 88,
                thread_id: Some(99),
                new_chat_member_ids: vec![7],
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn new_members_followup_update_queue_error_sends_go_failure_notice()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::failing();
        let update = new_members_update()?;

        let outcome = handle_new_members_followup_update_at(
            &dispatcher,
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
    async fn settings_update_wrapper_handles_private_settings_without_delegation()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = private_settings_update()?;
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_settings_update_or_else_at(
            SettingsUpdatePorts {
                dispatcher_queue: &dispatcher,
                control_queue: &control_queue,
            },
            update,
            SettingsUpdateContext {
                bot_username: "PlotvaBot",
                bot_id: 999,
                web_app_url: "https://plotva.example",
                created: OffsetDateTime::UNIX_EPOCH,
            },
            || "settings-wrapper-v1".to_owned(),
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await?;

        let SettingsUpdateRoute::PrivateSettings(SettingsCommandOutcome::Queued(report)) = route
        else {
            return Err(format!("expected private settings route, got {route:?}").into());
        };
        assert_eq!(report.parts[0].virtual_id, "settings-wrapper-v1");
        assert!(fallback_calls.lock().expect("fallback calls").is_empty());
        assert!(control_queue.assigned().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn settings_update_wrapper_runs_new_members_then_delegates_like_go()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = DispatcherQueue::new(DispatcherConfig::default());
        let control_queue = SettingsControlJobQueueStub::default();
        let update = new_members_update()?;
        let fallback_calls = Arc::new(Mutex::new(Vec::new()));
        let fallback_calls_for_handle = Arc::clone(&fallback_calls);

        let route = handle_settings_update_or_else_at(
            SettingsUpdatePorts {
                dispatcher_queue: &dispatcher,
                control_queue: &control_queue,
            },
            update,
            SettingsUpdateContext {
                bot_username: "PlotvaBot",
                bot_id: 999,
                web_app_url: "https://plotva.example",
                created: OffsetDateTime::UNIX_EPOCH,
            },
            || "unused-settings-wrapper-v1".to_owned(),
            move |update| {
                let fallback_calls = Arc::clone(&fallback_calls_for_handle);
                async move {
                    fallback_calls
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(
            route,
            SettingsUpdateRoute::NewMembersFollowup {
                outcome: super::NewMembersFollowupUpdateOutcome::Queued {
                    member_upsert_errors: 0
                }
            }
        );
        assert_eq!(
            fallback_calls.lock().expect("fallback calls").as_slice(),
            &[12350]
        );
        assert_eq!(control_queue.assigned().len(), 1);
        assert!(dispatcher.snapshot().immediate.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn settings_update_handler_intercepts_group_settings_before_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let control_queue = Arc::new(SettingsControlJobQueueStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let ids: VirtualIdFactory = Arc::new(|| "settings-handler-v1".to_owned());
        let handler = SettingsUpdateHandler::new(
            Arc::clone(&dispatcher),
            Arc::clone(&control_queue),
            SettingsUpdateHandlerConfig::new("PlotvaBot", 999, "https://plotva.example"),
            Arc::clone(&next),
        )
        .with_virtual_id_factory(ids);

        handler
            .handle_update(group_settings_update()?)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;

        assert!(next.calls().is_empty());
        assert_eq!(control_queue.assigned().len(), 1);
        assert_eq!(control_queue.assigned()[0].1.title, "group settings");
        assert_eq!(dispatcher.snapshot().immediate.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_private_start_settings_delegates_into_settings_handler_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-start-settings:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&private_start_settings_update()?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);

        let settings_dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let help_dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let control_queue = Arc::new(InMemoryPaymentControlJobQueue::new());
        let terminal = Arc::new(UpdateHandlerStub::default());
        let dialog_terminal = Arc::new(DialogMessageUpdateHandler::new(
            Arc::new(SettingsDialogSchedulerStub),
            Arc::new(SettingsDialogStoreStub),
            Arc::new(SettingsDialogEffectsStub),
            Arc::new(SettingsDialogRngStub),
            settings_dialog_config(),
            Arc::clone(&terminal),
        ));
        let ids: VirtualIdFactory = Arc::new(|| "start-settings-v1".to_owned());
        let settings_handler = Arc::new(
            SettingsUpdateHandler::new(
                Arc::clone(&settings_dispatcher),
                Arc::clone(&control_queue),
                SettingsUpdateHandlerConfig::new("PlotvaBot", 999, "https://plotva.example"),
                dialog_terminal,
            )
            .with_virtual_id_factory(ids),
        );
        let telegram = openplotva_telegram::telegram_client("123:token")?;
        let help_effects = Arc::new(HelpDispatcherEffects::new(
            telegram,
            Arc::clone(&settings_handler),
            Arc::new(crate::rich::MockRichSender::default()),
        ));
        let help_handler = HelpCommandUpdateHandler::new(
            HelpBotIdentity {
                first_name: "Plotva".to_owned(),
                username: "PlotvaBot".to_owned(),
                token: "123:token".to_owned(),
            },
            help_effects,
            Arc::clone(&terminal),
        );
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let update = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded private start settings update"))?;
        let report = process_update_with_state_store_at(
            update,
            config,
            UNIX_EPOCH + Duration::from_secs(1_710_000_010),
            &state_store,
            |update| help_handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 12352);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            vec!["chat:42:private".to_owned(), "user:42:Ada".to_owned()]
        );
        assert!(terminal.calls().is_empty());
        assert!(control_queue.snapshot().is_empty());
        assert!(help_dispatcher.snapshot().immediate.is_empty());
        assert!(help_dispatcher.snapshot().regular.is_empty());

        let private_item = settings_dispatcher
            .dequeue_immediate()
            .expect("start settings should delegate into private settings button");
        assert!(private_item.bypasses_chat_restrictions());
        assert_eq!(private_item.ephemeral_delete_after(), None);
        let private_value = serde_json::to_value(method_as_value(
            private_item
                .into_method()
                .expect("private start settings should become sendMessage"),
        )?)?;
        assert_eq!(private_value["chat_id"], json!(42));
        assert_eq!(private_value["text"], json!("Откройте настройки бота:"));
        assert_eq!(private_value["reply_parameters"]["message_id"], json!(81));
        assert_eq!(
            private_value["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/index.html?signature=780e28cf")
        );
        assert!(settings_dispatcher.snapshot().immediate.is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_settings_commands_and_new_members_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-settings:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&private_settings_update()?)
            .await?;
        update_queue
            .enqueue_update(&group_settings_update()?)
            .await?;
        update_queue.enqueue_update(&new_members_update()?).await?;
        assert_eq!(update_queue.len().await?, 3);

        let dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let control_queue = Arc::new(InMemoryPaymentControlJobQueue::new());
        let terminal = Arc::new(UpdateHandlerStub::default());
        let dialog_terminal = Arc::new(DialogMessageUpdateHandler::new(
            Arc::new(SettingsDialogSchedulerStub),
            Arc::new(SettingsDialogStoreStub),
            Arc::new(SettingsDialogEffectsStub),
            Arc::new(SettingsDialogRngStub),
            settings_dialog_config(),
            Arc::clone(&terminal),
        ));
        let next_counter = Arc::new(AtomicU64::new(1));
        let next_counter_for_factory = Arc::clone(&next_counter);
        let ids: VirtualIdFactory = Arc::new(move || {
            let id = next_counter_for_factory.fetch_add(1, Ordering::Relaxed);
            format!("settings-live-v{id}")
        });
        let handler = SettingsUpdateHandler::new(
            Arc::clone(&dispatcher),
            Arc::clone(&control_queue),
            SettingsUpdateHandlerConfig::new("PlotvaBot", 999, "https://plotva.example"),
            dialog_terminal,
        )
        .with_virtual_id_factory(ids);
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_010);

        for expected in [
            "expected private settings update",
            "expected group settings update",
            "expected new-members settings update",
        ] {
            let update = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other(expected))?;
            let report =
                process_update_with_state_store_at(update, config, now, &state_store, |update| {
                    handler.handle_update(update)
                })
                .await;
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
        }

        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private".to_owned(),
                "user:42:Ada".to_owned(),
                "chat:-10042:supergroup".to_owned(),
                "user:42:Ada".to_owned(),
                "chat:-10042:supergroup".to_owned(),
                "user:42:Ada".to_owned(),
            ]
        );
        assert!(terminal.calls().is_empty());

        let snapshot = control_queue.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(
            snapshot
                .iter()
                .filter_map(|item| item.job.data.control_data.as_ref().map(|data| data.kind))
                .collect::<Vec<_>>(),
            vec![ControlKind::GroupSettings, ControlKind::NewMembersFollowup]
        );
        assert_eq!(
            snapshot
                .iter()
                .map(|item| item.queue_name.as_str())
                .collect::<Vec<_>>(),
            vec![CONTROL_QUEUE_NAME, CONTROL_QUEUE_NAME]
        );

        let private_item = dispatcher
            .dequeue_immediate()
            .expect("private settings should enqueue settings button");
        assert!(private_item.bypasses_chat_restrictions());
        assert_eq!(private_item.ephemeral_delete_after(), None);
        let private_value = serde_json::to_value(method_as_value(
            private_item
                .into_method()
                .expect("private settings should be a sendMessage method"),
        )?)?;
        assert_eq!(private_value["chat_id"], json!(42));
        assert_eq!(private_value["text"], json!("Откройте настройки бота:"));
        assert_eq!(
            private_value["reply_markup"]["inline_keyboard"][0][0]["web_app"]["url"],
            json!("https://plotva.example/settings/index.html?signature=780e28cf")
        );

        let group_item = dispatcher
            .dequeue_immediate()
            .expect("group settings should enqueue wait notice");
        assert!(group_item.bypasses_chat_restrictions());
        assert_eq!(
            group_item.ephemeral_delete_after(),
            Some(Duration::from_secs(60))
        );
        let group_value = serde_json::to_value(method_as_value(
            group_item
                .into_method()
                .expect("group settings should be a sendMessage method"),
        )?)?;
        assert_eq!(group_value["chat_id"], json!(-10042));
        assert_eq!(
            group_value["text"],
            json!("⏳ Проверяю права и готовлю ссылку на настройки...")
        );
        assert_eq!(group_value["message_thread_id"], json!(99));
        assert!(dispatcher.snapshot().immediate.is_empty());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
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
    async fn group_settings_runtime_effects_check_permissions_through_sources()
    -> Result<(), Box<dyn Error>> {
        let member_store = GroupSettingsMemberStoreStub::with_member(ChatMemberRecord {
            chat_id: -10042,
            user_id: 42,
            status: openplotva_storage::CHAT_MEMBER_STATUS_CREATOR.to_owned(),
            ..ChatMemberRecord::default()
        });
        let member_api = GroupSettingsMemberApiStub::failing();
        let admin_store = GroupSettingsAdminSyncStoreStub::default();
        let admin_api = GroupSettingsAdminsApiStub::failing();
        let cache = GroupSettingsAdminCacheStub::default();
        let effects = GroupSettingsRuntimeEffects::new(
            member_store.clone(),
            member_api.clone(),
            admin_store,
            admin_api,
            cache,
        );

        let allowed = effects.can_open_group_settings(-10042, 42).await?;

        assert!(allowed);
        assert_eq!(member_store.get_calls(), vec![(-10042, 42)]);
        assert!(member_api.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn group_settings_runtime_effects_sync_admins_with_cache_nonfatal()
    -> Result<(), Box<dyn Error>> {
        let member_store = GroupSettingsMemberStoreStub::default();
        let member_api = GroupSettingsMemberApiStub::failing();
        let admin_store = GroupSettingsAdminSyncStoreStub::default();
        let admin_api = GroupSettingsAdminsApiStub::with_admins(vec![promoting_admin_member(42)]);
        let cache = GroupSettingsAdminCacheStub::default();
        let effects = GroupSettingsRuntimeEffects::new(
            member_store,
            member_api,
            admin_store.clone(),
            admin_api.clone(),
            cache.clone(),
        );

        effects.sync_chat_admins(-10042).await;

        assert_eq!(admin_api.calls(), vec![-10042]);
        assert_eq!(admin_store.upserts().len(), 1);
        assert_eq!(
            cache.saves(),
            vec![(-10042, vec![42], Duration::from_secs(30 * 60))]
        );
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
            "Admin IDs are cached only after an API success, not DB fallback"
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

    #[tokio::test]
    #[ignore = "live Telegram Bot API membership smoke"]
    async fn live_telegram_membership_smoke_gets_member_and_admins() -> Result<(), Box<dyn Error>> {
        let bot_key = std::env::var("BOT_KEY")
            .map_err(|_| "BOT_KEY is required for live Telegram membership smoke")?;
        let chat_id = std::env::var("OPENPLOTVA_SMOKE_CHAT_ID")
            .map_err(|_| "OPENPLOTVA_SMOKE_CHAT_ID is required")?
            .parse::<i64>()?;
        let user_id = std::env::var("OPENPLOTVA_SMOKE_USER_ID")
            .map_err(|_| "OPENPLOTVA_SMOKE_USER_ID is required")?
            .parse::<i64>()?;
        let bot_api_base_url = std::env::var("BOT_API_BASE_URL").unwrap_or_default();
        let telegram =
            openplotva_telegram::telegram_client_with_base_url(bot_key, bot_api_base_url)?;

        let member =
            super::GroupSettingsMemberApi::get_chat_member(&telegram, chat_id, user_id).await?;
        let upsert = chat_member_upsert_from_telegram(chat_id, user_id, &member);
        assert_eq!(upsert.chat_id, chat_id);
        assert_eq!(upsert.user_id, user_id);
        assert!(
            !upsert.status.trim().is_empty(),
            "Telegram member status must be present"
        );

        let admins =
            super::GroupSettingsAdminsApi::get_chat_administrators(&telegram, chat_id).await?;
        assert!(
            !admins.is_empty(),
            "Telegram group should report at least one administrator"
        );
        for admin in admins {
            let Some(admin_upsert) = admin_chat_member_upsert_from_telegram(chat_id, &admin) else {
                panic!("getChatAdministrators returned a non-admin member");
            };
            assert_eq!(admin_upsert.chat_id, chat_id);
            assert!(
                matches!(
                    admin_upsert.status.as_str(),
                    openplotva_storage::CHAT_MEMBER_STATUS_CREATOR
                        | openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
                ),
                "admin status should match the admin sync surface"
            );
        }
        Ok(())
    }

    #[derive(Clone, Debug, Default)]
    struct UpdateStateStoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl UpdateStateStoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("state calls").clone()
        }
    }

    impl UpdateStateStore for UpdateStateStoreStub {
        type Error = io::Error;

        fn upsert_chat_state<'a>(
            &'a self,
            chat: &'a openplotva_core::ChatState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?
                    .push(format!("chat:{}:{}", chat.id, chat.chat_type));
                Ok(())
            })
        }

        fn upsert_user_state<'a>(
            &'a self,
            user: &'a UserState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?
                    .push(format!("user:{}:{}", user.id, user.first_name));
                Ok(())
            })
        }

        fn upsert_telegram_file_metadata<'a>(
            &'a self,
            params: &'a openplotva_storage::TelegramFileMetadataUpsert,
        ) -> TelegramFileMetadataStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|error| io::Error::other(error.to_string()))?
                    .push(format!("file:{}", params.file_unique_id));
                Ok(())
            })
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

    fn private_start_settings_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12352,
            "message": {
                "message_id": 81,
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
                "text": "/start settings",
                "entities": [
                    {
                        "offset": 0,
                        "length": 6,
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

    fn group_admin_chat_settings_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        let command_len = text
            .split_whitespace()
            .next()
            .map(|command| command.encode_utf16().count())
            .unwrap_or_default();
        serde_json::from_value(json!({
            "update_id": 12351,
            "message": {
                "message_id": 80,
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

    fn human_new_members_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12351,
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
                    "first_name": "Ada"
                },
                "new_chat_members": [{
                    "id": 7,
                    "is_bot": false,
                    "first_name": "Grace"
                }]
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

    fn settings_test_control_job(kind: ControlKind, title: &str) -> StatelessJobItem {
        let mut params = group_settings_control_params();
        params.data.kind = kind;
        new_control_job_at(params, OffsetDateTime::UNIX_EPOCH)
            .with_name(title)
            .with_priority(HIGH_PRIORITY)
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
    struct UpdateHandlerStub {
        calls: Arc<Mutex<Vec<i64>>>,
    }

    impl UpdateHandlerStub {
        fn calls(&self) -> Vec<i64> {
            self.calls.lock().expect("update handler calls").clone()
        }
    }

    impl UpdateHandler for UpdateHandlerStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(update.id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SettingsDialogSchedulerStub;

    impl DialogMessageScheduler for SettingsDialogSchedulerStub {
        type Error = StubError;

        fn schedule_dialog_message<'a>(
            &'a self,
            _schedule: DialogMessageSchedule<'a>,
        ) -> DialogMessageFuture<'a, DialogMessageScheduleReport, Self::Error> {
            Box::pin(async move {
                Ok(DialogMessageScheduleReport {
                    queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
                    delay: Duration::ZERO,
                    replaced: false,
                    rate_limited: false,
                })
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SettingsDialogStoreStub;

    impl RandomDialogSettingsStore for SettingsDialogStoreStub {
        type Error = StubError;

        fn random_chat_settings<'a>(
            &'a self,
            chat_id: i64,
        ) -> DialogMessageFuture<'a, Option<ChatSettings>, Self::Error> {
            Box::pin(async move {
                Ok(Some(ChatSettings {
                    reactivity_percentage: 0,
                    ..ChatSettings::defaults(chat_id)
                }))
            })
        }

        fn random_user_settings<'a>(
            &'a self,
            _user_id: i64,
        ) -> DialogMessageFuture<'a, Option<UserSettings>, Self::Error> {
            Box::pin(async { Ok(None) })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SettingsDialogEffectsStub;

    impl RandomDialogEffects for SettingsDialogEffectsStub {
        type Error = StubError;

        fn send_random_obscenified_text<'a>(
            &'a self,
            _plan: RandomObscenifiedTextPlan,
        ) -> DialogMessageFuture<'a, (), Self::Error> {
            Box::pin(async { Ok(()) })
        }
    }

    impl DirectSongNoticeEffects for SettingsDialogEffectsStub {
        fn send_direct_song_notice<'a>(
            &'a self,
            _plan: DirectSongNoticePlan,
        ) -> DirectSongNoticeFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct SettingsDialogRngStub;

    impl RandomDialogRng for SettingsDialogRngStub {
        fn random_response_roll(&self) -> i32 {
            0
        }

        fn obscenifier_roll(&self) -> i32 {
            0
        }

        fn obscenify_variant_roll(&self) -> i32 {
            0
        }
    }

    fn settings_dialog_config() -> DialogMessageUpdateConfig {
        let mut bot_user = User::new(999, "Plotva".to_owned(), true);
        bot_user.username = Some("plotvabot".to_owned().into());
        DialogMessageUpdateConfig {
            bot_user,
            queue_name: DIALOG_AIFARM_QUEUE_NAME.to_owned(),
            use_aifarm_budgets: true,
            aifarm_max_tokens: 4096,
            aifarm_random_max_tokens: 768,
            aifarm_default_max_tokens: 1024,
            aifarm_long_max_tokens: 2048,
            processing_timeout_seconds: 720,
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

    impl super::NewMembersGreetingRunner for NewMembersFollowupEffectsStub {
        fn run_join_greeting<'a>(
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
    struct JoinGreetingCacheStub {
        state: Arc<Mutex<JoinGreetingCacheState>>,
    }

    #[derive(Default)]
    struct JoinGreetingCacheState {
        recent: Vec<String>,
        previous: Option<i32>,
        recorded: Vec<(i64, Vec<i64>, i64)>,
        debounce_started: Vec<i64>,
        saved_previous: Vec<(i64, i32)>,
    }

    impl JoinGreetingCacheStub {
        fn with_recent(values: Vec<&str>) -> Self {
            Self {
                state: Arc::new(Mutex::new(JoinGreetingCacheState {
                    recent: values.into_iter().map(str::to_owned).collect(),
                    ..JoinGreetingCacheState::default()
                })),
            }
        }

        fn set_previous(&self, previous: Option<i32>) {
            self.state
                .lock()
                .expect("join greeting cache state")
                .previous = previous;
        }

        fn recorded(&self) -> Vec<(i64, Vec<i64>, i64)> {
            self.state
                .lock()
                .expect("join greeting cache state")
                .recorded
                .clone()
        }

        fn debounce_started(&self) -> Vec<i64> {
            self.state
                .lock()
                .expect("join greeting cache state")
                .debounce_started
                .clone()
        }

        fn saved_previous(&self) -> Vec<(i64, i32)> {
            self.state
                .lock()
                .expect("join greeting cache state")
                .saved_previous
                .clone()
        }
    }

    impl super::NewMembersGreetingCache for JoinGreetingCacheStub {
        type Error = StubError;

        fn record_join_member_ids<'a>(
            &'a self,
            chat_id: i64,
            user_ids: &'a [i64],
            score: i64,
            _ttl: Duration,
        ) -> super::NewMembersRuntimeFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("join greeting cache state")
                    .recorded
                    .push((chat_id, user_ids.to_vec(), score));
                Ok(())
            })
        }

        fn start_debounce<'a>(
            &'a self,
            chat_id: i64,
            _ttl: Duration,
        ) -> super::NewMembersRuntimeFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("join greeting cache state")
                    .debounce_started
                    .push(chat_id);
                Ok(true)
            })
        }

        fn recent_join_member_ids<'a>(
            &'a self,
            _chat_id: i64,
            _min_score: i64,
        ) -> super::NewMembersRuntimeFuture<'a, Vec<String>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .state
                    .lock()
                    .expect("join greeting cache state")
                    .recent
                    .clone())
            })
        }

        fn previous_greeting_message_id<'a>(
            &'a self,
            _chat_id: i64,
        ) -> super::NewMembersRuntimeFuture<'a, Option<i32>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .state
                    .lock()
                    .expect("join greeting cache state")
                    .previous)
            })
        }

        fn set_previous_greeting_message_id<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            _ttl: Duration,
        ) -> super::NewMembersRuntimeFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("join greeting cache state")
                    .saved_previous
                    .push((chat_id, message_id));
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct JoinGreetingSettingsStub {
        settings: Option<ChatSettings>,
    }

    impl JoinGreetingSettingsStub {
        fn enabled(html: &str) -> Self {
            Self {
                settings: Some(ChatSettings {
                    chat_id: -10042,
                    enable_greet_joiners: true,
                    greeting_html: Some(html.to_owned()),
                    ..ChatSettings::defaults(-10042)
                }),
            }
        }

        fn disabled() -> Self {
            Self {
                settings: Some(ChatSettings::defaults(-10042)),
            }
        }
    }

    impl super::NewMembersGreetingSettingsStore for JoinGreetingSettingsStub {
        type Error = StubError;

        fn greeting_chat_settings<'a>(
            &'a self,
            _chat_id: i64,
        ) -> super::NewMembersRuntimeFuture<'a, Option<ChatSettings>, Self::Error> {
            Box::pin(async move { Ok(self.settings.clone()) })
        }
    }

    #[derive(Clone, Default)]
    struct JoinGreetingMemberDataStub {
        state: Arc<Mutex<JoinGreetingMemberDataState>>,
    }

    #[derive(Default)]
    struct JoinGreetingMemberDataState {
        users: Vec<UserState>,
        members: Vec<ChatMemberRecord>,
    }

    impl JoinGreetingMemberDataStub {
        fn set_users(&self, users: Vec<UserState>) {
            self.state
                .lock()
                .expect("join greeting member data state")
                .users = users;
        }

        fn set_members(&self, members: Vec<ChatMemberRecord>) {
            self.state
                .lock()
                .expect("join greeting member data state")
                .members = members;
        }
    }

    impl super::NewMembersGreetingMemberDataStore for JoinGreetingMemberDataStub {
        type Error = StubError;

        fn list_user_states_by_ids<'a>(
            &'a self,
            user_ids: &'a [i64],
        ) -> super::NewMembersRuntimeFuture<'a, Vec<UserState>, Self::Error> {
            Box::pin(async move {
                let state = self.state.lock().expect("join greeting member data state");
                Ok(state
                    .users
                    .iter()
                    .filter(|user| user_ids.contains(&user.id))
                    .cloned()
                    .collect())
            })
        }

        fn get_user_state<'a>(
            &'a self,
            user_id: i64,
        ) -> super::NewMembersRuntimeFuture<'a, Option<UserState>, Self::Error> {
            Box::pin(async move {
                let state = self.state.lock().expect("join greeting member data state");
                Ok(state.users.iter().find(|user| user.id == user_id).cloned())
            })
        }

        fn list_chat_members_by_user_ids<'a>(
            &'a self,
            chat_id: i64,
            user_ids: &'a [i64],
        ) -> super::NewMembersRuntimeFuture<'a, Vec<ChatMemberRecord>, Self::Error> {
            Box::pin(async move {
                let state = self.state.lock().expect("join greeting member data state");
                Ok(state
                    .members
                    .iter()
                    .filter(|member| {
                        member.chat_id == chat_id && user_ids.contains(&member.user_id)
                    })
                    .cloned()
                    .collect())
            })
        }

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::NewMembersRuntimeFuture<'a, Option<ChatMemberRecord>, Self::Error> {
            Box::pin(async move {
                let state = self.state.lock().expect("join greeting member data state");
                Ok(state
                    .members
                    .iter()
                    .find(|member| member.chat_id == chat_id && member.user_id == user_id)
                    .cloned())
            })
        }
    }

    #[derive(Clone, Default)]
    struct JoinGreetingSenderStub {
        state: Arc<Mutex<JoinGreetingSenderState>>,
    }

    #[derive(Default)]
    struct JoinGreetingSenderState {
        next_message_id: Option<i32>,
        deleted: Vec<(i64, i32)>,
        sent: Vec<super::NewMembersGreetingMessage>,
    }

    impl JoinGreetingSenderStub {
        fn with_next_message_id(next_message_id: Option<i32>) -> Self {
            Self {
                state: Arc::new(Mutex::new(JoinGreetingSenderState {
                    next_message_id,
                    ..JoinGreetingSenderState::default()
                })),
            }
        }

        fn deleted(&self) -> Vec<(i64, i32)> {
            self.state
                .lock()
                .expect("join greeting sender state")
                .deleted
                .clone()
        }

        fn sent(&self) -> Vec<super::NewMembersGreetingMessage> {
            self.state
                .lock()
                .expect("join greeting sender state")
                .sent
                .clone()
        }
    }

    impl super::NewMembersGreetingSender for JoinGreetingSenderStub {
        fn delete_previous_greeting_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
        ) -> super::NewMembersFollowupFuture<'a, ()> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("join greeting sender state")
                    .deleted
                    .push((chat_id, message_id));
            })
        }

        fn send_ephemeral_greeting<'a>(
            &'a self,
            message: super::NewMembersGreetingMessage,
        ) -> super::NewMembersFollowupFuture<'a, Option<i32>> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("join greeting sender state");
                state.sent.push(message);
                state.next_message_id
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
