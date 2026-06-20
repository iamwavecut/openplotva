//! App-level check-in callback control-job behavior.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, Chat as TelegramChat, MaybeInaccessibleMessage,
    Message as TelegramMessage, MessageData as TelegramMessageData, Text as TelegramText,
    TextEntity as TelegramTextEntity, TextEntityPosition as TelegramTextEntityPosition,
    Update as TelegramUpdate, UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY, JobType,
    StatelessJobItem, control_job_params_from_stateless_job, new_control_job_at,
};
use openplotva_telegram::{
    ChatRef, DispatcherConfig, DispatcherQueue, InlineKeyboardMarkup, OutboundBuildError,
    ReplyMarkup, ReplyMessageRef, RichMessageRequest, TELEGRAM_PARSE_MODE_HTML,
    TelegramOutboundMethod, TextMessageRequest, build_checkin_theme_selection_keyboard,
    format_rich_html,
};
use thiserror::Error;
use time::OffsetDateTime;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::{
    permissions::{ChatPermissionPolicy, ChatPermissionStore},
    virtual_messages::{
        QueueRichRequest, QueueTextRequest, VirtualIdFactory, VirtualMessageStore,
        monotonic_virtual_id_factory, queue_rich_message, queue_text_message_parts,
        send_work_item_and_resolve, send_work_item_and_resolve_with_ephemeral,
    },
};

const CHECKIN_CONTROL_JOB_TITLE: &str = "checkin";
const CHECKIN_QUEUE_ERROR_TEXT: &str = "❌ Не удалось поставить задачу в очередь.";
const CHECKIN_QUEUE_ERROR_DELETE_AFTER: Duration = Duration::from_secs(60);
const CHECKIN_COMMAND: &str = "checkin";
const CHECKIN_THEME_SELECTOR_TEXT: &str = "Выбери тему игры на сегодня 👀";
const CHECKIN_THEME_SELECTOR_DELETE_AFTER: Duration = Duration::from_secs(2 * 60);
const MIN_ACTIVE_PARTICIPANTS: usize = 2;
const MAX_PARTICIPANTS: usize = 100;
const BASE_ANIMATION_DELAY: Duration = Duration::from_millis(1500);
const MAX_ANIMATION_JITTER_MS: usize = 3000;
const DEFAULT_THEME_KEY: &str = "king";
const CHECKIN_EPHEMERAL_TIMEOUT: Duration = Duration::from_secs(30);
const NEW_CHAT_WARMUP_WINDOW: time::Duration = time::Duration::hours(24);

/// Boxed future returned by check-in control-job queue calls.
pub type CheckinControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by check-in control-job worker queue calls.
pub type CheckinControlJobWorkerFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by check-in control-job effects.
pub type CheckinControlJobFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by check-in game storage calls.
pub type CheckinGameStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by check-in game Telegram send calls.
pub type CheckinGameSendFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by check-in command effects.
pub type CheckinCommandEffectFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by check-in command permission checks.
pub type CheckinCommandPermissionFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<bool, E>> + Send + 'a>>;

/// Boxed future returned by check-in callback side effects.
pub type CheckinThemeEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by direct check-in callback method executors.
pub type CheckinCallbackMethodFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait CheckinControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn assign_checkin_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> CheckinControlJobQueueFuture<'a, Self::Error>;
}

pub trait CheckinCommandPermission {
    /// Error returned by the concrete permission policy.
    type Error: fmt::Display + Send + Sync + 'static;

    fn can_send_checkin_text<'a>(
        &'a self,
        chat_id: i64,
        chat_type: &'static str,
        now: OffsetDateTime,
    ) -> CheckinCommandPermissionFuture<'a, Self::Error>;
}

impl<S> CheckinCommandPermission for ChatPermissionPolicy<S>
where
    S: ChatPermissionStore + Send + Sync,
{
    type Error = std::convert::Infallible;

    fn can_send_checkin_text<'a>(
        &'a self,
        chat_id: i64,
        chat_type: &'static str,
        now: OffsetDateTime,
    ) -> CheckinCommandPermissionFuture<'a, Self::Error> {
        Box::pin(async move {
            Ok(self
                .can_perform_action_at(
                    chat_id,
                    Some(chat_type),
                    openplotva_server::ACTION_SEND_TEXT,
                    now,
                )
                .await
                .allowed)
        })
    }
}

impl<T> CheckinControlJobQueue for T
where
    T: crate::payments::PaymentControlJobQueue + Sync,
{
    type Error = T::Error;

    fn assign_checkin_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> CheckinControlJobQueueFuture<'a, Self::Error> {
        self.assign_payment_control_job(queue_name, job)
    }
}

/// Queue/status boundary for check-in taskman control-job workers.
pub trait CheckinControlJobWorkerQueue {
    /// Queue error type.
    type Error: fmt::Display + Send + Sync + 'static;

    fn dequeue_checkin_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> CheckinControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    >;

    /// Mark one check-in control job completed.
    fn complete_checkin_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> CheckinControlJobWorkerFuture<'a, (), Self::Error>;

    /// Mark one check-in control job failed.
    fn fail_checkin_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> CheckinControlJobWorkerFuture<'a, (), Self::Error>;
}

impl<T> CheckinControlJobWorkerQueue for T
where
    T: crate::payments::SharedControlJobWorkerQueue + Sync,
{
    type Error = T::Error;

    fn dequeue_checkin_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> CheckinControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    > {
        self.dequeue_shared_control_job_matching(
            queue_name,
            "checkin-control",
            is_checkin_control_job,
        )
    }

    fn complete_checkin_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> CheckinControlJobWorkerFuture<'a, (), Self::Error> {
        self.complete_shared_control_job(job_id)
    }

    fn fail_checkin_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> CheckinControlJobWorkerFuture<'a, (), Self::Error> {
        self.fail_shared_control_job(job_id, error)
    }
}

pub trait CheckinControlJobEffects {
    /// Error returned by the concrete game runner.
    type Error: fmt::Display + Send + Sync + 'static;

    fn run_checkin_game<'a>(
        &'a self,
        params: &'a ControlJobParams,
    ) -> CheckinControlJobFuture<'a, Self::Error>;
}

pub trait CheckinGameStore {
    /// Error returned by the concrete storage implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn get_checkin_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_core::ChatSettings>, Self::Error>;

    /// Load today's stored winner.
    fn get_today_chat_winner<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_storage::ChatGameResult>, Self::Error>;

    /// Insert today's winner row.
    fn record_chat_daily_winner<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        theme: &'a str,
    ) -> CheckinGameStoreFuture<'a, openplotva_storage::ChatGameResult, Self::Error>;

    /// Increment yearly/stat totals after a successful winner insert.
    fn increment_chat_game_win<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> CheckinGameStoreFuture<'a, (), Self::Error>;

    fn list_active_participants_from_table<'a>(
        &'a self,
        chat_id: i64,
        limit_count: i32,
    ) -> CheckinGameStoreFuture<'a, Vec<i64>, Self::Error>;

    fn list_active_participants<'a>(
        &'a self,
        chat_id: i64,
        limit_count: i32,
    ) -> CheckinGameStoreFuture<'a, Vec<i64>, Self::Error>;

    /// Batch-load candidate member rows.
    fn list_chat_members_by_user_ids<'a>(
        &'a self,
        chat_id: i64,
        user_ids: &'a [i64],
    ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatMemberRecord>, Self::Error>;

    /// Load one candidate member row.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_storage::ChatMemberRecord>, Self::Error>;

    /// Load all stored chat members for warmup fallback.
    fn list_chat_members<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatMemberRecord>, Self::Error>;

    fn get_chat_discovered<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<OffsetDateTime>, Self::Error>;

    /// Load one user row for winner display.
    fn get_user_state<'a>(
        &'a self,
        user_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_core::UserState>, Self::Error>;

    fn get_yearly_top<'a>(
        &'a self,
        chat_id: i64,
        limit_count: i32,
    ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatGameTopRow>, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckinGameMessage {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID to reply to.
    pub message_id: i32,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckinGameRunRequest {
    /// Reconstructed control-job message context.
    pub message: CheckinGameMessage,
    /// Telegram chat type string.
    pub chat_type: String,
    /// Optional theme override from callback/command data.
    pub theme_override: String,
    /// Current bot user ID used by warmup participant filtering.
    pub bot_id: i64,
    pub can_send_text: bool,
    /// Current time for warmup checks.
    pub now: OffsetDateTime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckinCommandUpdateContext<'a> {
    pub bot_username: &'a str,
    pub can_send_text: bool,
    /// Control-job creation timestamp.
    pub created: OffsetDateTime,
}

pub trait CheckinGameSender {
    /// Error returned by the concrete sender.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Send one immediate HTML reply. Returns Telegram message ID when available.
    fn send_checkin_html<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        html: String,
    ) -> CheckinGameSendFuture<'a, Option<i32>, Self::Error>;

    /// Edit one immediate HTML message.
    fn edit_checkin_html<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        message_id: i32,
        html: String,
    ) -> CheckinGameSendFuture<'a, (), Self::Error>;

    fn send_checkin_ephemeral<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        text: String,
        delete_after: Duration,
    ) -> CheckinGameSendFuture<'a, (), Self::Error>;
}

pub trait CheckinCommandEffects {
    /// Error returned by concrete command side effects.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_checkin_today_winner_with_stats<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        html: String,
    ) -> CheckinCommandEffectFuture<'a, Self::Error>;

    fn send_checkin_theme_selector<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        user_id: i64,
    ) -> CheckinCommandEffectFuture<'a, Self::Error>;

    fn send_checkin_command_queue_error<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
    ) -> CheckinCommandEffectFuture<'a, Self::Error>;
}

#[derive(Clone, Debug)]
pub struct PostgresCheckinGameStore {
    settings: openplotva_storage::PostgresChatSettingsStore,
    members: openplotva_storage::PostgresChatMemberStore,
}

impl PostgresCheckinGameStore {
    /// Build a concrete check-in game store.
    #[must_use]
    pub fn new(
        settings: openplotva_storage::PostgresChatSettingsStore,
        members: openplotva_storage::PostgresChatMemberStore,
    ) -> Self {
        Self { settings, members }
    }
}

impl CheckinGameStore for PostgresCheckinGameStore {
    type Error = openplotva_storage::StorageError;

    fn get_checkin_chat_settings<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_core::ChatSettings>, Self::Error> {
        Box::pin(async move { self.settings.get_chat_settings(chat_id).await })
    }

    fn get_today_chat_winner<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_storage::ChatGameResult>, Self::Error> {
        Box::pin(async move { self.members.get_today_chat_winner(chat_id).await })
    }

    fn record_chat_daily_winner<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        theme: &'a str,
    ) -> CheckinGameStoreFuture<'a, openplotva_storage::ChatGameResult, Self::Error> {
        Box::pin(async move {
            self.members
                .record_chat_daily_winner(chat_id, user_id, theme)
                .await
        })
    }

    fn increment_chat_game_win<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> CheckinGameStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.members.increment_chat_game_win(chat_id, user_id).await })
    }

    fn list_active_participants_from_table<'a>(
        &'a self,
        chat_id: i64,
        limit_count: i32,
    ) -> CheckinGameStoreFuture<'a, Vec<i64>, Self::Error> {
        Box::pin(async move {
            self.members
                .list_active_participants_from_table(chat_id, limit_count)
                .await
        })
    }

    fn list_active_participants<'a>(
        &'a self,
        chat_id: i64,
        limit_count: i32,
    ) -> CheckinGameStoreFuture<'a, Vec<i64>, Self::Error> {
        Box::pin(async move {
            self.members
                .list_active_participants(chat_id, limit_count)
                .await
        })
    }

    fn list_chat_members_by_user_ids<'a>(
        &'a self,
        chat_id: i64,
        user_ids: &'a [i64],
    ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatMemberRecord>, Self::Error> {
        Box::pin(async move {
            self.members
                .list_chat_members_by_user_ids(chat_id, user_ids)
                .await
        })
    }

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_storage::ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.members.get_chat_member(chat_id, user_id).await })
    }

    fn list_chat_members<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.members.list_chat_members(chat_id).await })
    }

    fn get_chat_discovered<'a>(
        &'a self,
        chat_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<OffsetDateTime>, Self::Error> {
        Box::pin(async move { self.members.get_chat_discovered(chat_id).await })
    }

    fn get_user_state<'a>(
        &'a self,
        user_id: i64,
    ) -> CheckinGameStoreFuture<'a, Option<openplotva_core::UserState>, Self::Error> {
        Box::pin(async move { self.members.get_user_state(user_id).await })
    }

    fn get_yearly_top<'a>(
        &'a self,
        chat_id: i64,
        limit_count: i32,
    ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatGameTopRow>, Self::Error> {
        Box::pin(async move { self.members.get_yearly_top(chat_id, limit_count).await })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckinGameRunOutcome {
    /// Private chat or text-send permission blocked the game before side effects.
    SkippedByChatPolicy,
    /// Game disabled in chat settings.
    Disabled,
    /// Existing daily winner was reported.
    ExistingWinner,
    /// Not enough valid participants.
    NotEnoughParticipants {
        /// Valid participant count.
        count: usize,
    },
    /// New winner recorded, stats increment attempted, and winner message sent.
    WinnerRecorded {
        /// Winner user ID.
        user_id: i64,
        /// Theme key used for storage/output.
        theme: String,
    },
    /// Insert raced with another worker; existing winner was reported when visible.
    RecordRaceExistingWinner,
    RecordFailedNoWinner,
}

/// Recoverable check-in game runner errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CheckinGameRunError {
    #[error("check-in storage failed: {message}")]
    Storage {
        /// Display error.
        message: String,
    },
    #[error("check-in sender failed: {message}")]
    Sender {
        /// Display error.
        message: String,
    },
}

#[derive(Clone)]
pub struct CheckinGameRuntimeEffects<Store, Sender> {
    store: Store,
    sender: Sender,
    permissions: Arc<ChatPermissionPolicy<openplotva_storage::PostgresChatSettingsStore>>,
    bot_id: i64,
    pick_index: Arc<dyn Fn(usize) -> usize + Send + Sync>,
}

#[derive(Clone)]
pub struct TelegramCheckinGameSender {
    virtual_store: openplotva_storage::PostgresVirtualMessageStore,
    ephemeral_store: openplotva_storage::RedisEphemeralMessageStore,
    telegram: openplotva_telegram::TelegramClient,
    rich: openplotva_telegram::RichApiClient,
    next_virtual_id: VirtualIdFactory,
}

impl TelegramCheckinGameSender {
    /// Build a concrete check-in sender.
    #[must_use]
    pub fn new(
        virtual_store: openplotva_storage::PostgresVirtualMessageStore,
        ephemeral_store: openplotva_storage::RedisEphemeralMessageStore,
        telegram: openplotva_telegram::TelegramClient,
        rich: openplotva_telegram::RichApiClient,
    ) -> Self {
        Self {
            virtual_store,
            ephemeral_store,
            telegram,
            rich,
            next_virtual_id: monotonic_virtual_id_factory("checkin-game-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl CheckinGameSender for TelegramCheckinGameSender {
    type Error = CheckinGameSenderError;

    fn send_checkin_html<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        html: String,
    ) -> CheckinGameSendFuture<'a, Option<i32>, Self::Error> {
        Box::pin(async move {
            let queue = DispatcherQueue::new(DispatcherConfig::default());
            queue_checkin_rich(
                &self.virtual_store,
                &queue,
                reply_to,
                html,
                None,
                None,
                || (self.next_virtual_id)(),
            )
            .await?;
            send_queued_checkin_items(
                &self.virtual_store,
                None,
                &queue,
                &self.telegram,
                &self.rich,
            )
            .await
        })
    }

    fn edit_checkin_html<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        message_id: i32,
        html: String,
    ) -> CheckinGameSendFuture<'a, (), Self::Error> {
        Box::pin(async move {
            self.rich
                .edit_message_text_rich(
                    reply_to.chat_id,
                    i64::from(message_id),
                    &format_rich_html(&html),
                    None,
                )
                .await
                .map_err(|error| CheckinGameSenderError::Telegram(error.to_string()))
        })
    }

    fn send_checkin_ephemeral<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        text: String,
        delete_after: Duration,
    ) -> CheckinGameSendFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let queue = DispatcherQueue::new(DispatcherConfig::default());
            queue_checkin_text(
                &self.virtual_store,
                &queue,
                reply_to,
                text,
                Some(delete_after),
                None,
                || (self.next_virtual_id)(),
            )
            .await?;
            send_queued_checkin_items(
                &self.virtual_store,
                Some(&self.ephemeral_store),
                &queue,
                &self.telegram,
                &self.rich,
            )
            .await
            .map(|_| ())
        })
    }
}

/// Concrete check-in sender errors.
#[derive(Debug, Error)]
pub enum CheckinGameSenderError {
    /// Queue/build failed.
    #[error("check-in outbound build failed: {0}")]
    Build(#[from] OutboundBuildError),
    /// Telegram send failed.
    #[error("check-in Telegram send failed: {0}")]
    Telegram(String),
}

#[derive(Clone)]
pub struct CheckinCommandDispatcherEffects<Store> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    rich: Arc<dyn crate::rich::RichSender>,
    next_virtual_id: VirtualIdFactory,
}

impl<Store> CheckinCommandDispatcherEffects<Store> {
    /// Build check-in command effects over an existing dispatcher queue.
    #[must_use]
    pub fn new(
        store: Store,
        queue: Arc<DispatcherQueue>,
        rich: Arc<dyn crate::rich::RichSender>,
    ) -> Self {
        Self {
            store,
            queue,
            rich,
            next_virtual_id: monotonic_virtual_id_factory("checkin-command-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

/// Errors from dispatcher-backed check-in command effects.
#[derive(Debug, thiserror::Error)]
pub enum CheckinCommandEffectError {
    #[error("failed to queue check-in message: {0}")]
    Queue(#[from] OutboundBuildError),
    #[error("failed to send check-in rich message: {0}")]
    RichSend(String),
}

impl<Store> CheckinCommandEffects for CheckinCommandDispatcherEffects<Store>
where
    Store: VirtualMessageStore + Send + Sync,
{
    type Error = CheckinCommandEffectError;

    fn send_checkin_today_winner_with_stats<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        html: String,
    ) -> CheckinCommandEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let options = openplotva_telegram::RichSendOptions {
                message_thread_id: reply_to.thread_id.map(i64::from),
                reply_to_message_id: Some(i64::from(reply_to.message_id)),
                allow_sending_without_reply: true,
                disable_notification: false,
                reply_markup: None,
            };
            self.rich
                .send_rich(reply_to.chat_id, &html, &options)
                .await
                .map_err(|error| CheckinCommandEffectError::RichSend(error.to_string()))?;
            Ok(())
        })
    }

    fn send_checkin_theme_selector<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
        user_id: i64,
    ) -> CheckinCommandEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            queue_checkin_rich(
                &self.store,
                &self.queue,
                reply_to,
                format!("<h3>{CHECKIN_THEME_SELECTOR_TEXT}</h3>"),
                Some(CHECKIN_THEME_SELECTOR_DELETE_AFTER),
                Some(build_checkin_theme_selection_keyboard(user_id)),
                || (self.next_virtual_id)(),
            )
            .await
            .map_err(CheckinCommandEffectError::from)
        })
    }

    fn send_checkin_command_queue_error<'a>(
        &'a self,
        reply_to: CheckinGameMessage,
    ) -> CheckinCommandEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            queue_checkin_text(
                &self.store,
                &self.queue,
                reply_to,
                CHECKIN_QUEUE_ERROR_TEXT.to_owned(),
                Some(CHECKIN_QUEUE_ERROR_DELETE_AFTER),
                None,
                || (self.next_virtual_id)(),
            )
            .await
            .map_err(CheckinCommandEffectError::from)
        })
    }
}

async fn queue_checkin_text<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    reply_to: CheckinGameMessage,
    text: String,
    ephemeral_delete_after: Option<Duration>,
    reply_markup: Option<InlineKeyboardMarkup>,
    next_virtual_id: NextId,
) -> Result<(), OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = checkin_chat_ref(reply_to);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: reply_to.thread_id.unwrap_or_default().into(),
        disable_notification: false,
        allow_sending_without_reply: None,
        text,
        render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
        reply_markup: reply_markup.map(ReplyMarkup::InlineKeyboardMarkup),
    };
    let reply = ReplyMessageRef {
        message_id: i64::from(reply_to.message_id),
        chat,
        is_topic_message: reply_to.thread_id.is_some(),
        message_thread_id: reply_to.thread_id.unwrap_or_default().into(),
    };
    queue_text_message_parts(
        store,
        queue,
        QueueTextRequest {
            message: &request,
            reply_to: Some(&reply),
            immediate_first: true,
            bypass_chat_restrictions: false,
            ephemeral_delete_after,
        },
        next_virtual_id,
    )
    .await?;
    Ok(())
}

async fn queue_checkin_rich<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    reply_to: CheckinGameMessage,
    html: String,
    ephemeral_delete_after: Option<Duration>,
    reply_markup: Option<InlineKeyboardMarkup>,
    next_virtual_id: NextId,
) -> Result<(), OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = checkin_chat_ref(reply_to);
    let request = RichMessageRequest {
        chat: Some(chat),
        message_thread_id: reply_to.thread_id.unwrap_or_default().into(),
        disable_notification: false,
        allow_sending_without_reply: None,
        html,
        reply_markup: reply_markup.map(ReplyMarkup::InlineKeyboardMarkup),
    };
    let reply = ReplyMessageRef {
        message_id: i64::from(reply_to.message_id),
        chat,
        is_topic_message: reply_to.thread_id.is_some(),
        message_thread_id: reply_to.thread_id.unwrap_or_default().into(),
    };
    queue_rich_message(
        store,
        queue,
        QueueRichRequest {
            message: &request,
            reply_to: Some(&reply),
            immediate: true,
            bypass_chat_restrictions: false,
            ephemeral_delete_after,
        },
        next_virtual_id,
    )
    .await?;
    Ok(())
}

async fn send_queued_checkin_items<S>(
    store: &S,
    ephemeral: Option<&openplotva_storage::RedisEphemeralMessageStore>,
    queue: &DispatcherQueue,
    telegram: &openplotva_telegram::TelegramClient,
    rich: &openplotva_telegram::RichApiClient,
) -> Result<Option<i32>, CheckinGameSenderError>
where
    S: VirtualMessageStore + Sync,
{
    let mut first_message_id = None;
    let mut next = queue
        .dequeue_immediate()
        .or_else(|| queue.dequeue_regular());
    while let Some(item) = next {
        let report = if let Some(ephemeral) = ephemeral {
            send_work_item_and_resolve_with_ephemeral(
                store,
                ephemeral,
                item,
                OffsetDateTime::now_utc(),
                |method| async move {
                    openplotva_telegram::execute_telegram_method_with_rich(telegram, rich, method)
                        .await
                },
            )
            .await
        } else {
            send_work_item_and_resolve(store, item, |method| async move {
                openplotva_telegram::execute_telegram_method_with_rich(telegram, rich, method).await
            })
            .await
        };
        if first_message_id.is_none() {
            first_message_id = report.resolved_message_id;
        }
        if let Some(error) = report.send_error {
            return Err(CheckinGameSenderError::Telegram(error));
        }
        next = queue
            .dequeue_immediate()
            .or_else(|| queue.dequeue_regular());
    }
    Ok(first_message_id)
}

fn checkin_chat_ref(message: CheckinGameMessage) -> ChatRef {
    ChatRef {
        id: message.chat_id,
        is_forum: message.thread_id.is_some(),
    }
}

impl<Store, Sender> CheckinGameRuntimeEffects<Store, Sender> {
    pub fn new(
        store: Store,
        sender: Sender,
        permissions: Arc<ChatPermissionPolicy<openplotva_storage::PostgresChatSettingsStore>>,
        bot_id: i64,
    ) -> Self {
        Self {
            store,
            sender,
            permissions,
            bot_id,
            pick_index: Arc::new(default_checkin_index),
        }
    }

    /// Override winner picker for deterministic tests.
    #[must_use]
    pub fn with_picker(mut self, pick_index: Arc<dyn Fn(usize) -> usize + Send + Sync>) -> Self {
        self.pick_index = pick_index;
        self
    }
}

impl<Store, Sender> CheckinControlJobEffects for CheckinGameRuntimeEffects<Store, Sender>
where
    Store: CheckinGameStore + Send + Sync,
    Sender: CheckinGameSender + Send + Sync,
{
    type Error = CheckinGameRunError;

    fn run_checkin_game<'a>(
        &'a self,
        params: &'a ControlJobParams,
    ) -> CheckinControlJobFuture<'a, Self::Error> {
        Box::pin(async move {
            let message = CheckinGameMessage {
                chat_id: params.chat_id,
                message_id: params.message_id,
                thread_id: params.thread_id,
            };
            let chat_type = (!params.data.chat_type.trim().is_empty())
                .then_some(params.data.chat_type.as_str());
            let permission = self
                .permissions
                .can_perform_action_at(
                    params.chat_id,
                    chat_type,
                    openplotva_server::ACTION_SEND_TEXT,
                    OffsetDateTime::now_utc(),
                )
                .await;
            run_checkin_game_inner(
                &self.store,
                &self.sender,
                CheckinGameRunRequest {
                    message,
                    chat_type: params.data.chat_type.clone(),
                    theme_override: params.data.theme.clone(),
                    bot_id: self.bot_id,
                    can_send_text: permission.allowed,
                    now: OffsetDateTime::now_utc(),
                },
                &*self.pick_index,
                true,
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn run_checkin_game_at<Store, Sender, Pick>(
    store: &Store,
    sender: &Sender,
    request: CheckinGameRunRequest,
    pick_index: Pick,
) -> Result<CheckinGameRunOutcome, CheckinGameRunError>
where
    Store: CheckinGameStore + Sync,
    Sender: CheckinGameSender + Sync,
    Pick: Fn(usize) -> usize + Sync,
{
    run_checkin_game_inner(store, sender, request, &pick_index, false).await
}

async fn run_checkin_game_inner<Store, Sender, Pick>(
    store: &Store,
    sender: &Sender,
    request: CheckinGameRunRequest,
    pick_index: &Pick,
    sleep_between_steps: bool,
) -> Result<CheckinGameRunOutcome, CheckinGameRunError>
where
    Store: CheckinGameStore + Sync,
    Sender: CheckinGameSender + Sync,
    Pick: Fn(usize) -> usize + Sync + ?Sized,
{
    if request.chat_type.eq_ignore_ascii_case("private") || !request.can_send_text {
        return Ok(CheckinGameRunOutcome::SkippedByChatPolicy);
    }

    let message = request.message;
    let settings = match store.get_checkin_chat_settings(message.chat_id).await {
        Ok(Some(settings)) => settings,
        Ok(None) | Err(_) => openplotva_core::ChatSettings::defaults(message.chat_id),
    };
    if daily_game_disabled(&settings) {
        sender
            .send_checkin_ephemeral(
                message,
                "🚫 Игра выключена в настройках".to_owned(),
                CHECKIN_EPHEMERAL_TIMEOUT,
            )
            .await
            .map_err(sender_error)?;
        return Ok(CheckinGameRunOutcome::Disabled);
    }

    if let Some(winner) = load_today_winner(store, message.chat_id).await {
        send_today_winner_message(store, sender, message, &winner).await;
        return Ok(CheckinGameRunOutcome::ExistingWinner);
    }

    let theme = resolve_checkin_theme(&settings, &request.theme_override);
    let participants =
        active_participants(store, message.chat_id, request.bot_id, request.now).await;
    if participants.len() < MIN_ACTIVE_PARTICIPANTS {
        sender
            .send_checkin_ephemeral(
                message,
                format!(
                    "ℹ️ Недостаточно участников (нужно ≥{MIN_ACTIVE_PARTICIPANTS} активных за 24 часа)"
                ),
                CHECKIN_EPHEMERAL_TIMEOUT,
            )
            .await
            .map_err(sender_error)?;
        return Ok(CheckinGameRunOutcome::NotEnoughParticipants {
            count: participants.len(),
        });
    }

    let picked = pick_index(participants.len()).min(participants.len() - 1);
    let winner_id = participants[picked];
    match store
        .record_chat_daily_winner(message.chat_id, winner_id, theme.key)
        .await
    {
        Ok(_) => {
            if let Err(error) = store
                .increment_chat_game_win(message.chat_id, winner_id)
                .await
            {
                tracing::warn!(message = %error, chat_id = message.chat_id, winner_id, "failed to increment check-in game stats");
            }
            run_checkin_animation(
                store,
                sender,
                message,
                theme,
                winner_id,
                pick_index,
                sleep_between_steps,
            )
            .await?;
            Ok(CheckinGameRunOutcome::WinnerRecorded {
                user_id: winner_id,
                theme: theme.key.to_owned(),
            })
        }
        Err(error) => {
            tracing::warn!(message = %error, chat_id = message.chat_id, winner_id, "failed to record check-in daily winner");
            if let Some(existing) = load_today_winner(store, message.chat_id).await {
                send_today_winner_message(store, sender, message, &existing).await;
                Ok(CheckinGameRunOutcome::RecordRaceExistingWinner)
            } else {
                Ok(CheckinGameRunOutcome::RecordFailedNoWinner)
            }
        }
    }
}

async fn load_today_winner<Store>(
    store: &Store,
    chat_id: i64,
) -> Option<openplotva_storage::ChatGameResult>
where
    Store: CheckinGameStore + Sync,
{
    match store.get_today_chat_winner(chat_id).await {
        Ok(winner) => winner.filter(|winner| winner.user_id != 0),
        Err(error) => {
            tracing::debug!(message = %error, chat_id, "failed to load today's check-in winner");
            None
        }
    }
}

async fn active_participants<Store>(
    store: &Store,
    chat_id: i64,
    bot_id: i64,
    now: OffsetDateTime,
) -> Vec<i64>
where
    Store: CheckinGameStore + Sync,
{
    let mut candidates = Vec::with_capacity(MAX_PARTICIPANTS * 2);
    if let Ok(ids) = store
        .list_active_participants_from_table(chat_id, MAX_PARTICIPANTS as i32)
        .await
    {
        candidates.extend(ids);
    }
    if let Ok(ids) = store
        .list_active_participants(chat_id, MAX_PARTICIPANTS as i32)
        .await
    {
        candidates.extend(ids);
    }
    let mut valid =
        filter_valid_participants(store, chat_id, unique_nonzero_user_ids(candidates)).await;
    if valid.len() < MIN_ACTIVE_PARTICIPANTS
        && is_chat_in_warmup_window(store, chat_id, now).await
        && let Ok(members) = store.list_chat_members(chat_id).await
    {
        valid = unique_nonzero_user_ids(
            valid
                .into_iter()
                .chain(warmup_participant_ids(&members, bot_id))
                .collect(),
        );
    }
    valid.truncate(MAX_PARTICIPANTS);
    valid
}

async fn filter_valid_participants<Store>(
    store: &Store,
    chat_id: i64,
    candidates: Vec<i64>,
) -> Vec<i64>
where
    Store: CheckinGameStore + Sync,
{
    if candidates.is_empty() {
        return Vec::new();
    }
    if let Ok(members) = store
        .list_chat_members_by_user_ids(chat_id, &candidates)
        .await
    {
        let members_by_id: HashMap<i64, openplotva_storage::ChatMemberRecord> = members
            .into_iter()
            .map(|member| (member.user_id, member))
            .collect();
        return candidates
            .into_iter()
            .filter(|user_id| {
                members_by_id
                    .get(user_id)
                    .is_none_or(|member| active_participant_status(&member.status))
            })
            .collect();
    }

    let mut valid = Vec::with_capacity(candidates.len());
    for user_id in candidates {
        match store.get_chat_member(chat_id, user_id).await {
            Ok(Some(member)) if !active_participant_status(&member.status) => {}
            _ => valid.push(user_id),
        }
    }
    valid
}

async fn is_chat_in_warmup_window<Store>(store: &Store, chat_id: i64, now: OffsetDateTime) -> bool
where
    Store: CheckinGameStore + Sync,
{
    if chat_id == 0 {
        return false;
    }
    matches!(
        store.get_chat_discovered(chat_id).await,
        Ok(Some(discovered)) if now - discovered < NEW_CHAT_WARMUP_WINDOW
    )
}

fn warmup_participant_ids(
    members: &[openplotva_storage::ChatMemberRecord],
    bot_id: i64,
) -> Vec<i64> {
    let mut seen = HashSet::with_capacity(members.len());
    let mut out = Vec::with_capacity(members.len().min(MAX_PARTICIPANTS));
    for member in members {
        if member.user_id == 0 || member.user_id == bot_id {
            continue;
        }
        if !active_participant_status(&member.status) || !seen.insert(member.user_id) {
            continue;
        }
        out.push(member.user_id);
        if out.len() >= MAX_PARTICIPANTS {
            break;
        }
    }
    out
}

fn unique_nonzero_user_ids(user_ids: Vec<i64>) -> Vec<i64> {
    let mut seen = HashSet::with_capacity(user_ids.len());
    user_ids
        .into_iter()
        .filter(|user_id| *user_id != 0 && seen.insert(*user_id))
        .collect()
}

fn active_participant_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "administrator" | "member" | "creator"
    )
}

fn daily_game_disabled(settings: &openplotva_core::ChatSettings) -> bool {
    settings.enable_daily_game == Some(false)
}

async fn run_checkin_animation<Store, Sender, Pick>(
    store: &Store,
    sender: &Sender,
    message: CheckinGameMessage,
    theme: CheckinGameTheme,
    winner_id: i64,
    pick_index: &Pick,
    sleep_between_steps: bool,
) -> Result<(), CheckinGameRunError>
where
    Store: CheckinGameStore + Sync,
    Sender: CheckinGameSender + Sync,
    Pick: Fn(usize) -> usize + Sync + ?Sized,
{
    let progress = checkin_progress_lines(theme, pick_index);
    let mut text = progress[0].clone();
    let Some(message_id) = sender
        .send_checkin_html(message, text.clone())
        .await
        .map_err(|error| {
            tracing::warn!(message = %error, chat_id = message.chat_id, "failed to send check-in animation start");
            error
        })
        .ok()
        .flatten()
    else {
        return Ok(());
    };
    for step in progress.iter().skip(1) {
        if sleep_between_steps {
            tokio::time::sleep(checkin_animation_delay(pick_index)).await;
        }
        text.push_str("\n\n");
        text.push_str(step);
        if let Err(error) = sender
            .edit_checkin_html(message, message_id, text.clone())
            .await
        {
            tracing::warn!(
                message = %error,
                chat_id = message.chat_id,
                message_id,
                "failed to edit check-in progress message"
            );
        }
    }
    let linked_name = winner_link(store, winner_id).await;
    text.push_str("\n\n");
    text.push_str(&theme.winner_text(&linked_name));
    if let Err(error) = sender.edit_checkin_html(message, message_id, text).await {
        tracing::warn!(
            message = %error,
            chat_id = message.chat_id,
            message_id,
            "failed to edit check-in winner message"
        );
    }
    Ok(())
}

fn checkin_progress_lines<Pick>(theme: CheckinGameTheme, pick_index: &Pick) -> Vec<String>
where
    Pick: Fn(usize) -> usize + ?Sized,
{
    let mut out = Vec::with_capacity(CHECKIN_PROGRESS_STEPS.len() + 1);
    out.push(format!("<h2>{}</h2>", theme.today));
    out.extend(CHECKIN_PROGRESS_STEPS.iter().map(|step| {
        let index = pick_index(step.len()).min(step.len() - 1);
        format!("<h2>{}</h2>", step[index])
    }));
    out
}

fn checkin_animation_delay<Pick>(pick_index: &Pick) -> Duration
where
    Pick: Fn(usize) -> usize + ?Sized,
{
    BASE_ANIMATION_DELAY
        + Duration::from_millis(
            pick_index(MAX_ANIMATION_JITTER_MS).min(MAX_ANIMATION_JITTER_MS - 1) as u64,
        )
}

async fn send_today_winner_message<Store, Sender>(
    store: &Store,
    sender: &Sender,
    message: CheckinGameMessage,
    winner: &openplotva_storage::ChatGameResult,
) where
    Store: CheckinGameStore + Sync,
    Sender: CheckinGameSender + Sync,
{
    let theme = theme_by_key(&winner.theme);
    let linked_name = winner_link(store, winner.user_id).await;
    let text = format!(
        "🏁 Сегодня мы играли в <b>{}</b>\n{}",
        theme.name,
        theme.winner_text(&linked_name)
    );
    if let Err(error) = sender.send_checkin_html(message, text).await {
        tracing::warn!(message = %error, chat_id = message.chat_id, "failed to send today's check-in winner");
    }
}

async fn today_winner_with_stats_html<Store>(
    store: &Store,
    chat_id: i64,
    winner: &openplotva_storage::ChatGameResult,
) -> String
where
    Store: CheckinGameStore + Sync,
{
    let theme = theme_by_key(&winner.theme);
    let name = winner_display_name(store, winner.user_id).await;
    let header = format!(
        "<p>🏁 Сегодня мы играли в <b>{}</b><br>Победитель: {}</p>",
        escape_game_html_text(theme.name),
        escape_game_html_text(&name)
    );
    let yearly = store.get_yearly_top(chat_id, 30).await.unwrap_or_default();
    format!(
        "{header}{}",
        render_yearly_top(&yearly, theme, OffsetDateTime::now_utc().date())
    )
}

fn render_yearly_top(
    rows: &[openplotva_storage::ChatGameTopRow],
    theme: CheckinGameTheme,
    day: time::Date,
) -> String {
    if rows.is_empty() {
        return "<p>📊 Нет данных для отображения</p>".to_owned();
    }
    let entries: Vec<crate::rich::LeaderboardRow> = rows
        .iter()
        .enumerate()
        .map(|(index, row)| crate::rich::LeaderboardRow {
            rank_title: daily_rank_title(theme, index, day).to_owned(),
            name: top_user_display_name(&row.user),
            wins: i64::from(row.wins_count),
        })
        .collect();
    crate::rich::compose_leaderboard(theme.name, &entries)
}

fn daily_rank_title(theme: CheckinGameTheme, index: usize, day: time::Date) -> &'static str {
    if theme.rank_titles.is_empty() {
        return "";
    }
    let seed = (usize::from(day.ordinal()) + 1) % theme.rank_titles.len();
    let pos = (seed + index) % theme.rank_titles.len();
    theme.rank_titles[pos]
}

fn top_user_display_name(user: &openplotva_core::UserState) -> String {
    if user
        .last_name
        .as_deref()
        .is_some_and(|last| !last.is_empty())
    {
        return format!(
            "{} {}",
            user.first_name,
            user.last_name.as_deref().unwrap_or_default()
        )
        .trim()
        .to_owned();
    }
    if !user.first_name.is_empty() {
        return user.first_name.clone();
    }
    if let Some(username) = user
        .username
        .as_deref()
        .filter(|username| !username.is_empty())
    {
        return username.to_owned();
    }
    "неопознанная капибара".to_owned()
}

async fn winner_link<Store>(store: &Store, user_id: i64) -> String
where
    Store: CheckinGameStore + Sync,
{
    let name = match store.get_user_state(user_id).await {
        Ok(Some(user)) => user_link_display_name(&user).unwrap_or_else(|| user_id.to_string()),
        Ok(None) => user_id.to_string(),
        Err(error) => {
            tracing::debug!(message = %error, user_id, "failed to load check-in winner user");
            user_id.to_string()
        }
    };
    format!(
        "<a href='tg://user?id={user_id}'>{}</a>",
        escape_game_html_text(&name)
    )
}

async fn winner_display_name<Store>(store: &Store, user_id: i64) -> String
where
    Store: CheckinGameStore + Sync,
{
    match store.get_user_state(user_id).await {
        Ok(Some(user)) => user_link_display_name(&user).unwrap_or_else(|| user_id.to_string()),
        Ok(None) => user_id.to_string(),
        Err(error) => {
            tracing::debug!(message = %error, user_id, "failed to load check-in winner user");
            user_id.to_string()
        }
    }
}

fn user_link_display_name(user: &openplotva_core::UserState) -> Option<String> {
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
    if let Some(username) = user
        .username
        .as_deref()
        .filter(|username| !username.is_empty())
    {
        return Some(format!("@{username}"));
    }
    Some(user.first_name.clone())
}

fn escape_game_html_text(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '&' => escaped.push_str("&amp;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn default_checkin_index(len: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    let nanos = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let nanos = u128::try_from(nanos).unwrap_or_default();
    (nanos % len as u128) as usize
}

const CHECKIN_PROGRESS_STEPS: &[&[&str]] = &[
    &[
        "Собираем претендентов... 🔍",
        "Сканируем чат... 📡",
        "Подсматриваем в историю... 📜",
        "<b>Проверяем alibi</b>... 🕵️",
        "Загружаем список подозреваемых... 📥",
        "Ищем активных за 24 часа... 🛰️",
        "Синхронизируемся со спутником мемов... 🛰️",
        "Берём анализы на удачу... 🧪",
        "Перемешиваем ДНК рандома... 🧬",
        "Распаковываем пак удачи... 📦",
        "Достаём священный рандомайзер... 🧰",
        "Полируем монетку судьбы... 🪙",
        "Настраиваем компас совпадений... 🧭",
        "Тушим токсичность... 🧯",
        "Проверяем паспорта участия... 🪪",
        "Ищем <i>тихих наблюдателей</i>... 👀",
        "Подметаем следы прошлых побед... 🧹",
        "Сверяем с летописями чата... 📚",
        "Охлаждаем разгорячённые головы... 🧊",
        "Принимаем печеньку согласия... 🍪",
        "Строим график меметичности... 📊",
        "Подкручиваем нейрончики... 🧠",
        "Калибруем шум судьбы... 🎛️",
        "Притягиваем удачу... 🧲",
        "Гасим пожары в чате... 🧯",
        "Достаём попкорн... 🍿",
        "Прикалываем бейджи участника... 🧷",
        "Протираем линзы объективности... 🧽",
        "Пингуем богов рандома... 📡",
        "Ищем следы активности... 🔎",
        "<b>Взвешиваем</b> карму на золотых весах... ⚖️😂",
        "Шепчем заклинания рандома под луной... 🌕🧙‍♂️",
        "Ловим бабочек удачи сачком... 🦋🕸️",
        "Раздаём <i>волшебные билетики</i> всем... 🎟️✨",
        "Проверяем, не спрятался ли кто в кустах... 🌳👀",
        "Готовим коктейль из мемов и хаоса... 🍹🤪",
        "Собираем пазл 'Кто следующий герой?'... 🧩🏆",
        "Танцуем с алгоритмами рандома... 💃🤖",
        "Вызываем духа фортуны на чай... 👻☕",
        "Полируем хрустальный шар предсказаний... 🔮🧼",
    ],
    &[
        "Крутим барабан... 🎰",
        "Подбрасываем монетку... 🪙",
        "Крутим рулетку... 🎡",
        "Дёргаем рычаг удачи... 🎰",
        "Взбалтываем, но не смешиваем... 🧪",
        "Запускаем фейерверк совпадений... 🎆",
        "Пшикаем удачу на всех... 🧯",
        "Считаем кармические очки... 🧮",
        "Перематываем кассету рандома... 📀",
        "Гоним белый шум до упора... 🎚️",
        "Рероллим вселенную... 🔁",
        "Собираем пазл вероятностей... 🧩",
        "Тянем нить судьбы... 🧵",
        "Переиспускаем сигнал... 📡",
        "Кастуем <i>Randomize()</i>... 🪄",
        "Сдуваем пыль с кнопки Фортуны... 🧹",
        "Жмём на зелёную кнопку... ✅",
        "Монетка улетела под стол... новая монетка! 🪙",
        "Фризим баг удачи... 🧊",
        "Сок судьбы разбавили мемами... 🧃",
        "Выкатываем D100... 🎲",
        "Линия вероятности крутится... 🧭",
        "Сжимаем аккордеон случайностей... 🪗",
        "Ставим рандому +5 к стабильности... 🧯",
        "Проверяем сглаз... 🧿",
        "Перерасчитываем орбиты шансов... 🛰️",
        "Вставляем кирпичик фортуны... 🧱",
        "Затягиваем гайки реролла... 🔧",
        "Мутация случайности прошла успешно... 🧬",
        "Бросаем <b>космический кубик</b> в бездну... 🎲🌌",
        "Подмигиваем звёздам за подсказку... ⭐😉",
        "Крутим диско-шар вероятностей... 🪩✨",
        "Дуем на кубики, чтобы выпал джекпот... 🎲💨",
        "Мешаем карты судьбы шаловливой рукой... 🃏🤭",
        "Запускаем ракету рандома в стратосферу... 🚀😂",
        "Трясём волшебный снежный шар... ❄️🔮",
        "Выбираем победителя методом 'эники-беники'... ✋🤚",
        "Подбрасываем пиццу удачи... 🍕🆙",
        "Катаем шар по лабиринту шансов... 🌀⚽",
    ],
    &[
        "Подкручиваем карму... 🪄",
        "Сверяем вибрации... 🔊",
        "Заряжаем воду... 💧",
        "Гримуар открыт на странице удачи... 🔮",
        "Ставим свечку богам рандома... 🕯️",
        "Входим в состояние <b>дзен-рандома</b>... 🧘",
        "Посылаем голубя вероятности... 🕊️",
        "Закручиваем вихрь шансов... 🌪️",
        "Рисуем радугу фортуны... 🌈",
        "Призываем единорога удачи... 🦄",
        "Вызываем джинна случайностей... 🧞",
        "Шепчем заклинание <code>rng.seed()</code>... 🧙",
        "Мажем крылья Фортуны глиттером... 🪽",
        "Пересчитываем бусы вероятностей... 📿",
        "Балансируем полюса шансов... 🧭",
        "Снимаем порчу неудач... 🪬",
        "Варим эликсир вероятности... ⚗️",
        "Охлаждаем раскалённый случай... 🧯",
        "Задерживаем дыхание вселенной... 🧘‍♀️",
        "Делаем бэкап судьбы... 🌌",
        "Настраиваем LLM на шутки... 🤖",
        "Пускаем пузырики удачи... 🫧",
        "Намазываем хлебушек фартом... 🧈",
        "Кладём ананас ради хаоса... 🍕",
        "Смещаем перекрестие шансов... 🎯",
        "Включаем диско-рандом... 🪩",
        "Делаем <i>abracadabra</i> над списком... ✨",
        "Притягиваем самый смешной исход... 🧲",
        "Пересылаем судьбу по VPN... 🛰️",
        "Вызываем <b>духа мемов</b> для совета... 👻🤣",
        "Раздаём виртуальные печеньки участникам... 🍪😋",
        "Шушукаемся с алгоритмами о секретах... 🤫🖥️",
        "Поливаем дерево удачи из лейки... 🌳🚿",
        "Играем в прятки с вероятностями... 🙈🎲",
        "Запускаем фейерверк идей... 🎇💡",
        "Мешаем зелье из смеха и случайностей... 🧪😄",
        "Подмигиваем зеркалу фортуны... 🪞😉",
        "Танцуем ритуальный танец рандома... 💃🕺",
        "Собираем букет из четырёхлистных клевера... 🍀💐",
    ],
    &[
        "Ещё чуть-чуть... ⏳",
        "Минутку... ⏱️",
        "Держим интригу... 🤫",
        "<b>Сейчас как бахнет!</b>... 💥",
        "И... камера... мотор... 🎬",
        "На донышке шанса... 🤏",
        "Погладь удачу и она сработает... 🧨",
        "Щепотка фортуны по вкусу... 🧂",
        "Гасим лампочку ожидания... 🧯",
        "Медленно, как судьба в понедельник... 🦥",
        "Удача примеряет маски... 🎭",
        "Нажимаем <b>Start</b> на геймпаде... 🎮",
        "Переждём ретроградный Меркурий... 🧘",
        "Достаём кролика из шляпы... он админ... 🎩",
        "Пьём сок рандома, без мякоти... 🧃",
        "Дотягиваем нить интриги... 🧵",
        "Лёд тронулся, господа присяжные... 🧊",
        "Вулкан вероятностей вот-вот... 🌋",
        "Бликуем хайлайтом удачи... 🌟",
        "И ещё разок монетку, на удачу... 🪙",
        "Заряжаем финальный импульс... ⚡",
        "Сейчас случится <i>магия</i>... 🪄",
        "Звонок судьбы уже в пути... 🔔",
        "Таймер на исходе... 🧨",
        "Почти попали в яблочко... 🎯",
        "Стрелка удачи навелась... 🧭",
        "Последний элемент пазла... 🧩",
        "Обратный отсчёт... 3... 2... 1... 🚀",
        "<b>Бум!</b> Готовы к сюрпризу? 💣😄",
        "Интрига накаляется, как попкорн в микроволновке... 🍿🔥",
        "Удача на низком старте... 🏃‍♂️💨",
        "Финальный штрих мастерства рандома... 🎨✨",
        "Держитесь за стулья, будет весело! 🪑😂",
        "Секундочку, добавляем перчинку юмора... 🌶️😆",
        "Фортуна делает селфи с победителем... 📸🏆",
        "Почти... Ещё один смешок судьбы... 🤭",
        "Раз, два, три... Удача, выходи! 🚪✨",
        "Завершаем с фейерверком эмоций... 🎆😍",
    ],
];

fn sender_error<E: fmt::Display>(error: E) -> CheckinGameRunError {
    CheckinGameRunError::Sender {
        message: error.to_string(),
    }
}

const KING_RANK_TITLES: &[&str] = &[
    "👑 Король горы",
    "🥈 Принц склонов",
    "🥉 Барон уступов",
    "🛡️ Рыцарь скалы",
    "⚔️ Пехотинец обрыва",
    "⛰️ Лезущий в гору",
    "🏹 Стрелок перевала",
    "🪖 Дежурный по гряде",
    "🦅 Орёл высот",
    "🪨 Каменотёс дня",
    "🏔️ Властелин вершин",
    "🧗 Повелитель крюков",
    "🧭 Командор перевалов",
    "🏕️ Смотритель лагеря",
    "🥾 Шлёпатель троп",
    "🪖 Маршал морены",
    "🧱 Архитектор карнизов",
    "🛷 Сани по курумнику",
    "🛰️ Навигатор серпантинов",
    "🪜 Лестничник уступов",
    "🦙 Альпака-носильщик",
    "❄️ Йети-охотник",
    "🪢 Веревочный маг",
    "🌋 Вулканный скалолаз",
    "🦅 Орлиный взгляд",
    "🧊 Ледорубный герой",
    "🌪️ Буря вершин",
    "🗿 Каменный страж",
    "🏞️ Речной переправщик",
    "🌄 Рассветный покоритель",
];

const PIDOR_RANK_TITLES: &[&str] = &[
    "🌈 Пидор дня",
    "💅 Почётный пидор",
    "🍑 Вице-пидор",
    "👠 Пидор-лайт",
    "🧼 Атлант, державший мыло",
    "🎀 Заднеприводный герой",
    "🫦 Главный по радуге",
    "🧷 Вязальщик париков",
    "🪩 Танцполовский",
    "🫧 Мыльный барон",
    "💄 Барон блёсток",
    "🫠 Флексолог попочек",
    "🎀 Кружевной дивизион",
    "🧯 Тушитель страстей",
    "🍸 Мартинезоносец",
    "🧁 Сладкий генерал",
    "🪄 Фея настроения",
    "🕺 Танцор каблуков",
    "🧤 Гуру перчаточек",
    "🪞 Властелин зеркал",
    "🦄 Единорог радуги",
    "💋 Поцелуйный магнат",
    "🪭 Веерный диверсант",
    "🎭 Маскарадный король",
    "🧚 Фея флирта",
    "🌟 Звезда подиума",
    "🍾 Шампанский фонтан",
    "🕶️ Стильный инкогнито",
    "💄 Губная помада-воин",
    "🩰 Балетный бунтарь",
];

const KOTIK_RANK_TITLES: &[&str] = &[
    "🐾 Котик дня",
    "😺 Пушистый принц",
    "🍼 Мяучный барон",
    "🧶 Хозяин клубка",
    "🐟 Лорд шпрот",
    "🛋️ Властелин дивана",
    "🪟 Смотритель окна",
    "🧘 Гибкий йог",
    "🧹 Охотник на швабры",
    "🛎️ Будильник утра",
    "😼 Властелин мисок",
    "🐾 Хозяин когтеточки",
    "🛌 Спящий чемпион",
    "🧴 Лизун лапок",
    "🧺 Король корзинок",
    "🪟 Охотник лучиков",
    "📦 Повелитель коробок",
    "🪵 Точильщик табуреток",
    "🫧 Мастер умывалок",
    "🧊 Сторож холодильника",
    "🦁 Лев в миниатюре",
    "🌙 Ночной охотник",
    "🧸 Плюшевый тиран",
    "🍤 Креветочный магнат",
    "🪁 Воздушный акробат",
    "😽 Поцелуйный эксперт",
    "🛀 Ванный пловец",
    "🌿 Травяной гурман",
    "🎾 Теннисный чемпион",
    "🦋 Бабочко-ловец",
];

const LUCKY_RANK_TITLES: &[&str] = &[
    "🏛️ Чиновник дня",
    "📋 Заместитель по бумагам",
    "🖊️ Начальник отдела печатей",
    "📂 Ведущий специалист по папкам",
    "💼 Инспектор по справкам",
    "📎 Координатор скрепок",
    "🗃️ Хранитель документов",
    "📝 Мастер по заявлениям",
    "🧾 Контролёр квитанций",
    "📋 Куратор бланков",
    "🗂️ Регистратор входящих",
    "📊 Аналитик отчётности",
    "📑 Распорядитель копий",
    "🔖 Смотритель уведомлений",
    "📄 Администратор справок",
    "📨 Почтовый инспектор",
    "🗄️ Архивариус дела",
    "📈 Статистик результатов",
    "⚖️ Оценщик процедур",
    "🏢 Завхоз канцелярии",
    "🖥️ Цифровой бюрократ",
    "☕ Кофейный дипломат",
    "🗞️ Газетный аналитик",
    "🖨️ Принтерный волшебник",
    "📏 Линейный инспектор",
    "🗳️ Выборный координатор",
    "🕰️ Хронометрист очередей",
    "🗝️ Ключевой хранитель",
    "📦 Посыльный герой",
    "🧮 Абакус-мастер",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CheckinGameTheme {
    key: &'static str,
    name: &'static str,
    today: &'static str,
    winner_fmt: &'static str,
    rank_titles: &'static [&'static str],
}

impl CheckinGameTheme {
    fn winner_text(self, linked_name: &str) -> String {
        self.winner_fmt.replace("%s", linked_name)
    }
}

fn resolve_checkin_theme(
    settings: &openplotva_core::ChatSettings,
    theme_override: &str,
) -> CheckinGameTheme {
    let mut key = theme_override.trim().to_ascii_lowercase();
    if key.is_empty() {
        if let Some(setting) = settings
            .daily_game_theme
            .as_deref()
            .filter(|setting| !setting.is_empty() && *setting != "auto")
        {
            key = setting.to_owned();
        }
        if key.is_empty() {
            key = DEFAULT_THEME_KEY.to_owned();
        }
    }
    theme_by_key(&key)
}

fn theme_by_key(key: &str) -> CheckinGameTheme {
    match key.trim().to_ascii_lowercase().as_str() {
        "pidor" => CheckinGameTheme {
            key: "pidor",
            name: "Пидор дня",
            today: "🌈 <b>Время Пидора дня!</b> Кто сегодня засияет ярче всех в радужном сиянии? ✨💅",
            winner_fmt: "🌟 <b>ПИДОР ДНЯ провозглашён!</b> %s — эталон блеска и гламура! 💖😂",
            rank_titles: PIDOR_RANK_TITLES,
        },
        "kotik" => CheckinGameTheme {
            key: "kotik",
            name: "Котик дня",
            today: "🐱 <b>День Котика дня!</b> Кто сегодня будет мурлыкать громче всех? 🐾😻",
            winner_fmt: "😻 <b>Мяу-триумф!</b> %s — самый пушистый Котик дня! Погладьте срочно! 🧶😂",
            rank_titles: KOTIK_RANK_TITLES,
        },
        "lucky" => CheckinGameTheme {
            key: "lucky",
            name: "Чиновник дня",
            today: "🏛️ <b>Час Чиновника дня!</b> Кто сегодня подпишет больше всего бумаг с улыбкой? 📄✍️",
            winner_fmt: "📜 <b>Бюрократический триумф!</b> %s — Чиновник дня, мастер печатей! 🏆😂",
            rank_titles: LUCKY_RANK_TITLES,
        },
        _ => CheckinGameTheme {
            key: "king",
            name: "Король горы",
            today: "🏔️ <b>Сегодня взбираемся на гору!</b> Кто захватит вершину и станет легендой? 🔥🧗‍♂️",
            winner_fmt: "👑 <b>Вершина покорена!</b> %s — истинный <i>Король горы</i>! 🏆😂",
            rank_titles: KING_RANK_TITLES,
        },
    }
}

pub trait CheckinThemeEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute one direct Telegram Bot API method.
    fn execute_checkin_callback_method<'a>(
        &'a self,
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> CheckinThemeEffectFuture<'a, Self::Error>;

    fn send_checkin_queue_error_notice<'a>(
        &'a self,
        message: CheckinCallbackMessage,
        text: &'static str,
    ) -> CheckinThemeEffectFuture<'a, Self::Error>;
}

/// Direct Telegram executor boundary used by concrete check-in callback effects.
pub trait CheckinCallbackMethodExecutor {
    /// Error returned by the concrete Telegram executor.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute one direct Telegram Bot API method.
    fn execute_checkin_callback_method_direct<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> CheckinCallbackMethodFuture<'a, Self::Error>;
}

impl CheckinCallbackMethodExecutor for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn execute_checkin_callback_method_direct<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> CheckinCallbackMethodFuture<'a, Self::Error> {
        Box::pin(async move { method.execute_with(self).await.map(|_| ()) })
    }
}

/// Generator for virtual-message IDs used by check-in callback notices.
pub type CheckinThemeVirtualIdFactory = VirtualIdFactory;

/// Concrete check-in callback effects backed by direct Telegram calls and the dispatcher.
#[derive(Clone)]
pub struct CheckinThemeRuntimeEffects<Store, Executor> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    executor: Executor,
    next_virtual_id: CheckinThemeVirtualIdFactory,
}

impl<Store, Executor> CheckinThemeRuntimeEffects<Store, Executor> {
    /// Build runtime check-in theme effects.
    #[must_use]
    pub fn new(store: Store, queue: Arc<DispatcherQueue>, executor: Executor) -> Self {
        Self {
            store,
            queue,
            executor,
            next_virtual_id: monotonic_virtual_id_factory("checkin-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(
        mut self,
        next_virtual_id: CheckinThemeVirtualIdFactory,
    ) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Store, Executor> CheckinThemeEffects for CheckinThemeRuntimeEffects<Store, Executor>
where
    Store: VirtualMessageStore + Send + Sync,
    Executor: CheckinCallbackMethodExecutor + Send + Sync,
{
    type Error = CheckinThemeRuntimeEffectError;

    fn execute_checkin_callback_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> CheckinThemeEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            self.executor
                .execute_checkin_callback_method_direct(method)
                .await
                .map_err(|error| CheckinThemeRuntimeEffectError::Telegram {
                    message: error.to_string(),
                })
        })
    }

    fn send_checkin_queue_error_notice<'a>(
        &'a self,
        message: CheckinCallbackMessage,
        text: &'static str,
    ) -> CheckinThemeEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat = ChatRef {
                id: message.chat_id,
                is_forum: message.thread_id.is_some(),
            };
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
                message_id: i64::from(message.message_id),
                chat,
                is_topic_message: message.thread_id.is_some(),
                message_thread_id: message.thread_id.map(i64::from).unwrap_or_default(),
            };
            queue_text_message_parts(
                &self.store,
                &self.queue,
                QueueTextRequest {
                    message: &request,
                    reply_to: Some(&reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(CHECKIN_QUEUE_ERROR_DELETE_AFTER),
                },
                || (self.next_virtual_id)(),
            )
            .await?;
            Ok(())
        })
    }
}

/// Recoverable errors from concrete check-in callback effects.
#[derive(Debug, Eq, Error, PartialEq)]
pub enum CheckinThemeRuntimeEffectError {
    /// Direct Telegram callback method failed.
    #[error("failed to execute check-in callback method: {message}")]
    Telegram {
        /// Display form of the Telegram executor error.
        message: String,
    },
    /// Queueing the one-minute queue-failure notice failed.
    #[error("failed to queue check-in callback notice: {0}")]
    Queue(#[from] OutboundBuildError),
}

/// Message context attached to a check-in callback query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckinCallbackMessage {
    /// Callback message chat.
    pub chat_id: i64,
    /// Callback message ID.
    pub message_id: i32,
    /// Forum topic ID when present.
    pub thread_id: Option<i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckinThemeCallbackOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    /// Callback matched check-in theme selection but did not carry an accessible message.
    MissingMessage,
    Blocked,
    /// Theme selection was acknowledged and queued as a high-priority control job.
    Queued,
    /// Theme selection was acknowledged, but task assignment failed.
    QueueError,
}

/// Result of one check-in control-job execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckinControlJobOutcome {
    Completed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckinCommandOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    Skipped,
    /// Existing winner was reported with yearly stats.
    ExistingWinnerWithStats,
    /// Theme selector was queued.
    ThemeSelectorQueued,
    /// Check-in control job was queued.
    ControlJobQueued,
    /// Check-in control-job assignment failed and the failure notice was attempted.
    QueueError,
    /// Command message could not be reconstructed into a control job.
    MissingMessage,
}

/// Result of one check-in taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckinControlJobWorkerReport {
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Check-in executor result, when payload decoding succeeded.
    pub execution: Option<CheckinControlJobOutcome>,
    /// The job was finalized as completed.
    pub completed: bool,
    /// The job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Game runner failed.
    pub execution_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

/// Fatal errors from the check-in callback wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum CheckinThemeCallbackError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
    /// Permission check failed before command handling.
    #[error("permission check failed: {message}")]
    Permission {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct CheckinThemeCallbackUpdateHandler<Queue, Effects, Next> {
    queue: Arc<Queue>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Queue, Effects, Next> CheckinThemeCallbackUpdateHandler<Queue, Effects, Next> {
    /// Build a check-in callback handler around the real downstream update handler.
    #[must_use]
    pub fn new(queue: Arc<Queue>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            queue,
            effects,
            next,
        }
    }
}

impl<Queue, Effects, Next> UpdateHandler for CheckinThemeCallbackUpdateHandler<Queue, Effects, Next>
where
    Queue: CheckinControlJobQueue + Send + Sync,
    Effects: CheckinThemeEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = CheckinThemeCallbackError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_checkin_theme_callback_update_or_else_at(
                self.queue.as_ref(),
                self.effects.as_ref(),
                update,
                OffsetDateTime::now_utc(),
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

#[derive(Clone, Debug)]
pub struct CheckinCommandUpdateHandler<Queue, Store, Permission, Effects, Next> {
    queue: Arc<Queue>,
    store: Arc<Store>,
    permission: Arc<Permission>,
    effects: Arc<Effects>,
    bot_username: String,
    next: Arc<Next>,
}

impl<Queue, Store, Permission, Effects, Next>
    CheckinCommandUpdateHandler<Queue, Store, Permission, Effects, Next>
{
    /// Build a check-in command handler around the real downstream update handler.
    #[must_use]
    pub fn new(
        queue: Arc<Queue>,
        store: Arc<Store>,
        permission: Arc<Permission>,
        effects: Arc<Effects>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            queue,
            store,
            permission,
            effects,
            bot_username: bot_username.into(),
            next,
        }
    }
}

impl<Queue, Store, Permission, Effects, Next> UpdateHandler
    for CheckinCommandUpdateHandler<Queue, Store, Permission, Effects, Next>
where
    Queue: CheckinControlJobQueue + Send + Sync,
    Store: CheckinGameStore + Send + Sync,
    Permission: CheckinCommandPermission + Send + Sync,
    Effects: CheckinCommandEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = CheckinThemeCallbackError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            let created = OffsetDateTime::now_utc();
            let can_send_text = match &update.update_type {
                TelegramUpdateType::Message(message)
                    if is_checkin_command_for_bot(message, &self.bot_username) =>
                {
                    self.permission
                        .can_send_checkin_text(
                            message.chat.get_id().into(),
                            chat_type_name(&message.chat),
                            created,
                        )
                        .await
                        .map_err(|error| CheckinThemeCallbackError::Permission {
                            message: error.to_string(),
                        })?
                }
                _ => true,
            };
            handle_checkin_command_update_or_else_at(
                self.queue.as_ref(),
                self.store.as_ref(),
                self.effects.as_ref(),
                update,
                CheckinCommandUpdateContext {
                    bot_username: &self.bot_username,
                    can_send_text,
                    created,
                },
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_checkin_theme_callback_update_or_else_at<
    Queue,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    queue: &Queue,
    effects: &Effects,
    update: TelegramUpdate,
    created: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<CheckinThemeCallbackOutcome, CheckinThemeCallbackError>
where
    Queue: CheckinControlJobQueue + Sync,
    Effects: CheckinThemeEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::CallbackQuery(query) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| CheckinThemeCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(CheckinThemeCallbackOutcome::Delegated);
    };

    let Some(data) = checkin_theme_callback_data(query) else {
        handle_other(update)
            .await
            .map_err(|error| CheckinThemeCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(CheckinThemeCallbackOutcome::Delegated);
    };

    Ok(handle_checkin_theme_callback_at(queue, effects, query, &data, created).await)
}

pub async fn handle_checkin_theme_callback_at<Queue, Effects>(
    queue: &Queue,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    data: &openplotva_telegram::CallbackActionData,
    created: OffsetDateTime,
) -> CheckinThemeCallbackOutcome
where
    Queue: CheckinControlJobQueue + Sync,
    Effects: CheckinThemeEffects + Sync,
{
    let (ack, blocked) = openplotva_telegram::checkin_theme_selection_ack_method(
        query.id.clone(),
        i64::from(query.from.id),
        data,
    );
    try_execute_checkin_callback_method(effects, ack, "check-in theme ack").await;
    if blocked {
        return CheckinThemeCallbackOutcome::Blocked;
    }

    let Some(message) = checkin_callback_message(query) else {
        return CheckinThemeCallbackOutcome::MissingMessage;
    };
    let Some(job) = checkin_control_job_from_callback_query_at(query, data, created) else {
        return CheckinThemeCallbackOutcome::MissingMessage;
    };

    if let Err(error) = queue
        .assign_checkin_control_job(CONTROL_QUEUE_NAME, job)
        .await
    {
        tracing::warn!(message = %error, "failed to assign check-in control job");
        try_send_checkin_queue_error_notice(effects, message).await;
        return CheckinThemeCallbackOutcome::QueueError;
    }

    CheckinThemeCallbackOutcome::Queued
}

#[must_use]
pub fn checkin_control_job_from_callback_query_at(
    query: &TelegramCallbackQuery,
    data: &openplotva_telegram::CallbackActionData,
    created: OffsetDateTime,
) -> Option<StatelessJobItem> {
    let MaybeInaccessibleMessage::Message(message) = query.message.as_ref()? else {
        return None;
    };
    checkin_control_job_from_message_at(
        message,
        openplotva_telegram::checkin_theme_callback_theme(data),
        created,
    )
}

#[must_use]
pub fn checkin_control_job_from_message_at(
    message: &TelegramMessage,
    theme: &str,
    created: OffsetDateTime,
) -> Option<StatelessJobItem> {
    let message_id = i32::try_from(message.id).ok()?;
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
    let mut data = control_data_from_checkin_message(message);
    data.kind = ControlKind::Checkin;
    data.theme = theme.to_owned();
    let params = ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id,
        user_id,
        user_full_name,
        thread_id,
        data,
    };
    Some(
        new_control_job_at(params, created)
            .with_name(CHECKIN_CONTROL_JOB_TITLE)
            .with_priority(HIGH_PRIORITY),
    )
}

pub async fn handle_checkin_command_update_or_else_at<
    Queue,
    Store,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    queue: &Queue,
    store: &Store,
    effects: &Effects,
    update: TelegramUpdate,
    context: CheckinCommandUpdateContext<'_>,
    handle_other: HandleFn,
) -> Result<CheckinCommandOutcome, CheckinThemeCallbackError>
where
    Queue: CheckinControlJobQueue + Sync,
    Store: CheckinGameStore + Sync,
    Effects: CheckinCommandEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| CheckinThemeCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(CheckinCommandOutcome::Delegated);
    };

    if !is_checkin_command_for_bot(message, context.bot_username) {
        handle_other(update)
            .await
            .map_err(|error| CheckinThemeCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(CheckinCommandOutcome::Delegated);
    }

    Ok(handle_checkin_command_message_at(
        queue,
        store,
        effects,
        message,
        context.can_send_text,
        context.created,
    )
    .await)
}

pub async fn handle_checkin_command_message_at<Queue, Store, Effects>(
    queue: &Queue,
    store: &Store,
    effects: &Effects,
    message: &TelegramMessage,
    can_send_text: bool,
    created: OffsetDateTime,
) -> CheckinCommandOutcome
where
    Queue: CheckinControlJobQueue + Sync,
    Store: CheckinGameStore + Sync,
    Effects: CheckinCommandEffects + Sync,
{
    if telegram_chat_is_private(&message.chat) || !can_send_text {
        return CheckinCommandOutcome::Skipped;
    }
    let Some(reply_to) = checkin_game_message(message) else {
        return CheckinCommandOutcome::MissingMessage;
    };

    if let Some(winner) = load_today_winner(store, reply_to.chat_id).await {
        let html = today_winner_with_stats_html(store, reply_to.chat_id, &winner).await;
        try_send_checkin_today_winner_with_stats(effects, reply_to, html).await;
        return CheckinCommandOutcome::ExistingWinnerWithStats;
    }

    let settings = match store.get_checkin_chat_settings(reply_to.chat_id).await {
        Ok(Some(settings)) => settings,
        Ok(None) | Err(_) => openplotva_core::ChatSettings::defaults(reply_to.chat_id),
    };
    if settings
        .daily_game_theme
        .as_deref()
        .is_some_and(|theme| theme.eq_ignore_ascii_case("auto"))
    {
        let sender = openplotva_updates::resolve_message_sender(Some(message));
        if sender.sender_type == openplotva_core::SENDER_TYPE_USER && !sender.is_bot {
            let user_id = message
                .sender
                .get_user()
                .map(|user| i64::from(user.id))
                .unwrap_or_default();
            try_send_checkin_theme_selector(effects, reply_to, user_id).await;
            return CheckinCommandOutcome::ThemeSelectorQueued;
        }
    }

    let Some(job) = checkin_control_job_from_message_at(message, "", created) else {
        return CheckinCommandOutcome::MissingMessage;
    };
    if let Err(error) = queue
        .assign_checkin_control_job(CONTROL_QUEUE_NAME, job)
        .await
    {
        tracing::warn!(message = %error, chat_id = reply_to.chat_id, "failed to assign /checkin control job");
        try_send_checkin_command_queue_error(effects, reply_to).await;
        return CheckinCommandOutcome::QueueError;
    }

    CheckinCommandOutcome::ControlJobQueued
}

/// Process one check-in control job, if available.
pub async fn process_checkin_control_job_once_at<Queue, Effects>(
    queue: &Queue,
    effects: &Effects,
) -> CheckinControlJobWorkerReport
where
    Queue: CheckinControlJobWorkerQueue + Sync,
    Effects: CheckinControlJobEffects + Sync,
{
    let mut report = CheckinControlJobWorkerReport::default();
    let item = match queue.dequeue_checkin_control_job(CONTROL_QUEUE_NAME).await {
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
            mark_checkin_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    if params.data.kind != ControlKind::Checkin {
        let error = format!(
            "unsupported check-in control job kind: {:?}",
            params.data.kind
        );
        report.execution_error = Some(error.clone());
        mark_checkin_control_job_failed(queue, item.id, &error, &mut report).await;
        return report;
    }

    match effects.run_checkin_game(&params).await {
        Ok(()) => match queue.complete_checkin_control_job(item.id).await {
            Ok(()) => {
                report.completed = true;
                report.execution = Some(CheckinControlJobOutcome::Completed);
            }
            Err(error) => report.status_error = Some(error.to_string()),
        },
        Err(error) => {
            let error = error.to_string();
            report.execution_error = Some(error.clone());
            mark_checkin_control_job_failed(queue, item.id, &error, &mut report).await;
        }
    }
    report
}

async fn mark_checkin_control_job_failed<Queue>(
    queue: &Queue,
    job_id: i64,
    error: &str,
    report: &mut CheckinControlJobWorkerReport,
) where
    Queue: CheckinControlJobWorkerQueue + Sync,
{
    match queue.fail_checkin_control_job(job_id, error).await {
        Ok(()) => report.failed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
}

fn is_checkin_control_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == JobType::Control
        && matches!(
            job.data.control_data.as_ref().map(|data| data.kind),
            Some(ControlKind::Checkin)
        )
}

fn checkin_theme_callback_data(
    query: &TelegramCallbackQuery,
) -> Option<openplotva_telegram::CallbackActionData> {
    let openplotva_telegram::CallbackActionParse::Action { action, data } =
        openplotva_telegram::parse_callback_action(query.data.as_deref().unwrap_or_default())
    else {
        return None;
    };
    matches!(action.as_str(), "checkin_theme_select" | "cts").then_some(data)
}

fn checkin_callback_message(query: &TelegramCallbackQuery) -> Option<CheckinCallbackMessage> {
    let MaybeInaccessibleMessage::Message(message) = query.message.as_ref()? else {
        return None;
    };
    Some(CheckinCallbackMessage {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        thread_id: message
            .message_thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok())
            .filter(|thread_id| *thread_id != 0),
    })
}

fn checkin_game_message(message: &TelegramMessage) -> Option<CheckinGameMessage> {
    Some(CheckinGameMessage {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).ok()?,
        thread_id: message
            .message_thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok())
            .filter(|thread_id| *thread_id != 0),
    })
}

fn control_data_from_checkin_message(message: &TelegramMessage) -> ControlJobData {
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

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Channel(_) => "channel",
        TelegramChat::Group(_) => "group",
        TelegramChat::Private(_) => "private",
        TelegramChat::Supergroup(_) => "supergroup",
    }
}

fn user_full_name(user: &TelegramUser) -> String {
    format!(
        "{} {}",
        user.first_name,
        user.last_name.as_deref().unwrap_or_default()
    )
    .trim()
    .to_owned()
}

fn is_checkin_command_for_bot(message: &TelegramMessage, _bot_username: &str) -> bool {
    if telegram_chat_is_private(&message.chat) {
        return false;
    }
    let Some(command) = leading_bot_command(message) else {
        return false;
    };
    // strips any @target, so even /checkin@OtherBot is handled in groups.
    command.command == CHECKIN_COMMAND
}

fn telegram_chat_is_private(chat: &TelegramChat) -> bool {
    matches!(chat, TelegramChat::Private(_))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BotCommandInMessage {
    command: String,
    target: Option<String>,
}

fn leading_bot_command(message: &TelegramMessage) -> Option<BotCommandInMessage> {
    let TelegramMessageData::Text(text) = &message.data else {
        return None;
    };
    leading_bot_command_from_text(text)
}

fn leading_bot_command_from_text(text: &TelegramText) -> Option<BotCommandInMessage> {
    let first = text.entities.as_ref()?.into_iter().next()?;
    let TelegramTextEntity::BotCommand(position) = first else {
        return None;
    };
    if position.offset != 0 {
        return None;
    }
    let command_with_slash = text_entity_content(&text.data, *position)?;
    let command_with_target = command_with_slash.strip_prefix('/')?;
    let (command, target) = match command_with_target.split_once('@') {
        Some((command, target)) => (command, Some(target.to_owned())),
        None => (command_with_target, None),
    };
    Some(BotCommandInMessage {
        command: command.to_owned(),
        target,
    })
}

fn text_entity_content(text: &str, position: TelegramTextEntityPosition) -> Option<String> {
    let offset = usize::try_from(position.offset).ok()?;
    let length = usize::try_from(position.length).ok()?;
    Some(String::from_utf16_lossy(
        &text
            .encode_utf16()
            .skip(offset)
            .take(length)
            .collect::<Vec<u16>>(),
    ))
}

async fn try_execute_checkin_callback_method<Effects>(
    effects: &Effects,
    method: openplotva_telegram::TelegramOutboundMethod,
    context: &'static str,
) where
    Effects: CheckinThemeEffects + Sync,
{
    let method_name = method.method_name();
    if let Err(error) = effects.execute_checkin_callback_method(method).await {
        tracing::warn!(
            message = %error,
            method = method_name,
            context,
            "check-in callback Telegram side effect failed"
        );
    }
}

async fn try_send_checkin_queue_error_notice<Effects>(
    effects: &Effects,
    message: CheckinCallbackMessage,
) where
    Effects: CheckinThemeEffects + Sync,
{
    if let Err(error) = effects
        .send_checkin_queue_error_notice(message, CHECKIN_QUEUE_ERROR_TEXT)
        .await
    {
        tracing::warn!(message = %error, "failed to send check-in queue error notice");
    }
}

async fn try_send_checkin_today_winner_with_stats<Effects>(
    effects: &Effects,
    message: CheckinGameMessage,
    html: String,
) where
    Effects: CheckinCommandEffects + Sync,
{
    if let Err(error) = effects
        .send_checkin_today_winner_with_stats(message, html)
        .await
    {
        tracing::warn!(message = %error, chat_id = message.chat_id, "failed to queue /checkin winner stats");
    }
}

async fn try_send_checkin_theme_selector<Effects>(
    effects: &Effects,
    message: CheckinGameMessage,
    user_id: i64,
) where
    Effects: CheckinCommandEffects + Sync,
{
    if let Err(error) = effects.send_checkin_theme_selector(message, user_id).await {
        tracing::warn!(message = %error, chat_id = message.chat_id, user_id, "failed to queue /checkin theme selector");
    }
}

async fn try_send_checkin_command_queue_error<Effects>(
    effects: &Effects,
    message: CheckinGameMessage,
) where
    Effects: CheckinCommandEffects + Sync,
{
    if let Err(error) = effects.send_checkin_command_queue_error(message).await {
        tracing::warn!(message = %error, chat_id = message.chat_id, "failed to queue /checkin queue-error notice");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callbacks::{
        CallbackQueryEffects, CallbackQueryFuture, CallbackQueryUpdateRoute,
        CallbackRateLimitFuture, CallbackRateLimitPolicy, handle_callback_query_update_or_else,
    };
    use crate::payments::{InMemoryPaymentControlJobQueue, InMemoryPaymentControlJobStatus};
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };
    use crate::virtual_messages::VirtualMessageStore;
    use openplotva_core::MessageIdMapping;
    use openplotva_taskman::{JobPayload, JobType};
    use openplotva_telegram::{
        CallbackHandlerKind, DispatcherConfig, DispatcherQueue, TelegramOutboundMethodKind,
    };
    use openplotva_updates::UpdateConsumerConfig;
    use serde_json::{Value, json};
    use std::{
        collections::{HashMap, HashSet},
        env,
        future::Future,
        io,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    #[tokio::test]
    async fn checkin_theme_callback_acks_and_queues_go_control_job()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let queue = QueueStub::default();
        let effects = EffectsStub::default();
        let next = NextStub::default();

        let outcome = handle_checkin_theme_callback_update_or_else_at(
            &queue,
            &effects,
            sample_callback_update(
                r#"{"action":"checkin_theme_select","init":"42","theme":"classic"}"#,
                42,
            )?,
            created,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, CheckinThemeCallbackOutcome::Queued);
        assert!(next.calls().is_empty());
        assert_eq!(
            method_names(&effects.methods()),
            vec!["answerCallbackQuery"]
        );
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "checkin");
        assert_eq!(assigned[0].1.priority, HIGH_PRIORITY);
        let JobPayload {
            job_type,
            telegram_data,
            control_data,
            ..
        } = &assigned[0].1.data;
        assert_eq!(*job_type, JobType::Control);
        let telegram = telegram_data.as_ref().expect("telegram data");
        assert_eq!(telegram.chat_id, -10042);
        assert_eq!(telegram.message_id, 555);
        assert_eq!(telegram.user_id, 777000);
        assert_eq!(telegram.thread_message_id, Some(77));
        assert_eq!(telegram.user_full_name, "Plotva Bot");
        let control = control_data.as_ref().expect("control data");
        assert_eq!(control.kind, ControlKind::Checkin);
        assert_eq!(control.theme, "classic");
        assert_eq!(control.chat_type, "supergroup");
        assert_eq!(control.user_name, "plotva_bot");
        assert_eq!(control.first_name, "Plotva");
        assert_eq!(control.last_name, "Bot");
        Ok(())
    }

    #[tokio::test]
    async fn checkin_theme_callback_blocks_non_initiator_without_queueing()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let effects = EffectsStub::default();

        let outcome = handle_checkin_theme_callback_update_or_else_at(
            &queue,
            &effects,
            sample_callback_update(r#"{"a":"cts","i":"99","t":"short"}"#, 42)?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, CheckinThemeCallbackOutcome::Blocked);
        assert!(queue.assigned().is_empty());
        let methods = effects.methods();
        assert_eq!(method_names(&methods), vec!["answerCallbackQuery"]);
        assert_eq!(
            methods[0].1["text"],
            json!("Только инициатор может выбрать тему")
        );
        assert_eq!(methods[0].1["show_alert"], json!(true));
        Ok(())
    }

    #[tokio::test]
    async fn checkin_theme_callback_reports_queue_error_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::failing();
        let effects = EffectsStub::default();

        let outcome = handle_checkin_theme_callback_update_or_else_at(
            &queue,
            &effects,
            sample_callback_update(
                r#"{"action":"checkin_theme_select","init":"42","theme":"daily"}"#,
                42,
            )?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, CheckinThemeCallbackOutcome::QueueError);
        assert_eq!(
            effects.notices(),
            vec![(
                CheckinCallbackMessage {
                    chat_id: -10042,
                    message_id: 555,
                    thread_id: Some(77),
                },
                CHECKIN_QUEUE_ERROR_TEXT.to_owned(),
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn checkin_theme_callback_delegates_other_callbacks_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let effects = EffectsStub::default();
        let next = NextStub::default();

        let outcome = handle_checkin_theme_callback_update_or_else_at(
            &queue,
            &effects,
            sample_callback_update(r#"{"action":"delete"}"#, 42)?,
            OffsetDateTime::UNIX_EPOCH,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, CheckinThemeCallbackOutcome::Delegated);
        assert_eq!(next.calls(), vec![1001]);
        assert!(effects.methods().is_empty());
        assert!(queue.assigned().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_checkin_theme_callback_delegates_and_queues_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-checkin-theme-callback:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&sample_callback_update(
                r#"{"action":"checkin_theme_select","init":"42","theme":"classic"}"#,
                42,
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded check-in theme callback update"))?;

        let state_store = UpdateStateStoreStub::default();
        let callback_effects = CallbackAckEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let queue = QueueStub::default();
        let effects = EffectsStub::default();
        let next = NextStub::default();
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let prehandler_route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&prehandler_route);
        let checkin_outcome = Arc::new(Mutex::new(None));
        let captured_outcome = Arc::clone(&checkin_outcome);

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + Duration::from_secs(1_710_000_000),
            &state_store,
            |update| async {
                let route = handle_callback_query_update_or_else(
                    &callback_effects,
                    &rate_limit,
                    update,
                    |update| async {
                        let outcome = handle_checkin_theme_callback_update_or_else_at(
                            &queue,
                            &effects,
                            update,
                            created,
                            |update| next.handle_update(update),
                        )
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                        *captured_outcome.lock().expect("check-in outcome") = Some(outcome);
                        Ok::<(), io::Error>(())
                    },
                )
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
                *captured_route.lock().expect("callback route") = Some(route);
                Ok::<(), io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 1001);
        assert_eq!(report.update_name, "callback_query");
        assert_eq!(
            report.state.outcome,
            openplotva_updates::UpdateStageOutcome::Completed
        );
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&openplotva_updates::UpdateStageOutcome::Completed)
        );
        assert_eq!(
            state_store.calls(),
            vec!["chat:-10042".to_owned(), "user:42".to_owned()]
        );
        assert_eq!(
            *prehandler_route.lock().expect("callback route"),
            Some(CallbackQueryUpdateRoute::HandlerDelegated {
                handler: CallbackHandlerKind::CheckinThemeSelect,
                action: "checkin_theme_select".to_owned(),
            })
        );
        assert_eq!(
            *checkin_outcome.lock().expect("check-in outcome"),
            Some(CheckinThemeCallbackOutcome::Queued)
        );
        assert!(callback_effects.methods().is_empty());
        assert!(next.calls().is_empty());
        assert_eq!(
            method_names(&effects.methods()),
            vec!["answerCallbackQuery"]
        );
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "checkin");
        assert_eq!(assigned[0].1.priority, HIGH_PRIORITY);
        assert_eq!(
            assigned[0]
                .1
                .data
                .control_data
                .as_ref()
                .expect("control data")
                .theme,
            "classic"
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_checkin_commands_route_selector_job_and_stats_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Clone, Copy)]
        enum ExpectedRoute {
            ThemeSelector,
            ControlJob,
            ExistingWinnerStats,
        }

        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-checkin-command:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let cases = vec![
            (
                "auto selector",
                "/checkin",
                false,
                ExpectedRoute::ThemeSelector,
            ),
            (
                "explicit job",
                "/checkin@plotva_bot",
                false,
                ExpectedRoute::ControlJob,
            ),
            (
                "existing winner",
                "/checkin",
                false,
                ExpectedRoute::ExistingWinnerStats,
            ),
        ];
        for (_, text, from_bot, _) in &cases {
            update_queue
                .enqueue_update(&sample_message_update(text, *from_bot)?)
                .await?;
        }
        assert_eq!(update_queue.len().await?, cases.len() as i64);

        let state_store = UpdateStateStoreStub::default();
        let mut outcomes = Vec::new();

        for (label, _, _, expected) in cases {
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| {
                    io::Error::other(format!("expected decoded check-in {label} update"))
                })?;
            let queue = QueueStub::default();
            let store = GameStoreStub::default();
            let effects = CommandEffectsStub::default();
            let next = NextStub::default();
            match expected {
                ExpectedRoute::ThemeSelector => {
                    let mut settings = openplotva_core::ChatSettings::defaults(-10042);
                    settings.daily_game_theme = Some("auto".to_owned());
                    store.set_settings(settings);
                }
                ExpectedRoute::ControlJob => {
                    let mut settings = openplotva_core::ChatSettings::defaults(-10042);
                    settings.daily_game_theme = Some("king".to_owned());
                    store.set_settings(settings);
                }
                ExpectedRoute::ExistingWinnerStats => {
                    store.set_today_winner(game_result(101, "lucky")?);
                    store.set_users(vec![user(101, "Alice", Some("Smith"), Some("alice"))]);
                    store.set_yearly_top(vec![openplotva_storage::ChatGameTopRow {
                        user: user(101, "Alice", Some("Smith"), Some("alice")),
                        wins_count: 3,
                        last_win_at: Some(OffsetDateTime::UNIX_EPOCH),
                    }]);
                }
            }
            let mut routed = None;
            let report = process_update_with_state_store_at(
                decoded,
                UpdateConsumerConfig {
                    dequeue_timeout: Duration::from_millis(1),
                    state_timeout: Duration::from_secs(1),
                    handle_timeout: Duration::from_secs(1),
                    side_effect_max_age: Duration::from_secs(60),
                    worker_limit: 1,
                },
                UNIX_EPOCH + Duration::from_secs(1_710_000_000),
                &state_store,
                |update| async {
                    let outcome = handle_checkin_command_update_or_else_at(
                        &queue,
                        &store,
                        &effects,
                        update,
                        CheckinCommandUpdateContext {
                            bot_username: "plotva_bot",
                            can_send_text: true,
                            created: OffsetDateTime::from_unix_timestamp(1_779_193_800)
                                .map_err(io::Error::other)?,
                        },
                        |update| next.handle_update(update),
                    )
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?;
                    routed = Some(outcome);
                    Ok::<(), io::Error>(())
                },
            )
            .await;

            assert_eq!(report.update_id, 1001, "{label}");
            assert_eq!(report.update_name, "message", "{label}");
            assert_eq!(
                report.state.outcome,
                openplotva_updates::UpdateStageOutcome::Completed,
                "{label}"
            );
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&openplotva_updates::UpdateStageOutcome::Completed),
                "{label}"
            );
            assert!(!report.skipped_handle, "{label}");
            assert!(next.calls().is_empty(), "{label}");

            match expected {
                ExpectedRoute::ThemeSelector => {
                    assert_eq!(routed, Some(CheckinCommandOutcome::ThemeSelectorQueued));
                    assert_eq!(effects.selectors(), vec![(game_message(), 42)]);
                    assert!(queue.assigned().is_empty());
                    assert!(effects.stats().is_empty());
                }
                ExpectedRoute::ControlJob => {
                    assert_eq!(routed, Some(CheckinCommandOutcome::ControlJobQueued));
                    assert!(effects.selectors().is_empty());
                    assert!(effects.stats().is_empty());
                    let assigned = queue.assigned();
                    assert_eq!(assigned.len(), 1);
                    assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
                    assert_eq!(assigned[0].1.title, CHECKIN_CONTROL_JOB_TITLE);
                    assert_eq!(assigned[0].1.priority, HIGH_PRIORITY);
                    let params = control_job_params_from_stateless_job(&assigned[0].1)?;
                    assert_eq!(params.data.kind, ControlKind::Checkin);
                    assert_eq!(params.data.theme, "");
                    assert_eq!(params.chat_id, -10042);
                    assert_eq!(params.message_id, 555);
                    assert_eq!(params.thread_id, Some(77));
                }
                ExpectedRoute::ExistingWinnerStats => {
                    assert_eq!(routed, Some(CheckinCommandOutcome::ExistingWinnerWithStats));
                    assert!(queue.assigned().is_empty());
                    assert!(effects.selectors().is_empty());
                    let stats = effects.stats();
                    assert_eq!(stats.len(), 1);
                    assert!(stats[0].contains("🏁 Сегодня мы играли в <b>Чиновник дня</b>"));
                    assert!(stats[0].contains("Победитель: Alice Smith"));
                    assert!(stats[0].contains("🏆 Чиновник дня — Лидеры года"));
                }
            }
            outcomes.push(routed);
        }

        let state_calls = state_store.calls();
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.as_str() == "chat:-10042")
                .count(),
            3
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.as_str() == "user:42")
                .count(),
            3
        );
        assert_eq!(outcomes.len(), 3);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn checkin_control_worker_skips_other_jobs_and_runs_checkin_game()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let effects = CheckinControlEffectsStub::default();
        let translate_job = checkin_test_control_job(ControlKind::Translate, "translate", "");
        let checkin_job = checkin_test_control_job(ControlKind::Checkin, "checkin", "classic");
        queue
            .assign_checkin_control_job(CONTROL_QUEUE_NAME, translate_job)
            .await?;
        queue
            .assign_checkin_control_job(CONTROL_QUEUE_NAME, checkin_job.clone())
            .await?;

        let report = process_checkin_control_job_once_at(&queue, &effects).await;

        assert!(report.dequeued);
        assert_eq!(report.job_id, Some(2));
        assert_eq!(report.execution, Some(CheckinControlJobOutcome::Completed));
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(effects.runs(), vec![(-10042, 777000, "classic".to_owned())]);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot[0].status, InMemoryPaymentControlJobStatus::Pending);
        assert_eq!(
            snapshot[1].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(snapshot[1].job, checkin_job);
        Ok(())
    }

    #[tokio::test]
    async fn checkin_game_records_winner_from_go_active_participant_sources()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = GameStoreStub::default();
        store.set_settings(openplotva_core::ChatSettings::defaults(-10042));
        store.set_table_participants(vec![0, 101, 102, 101]);
        store.set_members(vec![
            member(101, "member"),
            member(102, "administrator"),
            member(103, "left"),
        ]);
        store.set_users(vec![
            user(101, "Alice", None, Some("alice")),
            user(102, "Bob", Some("Builder"), Some("bob")),
        ]);
        let sender = GameSenderStub::default();

        let outcome =
            run_checkin_game_at(&store, &sender, game_request("supergroup", "kotik"), |_| 1)
                .await?;

        assert_eq!(
            outcome,
            CheckinGameRunOutcome::WinnerRecorded {
                user_id: 102,
                theme: "kotik".to_owned(),
            }
        );
        assert_eq!(store.recorded(), vec![(-10042, 102, "kotik".to_owned())]);
        assert_eq!(store.incremented(), vec![(-10042, 102)]);
        assert_eq!(sender.ephemeral(), Vec::<String>::new());
        assert_eq!(
            sender.sent_html(),
            vec![
                "<h2>🐱 <b>День Котика дня!</b> Кто сегодня будет мурлыкать громче всех? 🐾😻</h2>"
                    .to_owned()
            ]
        );
        let edits = sender.edits();
        assert_eq!(edits.len(), CHECKIN_PROGRESS_STEPS.len() + 1);
        assert!(edits[0].contains("Сканируем чат... 📡"));
        assert!(edits[1].contains("Подбрасываем монетку... 🪙"));
        assert!(edits[2].contains("Сверяем вибрации... 🔊"));
        assert!(edits[3].contains("Минутку... ⏱️"));
        assert!(
            edits[4]
                .contains("<a href='tg://user?id=102'>Bob Builder</a> — самый пушистый Котик дня")
        );
        Ok(())
    }

    #[test]
    fn checkin_progress_catalogue_and_delay_match_go_shape() {
        let last = |len: usize| len.saturating_sub(1);
        let progress = checkin_progress_lines(theme_by_key("king"), &last);

        assert_eq!(progress.len(), CHECKIN_PROGRESS_STEPS.len() + 1);
        assert_eq!(
            progress[0],
            "<h2>🏔️ <b>Сегодня взбираемся на гору!</b> Кто захватит вершину и станет легендой? 🔥🧗‍♂️</h2>"
        );
        assert_eq!(
            progress[1],
            "<h2>Полируем хрустальный шар предсказаний... 🔮🧼</h2>"
        );
        assert_eq!(
            progress[2],
            "<h2>Катаем шар по лабиринту шансов... 🌀⚽</h2>"
        );
        assert_eq!(
            progress[3],
            "<h2>Собираем букет из четырёхлистных клевера... 🍀💐</h2>"
        );
        assert_eq!(
            progress[4],
            "<h2>Завершаем с фейерверком эмоций... 🎆😍</h2>"
        );
        assert_eq!(checkin_animation_delay(&|_| 0), Duration::from_millis(1500));
        assert_eq!(
            checkin_animation_delay(&|_| 2999),
            Duration::from_millis(4499)
        );
    }

    #[tokio::test]
    async fn checkin_game_reports_existing_winner_without_recording()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = GameStoreStub::default();
        store.set_today_winner(game_result(101, "lucky")?);
        store.set_users(vec![user(101, "", None, Some("alice"))]);
        let sender = GameSenderStub::default();

        let outcome =
            run_checkin_game_at(&store, &sender, game_request("group", ""), |_| 0).await?;

        assert_eq!(outcome, CheckinGameRunOutcome::ExistingWinner);
        assert!(store.recorded().is_empty());
        assert_eq!(sender.edits(), Vec::<String>::new());
        assert_eq!(sender.ephemeral(), Vec::<String>::new());
        assert!(sender.sent_html()[0].contains("🏁 Сегодня мы играли в <b>Чиновник дня</b>"));
        assert!(sender.sent_html()[0].contains("<a href='tg://user?id=101'>@alice</a>"));
        Ok(())
    }

    #[tokio::test]
    async fn checkin_game_disabled_sends_go_ephemeral_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = GameStoreStub::default();
        let mut settings = openplotva_core::ChatSettings::defaults(-10042);
        settings.enable_daily_game = Some(false);
        store.set_settings(settings);
        let sender = GameSenderStub::default();

        let outcome =
            run_checkin_game_at(&store, &sender, game_request("supergroup", ""), |_| 0).await?;

        assert_eq!(outcome, CheckinGameRunOutcome::Disabled);
        assert_eq!(
            sender.ephemeral(),
            vec!["🚫 Игра выключена в настройках".to_owned()]
        );
        assert!(store.recorded().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn checkin_theme_runtime_effects_execute_ack_method_directly()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = VirtualStoreStub::default();
        let dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let executor = MethodExecutorStub::default();
        let effects = CheckinThemeRuntimeEffects::new(
            store.clone(),
            Arc::clone(&dispatcher),
            executor.clone(),
        )
        .with_virtual_id_factory(Arc::new(|| "checkin-vmsg-1".to_owned()));
        let openplotva_telegram::CallbackActionParse::Action { data, .. } =
            openplotva_telegram::parse_callback_action(
                r#"{"action":"checkin_theme_select","init":"42","theme":"classic"}"#,
            )
        else {
            panic!("callback action");
        };
        let (method, blocked) =
            openplotva_telegram::checkin_theme_selection_ack_method("query-id", 42, &data);
        assert!(!blocked);

        effects.execute_checkin_callback_method(method).await?;

        assert_eq!(
            executor.method_kinds(),
            vec![TelegramOutboundMethodKind::AnswerCallbackQuery]
        );
        assert!(store.inserted().is_empty());
        assert!(dispatcher.snapshot().immediate.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn checkin_theme_runtime_effects_queue_error_notice_as_ephemeral_immediate()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = VirtualStoreStub::default();
        let dispatcher = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let executor = MethodExecutorStub::default();
        let effects =
            CheckinThemeRuntimeEffects::new(store.clone(), Arc::clone(&dispatcher), executor)
                .with_virtual_id_factory(Arc::new(|| "checkin-notice-vmsg-1".to_owned()));

        effects
            .send_checkin_queue_error_notice(
                CheckinCallbackMessage {
                    chat_id: -10042,
                    message_id: 555,
                    thread_id: Some(77),
                },
                CHECKIN_QUEUE_ERROR_TEXT,
            )
            .await?;

        assert_eq!(
            store.inserted(),
            vec![("checkin-notice-vmsg-1".to_owned(), -10042, Some(0))]
        );
        let item = dispatcher
            .dequeue_immediate()
            .expect("queued check-in queue-error notice");
        assert!(dispatcher.dequeue_regular().is_none());
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        assert_eq!(item.ephemeral_delete_after(), Some(Duration::from_secs(60)));
        assert!(!item.bypasses_chat_restrictions());
        let (_metadata, method) = item.into_parts();
        let payload = method_as_value(method.expect("queued method"))?;
        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(payload["text"], json!(CHECKIN_QUEUE_ERROR_TEXT));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(555));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn checkin_command_auto_theme_queues_selector_for_human_user()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let store = GameStoreStub::default();
        let mut settings = openplotva_core::ChatSettings::defaults(-10042);
        settings.daily_game_theme = Some("auto".to_owned());
        store.set_settings(settings);
        let effects = CommandEffectsStub::default();
        let message = sample_command_message("/checkin@plotva_bot", false)?;

        let outcome = handle_checkin_command_message_at(
            &queue,
            &store,
            &effects,
            &message,
            true,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await;

        assert_eq!(outcome, CheckinCommandOutcome::ThemeSelectorQueued);
        assert!(queue.assigned().is_empty());
        assert!(effects.stats().is_empty());
        assert_eq!(effects.selectors(), vec![(game_message(), 42)]);
        Ok(())
    }

    #[tokio::test]
    async fn checkin_command_auto_theme_from_bot_queues_control_job()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let store = GameStoreStub::default();
        let mut settings = openplotva_core::ChatSettings::defaults(-10042);
        settings.daily_game_theme = Some("auto".to_owned());
        store.set_settings(settings);
        let effects = CommandEffectsStub::default();
        let message = sample_command_message("/checkin", true)?;

        let outcome = handle_checkin_command_message_at(
            &queue,
            &store,
            &effects,
            &message,
            true,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await;

        assert_eq!(outcome, CheckinCommandOutcome::ControlJobQueued);
        assert!(effects.selectors().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, CHECKIN_CONTROL_JOB_TITLE);
        let params = control_job_params_from_stateless_job(&assigned[0].1)?;
        assert_eq!(params.data.kind, ControlKind::Checkin);
        assert_eq!(params.data.theme, "");
        assert_eq!(params.chat_id, -10042);
        assert_eq!(params.message_id, 555);
        assert_eq!(params.thread_id, Some(77));
        Ok(())
    }

    #[tokio::test]
    async fn checkin_command_existing_winner_sends_yearly_stats()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let store = GameStoreStub::default();
        store.set_today_winner(game_result(101, "lucky")?);
        store.set_users(vec![user(101, "Alice", Some("Smith"), Some("alice"))]);
        store.set_yearly_top(vec![openplotva_storage::ChatGameTopRow {
            user: user(101, "Alice", Some("Smith"), Some("alice")),
            wins_count: 3,
            last_win_at: Some(OffsetDateTime::UNIX_EPOCH),
        }]);
        let effects = CommandEffectsStub::default();
        let message = sample_command_message("/checkin", false)?;

        let outcome = handle_checkin_command_message_at(
            &queue,
            &store,
            &effects,
            &message,
            true,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await;

        assert_eq!(outcome, CheckinCommandOutcome::ExistingWinnerWithStats);
        assert!(queue.assigned().is_empty());
        let stats = effects.stats();
        assert_eq!(stats.len(), 1);
        assert!(stats[0].contains("🏁 Сегодня мы играли в <b>Чиновник дня</b>"));
        assert!(stats[0].contains("Победитель: Alice Smith"));
        assert!(stats[0].contains("🏆 Чиновник дня — Лидеры года"));
        assert!(stats[0].contains("<td>Alice Smith</td>"));
        assert!(stats[0].contains(r#"<td align="right">3</td>"#));
        Ok(())
    }

    #[tokio::test]
    async fn checkin_command_queue_error_sends_go_ephemeral_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::failing();
        let store = GameStoreStub::default();
        let mut settings = openplotva_core::ChatSettings::defaults(-10042);
        settings.daily_game_theme = Some("king".to_owned());
        store.set_settings(settings);
        let effects = CommandEffectsStub::default();
        let message = sample_command_message("/checkin", false)?;

        let outcome = handle_checkin_command_message_at(
            &queue,
            &store,
            &effects,
            &message,
            true,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await;

        assert_eq!(outcome, CheckinCommandOutcome::QueueError);
        assert_eq!(effects.queue_errors(), vec![game_message()]);
        assert!(effects.selectors().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn checkin_command_update_delegates_non_checkin_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let store = GameStoreStub::default();
        let effects = CommandEffectsStub::default();
        let next = NextStub::default();

        let outcome = handle_checkin_command_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_message_update("/reset", false)?,
            CheckinCommandUpdateContext {
                bot_username: "plotva_bot",
                can_send_text: true,
                created: OffsetDateTime::UNIX_EPOCH,
            },
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, CheckinCommandOutcome::Delegated);
        assert_eq!(next.calls(), vec![1001]);
        assert!(queue.assigned().is_empty());
        assert!(effects.selectors().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn checkin_command_update_routes_group_wrong_target_like_go_command_strip()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = QueueStub::default();
        let store = GameStoreStub::default();
        let mut settings = openplotva_core::ChatSettings::defaults(-10042);
        settings.daily_game_theme = Some("king".to_owned());
        store.set_settings(settings);
        let effects = CommandEffectsStub::default();
        let next = NextStub::default();

        let outcome = handle_checkin_command_update_or_else_at(
            &queue,
            &store,
            &effects,
            sample_message_update("/checkin@OtherBot", false)?,
            CheckinCommandUpdateContext {
                bot_username: "plotva_bot",
                can_send_text: true,
                created: OffsetDateTime::UNIX_EPOCH,
            },
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, CheckinCommandOutcome::ControlJobQueued);
        assert!(next.calls().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        let params = control_job_params_from_stateless_job(&assigned[0].1)?;
        assert_eq!(params.data.kind, ControlKind::Checkin);
        assert_eq!(params.chat_id, -10042);
        assert_eq!(params.message_id, 555);
        assert_eq!(params.thread_id, Some(77));
        assert!(effects.selectors().is_empty());
        assert!(effects.stats().is_empty());
        Ok(())
    }

    fn sample_callback_update(
        data: &str,
        from_user_id: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 1001,
            "callback_query": {
                "id": "checkin-callback-id",
                "from": {
                    "id": from_user_id,
                    "is_bot": false,
                    "first_name": "Alice",
                    "username": "alice"
                },
                "chat_instance": "chat-instance",
                "data": data,
                "message": {
                    "message_id": 555,
                    "message_thread_id": 77,
                    "date": 1_710_000_000,
                    "chat": {
                        "id": -10042,
                        "type": "supergroup",
                        "title": "Plotva Group"
                    },
                    "from": {
                        "id": 777000,
                        "is_bot": true,
                        "first_name": "Plotva",
                        "last_name": "Bot",
                        "username": "plotva_bot"
                    },
                    "text": "choose theme"
                }
            }
        }))
    }

    fn sample_message_update(
        text: &str,
        from_bot: bool,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 1001,
            "message": sample_command_message_value(text, from_bot),
        }))
    }

    fn sample_command_message(
        text: &str,
        from_bot: bool,
    ) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(sample_command_message_value(text, from_bot))
    }

    fn sample_command_message_value(text: &str, from_bot: bool) -> Value {
        let command_len = text
            .split_whitespace()
            .next()
            .map(|command| command.encode_utf16().count())
            .unwrap_or_default();
        json!({
            "message_id": 555,
            "message_thread_id": 77,
            "date": 1_710_000_000,
            "chat": {
                "id": -10042,
                "type": "supergroup",
                "title": "Plotva Group"
            },
            "from": {
                "id": 42,
                "is_bot": from_bot,
                "first_name": "Alice",
                "username": "alice"
            },
            "text": text,
            "entities": [
                {
                    "offset": 0,
                    "length": command_len,
                    "type": "bot_command"
                }
            ]
        })
    }

    fn checkin_test_control_job(kind: ControlKind, title: &str, theme: &str) -> StatelessJobItem {
        new_control_job_at(
            ControlJobParams {
                chat_id: -10042,
                message_id: 555,
                user_id: 777000,
                user_full_name: "Plotva Bot".to_owned(),
                thread_id: Some(77),
                data: ControlJobData {
                    kind,
                    theme: theme.to_owned(),
                    chat_type: "supergroup".to_owned(),
                    user_name: "plotva_bot".to_owned(),
                    first_name: "Plotva".to_owned(),
                    last_name: "Bot".to_owned(),
                    ..ControlJobData::default()
                },
            },
            OffsetDateTime::UNIX_EPOCH,
        )
        .with_name(title)
        .with_priority(HIGH_PRIORITY)
    }

    #[derive(Default)]
    struct QueueStub {
        state: Mutex<QueueState>,
    }

    #[derive(Default)]
    struct QueueState {
        assigned: Vec<(&'static str, StatelessJobItem)>,
        fail: bool,
    }

    impl QueueStub {
        fn failing() -> Self {
            Self {
                state: Mutex::new(QueueState {
                    fail: true,
                    ..QueueState::default()
                }),
            }
        }

        fn assigned(&self) -> Vec<(&'static str, StatelessJobItem)> {
            self.lock().assigned.clone()
        }

        fn lock(&self) -> MutexGuard<'_, QueueState> {
            self.state.lock().expect("queue state")
        }
    }

    impl CheckinControlJobQueue for QueueStub {
        type Error = io::Error;

        fn assign_checkin_control_job<'a>(
            &'a self,
            queue_name: &'static str,
            job: StatelessJobItem,
        ) -> CheckinControlJobQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.assigned.push((queue_name, job));
                if state.fail {
                    Err(io::Error::other("queue failed"))
                } else {
                    Ok(())
                }
            })
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        methods: Mutex<Vec<(String, serde_json::Value)>>,
        notices: Mutex<Vec<(CheckinCallbackMessage, String)>>,
    }

    impl EffectsStub {
        fn methods(&self) -> Vec<(String, serde_json::Value)> {
            self.methods.lock().expect("methods").clone()
        }

        fn notices(&self) -> Vec<(CheckinCallbackMessage, String)> {
            self.notices.lock().expect("notices").clone()
        }
    }

    impl CheckinThemeEffects for EffectsStub {
        type Error = io::Error;

        fn execute_checkin_callback_method<'a>(
            &'a self,
            method: openplotva_telegram::TelegramOutboundMethod,
        ) -> CheckinThemeEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let name = method.method_name().to_owned();
                let payload = match method {
                    openplotva_telegram::TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                        serde_json::to_value(method.as_ref()).map_err(io::Error::other)?
                    }
                    other => {
                        return Err(io::Error::other(format!(
                            "unexpected method {}",
                            other.method_name()
                        )));
                    }
                };
                self.methods.lock().expect("methods").push((name, payload));
                Ok(())
            })
        }

        fn send_checkin_queue_error_notice<'a>(
            &'a self,
            message: CheckinCallbackMessage,
            text: &'static str,
        ) -> CheckinThemeEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.notices
                    .lock()
                    .expect("notices")
                    .push((message, text.to_owned()));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CommandEffectsStub {
        stats: Mutex<Vec<String>>,
        selectors: Mutex<Vec<(CheckinGameMessage, i64)>>,
        queue_errors: Mutex<Vec<CheckinGameMessage>>,
    }

    impl CommandEffectsStub {
        fn stats(&self) -> Vec<String> {
            self.stats.lock().expect("stats").clone()
        }

        fn selectors(&self) -> Vec<(CheckinGameMessage, i64)> {
            self.selectors.lock().expect("selectors").clone()
        }

        fn queue_errors(&self) -> Vec<CheckinGameMessage> {
            self.queue_errors.lock().expect("queue errors").clone()
        }
    }

    impl CheckinCommandEffects for CommandEffectsStub {
        type Error = io::Error;

        fn send_checkin_today_winner_with_stats<'a>(
            &'a self,
            _reply_to: CheckinGameMessage,
            html: String,
        ) -> CheckinCommandEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.stats.lock().expect("stats").push(html);
                Ok(())
            })
        }

        fn send_checkin_theme_selector<'a>(
            &'a self,
            reply_to: CheckinGameMessage,
            user_id: i64,
        ) -> CheckinCommandEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.selectors
                    .lock()
                    .expect("selectors")
                    .push((reply_to, user_id));
                Ok(())
            })
        }

        fn send_checkin_command_queue_error<'a>(
            &'a self,
            reply_to: CheckinGameMessage,
        ) -> CheckinCommandEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.queue_errors
                    .lock()
                    .expect("queue errors")
                    .push(reply_to);
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CheckinControlEffectsStub {
        runs: Mutex<Vec<(i64, i64, String)>>,
    }

    impl CheckinControlEffectsStub {
        fn runs(&self) -> Vec<(i64, i64, String)> {
            self.runs.lock().expect("checkin runs").clone()
        }
    }

    impl CheckinControlJobEffects for CheckinControlEffectsStub {
        type Error = io::Error;

        fn run_checkin_game<'a>(
            &'a self,
            params: &'a ControlJobParams,
        ) -> CheckinControlJobFuture<'a, Self::Error> {
            Box::pin(async move {
                self.runs.lock().expect("checkin runs").push((
                    params.chat_id,
                    params.user_id,
                    params.data.theme.clone(),
                ));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct GameStoreStub {
        state: Mutex<GameStoreState>,
    }

    #[derive(Default)]
    struct GameStoreState {
        settings: Option<openplotva_core::ChatSettings>,
        today_winner: Option<openplotva_storage::ChatGameResult>,
        table_participants: Vec<i64>,
        active_participants: Vec<i64>,
        members: Vec<openplotva_storage::ChatMemberRecord>,
        users: HashMap<i64, openplotva_core::UserState>,
        yearly_top: Vec<openplotva_storage::ChatGameTopRow>,
        recorded: Vec<(i64, i64, String)>,
        incremented: Vec<(i64, i64)>,
        discovered: Option<OffsetDateTime>,
    }

    impl GameStoreStub {
        fn lock(&self) -> MutexGuard<'_, GameStoreState> {
            self.state.lock().expect("game store")
        }

        fn set_settings(&self, settings: openplotva_core::ChatSettings) {
            self.lock().settings = Some(settings);
        }

        fn set_today_winner(&self, winner: openplotva_storage::ChatGameResult) {
            self.lock().today_winner = Some(winner);
        }

        fn set_table_participants(&self, participants: Vec<i64>) {
            self.lock().table_participants = participants;
        }

        fn set_members(&self, members: Vec<openplotva_storage::ChatMemberRecord>) {
            self.lock().members = members;
        }

        fn set_users(&self, users: Vec<openplotva_core::UserState>) {
            self.lock().users = users.into_iter().map(|user| (user.id, user)).collect();
        }

        fn set_yearly_top(&self, rows: Vec<openplotva_storage::ChatGameTopRow>) {
            self.lock().yearly_top = rows;
        }

        fn recorded(&self) -> Vec<(i64, i64, String)> {
            self.lock().recorded.clone()
        }

        fn incremented(&self) -> Vec<(i64, i64)> {
            self.lock().incremented.clone()
        }
    }

    impl CheckinGameStore for GameStoreStub {
        type Error = io::Error;

        fn get_checkin_chat_settings<'a>(
            &'a self,
            _chat_id: i64,
        ) -> CheckinGameStoreFuture<'a, Option<openplotva_core::ChatSettings>, Self::Error>
        {
            Box::pin(async move { Ok(self.lock().settings.clone()) })
        }

        fn get_today_chat_winner<'a>(
            &'a self,
            _chat_id: i64,
        ) -> CheckinGameStoreFuture<'a, Option<openplotva_storage::ChatGameResult>, Self::Error>
        {
            Box::pin(async move { Ok(self.lock().today_winner.clone()) })
        }

        fn record_chat_daily_winner<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
            theme: &'a str,
        ) -> CheckinGameStoreFuture<'a, openplotva_storage::ChatGameResult, Self::Error> {
            Box::pin(async move {
                self.lock()
                    .recorded
                    .push((chat_id, user_id, theme.to_owned()));
                game_result(user_id, theme)
            })
        }

        fn increment_chat_game_win<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> CheckinGameStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.lock().incremented.push((chat_id, user_id));
                Ok(())
            })
        }

        fn list_active_participants_from_table<'a>(
            &'a self,
            _chat_id: i64,
            _limit_count: i32,
        ) -> CheckinGameStoreFuture<'a, Vec<i64>, Self::Error> {
            Box::pin(async move { Ok(self.lock().table_participants.clone()) })
        }

        fn list_active_participants<'a>(
            &'a self,
            _chat_id: i64,
            _limit_count: i32,
        ) -> CheckinGameStoreFuture<'a, Vec<i64>, Self::Error> {
            Box::pin(async move { Ok(self.lock().active_participants.clone()) })
        }

        fn list_chat_members_by_user_ids<'a>(
            &'a self,
            _chat_id: i64,
            user_ids: &'a [i64],
        ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatMemberRecord>, Self::Error>
        {
            Box::pin(async move {
                let ids: HashSet<i64> = user_ids.iter().copied().collect();
                Ok(self
                    .lock()
                    .members
                    .iter()
                    .filter(|member| ids.contains(&member.user_id))
                    .cloned()
                    .collect())
            })
        }

        fn get_chat_member<'a>(
            &'a self,
            _chat_id: i64,
            user_id: i64,
        ) -> CheckinGameStoreFuture<'a, Option<openplotva_storage::ChatMemberRecord>, Self::Error>
        {
            Box::pin(async move {
                Ok(self
                    .lock()
                    .members
                    .iter()
                    .find(|member| member.user_id == user_id)
                    .cloned())
            })
        }

        fn list_chat_members<'a>(
            &'a self,
            _chat_id: i64,
        ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatMemberRecord>, Self::Error>
        {
            Box::pin(async move { Ok(self.lock().members.clone()) })
        }

        fn get_chat_discovered<'a>(
            &'a self,
            _chat_id: i64,
        ) -> CheckinGameStoreFuture<'a, Option<OffsetDateTime>, Self::Error> {
            Box::pin(async move { Ok(self.lock().discovered) })
        }

        fn get_user_state<'a>(
            &'a self,
            user_id: i64,
        ) -> CheckinGameStoreFuture<'a, Option<openplotva_core::UserState>, Self::Error> {
            Box::pin(async move { Ok(self.lock().users.get(&user_id).cloned()) })
        }

        fn get_yearly_top<'a>(
            &'a self,
            _chat_id: i64,
            _limit_count: i32,
        ) -> CheckinGameStoreFuture<'a, Vec<openplotva_storage::ChatGameTopRow>, Self::Error>
        {
            Box::pin(async move { Ok(self.lock().yearly_top.clone()) })
        }
    }

    #[derive(Default)]
    struct GameSenderStub {
        sent_html: Mutex<Vec<String>>,
        edits: Mutex<Vec<String>>,
        ephemeral: Mutex<Vec<String>>,
    }

    impl GameSenderStub {
        fn sent_html(&self) -> Vec<String> {
            self.sent_html.lock().expect("sent html").clone()
        }

        fn edits(&self) -> Vec<String> {
            self.edits.lock().expect("edits").clone()
        }

        fn ephemeral(&self) -> Vec<String> {
            self.ephemeral.lock().expect("ephemeral").clone()
        }
    }

    impl CheckinGameSender for GameSenderStub {
        type Error = io::Error;

        fn send_checkin_html<'a>(
            &'a self,
            _reply_to: CheckinGameMessage,
            html: String,
        ) -> CheckinGameSendFuture<'a, Option<i32>, Self::Error> {
            Box::pin(async move {
                self.sent_html.lock().expect("sent html").push(html);
                Ok(Some(9001))
            })
        }

        fn edit_checkin_html<'a>(
            &'a self,
            _reply_to: CheckinGameMessage,
            _message_id: i32,
            html: String,
        ) -> CheckinGameSendFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.edits.lock().expect("edits").push(html);
                Ok(())
            })
        }

        fn send_checkin_ephemeral<'a>(
            &'a self,
            _reply_to: CheckinGameMessage,
            text: String,
            _delete_after: Duration,
        ) -> CheckinGameSendFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.ephemeral.lock().expect("ephemeral").push(text);
                Ok(())
            })
        }
    }

    fn game_message() -> CheckinGameMessage {
        CheckinGameMessage {
            chat_id: -10042,
            message_id: 555,
            thread_id: Some(77),
        }
    }

    fn game_request(chat_type: &str, theme_override: &str) -> CheckinGameRunRequest {
        CheckinGameRunRequest {
            message: game_message(),
            chat_type: chat_type.to_owned(),
            theme_override: theme_override.to_owned(),
            bot_id: 777000,
            can_send_text: true,
            now: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn member(user_id: i64, status: &str) -> openplotva_storage::ChatMemberRecord {
        openplotva_storage::ChatMemberRecord {
            chat_id: -10042,
            user_id,
            status: status.to_owned(),
            ..openplotva_storage::ChatMemberRecord::default()
        }
    }

    fn user(
        id: i64,
        first_name: &str,
        last_name: Option<&str>,
        username: Option<&str>,
    ) -> openplotva_core::UserState {
        openplotva_core::UserState {
            id,
            first_name: first_name.to_owned(),
            last_name: last_name.map(str::to_owned),
            username: username.map(str::to_owned),
            language_code: None,
            is_premium: None,
        }
    }

    fn game_result(
        user_id: i64,
        theme: &str,
    ) -> Result<openplotva_storage::ChatGameResult, io::Error> {
        Ok(openplotva_storage::ChatGameResult {
            id: 1,
            chat_id: -10042,
            user_id,
            theme: theme.to_owned(),
            won_at: OffsetDateTime::UNIX_EPOCH,
            won_on_date: time::Date::from_calendar_date(2026, time::Month::May, 20)
                .map_err(io::Error::other)?,
        })
    }

    #[derive(Clone, Default)]
    struct MethodExecutorStub {
        methods: Arc<Mutex<Vec<openplotva_telegram::TelegramOutboundMethodKind>>>,
    }

    impl MethodExecutorStub {
        fn method_kinds(&self) -> Vec<openplotva_telegram::TelegramOutboundMethodKind> {
            self.methods.lock().expect("method kinds").clone()
        }
    }

    impl CheckinCallbackMethodExecutor for MethodExecutorStub {
        type Error = io::Error;

        fn execute_checkin_callback_method_direct<'a>(
            &'a self,
            method: openplotva_telegram::TelegramOutboundMethod,
        ) -> CheckinThemeEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.methods
                    .lock()
                    .expect("method kinds")
                    .push(method.kind());
                Ok(())
            })
        }
    }

    type InsertedVirtualMessages = Arc<Mutex<Vec<(String, i64, Option<i32>)>>>;

    #[derive(Clone, Default)]
    struct VirtualStoreStub {
        inserted: InsertedVirtualMessages,
    }

    impl VirtualStoreStub {
        fn inserted(&self) -> Vec<(String, i64, Option<i32>)> {
            self.inserted.lock().expect("inserted virtual ids").clone()
        }
    }

    impl VirtualMessageStore for VirtualStoreStub {
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
            self.inserted
                .lock()
                .expect("inserted virtual ids")
                .push((vmsg_id, chat_id, thread_id));
            Box::pin(async { Ok(()) })
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

    #[derive(Default)]
    struct NextStub {
        calls: Mutex<Vec<i64>>,
    }

    impl NextStub {
        fn calls(&self) -> Vec<i64> {
            self.calls.lock().expect("next calls").clone()
        }
    }

    impl UpdateHandler for NextStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls.lock().expect("next calls").push(update.id);
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CallbackAckEffectsStub {
        methods: Mutex<Vec<(TelegramOutboundMethodKind, serde_json::Value)>>,
    }

    impl CallbackAckEffectsStub {
        fn methods(&self) -> Vec<(TelegramOutboundMethodKind, serde_json::Value)> {
            self.methods.lock().expect("callback ack methods").clone()
        }
    }

    impl CallbackQueryEffects for CallbackAckEffectsStub {
        type Error = io::Error;

        fn answer_callback_query<'a>(
            &'a self,
            method: openplotva_telegram::TelegramOutboundMethod,
        ) -> CallbackQueryFuture<'a, Self::Error> {
            Box::pin(async move {
                let kind = method.kind();
                let payload = match method {
                    openplotva_telegram::TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                        serde_json::to_value(method.as_ref()).map_err(io::Error::other)?
                    }
                    _ => return Err(io::Error::other("unexpected callback ack method")),
                };
                self.methods
                    .lock()
                    .expect("callback ack methods")
                    .push((kind, payload));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct CallbackRateLimitStub {
        limited_chat_id: Option<i64>,
    }

    impl CallbackRateLimitPolicy for CallbackRateLimitStub {
        fn is_callback_chat_rate_limited<'a>(
            &'a self,
            chat_id: i64,
        ) -> CallbackRateLimitFuture<'a> {
            Box::pin(async move { self.limited_chat_id == Some(chat_id) })
        }
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
                    .expect("state calls")
                    .push(format!("chat:{}", chat.id));
                Ok(())
            })
        }

        fn upsert_user_state<'a>(
            &'a self,
            user: &'a openplotva_core::UserState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("state calls")
                    .push(format!("user:{}", user.id));
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
                    .expect("state calls")
                    .push(format!("file:{}", params.file_unique_id));
                Ok(())
            })
        }
    }

    fn method_names(methods: &[(String, serde_json::Value)]) -> Vec<&str> {
        methods.iter().map(|(name, _)| name.as_str()).collect()
    }
}
