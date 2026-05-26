//! App-level chat-member service-event behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    Chat as TelegramChat, ChatMember as TelegramChatMember,
    ChatMemberUpdated as TelegramChatMemberUpdated, Message as TelegramMessage,
    MessageData as TelegramMessageData, Update as TelegramUpdate, UpdateType, User as TelegramUser,
};
use openplotva_core::{ChatSettings, ChatSettingsUpdate, UserState};
use openplotva_storage::{ChatMemberRecord, ChatMemberUpsert};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY, JobType,
    StatelessJobItem, control_job_params_from_stateless_job, new_control_job_at,
};
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    permissions::{ChatPermissionContext, ChatPermissionStore},
    updates::{UpdateHandler, UpdateHandlerFuture},
};

/// Boxed future returned by member storage calls.
pub type MemberStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by chat-communication effects.
pub type ChatCommunicationFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by member-state control-job queues.
pub type MemberStateControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by member-state control-job worker queues.
pub type MemberStateControlJobWorkerFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by member-state sync side effects.
pub type MemberStateControlJobFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait LeftChatMemberStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Delete one stored chat member.
    fn delete_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStoreFuture<'a, (), Self::Error>;
}

pub trait ChatMemberStateStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load current member state before deciding whether the status changed.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStoreFuture<'a, Option<ChatMemberRecord>, Self::Error>;

    /// Delete departed member state.
    fn delete_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStoreFuture<'a, (), Self::Error>;

    /// Persist new member state.
    fn upsert_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> MemberStoreFuture<'a, (), Self::Error>;
}

impl LeftChatMemberStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn delete_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.delete_chat_member(chat_id, user_id).await })
    }
}

impl ChatMemberStateStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStoreFuture<'a, Option<ChatMemberRecord>, Self::Error> {
        Box::pin(async move { self.get_chat_member(chat_id, user_id).await })
    }

    fn delete_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.delete_chat_member(chat_id, user_id).await })
    }

    fn upsert_chat_member<'a>(
        &'a self,
        member: ChatMemberUpsert,
    ) -> MemberStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_chat_member(&member).await })
    }
}

pub trait MemberStateControlJobQueue {
    /// Queue error type.
    type Error: fmt::Display + Send + Sync + 'static;

    fn assign_member_state_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> MemberStateControlJobQueueFuture<'a, Self::Error>;
}

impl<T> MemberStateControlJobQueue for T
where
    T: crate::payments::PaymentControlJobQueue + Sync,
{
    type Error = T::Error;

    fn assign_member_state_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> MemberStateControlJobQueueFuture<'a, Self::Error> {
        self.assign_payment_control_job(queue_name, job)
    }
}

/// Queue/status boundary for member-state taskman control-job workers.
pub trait MemberStateControlJobWorkerQueue {
    /// Queue error type.
    type Error: fmt::Display + Send + Sync + 'static;

    fn dequeue_member_state_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> MemberStateControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    >;

    /// Mark one member-state control job completed.
    fn complete_member_state_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> MemberStateControlJobWorkerFuture<'a, (), Self::Error>;

    /// Mark one member-state control job failed.
    fn fail_member_state_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> MemberStateControlJobWorkerFuture<'a, (), Self::Error>;
}

impl<T> MemberStateControlJobWorkerQueue for T
where
    T: crate::payments::SharedControlJobWorkerQueue + Sync,
{
    type Error = T::Error;

    fn dequeue_member_state_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> MemberStateControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    > {
        self.dequeue_shared_control_job_matching(
            queue_name,
            "member-state-control",
            is_member_state_control_job,
        )
    }

    fn complete_member_state_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> MemberStateControlJobWorkerFuture<'a, (), Self::Error> {
        self.complete_shared_control_job(job_id)
    }

    fn fail_member_state_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> MemberStateControlJobWorkerFuture<'a, (), Self::Error> {
        self.fail_shared_control_job(job_id, error)
    }
}

pub trait MemberStateControlJobEffects {
    fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> MemberStateControlJobFuture<'a>;

    fn sync_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStateControlJobFuture<'a>;
}

pub trait ChatCommunicationEffects {
    /// Effect error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Disable text and draw replies for a chat.
    fn disable_chat_communication<'a>(
        &'a self,
        chat_id: i64,
    ) -> ChatCommunicationFuture<'a, (), Self::Error>;

    /// Enable text and draw replies for a chat.
    fn enable_chat_communication<'a>(
        &'a self,
        chat_id: i64,
    ) -> ChatCommunicationFuture<'a, (), Self::Error>;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemberStateSyncMemberReport {
    /// Telegram API returned a member.
    pub fetched: bool,
    /// User row persisted successfully.
    pub user_upserted: bool,
    /// Chat-member row persisted successfully.
    pub member_upserted: bool,
    /// Best-effort user upsert failure.
    pub user_upsert_error: Option<String>,
    /// Best-effort chat-member upsert failure.
    pub member_upsert_error: Option<String>,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum MemberStateSyncMemberError {
    /// Telegram `getChatMember` failed before storage writes.
    #[error("{0}")]
    Telegram(String),
}

/// Runtime member-state control-job effects backed by Telegram, Postgres, and optional Redis cache.
#[derive(Clone, Debug)]
pub struct MemberStateRuntimeEffects<Store, MemberApi, AdminApi, AdminCache> {
    store: Store,
    member_api: MemberApi,
    admin_api: AdminApi,
    admin_cache: AdminCache,
}

impl<Store, MemberApi, AdminApi, AdminCache>
    MemberStateRuntimeEffects<Store, MemberApi, AdminApi, AdminCache>
{
    #[must_use]
    pub fn new(
        store: Store,
        member_api: MemberApi,
        admin_api: AdminApi,
        admin_cache: AdminCache,
    ) -> Self {
        Self {
            store,
            member_api,
            admin_api,
            admin_cache,
        }
    }
}

impl<Store, MemberApi, AdminApi, AdminCache> MemberStateControlJobEffects
    for MemberStateRuntimeEffects<Store, MemberApi, AdminApi, AdminCache>
where
    Store: crate::settings::GroupSettingsAdminSyncStore + Send + Sync,
    MemberApi: crate::settings::GroupSettingsMemberApi + Send + Sync,
    AdminApi: crate::settings::GroupSettingsAdminsApi + Send + Sync,
    AdminCache: crate::settings::GroupSettingsAdminCache + Send + Sync,
{
    fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> MemberStateControlJobFuture<'a> {
        Box::pin(async move {
            if let Err(error) = crate::settings::sync_chat_admins_with_cache(
                &self.store,
                &self.admin_api,
                &self.admin_cache,
                chat_id,
            )
            .await
            {
                tracing::warn!(
                    %error,
                    chat_id,
                    "failed to sync chat administrators for member-state control job"
                );
            }
        })
    }

    fn sync_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> MemberStateControlJobFuture<'a> {
        Box::pin(async move {
            if let Err(error) =
                sync_chat_member_with_sources(&self.store, &self.member_api, chat_id, user_id).await
            {
                tracing::warn!(
                    %error,
                    chat_id,
                    user_id,
                    "failed to sync chat member for member-state control job"
                );
            }
        })
    }
}

pub async fn sync_chat_member_with_sources<Store, Api>(
    store: &Store,
    telegram: &Api,
    chat_id: i64,
    user_id: i64,
) -> Result<MemberStateSyncMemberReport, MemberStateSyncMemberError>
where
    Store: crate::settings::GroupSettingsAdminSyncStore + Sync,
    Api: crate::settings::GroupSettingsMemberApi + Sync,
{
    let member = telegram
        .get_chat_member(chat_id, user_id)
        .await
        .map_err(|error| MemberStateSyncMemberError::Telegram(error.to_string()))?;
    let mut report = MemberStateSyncMemberReport {
        fetched: true,
        ..MemberStateSyncMemberReport::default()
    };

    if let Err(error) = store
        .upsert_user_state(user_state_from_telegram_user(member.get_user()))
        .await
    {
        report.user_upsert_error = Some(error.to_string());
    } else {
        report.user_upserted = true;
    }

    let upsert = chat_member_state_upsert_from_telegram(chat_id, user_id, &member);
    if let Err(error) =
        crate::settings::GroupSettingsAdminSyncStore::upsert_chat_member(store, upsert).await
    {
        report.member_upsert_error = Some(error.to_string());
    } else {
        report.member_upserted = true;
    }

    Ok(report)
}

#[derive(Clone, Debug)]
pub struct ChatSettingsCommunicationEffects<Store> {
    store: Store,
}

impl<Store> ChatSettingsCommunicationEffects<Store> {
    /// Build communication effects from a chat-settings store.
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

impl<Store> ChatCommunicationEffects for ChatSettingsCommunicationEffects<Store>
where
    Store: ChatPermissionStore + Send + Sync,
{
    type Error = Store::Error;

    fn disable_chat_communication<'a>(
        &'a self,
        chat_id: i64,
    ) -> ChatCommunicationFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let context = self.store.load_context(chat_id).await?;
            self.store
                .save_settings(chat_communication_update(chat_id, context, false))
                .await
        })
    }

    fn enable_chat_communication<'a>(
        &'a self,
        chat_id: i64,
    ) -> ChatCommunicationFuture<'a, (), Self::Error> {
        Box::pin(async move {
            let context = self.store.load_context(chat_id).await?;
            self.store
                .save_settings(chat_communication_update(chat_id, context, true))
                .await
        })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LeftChatMemberOutcome {
    /// Chat where the member left.
    pub chat_id: i64,
    /// User that left.
    pub user_id: i64,
    /// Whether the leaving user was the current bot.
    pub bot_left: bool,
    /// Non-fatal delete error, if any.
    pub delete_error: Option<String>,
    /// Non-fatal communication-disable error, if any.
    pub disable_error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChatMemberStateOutcome {
    /// Chat whose member state changed.
    pub chat_id: i64,
    /// Member whose state changed.
    pub user_id: i64,
    /// New Telegram status string.
    pub status: String,
    /// Whether the state update targeted the current bot.
    pub own_member: bool,
    /// Whether a departed row was deleted instead of upserted.
    pub deleted: bool,
    /// Whether an active row was upserted.
    pub upserted: bool,
    /// Whether stored status differed or could not be loaded.
    pub changed: bool,
    /// Sync job kind assigned before storage mutation, if any.
    pub queued_job: Option<ControlKind>,
    /// Non-fatal queue error, if any.
    pub queue_error: Option<String>,
    /// Non-fatal member load error, if any.
    pub load_error: Option<String>,
    /// Non-fatal member delete error, if any.
    pub delete_error: Option<String>,
    /// Non-fatal member upsert error, if any.
    pub upsert_error: Option<String>,
    /// Non-fatal communication toggle error, if any.
    pub communication_error: Option<String>,
}

/// Member-state control-job execution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MemberStateControlJobExecution {
    ChatAdminsSync,
    ChatMemberSync,
}

/// Result of one member-state taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemberStateControlJobWorkerReport {
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Member-state executor result, when payload decoding succeeded.
    pub execution: Option<MemberStateControlJobExecution>,
    /// The job was finalized as completed.
    pub completed: bool,
    /// The job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Unsupported decoded control kind.
    pub execution_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

/// Route chosen by the left-chat-member update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeftChatMemberUpdateRoute {
    /// No left-member side effect ran and the update was delegated.
    Delegated,
    LeftMember(LeftChatMemberOutcome),
}

/// Route chosen by the chat-member-state update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChatMemberStateUpdateRoute {
    /// No member-state side effect ran and the update was delegated.
    Delegated,
    MemberState(ChatMemberStateOutcome),
}

/// Error returned when the delegated handler fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LeftChatMemberUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

/// Error returned when the delegated member-state handler fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ChatMemberStateUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct LeftChatMemberUpdateHandler<Store, Effects, Next> {
    store: Arc<Store>,
    effects: Arc<Effects>,
    bot_id: i64,
    next: Arc<Next>,
}

#[derive(Clone, Debug)]
pub struct ChatMemberStateUpdateHandler<Store, Queue, Effects, Next> {
    store: Arc<Store>,
    queue: Arc<Queue>,
    effects: Arc<Effects>,
    bot_id: i64,
    next: Arc<Next>,
}

impl<Store, Effects, Next> LeftChatMemberUpdateHandler<Store, Effects, Next> {
    /// Build a left-chat-member handler around the real downstream update handler.
    pub fn new(store: Arc<Store>, effects: Arc<Effects>, bot_id: i64, next: Arc<Next>) -> Self {
        Self {
            store,
            effects,
            bot_id,
            next,
        }
    }
}

impl<Store, Queue, Effects, Next> ChatMemberStateUpdateHandler<Store, Queue, Effects, Next> {
    /// Build a member-state handler around the real downstream update handler.
    pub fn new(
        store: Arc<Store>,
        queue: Arc<Queue>,
        effects: Arc<Effects>,
        bot_id: i64,
        next: Arc<Next>,
    ) -> Self {
        Self {
            store,
            queue,
            effects,
            bot_id,
            next,
        }
    }
}

impl<Store, Effects, Next> UpdateHandler for LeftChatMemberUpdateHandler<Store, Effects, Next>
where
    Store: LeftChatMemberStore + Send + Sync,
    Effects: ChatCommunicationEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = LeftChatMemberUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_left_chat_member_update_or_else(
                self.store.as_ref(),
                self.effects.as_ref(),
                self.bot_id,
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

impl<Store, Queue, Effects, Next> UpdateHandler
    for ChatMemberStateUpdateHandler<Store, Queue, Effects, Next>
where
    Store: ChatMemberStateStore + Send + Sync,
    Queue: MemberStateControlJobQueue + Send + Sync,
    Effects: ChatCommunicationEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = ChatMemberStateUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_chat_member_state_update_or_else_at(
                self.store.as_ref(),
                self.queue.as_ref(),
                self.effects.as_ref(),
                self.bot_id,
                update,
                OffsetDateTime::now_utc(),
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_left_chat_member_update_or_else<
    Store,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    store: &Store,
    effects: &Effects,
    bot_id: i64,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<LeftChatMemberUpdateRoute, LeftChatMemberUpdateError>
where
    Store: LeftChatMemberStore + Sync,
    Effects: ChatCommunicationEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let outcome = left_chat_member_outcome(store, effects, bot_id, &update).await;
    handle_other(update)
        .await
        .map_err(|error| LeftChatMemberUpdateError::Downstream {
            message: error.to_string(),
        })?;

    Ok(match outcome {
        Some(outcome) => LeftChatMemberUpdateRoute::LeftMember(outcome),
        None => LeftChatMemberUpdateRoute::Delegated,
    })
}

pub async fn handle_chat_member_state_update_or_else_at<
    Store,
    Queue,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    store: &Store,
    queue: &Queue,
    effects: &Effects,
    bot_id: i64,
    update: TelegramUpdate,
    created: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<ChatMemberStateUpdateRoute, ChatMemberStateUpdateError>
where
    Store: ChatMemberStateStore + Sync,
    Queue: MemberStateControlJobQueue + Sync,
    Effects: ChatCommunicationEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let outcome = chat_member_state_outcome(store, queue, effects, bot_id, &update, created).await;
    handle_other(update)
        .await
        .map_err(|error| ChatMemberStateUpdateError::Downstream {
            message: error.to_string(),
        })?;

    Ok(match outcome {
        Some(outcome) => ChatMemberStateUpdateRoute::MemberState(outcome),
        None => ChatMemberStateUpdateRoute::Delegated,
    })
}

async fn chat_member_state_outcome<Store, Queue, Effects>(
    store: &Store,
    queue: &Queue,
    effects: &Effects,
    bot_id: i64,
    update: &TelegramUpdate,
    created: OffsetDateTime,
) -> Option<ChatMemberStateOutcome>
where
    Store: ChatMemberStateStore + Sync,
    Queue: MemberStateControlJobQueue + Sync,
    Effects: ChatCommunicationEffects + Sync,
{
    let member_update = chat_member_updated(update)?;
    let chat_id = member_update.chat.get_id().into();
    let member = &member_update.new_chat_member;
    let user_id = member.get_user().id.into();
    if chat_id == 0 || user_id == 0 {
        return None;
    }

    let status = telegram_chat_member_status(member).to_owned();
    let own_member = bot_id != 0 && user_id == bot_id;
    let mut outcome = ChatMemberStateOutcome {
        chat_id,
        user_id,
        status: status.clone(),
        own_member,
        ..ChatMemberStateOutcome::default()
    };

    if let Some(job) = member_state_sync_job_from_update(member_update, bot_id, created)
        && let Some(kind) = job.data.control_data.as_ref().map(|data| data.kind)
    {
        outcome.queued_job = Some(kind);
        if let Err(error) = queue
            .assign_member_state_control_job(CONTROL_QUEUE_NAME, job)
            .await
        {
            let message = error.to_string();
            tracing::warn!(
                message,
                chat_id,
                user_id,
                "failed to assign member-state sync job"
            );
            outcome.queue_error = Some(message);
        }
    }

    if status == openplotva_storage::CHAT_MEMBER_STATUS_LEFT
        || status == openplotva_storage::CHAT_MEMBER_STATUS_KICKED
    {
        outcome.deleted = true;
        if let Err(error) = ChatMemberStateStore::delete_chat_member(store, chat_id, user_id).await
        {
            let message = error.to_string();
            tracing::debug!(
                message,
                chat_id,
                user_id,
                "failed to delete chat member on leave/kick"
            );
            outcome.delete_error = Some(message);
        }
        if own_member && let Err(error) = effects.disable_chat_communication(chat_id).await {
            let message = error.to_string();
            tracing::warn!(
                message,
                chat_id,
                user_id,
                "failed to disable chat communication for departed bot"
            );
            outcome.communication_error = Some(message);
        }
        return Some(outcome);
    }

    match store.get_chat_member(chat_id, user_id).await {
        Ok(Some(current)) => {
            outcome.changed = current.status != status;
        }
        Ok(None) => {
            outcome.changed = true;
        }
        Err(error) => {
            let message = error.to_string();
            tracing::debug!(
                message,
                chat_id,
                user_id,
                "failed to load current chat member"
            );
            outcome.changed = true;
            outcome.load_error = Some(message);
        }
    }

    if let Err(error) = store
        .upsert_chat_member(chat_member_state_upsert_from_telegram(
            chat_id, user_id, member,
        ))
        .await
    {
        let message = error.to_string();
        tracing::warn!(
            message,
            chat_id,
            user_id,
            "failed to upsert chat member state"
        );
        outcome.upsert_error = Some(message);
    } else {
        outcome.upserted = true;
    }

    if own_member
        && (status == openplotva_storage::CHAT_MEMBER_STATUS_MEMBER
            || status == openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR)
        && let Err(error) = effects.enable_chat_communication(chat_id).await
    {
        let message = error.to_string();
        tracing::warn!(
            message,
            chat_id,
            user_id,
            "failed to enable chat communication for active bot"
        );
        outcome.communication_error = Some(message);
    }

    Some(outcome)
}

/// Process one member-state control job, if available.
pub async fn process_member_state_control_job_once_at<Queue, Effects>(
    queue: &Queue,
    effects: &Effects,
) -> MemberStateControlJobWorkerReport
where
    Queue: MemberStateControlJobWorkerQueue + Sync,
    Effects: MemberStateControlJobEffects + Sync,
{
    let mut report = MemberStateControlJobWorkerReport::default();
    let item = match queue
        .dequeue_member_state_control_job(CONTROL_QUEUE_NAME)
        .await
    {
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
            mark_member_state_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    report.execution = match params.data.kind {
        ControlKind::ChatAdminsSync => {
            effects.sync_chat_admins(params.chat_id).await;
            Some(MemberStateControlJobExecution::ChatAdminsSync)
        }
        ControlKind::ChatMemberSync => {
            effects
                .sync_chat_member(params.chat_id, params.user_id)
                .await;
            Some(MemberStateControlJobExecution::ChatMemberSync)
        }
        kind => {
            let error = format!("unsupported member-state control job kind: {kind:?}");
            report.execution_error = Some(error.clone());
            mark_member_state_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    match queue.complete_member_state_control_job(item.id).await {
        Ok(()) => report.completed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
    report
}

async fn mark_member_state_control_job_failed<Queue>(
    queue: &Queue,
    job_id: i64,
    error: &str,
    report: &mut MemberStateControlJobWorkerReport,
) where
    Queue: MemberStateControlJobWorkerQueue + Sync,
{
    match queue.fail_member_state_control_job(job_id, error).await {
        Ok(()) => report.failed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
}

fn is_member_state_control_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == JobType::Control
        && matches!(
            job.data.control_data.as_ref().map(|data| data.kind),
            Some(ControlKind::ChatAdminsSync | ControlKind::ChatMemberSync)
        )
}

fn chat_member_updated(update: &TelegramUpdate) -> Option<&TelegramChatMemberUpdated> {
    match &update.update_type {
        UpdateType::BotStatus(update) | UpdateType::UserStatus(update) => Some(update.as_ref()),
        _ => None,
    }
}

fn member_state_sync_job_from_update(
    update: &TelegramChatMemberUpdated,
    bot_id: i64,
    created: OffsetDateTime,
) -> Option<StatelessJobItem> {
    let member = &update.new_chat_member;
    let user_id = i64::from(member.get_user().id);
    let status = telegram_chat_member_status(member);
    let (kind, title, job_user_id, user_full_name, data) = if bot_id != 0 && user_id == bot_id {
        if status != openplotva_storage::CHAT_MEMBER_STATUS_ADMINISTRATOR
            && status != openplotva_storage::CHAT_MEMBER_STATUS_MEMBER
        {
            return None;
        }
        (
            ControlKind::ChatAdminsSync,
            "chat admins sync",
            0,
            String::new(),
            ControlJobData {
                kind: ControlKind::ChatAdminsSync,
                chat_type: chat_type_name(&update.chat).to_owned(),
                ..ControlJobData::default()
            },
        )
    } else if update.from.id != 0 {
        (
            ControlKind::ChatMemberSync,
            "chat member sync",
            i64::from(update.from.id),
            user_full_name(&update.from),
            ControlJobData {
                kind: ControlKind::ChatMemberSync,
                chat_type: chat_type_name(&update.chat).to_owned(),
                user_name: update
                    .from
                    .username
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                first_name: update.from.first_name.clone(),
                last_name: update.from.last_name.clone().unwrap_or_default(),
                language_code: update.from.language_code.clone().unwrap_or_default(),
                is_premium: update.from.is_premium.unwrap_or_default(),
                ..ControlJobData::default()
            },
        )
    } else {
        return None;
    };

    let params = ControlJobParams {
        chat_id: update.chat.get_id().into(),
        message_id: 0,
        user_id: job_user_id,
        user_full_name,
        thread_id: None,
        data,
    };
    Some(
        new_control_job_at(params, created)
            .with_name(title)
            .with_priority(HIGH_PRIORITY),
    )
    .filter(|job| {
        job.data
            .control_data
            .as_ref()
            .is_some_and(|data| data.kind == kind)
    })
}

#[must_use]
pub fn chat_member_state_upsert_from_telegram(
    chat_id: i64,
    user_id: i64,
    member: &TelegramChatMember,
) -> ChatMemberUpsert {
    let mut params = ChatMemberUpsert {
        chat_id,
        user_id,
        status: telegram_chat_member_status(member).to_owned(),
        is_anonymous: Some(telegram_chat_member_is_anonymous(member)),
        can_be_edited: Some(false),
        ..ChatMemberUpsert::default()
    };
    apply_chat_member_state_role_permissions(&mut params, member);
    if let TelegramChatMember::Administrator(admin) = member
        && let Some(custom_title) = admin
            .custom_title
            .as_ref()
            .filter(|title| !title.is_empty())
    {
        params.custom_title = Some(custom_title.clone());
    }
    params
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

fn apply_chat_member_state_role_permissions(
    params: &mut ChatMemberUpsert,
    member: &TelegramChatMember,
) {
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

fn user_state_from_telegram_user(user: &TelegramUser) -> UserState {
    UserState::new(
        user.id.into(),
        user.first_name.clone(),
        user.last_name.clone(),
        user.username.as_ref().map(ToString::to_string),
        user.language_code.clone(),
        user.is_premium,
    )
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

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Private(_) => "private",
        TelegramChat::Group(_) => "group",
        TelegramChat::Supergroup(_) => "supergroup",
        TelegramChat::Channel(_) => "channel",
    }
}

fn user_full_name(user: &carapax::types::User) -> String {
    match user.last_name.as_deref().filter(|last| !last.is_empty()) {
        Some(last_name) => format!("{} {last_name}", user.first_name),
        None => user.first_name.clone(),
    }
}

async fn left_chat_member_outcome<Store, Effects>(
    store: &Store,
    effects: &Effects,
    bot_id: i64,
    update: &TelegramUpdate,
) -> Option<LeftChatMemberOutcome>
where
    Store: LeftChatMemberStore + Sync,
    Effects: ChatCommunicationEffects + Sync,
{
    let UpdateType::Message(message) = &update.update_type else {
        return None;
    };
    let (chat_id, user_id) = left_chat_member_ids(message)?;
    let bot_left = bot_id != 0 && user_id == bot_id;
    let mut outcome = LeftChatMemberOutcome {
        chat_id,
        user_id,
        bot_left,
        delete_error: None,
        disable_error: None,
    };

    if bot_left && let Err(error) = effects.disable_chat_communication(chat_id).await {
        let message = error.to_string();
        tracing::warn!(
            message,
            chat_id,
            user_id,
            "failed to disable chat communication after bot left"
        );
        outcome.disable_error = Some(message);
    }

    if let Err(error) = store.delete_chat_member(chat_id, user_id).await {
        let message = error.to_string();
        tracing::warn!(
            message,
            chat_id,
            user_id,
            "failed to delete left chat member"
        );
        outcome.delete_error = Some(message);
    }
    Some(outcome)
}

fn left_chat_member_ids(message: &TelegramMessage) -> Option<(i64, i64)> {
    let TelegramMessageData::LeftChatMember(user) = &message.data else {
        return None;
    };
    let chat_id = message.chat.get_id().into();
    let user_id = user.id.into();
    if chat_id == 0 || user_id == 0 {
        return None;
    }
    Some((chat_id, user_id))
}

fn chat_communication_update(
    chat_id: i64,
    context: ChatPermissionContext,
    enabled: bool,
) -> ChatSettingsUpdate {
    let settings = context
        .settings
        .unwrap_or_else(|| ChatSettings::defaults(chat_id));
    let chat_type = context
        .chat_type
        .as_deref()
        .map(str::trim)
        .filter(|chat_type| !chat_type.is_empty())
        .unwrap_or("private")
        .to_owned();
    let enable_daily_game = settings.enable_daily_game.unwrap_or(true);
    let daily_game_theme = settings
        .daily_game_theme
        .as_deref()
        .filter(|theme| !theme.is_empty())
        .unwrap_or("auto")
        .to_owned();

    ChatSettingsUpdate {
        chat_id,
        chat_type,
        mood_alignment: settings.mood_alignment,
        custom_persona: settings.custom_persona,
        reactivity_percentage: settings.reactivity_percentage,
        proactivity_percentage: settings.proactivity_percentage,
        enable_global_text_reply: enabled,
        enable_global_draw_reply: enabled,
        enable_obscenifier: settings.enable_obscenifier,
        enable_profanity: settings.enable_profanity,
        enable_greet_joiners: settings.enable_greet_joiners,
        enable_daily_game,
        daily_game_theme,
        greeting_html: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        error::Error,
        fmt, io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::{
        ChatMember as TelegramChatMember, Update as TelegramUpdate, User as TelegramUser,
    };
    use openplotva_core::{ChatSettings, ChatSettingsUpdate, UserSettings, UserState};
    use openplotva_storage::{ChatMemberRecord, ChatMemberUpsert};
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind,
        DIALOG_AIFARM_QUEUE_NAME, StatelessJobItem, new_control_job_at,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use crate::payments::{InMemoryPaymentControlJobQueue, InMemoryPaymentControlJobStatus};
    use crate::{
        dialog_messages::{
            DialogMessageFuture, DialogMessageSchedule, DialogMessageScheduleReport,
            DialogMessageScheduler, DialogMessageUpdateConfig, DialogMessageUpdateHandler,
            DirectSongNoticeEffects, DirectSongNoticeFuture, DirectSongNoticePlan,
            RandomDialogEffects, RandomDialogRng, RandomDialogSettingsStore,
            RandomObscenifiedTextPlan,
        },
        permissions::{ChatPermissionContext, ChatPermissionStore, ChatPermissionStoreFuture},
        updates::{
            TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
            UpdateStateStoreFuture, process_update_with_state_store_at,
        },
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};

    use super::{
        ChatCommunicationEffects, ChatCommunicationFuture, ChatMemberStateStore,
        ChatMemberStateUpdateHandler, ChatMemberStateUpdateRoute, ChatSettingsCommunicationEffects,
        LeftChatMemberStore, LeftChatMemberUpdateHandler, LeftChatMemberUpdateRoute,
        MemberStateControlJobEffects, MemberStateControlJobQueue, MemberStateControlJobQueueFuture,
        MemberStoreFuture, handle_chat_member_state_update_or_else_at,
        handle_left_chat_member_update_or_else, process_member_state_control_job_once_at,
        sync_chat_member_with_sources,
    };

    #[tokio::test]
    async fn member_state_control_jobs_can_use_shared_persistent_control_queue()
    -> Result<(), Box<dyn Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let job = new_control_job_at(
            ControlJobParams {
                chat_id: -10042,
                message_id: 77,
                user_id: 42,
                user_full_name: "Owner".to_owned(),
                thread_id: None,
                data: ControlJobData {
                    kind: ControlKind::ChatMemberSync,
                    ..ControlJobData::default()
                },
            },
            OffsetDateTime::UNIX_EPOCH,
        );

        queue
            .assign_member_state_control_job(CONTROL_QUEUE_NAME, job.clone())
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
            Some(ControlKind::ChatMemberSync)
        );
        Ok(())
    }

    #[tokio::test]
    async fn member_state_control_worker_skips_other_jobs_and_syncs_admins_then_member()
    -> Result<(), Box<dyn Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let effects = MemberStateSyncEffectsStub::default();
        let translate_job = member_state_test_control_job(ControlKind::Translate, "translate");
        let admins_job =
            member_state_test_control_job(ControlKind::ChatAdminsSync, "chat admins sync");
        let member_job =
            member_state_test_control_job(ControlKind::ChatMemberSync, "chat member sync");
        queue
            .assign_member_state_control_job(CONTROL_QUEUE_NAME, translate_job)
            .await?;
        queue
            .assign_member_state_control_job(CONTROL_QUEUE_NAME, admins_job.clone())
            .await?;
        queue
            .assign_member_state_control_job(CONTROL_QUEUE_NAME, member_job.clone())
            .await?;

        let admins_report = process_member_state_control_job_once_at(&queue, &effects).await;
        let member_report = process_member_state_control_job_once_at(&queue, &effects).await;

        assert_eq!(admins_report.job_id, Some(2));
        assert!(admins_report.completed);
        assert_eq!(member_report.job_id, Some(3));
        assert!(member_report.completed);
        assert_eq!(effects.admin_syncs(), vec![-10042]);
        assert_eq!(effects.member_syncs(), vec![(-10042, 42)]);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot[0].status, InMemoryPaymentControlJobStatus::Pending);
        assert_eq!(
            snapshot[1].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(
            snapshot[2].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(snapshot[1].job, admins_job);
        assert_eq!(snapshot[2].job, member_job);
        Ok(())
    }

    #[tokio::test]
    async fn sync_chat_member_with_sources_fetches_user_and_member_like_go()
    -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let api = MemberApiStub::with_member(telegram_admin_member(7)?);

        let report = sync_chat_member_with_sources(&store, &api, -10042, 7).await?;

        assert_eq!(
            report,
            super::MemberStateSyncMemberReport {
                fetched: true,
                user_upserted: true,
                member_upserted: true,
                user_upsert_error: None,
                member_upsert_error: None,
            }
        );
        assert_eq!(api.calls(), vec![(-10042, 7)]);
        assert_eq!(
            store.users(),
            vec![UserState::new(
                7,
                "Tracked".to_owned(),
                Some("Member".to_owned()),
                Some("tracked".to_owned()),
                Some("ru".to_owned()),
                Some(true),
            )]
        );
        assert_eq!(store.upserted(), vec![admin_member_state_upsert(-10042, 7)]);
        Ok(())
    }

    #[tokio::test]
    async fn member_state_runtime_effects_runs_admin_sync_through_existing_port()
    -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let member_api = MemberApiStub::default();
        let admin_api = AdminApiStub::with_admins(vec![telegram_admin_member(11)?]);
        let cache = AdminCacheStub::default();
        let effects = super::MemberStateRuntimeEffects::new(
            store.clone(),
            member_api,
            admin_api.clone(),
            cache.clone(),
        );

        effects.sync_chat_admins(-10042).await;

        assert_eq!(admin_api.calls(), vec![-10042]);
        assert_eq!(cache.saved(), vec![(-10042, vec![11])]);
        assert_eq!(
            store.users(),
            vec![UserState::new(
                11,
                "Tracked".to_owned(),
                Some("Member".to_owned()),
                Some("tracked".to_owned()),
                Some("ru".to_owned()),
                Some(true),
            )]
        );
        assert_eq!(store.upserted(), vec![admin_member_sync_upsert(-10042, 11)]);
        Ok(())
    }

    #[tokio::test]
    async fn left_chat_member_update_deletes_member_then_delegates() -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let effects = CommunicationEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_left_chat_member_update_or_else(
            &store,
            &effects,
            999,
            left_member_update(7)?,
            |update| next.handle_update(update),
        )
        .await?;

        let LeftChatMemberUpdateRoute::LeftMember(outcome) = route else {
            return Err(format!("expected left-member route, got {route:?}").into());
        };
        assert_eq!(outcome.chat_id, -10042);
        assert_eq!(outcome.user_id, 7);
        assert!(!outcome.bot_left);
        assert_eq!(store.deleted(), vec![(-10042, 7)]);
        assert!(effects.disabled().is_empty());
        assert_eq!(next.calls(), vec![12351]);
        Ok(())
    }

    #[tokio::test]
    async fn left_chat_member_update_disables_chat_when_bot_left() -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let effects = CommunicationEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_left_chat_member_update_or_else(
            &store,
            &effects,
            999,
            left_member_update(999)?,
            |update| next.handle_update(update),
        )
        .await?;

        let LeftChatMemberUpdateRoute::LeftMember(outcome) = route else {
            return Err(format!("expected left-member route, got {route:?}").into());
        };
        assert!(outcome.bot_left);
        assert_eq!(effects.disabled(), vec![-10042]);
        assert_eq!(store.deleted(), vec![(-10042, 999)]);
        assert_eq!(next.calls(), vec![12351]);
        Ok(())
    }

    #[tokio::test]
    async fn left_chat_member_update_delegates_non_left_messages_once() -> Result<(), Box<dyn Error>>
    {
        let store = MemberStoreStub::default();
        let effects = CommunicationEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_left_chat_member_update_or_else(
            &store,
            &effects,
            999,
            text_update()?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(route, LeftChatMemberUpdateRoute::Delegated);
        assert!(store.deleted().is_empty());
        assert_eq!(next.calls(), vec![777]);
        Ok(())
    }

    #[tokio::test]
    async fn left_chat_member_handler_intercepts_before_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let store = Arc::new(MemberStoreStub::default());
        let effects = Arc::new(CommunicationEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler =
            LeftChatMemberUpdateHandler::new(Arc::clone(&store), Arc::clone(&effects), 999, next);

        handler
            .handle_update(left_member_update(999)?)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;

        assert_eq!(effects.disabled(), vec![-10042]);
        assert_eq!(store.deleted(), vec![(-10042, 999)]);
        Ok(())
    }

    #[tokio::test]
    async fn chat_member_state_update_upserts_member_and_queues_actor_sync()
    -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let queue = MemberStateControlJobQueueStub::default();
        let effects = CommunicationEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_chat_member_state_update_or_else_at(
            &store,
            &queue,
            &effects,
            999,
            chat_member_update("chat_member", 7, "member")?,
            fixed_time()?,
            |update| next.handle_update(update),
        )
        .await?;

        let ChatMemberStateUpdateRoute::MemberState(outcome) = route else {
            return Err(format!("expected member-state route, got {route:?}").into());
        };
        assert_eq!(outcome.chat_id, -10042);
        assert_eq!(outcome.user_id, 7);
        assert_eq!(outcome.status, "member");
        assert!(outcome.changed);
        assert!(outcome.upserted);
        assert_eq!(outcome.queued_job, Some(ControlKind::ChatMemberSync));
        assert_eq!(store.upserted(), vec![member_upsert(-10042, 7, "member")]);
        assert!(store.deleted().is_empty());
        assert!(effects.enabled().is_empty());
        assert!(effects.disabled().is_empty());
        assert_eq!(next.calls(), vec![2468]);

        let jobs = queue.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].title, "chat member sync");
        let telegram = jobs[0].data.telegram_data.as_ref().expect("telegram data");
        assert_eq!(telegram.chat_id, -10042);
        assert_eq!(telegram.message_id, 0);
        assert_eq!(telegram.user_id, 42);
        assert_eq!(telegram.user_full_name, "Admin Actor");
        let control = jobs[0].data.control_data.as_ref().expect("control data");
        assert_eq!(control.kind, ControlKind::ChatMemberSync);
        assert_eq!(control.chat_type, "supergroup");
        assert_eq!(control.user_name, "actor");
        Ok(())
    }

    #[tokio::test]
    async fn own_member_state_enables_chat_and_queues_admin_sync() -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let queue = MemberStateControlJobQueueStub::default();
        let effects = CommunicationEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_chat_member_state_update_or_else_at(
            &store,
            &queue,
            &effects,
            999,
            chat_member_update("my_chat_member", 999, "administrator")?,
            fixed_time()?,
            |update| next.handle_update(update),
        )
        .await?;

        let ChatMemberStateUpdateRoute::MemberState(outcome) = route else {
            return Err(format!("expected member-state route, got {route:?}").into());
        };
        assert_eq!(outcome.user_id, 999);
        assert_eq!(outcome.status, "administrator");
        assert!(outcome.own_member);
        assert_eq!(outcome.queued_job, Some(ControlKind::ChatAdminsSync));
        assert_eq!(
            store.upserted(),
            vec![admin_member_state_upsert(-10042, 999)]
        );
        assert_eq!(effects.enabled(), vec![-10042]);
        assert!(effects.disabled().is_empty());

        let jobs = queue.jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].title, "chat admins sync");
        let telegram = jobs[0].data.telegram_data.as_ref().expect("telegram data");
        assert_eq!(telegram.user_id, 0);
        assert!(telegram.user_full_name.is_empty());
        assert_eq!(
            jobs[0].data.control_data.as_ref().map(|data| data.kind),
            Some(ControlKind::ChatAdminsSync)
        );
        Ok(())
    }

    #[tokio::test]
    async fn departed_own_member_state_deletes_and_disables_chat() -> Result<(), Box<dyn Error>> {
        let store = MemberStoreStub::default();
        let queue = MemberStateControlJobQueueStub::default();
        let effects = CommunicationEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_chat_member_state_update_or_else_at(
            &store,
            &queue,
            &effects,
            999,
            chat_member_update("my_chat_member", 999, "left")?,
            fixed_time()?,
            |update| next.handle_update(update),
        )
        .await?;

        let ChatMemberStateUpdateRoute::MemberState(outcome) = route else {
            return Err(format!("expected member-state route, got {route:?}").into());
        };
        assert!(outcome.deleted);
        assert!(!outcome.upserted);
        assert_eq!(outcome.queued_job, None);
        assert_eq!(store.deleted(), vec![(-10042, 999)]);
        assert!(store.upserted().is_empty());
        assert_eq!(effects.disabled(), vec![-10042]);
        assert!(effects.enabled().is_empty());
        assert_eq!(queue.jobs(), Vec::<StatelessJobItem>::new());
        assert_eq!(next.calls(), vec![2468]);
        Ok(())
    }

    #[tokio::test]
    async fn chat_member_state_handler_intercepts_before_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let store = Arc::new(MemberStoreStub::default());
        let queue = Arc::new(MemberStateControlJobQueueStub::default());
        let effects = Arc::new(CommunicationEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = ChatMemberStateUpdateHandler::new(
            Arc::clone(&store),
            Arc::clone(&queue),
            Arc::clone(&effects),
            999,
            next,
        );

        handler
            .handle_update(chat_member_update("my_chat_member", 999, "left")?)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;

        assert_eq!(effects.disabled(), vec![-10042]);
        assert_eq!(store.deleted(), vec![(-10042, 999)]);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_member_state_updates_side_effect_then_skip_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-member-state:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&chat_member_update("chat_member", 7, "member")?)
            .await?;
        update_queue
            .enqueue_update(&chat_member_update("my_chat_member", 999, "administrator")?)
            .await?;
        update_queue
            .enqueue_update(&left_member_update(999)?)
            .await?;
        assert_eq!(update_queue.len().await?, 3);

        let store = Arc::new(MemberStoreStub::default());
        let queue = Arc::new(MemberStateControlJobQueueStub::default());
        let effects = Arc::new(CommunicationEffectsStub::default());
        let terminal = Arc::new(UpdateHandlerStub::default());
        let dialog_terminal = Arc::new(DialogMessageUpdateHandler::new(
            Arc::new(MemberDialogSchedulerStub),
            Arc::new(MemberDialogSettingsStub),
            Arc::new(MemberDialogEffectsStub),
            Arc::new(MemberDialogRngStub),
            member_dialog_config(),
            Arc::clone(&terminal),
        ));
        let skipped = Arc::new(crate::skipped::SkippedUpdateHandler::new(Arc::clone(
            &dialog_terminal,
        )));
        let left_member = Arc::new(LeftChatMemberUpdateHandler::new(
            Arc::clone(&store),
            Arc::clone(&effects),
            999,
            skipped,
        ));
        let member_state = ChatMemberStateUpdateHandler::new(
            Arc::clone(&store),
            Arc::clone(&queue),
            Arc::clone(&effects),
            999,
            left_member,
        );
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let now = UNIX_EPOCH + Duration::from_secs(1_710_000_010);

        for expected_name in ["chat_member", "my_chat_member", "message"] {
            let update = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other(format!("expected {expected_name} update")))?;
            let report =
                process_update_with_state_store_at(update, config, now, &state_store, |update| {
                    member_state.handle_update(update)
                })
                .await;
            assert_eq!(report.update_name, expected_name);
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
        }

        assert_eq!(
            store.upserted(),
            vec![
                member_upsert(-10042, 7, "member"),
                admin_member_state_upsert(-10042, 999),
            ]
        );
        assert_eq!(queue.jobs().len(), 2);
        assert_eq!(
            queue
                .jobs()
                .iter()
                .filter_map(|job| job.data.control_data.as_ref().map(|data| data.kind))
                .collect::<Vec<_>>(),
            vec![ControlKind::ChatMemberSync, ControlKind::ChatAdminsSync]
        );
        assert_eq!(effects.enabled(), vec![-10042]);
        assert_eq!(effects.disabled(), vec![-10042]);
        assert_eq!(store.deleted(), vec![(-10042, 999)]);
        assert_eq!(terminal.calls(), Vec::<i64>::new());
        let state_calls = state_store.calls();
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.as_str() == "chat:-10042:supergroup")
                .count(),
            3
        );
        assert_eq!(
            state_calls
                .iter()
                .filter(|call| call.as_str() == "user:42:Admin")
                .count(),
            2
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn communication_effects_preserve_go_settings_shape() -> Result<(), Box<dyn Error>> {
        let store = ChatPermissionStoreStub::with_context(ChatPermissionContext {
            chat_type: Some("supergroup".to_owned()),
            settings: Some(ChatSettings {
                chat_id: -10042,
                mood_alignment: Some("chaotic".to_owned()),
                custom_persona: Some("sardonic".to_owned()),
                reactivity_percentage: 17,
                proactivity_percentage: 5,
                enable_global_text_reply: true,
                enable_global_draw_reply: true,
                enable_obscenifier: false,
                enable_profanity: true,
                enable_greet_joiners: true,
                enable_daily_game: None,
                daily_game_theme: Some(String::new()),
                greeting_html: Some("<b>hi</b>".to_owned()),
            }),
        });
        let effects = ChatSettingsCommunicationEffects::new(store.clone());

        effects.disable_chat_communication(-10042).await?;

        assert_eq!(
            store.saved(),
            vec![ChatSettingsUpdate {
                chat_id: -10042,
                chat_type: "supergroup".to_owned(),
                mood_alignment: Some("chaotic".to_owned()),
                custom_persona: Some("sardonic".to_owned()),
                reactivity_percentage: 17,
                proactivity_percentage: 5,
                enable_global_text_reply: false,
                enable_global_draw_reply: false,
                enable_obscenifier: false,
                enable_profanity: true,
                enable_greet_joiners: true,
                enable_daily_game: true,
                daily_game_theme: "auto".to_owned(),
                greeting_html: None,
            }]
        );
        Ok(())
    }

    fn left_member_update(user_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12351,
            "message": {
                "message_id": 89,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "left_chat_member": {
                    "id": user_id,
                    "is_bot": user_id == 999,
                    "first_name": "Departed"
                }
            }
        }))
    }

    fn text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 777,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private"
                },
                "text": "hello"
            }
        }))
    }

    fn chat_member_update(
        update_field: &str,
        user_id: i64,
        status: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut update = json!({
            "update_id": 2468,
        });
        update[update_field] = json!({
            "chat": {
                "id": -10042,
                "type": "supergroup",
                "title": "Plotva Lab"
            },
            "from": {
                "id": 42,
                "is_bot": false,
                "first_name": "Admin",
                "last_name": "Actor",
                "username": "actor",
                "language_code": "ru",
                "is_premium": true
            },
            "date": 1_710_000_000,
            "old_chat_member": {
                "status": "left",
                "user": {
                    "id": user_id,
                    "is_bot": user_id == 999,
                    "first_name": "Tracked"
                }
            },
            "new_chat_member": chat_member_json(user_id, status)
        });
        serde_json::from_value(update)
    }

    fn chat_member_json(user_id: i64, status: &str) -> serde_json::Value {
        let user = json!({
            "id": user_id,
            "is_bot": user_id == 999,
            "first_name": "Tracked",
        });
        match status {
            "administrator" => json!({
                "status": "administrator",
                "user": user,
                "can_be_edited": false,
                "is_anonymous": false,
                "can_manage_chat": true,
                "can_delete_messages": true,
                "can_manage_video_chats": true,
                "can_restrict_members": true,
                "can_promote_members": true,
                "can_change_info": true,
                "can_invite_users": true,
                "can_post_messages": true,
                "can_edit_messages": true,
                "can_pin_messages": true,
                "can_manage_topics": true,
                "custom_title": "Boss"
            }),
            "left" => json!({
                "status": "left",
                "user": user
            }),
            _ => json!({
                "status": "member",
                "user": user
            }),
        }
    }

    fn telegram_admin_member(user_id: i64) -> Result<TelegramChatMember, serde_json::Error> {
        serde_json::from_value(json!({
            "status": "administrator",
            "user": {
                "id": user_id,
                "is_bot": false,
                "first_name": "Tracked",
                "last_name": "Member",
                "username": "tracked",
                "language_code": "ru",
                "is_premium": true
            },
            "can_be_edited": false,
            "is_anonymous": false,
            "can_manage_chat": true,
            "can_delete_messages": true,
            "can_manage_video_chats": true,
            "can_restrict_members": true,
            "can_promote_members": true,
            "can_change_info": true,
            "can_invite_users": true,
            "can_post_messages": true,
            "can_edit_messages": true,
            "can_pin_messages": true,
            "can_manage_topics": true,
            "custom_title": "Boss"
        }))
    }

    fn fixed_time() -> Result<OffsetDateTime, time::error::ComponentRange> {
        OffsetDateTime::from_unix_timestamp(1_710_000_000)
    }

    fn member_upsert(chat_id: i64, user_id: i64, status: &str) -> ChatMemberUpsert {
        ChatMemberUpsert {
            chat_id,
            user_id,
            status: status.to_owned(),
            is_anonymous: Some(false),
            can_be_edited: Some(false),
            can_delete_messages: (status == "administrator").then_some(true),
            can_manage_video_chats: (status == "administrator").then_some(true),
            can_restrict_members: (status == "administrator").then_some(true),
            can_promote_members: (status == "administrator").then_some(true),
            can_change_info: (status == "administrator").then_some(true),
            can_invite_users: (status == "administrator").then_some(true),
            can_post_messages: (status == "administrator").then_some(true),
            can_edit_messages: (status == "administrator").then_some(true),
            can_pin_messages: (status == "administrator").then_some(true),
            can_manage_topics: (status == "administrator").then_some(true),
            ..ChatMemberUpsert::default()
        }
    }

    fn admin_member_state_upsert(chat_id: i64, user_id: i64) -> ChatMemberUpsert {
        ChatMemberUpsert {
            custom_title: Some("Boss".to_owned()),
            ..member_upsert(chat_id, user_id, "administrator")
        }
    }

    fn admin_member_sync_upsert(chat_id: i64, user_id: i64) -> ChatMemberUpsert {
        ChatMemberUpsert {
            can_manage_chat: Some(true),
            can_manage_topics: Some(false),
            ..admin_member_state_upsert(chat_id, user_id)
        }
    }

    #[derive(Clone, Default)]
    struct MemberStoreStub {
        current: Arc<Mutex<Vec<ChatMemberRecord>>>,
        deleted: Arc<Mutex<Vec<(i64, i64)>>>,
        upserted: Arc<Mutex<Vec<ChatMemberUpsert>>>,
        users: Arc<Mutex<Vec<UserState>>>,
    }

    impl MemberStoreStub {
        fn deleted(&self) -> Vec<(i64, i64)> {
            self.deleted.lock().expect("member store").clone()
        }

        fn upserted(&self) -> Vec<ChatMemberUpsert> {
            self.upserted.lock().expect("member store").clone()
        }

        fn users(&self) -> Vec<UserState> {
            self.users.lock().expect("member store").clone()
        }
    }

    impl LeftChatMemberStore for MemberStoreStub {
        type Error = StubError;

        fn delete_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> MemberStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.deleted
                    .lock()
                    .expect("member store")
                    .push((chat_id, user_id));
                Ok(())
            })
        }
    }

    impl ChatMemberStateStore for MemberStoreStub {
        type Error = StubError;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> MemberStoreFuture<'a, Option<ChatMemberRecord>, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .current
                    .lock()
                    .expect("member store")
                    .iter()
                    .find(|member| member.chat_id == chat_id && member.user_id == user_id)
                    .cloned())
            })
        }

        fn delete_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> MemberStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.deleted
                    .lock()
                    .expect("member store")
                    .push((chat_id, user_id));
                Ok(())
            })
        }

        fn upsert_chat_member<'a>(
            &'a self,
            member: ChatMemberUpsert,
        ) -> MemberStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.upserted.lock().expect("member store").push(member);
                Ok(())
            })
        }
    }

    impl crate::settings::GroupSettingsAdminSyncStore for MemberStoreStub {
        type Error = StubError;

        fn list_chat_members<'a>(
            &'a self,
            chat_id: i64,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, Vec<ChatMemberRecord>, Self::Error>
        {
            Box::pin(async move {
                Ok(self
                    .current
                    .lock()
                    .expect("member store")
                    .iter()
                    .filter(|member| member.chat_id == chat_id)
                    .cloned()
                    .collect())
            })
        }

        fn upsert_chat_member<'a>(
            &'a self,
            member: ChatMemberUpsert,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.upserted.lock().expect("member store").push(member);
                Ok(())
            })
        }

        fn upsert_user_state<'a>(
            &'a self,
            user: UserState,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.users.lock().expect("member store").push(user);
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct MemberApiStub {
        member: Arc<Mutex<Option<TelegramChatMember>>>,
        calls: Arc<Mutex<Vec<(i64, i64)>>>,
    }

    impl MemberApiStub {
        fn with_member(member: TelegramChatMember) -> Self {
            Self {
                member: Arc::new(Mutex::new(Some(member))),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<(i64, i64)> {
            self.calls.lock().expect("member api").clone()
        }
    }

    impl crate::settings::GroupSettingsMemberApi for MemberApiStub {
        type Error = StubError;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, TelegramChatMember, Self::Error>
        {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("member api")
                    .push((chat_id, user_id));
                self.member
                    .lock()
                    .expect("member api")
                    .clone()
                    .ok_or(StubError)
            })
        }
    }

    #[derive(Clone, Default)]
    struct AdminApiStub {
        admins: Arc<Mutex<Vec<TelegramChatMember>>>,
        calls: Arc<Mutex<Vec<i64>>>,
    }

    impl AdminApiStub {
        fn with_admins(admins: Vec<TelegramChatMember>) -> Self {
            Self {
                admins: Arc::new(Mutex::new(admins)),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<i64> {
            self.calls.lock().expect("admin api").clone()
        }
    }

    impl crate::settings::GroupSettingsAdminsApi for AdminApiStub {
        type Error = StubError;

        fn get_chat_administrators<'a>(
            &'a self,
            chat_id: i64,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, Vec<TelegramChatMember>, Self::Error>
        {
            Box::pin(async move {
                self.calls.lock().expect("admin api").push(chat_id);
                Ok(self.admins.lock().expect("admin api").clone())
            })
        }
    }

    type AdminCacheSaves = Arc<Mutex<Vec<(i64, Vec<i64>)>>>;

    #[derive(Clone, Default)]
    struct AdminCacheStub {
        saved: AdminCacheSaves,
    }

    impl AdminCacheStub {
        fn saved(&self) -> Vec<(i64, Vec<i64>)> {
            self.saved.lock().expect("admin cache").clone()
        }
    }

    impl crate::settings::GroupSettingsAdminCache for AdminCacheStub {
        type Error = StubError;

        fn save_chat_admin_ids<'a>(
            &'a self,
            chat_id: i64,
            admin_ids: Vec<i64>,
            _ttl: std::time::Duration,
        ) -> crate::settings::GroupSettingsMemberFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.saved
                    .lock()
                    .expect("admin cache")
                    .push((chat_id, admin_ids));
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct MemberStateControlJobQueueStub {
        jobs: Arc<Mutex<Vec<StatelessJobItem>>>,
    }

    impl MemberStateControlJobQueueStub {
        fn jobs(&self) -> Vec<StatelessJobItem> {
            self.jobs.lock().expect("member queue").clone()
        }
    }

    fn member_state_test_control_job(kind: ControlKind, title: &str) -> StatelessJobItem {
        new_control_job_at(
            ControlJobParams {
                chat_id: -10042,
                message_id: 0,
                user_id: 42,
                user_full_name: "Admin Actor".to_owned(),
                thread_id: None,
                data: ControlJobData {
                    kind,
                    chat_type: "supergroup".to_owned(),
                    ..ControlJobData::default()
                },
            },
            OffsetDateTime::UNIX_EPOCH,
        )
        .with_name(title)
    }

    impl MemberStateControlJobQueue for MemberStateControlJobQueueStub {
        type Error = StubError;

        fn assign_member_state_control_job<'a>(
            &'a self,
            _queue_name: &'static str,
            job: StatelessJobItem,
        ) -> MemberStateControlJobQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                self.jobs.lock().expect("member queue").push(job);
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct MemberStateSyncEffectsStub {
        admin_syncs: Arc<Mutex<Vec<i64>>>,
        member_syncs: Arc<Mutex<Vec<(i64, i64)>>>,
    }

    impl MemberStateSyncEffectsStub {
        fn admin_syncs(&self) -> Vec<i64> {
            self.admin_syncs.lock().expect("admin syncs").clone()
        }

        fn member_syncs(&self) -> Vec<(i64, i64)> {
            self.member_syncs.lock().expect("member syncs").clone()
        }
    }

    impl super::MemberStateControlJobEffects for MemberStateSyncEffectsStub {
        fn sync_chat_admins<'a>(&'a self, chat_id: i64) -> super::MemberStateControlJobFuture<'a> {
            Box::pin(async move {
                self.admin_syncs.lock().expect("admin syncs").push(chat_id);
            })
        }

        fn sync_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> super::MemberStateControlJobFuture<'a> {
            Box::pin(async move {
                self.member_syncs
                    .lock()
                    .expect("member syncs")
                    .push((chat_id, user_id));
            })
        }
    }

    #[derive(Clone, Default)]
    struct CommunicationEffectsStub {
        disabled: Arc<Mutex<Vec<i64>>>,
        enabled: Arc<Mutex<Vec<i64>>>,
    }

    impl CommunicationEffectsStub {
        fn disabled(&self) -> Vec<i64> {
            self.disabled.lock().expect("communication effects").clone()
        }

        fn enabled(&self) -> Vec<i64> {
            self.enabled.lock().expect("communication effects").clone()
        }
    }

    impl ChatCommunicationEffects for CommunicationEffectsStub {
        type Error = StubError;

        fn disable_chat_communication<'a>(
            &'a self,
            chat_id: i64,
        ) -> ChatCommunicationFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.disabled
                    .lock()
                    .expect("communication effects")
                    .push(chat_id);
                Ok(())
            })
        }

        fn enable_chat_communication<'a>(
            &'a self,
            chat_id: i64,
        ) -> ChatCommunicationFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.enabled
                    .lock()
                    .expect("communication effects")
                    .push(chat_id);
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct MemberDialogSchedulerStub;

    impl DialogMessageScheduler for MemberDialogSchedulerStub {
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
                })
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct MemberDialogSettingsStub;

    impl RandomDialogSettingsStore for MemberDialogSettingsStub {
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
    struct MemberDialogEffectsStub;

    impl RandomDialogEffects for MemberDialogEffectsStub {
        type Error = StubError;

        fn send_random_obscenified_text<'a>(
            &'a self,
            _plan: RandomObscenifiedTextPlan,
        ) -> DialogMessageFuture<'a, (), Self::Error> {
            Box::pin(async { Ok(()) })
        }
    }

    impl DirectSongNoticeEffects for MemberDialogEffectsStub {
        fn send_direct_song_notice<'a>(
            &'a self,
            _plan: DirectSongNoticePlan,
        ) -> DirectSongNoticeFuture<'a> {
            Box::pin(async { Ok(()) })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct MemberDialogRngStub;

    impl RandomDialogRng for MemberDialogRngStub {
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

    fn member_dialog_config() -> DialogMessageUpdateConfig {
        let mut bot_user = TelegramUser::new(999, "Plotva".to_owned(), true);
        bot_user.username = Some("plotva_bot".to_owned().into());
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

    #[derive(Clone)]
    struct ChatPermissionStoreStub {
        context: ChatPermissionContext,
        saved: Arc<Mutex<Vec<ChatSettingsUpdate>>>,
    }

    impl ChatPermissionStoreStub {
        fn with_context(context: ChatPermissionContext) -> Self {
            Self {
                context,
                saved: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn saved(&self) -> Vec<ChatSettingsUpdate> {
            self.saved.lock().expect("chat permission store").clone()
        }
    }

    impl ChatPermissionStore for ChatPermissionStoreStub {
        type Error = StubError;

        fn load_context<'a>(
            &'a self,
            _chat_id: i64,
        ) -> ChatPermissionStoreFuture<'a, Result<ChatPermissionContext, Self::Error>> {
            Box::pin(async move { Ok(self.context.clone()) })
        }

        fn save_settings<'a>(
            &'a self,
            update: ChatSettingsUpdate,
        ) -> ChatPermissionStoreFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.saved
                    .lock()
                    .expect("chat permission store")
                    .push(update);
                Ok(())
            })
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct StubError;

    impl fmt::Display for StubError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("member effect failed")
        }
    }

    impl Error for StubError {}
}
