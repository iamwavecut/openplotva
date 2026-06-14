//! App-level Telegram update consumer glue.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use carapax::types::{
    ChatMember as TelegramChatMember, ChatMemberUpdated as TelegramChatMemberUpdated,
    Update as TelegramUpdate, UpdateType as TelegramUpdateType,
};
use openplotva_core::{ChatMessageMeta, ChatState, UserState};
use openplotva_storage::TelegramFileMetadataUpsert;
use openplotva_updates::{
    HistoryTextEntry, NoopUpdateStageTracker, UpdateConsumerConfig, UpdateProcessReport,
    UpdateStageOutcome, UpdateStageTracker, build_fetcher_message_context,
    build_history_text_entry, extract_update_state, fetcher_message_text,
    should_skip_side_effects_at,
};
use thiserror::Error;
use time::{Date, OffsetDateTime};
use tokio::{sync::Semaphore, task::JoinSet};

/// Boxed future returned by update sources.
pub type UpdateSourceFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<TelegramUpdate>, E>> + Send + 'a>>;
pub type UpdateSourceBatchFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Vec<TelegramUpdate>, E>> + Send + 'a>>;

/// Boxed future returned by update handlers.
pub type UpdateHandlerFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;
type UpdateProcessorFuture = Pin<Box<dyn Future<Output = UpdateProcessReport> + Send>>;
type UpdateProcessor = Arc<
    dyn Fn(TelegramUpdate, UpdateConsumerConfig, SystemTime) -> UpdateProcessorFuture + Send + Sync,
>;

/// Boxed future returned by update state storage calls.
pub type UpdateStateStoreFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by Telegram file metadata storage calls.
pub type TelegramFileMetadataStoreFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by inbound history storage calls.
pub type InboundHistoryStoreFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by edited-message history storage calls.
pub type EditedHistoryStoreFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<bool, E>> + Send + 'a>>;

const UPDATE_CONSUMER_DEQUEUE_BATCH_MAX: usize = 256;

/// Source of decoded Telegram updates for the app-level consumer loop.
pub trait UpdateSource {
    /// Error returned by the concrete update source.
    type Error: fmt::Display;

    /// Dequeue one decoded update, returning `None` for a timeout or empty poll.
    fn dequeue_update<'a>(&'a self, timeout: Duration) -> UpdateSourceFuture<'a, Self::Error>;

    fn dequeue_updates<'a>(
        &'a self,
        timeout: Duration,
        max_count: usize,
    ) -> UpdateSourceBatchFuture<'a, Self::Error>
    where
        Self: Sync,
    {
        Box::pin(async move {
            let Some(update) = self.dequeue_update(timeout).await? else {
                return Ok(Vec::new());
            };
            let mut updates = Vec::with_capacity(max_count.max(1));
            updates.push(update);
            Ok(updates)
        })
    }
}

impl UpdateSource for openplotva_updates::RedisUpdateQueue {
    type Error = openplotva_updates::UpdateQueueError;

    fn dequeue_update<'a>(&'a self, timeout: Duration) -> UpdateSourceFuture<'a, Self::Error> {
        Box::pin(openplotva_updates::RedisUpdateQueue::dequeue_update(
            self, timeout,
        ))
    }

    fn dequeue_updates<'a>(
        &'a self,
        timeout: Duration,
        max_count: usize,
    ) -> UpdateSourceBatchFuture<'a, Self::Error> {
        Box::pin(openplotva_updates::RedisUpdateQueue::dequeue_updates(
            self, timeout, max_count,
        ))
    }
}

pub trait UpdateHandler {
    /// Error returned by the concrete handler.
    type Error: fmt::Display;

    /// Handle one decoded update after the state stage has been scheduled.
    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error>;
}

/// Storage capability needed by the app-level update consumer state stage.
pub trait UpdateStateStore {
    /// Error returned by the concrete state store.
    type Error: fmt::Display;

    /// Persist chat state extracted from a Telegram update.
    fn upsert_chat_state<'a>(
        &'a self,
        chat: &'a ChatState,
    ) -> UpdateStateStoreFuture<'a, Self::Error>;

    /// Persist user state extracted from a Telegram update.
    fn upsert_user_state<'a>(
        &'a self,
        user: &'a UserState,
    ) -> UpdateStateStoreFuture<'a, Self::Error>;

    /// Persist Telegram file metadata extracted from decoded updates.
    fn upsert_telegram_file_metadata<'a>(
        &'a self,
        params: &'a TelegramFileMetadataUpsert,
    ) -> TelegramFileMetadataStoreFuture<'a, Self::Error>;
}

impl UpdateStateStore for openplotva_storage::PostgresVirtualMessageStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_chat_state<'a>(
        &'a self,
        chat: &'a ChatState,
    ) -> UpdateStateStoreFuture<'a, Self::Error> {
        Box::pin(openplotva_storage::PostgresVirtualMessageStore::upsert_chat_state(self, chat))
    }

    fn upsert_user_state<'a>(
        &'a self,
        user: &'a UserState,
    ) -> UpdateStateStoreFuture<'a, Self::Error> {
        Box::pin(openplotva_storage::PostgresVirtualMessageStore::upsert_user_state(self, user))
    }

    fn upsert_telegram_file_metadata<'a>(
        &'a self,
        params: &'a TelegramFileMetadataUpsert,
    ) -> TelegramFileMetadataStoreFuture<'a, Self::Error> {
        Box::pin(async move {
            openplotva_storage::PostgresTelegramFileStore::new(self.pool().clone())
                .upsert_metadata(params)
                .await
                .map(|_| ())
        })
    }
}

/// Storage-shaped inbound chat-history text entry.
#[derive(Clone, Copy, Debug)]
pub struct InboundHistoryUpsert<'payload> {
    /// UTC bucket day partition.
    pub bucket_day: Date,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram thread/topic ID.
    pub thread_id: i32,
    /// Telegram message ID.
    pub message_id: i32,
    /// Stable history entry ID, such as `msg:123`.
    pub entry_id: &'payload str,
    /// History message kind.
    pub kind: &'payload str,
    /// Dialog role.
    pub role: &'payload str,
    /// Message timestamp.
    pub occurred_at: OffsetDateTime,
    /// Sender ID.
    pub sender_id: i64,
    pub payload: &'payload [u8],
}

/// Storage capability needed to persist inbound Telegram text history entries.
pub trait InboundHistoryStore {
    /// Error returned by the concrete history store.
    type Error: fmt::Display;

    fn upsert_inbound_history<'a>(
        &'a self,
        entry: InboundHistoryUpsert<'a>,
    ) -> InboundHistoryStoreFuture<'a, Self::Error>;
}

impl InboundHistoryStore for openplotva_storage::PostgresHistoryStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_inbound_history<'a>(
        &'a self,
        entry: InboundHistoryUpsert<'a>,
    ) -> InboundHistoryStoreFuture<'a, Self::Error> {
        Box::pin(async move {
            openplotva_storage::PostgresHistoryStore::upsert_history_entry(
                self,
                openplotva_storage::HistoryEntryUpsert {
                    bucket_day: entry.bucket_day,
                    chat_id: entry.chat_id,
                    thread_id: entry.thread_id,
                    message_id: entry.message_id,
                    entry_id: entry.entry_id,
                    kind: entry.kind,
                    role: entry.role,
                    occurred_at: entry.occurred_at,
                    sender_id: entry.sender_id,
                    payload: entry.payload,
                },
            )
            .await
        })
    }
}

/// Storage capability needed to persist edited Telegram message history updates.
pub trait EditedHistoryStore {
    /// Error returned by the concrete history store.
    type Error: fmt::Display;

    fn update_edited_message_history<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        new_text: &'a str,
        original_text: &'a str,
        meta: &'a ChatMessageMeta,
    ) -> EditedHistoryStoreFuture<'a, Self::Error>;
}

impl EditedHistoryStore for openplotva_storage::PostgresHistoryStore {
    type Error = openplotva_storage::StorageError;

    fn update_edited_message_history<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        new_text: &'a str,
        original_text: &'a str,
        meta: &'a ChatMessageMeta,
    ) -> EditedHistoryStoreFuture<'a, Self::Error> {
        Box::pin(
            openplotva_storage::PostgresHistoryStore::update_message_entry(
                self,
                chat_id,
                message_id,
                new_text,
                original_text,
                meta,
            ),
        )
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UpdateStatePersistenceReport {
    /// Whether a chat row was upserted.
    pub chat_persisted: bool,
    /// Whether a user row was upserted.
    pub user_persisted: bool,
    /// Telegram file refs extracted from the update and reply message.
    pub telegram_files_seen: usize,
    /// Telegram file metadata rows successfully upserted.
    pub telegram_files_persisted: usize,
    /// Non-fatal Telegram file metadata upsert failures.
    pub telegram_file_errors: usize,
}

/// Error returned while persisting extracted update state.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UpdateStatePersistenceError {
    /// Chat upsert failed.
    #[error("update chat: {message}")]
    Chat {
        /// Display form of the storage error.
        message: String,
    },
    /// User upsert failed.
    #[error("update user: {message}")]
    User {
        /// Display form of the storage error.
        message: String,
    },
}

pub async fn persist_update_state<S>(
    store: &S,
    update: &TelegramUpdate,
) -> Result<UpdateStatePersistenceReport, UpdateStatePersistenceError>
where
    S: UpdateStateStore + Sync,
{
    let Some(state) = extract_update_state(update) else {
        return Ok(UpdateStatePersistenceReport::default());
    };

    let mut report = UpdateStatePersistenceReport::default();
    if let Some(chat) = state.chat.as_ref() {
        store
            .upsert_chat_state(chat)
            .await
            .map_err(|error| UpdateStatePersistenceError::Chat {
                message: error.to_string(),
            })?;
        report.chat_persisted = true;
    }
    if let Some(user) = state.user.as_ref() {
        store
            .upsert_user_state(user)
            .await
            .map_err(|error| UpdateStatePersistenceError::User {
                message: error.to_string(),
            })?;
        report.user_persisted = true;
    }
    let refs = openplotva_updates::update_file_metadata_refs(update);
    report.telegram_files_seen = refs.len();
    for file_ref in refs {
        let Some(params) = telegram_file_metadata_upsert_from_ref(&file_ref) else {
            continue;
        };
        match store.upsert_telegram_file_metadata(&params).await {
            Ok(()) => {
                report.telegram_files_persisted += 1;
            }
            Err(error) => {
                report.telegram_file_errors += 1;
                tracing::warn!(
                    error = error.to_string(),
                    file_unique_id = params.file_unique_id,
                    "failed to upsert Telegram file metadata"
                );
            }
        }
    }
    Ok(report)
}

fn telegram_file_metadata_upsert_from_ref(
    file_ref: &openplotva_updates::TelegramFileMetadataRef,
) -> Option<TelegramFileMetadataUpsert> {
    if file_ref.file_id.is_empty() || file_ref.file_unique_id.is_empty() {
        return None;
    }

    let first_seen_chat_id = (file_ref.chat_id != 0).then_some(file_ref.chat_id);
    let first_seen_message_id = (file_ref.message_id != 0).then_some(file_ref.message_id);
    Some(TelegramFileMetadataUpsert {
        file_unique_id: file_ref.file_unique_id.clone(),
        latest_file_id: file_ref.file_id.clone(),
        media_kind: file_ref.media_kind.clone(),
        mime_type: (!file_ref.mime_type.is_empty()).then(|| file_ref.mime_type.clone()),
        width: (file_ref.width > 0).then_some(file_ref.width),
        height: (file_ref.height > 0).then_some(file_ref.height),
        file_size: (file_ref.file_size > 0).then_some(file_ref.file_size),
        first_seen_chat_id,
        first_seen_message_id,
        last_seen_chat_id: first_seen_chat_id,
        last_seen_message_id: first_seen_message_id,
    })
}

/// Report for inbound Telegram history persistence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InboundMessageHistoryReport {
    /// Whether a history entry was built and upserted.
    pub entry_persisted: bool,
}

/// Error returned while persisting inbound message history.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InboundMessageHistoryError {
    /// History entry upsert failed.
    #[error("upsert inbound history: {message}")]
    Upsert {
        /// Display form of the storage error.
        message: String,
    },
}

/// Report for edited Telegram history persistence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EditedMessageHistoryReport {
    /// Whether an existing history entry was found and updated.
    pub entry_updated: bool,
}

/// Error returned while persisting edited message history.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EditedMessageHistoryError {
    /// History entry update failed.
    #[error("update edited message history: {message}")]
    Update {
        /// Display form of the storage error.
        message: String,
    },
}

/// Report for history side effects derived directly from one decoded update.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UpdateHistoryPersistenceReport {
    /// Whether an inbound `message` update created or replaced a text history entry.
    pub inbound_entry_persisted: bool,
    /// Whether an inbound `edited_message` update found and updated an existing text entry.
    pub edited_entry_updated: bool,
}

/// Error returned while persisting history side effects derived from a decoded update.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UpdateHistoryPersistenceError {
    /// Inbound message history persistence failed.
    #[error("inbound message history: {message}")]
    Inbound {
        /// Display form of the persistence error.
        message: String,
    },
    /// Edited message history persistence failed.
    #[error("edited message history: {message}")]
    Edited {
        /// Display form of the persistence error.
        message: String,
    },
}

/// Non-fatal history side-effect result for one handled update.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateHistorySideEffectReport {
    /// History persistence outcome when storage completed or the update had no history work.
    pub persistence: UpdateHistoryPersistenceReport,
    /// Display form of a history error that was logged and suppressed.
    pub error: Option<String>,
}

impl UpdateHistorySideEffectReport {
    fn from_persistence_result(
        result: Result<UpdateHistoryPersistenceReport, UpdateHistoryPersistenceError>,
    ) -> Self {
        match result {
            Ok(persistence) => Self {
                persistence,
                error: None,
            },
            Err(error) => Self {
                persistence: UpdateHistoryPersistenceReport::default(),
                error: Some(error.to_string()),
            },
        }
    }
}

fn trace_update_history_error(update: &TelegramUpdate, error: &str) {
    tracing::warn!(
        error,
        update_id = update.id,
        update_name = openplotva_updates::update_name(update),
        "update history persistence failed"
    );
}

/// Error returned by a handler wrapped with non-fatal history persistence.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum UpdateHandleWithHistoryError {
    /// The injected handler failed after history side effects were attempted.
    #[error("handle update: {message}")]
    Handler {
        /// Display form of the handler error.
        message: String,
        /// History side-effect outcome observed before the handler error.
        history: UpdateHistorySideEffectReport,
    },
}

/// Adapter that adds non-fatal history persistence to an existing update handler.
#[derive(Clone, Debug)]
pub struct UpdateHandlerWithHistory<History, Handler> {
    history_store: Arc<History>,
    handler: Arc<Handler>,
    bot_id: i64,
}

impl<History, Handler> UpdateHandlerWithHistory<History, Handler> {
    pub fn new(history_store: Arc<History>, handler: Arc<Handler>, bot_id: i64) -> Self {
        Self {
            history_store,
            handler,
            bot_id,
        }
    }
}

impl<History, Handler> UpdateHandler for UpdateHandlerWithHistory<History, Handler>
where
    History: InboundHistoryStore + EditedHistoryStore + Send + Sync,
    Handler: UpdateHandler + Send + Sync,
{
    type Error = UpdateHandleWithHistoryError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_update_with_history(self.history_store.as_ref(), update, self.bot_id, |update| {
                self.handler.handle_update(update)
            })
            .await
            .map(|_| ())
        })
    }
}

pub async fn persist_inbound_message_history<S>(
    store: &S,
    update: &TelegramUpdate,
    original_text: &str,
    meta: ChatMessageMeta,
    bot_id: i64,
) -> Result<InboundMessageHistoryReport, InboundMessageHistoryError>
where
    S: InboundHistoryStore + Sync,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        return Ok(InboundMessageHistoryReport::default());
    };
    let Some(entry) = build_history_text_entry(message, original_text, meta, bot_id) else {
        return Ok(InboundMessageHistoryReport::default());
    };

    store
        .upsert_inbound_history(inbound_history_upsert(&entry))
        .await
        .map_err(|error| InboundMessageHistoryError::Upsert {
            message: error.to_string(),
        })?;

    Ok(InboundMessageHistoryReport {
        entry_persisted: true,
    })
}

pub async fn persist_edited_message_history<S>(
    store: &S,
    update: &TelegramUpdate,
    original_text: &str,
    meta: ChatMessageMeta,
) -> Result<EditedMessageHistoryReport, EditedMessageHistoryError>
where
    S: EditedHistoryStore + Sync,
{
    let TelegramUpdateType::EditedMessage(message) = &update.update_type else {
        return Ok(EditedMessageHistoryReport::default());
    };
    let chat_id = message.chat.get_id().into();
    if chat_id == 0 {
        return Ok(EditedMessageHistoryReport::default());
    }
    let Ok(message_id) = i32::try_from(message.id) else {
        return Ok(EditedMessageHistoryReport::default());
    };
    let new_text = fetcher_message_text(message);
    let entry_updated = store
        .update_edited_message_history(chat_id, message_id, &new_text, original_text, &meta)
        .await
        .map_err(|error| EditedMessageHistoryError::Update {
            message: error.to_string(),
        })?;

    Ok(EditedMessageHistoryReport { entry_updated })
}

pub async fn persist_update_history<S>(
    store: &S,
    update: &TelegramUpdate,
    bot_id: i64,
) -> Result<UpdateHistoryPersistenceReport, UpdateHistoryPersistenceError>
where
    S: InboundHistoryStore + EditedHistoryStore + Sync,
{
    match &update.update_type {
        TelegramUpdateType::Message(message) => {
            let context = build_fetcher_message_context(message);
            let report = persist_inbound_message_history(
                store,
                update,
                &context.original_text,
                context.meta,
                bot_id,
            )
            .await
            .map_err(|error| UpdateHistoryPersistenceError::Inbound {
                message: error.to_string(),
            })?;

            Ok(UpdateHistoryPersistenceReport {
                inbound_entry_persisted: report.entry_persisted,
                edited_entry_updated: false,
            })
        }
        TelegramUpdateType::EditedMessage(message) => {
            let context = build_fetcher_message_context(message);
            let report =
                persist_edited_message_history(store, update, &context.original_text, context.meta)
                    .await
                    .map_err(|error| UpdateHistoryPersistenceError::Edited {
                        message: error.to_string(),
                    })?;

            Ok(UpdateHistoryPersistenceReport {
                inbound_entry_persisted: false,
                edited_entry_updated: report.entry_updated,
            })
        }
        _ => Ok(UpdateHistoryPersistenceReport::default()),
    }
}

/// Persist update history before and after invoking the update handler.
/// History failures are logged and returned in the report but do not prevent
pub async fn handle_update_with_history<S, HandleFn, HandleFuture, HandleError>(
    store: &S,
    update: TelegramUpdate,
    bot_id: i64,
    handle: HandleFn,
) -> Result<UpdateHistorySideEffectReport, UpdateHandleWithHistoryError>
where
    S: InboundHistoryStore + EditedHistoryStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let history = UpdateHistorySideEffectReport::from_persistence_result(
        persist_update_history(store, &update, bot_id).await,
    );
    if let Some(error) = history.error.as_deref() {
        trace_update_history_error(&update, error);
    }

    handle(update)
        .await
        .map_err(|error| UpdateHandleWithHistoryError::Handler {
            message: error.to_string(),
            history: history.clone(),
        })?;

    Ok(history)
}

/// Process one decoded update with app-owned state persistence and an injected handler.
pub async fn process_update_with_state_store<S, HandleFn, HandleFuture, HandleError>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    store: &S,
    handle: HandleFn,
) -> UpdateProcessReport
where
    S: UpdateStateStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    process_update_with_state_store_at(update, config, SystemTime::now(), store, handle).await
}

pub async fn process_update_with_state_store_at<S, HandleFn, HandleFuture, HandleError>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    store: &S,
    handle: HandleFn,
) -> UpdateProcessReport
where
    S: UpdateStateStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    openplotva_updates::process_update_at(
        update,
        config,
        now,
        |update| async move { persist_update_state(store, &update).await.map(|_| ()) },
        handle,
    )
    .await
}

/// Process one decoded update with app-owned state persistence and live stage diagnostics.
pub async fn process_update_with_state_store_tracked_at<
    S,
    HandleFn,
    HandleFuture,
    HandleError,
    Tracker,
>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    store: &S,
    handle: HandleFn,
    tracker: &Tracker,
) -> UpdateProcessReport
where
    S: UpdateStateStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
    Tracker: UpdateStageTracker + ?Sized,
{
    openplotva_updates::process_update_with_stage_tracker_at(
        update,
        config,
        now,
        |update| async move { persist_update_state(store, &update).await.map(|_| ()) },
        handle,
        tracker,
    )
    .await
}

/// Process one decoded update with app-owned state persistence, stale-only
/// history persistence, and an injected handler.
pub async fn process_update_with_state_and_history_store<
    S,
    History,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    state_store: &S,
    history_store: &History,
    bot_id: i64,
    handle: HandleFn,
) -> UpdateProcessReport
where
    S: UpdateStateStore + Sync,
    History: InboundHistoryStore + EditedHistoryStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    process_update_with_state_and_history_store_at(
        update,
        config,
        SystemTime::now(),
        state_store,
        history_store,
        bot_id,
        handle,
    )
    .await
}

/// Process one decoded update with an explicit clock instant and stale-only
/// history persistence before the handle stage.
pub async fn process_update_with_state_and_history_store_at<
    S,
    History,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    state_store: &S,
    history_store: &History,
    bot_id: i64,
    handle: HandleFn,
) -> UpdateProcessReport
where
    S: UpdateStateStore + Sync,
    History: InboundHistoryStore + EditedHistoryStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    process_update_with_state_and_history_store_tracked_at(
        update,
        config,
        now,
        state_store,
        history_store,
        bot_id,
        handle,
        &NoopUpdateStageTracker,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn process_update_with_state_and_history_store_tracked_at<
    S,
    History,
    HandleFn,
    HandleFuture,
    HandleError,
    Tracker,
>(
    update: TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    state_store: &S,
    history_store: &History,
    bot_id: i64,
    handle: HandleFn,
    tracker: &Tracker,
) -> UpdateProcessReport
where
    S: UpdateStateStore + Sync,
    History: InboundHistoryStore + EditedHistoryStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
    Tracker: UpdateStageTracker + ?Sized,
{
    let persist_history_before_handle =
        openplotva_updates::should_skip_side_effects_at(&update, config.side_effect_max_age, now);
    openplotva_updates::process_update_with_stage_tracker_at(
        update,
        config,
        now,
        |update| async move {
            let state = persist_update_state(state_store, &update)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string());
            if persist_history_before_handle {
                let history = UpdateHistorySideEffectReport::from_persistence_result(
                    persist_update_history(history_store, &update, bot_id).await,
                );
                if let Some(error) = history.error.as_deref() {
                    trace_update_history_error(&update, error);
                }
            }
            state
        },
        handle,
        tracker,
    )
    .await
}

/// Summary for one app-level update consumer run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateConsumerRunReport {
    /// Updates popped from the source.
    pub dequeued: usize,
    /// Empty polls/timeouts observed while waiting for updates.
    pub empty_polls: usize,
    pub dequeue_errors: Vec<String>,
    /// Processed update reports completed before shutdown returned.
    pub processed: usize,
    /// State stages that returned an error.
    pub state_failures: usize,
    /// Handler stages that returned an error.
    pub handle_failures: usize,
    /// State or handler stages that timed out.
    pub timeouts: usize,
    /// Handler stages skipped because the update was stale.
    pub skipped_handles: usize,
    /// Buffered human chat-member updates dropped by net-effect coalescing.
    pub coalesced_member_updates: usize,
    /// Worker task join failures.
    pub join_errors: Vec<String>,
}

/// Run the app-level update consumer until `stop` resolves, then wait for in-flight work.
pub async fn run_update_consumer_until<Q, S, H, Stop>(
    source: Arc<Q>,
    config: UpdateConsumerConfig,
    store: Arc<S>,
    handler: Arc<H>,
    stop: Stop,
) -> UpdateConsumerRunReport
where
    Q: UpdateSource + Send + Sync + 'static,
    S: UpdateStateStore + Send + Sync + 'static,
    H: UpdateHandler + Send + Sync + 'static,
    Stop: Future<Output = ()> + Send,
{
    run_update_consumer_with_stage_tracker_until(
        source,
        config,
        store,
        handler,
        Arc::new(NoopUpdateStageTracker),
        stop,
    )
    .await
}

/// Run the app-level update consumer with live stage diagnostics.
pub async fn run_update_consumer_with_stage_tracker_until<Q, S, H, Tracker, Stop>(
    source: Arc<Q>,
    config: UpdateConsumerConfig,
    store: Arc<S>,
    handler: Arc<H>,
    tracker: Arc<Tracker>,
    stop: Stop,
) -> UpdateConsumerRunReport
where
    Q: UpdateSource + Send + Sync + 'static,
    S: UpdateStateStore + Send + Sync + 'static,
    H: UpdateHandler + Send + Sync + 'static,
    Tracker: UpdateStageTracker + Send + Sync + 'static,
    Stop: Future<Output = ()> + Send,
{
    let processor: UpdateProcessor = Arc::new(move |update, config, now| {
        let store = Arc::clone(&store);
        let handler = Arc::clone(&handler);
        let tracker = Arc::clone(&tracker);
        Box::pin(async move {
            process_update_with_state_store_tracked_at(
                update,
                config,
                now,
                store.as_ref(),
                |update| handler.handle_update(update),
                tracker.as_ref(),
            )
            .await
        })
    });
    run_update_consumer_with_processor_until(source, config, processor, stop).await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_update_consumer_with_history_stage_tracker_until<Q, S, History, H, Tracker, Stop>(
    source: Arc<Q>,
    config: UpdateConsumerConfig,
    store: Arc<S>,
    history_store: Arc<History>,
    bot_id: i64,
    handler: Arc<H>,
    tracker: Arc<Tracker>,
    stop: Stop,
) -> UpdateConsumerRunReport
where
    Q: UpdateSource + Send + Sync + 'static,
    S: UpdateStateStore + Send + Sync + 'static,
    History: InboundHistoryStore + EditedHistoryStore + Send + Sync + 'static,
    H: UpdateHandler + Send + Sync + 'static,
    Tracker: UpdateStageTracker + Send + Sync + 'static,
    Stop: Future<Output = ()> + Send,
{
    let processor: UpdateProcessor = Arc::new(move |update, config, now| {
        let store = Arc::clone(&store);
        let history_store = Arc::clone(&history_store);
        let handler = Arc::clone(&handler);
        let tracker = Arc::clone(&tracker);
        Box::pin(async move {
            process_update_with_state_and_history_store_tracked_at(
                update,
                config,
                now,
                store.as_ref(),
                history_store.as_ref(),
                bot_id,
                |update| handler.handle_update(update),
                tracker.as_ref(),
            )
            .await
        })
    });
    run_update_consumer_with_processor_until(source, config, processor, stop).await
}

async fn run_update_consumer_with_processor_until<Q, Stop>(
    source: Arc<Q>,
    config: UpdateConsumerConfig,
    processor: UpdateProcessor,
    stop: Stop,
) -> UpdateConsumerRunReport
where
    Q: UpdateSource + Send + Sync + 'static,
    Stop: Future<Output = ()> + Send,
{
    let worker_limit = config.worker_limit.max(1);
    let semaphore = Arc::new(Semaphore::new(worker_limit));
    let mut workers = JoinSet::new();
    let mut report = UpdateConsumerRunReport::default();
    tokio::pin!(stop);

    'outer: loop {
        tokio::select! {
            _ = &mut stop => break,
            dequeued = source.dequeue_updates(config.dequeue_timeout, UPDATE_CONSUMER_DEQUEUE_BATCH_MAX) => {
                match dequeued {
                    Ok(updates) if updates.is_empty() => report.empty_polls += 1,
                    Ok(updates) => {
                        report.dequeued += updates.len();
                        let updates = coalesce_buffered_member_updates(updates, &mut report);
                        for update in updates {
                            let now = SystemTime::now();
                            let permit_count =
                                stage_permits_for_update(&update, config, now, worker_limit);
                            let permits = tokio::select! {
                                _ = &mut stop => break 'outer,
                                acquired = semaphore.clone().acquire_many_owned(permit_count) => {
                                    match acquired {
                                        Ok(permits) => permits,
                                        Err(_) => break 'outer,
                                    }
                                }
                            };

                            let processor = Arc::clone(&processor);
                            workers.spawn(async move {
                                let _permits = permits;
                                processor(update, config, now).await
                            });
                        }
                    }
                    Err(error) => {
                        let error = error.to_string();
                        tracing::warn!(%error, "failed to dequeue Telegram update");
                        report.dequeue_errors.push(error);
                    }
                }
            }
            Some(joined) = workers.join_next(), if !workers.is_empty() => {
                record_worker_join(joined, &mut report);
            }
        }
    }

    while let Some(joined) = workers.join_next().await {
        record_worker_join(joined, &mut report);
    }

    report
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct MemberCoalesceKey {
    chat_id: i64,
    user_id: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MemberPresence {
    Active,
    Inactive,
}

struct MemberCoalesceEntry {
    key: MemberCoalesceKey,
    old_presence: MemberPresence,
    new_presence: MemberPresence,
    update: TelegramUpdate,
}

struct MemberCoalesceSummary {
    first_old_presence: MemberPresence,
    final_new_presence: MemberPresence,
    count: usize,
    last_index: usize,
    last_update: TelegramUpdate,
}

fn coalesce_buffered_member_updates(
    updates: Vec<TelegramUpdate>,
    report: &mut UpdateConsumerRunReport,
) -> Vec<TelegramUpdate> {
    let mut out = Vec::with_capacity(updates.len());
    let mut run = Vec::new();

    for update in updates {
        if let Some(transition) = coalescible_member_transition_from_update(&update) {
            run.push(MemberCoalesceEntry {
                key: transition.key,
                old_presence: transition.old_presence,
                new_presence: transition.new_presence,
                update,
            });
        } else {
            flush_member_coalesce_run(&mut run, &mut out, report);
            out.push(update);
        }
    }
    flush_member_coalesce_run(&mut run, &mut out, report);
    out
}

fn flush_member_coalesce_run(
    run: &mut Vec<MemberCoalesceEntry>,
    out: &mut Vec<TelegramUpdate>,
    report: &mut UpdateConsumerRunReport,
) {
    if run.is_empty() {
        return;
    }

    let mut order = Vec::new();
    let mut summaries: HashMap<MemberCoalesceKey, MemberCoalesceSummary> = HashMap::new();
    for (index, entry) in run.drain(..).enumerate() {
        match summaries.get_mut(&entry.key) {
            Some(summary) => {
                summary.final_new_presence = entry.new_presence;
                summary.count += 1;
                summary.last_index = index;
                summary.last_update = entry.update;
            }
            None => {
                order.push(entry.key);
                summaries.insert(
                    entry.key,
                    MemberCoalesceSummary {
                        first_old_presence: entry.old_presence,
                        final_new_presence: entry.new_presence,
                        count: 1,
                        last_index: index,
                        last_update: entry.update,
                    },
                );
            }
        }
    }

    order.sort_by_key(|key| {
        summaries
            .get(key)
            .map_or(usize::MAX, |summary| summary.last_index)
    });
    for key in order {
        let Some(summary) = summaries.remove(&key) else {
            continue;
        };
        if summary.first_old_presence == summary.final_new_presence {
            report.coalesced_member_updates += summary.count;
        } else {
            report.coalesced_member_updates += summary.count.saturating_sub(1);
            out.push(summary.last_update);
        }
    }
}

fn coalescible_member_transition_from_update(
    update: &TelegramUpdate,
) -> Option<MemberCoalesceTransition> {
    let TelegramUpdateType::UserStatus(member_update) = &update.update_type else {
        return None;
    };
    coalescible_member_transition(member_update.as_ref())
}

struct MemberCoalesceTransition {
    key: MemberCoalesceKey,
    old_presence: MemberPresence,
    new_presence: MemberPresence,
}

fn coalescible_member_transition(
    update: &TelegramChatMemberUpdated,
) -> Option<MemberCoalesceTransition> {
    let old_user = update.old_chat_member.get_user();
    let new_user = update.new_chat_member.get_user();
    let user_id = i64::from(new_user.id);
    if user_id == 0 || i64::from(old_user.id) != user_id || old_user.is_bot || new_user.is_bot {
        return None;
    }
    let chat_id = update.chat.get_id().into();
    if chat_id == 0 {
        return None;
    }

    Some(MemberCoalesceTransition {
        key: MemberCoalesceKey { chat_id, user_id },
        old_presence: coalescible_member_presence(&update.old_chat_member)?,
        new_presence: coalescible_member_presence(&update.new_chat_member)?,
    })
}

fn coalescible_member_presence(member: &TelegramChatMember) -> Option<MemberPresence> {
    match member {
        TelegramChatMember::Member { .. } => Some(MemberPresence::Active),
        TelegramChatMember::Left(_) | TelegramChatMember::Kicked(_) => {
            Some(MemberPresence::Inactive)
        }
        TelegramChatMember::Administrator(_)
        | TelegramChatMember::Creator(_)
        | TelegramChatMember::Restricted(_) => None,
    }
}

fn stage_permits_for_update(
    update: &TelegramUpdate,
    config: UpdateConsumerConfig,
    now: SystemTime,
    worker_limit: usize,
) -> u32 {
    let desired = if should_skip_side_effects_at(update, config.side_effect_max_age, now) {
        1
    } else {
        2
    };
    desired.min(worker_limit.max(1)) as u32
}

fn inbound_history_upsert(entry: &HistoryTextEntry) -> InboundHistoryUpsert<'_> {
    InboundHistoryUpsert {
        bucket_day: entry.occurred_at.date(),
        chat_id: entry.chat_id,
        thread_id: entry.thread_id,
        message_id: entry.message_id,
        entry_id: &entry.entry_id,
        kind: &entry.kind,
        role: &entry.role,
        occurred_at: entry.occurred_at,
        sender_id: entry.sender_id,
        payload: &entry.payload,
    }
}

fn record_worker_join(
    joined: Result<UpdateProcessReport, tokio::task::JoinError>,
    run: &mut UpdateConsumerRunReport,
) {
    match joined {
        Ok(report) => record_update_report(&report, run),
        Err(error) => run.join_errors.push(error.to_string()),
    }
}

fn record_update_report(report: &UpdateProcessReport, run: &mut UpdateConsumerRunReport) {
    run.processed += 1;
    record_stage_outcome(&report.state.outcome, true, run);
    match &report.handle {
        Some(handle) => record_stage_outcome(&handle.outcome, false, run),
        None if report.skipped_handle => run.skipped_handles += 1,
        None => {}
    }
}

fn record_stage_outcome(
    outcome: &UpdateStageOutcome,
    is_state: bool,
    run: &mut UpdateConsumerRunReport,
) {
    match outcome {
        UpdateStageOutcome::Completed => {}
        UpdateStageOutcome::Failed(_) if is_state => run.state_failures += 1,
        UpdateStageOutcome::Failed(_) => run.handle_failures += 1,
        UpdateStageOutcome::TimedOut => run.timeouts += 1,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::{ChatAttachment, ChatMessageMeta, ChatState, UserState};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::{Value, json};
    use tokio::sync::Notify;

    use super::{
        EditedHistoryStore, EditedHistoryStoreFuture, InboundHistoryStore,
        InboundHistoryStoreFuture, InboundHistoryUpsert, TelegramFileMetadataStoreFuture,
        UpdateHandler, UpdateHandlerFuture, UpdateHandlerWithHistory, UpdateSource,
        UpdateSourceBatchFuture, UpdateSourceFuture, UpdateStateStore, UpdateStateStoreFuture,
        persist_update_state, process_update_with_state_and_history_store_at,
        process_update_with_state_store_at, run_update_consumer_until,
    };

    #[tokio::test]
    async fn persist_update_state_writes_chat_before_user_like_go_consumer()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();

        let report = persist_update_state(&store, &sample_message_update()?).await?;

        assert!(report.chat_persisted);
        assert!(report.user_persisted);
        assert_eq!(
            store.calls(),
            vec![
                "chat:42:private:Ada:ada_l".to_owned(),
                "user:99:Ada:ada_l".to_owned(),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn persist_update_state_skips_guest_updates_without_storage_calls()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();

        let report = persist_update_state(&store, &sample_guest_update()?).await?;

        assert!(!report.chat_persisted);
        assert!(!report.user_persisted);
        assert!(store.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn persist_update_state_captures_telegram_file_metadata_like_go_fetcher()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();

        let report = persist_update_state(&store, &sample_caption_message_update()?).await?;

        assert!(report.chat_persisted);
        assert!(report.user_persisted);
        assert_eq!(report.telegram_files_seen, 1);
        assert_eq!(report.telegram_files_persisted, 1);
        assert_eq!(report.telegram_file_errors, 0);
        assert_eq!(
            store.calls(),
            vec![
                "chat:42:private:Ada:ada_l".to_owned(),
                "user:99:Ada:ada_l".to_owned(),
                "file:photo-large-u:photo-large:photo::Some(1024):Some(768):None:Some(42):Some(78)"
                    .to_owned(),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn persist_inbound_message_history_builds_and_upserts_go_entry()
    -> Result<(), Box<dyn Error>> {
        let store = HistoryStoreStub::default();

        let report = super::persist_inbound_message_history(
            &store,
            &sample_message_update()?,
            " /start hello ",
            ChatMessageMeta::default(),
            0,
        )
        .await?;

        assert!(report.entry_persisted);
        let entries = store.entries();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.bucket_day.to_string(), "2024-03-09");
        assert_eq!(entry.chat_id, 42);
        assert_eq!(entry.thread_id, 0);
        assert_eq!(entry.message_id, 77);
        assert_eq!(entry.entry_id, "msg:77");
        assert_eq!(entry.kind, "text");
        assert_eq!(entry.role, "user");
        assert_eq!(entry.sender_id, 99);
        assert_eq!(entry.occurred_at.unix_timestamp(), 1_710_000_000);

        let payload: Value = serde_json::from_slice(&entry.payload)?;
        assert_eq!(payload["entry_id"], "msg:77");
        assert_eq!(payload["text"], "/start hello");
        assert!(payload.get("original_text").is_none());
        assert_eq!(payload["meta"]["sender_id"], 99);
        assert_eq!(payload["meta"]["sender_name"], "Ada");
        Ok(())
    }

    #[tokio::test]
    async fn persist_edited_message_history_updates_existing_go_entry() -> Result<(), Box<dyn Error>>
    {
        let store = HistoryStoreStub::default();
        let meta = ChatMessageMeta {
            sender_id: 99,
            attachments: vec![ChatAttachment {
                kind: "image".to_owned(),
                source: "message".to_owned(),
                caption: " edited text ".to_owned(),
                content: "edited text".to_owned(),
                ..ChatAttachment::default()
            }],
            ..ChatMessageMeta::default()
        };

        let report = super::persist_edited_message_history(
            &store,
            &sample_edited_message_update()?,
            " original edit ",
            meta.clone(),
        )
        .await?;

        assert!(report.entry_updated);
        assert!(store.entries().is_empty());
        assert_eq!(
            store.edits(),
            vec![RecordedHistoryEdit {
                chat_id: 42,
                message_id: 77,
                new_text: " edited text ".to_owned(),
                original_text: " original edit ".to_owned(),
                meta,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn persist_update_history_derives_go_context_for_inbound_caption_message()
    -> Result<(), Box<dyn Error>> {
        let store = HistoryStoreStub::default();

        let report =
            super::persist_update_history(&store, &sample_caption_message_update()?, 0).await?;

        assert!(report.inbound_entry_persisted);
        assert!(!report.edited_entry_updated);
        assert!(store.edits().is_empty());
        let entries = store.entries();
        assert_eq!(entries.len(), 1);

        let payload: Value = serde_json::from_slice(&entries[0].payload)?;
        assert_eq!(payload["text"], "photo caption");
        assert!(payload.get("original_text").is_none());
        assert_eq!(payload["meta"]["type"], "image");
        assert_eq!(payload["meta"]["attachments"][0]["kind"], "image");
        assert_eq!(
            payload["meta"]["attachments"][0]["file_unique_id"],
            "photo-large-u"
        );
        assert!(payload["meta"]["attachments"][0].get("caption").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn persist_update_history_derives_go_context_for_edited_message()
    -> Result<(), Box<dyn Error>> {
        let store = HistoryStoreStub::default();

        let report =
            super::persist_update_history(&store, &sample_edited_message_update()?, 0).await?;

        assert!(!report.inbound_entry_persisted);
        assert!(report.edited_entry_updated);
        assert!(store.entries().is_empty());
        let edits = store.edits();
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].chat_id, 42);
        assert_eq!(edits[0].message_id, 77);
        assert_eq!(edits[0].new_text, " edited text ");
        assert_eq!(edits[0].original_text, " edited text ");
        assert_eq!(edits[0].meta.message_type, "text");
        assert_eq!(edits[0].meta.sender_id, 99);
        assert_eq!(edits[0].meta.sender_name, "Ada");
        Ok(())
    }

    #[tokio::test]
    async fn process_update_with_state_store_persists_state_and_calls_injected_handler()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let handled = Arc::new(Mutex::new(Vec::new()));
        let handled_updates = Arc::clone(&handled);

        let report = process_update_with_state_store_at(
            sample_message_update()?,
            UpdateConsumerConfig {
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                ..UpdateConsumerConfig::default()
            },
            unix_time(1_710_000_010),
            &store,
            move |update| {
                let handled = Arc::clone(&handled_updates);
                async move {
                    handled
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await;

        assert_eq!(report.update_id, 12345);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            handled.lock().expect("handled updates").as_slice(),
            &[12345]
        );
        assert!(store.calls().iter().any(|call| call.starts_with("chat:42")));
        assert!(store.calls().iter().any(|call| call.starts_with("user:99")));
        Ok(())
    }

    #[tokio::test]
    async fn process_update_with_state_store_skips_stale_handler_but_keeps_state()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();

        let report = process_update_with_state_store_at(
            sample_message_update()?,
            UpdateConsumerConfig {
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(60),
                ..UpdateConsumerConfig::default()
            },
            unix_time(1_710_000_060),
            &store,
            |_update| async {
                Err::<(), io::Error>(io::Error::other(
                    "handler should be skipped for stale updates",
                ))
            },
        )
        .await;

        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert!(report.skipped_handle);
        assert_eq!(report.handle, None);
        assert_eq!(store.calls().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn process_update_with_state_and_history_store_keeps_stale_history_without_handler()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let history = HistoryStoreStub::default();
        let handled = Arc::new(Mutex::new(Vec::new()));
        let handled_updates = Arc::clone(&handled);

        let report = process_update_with_state_and_history_store_at(
            sample_message_update()?,
            UpdateConsumerConfig {
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(5 * 60),
                ..UpdateConsumerConfig::default()
            },
            unix_time(1_710_000_300),
            &store,
            &history,
            0,
            move |update| {
                let handled = Arc::clone(&handled_updates);
                async move {
                    handled
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await;

        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert!(report.skipped_handle);
        assert_eq!(report.handle, None);
        assert!(handled.lock().expect("handled updates").is_empty());
        assert_eq!(history.entries().len(), 1);
        assert_eq!(store.calls().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn process_update_with_state_store_skips_go_unprocessed_update_after_state()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let terminal = Arc::new(FailingUpdateHandler);
        let skipped = crate::skipped::SkippedUpdateHandler::new(terminal);

        let report = process_update_with_state_store_at(
            sample_poll_update_with_id(12348)?,
            UpdateConsumerConfig {
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                ..UpdateConsumerConfig::default()
            },
            unix_time(1_710_000_010),
            &store,
            |update| skipped.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 12348);
        assert_eq!(report.update_name, "poll");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        Ok(())
    }

    #[tokio::test]
    async fn process_update_with_state_and_history_store_keeps_history_failures_nonfatal()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let history = HistoryStoreStub::with_inbound_failure("history unavailable");
        let handled = Arc::new(Mutex::new(Vec::new()));
        let handled_updates = Arc::clone(&handled);

        let report = process_update_with_state_and_history_store_at(
            sample_message_update()?,
            UpdateConsumerConfig {
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(5 * 60),
                ..UpdateConsumerConfig::default()
            },
            unix_time(1_710_000_300),
            &store,
            &history,
            0,
            move |update| {
                let handled = Arc::clone(&handled_updates);
                async move {
                    handled
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await;

        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert!(report.skipped_handle);
        assert_eq!(report.handle, None);
        assert!(handled.lock().expect("handled updates").is_empty());
        assert!(history.entries().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn process_update_with_state_and_history_store_leaves_fresh_history_to_handler()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let history = HistoryStoreStub::default();
        let handled = Arc::new(Mutex::new(Vec::new()));
        let handled_updates = Arc::clone(&handled);

        let report = process_update_with_state_and_history_store_at(
            sample_message_update()?,
            UpdateConsumerConfig {
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(400_000_000),
                ..UpdateConsumerConfig::default()
            },
            unix_time(1_710_000_010),
            &store,
            &history,
            0,
            move |update| {
                let handled = Arc::clone(&handled_updates);
                async move {
                    handled
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?
                        .push(update.id);
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await;

        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert_eq!(
            handled.lock().expect("handled updates").as_slice(),
            &[12345]
        );
        assert!(history.entries().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn run_update_consumer_until_processes_updates_and_reports_stage_outcomes()
    -> Result<(), Box<dyn Error>> {
        let source = Arc::new(SourceStub::new(vec![
            SourceAction::Update(Box::new(sample_message_update_with_id(12345)?)),
            SourceAction::Update(Box::new(sample_message_update_with_id(12346)?)),
        ]));
        let store = Arc::new(StoreStub::default());
        let handler = Arc::new(HandlerStub::default());

        let report = run_update_consumer_until(
            Arc::clone(&source),
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(400_000_000),
                worker_limit: 2,
            },
            Arc::clone(&store),
            Arc::clone(&handler),
            handler.wait_for_calls(2),
        )
        .await;

        assert_eq!(report.dequeued, 2);
        assert_eq!(report.processed, 2);
        assert_eq!(report.state_failures, 0);
        assert_eq!(report.handle_failures, 0);
        assert_eq!(report.timeouts, 0);
        assert_eq!(handler.calls(), vec![12345, 12346]);
        assert_eq!(store.calls().len(), 4);
        assert!(
            source
                .timeouts()
                .iter()
                .all(|timeout| *timeout == Duration::from_millis(1))
        );
        Ok(())
    }

    #[tokio::test]
    async fn run_update_consumer_until_continues_after_dequeue_errors() -> Result<(), Box<dyn Error>>
    {
        let source = Arc::new(SourceStub::new(vec![
            SourceAction::Error("temporary redis error"),
            SourceAction::Update(Box::new(sample_message_update()?)),
        ]));
        let store = Arc::new(StoreStub::default());
        let handler = Arc::new(HandlerStub::default());

        let report = run_update_consumer_until(
            source,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(400_000_000),
                worker_limit: 2,
            },
            store,
            Arc::clone(&handler),
            handler.wait_for_calls(1),
        )
        .await;

        assert_eq!(report.dequeued, 1);
        assert_eq!(report.processed, 1);
        assert_eq!(
            report.dequeue_errors,
            vec!["temporary redis error".to_owned()]
        );
        assert_eq!(handler.calls(), vec![12345]);
        Ok(())
    }

    #[tokio::test]
    async fn run_update_consumer_until_coalesces_buffered_join_kick_without_state_or_handler()
    -> Result<(), Box<dyn Error>> {
        let source = Arc::new(SourceStub::new(vec![
            SourceAction::Update(Box::new(sample_chat_member_update(
                12350, 7, "left", "member",
            )?)),
            SourceAction::Update(Box::new(sample_chat_member_update(
                12351, 7, "member", "kicked",
            )?)),
        ]));
        let store = Arc::new(StoreStub::default());
        let handler = Arc::new(HandlerStub::default());

        let report = run_update_consumer_until(
            source,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(400_000_000),
                worker_limit: 2,
            },
            Arc::clone(&store),
            Arc::clone(&handler),
            async {
                tokio::time::sleep(Duration::from_millis(25)).await;
            },
        )
        .await;

        assert_eq!(report.dequeued, 2);
        assert_eq!(report.coalesced_member_updates, 2);
        assert_eq!(report.processed, 0);
        assert!(store.calls().is_empty());
        assert!(handler.calls().is_empty());
        Ok(())
    }

    #[test]
    fn buffered_member_coalescing_preserves_active_to_inactive_delete() -> Result<(), Box<dyn Error>>
    {
        let update = sample_chat_member_update(12352, 7, "member", "left")?;
        let mut report = super::UpdateConsumerRunReport::default();

        let updates = super::coalesce_buffered_member_updates(vec![update], &mut report);

        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].id, 12352);
        assert_eq!(report.coalesced_member_updates, 0);
        Ok(())
    }

    #[test]
    fn buffered_member_coalescing_does_not_collapse_admin_or_bot_sensitive_updates()
    -> Result<(), Box<dyn Error>> {
        let admin_update = sample_chat_member_update(12353, 7, "member", "administrator")?;
        let bot_update = sample_bot_chat_member_update(12354, 8, "left", "member")?;
        let mut report = super::UpdateConsumerRunReport::default();

        let updates =
            super::coalesce_buffered_member_updates(vec![admin_update, bot_update], &mut report);

        assert_eq!(
            updates.iter().map(|update| update.id).collect::<Vec<_>>(),
            vec![12353, 12354]
        );
        assert_eq!(report.coalesced_member_updates, 0);
        Ok(())
    }

    #[tokio::test]
    async fn run_update_consumer_until_can_wrap_handler_with_nonfatal_history()
    -> Result<(), Box<dyn Error>> {
        let source = Arc::new(SourceStub::new(vec![SourceAction::Update(Box::new(
            sample_message_update()?,
        ))]));
        let store = Arc::new(StoreStub::default());
        let history = Arc::new(HistoryStoreStub::with_inbound_failure(
            "history unavailable",
        ));
        let inner_handler = Arc::new(HandlerStub::default());
        let handler = Arc::new(UpdateHandlerWithHistory::new(
            Arc::clone(&history),
            Arc::clone(&inner_handler),
            0,
        ));

        let report = run_update_consumer_until(
            source,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(400_000_000),
                worker_limit: 2,
            },
            store,
            handler,
            inner_handler.wait_for_calls(1),
        )
        .await;

        assert_eq!(report.dequeued, 1);
        assert_eq!(report.processed, 1);
        assert_eq!(report.handle_failures, 0);
        assert_eq!(inner_handler.calls(), vec![12345]);
        assert!(history.entries().is_empty());
        Ok(())
    }

    #[derive(Clone, Default)]
    struct StoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl StoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("store calls").clone()
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedHistoryUpsert {
        bucket_day: time::Date,
        chat_id: i64,
        thread_id: i32,
        message_id: i32,
        entry_id: String,
        kind: String,
        role: String,
        occurred_at: time::OffsetDateTime,
        sender_id: i64,
        payload: Vec<u8>,
    }

    #[derive(Clone, Default)]
    struct HistoryStoreStub {
        entries: Arc<Mutex<Vec<RecordedHistoryUpsert>>>,
        edits: Arc<Mutex<Vec<RecordedHistoryEdit>>>,
        fail_next_inbound: Arc<Mutex<Option<&'static str>>>,
    }

    impl HistoryStoreStub {
        fn with_inbound_failure(message: &'static str) -> Self {
            Self {
                fail_next_inbound: Arc::new(Mutex::new(Some(message))),
                ..Self::default()
            }
        }

        fn entries(&self) -> Vec<RecordedHistoryUpsert> {
            self.entries.lock().expect("history entries").clone()
        }

        fn edits(&self) -> Vec<RecordedHistoryEdit> {
            self.edits.lock().expect("history edits").clone()
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct RecordedHistoryEdit {
        chat_id: i64,
        message_id: i32,
        new_text: String,
        original_text: String,
        meta: ChatMessageMeta,
    }

    impl InboundHistoryStore for HistoryStoreStub {
        type Error = io::Error;

        fn upsert_inbound_history<'a>(
            &'a self,
            entry: InboundHistoryUpsert<'a>,
        ) -> InboundHistoryStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                if let Some(message) = self
                    .fail_next_inbound
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .take()
                {
                    return Err(io::Error::other(message));
                }
                self.entries
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(RecordedHistoryUpsert {
                        bucket_day: entry.bucket_day,
                        chat_id: entry.chat_id,
                        thread_id: entry.thread_id,
                        message_id: entry.message_id,
                        entry_id: entry.entry_id.to_owned(),
                        kind: entry.kind.to_owned(),
                        role: entry.role.to_owned(),
                        occurred_at: entry.occurred_at,
                        sender_id: entry.sender_id,
                        payload: entry.payload.to_vec(),
                    });
                Ok(())
            })
        }
    }

    impl EditedHistoryStore for HistoryStoreStub {
        type Error = io::Error;

        fn update_edited_message_history<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            new_text: &'a str,
            original_text: &'a str,
            meta: &'a ChatMessageMeta,
        ) -> EditedHistoryStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.edits
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(RecordedHistoryEdit {
                        chat_id,
                        message_id,
                        new_text: new_text.to_owned(),
                        original_text: original_text.to_owned(),
                        meta: meta.clone(),
                    });
                Ok(true)
            })
        }
    }

    impl UpdateStateStore for StoreStub {
        type Error = io::Error;

        fn upsert_chat_state<'a>(
            &'a self,
            chat: &'a ChatState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "chat:{}:{}:{}:{}",
                        chat.id,
                        chat.chat_type,
                        chat.first_name.as_deref().unwrap_or_default(),
                        chat.username.as_deref().unwrap_or_default()
                    ));
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
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "user:{}:{}:{}",
                        user.id,
                        user.first_name,
                        user.username.as_deref().unwrap_or_default()
                    ));
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
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "file:{}:{}:{}:{}:{:?}:{:?}:{:?}:{:?}:{:?}",
                        params.file_unique_id,
                        params.latest_file_id,
                        params.media_kind,
                        params.mime_type.as_deref().unwrap_or_default(),
                        params.width,
                        params.height,
                        params.file_size,
                        params.last_seen_chat_id,
                        params.last_seen_message_id
                    ));
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct SourceStub {
        actions: Arc<Mutex<VecDeque<SourceAction>>>,
        timeouts: Arc<Mutex<Vec<Duration>>>,
    }

    impl SourceStub {
        fn new(actions: Vec<SourceAction>) -> Self {
            Self {
                actions: Arc::new(Mutex::new(actions.into())),
                timeouts: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn timeouts(&self) -> Vec<Duration> {
            self.timeouts.lock().expect("source timeouts").clone()
        }
    }

    enum SourceAction {
        Update(Box<TelegramUpdate>),
        Error(&'static str),
    }

    impl UpdateSource for SourceStub {
        type Error = io::Error;

        fn dequeue_update<'a>(&'a self, timeout: Duration) -> UpdateSourceFuture<'a, Self::Error> {
            Box::pin(async move {
                self.timeouts
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(timeout);
                let action = self
                    .actions
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .pop_front();
                match action {
                    Some(SourceAction::Update(update)) => Ok(Some(*update)),
                    Some(SourceAction::Error(message)) => Err(io::Error::other(message)),
                    None => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        Ok(None)
                    }
                }
            })
        }

        fn dequeue_updates<'a>(
            &'a self,
            timeout: Duration,
            max_count: usize,
        ) -> UpdateSourceBatchFuture<'a, Self::Error> {
            Box::pin(async move {
                self.timeouts
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(timeout);
                let updates = {
                    let mut updates = Vec::new();
                    let mut actions = self
                        .actions
                        .lock()
                        .map_err(|err| io::Error::other(err.to_string()))?;
                    while updates.len() < max_count.max(1) {
                        match actions.pop_front() {
                            Some(SourceAction::Update(update)) => updates.push(*update),
                            Some(SourceAction::Error(message)) if updates.is_empty() => {
                                return Err(io::Error::other(message));
                            }
                            Some(SourceAction::Error(message)) => {
                                actions.push_front(SourceAction::Error(message));
                                break;
                            }
                            None => break,
                        }
                    }
                    updates
                };
                if updates.is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok(updates)
            })
        }
    }

    #[derive(Clone, Default)]
    struct HandlerStub {
        calls: Arc<Mutex<Vec<i64>>>,
        notify: Arc<Notify>,
    }

    impl HandlerStub {
        fn calls(&self) -> Vec<i64> {
            self.calls.lock().expect("handler calls").clone()
        }

        async fn wait_for_calls(&self, expected: usize) {
            loop {
                if self.calls.lock().expect("handler calls").len() >= expected {
                    return;
                }
                self.notify.notified().await;
            }
        }
    }

    impl UpdateHandler for HandlerStub {
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
                self.notify.notify_waiters();
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug)]
    struct FailingUpdateHandler;

    impl UpdateHandler for FailingUpdateHandler {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                Err(io::Error::other(format!(
                    "unexpected delegated update {}",
                    update.id
                )))
            })
        }
    }

    fn sample_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        sample_message_update_with_id(12345)
    }

    fn sample_message_update_with_id(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
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
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": "/start hello"
            }
        }))
    }

    fn sample_chat_member_update(
        update_id: i64,
        user_id: i64,
        old_status: &str,
        new_status: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "chat_member": {
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Admin"
                },
                "date": 1_710_000_000,
                "old_chat_member": chat_member_json(user_id, old_status),
                "new_chat_member": chat_member_json(user_id, new_status)
            }
        }))
    }

    fn sample_bot_chat_member_update(
        update_id: i64,
        user_id: i64,
        old_status: &str,
        new_status: &str,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "chat_member": {
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Admin"
                },
                "date": 1_710_000_000,
                "old_chat_member": chat_member_json_for_user(user_id, old_status, true),
                "new_chat_member": chat_member_json_for_user(user_id, new_status, true)
            }
        }))
    }

    fn chat_member_json(user_id: i64, status: &str) -> serde_json::Value {
        chat_member_json_for_user(user_id, status, false)
    }

    fn chat_member_json_for_user(user_id: i64, status: &str, is_bot: bool) -> serde_json::Value {
        let user = json!({
            "id": user_id,
            "is_bot": is_bot,
            "first_name": "Tracked"
        });
        match status {
            "kicked" => json!({
                "status": "kicked",
                "until_date": 0,
                "user": user
            }),
            "left" => json!({
                "status": "left",
                "user": user
            }),
            "member" => json!({
                "status": "member",
                "user": user
            }),
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
                "can_manage_topics": false
            }),
            other => panic!("unsupported sample member status {other}"),
        }
    }

    fn sample_poll_update_with_id(update_id: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": update_id,
            "poll": {
                "type": "regular",
                "allows_multiple_answers": false,
                "allows_revoting": false,
                "id": "poll-id",
                "is_anonymous": true,
                "is_closed": true,
                "members_only": false,
                "options": [
                    {
                        "persistent_id": "1",
                        "text": "Yes",
                        "voter_count": 1000
                    },
                    {
                        "persistent_id": "2",
                        "text": "No",
                        "voter_count": 0
                    }
                ],
                "question": "Rust?",
                "total_voter_count": 1000
            }
        }))
    }

    fn sample_edited_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12346,
            "edited_message": {
                "message_id": 77,
                "date": 1_710_000_000,
                "edit_date": 1_710_000_010,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "text": " edited text "
            }
        }))
    }

    fn sample_caption_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 12347,
            "message": {
                "message_id": 78,
                "date": 1_710_000_000,
                "chat": {
                    "id": 42,
                    "type": "private",
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "from": {
                    "id": 99,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada_l"
                },
                "caption": " photo caption ",
                "photo": [
                    {
                        "file_id": "photo-small",
                        "file_unique_id": "photo-small-u",
                        "width": 1,
                        "height": 1
                    },
                    {
                        "file_id": "photo-large",
                        "file_unique_id": "photo-large-u",
                        "width": 1024,
                        "height": 768
                    }
                ]
            }
        }))
    }

    fn sample_guest_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 123,
            "guest_message": {
                "message_id": 55,
                "date": 1_710_000_000,
                "chat": {
                    "id": -100,
                    "type": "supergroup",
                    "title": "Team",
                    "is_forum": true
                },
                "guest_query_id": "guest-query",
                "text": "hello"
            }
        }))
    }

    fn unix_time(seconds: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(seconds)
    }
}
