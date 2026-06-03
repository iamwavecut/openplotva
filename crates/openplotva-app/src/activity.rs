//! App-level message activity tracking.

use std::{
    collections::HashSet,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use carapax::types::{Message as TelegramMessage, Update as TelegramUpdate, UpdateType};
use openplotva_core::{SENDER_TYPE_USER, UserState};
use openplotva_updates::resolve_message_sender;
use thiserror::Error;
use tokio::{sync::watch, task::JoinHandle};

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

/// Boxed future returned by activity storage calls.
pub type ActivityStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub const MESSAGE_ACTIVITY_BUFFER_FLUSH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MessageActivityKey {
    pub chat_id: i64,
    pub user_id: i64,
}

pub trait MessageActivityBatchWriter {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    fn flush_member_last_messages<'a>(
        &'a self,
        keys: &'a [MessageActivityKey],
    ) -> ActivityStoreFuture<'a, (), Self::Error>;

    fn flush_chat_active_users<'a>(
        &'a self,
        keys: &'a [MessageActivityKey],
    ) -> ActivityStoreFuture<'a, (), Self::Error>;

    fn upsert_activity_user<'a>(
        &'a self,
        user: UserState,
    ) -> ActivityStoreFuture<'a, (), Self::Error>;
}

pub trait MessageActivityStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    fn update_member_last_message<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error>;

    fn upsert_chat_active_user<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error>;

    /// Persist the user before retrying the active-user write.
    fn upsert_activity_user<'a>(
        &'a self,
        user: UserState,
    ) -> ActivityStoreFuture<'a, (), Self::Error>;
}

impl MessageActivityStore for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn update_member_last_message<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.update_member_last_message(chat_id, user_id).await })
    }

    fn upsert_chat_active_user<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_chat_active_user(chat_id, user_id).await })
    }

    fn upsert_activity_user<'a>(
        &'a self,
        user: UserState,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_user_state(&user).await })
    }
}

impl MessageActivityBatchWriter for openplotva_storage::PostgresChatMemberStore {
    type Error = openplotva_storage::StorageError;

    fn flush_member_last_messages<'a>(
        &'a self,
        keys: &'a [MessageActivityKey],
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        let pairs = keys
            .iter()
            .map(|key| (key.chat_id, key.user_id))
            .collect::<Vec<_>>();
        Box::pin(async move { self.update_member_last_messages(&pairs).await })
    }

    fn flush_chat_active_users<'a>(
        &'a self,
        keys: &'a [MessageActivityKey],
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        let pairs = keys
            .iter()
            .map(|key| (key.chat_id, key.user_id))
            .collect::<Vec<_>>();
        Box::pin(async move { self.upsert_chat_active_users(&pairs).await })
    }

    fn upsert_activity_user<'a>(
        &'a self,
        user: UserState,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { self.upsert_user_state(&user).await })
    }
}

pub struct BufferedMessageActivityStore<Writer> {
    writer: Arc<Writer>,
    pending: Arc<Mutex<BufferedMessageActivityPending>>,
}

impl<Writer> Clone for BufferedMessageActivityStore<Writer> {
    fn clone(&self) -> Self {
        Self {
            writer: Arc::clone(&self.writer),
            pending: Arc::clone(&self.pending),
        }
    }
}

#[derive(Debug, Default)]
struct BufferedMessageActivityPending {
    member_last_messages: HashSet<MessageActivityKey>,
    active_users: HashSet<MessageActivityKey>,
}

impl<Writer> BufferedMessageActivityStore<Writer> {
    pub fn new(writer: Arc<Writer>) -> Self {
        Self {
            writer,
            pending: Arc::new(Mutex::new(BufferedMessageActivityPending::default())),
        }
    }

    pub fn spawn(writer: Arc<Writer>, stop: watch::Receiver<bool>) -> (Self, JoinHandle<()>)
    where
        Writer: MessageActivityBatchWriter + Send + Sync + 'static,
    {
        let store = Self::new(writer);
        let worker_store = store.clone();
        let handle = tokio::spawn(run_buffered_message_activity_writer(
            worker_store,
            stop,
            MESSAGE_ACTIVITY_BUFFER_FLUSH_INTERVAL,
        ));
        (store, handle)
    }

    pub async fn flush_pending(&self) -> Result<(), Writer::Error>
    where
        Writer: MessageActivityBatchWriter,
    {
        let (member_last_messages, active_users) = self.take_pending();
        if member_last_messages.is_empty() && active_users.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.flush_member_last_messages(&member_last_messages).await {
            self.requeue_pending(member_last_messages, active_users);
            return Err(error);
        }
        if let Err(error) = self.flush_chat_active_users(&active_users).await {
            self.requeue_pending(Vec::new(), active_users);
            return Err(error);
        }
        Ok(())
    }

    async fn flush_member_last_messages(
        &self,
        member_last_messages: &[MessageActivityKey],
    ) -> Result<(), Writer::Error>
    where
        Writer: MessageActivityBatchWriter,
    {
        if member_last_messages.is_empty() {
            return Ok(());
        }
        self.writer
            .flush_member_last_messages(member_last_messages)
            .await
    }

    async fn flush_chat_active_users(
        &self,
        active_users: &[MessageActivityKey],
    ) -> Result<(), Writer::Error>
    where
        Writer: MessageActivityBatchWriter,
    {
        if active_users.is_empty() {
            return Ok(());
        }
        self.writer.flush_chat_active_users(active_users).await
    }

    fn enqueue_member_last_message(&self, key: MessageActivityKey) {
        self.lock_pending().member_last_messages.insert(key);
    }

    fn enqueue_active_user(&self, key: MessageActivityKey) {
        self.lock_pending().active_users.insert(key);
    }

    fn take_pending(&self) -> (Vec<MessageActivityKey>, Vec<MessageActivityKey>) {
        let mut pending = self.lock_pending();
        let mut member_last_messages = pending.member_last_messages.drain().collect::<Vec<_>>();
        let mut active_users = pending.active_users.drain().collect::<Vec<_>>();
        member_last_messages.sort_unstable();
        active_users.sort_unstable();
        (member_last_messages, active_users)
    }

    fn requeue_pending(
        &self,
        member_last_messages: Vec<MessageActivityKey>,
        active_users: Vec<MessageActivityKey>,
    ) {
        let mut pending = self.lock_pending();
        pending.member_last_messages.extend(member_last_messages);
        pending.active_users.extend(active_users);
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, BufferedMessageActivityPending> {
        self.pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl<Writer> MessageActivityStore for BufferedMessageActivityStore<Writer>
where
    Writer: MessageActivityBatchWriter + Send + Sync,
{
    type Error = Writer::Error;

    fn update_member_last_message<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        self.enqueue_member_last_message(MessageActivityKey { chat_id, user_id });
        Box::pin(std::future::ready(Ok(())))
    }

    fn upsert_chat_active_user<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        self.enqueue_active_user(MessageActivityKey { chat_id, user_id });
        Box::pin(std::future::ready(Ok(())))
    }

    fn upsert_activity_user<'a>(
        &'a self,
        user: UserState,
    ) -> ActivityStoreFuture<'a, (), Self::Error> {
        self.writer.upsert_activity_user(user)
    }
}

async fn run_buffered_message_activity_writer<Writer>(
    store: BufferedMessageActivityStore<Writer>,
    mut stop: watch::Receiver<bool>,
    flush_interval: Duration,
) where
    Writer: MessageActivityBatchWriter + Send + Sync + 'static,
{
    let flush_interval = flush_interval.max(Duration::from_millis(1));
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if *stop.borrow() {
            flush_buffered_message_activity(&store).await;
            break;
        }

        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    flush_buffered_message_activity(&store).await;
                    break;
                }
            }
            _ = interval.tick() => {
                flush_buffered_message_activity(&store).await;
            }
        }
    }
}

async fn flush_buffered_message_activity<Writer>(store: &BufferedMessageActivityStore<Writer>)
where
    Writer: MessageActivityBatchWriter,
{
    if let Err(error) = store.flush_pending().await {
        tracing::warn!(%error, "failed to flush buffered message activity");
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum MessageActivityRoute {
    #[default]
    Skipped,
    /// A user message was tracked.
    Tracked(MessageActivityReport),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MessageActivityReport {
    pub chat_id: i64,
    pub user_id: i64,
    pub member_last_message_error: Option<String>,
    pub active_user_attempts: usize,
    pub first_active_user_error: Option<String>,
    pub user_persisted_after_active_error: bool,
    pub user_persist_error: Option<String>,
    pub retry_active_user_error: Option<String>,
}

/// Error returned when delegated handling fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MessageActivityError {
    /// Downstream handler failed after activity tracking.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct MessageActivityUpdateHandler<Store, Next> {
    store: Arc<Store>,
    next: Arc<Next>,
}

impl<Store, Next> MessageActivityUpdateHandler<Store, Next> {
    /// Build an activity handler around the real downstream update handler.
    pub fn new(store: Arc<Store>, next: Arc<Next>) -> Self {
        Self { store, next }
    }
}

impl<Store, Next> UpdateHandler for MessageActivityUpdateHandler<Store, Next>
where
    Store: MessageActivityStore + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = MessageActivityError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_message_activity_update_or_else(self.store.as_ref(), update, |update| {
                self.next.handle_update(update)
            })
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_message_activity_update_or_else<Store, HandleFn, HandleFuture, HandleError>(
    store: &Store,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<MessageActivityRoute, MessageActivityError>
where
    Store: MessageActivityStore + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let route = track_message_activity(store, &update).await;
    handle_other(update)
        .await
        .map_err(|error| MessageActivityError::Downstream {
            message: error.to_string(),
        })?;
    Ok(route)
}

pub async fn track_message_activity<Store>(
    store: &Store,
    update: &TelegramUpdate,
) -> MessageActivityRoute
where
    Store: MessageActivityStore + Sync,
{
    let UpdateType::Message(message) = &update.update_type else {
        return MessageActivityRoute::Skipped;
    };
    let Some((chat_id, user_id, user)) = activity_target(message) else {
        return MessageActivityRoute::Skipped;
    };

    let mut report = MessageActivityReport {
        chat_id,
        user_id,
        ..MessageActivityReport::default()
    };

    if let Err(error) = store.update_member_last_message(chat_id, user_id).await {
        let message = error.to_string();
        tracing::warn!(
            message,
            chat_id,
            user_id,
            "failed to update member last message"
        );
        report.member_last_message_error = Some(message);
    }

    report.active_user_attempts += 1;
    if let Err(error) = store.upsert_chat_active_user(chat_id, user_id).await {
        let message = error.to_string();
        tracing::warn!(message, chat_id, user_id, "failed to upsert active user");
        report.first_active_user_error = Some(message);

        if let Err(error) = store.upsert_activity_user(user_state(user)).await {
            let message = error.to_string();
            tracing::warn!(message, user_id, "failed to persist activity user");
            report.user_persist_error = Some(message);
        } else {
            report.user_persisted_after_active_error = true;
        }

        report.active_user_attempts += 1;
        if let Err(error) = store.upsert_chat_active_user(chat_id, user_id).await {
            let message = error.to_string();
            tracing::warn!(
                message,
                chat_id,
                user_id,
                "failed to retry active user upsert"
            );
            report.retry_active_user_error = Some(message);
        }
    }

    MessageActivityRoute::Tracked(report)
}

fn activity_target(message: &TelegramMessage) -> Option<(i64, i64, &carapax::types::User)> {
    let sender = resolve_message_sender(Some(message));
    if sender.sender_type != SENDER_TYPE_USER || sender.is_bot {
        return None;
    }
    let user = message.sender.get_user()?;
    let user_id = i64::from(user.id);
    if user_id == 0 || user.is_bot {
        return None;
    }
    let chat_id = message.chat.get_id().into();
    if chat_id == 0 {
        return None;
    }
    Some((chat_id, user_id, user))
}

fn user_state(user: &carapax::types::User) -> UserState {
    UserState::new(
        i64::from(user.id),
        user.first_name.clone(),
        user.last_name.clone(),
        user.username.as_ref().map(ToString::to_string),
        user.language_code.clone(),
        user.is_premium,
    )
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::UserState;
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };

    use super::{
        ActivityStoreFuture, BufferedMessageActivityStore, MessageActivityBatchWriter,
        MessageActivityKey, MessageActivityRoute, MessageActivityStore,
        MessageActivityUpdateHandler, handle_message_activity_update_or_else,
        track_message_activity,
    };

    #[tokio::test]
    async fn user_message_tracks_last_message_and_active_user() -> Result<(), Box<dyn Error>> {
        let store = ActivityStoreStub::default();
        let route = track_message_activity(&store, &message_update()?).await;

        assert_eq!(
            route,
            MessageActivityRoute::Tracked(super::MessageActivityReport {
                chat_id: 42,
                user_id: 111,
                active_user_attempts: 1,
                ..super::MessageActivityReport::default()
            })
        );
        assert_eq!(
            store.calls(),
            vec!["last:42:111".to_owned(), "active:42:111".to_owned(),]
        );
        Ok(())
    }

    #[tokio::test]
    async fn active_user_failure_persists_user_and_retries_like_go() -> Result<(), Box<dyn Error>> {
        let store = ActivityStoreStub::default().fail_first_active();
        let route = track_message_activity(&store, &message_update()?).await;
        let MessageActivityRoute::Tracked(report) = route else {
            panic!("activity should be tracked");
        };

        assert_eq!(report.active_user_attempts, 2);
        assert_eq!(
            report.first_active_user_error,
            Some("active failed".to_owned())
        );
        assert!(report.user_persisted_after_active_error);
        assert_eq!(report.retry_active_user_error, None);
        assert_eq!(
            store.calls(),
            vec![
                "last:42:111".to_owned(),
                "active:42:111".to_owned(),
                "user:111:Ada".to_owned(),
                "active:42:111".to_owned(),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn bot_and_channel_messages_skip_activity() -> Result<(), Box<dyn Error>> {
        let store = ActivityStoreStub::default();
        assert_eq!(
            track_message_activity(&store, &bot_message_update()?).await,
            MessageActivityRoute::Skipped
        );
        assert_eq!(
            track_message_activity(&store, &channel_sender_update()?).await,
            MessageActivityRoute::Skipped
        );
        assert_eq!(
            track_message_activity(&store, &zero_chat_update()?).await,
            MessageActivityRoute::Skipped
        );
        assert!(store.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn activity_handler_delegates_after_tracking() -> Result<(), Box<dyn Error>> {
        let store = Arc::new(ActivityStoreStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = MessageActivityUpdateHandler::new(Arc::clone(&store), Arc::clone(&next));

        handler.handle_update(message_update()?).await?;

        assert_eq!(next.handled_count(), 1);
        assert_eq!(
            store.calls(),
            vec!["last:42:111".to_owned(), "active:42:111".to_owned(),]
        );
        Ok(())
    }

    #[tokio::test]
    async fn activity_wrapper_delegates_non_message_once() -> Result<(), Box<dyn Error>> {
        let store = ActivityStoreStub::default();
        let next = UpdateHandlerStub::default();
        let route = handle_message_activity_update_or_else(&store, inline_update()?, |update| {
            next.handle_update(update)
        })
        .await?;

        assert_eq!(route, MessageActivityRoute::Skipped);
        assert_eq!(next.handled_count(), 1);
        assert!(store.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn buffered_activity_store_coalesces_repeated_writes_until_flush() {
        let writer = Arc::new(BatchActivityStoreStub::default());
        let store = BufferedMessageActivityStore::new(Arc::clone(&writer));

        store
            .update_member_last_message(42, 111)
            .await
            .expect("enqueue first last-message activity");
        store
            .update_member_last_message(42, 111)
            .await
            .expect("coalesce duplicate last-message activity");
        store
            .update_member_last_message(42, 222)
            .await
            .expect("enqueue second last-message activity");
        store
            .upsert_chat_active_user(42, 111)
            .await
            .expect("enqueue first active user");
        store
            .upsert_chat_active_user(42, 111)
            .await
            .expect("coalesce duplicate active user");

        assert!(writer.calls().is_empty());

        store.flush_pending().await.expect("flush pending activity");

        assert_eq!(
            writer.calls(),
            vec!["last:42:111,42:222".to_owned(), "active:42:111".to_owned()]
        );
    }

    #[tokio::test]
    async fn buffered_activity_store_empty_flush_is_noop() {
        let writer = Arc::new(BatchActivityStoreStub::default());
        let store = BufferedMessageActivityStore::new(Arc::clone(&writer));

        store.flush_pending().await.expect("empty flush");

        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn live_redis_decoded_activity_tracks_retries_and_delegates_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url.as_str())?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-activity:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue.enqueue_update(&message_update()?).await?;
        assert_eq!(update_queue.len().await?, 1);

        let store = Arc::new(ActivityStoreStub::default().fail_first_active());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = MessageActivityUpdateHandler::new(Arc::clone(&store), Arc::clone(&next));
        let state_store = UpdateStateStoreStub::default();
        let config = UpdateConsumerConfig {
            dequeue_timeout: Duration::from_millis(1),
            state_timeout: Duration::from_secs(1),
            handle_timeout: Duration::from_secs(1),
            side_effect_max_age: Duration::from_secs(60),
            worker_limit: 1,
        };
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded activity update"))?;
        let report = process_update_with_state_store_at(
            decoded,
            config,
            UNIX_EPOCH + Duration::from_secs(1_710_000_010),
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 501);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            vec!["chat:42:private".to_owned(), "user:111:Ada".to_owned()]
        );
        assert_eq!(
            store.calls(),
            vec![
                "last:42:111".to_owned(),
                "active:42:111".to_owned(),
                "user:111:Ada".to_owned(),
                "active:42:111".to_owned(),
            ]
        );
        assert_eq!(next.handled_count(), 1);
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[derive(Clone, Debug, Default)]
    struct BatchActivityStoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl BatchActivityStoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("batch calls").clone()
        }
    }

    impl MessageActivityBatchWriter for BatchActivityStoreStub {
        type Error = io::Error;

        fn flush_member_last_messages<'a>(
            &'a self,
            keys: &'a [MessageActivityKey],
        ) -> ActivityStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("batch calls")
                    .push(format!("last:{}", activity_key_list(keys)));
                Ok(())
            })
        }

        fn flush_chat_active_users<'a>(
            &'a self,
            keys: &'a [MessageActivityKey],
        ) -> ActivityStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("batch calls")
                    .push(format!("active:{}", activity_key_list(keys)));
                Ok(())
            })
        }

        fn upsert_activity_user<'a>(
            &'a self,
            user: UserState,
        ) -> ActivityStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("batch calls")
                    .push(format!("user:{}:{}", user.id, user.first_name));
                Ok(())
            })
        }
    }

    fn activity_key_list(keys: &[MessageActivityKey]) -> String {
        keys.iter()
            .map(|key| format!("{}:{}", key.chat_id, key.user_id))
            .collect::<Vec<_>>()
            .join(",")
    }

    #[derive(Clone, Debug, Default)]
    struct ActivityStoreStub {
        state: Arc<Mutex<ActivityStoreState>>,
    }

    #[derive(Debug, Default)]
    struct ActivityStoreState {
        calls: Vec<String>,
        fail_next_active: bool,
    }

    impl ActivityStoreStub {
        fn fail_first_active(self) -> Self {
            self.state.lock().expect("state").fail_next_active = true;
            self
        }

        fn calls(&self) -> Vec<String> {
            self.state.lock().expect("state").calls.clone()
        }
    }

    impl MessageActivityStore for ActivityStoreStub {
        type Error = io::Error;

        fn update_member_last_message<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> ActivityStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("state")
                    .calls
                    .push(format!("last:{chat_id}:{user_id}"));
                Ok(())
            })
        }

        fn upsert_chat_active_user<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> ActivityStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                state.calls.push(format!("active:{chat_id}:{user_id}"));
                if state.fail_next_active {
                    state.fail_next_active = false;
                    return Err(io::Error::other("active failed"));
                }
                Ok(())
            })
        }

        fn upsert_activity_user<'a>(
            &'a self,
            user: UserState,
        ) -> ActivityStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("state")
                    .calls
                    .push(format!("user:{}:{}", user.id, user.first_name));
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

    #[derive(Clone, Debug, Default)]
    struct UpdateHandlerStub {
        handled: Arc<Mutex<usize>>,
    }

    impl UpdateHandlerStub {
        fn handled_count(&self) -> usize {
            *self.handled.lock().expect("handled")
        }
    }

    impl UpdateHandler for UpdateHandlerStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            _update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                *self.handled.lock().expect("handled") += 1;
                Ok(())
            })
        }
    }

    fn message_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 501,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada",
                    "last_name": "Lovelace",
                    "username": "ada",
                    "language_code": "en",
                    "is_premium": true
                },
                "text": "hello"
            }
        }))
    }

    fn bot_message_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 502,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 42, "type": "private", "first_name": "Bot"},
                "from": {"id": 222, "is_bot": true, "first_name": "Bot"},
                "text": "hello"
            }
        }))
    }

    fn channel_sender_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 503,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": -10042, "type": "supergroup", "title": "Team"},
                "sender_chat": {"id": -10099, "type": "channel", "title": "News"},
                "text": "hello"
            }
        }))
    }

    fn zero_chat_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 504,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 0, "type": "private", "first_name": "Ada"},
                "from": {"id": 111, "is_bot": false, "first_name": "Ada"},
                "text": "hello"
            }
        }))
    }

    fn inline_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 505,
            "inline_query": {
                "id": "inline-1",
                "from": {"id": 111, "is_bot": false, "first_name": "Ada"},
                "query": "hello",
                "offset": ""
            }
        }))
    }
}
