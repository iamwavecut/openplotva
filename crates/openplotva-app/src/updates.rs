//! App-level Telegram update consumer glue.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime},
};

use carapax::types::Update as TelegramUpdate;
use openplotva_core::{ChatState, UserState};
use openplotva_updates::{
    UpdateConsumerConfig, UpdateProcessReport, UpdateStageOutcome, extract_update_state,
    should_skip_side_effects_at,
};
use thiserror::Error;
use tokio::{sync::Semaphore, task::JoinSet};

/// Boxed future returned by update sources.
pub type UpdateSourceFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<TelegramUpdate>, E>> + Send + 'a>>;

/// Boxed future returned by update handlers.
pub type UpdateHandlerFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by update state storage calls.
pub type UpdateStateStoreFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Source of decoded Telegram updates for the app-level consumer loop.
pub trait UpdateSource {
    /// Error returned by the concrete update source.
    type Error: fmt::Display;

    /// Dequeue one decoded update, returning `None` for a timeout or empty poll.
    fn dequeue_update<'a>(&'a self, timeout: Duration) -> UpdateSourceFuture<'a, Self::Error>;
}

impl UpdateSource for openplotva_updates::RedisUpdateQueue {
    type Error = openplotva_updates::UpdateQueueError;

    fn dequeue_update<'a>(&'a self, timeout: Duration) -> UpdateSourceFuture<'a, Self::Error> {
        Box::pin(openplotva_updates::RedisUpdateQueue::dequeue_update(
            self, timeout,
        ))
    }
}

/// User-visible Telegram update handler plugged in once fetcher routing is ported.
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
}

/// Report for the Go consumer state-persistence stage.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UpdateStatePersistenceReport {
    /// Whether a chat row was upserted.
    pub chat_persisted: bool,
    /// Whether a user row was upserted.
    pub user_persisted: bool,
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

/// Persist the chat/user state that Go writes before user-visible side effects.
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
    Ok(report)
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

/// Summary for one app-level update consumer run.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateConsumerRunReport {
    /// Updates popped from the source.
    pub dequeued: usize,
    /// Empty polls/timeouts observed while waiting for updates.
    pub empty_polls: usize,
    /// Dequeue errors observed; the loop continues after these like Go logs and continues.
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
    let worker_limit = config.worker_limit.max(1);
    let semaphore = Arc::new(Semaphore::new(worker_limit));
    let mut workers = JoinSet::new();
    let mut report = UpdateConsumerRunReport::default();
    tokio::pin!(stop);

    loop {
        tokio::select! {
            _ = &mut stop => break,
            dequeued = source.dequeue_update(config.dequeue_timeout) => {
                match dequeued {
                    Ok(Some(update)) => {
                        report.dequeued += 1;
                        let now = SystemTime::now();
                        let permit_count = stage_permits_for_update(&update, config, now, worker_limit);
                        let permits = tokio::select! {
                            _ = &mut stop => break,
                            acquired = semaphore.clone().acquire_many_owned(permit_count) => {
                                match acquired {
                                    Ok(permits) => permits,
                                    Err(_) => break,
                                }
                            }
                        };

                        let store = Arc::clone(&store);
                        let handler = Arc::clone(&handler);
                        workers.spawn(async move {
                            let _permits = permits;
                            process_update_with_state_store_at(
                                update,
                                config,
                                now,
                                store.as_ref(),
                                |update| handler.handle_update(update),
                            )
                            .await
                        });
                    }
                    Ok(None) => report.empty_polls += 1,
                    Err(error) => report.dequeue_errors.push(error.to_string()),
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
    use openplotva_core::{ChatState, UserState};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;
    use tokio::sync::Notify;

    use super::{
        UpdateHandler, UpdateHandlerFuture, UpdateSource, UpdateSourceFuture, UpdateStateStore,
        UpdateStateStoreFuture, persist_update_state, process_update_with_state_store_at,
        run_update_consumer_until,
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

    #[derive(Clone, Default)]
    struct StoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl StoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("store calls").clone()
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
