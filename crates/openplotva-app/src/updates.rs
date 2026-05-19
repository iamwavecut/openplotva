//! App-level Telegram update consumer glue.

use std::{fmt, future::Future, pin::Pin, time::SystemTime};

use carapax::types::Update as TelegramUpdate;
use openplotva_core::{ChatState, UserState};
use openplotva_updates::{UpdateConsumerConfig, UpdateProcessReport, extract_update_state};
use thiserror::Error;

/// Storage capability needed by the app-level update consumer state stage.
pub trait UpdateStateStore {
    /// Error returned by the concrete state store.
    type Error: fmt::Display;

    /// Persist chat state extracted from a Telegram update.
    fn upsert_chat_state<'a>(
        &'a self,
        chat: &'a ChatState,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;

    /// Persist user state extracted from a Telegram update.
    fn upsert_user_state<'a>(
        &'a self,
        user: &'a UserState,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>>;
}

impl UpdateStateStore for openplotva_storage::PostgresVirtualMessageStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_chat_state<'a>(
        &'a self,
        chat: &'a ChatState,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
        Box::pin(openplotva_storage::PostgresVirtualMessageStore::upsert_chat_state(self, chat))
    }

    fn upsert_user_state<'a>(
        &'a self,
        user: &'a UserState,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
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

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        future::Future,
        io,
        pin::Pin,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::{ChatState, UserState};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;

    use super::{UpdateStateStore, persist_update_state, process_update_with_state_store_at};

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
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
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
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
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

    fn sample_message_update() -> Result<TelegramUpdate, serde_json::Error> {
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
