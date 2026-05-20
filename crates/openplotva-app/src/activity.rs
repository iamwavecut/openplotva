//! App-level message activity tracking.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{Message as TelegramMessage, Update as TelegramUpdate, UpdateType};
use openplotva_core::{SENDER_TYPE_USER, UserState};
use openplotva_updates::resolve_message_sender;
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

/// Boxed future returned by activity storage calls.
pub type ActivityStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Store boundary for Go `trackUserActivity`.
pub trait MessageActivityStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Update Go `chat_members.last_message_at`.
    fn update_member_last_message<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> ActivityStoreFuture<'a, (), Self::Error>;

    /// Upsert Go `chat_active_users`.
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

/// Route chosen by Go message-activity tracking.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum MessageActivityRoute {
    /// The update was not a user message that Go tracks.
    #[default]
    Skipped,
    /// A user message was tracked.
    Tracked(MessageActivityReport),
}

/// Side-effect report for one Go `trackUserActivity` run.
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

/// `UpdateHandler` adapter that tracks Go message activity before downstream routing.
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

/// Track Go message activity, then always delegate update handling.
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

/// Run Go `trackMessageSenderActivity`/`trackUserActivity` for one decoded update.
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
        error::Error,
        io,
        sync::{Arc, Mutex},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_core::UserState;
    use serde_json::json;

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};

    use super::{
        ActivityStoreFuture, MessageActivityRoute, MessageActivityStore,
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
