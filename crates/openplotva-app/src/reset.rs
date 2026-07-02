//! App-level fetcher `/reset` command behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    Text as TelegramText, TextEntity as TelegramTextEntity,
    TextEntityPosition as TelegramTextEntityPosition, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType,
};
use openplotva_telegram::{ChatRef, DispatcherQueue, ReplyMessageRef, TextMessageRequest};
use thiserror::Error;
use time::OffsetDateTime;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueTextReport, QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory,
    queue_text_message_parts,
};

const RESET_COMMAND: &str = "reset";
const RESET_CONFIRMATION_TEXT: &str = "Все забыто!";
const RESET_CONFIRMATION_DELETE_AFTER: Duration = Duration::from_secs(60);

pub type ResetHistoryFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait ResetHistoryStore {
    /// Concrete storage error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Persist the reset cursor for a chat or one forum thread.
    fn reset_history_at<'a>(
        &'a self,
        chat_id: i64,
        thread_id: i32,
        reset_at: OffsetDateTime,
    ) -> ResetHistoryFuture<'a, Self::Error>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResetScope {
    Chat { chat_id: i64 },
    Thread { chat_id: i64, thread_id: i32 },
}

impl ResetScope {
    fn chat_id(self) -> i64 {
        match self {
            Self::Chat { chat_id } | Self::Thread { chat_id, .. } => chat_id,
        }
    }

    fn thread_id(self) -> i32 {
        match self {
            Self::Chat { .. } => 0,
            Self::Thread { thread_id, .. } => thread_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResetCommandOutcome {
    /// Update was not a reset command for this bot and went downstream.
    Delegated,
    /// History cursor was written or attempted, and confirmation was queued.
    Reset {
        scope: ResetScope,
        history_error: Option<String>,
        confirmation: QueueTextReport,
    },
    ConfirmationError {
        scope: ResetScope,
        history_error: Option<String>,
        message: String,
    },
}

/// Fatal errors from the reset update wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ResetCommandError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

/// Boundaries used by one reset-command update pass.
#[derive(Clone, Copy, Debug)]
pub struct ResetCommandPorts<'a, History> {
    pub history: &'a History,
    /// Outbound dispatcher queue for the confirmation.
    pub dispatcher_queue: &'a DispatcherQueue,
}

/// Runtime context for one reset-command update pass.
#[derive(Clone, Copy, Debug)]
pub struct ResetCommandContext<'a> {
    /// Current bot username for group command targeting.
    pub bot_username: &'a str,
    /// Reset cursor timestamp.
    pub now: OffsetDateTime,
}

#[derive(Clone)]
pub struct ResetCommandUpdateHandler<History, Next> {
    history: Arc<History>,
    dispatcher_queue: Arc<DispatcherQueue>,
    bot_username: String,
    next_virtual_id: VirtualIdFactory,
    next: Arc<Next>,
}

impl<History, Next> ResetCommandUpdateHandler<History, Next> {
    /// Build a reset handler around the real downstream update handler.
    pub fn new(
        history: Arc<History>,
        dispatcher_queue: Arc<DispatcherQueue>,
        bot_username: impl Into<String>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            history,
            dispatcher_queue,
            bot_username: bot_username.into(),
            next_virtual_id: monotonic_virtual_id_factory("reset-vmsg"),
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

impl<History, Next> UpdateHandler for ResetCommandUpdateHandler<History, Next>
where
    History: ResetHistoryStore + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = ResetCommandError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_reset_command_update_or_else_at(
                ResetCommandPorts {
                    history: self.history.as_ref(),
                    dispatcher_queue: self.dispatcher_queue.as_ref(),
                },
                update,
                ResetCommandContext {
                    bot_username: &self.bot_username,
                    now: OffsetDateTime::now_utc(),
                },
                || (self.next_virtual_id)(),
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_reset_command_update_or_else_at<
    History,
    NextId,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    ports: ResetCommandPorts<'_, History>,
    update: TelegramUpdate,
    context: ResetCommandContext<'_>,
    next_virtual_id: NextId,
    handle_other: HandleFn,
) -> Result<ResetCommandOutcome, ResetCommandError>
where
    History: ResetHistoryStore + Sync,
    NextId: FnMut() -> String,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| ResetCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(ResetCommandOutcome::Delegated);
    };

    if !is_reset_command_for_bot(message, context.bot_username) {
        handle_other(update)
            .await
            .map_err(|error| ResetCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(ResetCommandOutcome::Delegated);
    }

    let scope = reset_scope(message);
    let history_error = ports
        .history
        .reset_history_at(scope.chat_id(), scope.thread_id(), context.now)
        .await
        .err()
        .map(|error| error.to_string());

    match queue_reset_confirmation(ports.dispatcher_queue, message, next_virtual_id).await {
        Ok(confirmation) => Ok(ResetCommandOutcome::Reset {
            scope,
            history_error,
            confirmation,
        }),
        Err(error) => Ok(ResetCommandOutcome::ConfirmationError {
            scope,
            history_error,
            message: error.to_string(),
        }),
    }
}

#[must_use]
pub fn is_reset_command_for_bot(message: &TelegramMessage, bot_username: &str) -> bool {
    let Some(command) = leading_bot_command(message) else {
        return false;
    };
    if command.command != RESET_COMMAND {
        return false;
    }
    if telegram_chat_is_private(&message.chat) {
        return true;
    }
    command.target.as_deref() == Some(bot_username)
}

#[must_use]
pub fn reset_scope(message: &TelegramMessage) -> ResetScope {
    let chat_id = message.chat.get_id().into();
    match message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0)
    {
        Some(thread_id) => ResetScope::Thread { chat_id, thread_id },
        None => ResetScope::Chat { chat_id },
    }
}

pub async fn queue_reset_confirmation<NextId>(
    dispatcher_queue: &DispatcherQueue,
    message: &TelegramMessage,
    next_virtual_id: NextId,
) -> Result<QueueTextReport, openplotva_telegram::OutboundBuildError>
where
    NextId: FnMut() -> String,
{
    let chat = message_chat_ref(message);
    let request = TextMessageRequest {
        chat: Some(chat),
        message_thread_id: 0,
        disable_notification: false,
        allow_sending_without_reply: None,
        text: RESET_CONFIRMATION_TEXT.to_owned(),
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
        dispatcher_queue,
        QueueTextRequest {
            protected: false,
            debounce_key: None,
            message: &request,
            reply_to: Some(&reply_to),
            immediate_first: true,
            bypass_chat_restrictions: false,
            ephemeral_delete_after: Some(RESET_CONFIRMATION_DELETE_AFTER),
        },
        next_virtual_id,
    )
    .await
}

fn message_chat_ref(message: &TelegramMessage) -> ChatRef {
    ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    }
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

impl ResetHistoryStore for openplotva_storage::PostgresHistoryStore {
    type Error = openplotva_storage::StorageError;

    fn reset_history_at<'a>(
        &'a self,
        chat_id: i64,
        thread_id: i32,
        reset_at: OffsetDateTime,
    ) -> ResetHistoryFuture<'a, Self::Error> {
        Box::pin(async move {
            self.reset_history_at(chat_id, thread_id, reset_at)
                .await
                .map(|_| ())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_telegram::{DispatcherConfig, TelegramOutboundMethodKind};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use std::{
        env, io,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateStateStore, UpdateStateStoreFuture,
        process_update_with_state_store_at,
    };

    fn sample_message(value: serde_json::Value) -> Result<TelegramMessage, serde_json::Error> {
        serde_json::from_value(value)
    }

    fn command_message(
        chat: serde_json::Value,
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message_at(chat, text, command_len, thread_id, 1_710_000_000)
    }

    fn command_message_at(
        chat: serde_json::Value,
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        let mut value = serde_json::json!({
            "message_id": 10,
            "date": date,
            "chat": chat,
            "from": {"id": 5, "is_bot": false, "first_name": "Ada"},
            "text": text,
            "entities": [{"offset": 0, "length": command_len, "type": "bot_command"}]
        });
        if let Some(thread_id) = thread_id {
            value["message_thread_id"] = serde_json::json!(thread_id);
        }
        sample_message(value)
    }

    fn private_reset_message() -> Result<TelegramMessage, serde_json::Error> {
        command_message(
            serde_json::json!({"id": 42, "type": "private", "first_name": "Ada"}),
            "/reset",
            6,
            None,
        )
    }

    fn group_reset_message(
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        group_reset_message_at(text, command_len, thread_id, 1_710_000_000)
    }

    fn group_reset_message_at(
        text: &str,
        command_len: i64,
        thread_id: Option<i64>,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        command_message_at(
            serde_json::json!({"id": -42, "type": "supergroup", "title": "Group"}),
            text,
            command_len,
            thread_id,
            date,
        )
    }

    fn update_from_message(message: TelegramMessage) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "update_id": 500,
            "message": serde_json::to_value(message)?,
        }))
    }

    #[test]
    fn reset_command_gate_matches_go_command_for_bot() -> Result<(), Box<dyn std::error::Error>> {
        assert!(is_reset_command_for_bot(
            &private_reset_message()?,
            "PlotvaBot"
        ));
        assert!(is_reset_command_for_bot(
            &command_message(
                serde_json::json!({"id": 42, "type": "private", "first_name": "Ada"}),
                "/reset@OtherBot",
                15,
                None,
            )?,
            "PlotvaBot"
        ));
        assert!(is_reset_command_for_bot(
            &group_reset_message("/reset@PlotvaBot", 16, None)?,
            "PlotvaBot"
        ));
        assert!(!is_reset_command_for_bot(
            &group_reset_message("/reset", 6, None)?,
            "PlotvaBot"
        ));
        assert!(!is_reset_command_for_bot(
            &group_reset_message("/reset@OtherBot", 15, None)?,
            "PlotvaBot"
        ));

        Ok(())
    }

    #[test]
    fn reset_scope_uses_thread_only_when_present_and_nonzero()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            reset_scope(&private_reset_message()?),
            ResetScope::Chat { chat_id: 42 }
        );
        assert_eq!(
            reset_scope(&group_reset_message("/reset@PlotvaBot", 16, Some(7))?),
            ResetScope::Thread {
                chat_id: -42,
                thread_id: 7
            }
        );
        assert_eq!(
            reset_scope(&group_reset_message("/reset@PlotvaBot", 16, Some(0))?),
            ResetScope::Chat { chat_id: -42 }
        );

        Ok(())
    }

    #[tokio::test]
    async fn reset_update_resets_chat_and_queues_ephemeral_confirmation()
    -> Result<(), Box<dyn std::error::Error>> {
        let history = HistoryStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let outcome = handle_reset_command_update_or_else_at(
            ResetCommandPorts {
                history: &history,
                dispatcher_queue: &queue,
            },
            update_from_message(private_reset_message()?)?,
            ResetCommandContext {
                bot_username: "PlotvaBot",
                now,
            },
            fixed_virtual_id("reset-v1"),
            |_| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        let ResetCommandOutcome::Reset {
            scope,
            history_error,
            confirmation,
        } = outcome
        else {
            panic!("unexpected reset outcome");
        };
        assert_eq!(scope, ResetScope::Chat { chat_id: 42 });
        assert_eq!(history_error, None);
        assert_eq!(history.resets(), vec![(42, 0, now)]);
        assert_eq!(confirmation.parts[0].virtual_id, "reset-v1");

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert_eq!(snapshot.regular.len(), 0);
        assert_eq!(
            snapshot.immediate[0].ephemeral_delete_after,
            Some(Duration::from_secs(60))
        );

        Ok(())
    }

    #[tokio::test]
    async fn reset_update_uses_thread_scope_and_keeps_history_errors_nonfatal()
    -> Result<(), Box<dyn std::error::Error>> {
        let history = HistoryStub {
            reset_error: Some("db down".to_owned()),
            ..HistoryStub::default()
        };
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let outcome = handle_reset_command_update_or_else_at(
            ResetCommandPorts {
                history: &history,
                dispatcher_queue: &queue,
            },
            update_from_message(group_reset_message("/reset@PlotvaBot", 16, Some(9))?)?,
            ResetCommandContext {
                bot_username: "PlotvaBot",
                now,
            },
            fixed_virtual_id("reset-v2"),
            |_| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert!(matches!(
            outcome,
            ResetCommandOutcome::Reset {
                scope: ResetScope::Thread {
                    chat_id: -42,
                    thread_id: 9
                },
                history_error: Some(_),
                ..
            }
        ));
        assert_eq!(history.resets(), vec![(-42, 9, now)]);
        assert_eq!(queue.snapshot().immediate.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn reset_update_delegates_non_reset_messages() -> Result<(), Box<dyn std::error::Error>> {
        let history = HistoryStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let delegated = Arc::new(Mutex::new(0usize));
        let delegated_for_handler = Arc::clone(&delegated);
        let outcome = handle_reset_command_update_or_else_at(
            ResetCommandPorts {
                history: &history,
                dispatcher_queue: &queue,
            },
            update_from_message(group_reset_message("/reset", 6, None)?)?,
            ResetCommandContext {
                bot_username: "PlotvaBot",
                now: OffsetDateTime::now_utc(),
            },
            fixed_virtual_id("unused"),
            move |_| {
                let delegated = Arc::clone(&delegated_for_handler);
                async move {
                    *delegated.lock().expect("delegated") += 1;
                    Ok::<(), io::Error>(())
                }
            },
        )
        .await?;

        assert_eq!(outcome, ResetCommandOutcome::Delegated);
        assert_eq!(*delegated.lock().expect("delegated"), 1);
        assert!(history.resets().is_empty());
        assert!(queue.snapshot().immediate.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn reset_update_handler_consumes_reset_before_wrapped_handler()
    -> Result<(), Box<dyn std::error::Error>> {
        let history = Arc::new(HistoryStub::default());
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = ResetCommandUpdateHandler::new(
            Arc::clone(&history),
            Arc::clone(&queue),
            "PlotvaBot",
            Arc::clone(&next),
        )
        .with_virtual_id_factory(Arc::new(|| "reset-v3".to_owned()));

        handler
            .handle_update(update_from_message(private_reset_message()?)?)
            .await?;

        assert_eq!(next.handled_count(), 0);
        assert_eq!(history.resets().len(), 1);
        assert_eq!(queue.snapshot().immediate.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_reset_updates_history_and_queues_confirmation_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-reset:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let history = Arc::new(HistoryStub::default());
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = ResetCommandUpdateHandler::new(
            Arc::clone(&history),
            Arc::clone(&queue),
            "PlotvaBot",
            Arc::clone(&next),
        )
        .with_virtual_id_factory(Arc::new(|| "reset-live-redis-vmsg-1".to_owned()));

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let update = update_from_message(group_reset_message_at(
            "/reset@PlotvaBot",
            16,
            Some(9),
            now_secs as i64,
        )?)?;
        update_queue.enqueue_update(&update).await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded reset update"))?;

        let report = process_update_with_state_store_at(
            decoded,
            UpdateConsumerConfig {
                dequeue_timeout: Duration::from_millis(1),
                state_timeout: Duration::from_secs(1),
                handle_timeout: Duration::from_secs(1),
                side_effect_max_age: Duration::from_secs(60),
                worker_limit: 1,
            },
            UNIX_EPOCH + Duration::from_secs(now_secs),
            &state_store,
            |update| handler.handle_update(update),
        )
        .await;

        assert_eq!(report.update_id, 500);
        assert_eq!(report.update_name, "message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            vec!["chat:-42:supergroup::".to_owned(), "user:5:Ada:".to_owned()]
        );
        assert_eq!(next.handled_count(), 0);
        let resets = history.resets();
        assert_eq!(resets.len(), 1);
        assert_eq!((resets[0].0, resets[0].1), (-42, 9));
        let item = queue
            .dequeue_immediate()
            .ok_or_else(|| io::Error::other("expected reset confirmation payload"))?;
        assert_eq!(item.metadata().virtual_id, "reset-live-redis-vmsg-1");
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        assert_eq!(
            item.ephemeral_delete_after(),
            Some(RESET_CONFIRMATION_DELETE_AFTER)
        );
        let value = method_as_value(
            item.into_method()
                .ok_or_else(|| io::Error::other("expected sendMessage method"))?,
        )?;
        assert_eq!(value["chat_id"], serde_json::json!(-42));
        assert_eq!(value["text"], serde_json::json!(RESET_CONFIRMATION_TEXT));
        assert_eq!(
            value["reply_parameters"]["message_id"],
            serde_json::json!(10)
        );
        assert_eq!(value["reply_parameters"]["chat_id"], serde_json::json!(-42));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            serde_json::json!(true)
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    fn fixed_virtual_id(value: &'static str) -> impl FnMut() -> String {
        move || value.to_owned()
    }

    #[derive(Default)]
    struct HistoryStub {
        resets: Mutex<Vec<(i64, i32, OffsetDateTime)>>,
        reset_error: Option<String>,
    }

    impl HistoryStub {
        fn resets(&self) -> Vec<(i64, i32, OffsetDateTime)> {
            self.resets.lock().expect("resets").clone()
        }
    }

    impl ResetHistoryStore for HistoryStub {
        type Error = io::Error;

        fn reset_history_at<'a>(
            &'a self,
            chat_id: i64,
            thread_id: i32,
            reset_at: OffsetDateTime,
        ) -> ResetHistoryFuture<'a, Self::Error> {
            Box::pin(async move {
                self.resets
                    .lock()
                    .expect("resets")
                    .push((chat_id, thread_id, reset_at));
                if let Some(error) = &self.reset_error {
                    return Err(io::Error::other(error.clone()));
                }
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
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
            user: &'a openplotva_core::UserState,
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
                    .push(format!("file:{}", params.file_unique_id));
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct UpdateHandlerStub {
        handled: Mutex<usize>,
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

    fn method_as_value(
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> io::Result<serde_json::Value> {
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
}
