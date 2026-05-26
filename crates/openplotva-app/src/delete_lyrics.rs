//! App-level generated-lyrics delete callback behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, ChatMember as TelegramChatMember,
    MaybeInaccessibleMessage, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
};
use openplotva_telegram::{
    CallbackActionParse, CallbackAnswerRequest, ChatRef, DeleteMessageRequest,
    EditReplyMarkupMessageRequest, TelegramOutboundMethod, build_callback_answer_method,
    build_delete_message_method, build_edit_reply_markup_message_method,
    build_get_chat_member_method, build_lyrics_delete_confirm_keyboard, parse_callback_action,
    parse_callback_i64,
};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

const DELETE_LYRICS_AUTH_ALERT_TEXT: &str = "Удаление доступно только автору или администраторам.";
const DELETE_LYRICS_DELETED_TEXT: &str = "✅ Текст удалён";

/// Boxed future returned by Telegram membership APIs.
pub type DeleteLyricsMemberFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TelegramChatMember, E>> + Send + 'a>>;

pub trait DeleteLyricsMemberApi {
    /// Concrete API error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Fetch one Telegram chat member.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteLyricsMemberFuture<'a, Self::Error>;
}

impl DeleteLyricsMemberApi for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteLyricsMemberFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(build_get_chat_member_method(chat_id, user_id))
                .await
        })
    }
}

/// Boxed future returned by delete-lyrics effects.
pub type DeleteLyricsEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait DeleteLyricsEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute one direct Telegram Bot API method.
    fn execute_delete_lyrics_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> DeleteLyricsEffectFuture<'a, Self::Error>;
}

impl DeleteLyricsEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn execute_delete_lyrics_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> DeleteLyricsEffectFuture<'a, Self::Error> {
        Box::pin(async move { method.execute_with(self).await.map(|_| ()) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteLyricsCallbackData {
    pub action: String,
    pub user_id: i64,
    pub chat_id: i64,
}

/// Control message attached to a callback query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteLyricsControlMessage {
    /// Control message chat.
    pub chat: ChatRef,
    /// Control message ID.
    pub message_id: i64,
}

/// Result of one delete-lyrics callback update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteLyricsCallbackOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    BadDataAck,
    /// Callback author was neither the message author nor an admin with delete rights.
    Unauthorized,
    /// Confirmation keyboard was shown.
    ConfirmShown,
    /// Lyrics message was deleted.
    Deleted,
}

/// Fatal errors from the delete-lyrics callback wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DeleteLyricsCallbackError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct DeleteLyricsCallbackUpdateHandler<MemberApi, Effects, Next> {
    member_api: Arc<MemberApi>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<MemberApi, Effects, Next> DeleteLyricsCallbackUpdateHandler<MemberApi, Effects, Next> {
    /// Build a delete-lyrics callback handler around the real downstream update handler.
    pub fn new(member_api: Arc<MemberApi>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            member_api,
            effects,
            next,
        }
    }
}

impl<MemberApi, Effects, Next> UpdateHandler
    for DeleteLyricsCallbackUpdateHandler<MemberApi, Effects, Next>
where
    MemberApi: DeleteLyricsMemberApi + Send + Sync,
    Effects: DeleteLyricsEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = DeleteLyricsCallbackError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_delete_lyrics_callback_update_or_else(
                self.member_api.as_ref(),
                self.effects.as_ref(),
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_delete_lyrics_callback_update_or_else<
    MemberApi,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    member_api: &MemberApi,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<DeleteLyricsCallbackOutcome, DeleteLyricsCallbackError>
where
    MemberApi: DeleteLyricsMemberApi + Sync,
    Effects: DeleteLyricsEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let data = match &update.update_type {
        TelegramUpdateType::CallbackQuery(query) => {
            delete_lyrics_callback_data_from_query(query).zip(delete_lyrics_control_message(query))
        }
        _ => None,
    };

    let Some((data, control_message)) = data else {
        handle_other(update)
            .await
            .map_err(|error| DeleteLyricsCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DeleteLyricsCallbackOutcome::Delegated);
    };

    let TelegramUpdateType::CallbackQuery(query) = &update.update_type else {
        unreachable!("callback query was already matched");
    };

    Ok(handle_delete_lyrics_callback(member_api, effects, query, control_message, data).await)
}

pub async fn handle_delete_lyrics_callback<MemberApi, Effects>(
    member_api: &MemberApi,
    effects: &Effects,
    query: &TelegramCallbackQuery,
    control_message: DeleteLyricsControlMessage,
    data: DeleteLyricsCallbackData,
) -> DeleteLyricsCallbackOutcome
where
    MemberApi: DeleteLyricsMemberApi + Sync,
    Effects: DeleteLyricsEffects + Sync,
{
    if data.user_id == 0 || data.chat_id == 0 {
        try_answer_callback(effects, &query.id, "", false, "bad lyrics data ack").await;
        return DeleteLyricsCallbackOutcome::BadDataAck;
    }

    if !is_authorized_for_delete_lyrics(member_api, query, data.user_id, data.chat_id).await {
        try_answer_callback(
            effects,
            &query.id,
            DELETE_LYRICS_AUTH_ALERT_TEXT,
            true,
            "lyrics auth alert",
        )
        .await;
        return DeleteLyricsCallbackOutcome::Unauthorized;
    }

    match data.action.as_str() {
        openplotva_telegram::DELETE_LYRICS_ACTION_INIT => {
            try_edit_reply_markup(
                effects,
                EditReplyMarkupMessageRequest {
                    chat: control_message.chat,
                    message_id: control_message.message_id,
                    reply_markup: build_lyrics_delete_confirm_keyboard(data.user_id, data.chat_id),
                },
                "lyrics confirm keyboard",
            )
            .await;
            try_answer_callback(effects, &query.id, "", false, "lyrics init ack").await;
            DeleteLyricsCallbackOutcome::ConfirmShown
        }
        openplotva_telegram::DELETE_LYRICS_ACTION_CONFIRM => {
            try_delete_message(
                effects,
                control_message.chat.id,
                control_message.message_id,
                "delete lyrics message",
            )
            .await;
            try_answer_callback(
                effects,
                &query.id,
                DELETE_LYRICS_DELETED_TEXT,
                false,
                "lyrics delete ack",
            )
            .await;
            DeleteLyricsCallbackOutcome::Deleted
        }
        _ => DeleteLyricsCallbackOutcome::Delegated,
    }
}

async fn try_answer_callback<Effects>(
    effects: &Effects,
    query_id: &str,
    text: &str,
    show_alert: bool,
    context: &'static str,
) where
    Effects: DeleteLyricsEffects + Sync,
{
    let request = CallbackAnswerRequest {
        callback_query_id: query_id.to_owned(),
        text: text.to_owned(),
        show_alert,
        url: String::new(),
        cache_time: 0,
    };
    try_execute_delete_lyrics_method(
        effects,
        build_callback_answer_method(&request).into(),
        context,
    )
    .await;
}

async fn try_edit_reply_markup<Effects>(
    effects: &Effects,
    request: EditReplyMarkupMessageRequest,
    context: &'static str,
) where
    Effects: DeleteLyricsEffects + Sync,
{
    match build_edit_reply_markup_message_method(&request) {
        Ok(method) => try_execute_delete_lyrics_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete lyrics method"),
    }
}

async fn try_delete_message<Effects>(
    effects: &Effects,
    chat_id: i64,
    message_id: i64,
    context: &'static str,
) where
    Effects: DeleteLyricsEffects + Sync,
{
    match build_delete_message_method(&DeleteMessageRequest {
        chat_id,
        message_id,
    }) {
        Ok(method) => try_execute_delete_lyrics_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete lyrics method"),
    }
}

async fn try_execute_delete_lyrics_method<Effects>(
    effects: &Effects,
    method: TelegramOutboundMethod,
    context: &'static str,
) where
    Effects: DeleteLyricsEffects + Sync,
{
    let method_name = method.method_name();
    if let Err(error) = effects.execute_delete_lyrics_method(method).await {
        tracing::warn!(
            message = %error,
            method = method_name,
            context,
            "delete lyrics callback Telegram side effect failed"
        );
    }
}

async fn is_authorized_for_delete_lyrics<MemberApi>(
    member_api: &MemberApi,
    query: &TelegramCallbackQuery,
    data_user_id: i64,
    data_chat_id: i64,
) -> bool
where
    MemberApi: DeleteLyricsMemberApi + Sync,
{
    let requester_id = i64::from(query.from.id);
    if requester_id == data_user_id {
        return true;
    }

    match member_api.get_chat_member(data_chat_id, requester_id).await {
        Ok(member) => telegram_member_can_delete_lyrics(&member),
        Err(error) => {
            tracing::warn!(
                message = %error,
                chat_id = data_chat_id,
                user_id = requester_id,
                "failed to authorize delete lyrics callback"
            );
            false
        }
    }
}

#[must_use]
pub fn telegram_member_can_delete_lyrics(member: &TelegramChatMember) -> bool {
    match member {
        TelegramChatMember::Creator(_) => true,
        TelegramChatMember::Administrator(admin) => admin.can_delete_messages,
        TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => false,
    }
}

fn delete_lyrics_callback_data_from_query(
    query: &TelegramCallbackQuery,
) -> Option<DeleteLyricsCallbackData> {
    delete_lyrics_callback_data_from_raw(query.data.as_deref().unwrap_or_default())
}

fn delete_lyrics_callback_data_from_raw(raw: &str) -> Option<DeleteLyricsCallbackData> {
    let CallbackActionParse::Action { data, action } = parse_callback_action(raw) else {
        return None;
    };
    if !matches!(
        action.as_str(),
        openplotva_telegram::DELETE_LYRICS_ACTION_INIT
            | openplotva_telegram::DELETE_LYRICS_ACTION_CONFIRM
    ) {
        return None;
    }
    let user_id = data.get("u").map_or(0, |raw| parse_callback_i64(raw));
    let chat_id = data.get("c").map_or(0, |raw| parse_callback_i64(raw));

    Some(DeleteLyricsCallbackData {
        action,
        user_id,
        chat_id,
    })
}

fn delete_lyrics_control_message(
    query: &TelegramCallbackQuery,
) -> Option<DeleteLyricsControlMessage> {
    match query.message.as_ref()? {
        MaybeInaccessibleMessage::Message(message) => Some(DeleteLyricsControlMessage {
            chat: ChatRef {
                id: message.chat.get_id().into(),
                is_forum: message.message_thread_id.is_some(),
            },
            message_id: message.id,
        }),
        MaybeInaccessibleMessage::InaccessibleMessage(message) => {
            Some(DeleteLyricsControlMessage {
                chat: ChatRef {
                    id: message.chat.get_id().into(),
                    is_forum: false,
                },
                message_id: message.message_id,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        callbacks::{
            CallbackQueryEffects, CallbackQueryFuture, CallbackQueryUpdateRoute,
            CallbackRateLimitFuture, CallbackRateLimitPolicy, handle_callback_query_update_or_else,
        },
        updates::{
            TelegramFileMetadataStoreFuture, UpdateStateStore, UpdateStateStoreFuture,
            process_update_with_state_store_at,
        },
    };
    use carapax::types::{ChatMember, ChatMemberAdministrator, ChatMemberCreator, User};
    use openplotva_telegram::{CallbackHandlerKind, TelegramOutboundMethodKind};
    use openplotva_updates::UpdateConsumerConfig;
    use serde_json::json;
    use std::{
        env, io,
        sync::{Arc, Mutex, MutexGuard},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    fn delete_callback_update(
        data: String,
        from_user_id: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "update_id": 7171,
            "callback_query": {
                "id": "lyrics-callback-id",
                "from": {"id": from_user_id, "is_bot": false, "first_name": "Ada"},
                "chat_instance": "chat-instance",
                "data": data,
                "message": {
                    "message_id": 66,
                    "date": 1_710_000_000,
                    "chat": {"id": -10042, "type": "supergroup", "title": "Plotva Lab"},
                    "text": "lyrics"
                }
            }
        }))
    }

    #[test]
    fn delete_lyrics_callback_parse_matches_go_zero_fallback_and_unrouted_close()
    -> Result<(), Box<dyn std::error::Error>> {
        let data = delete_lyrics_callback_data_from_raw(r#"{"a":"dl_i","u":"bad","c":""}"#)
            .ok_or_else(|| io::Error::other("expected lyrics data"))?;
        assert_eq!(
            data,
            DeleteLyricsCallbackData {
                action: "dl_i".to_owned(),
                user_id: 0,
                chat_id: 0,
            }
        );
        assert!(
            delete_lyrics_callback_data_from_raw(r#"{"a":"dl_x","u":"5","c":"-42"}"#).is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn delete_lyrics_callback_bad_data_gets_empty_ack_before_auth()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::with_member(deleting_admin_member(7));
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_lyrics_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(
                openplotva_telegram::delete_lyrics_callback_data(
                    openplotva_telegram::DELETE_LYRICS_ACTION_INIT,
                    0,
                    -42,
                ),
                7,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteLyricsCallbackOutcome::BadDataAck);
        assert_eq!(member_api.calls(), Vec::<(i64, i64)>::new());
        assert_eq!(effects.methods()[0].0, "answerCallbackQuery");
        assert!(effects.methods()[0].1.get("text").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete_lyrics_callback_denies_non_author_without_admin_rights()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::default();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_lyrics_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(
                openplotva_telegram::delete_lyrics_callback_data(
                    openplotva_telegram::DELETE_LYRICS_ACTION_INIT,
                    5,
                    -42,
                ),
                7,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteLyricsCallbackOutcome::Unauthorized);
        assert_eq!(member_api.calls(), vec![(-42, 7)]);
        let methods = effects.methods();
        assert_eq!(methods[0].0, "answerCallbackQuery");
        assert_eq!(methods[0].1["text"], DELETE_LYRICS_AUTH_ALERT_TEXT);
        assert_eq!(methods[0].1["show_alert"], true);
        Ok(())
    }

    #[tokio::test]
    async fn delete_lyrics_callback_init_shows_confirm_keyboard()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::default();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_lyrics_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(
                openplotva_telegram::delete_lyrics_callback_data(
                    openplotva_telegram::DELETE_LYRICS_ACTION_INIT,
                    5,
                    -42,
                ),
                5,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteLyricsCallbackOutcome::ConfirmShown);
        let methods = effects.methods();
        assert_eq!(methods[0].0, "editMessageReplyMarkup");
        assert_eq!(methods[0].1["chat_id"], -10042);
        assert_eq!(methods[0].1["message_id"], 66);
        assert_eq!(
            methods[0].1["reply_markup"]["inline_keyboard"][0][0]["text"],
            "Да, удалить"
        );
        assert_eq!(methods[1].0, "answerCallbackQuery");
        Ok(())
    }

    #[tokio::test]
    async fn delete_lyrics_callback_confirm_deletes_message_and_acks()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::with_member(deleting_admin_member(7));
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_lyrics_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(
                openplotva_telegram::delete_lyrics_callback_data(
                    openplotva_telegram::DELETE_LYRICS_ACTION_CONFIRM,
                    5,
                    -42,
                ),
                7,
            )?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteLyricsCallbackOutcome::Deleted);
        let methods = effects.methods();
        assert_eq!(
            method_names(&methods),
            vec!["deleteMessage", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], -10042);
        assert_eq!(methods[0].1["message_id"], 66);
        assert_eq!(methods[1].1["text"], DELETE_LYRICS_DELETED_TEXT);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_delete_lyrics_callback_delegates_and_deletes_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-delete-lyrics-callback:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&delete_callback_update(
                openplotva_telegram::delete_lyrics_callback_data(
                    openplotva_telegram::DELETE_LYRICS_ACTION_CONFIRM,
                    5,
                    -42,
                ),
                7,
            )?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded delete lyrics callback update"))?;

        let state_store = UpdateStateStoreStub::default();
        let callback_effects = CallbackAckEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let member_api = MemberApiStub::with_member(deleting_admin_member(7));
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let prehandler_route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&prehandler_route);
        let lyrics_outcome = Arc::new(Mutex::new(None));
        let captured_outcome = Arc::clone(&lyrics_outcome);

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
                        let outcome = handle_delete_lyrics_callback_update_or_else(
                            &member_api,
                            &effects,
                            update,
                            |update| next.handle_update(update),
                        )
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                        *captured_outcome.lock().expect("lyrics outcome") = Some(outcome);
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

        assert_eq!(report.update_id, 7171);
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
            vec!["chat:-10042".to_owned(), "user:7".to_owned()]
        );
        assert_eq!(
            *prehandler_route.lock().expect("callback route"),
            Some(CallbackQueryUpdateRoute::HandlerDelegated {
                handler: CallbackHandlerKind::DeleteLyrics,
                action: "dl_c".to_owned(),
            })
        );
        assert_eq!(
            *lyrics_outcome.lock().expect("lyrics outcome"),
            Some(DeleteLyricsCallbackOutcome::Deleted)
        );
        assert!(callback_effects.methods().is_empty());
        assert_eq!(member_api.calls(), vec![(-42, 7)]);
        assert_eq!(next.handled_count(), 0);

        let methods = effects.methods();
        assert_eq!(
            method_names(&methods),
            vec!["deleteMessage", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], json!(-10042));
        assert_eq!(methods[0].1["message_id"], json!(66));
        assert_eq!(
            methods[1].1["callback_query_id"],
            json!("lyrics-callback-id")
        );
        assert_eq!(methods[1].1["text"], json!(DELETE_LYRICS_DELETED_TEXT));
        assert!(methods[1].1.get("cache_time").is_none());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[derive(Default)]
    struct MemberApiStub {
        member: Mutex<Option<TelegramChatMember>>,
        calls: Mutex<Vec<(i64, i64)>>,
    }

    impl MemberApiStub {
        fn with_member(member: TelegramChatMember) -> Self {
            Self {
                member: Mutex::new(Some(member)),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<(i64, i64)> {
            self.calls.lock().expect("member calls").clone()
        }
    }

    impl DeleteLyricsMemberApi for MemberApiStub {
        type Error = io::Error;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> DeleteLyricsMemberFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("member calls")
                    .push((chat_id, user_id));
                self.member
                    .lock()
                    .expect("member")
                    .clone()
                    .ok_or_else(|| io::Error::other("missing member"))
            })
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        methods: Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl EffectsStub {
        fn methods(&self) -> Vec<(String, serde_json::Value)> {
            self.methods.lock().expect("methods").clone()
        }

        fn lock_methods(&self) -> MutexGuard<'_, Vec<(String, serde_json::Value)>> {
            self.methods.lock().expect("methods")
        }
    }

    impl DeleteLyricsEffects for EffectsStub {
        type Error = io::Error;

        fn execute_delete_lyrics_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> DeleteLyricsEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let name = method.method_name().to_owned();
                let payload = method_payload(method)?;
                self.lock_methods().push((name, payload));
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
            method: TelegramOutboundMethod,
        ) -> CallbackQueryFuture<'a, Self::Error> {
            Box::pin(async move {
                let kind = method.kind();
                let payload = match method {
                    TelegramOutboundMethod::AnswerCallbackQuery(method) => {
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

    fn method_payload(method: TelegramOutboundMethod) -> Result<serde_json::Value, io::Error> {
        match method {
            TelegramOutboundMethod::AnswerCallbackQuery(method) => {
                serde_json::to_value(method.as_ref())
            }
            TelegramOutboundMethod::EditMessageReplyMarkup(method) => {
                serde_json::to_value(method.as_ref())
            }
            TelegramOutboundMethod::DeleteMessage(method) => serde_json::to_value(method.as_ref()),
            _ => return Err(io::Error::other("unexpected method")),
        }
        .map_err(io::Error::other)
    }

    fn method_names(methods: &[(String, serde_json::Value)]) -> Vec<&str> {
        methods.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn deleting_admin_member(user_id: i64) -> TelegramChatMember {
        ChatMember::Administrator(
            ChatMemberAdministrator::new(User::new(user_id, "Ada", false))
                .with_can_delete_messages(true),
        )
    }

    fn _creator_member(user_id: i64) -> TelegramChatMember {
        ChatMember::Creator(ChatMemberCreator::new(User::new(user_id, "Ada", false)))
    }
}
