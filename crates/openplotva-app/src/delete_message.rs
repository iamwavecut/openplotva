//! App-level generic callback message deletion behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, ChatMember as TelegramChatMember,
    MaybeInaccessibleMessage, ReplyTo, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
};
use openplotva_telegram::{
    CallbackActionParse, CallbackAnswerRequest, ChatRef, DeleteMessageRequest,
    TelegramOutboundMethod, build_callback_answer_method, build_delete_message_method,
    build_get_chat_member_method, parse_callback_action,
};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

const DELETE_CALLBACK_ACTION: &str = "delete";
const DELETE_CALLBACK_UNAUTHORIZED_TEXT: &str = "Я подчинюсь только автору или администраторам.";

/// Boxed future returned by Telegram membership APIs.
pub type DeleteMessageMemberFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TelegramChatMember, E>> + Send + 'a>>;

pub trait DeleteMessageMemberApi {
    /// Concrete API error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Fetch one Telegram chat member.
    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteMessageMemberFuture<'a, Self::Error>;
}

impl DeleteMessageMemberApi for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn get_chat_member<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
    ) -> DeleteMessageMemberFuture<'a, Self::Error> {
        Box::pin(async move {
            self.execute(build_get_chat_member_method(chat_id, user_id))
                .await
        })
    }
}

/// Boxed future returned by delete callback effects.
pub type DeleteMessageEffectFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

pub trait DeleteMessageEffects {
    /// Concrete effect error.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute one direct Telegram Bot API method.
    fn execute_delete_message_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> DeleteMessageEffectFuture<'a, Self::Error>;
}

impl DeleteMessageEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn execute_delete_message_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> DeleteMessageEffectFuture<'a, Self::Error> {
        Box::pin(async move { method.execute_with(self).await.map(|_| ()) })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeleteMessageCallbackTarget {
    /// Callback message chat.
    pub message_chat: ChatRef,
    /// Callback message ID to delete.
    pub message_id: i64,
    /// Replied-to chat ID used for the membership lookup.
    pub reply_chat_id: i64,
}

/// Result of one generic delete callback update.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeleteMessageCallbackOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    NoTargetAck,
    LookupErrorAck,
    /// Callback message was deleted and acknowledged.
    Deleted,
    /// but is kept to preserve the handler branch itself.
    Unauthorized,
}

/// Fatal errors from the generic delete callback wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DeleteMessageCallbackError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct DeleteMessageCallbackUpdateHandler<MemberApi, Effects, Next> {
    member_api: Arc<MemberApi>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<MemberApi, Effects, Next> DeleteMessageCallbackUpdateHandler<MemberApi, Effects, Next> {
    /// Build a generic delete callback handler around the real downstream update handler.
    pub fn new(member_api: Arc<MemberApi>, effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self {
            member_api,
            effects,
            next,
        }
    }
}

impl<MemberApi, Effects, Next> UpdateHandler
    for DeleteMessageCallbackUpdateHandler<MemberApi, Effects, Next>
where
    MemberApi: DeleteMessageMemberApi + Send + Sync,
    Effects: DeleteMessageEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = DeleteMessageCallbackError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_delete_message_callback_update_or_else(
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

pub async fn handle_delete_message_callback_update_or_else<
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
) -> Result<DeleteMessageCallbackOutcome, DeleteMessageCallbackError>
where
    MemberApi: DeleteMessageMemberApi + Sync,
    Effects: DeleteMessageEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::CallbackQuery(query) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| DeleteMessageCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DeleteMessageCallbackOutcome::Delegated);
    };

    if !is_delete_message_callback(query) {
        handle_other(update)
            .await
            .map_err(|error| DeleteMessageCallbackError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(DeleteMessageCallbackOutcome::Delegated);
    }

    Ok(handle_delete_message_callback(member_api, effects, query).await)
}

pub async fn handle_delete_message_callback<MemberApi, Effects>(
    member_api: &MemberApi,
    effects: &Effects,
    query: &TelegramCallbackQuery,
) -> DeleteMessageCallbackOutcome
where
    MemberApi: DeleteMessageMemberApi + Sync,
    Effects: DeleteMessageEffects + Sync,
{
    let Some(target) = delete_message_callback_target(query) else {
        try_answer_callback(
            effects,
            &query.id,
            "",
            false,
            0,
            "delete callback no target",
        )
        .await;
        return DeleteMessageCallbackOutcome::NoTargetAck;
    };

    let requester_id = i64::from(query.from.id);
    let member = match member_api
        .get_chat_member(target.reply_chat_id, requester_id)
        .await
    {
        Ok(member) => member,
        Err(error) => {
            tracing::warn!(
                message = %error,
                chat_id = target.reply_chat_id,
                user_id = requester_id,
                "failed to authorize generic delete callback"
            );
            try_answer_callback(
                effects,
                &query.id,
                "",
                false,
                10,
                "delete callback lookup error",
            )
            .await;
            return DeleteMessageCallbackOutcome::LookupErrorAck;
        }
    };

    if delete_callback_allows_delete(&target, &member) {
        try_delete_message(
            effects,
            target.message_chat.id,
            target.message_id,
            "delete callback message",
        )
        .await;
        try_answer_callback(effects, &query.id, "", false, 10, "delete callback ack").await;
        return DeleteMessageCallbackOutcome::Deleted;
    }

    try_answer_callback(
        effects,
        &query.id,
        DELETE_CALLBACK_UNAUTHORIZED_TEXT,
        true,
        10,
        "delete callback unauthorized",
    )
    .await;
    DeleteMessageCallbackOutcome::Unauthorized
}

fn delete_callback_allows_delete(
    target: &DeleteMessageCallbackTarget,
    member: &TelegramChatMember,
) -> bool {
    target.reply_chat_id != 0 || telegram_member_can_delete_message(member)
}

#[must_use]
pub fn telegram_member_can_delete_message(member: &TelegramChatMember) -> bool {
    match member {
        TelegramChatMember::Creator(_) => true,
        TelegramChatMember::Administrator(admin) => admin.can_delete_messages,
        TelegramChatMember::Kicked(_)
        | TelegramChatMember::Left(_)
        | TelegramChatMember::Member { .. }
        | TelegramChatMember::Restricted(_) => false,
    }
}

fn is_delete_message_callback(query: &TelegramCallbackQuery) -> bool {
    let Some(raw) = query.data.as_deref() else {
        return false;
    };
    matches!(
        parse_callback_action(raw),
        CallbackActionParse::Action { action, .. } if action == DELETE_CALLBACK_ACTION
    )
}

fn delete_message_callback_target(
    query: &TelegramCallbackQuery,
) -> Option<DeleteMessageCallbackTarget> {
    let MaybeInaccessibleMessage::Message(message) = query.message.as_ref()? else {
        return None;
    };
    let Some(ReplyTo::Message(reply_to)) = message.reply_to.as_ref() else {
        return None;
    };
    Some(DeleteMessageCallbackTarget {
        message_chat: ChatRef {
            id: message.chat.get_id().into(),
            is_forum: message.message_thread_id.is_some(),
        },
        message_id: message.id,
        reply_chat_id: reply_to.chat.get_id().into(),
    })
}

async fn try_answer_callback<Effects>(
    effects: &Effects,
    query_id: &str,
    text: &str,
    show_alert: bool,
    cache_time: i64,
    context: &'static str,
) where
    Effects: DeleteMessageEffects + Sync,
{
    let request = CallbackAnswerRequest {
        callback_query_id: query_id.to_owned(),
        text: text.to_owned(),
        show_alert,
        url: String::new(),
        cache_time,
    };
    try_execute_delete_message_method(
        effects,
        build_callback_answer_method(&request).into(),
        context,
    )
    .await;
}

async fn try_delete_message<Effects>(
    effects: &Effects,
    chat_id: i64,
    message_id: i64,
    context: &'static str,
) where
    Effects: DeleteMessageEffects + Sync,
{
    match build_delete_message_method(&DeleteMessageRequest {
        chat_id,
        message_id,
    }) {
        Ok(method) => try_execute_delete_message_method(effects, method.into(), context).await,
        Err(error) => tracing::warn!(message = %error, "failed to build delete callback method"),
    }
}

async fn try_execute_delete_message_method<Effects>(
    effects: &Effects,
    method: TelegramOutboundMethod,
    context: &'static str,
) where
    Effects: DeleteMessageEffects + Sync,
{
    let method_name = method.method_name();
    if let Err(error) = effects.execute_delete_message_method(method).await {
        tracing::warn!(
            message = %error,
            method = method_name,
            context,
            "generic delete callback Telegram side effect failed"
        );
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
        include_reply: bool,
        from_user_id: i64,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut message = serde_json::json!({
            "message_id": 77,
            "date": 1_710_000_000,
            "chat": {"id": -10042, "type": "supergroup", "title": "Plotva Lab"},
            "text": "delete me"
        });
        if include_reply {
            message["reply_to_message"] = serde_json::json!({
                "message_id": 55,
                "date": 1_710_000_000,
                "chat": {"id": -42, "type": "supergroup", "title": "Source"},
                "text": "source"
            });
        }

        serde_json::from_value(serde_json::json!({
            "update_id": 8181,
            "callback_query": {
                "id": "delete-callback-id",
                "from": {"id": from_user_id, "is_bot": false, "first_name": "Ada"},
                "chat_instance": "chat-instance",
                "data": r#"{"action":"delete"}"#,
                "message": message
            }
        }))
    }

    #[tokio::test]
    async fn delete_message_callback_without_reply_target_gets_empty_uncached_ack()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::with_member(member(7));
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_message_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(false, 7)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteMessageCallbackOutcome::NoTargetAck);
        assert_eq!(member_api.calls(), Vec::<(i64, i64)>::new());
        assert_eq!(next.handled_count(), 0);
        let methods = effects.methods();
        assert_eq!(methods[0].0, "answerCallbackQuery");
        assert!(methods[0].1.get("text").is_none());
        assert!(methods[0].1.get("cache_time").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn delete_message_callback_lookup_error_gets_empty_cached_ack()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::default();
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_message_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(true, 7)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteMessageCallbackOutcome::LookupErrorAck);
        assert_eq!(member_api.calls(), vec![(-42, 7)]);
        let methods = effects.methods();
        assert_eq!(methods[0].0, "answerCallbackQuery");
        assert!(methods[0].1.get("text").is_none());
        assert_eq!(methods[0].1["cache_time"], 10);
        Ok(())
    }

    #[tokio::test]
    async fn delete_message_callback_deletes_replied_control_message_after_lookup()
    -> Result<(), Box<dyn std::error::Error>> {
        let member_api = MemberApiStub::with_member(member(7));
        let effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();

        let outcome = handle_delete_message_callback_update_or_else(
            &member_api,
            &effects,
            delete_callback_update(true, 7)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, DeleteMessageCallbackOutcome::Deleted);
        let methods = effects.methods();
        assert_eq!(
            method_names(&methods),
            vec!["deleteMessage", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], -10042);
        assert_eq!(methods[0].1["message_id"], 77);
        assert_eq!(methods[1].1["cache_time"], 10);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_delete_callback_delegates_and_sends_methods_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-delete-callback:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        update_queue
            .enqueue_update(&delete_callback_update(true, 7)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded generic delete callback update"))?;

        let state_store = UpdateStateStoreStub::default();
        let callback_effects = CallbackAckEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let member_api = MemberApiStub::with_member(member(7));
        let delete_effects = EffectsStub::default();
        let next = UpdateHandlerStub::default();
        let prehandler_route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&prehandler_route);
        let delete_outcome = Arc::new(Mutex::new(None));
        let captured_outcome = Arc::clone(&delete_outcome);

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
                        let outcome = handle_delete_message_callback_update_or_else(
                            &member_api,
                            &delete_effects,
                            update,
                            |update| next.handle_update(update),
                        )
                        .await
                        .map_err(|error| io::Error::other(error.to_string()))?;
                        *captured_outcome.lock().expect("delete outcome") = Some(outcome);
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

        assert_eq!(report.update_id, 8181);
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
                handler: CallbackHandlerKind::Delete,
                action: "delete".to_owned(),
            })
        );
        assert_eq!(
            *delete_outcome.lock().expect("delete outcome"),
            Some(DeleteMessageCallbackOutcome::Deleted)
        );
        assert!(callback_effects.methods().is_empty());
        assert_eq!(member_api.calls(), vec![(-42, 7)]);
        assert_eq!(next.handled_count(), 0);

        let methods = delete_effects.methods();
        assert_eq!(
            method_names(&methods),
            vec!["deleteMessage", "answerCallbackQuery"]
        );
        assert_eq!(methods[0].1["chat_id"], json!(-10042));
        assert_eq!(methods[0].1["message_id"], json!(77));
        assert_eq!(
            methods[1].1["callback_query_id"],
            json!("delete-callback-id")
        );
        assert_eq!(methods[1].1["cache_time"], json!(10));
        assert!(methods[1].1.get("text").is_none());
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[test]
    fn delete_message_member_permission_matches_go_rights() {
        assert!(telegram_member_can_delete_message(&creator(1)));
        assert!(telegram_member_can_delete_message(&deleting_admin(2)));
        assert!(!telegram_member_can_delete_message(&member(3)));
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

    impl DeleteMessageMemberApi for MemberApiStub {
        type Error = io::Error;

        fn get_chat_member<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
        ) -> DeleteMessageMemberFuture<'a, Self::Error> {
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

    impl DeleteMessageEffects for EffectsStub {
        type Error = io::Error;

        fn execute_delete_message_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> DeleteMessageEffectFuture<'a, Self::Error> {
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
            TelegramOutboundMethod::DeleteMessage(method) => serde_json::to_value(method.as_ref()),
            _ => return Err(io::Error::other("unexpected method")),
        }
        .map_err(io::Error::other)
    }

    fn method_names(methods: &[(String, serde_json::Value)]) -> Vec<&str> {
        methods.iter().map(|(name, _)| name.as_str()).collect()
    }

    fn member(user_id: i64) -> TelegramChatMember {
        ChatMember::Member {
            user: User::new(user_id, "Ada", false),
            tag: None,
            until_date: None,
        }
    }

    fn deleting_admin(user_id: i64) -> TelegramChatMember {
        ChatMember::Administrator(
            ChatMemberAdministrator::new(User::new(user_id, "Ada", false))
                .with_can_delete_messages(true),
        )
    }

    fn creator(user_id: i64) -> TelegramChatMember {
        ChatMember::Creator(ChatMemberCreator::new(User::new(user_id, "Ada", false)))
    }
}
