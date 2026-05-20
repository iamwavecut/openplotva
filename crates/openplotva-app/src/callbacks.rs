//! App-level callback query pre-handler behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{
    CallbackQuery as TelegramCallbackQuery, MaybeInaccessibleMessage, Update as TelegramUpdate,
    UpdateType,
};
use openplotva_telegram::{
    CallbackHandlerKind, CallbackQueryRoute, TelegramOutboundMethod, callback_query_ack_method,
    callback_query_route, settings_callback_ack_method,
};
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    rate_limits::{ChatRateLimitPolicy, RateLimitStore},
    updates::{UpdateHandler, UpdateHandlerFuture},
};

/// Boxed future returned by callback-query side effects.
pub type CallbackQueryFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by callback rate-limit checks.
pub type CallbackRateLimitFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Side-effect boundary for Go callback acknowledgements.
pub trait CallbackQueryEffects {
    /// Error returned by the concrete effect.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute Telegram `answerCallbackQuery`.
    fn answer_callback_query<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> CallbackQueryFuture<'a, Self::Error>;
}

impl CallbackQueryEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn answer_callback_query<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> CallbackQueryFuture<'a, Self::Error> {
        Box::pin(async move { method.execute_with(self).await.map(|_| ()) })
    }
}

/// Rate-limit boundary for Go `srv.IsRateLimited` callback checks.
pub trait CallbackRateLimitPolicy {
    /// Return whether the callback's chat is rate-limited.
    fn is_callback_chat_rate_limited<'a>(&'a self, chat_id: i64) -> CallbackRateLimitFuture<'a>;
}

impl<S> CallbackRateLimitPolicy for ChatRateLimitPolicy<S>
where
    S: RateLimitStore + Send + Sync,
{
    fn is_callback_chat_rate_limited<'a>(&'a self, chat_id: i64) -> CallbackRateLimitFuture<'a> {
        Box::pin(async move {
            self.is_rate_limited_at(chat_id, OffsetDateTime::now_utc())
                .await
                .rate_limited
        })
    }
}

/// Route chosen by the callback-query update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallbackQueryUpdateRoute {
    /// The update was not a callback query and was delegated.
    Delegated,
    /// Go skipped a rate-limited callback without acknowledgement.
    SkippedRateLimited {
        /// Rate-limited chat ID.
        chat_id: i64,
    },
    /// Go sent a terminal empty acknowledgement.
    Acked(CallbackQueryRoute),
    /// Go tried to acknowledge but Telegram returned an error; the update is consumed.
    AckError {
        /// Route that required an acknowledgement.
        route: CallbackQueryRoute,
        /// Display form of the effect error.
        message: String,
    },
    /// Go sent the cached settings callback acknowledgement.
    SettingsAcked {
        /// Raw settings callback data.
        data: String,
    },
    /// Go tried to acknowledge a settings callback but Telegram returned an error.
    SettingsAckError {
        /// Raw settings callback data.
        data: String,
        /// Display form of the effect error.
        message: String,
    },
    /// A known concrete callback handler is still owned by the downstream fetcher route.
    HandlerDelegated {
        /// Go handler group.
        handler: CallbackHandlerKind,
        /// Resolved callback action.
        action: String,
    },
}

/// Error returned when the delegated handler fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CallbackQueryUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

/// `UpdateHandler` adapter for Go callback query pre-handler behavior.
#[derive(Clone, Debug)]
pub struct CallbackQueryUpdateHandler<Effects, RateLimit, Next> {
    effects: Arc<Effects>,
    rate_limit: Arc<RateLimit>,
    next: Arc<Next>,
}

impl<Effects, RateLimit, Next> CallbackQueryUpdateHandler<Effects, RateLimit, Next> {
    /// Build a callback-query handler around the real downstream update handler.
    pub fn new(effects: Arc<Effects>, rate_limit: Arc<RateLimit>, next: Arc<Next>) -> Self {
        Self {
            effects,
            rate_limit,
            next,
        }
    }
}

impl<Effects, RateLimit, Next> UpdateHandler
    for CallbackQueryUpdateHandler<Effects, RateLimit, Next>
where
    Effects: CallbackQueryEffects + Send + Sync,
    RateLimit: CallbackRateLimitPolicy + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = CallbackQueryUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_callback_query_update_or_else(
                self.effects.as_ref(),
                self.rate_limit.as_ref(),
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

/// Handle Go `processCallbackQuery` terminal pre-handler routes and delegate known handlers.
pub async fn handle_callback_query_update_or_else<
    Effects,
    RateLimit,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    effects: &Effects,
    rate_limit: &RateLimit,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<CallbackQueryUpdateRoute, CallbackQueryUpdateError>
where
    Effects: CallbackQueryEffects + Sync,
    RateLimit: CallbackRateLimitPolicy + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let UpdateType::CallbackQuery(query) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| CallbackQueryUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(CallbackQueryUpdateRoute::Delegated);
    };

    let has_message = query.message.is_some();
    let chat_id = callback_message_chat_id(query);
    let is_rate_limited = match chat_id {
        Some(chat_id) => rate_limit.is_callback_chat_rate_limited(chat_id).await,
        None => false,
    };
    let data = query.data.as_deref().unwrap_or_default();
    let route = callback_query_route(has_message, is_rate_limited, data);

    match &route {
        CallbackQueryRoute::SkipRateLimited => Ok(CallbackQueryUpdateRoute::SkippedRateLimited {
            chat_id: chat_id.unwrap_or_default(),
        }),
        CallbackQueryRoute::Settings { data } => {
            let method = settings_callback_ack_method(query.id.clone());
            match effects.answer_callback_query(method).await {
                Ok(()) => Ok(CallbackQueryUpdateRoute::SettingsAcked { data: data.clone() }),
                Err(error) => {
                    let message = error.to_string();
                    tracing::warn!(
                        message,
                        query_id = query.id,
                        "failed to send settings callback response"
                    );
                    Ok(CallbackQueryUpdateRoute::SettingsAckError {
                        data: data.clone(),
                        message,
                    })
                }
            }
        }
        CallbackQueryRoute::Handle {
            handler, action, ..
        } => {
            let handler = *handler;
            let action = action.clone();
            handle_other(update)
                .await
                .map_err(|error| CallbackQueryUpdateError::Downstream {
                    message: error.to_string(),
                })?;
            Ok(CallbackQueryUpdateRoute::HandlerDelegated { handler, action })
        }
        _ => {
            let Some(method) = callback_query_ack_method(query.id.clone(), &route) else {
                return Ok(CallbackQueryUpdateRoute::Acked(route));
            };
            match effects.answer_callback_query(method).await {
                Ok(()) => Ok(CallbackQueryUpdateRoute::Acked(route)),
                Err(error) => {
                    let message = error.to_string();
                    tracing::warn!(
                        message,
                        query_id = query.id,
                        "failed to acknowledge callback query"
                    );
                    Ok(CallbackQueryUpdateRoute::AckError { route, message })
                }
            }
        }
    }
}

fn callback_message_chat_id(query: &TelegramCallbackQuery) -> Option<i64> {
    match query.message.as_ref()? {
        MaybeInaccessibleMessage::Message(message) => Some(message.chat.get_id().into()),
        MaybeInaccessibleMessage::InaccessibleMessage(message) => {
            Some(message.chat.get_id().into())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt, io,
        sync::{Arc, Mutex},
    };

    use carapax::types::Update as TelegramUpdate;
    use openplotva_telegram::{CallbackHandlerKind, TelegramOutboundMethod};
    use serde_json::{Value, json};

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};

    use super::{
        CallbackQueryEffects, CallbackQueryFuture, CallbackQueryUpdateHandler,
        CallbackQueryUpdateRoute, CallbackRateLimitFuture, CallbackRateLimitPolicy,
        handle_callback_query_update_or_else,
    };

    #[tokio::test]
    async fn callback_query_acks_legacy_data_without_delegation() -> Result<(), Box<dyn Error>> {
        let effects = CallbackEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_callback_query_update_or_else(
            &effects,
            &rate_limit,
            callback_query_update("old-format", true)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert!(matches!(route, CallbackQueryUpdateRoute::Acked(_)));
        assert_eq!(next.calls(), Vec::<i64>::new());
        let payloads = effects.payloads();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["callback_query_id"], "callback-query-id");
        assert!(payloads[0].get("text").is_none());
        assert!(payloads[0].get("cache_time").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn settings_callback_gets_cached_ack_without_delegation() -> Result<(), Box<dyn Error>> {
        let effects = CallbackEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_callback_query_update_or_else(
            &effects,
            &rate_limit,
            callback_query_update("settings:enable_global_text_reply=true", true)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            route,
            CallbackQueryUpdateRoute::SettingsAcked {
                data: "settings:enable_global_text_reply=true".to_owned()
            }
        );
        assert_eq!(next.calls(), Vec::<i64>::new());
        let payloads = effects.payloads();
        assert_eq!(payloads[0]["callback_query_id"], "callback-query-id");
        assert_eq!(payloads[0]["cache_time"], 10);
        Ok(())
    }

    #[tokio::test]
    async fn rate_limited_callback_skips_ack_and_delegation() -> Result<(), Box<dyn Error>> {
        let effects = CallbackEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::with_limited_chat(-10042);
        let next = UpdateHandlerStub::default();

        let route = handle_callback_query_update_or_else(
            &effects,
            &rate_limit,
            callback_query_update(r#"{"a":"delete"}"#, true)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            route,
            CallbackQueryUpdateRoute::SkippedRateLimited { chat_id: -10042 }
        );
        assert!(effects.payloads().is_empty());
        assert_eq!(next.calls(), Vec::<i64>::new());
        Ok(())
    }

    #[tokio::test]
    async fn known_callback_handler_delegates_once_without_ack() -> Result<(), Box<dyn Error>> {
        let effects = CallbackEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_callback_query_update_or_else(
            &effects,
            &rate_limit,
            callback_query_update(r#"{"a":"delete"}"#, true)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            route,
            CallbackQueryUpdateRoute::HandlerDelegated {
                handler: CallbackHandlerKind::Delete,
                action: "delete".to_owned(),
            }
        );
        assert!(effects.payloads().is_empty());
        assert_eq!(next.calls(), vec![5151]);
        Ok(())
    }

    #[tokio::test]
    async fn callback_query_update_handler_intercepts_before_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let effects = Arc::new(CallbackEffectsStub::default());
        let rate_limit = Arc::new(CallbackRateLimitStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = CallbackQueryUpdateHandler::new(
            Arc::clone(&effects),
            Arc::clone(&rate_limit),
            Arc::clone(&next),
        );

        handler
            .handle_update(callback_query_update("", true)?)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;

        assert_eq!(effects.payloads().len(), 1);
        assert!(next.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn callback_query_update_delegates_non_callback_once() -> Result<(), Box<dyn Error>> {
        let effects = CallbackEffectsStub::default();
        let rate_limit = CallbackRateLimitStub::default();
        let next = UpdateHandlerStub::default();

        let route =
            handle_callback_query_update_or_else(&effects, &rate_limit, text_update()?, |update| {
                next.handle_update(update)
            })
            .await?;

        assert_eq!(route, CallbackQueryUpdateRoute::Delegated);
        assert!(effects.payloads().is_empty());
        assert_eq!(next.calls(), vec![777]);
        Ok(())
    }

    fn callback_query_update(
        data: &str,
        include_message: bool,
    ) -> Result<TelegramUpdate, serde_json::Error> {
        let mut callback = json!({
            "id": "callback-query-id",
            "from": {
                "id": 7,
                "is_bot": false,
                "first_name": "Ada"
            },
            "chat_instance": "chat-instance",
            "data": data
        });
        if include_message {
            callback["message"] = json!({
                "message_id": 55,
                "date": 1_710_000_000,
                "chat": {
                    "id": -10042,
                    "type": "supergroup",
                    "title": "Plotva Lab"
                },
                "text": "controls"
            });
        }
        serde_json::from_value(json!({
            "update_id": 5151,
            "callback_query": callback,
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

    #[derive(Clone, Default)]
    struct CallbackEffectsStub {
        payloads: Arc<Mutex<Vec<Value>>>,
    }

    impl CallbackEffectsStub {
        fn payloads(&self) -> Vec<Value> {
            self.payloads.lock().expect("callback payloads").clone()
        }
    }

    impl CallbackQueryEffects for CallbackEffectsStub {
        type Error = StubError;

        fn answer_callback_query<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> CallbackQueryFuture<'a, Self::Error> {
            Box::pin(async move {
                let TelegramOutboundMethod::AnswerCallbackQuery(method) = method else {
                    return Err(StubError);
                };
                self.payloads
                    .lock()
                    .expect("callback payloads")
                    .push(serde_json::to_value(method.as_ref()).map_err(|_| StubError)?);
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct CallbackRateLimitStub {
        limited_chat_id: Option<i64>,
    }

    impl CallbackRateLimitStub {
        fn with_limited_chat(chat_id: i64) -> Self {
            Self {
                limited_chat_id: Some(chat_id),
            }
        }
    }

    impl CallbackRateLimitPolicy for CallbackRateLimitStub {
        fn is_callback_chat_rate_limited<'a>(
            &'a self,
            chat_id: i64,
        ) -> CallbackRateLimitFuture<'a> {
            Box::pin(async move { self.limited_chat_id == Some(chat_id) })
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
            f.write_str("callback effect failed")
        }
    }

    impl Error for StubError {}
}
