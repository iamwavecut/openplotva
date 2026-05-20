//! App-level inline query behavior.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{AnswerInlineQuery, Update as TelegramUpdate, UpdateType};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

/// Boxed future returned by inline-query side effects.
pub type InlineQueryFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Side-effect boundary for Go `processInlineQuery`.
pub trait InlineQueryEffects {
    /// Error returned by the concrete effect.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Execute Telegram `answerInlineQuery`.
    fn answer_inline_query<'a>(
        &'a self,
        method: AnswerInlineQuery,
    ) -> InlineQueryFuture<'a, Self::Error>;
}

impl InlineQueryEffects for openplotva_telegram::TelegramClient {
    type Error = carapax::api::ExecuteError;

    fn answer_inline_query<'a>(
        &'a self,
        method: AnswerInlineQuery,
    ) -> InlineQueryFuture<'a, Self::Error> {
        Box::pin(async move { self.execute(method).await.map(|_: bool| ()) })
    }
}

/// Route chosen by the inline-query update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InlineQueryUpdateRoute {
    /// The update was not an inline query and was delegated.
    Delegated,
    Answered,
    /// Telegram answer failed; Go logs and consumes the update.
    AnswerError {
        /// Display form of the effect error.
        message: String,
    },
    /// Building the answer failed; this should stay unreachable for the Go plain-text result.
    BuildError {
        /// Display form of the builder error.
        message: String,
    },
}

/// Error returned when the delegated handler fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum InlineQueryUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

/// `UpdateHandler` adapter for Go inline query handling.
#[derive(Clone, Debug)]
pub struct InlineQueryUpdateHandler<Effects, Next> {
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Effects, Next> InlineQueryUpdateHandler<Effects, Next> {
    /// Build an inline-query handler around the real downstream update handler.
    pub fn new(effects: Arc<Effects>, next: Arc<Next>) -> Self {
        Self { effects, next }
    }
}

impl<Effects, Next> UpdateHandler for InlineQueryUpdateHandler<Effects, Next>
where
    Effects: InlineQueryEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = InlineQueryUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_inline_query_update_or_else(self.effects.as_ref(), update, |update| {
                self.next.handle_update(update)
            })
            .await
            .map(|_| ())
        })
    }
}

/// Handle Go's inline-query processor and delegate every non-inline update.
pub async fn handle_inline_query_update_or_else<Effects, HandleFn, HandleFuture, HandleError>(
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<InlineQueryUpdateRoute, InlineQueryUpdateError>
where
    Effects: InlineQueryEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let UpdateType::InlineQuery(query) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| InlineQueryUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(InlineQueryUpdateRoute::Delegated);
    };

    let method = match inline_query_answer_method(query) {
        Ok(method) => method,
        Err(error) => {
            let message = error.to_string();
            tracing::warn!(message, "failed to build inline query answer");
            return Ok(InlineQueryUpdateRoute::BuildError { message });
        }
    };

    match effects.answer_inline_query(method).await {
        Ok(()) => Ok(InlineQueryUpdateRoute::Answered),
        Err(error) => {
            let message = error.to_string();
            tracing::warn!(
                message,
                query_id = query.id,
                "failed to answer inline query"
            );
            Ok(InlineQueryUpdateRoute::AnswerError { message })
        }
    }
}

/// Build the Go `api.InlineConfig` response for one inline query.
pub fn inline_query_answer_method(
    query: &carapax::types::InlineQuery,
) -> Result<AnswerInlineQuery, openplotva_telegram::OutboundBuildError> {
    let result = openplotva_telegram::build_inline_query_result_article(
        &openplotva_telegram::InlineArticleRequest {
            id: query.id.clone(),
            title: "Шевелись, Плотва!".to_owned(),
            message_text: query.query.clone(),
            render_as: String::new(),
            description: String::new(),
            reply_markup: None,
        },
    )?;
    Ok(openplotva_telegram::build_inline_query_answer_method(
        &openplotva_telegram::InlineQueryAnswerRequest {
            inline_query_id: query.id.clone(),
            results: vec![result],
            cache_time: 1,
            is_personal: true,
            next_offset: String::new(),
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt, io,
        sync::{Arc, Mutex},
    };

    use carapax::types::{AnswerInlineQuery, Update as TelegramUpdate};
    use serde_json::json;

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};

    use super::{
        InlineQueryEffects, InlineQueryFuture, InlineQueryUpdateHandler, InlineQueryUpdateRoute,
        handle_inline_query_update_or_else, inline_query_answer_method,
    };

    #[test]
    fn inline_query_answer_method_matches_go_inline_config() -> Result<(), Box<dyn Error>> {
        let update = inline_query_update()?;
        let carapax::types::UpdateType::InlineQuery(query) = &update.update_type else {
            return Err("expected inline query update".into());
        };

        let method = inline_query_answer_method(query)?;
        let value = serde_json::to_value(method)?;

        assert_eq!(value["inline_query_id"], json!("inline-query-id"));
        assert_eq!(value["cache_time"], json!(1));
        assert_eq!(value["is_personal"], json!(true));
        assert_eq!(value["results"][0]["id"], json!("inline-query-id"));
        assert_eq!(value["results"][0]["title"], json!("Шевелись, Плотва!"));
        assert_eq!(
            value["results"][0]["input_message_content"]["message_text"],
            json!("хочу текст")
        );
        Ok(())
    }

    #[tokio::test]
    async fn inline_query_update_answers_without_delegation() -> Result<(), Box<dyn Error>> {
        let effects = InlineEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route =
            handle_inline_query_update_or_else(&effects, inline_query_update()?, |update| {
                next.handle_update(update)
            })
            .await?;

        assert_eq!(route, InlineQueryUpdateRoute::Answered);
        assert_eq!(
            effects.answered_query_ids(),
            vec!["inline-query-id".to_owned()]
        );
        assert!(next.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn inline_query_update_delegates_non_inline_once() -> Result<(), Box<dyn Error>> {
        let effects = InlineEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_inline_query_update_or_else(&effects, text_update()?, |update| {
            next.handle_update(update)
        })
        .await?;

        assert_eq!(route, InlineQueryUpdateRoute::Delegated);
        assert!(effects.answered_query_ids().is_empty());
        assert_eq!(next.calls(), vec![777]);
        Ok(())
    }

    #[tokio::test]
    async fn inline_query_update_handler_intercepts_before_wrapped_handler()
    -> Result<(), Box<dyn Error>> {
        let effects = Arc::new(InlineEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = InlineQueryUpdateHandler::new(Arc::clone(&effects), Arc::clone(&next));

        handler
            .handle_update(inline_query_update()?)
            .await
            .map_err(|error| io::Error::other(error.to_string()))?;

        assert_eq!(
            effects.answered_query_ids(),
            vec!["inline-query-id".to_owned()]
        );
        assert!(next.calls().is_empty());
        Ok(())
    }

    fn inline_query_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 999,
            "inline_query": {
                "id": "inline-query-id",
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "query": "хочу текст",
                "offset": ""
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
                "from": {
                    "id": 42,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "text": "hello"
            }
        }))
    }

    #[derive(Clone, Default)]
    struct InlineEffectsStub {
        methods: Arc<Mutex<Vec<AnswerInlineQuery>>>,
        fail: bool,
    }

    impl InlineEffectsStub {
        fn answered_query_ids(&self) -> Vec<String> {
            self.methods
                .lock()
                .expect("inline effects")
                .iter()
                .filter_map(|method| {
                    serde_json::to_value(method)
                        .ok()
                        .and_then(|value| value["inline_query_id"].as_str().map(str::to_owned))
                })
                .collect()
        }
    }

    impl InlineQueryEffects for InlineEffectsStub {
        type Error = StubError;

        fn answer_inline_query<'a>(
            &'a self,
            method: AnswerInlineQuery,
        ) -> InlineQueryFuture<'a, Self::Error> {
            Box::pin(async move {
                if self.fail {
                    return Err(StubError);
                }
                self.methods.lock().expect("inline effects").push(method);
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
            f.write_str("inline answer failed")
        }
    }
}
