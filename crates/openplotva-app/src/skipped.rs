//! App-level no-op update processors that Go explicitly skips.

use std::{fmt, future::Future, sync::Arc};

use carapax::types::{Update as TelegramUpdate, UpdateType};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

/// Route chosen by the Go skipped-update wrapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SkippedUpdateRoute {
    /// The update was not a Go skipped type and was delegated.
    Delegated,
    /// Go accepts this update type from Telegram but logs and skips it after state extraction.
    Skipped { update_name: &'static str },
}

/// Error returned when delegated handling fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SkippedUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

/// `UpdateHandler` adapter for Go accepted-but-unprocessed update types.
#[derive(Clone, Debug)]
pub struct SkippedUpdateHandler<Next> {
    next: Arc<Next>,
}

impl<Next> SkippedUpdateHandler<Next> {
    /// Build a skipped-update handler around the real downstream update handler.
    pub fn new(next: Arc<Next>) -> Self {
        Self { next }
    }
}

impl<Next> UpdateHandler for SkippedUpdateHandler<Next>
where
    Next: UpdateHandler + Send + Sync,
{
    type Error = SkippedUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_skipped_update_or_else(update, |update| self.next.handle_update(update))
                .await
                .map(|_| ())
        })
    }
}

/// Consume Go accepted-but-unprocessed update types and delegate every other update.
pub async fn handle_skipped_update_or_else<HandleFn, HandleFuture, HandleError>(
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<SkippedUpdateRoute, SkippedUpdateError>
where
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    if let Some(update_name) = go_skipped_update_name(&update.update_type) {
        tracing::debug!(update_name, "skipping Go accepted update without processor");
        return Ok(SkippedUpdateRoute::Skipped { update_name });
    }

    handle_other(update)
        .await
        .map_err(|error| SkippedUpdateError::Downstream {
            message: error.to_string(),
        })?;
    Ok(SkippedUpdateRoute::Delegated)
}

/// Go `updateProcessors` intentionally omits these allowed update types.
#[must_use]
pub fn go_skipped_update_name(update: &UpdateType) -> Option<&'static str> {
    match update {
        UpdateType::ChosenInlineResult(_) => Some("chosen_inline_result"),
        UpdateType::ChatJoinRequest(_) => Some("chat_join_request"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io,
        sync::{Arc, Mutex},
    };

    use carapax::types::Update as TelegramUpdate;
    use serde_json::json;

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};

    use super::{SkippedUpdateHandler, SkippedUpdateRoute, handle_skipped_update_or_else};

    #[tokio::test]
    async fn chosen_inline_result_is_consumed_without_delegation() -> Result<(), Box<dyn Error>> {
        let next = UpdateHandlerStub::default();

        let route = handle_skipped_update_or_else(chosen_inline_update()?, |update| {
            next.handle_update(update)
        })
        .await?;

        assert_eq!(
            route,
            SkippedUpdateRoute::Skipped {
                update_name: "chosen_inline_result"
            }
        );
        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn chat_join_request_is_consumed_without_delegation() -> Result<(), Box<dyn Error>> {
        let next = UpdateHandlerStub::default();

        let route = handle_skipped_update_or_else(chat_join_request_update()?, |update| {
            next.handle_update(update)
        })
        .await?;

        assert_eq!(
            route,
            SkippedUpdateRoute::Skipped {
                update_name: "chat_join_request"
            }
        );
        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn message_update_delegates_once() -> Result<(), Box<dyn Error>> {
        let next = UpdateHandlerStub::default();

        let route =
            handle_skipped_update_or_else(text_update()?, |update| next.handle_update(update))
                .await?;

        assert_eq!(route, SkippedUpdateRoute::Delegated);
        assert_eq!(next.handled_count(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn handler_adapter_skips_before_wrapped_handler() -> Result<(), Box<dyn Error>> {
        let next = Arc::new(UpdateHandlerStub::default());
        let handler = SkippedUpdateHandler::new(Arc::clone(&next));

        handler.handle_update(chosen_inline_update()?).await?;

        assert_eq!(next.handled_count(), 0);

        Ok(())
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

    fn chosen_inline_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 201,
            "chosen_inline_result": {
                "result_id": "result-1",
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "query": "hello"
            }
        }))
    }

    fn chat_join_request_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 202,
            "chat_join_request": {
                "chat": {"id": -10042, "type": "supergroup", "title": "Team"},
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "user_chat_id": 111,
                "date": 1_710_000_000
            }
        }))
    }

    fn text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 203,
            "message": {
                "message_id": 1,
                "date": 1_710_000_000,
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada"
                },
                "text": "hello"
            }
        }))
    }
}
