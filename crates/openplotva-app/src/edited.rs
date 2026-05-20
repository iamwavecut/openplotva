//! App-level edited-message side effects.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{Update as TelegramUpdate, UpdateType, User as TelegramUser};
use openplotva_core::ChatMessageMeta;
use openplotva_updates::{
    build_fetcher_message_context, edited_image_prompt_update, parse_if_addressed,
};
use thiserror::Error;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};

/// Boxed future returned by edited-message side effects.
pub type EditedMessageFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Go-shaped data used to update pending dialog jobs after message edits.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EditedDialogJobUpdate<'payload> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID.
    pub message_id: i32,
    /// Message sender ID used by dialog debounce keys.
    pub sender_id: i64,
    /// Telegram forum topic/thread ID, when present.
    pub thread_id: Option<i32>,
    /// Go `parseIfAdressed` message text after fallback extraction.
    pub message_text: &'payload str,
    /// Original `Message.Text` before fallback extraction.
    pub original_text: &'payload str,
    /// Go-shaped message metadata.
    pub meta: &'payload ChatMessageMeta,
}

/// Go-shaped data used to update pending image jobs after message edits.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EditedImageJobUpdate<'payload> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Telegram message ID.
    pub message_id: i32,
    /// Prompt with edited message metadata appended.
    pub prompt: &'payload str,
    /// Trimmed prompt before metadata context is appended.
    pub original_prompt: &'payload str,
    /// Go-shaped message metadata.
    pub meta: &'payload ChatMessageMeta,
}

/// Side-effect boundary for Go `processEditedMessage` after history persistence.
pub trait EditedMessageEffects {
    /// Error returned by concrete effects.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Update an in-memory dialog debounce entry for this edited message.
    fn update_dialog_debounce<'a>(
        &'a self,
        update: EditedDialogJobUpdate<'a>,
    ) -> EditedMessageFuture<'a, bool, Self::Error>;

    /// Update a pending dialog job indexed by this edited message.
    fn update_pending_dialog_job<'a>(
        &'a self,
        update: EditedDialogJobUpdate<'a>,
    ) -> EditedMessageFuture<'a, bool, Self::Error>;

    /// Update a pending image job indexed by this edited draw message.
    fn update_pending_image_job<'a>(
        &'a self,
        update: EditedImageJobUpdate<'a>,
    ) -> EditedMessageFuture<'a, bool, Self::Error>;
}

/// Route chosen by the edited-message update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EditedMessageUpdateRoute {
    /// The update was not an edited message and was delegated.
    Delegated,
    /// Go ignores nil/zero-chat edited messages.
    IgnoredInvalidMessage,
    /// Go edited-message side effects were attempted and the update was consumed.
    Handled {
        /// Whether an existing dialog debounce entry was updated.
        dialog_debounce_updated: bool,
        /// Whether an existing pending dialog job was updated.
        pending_dialog_updated: bool,
        /// Whether an existing pending image job was updated.
        pending_image_updated: bool,
        /// Effect errors that Go would log and suppress.
        suppressed_errors: Vec<String>,
    },
}

/// Error returned when delegated handling fails.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum EditedMessageUpdateError {
    /// Downstream handler failed after delegation.
    #[error("downstream update handler: {message}")]
    Downstream {
        /// Display form of the downstream error.
        message: String,
    },
}

/// `UpdateHandler` adapter for Go edited-message side effects.
#[derive(Clone, Debug)]
pub struct EditedMessageUpdateHandler<Effects, Next> {
    effects: Arc<Effects>,
    bot_user: TelegramUser,
    next: Arc<Next>,
}

impl<Effects, Next> EditedMessageUpdateHandler<Effects, Next> {
    /// Build an edited-message handler around the real downstream update handler.
    pub fn new(effects: Arc<Effects>, bot_user: TelegramUser, next: Arc<Next>) -> Self {
        Self {
            effects,
            bot_user,
            next,
        }
    }
}

impl<Effects, Next> UpdateHandler for EditedMessageUpdateHandler<Effects, Next>
where
    Effects: EditedMessageEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = EditedMessageUpdateError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_edited_message_update_or_else(
                self.effects.as_ref(),
                &self.bot_user,
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

/// Handle Go's edited-message processor and delegate every non-edited update.
pub async fn handle_edited_message_update_or_else<Effects, HandleFn, HandleFuture, HandleError>(
    effects: &Effects,
    bot_user: &TelegramUser,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<EditedMessageUpdateRoute, EditedMessageUpdateError>
where
    Effects: EditedMessageEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let UpdateType::EditedMessage(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| EditedMessageUpdateError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(EditedMessageUpdateRoute::Delegated);
    };

    let chat_id: i64 = message.chat.get_id().into();
    let Ok(message_id) = i32::try_from(message.id) else {
        return Ok(EditedMessageUpdateRoute::IgnoredInvalidMessage);
    };
    if chat_id == 0 || message_id == 0 {
        return Ok(EditedMessageUpdateRoute::IgnoredInvalidMessage);
    }

    let context = build_fetcher_message_context(message);
    let parsed = parse_if_addressed(message, bot_user);
    let thread_id = message
        .message_thread_id
        .and_then(|thread_id| i32::try_from(thread_id).ok())
        .filter(|thread_id| *thread_id != 0);
    let dialog_update = EditedDialogJobUpdate {
        chat_id,
        message_id,
        sender_id: context.meta.sender_id,
        thread_id,
        message_text: &parsed.message_text,
        original_text: &context.original_text,
        meta: &context.meta,
    };

    let mut suppressed_errors = Vec::new();
    let dialog_debounce_updated = suppress_edited_effect(
        effects.update_dialog_debounce(dialog_update),
        &mut suppressed_errors,
        "update edited dialog debounce",
    )
    .await
    .unwrap_or(false);
    let pending_dialog_updated = suppress_edited_effect(
        effects.update_pending_dialog_job(dialog_update),
        &mut suppressed_errors,
        "update pending dialog job from edited message",
    )
    .await
    .unwrap_or(false);

    let pending_image_updated = if let Some(image) = edited_image_prompt_update(
        message,
        &parsed.first_word,
        &parsed.rest_text,
        &context.meta,
    ) {
        let image_update = EditedImageJobUpdate {
            chat_id,
            message_id,
            prompt: &image.prompt,
            original_prompt: &image.original_prompt,
            meta: &context.meta,
        };
        suppress_edited_effect(
            effects.update_pending_image_job(image_update),
            &mut suppressed_errors,
            "update pending image job from edited message",
        )
        .await
        .unwrap_or(false)
    } else {
        false
    };

    Ok(EditedMessageUpdateRoute::Handled {
        dialog_debounce_updated,
        pending_dialog_updated,
        pending_image_updated,
        suppressed_errors,
    })
}

async fn suppress_edited_effect<T, E>(
    effect: EditedMessageFuture<'_, T, E>,
    suppressed_errors: &mut Vec<String>,
    context: &'static str,
) -> Option<T>
where
    E: fmt::Display,
{
    match effect.await {
        Ok(value) => Some(value),
        Err(error) => {
            let message = error.to_string();
            tracing::warn!(message, context, "edited-message effect failed");
            suppressed_errors.push(message);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io,
        sync::{Arc, Mutex},
    };

    use carapax::types::{Update as TelegramUpdate, User as TelegramUser};
    use openplotva_core::ChatMessageMeta;
    use serde_json::json;

    use crate::updates::{UpdateHandler, UpdateHandlerFuture};

    use super::{
        EditedDialogJobUpdate, EditedImageJobUpdate, EditedMessageEffects, EditedMessageFuture,
        EditedMessageUpdateHandler, EditedMessageUpdateRoute, handle_edited_message_update_or_else,
    };

    #[tokio::test]
    async fn edited_message_updates_debounce_and_pending_dialog_job() -> Result<(), Box<dyn Error>>
    {
        let effects = EditedEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_edited_message_update_or_else(
            &effects,
            &sample_bot_user()?,
            edited_text_update(" hello ")?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            route,
            EditedMessageUpdateRoute::Handled {
                dialog_debounce_updated: true,
                pending_dialog_updated: true,
                pending_image_updated: false,
                suppressed_errors: vec![],
            }
        );
        assert_eq!(effects.dialog_updates().len(), 2);
        assert_eq!(effects.dialog_updates()[0].message_text, " hello ");
        assert_eq!(effects.dialog_updates()[0].original_text, " hello ");
        assert_eq!(effects.dialog_updates()[0].sender_id, 111);
        assert!(effects.image_updates().is_empty());
        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn edited_draw_message_updates_pending_image_job() -> Result<(), Box<dyn Error>> {
        let effects = EditedEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_edited_message_update_or_else(
            &effects,
            &sample_bot_user()?,
            edited_draw_update()?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            route,
            EditedMessageUpdateRoute::Handled {
                dialog_debounce_updated: true,
                pending_dialog_updated: true,
                pending_image_updated: true,
                suppressed_errors: vec![],
            }
        );
        let images = effects.image_updates();
        assert_eq!(images.len(), 1);
        assert_eq!(images[0].prompt, "кота");
        assert_eq!(images[0].original_prompt, "кота");
        assert_eq!(images[0].chat_id, 42);
        assert_eq!(images[0].message_id, 77);
        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn edited_effect_errors_are_suppressed_like_go_logs() -> Result<(), Box<dyn Error>> {
        let effects = EditedEffectsStub::failing();
        let next = UpdateHandlerStub::default();

        let route = handle_edited_message_update_or_else(
            &effects,
            &sample_bot_user()?,
            edited_draw_update()?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(
            route,
            EditedMessageUpdateRoute::Handled {
                dialog_debounce_updated: false,
                pending_dialog_updated: false,
                pending_image_updated: false,
                suppressed_errors: vec![
                    "effect failed".to_owned(),
                    "effect failed".to_owned(),
                    "effect failed".to_owned(),
                ],
            }
        );
        assert_eq!(next.handled_count(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn non_edited_update_delegates_once() -> Result<(), Box<dyn Error>> {
        let effects = EditedEffectsStub::default();
        let next = UpdateHandlerStub::default();

        let route = handle_edited_message_update_or_else(
            &effects,
            &sample_bot_user()?,
            text_update()?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(route, EditedMessageUpdateRoute::Delegated);
        assert_eq!(next.handled_count(), 1);
        assert!(effects.dialog_updates().is_empty());
        assert!(effects.image_updates().is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn handler_adapter_consumes_edited_messages() -> Result<(), Box<dyn Error>> {
        let effects = Arc::new(EditedEffectsStub::default());
        let next = Arc::new(UpdateHandlerStub::default());
        let handler =
            EditedMessageUpdateHandler::new(Arc::clone(&effects), sample_bot_user()?, next);

        handler.handle_update(edited_text_update("hi")?).await?;

        assert_eq!(effects.dialog_updates().len(), 2);

        Ok(())
    }

    #[derive(Clone, Debug, Default)]
    struct EditedEffectsStub {
        state: Arc<Mutex<EditedEffectsState>>,
    }

    #[derive(Debug, Default)]
    struct EditedEffectsState {
        dialog_updates: Vec<OwnedDialogJobUpdate>,
        image_updates: Vec<OwnedImageJobUpdate>,
        fail: bool,
    }

    impl EditedEffectsStub {
        fn failing() -> Self {
            let this = Self::default();
            this.state.lock().expect("state").fail = true;
            this
        }

        fn dialog_updates(&self) -> Vec<OwnedDialogJobUpdate> {
            self.state.lock().expect("state").dialog_updates.clone()
        }

        fn image_updates(&self) -> Vec<OwnedImageJobUpdate> {
            self.state.lock().expect("state").image_updates.clone()
        }
    }

    impl EditedMessageEffects for EditedEffectsStub {
        type Error = io::Error;

        fn update_dialog_debounce<'a>(
            &'a self,
            update: EditedDialogJobUpdate<'a>,
        ) -> EditedMessageFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                if state.fail {
                    return Err(io::Error::other("effect failed"));
                }
                state
                    .dialog_updates
                    .push(OwnedDialogJobUpdate::from(update));
                Ok(true)
            })
        }

        fn update_pending_dialog_job<'a>(
            &'a self,
            update: EditedDialogJobUpdate<'a>,
        ) -> EditedMessageFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                if state.fail {
                    return Err(io::Error::other("effect failed"));
                }
                state
                    .dialog_updates
                    .push(OwnedDialogJobUpdate::from(update));
                Ok(true)
            })
        }

        fn update_pending_image_job<'a>(
            &'a self,
            update: EditedImageJobUpdate<'a>,
        ) -> EditedMessageFuture<'a, bool, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("state");
                if state.fail {
                    return Err(io::Error::other("effect failed"));
                }
                state.image_updates.push(OwnedImageJobUpdate::from(update));
                Ok(true)
            })
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct OwnedDialogJobUpdate {
        chat_id: i64,
        message_id: i32,
        sender_id: i64,
        thread_id: Option<i32>,
        message_text: String,
        original_text: String,
        meta: ChatMessageMeta,
    }

    impl From<EditedDialogJobUpdate<'_>> for OwnedDialogJobUpdate {
        fn from(update: EditedDialogJobUpdate<'_>) -> Self {
            Self {
                chat_id: update.chat_id,
                message_id: update.message_id,
                sender_id: update.sender_id,
                thread_id: update.thread_id,
                message_text: update.message_text.to_owned(),
                original_text: update.original_text.to_owned(),
                meta: update.meta.clone(),
            }
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct OwnedImageJobUpdate {
        chat_id: i64,
        message_id: i32,
        prompt: String,
        original_prompt: String,
        meta: ChatMessageMeta,
    }

    impl From<EditedImageJobUpdate<'_>> for OwnedImageJobUpdate {
        fn from(update: EditedImageJobUpdate<'_>) -> Self {
            Self {
                chat_id: update.chat_id,
                message_id: update.message_id,
                prompt: update.prompt.to_owned(),
                original_prompt: update.original_prompt.to_owned(),
                meta: update.meta.clone(),
            }
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

    fn sample_bot_user() -> Result<TelegramUser, serde_json::Error> {
        serde_json::from_value(json!({
            "id": 777,
            "is_bot": true,
            "first_name": "Плотва",
            "username": "PlotvaBot"
        }))
    }

    fn text_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 100,
            "message": sample_message_json("hello")
        }))
    }

    fn edited_text_update(text: &str) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 101,
            "edited_message": sample_message_json(text)
        }))
    }

    fn edited_draw_update() -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 102,
            "edited_message": {
                "message_id": 77,
                "date": 1_710_000_000,
                "message_thread_id": 9,
                "chat": {"id": 42, "type": "private", "first_name": "Ada"},
                "from": {
                    "id": 111,
                    "is_bot": false,
                    "first_name": "Ada",
                    "username": "ada"
                },
                "text": "нарисуй кота"
            }
        }))
    }

    fn sample_message_json(text: &str) -> serde_json::Value {
        json!({
            "message_id": 77,
            "date": 1_710_000_000,
            "chat": {"id": 42, "type": "private", "first_name": "Ada"},
            "from": {
                "id": 111,
                "is_bot": false,
                "first_name": "Ada",
                "username": "ada"
            },
            "text": text
        })
    }
}
