//! App-level edited-message side effects.

use std::{fmt, future::Future, pin::Pin, sync::Arc};

use carapax::types::{Update as TelegramUpdate, UpdateType, User as TelegramUser};
use openplotva_core::ChatMessageMeta;
use openplotva_taskman::{
    DialogJobParams, InMemoryTaskQueue, JobStatus, JobType, TaskQueueRecord, TaskQueueSchedule,
    clean_unicode_non_printables,
};
use openplotva_updates::{
    build_fetcher_message_context, edited_image_prompt_update, parse_if_addressed,
};
use thiserror::Error;

use crate::{
    dialog_debounce::InMemoryDialogDebounce,
    dialog_messages::DialogTaskDurabilityBarrier,
    updates::{UpdateHandler, UpdateHandlerFuture},
};

/// Boxed future returned by edited-message side effects.
pub type EditedMessageFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

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
    pub message_text: &'payload str,
    /// Original `Message.Text` before fallback extraction.
    pub original_text: &'payload str,
    pub meta: &'payload ChatMessageMeta,
    pub source_update_id: i64,
}

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
    pub meta: &'payload ChatMessageMeta,
}

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

    /// Durability failures must escape the edited-message best-effort boundary
    /// so the durable update consumer retries instead of acknowledging loss.
    fn should_retry_error(&self, _error: &Self::Error) -> bool {
        false
    }
}

/// Route chosen by the edited-message update wrapper.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EditedMessageUpdateRoute {
    /// The update was not an edited message and was delegated.
    Delegated,
    IgnoredInvalidMessage,
    Handled {
        /// Whether an existing dialog debounce entry was updated.
        dialog_debounce_updated: bool,
        /// Whether an existing pending dialog job was updated.
        pending_dialog_updated: bool,
        /// Whether an existing pending image job was updated.
        pending_image_updated: bool,
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
    #[error("durable edited-message effect: {message}")]
    Durability { message: String },
}

#[derive(Clone, Debug)]
pub struct EditedMessageUpdateHandler<Effects, Next> {
    effects: Arc<Effects>,
    bot_user: TelegramUser,
    next: Arc<Next>,
}

/// Concrete edited-message effects backed by the shared taskman queue core.
#[derive(Clone)]
pub struct TaskmanEditedMessageEffects {
    queue: Arc<InMemoryTaskQueue>,
    dialog_debounce: Option<Arc<InMemoryDialogDebounce>>,
    durability: Arc<dyn DialogTaskDurabilityBarrier>,
}

impl TaskmanEditedMessageEffects {
    /// Build taskman-backed edited-message effects.
    #[must_use]
    pub fn new(
        queue: Arc<InMemoryTaskQueue>,
        durability: Arc<dyn DialogTaskDurabilityBarrier>,
    ) -> Self {
        Self {
            queue,
            dialog_debounce: None,
            durability,
        }
    }

    #[must_use]
    pub fn with_dialog_debounce(mut self, dialog_debounce: Arc<InMemoryDialogDebounce>) -> Self {
        self.dialog_debounce = Some(dialog_debounce);
        self
    }
}

impl fmt::Debug for TaskmanEditedMessageEffects {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskmanEditedMessageEffects")
            .finish_non_exhaustive()
    }
}

/// Error returned by taskman-backed edited-message effects.
#[derive(Debug, Error)]
pub enum TaskmanEditedMessageEffectsError {
    /// Chat metadata could not be converted into taskman JSON.
    #[error("serialize edited-message metadata: {0}")]
    SerializeMeta(#[from] serde_json::Error),
    #[error("persist edited dialog task {task_id}: {message}")]
    Durability { task_id: i64, message: String },
}

fn pending_dialog_record(
    queue: &InMemoryTaskQueue,
    chat_id: i64,
    message_id: i32,
) -> Option<TaskQueueRecord> {
    queue.records().into_iter().find(|record| {
        record.status == JobStatus::Pending
            && record.job.data.job_type == JobType::Dialog
            && record
                .job
                .data
                .telegram_data
                .as_ref()
                .is_some_and(|telegram| {
                    telegram.chat_id == chat_id && telegram.message_id == message_id
                })
            && record.job.data.dialog_data.is_some()
    })
}

impl EditedMessageEffects for TaskmanEditedMessageEffects {
    type Error = TaskmanEditedMessageEffectsError;

    fn update_dialog_debounce<'a>(
        &'a self,
        update: EditedDialogJobUpdate<'a>,
    ) -> EditedMessageFuture<'a, bool, Self::Error> {
        Box::pin(async move {
            let Some(dialog_debounce) = self.dialog_debounce.as_ref() else {
                return Ok(false);
            };
            Ok(dialog_debounce.update(update))
        })
    }

    fn update_pending_dialog_job<'a>(
        &'a self,
        update: EditedDialogJobUpdate<'a>,
    ) -> EditedMessageFuture<'a, bool, Self::Error> {
        Box::pin(async move {
            let Some(record) =
                pending_dialog_record(&self.queue, update.chat_id, update.message_id)
            else {
                return Ok(false);
            };
            let meta = serde_json::to_value(update.meta)?;
            let task_id = if record.debounce_key.is_some() {
                let telegram = record
                    .job
                    .data
                    .telegram_data
                    .as_ref()
                    .expect("matched dialog record has Telegram data");
                let dialog = record
                    .job
                    .data
                    .dialog_data
                    .as_ref()
                    .expect("matched dialog record has dialog data");
                let params = DialogJobParams {
                    chat_id: update.chat_id,
                    message_id: update.message_id,
                    user_id: telegram.user_id,
                    user_full_name: telegram.user_full_name.clone(),
                    message_text: clean_unicode_non_printables(update.message_text),
                    original_text: update.original_text.trim().to_owned(),
                    meta: meta.clone(),
                    max_output_tokens: dialog.max_output_tokens,
                    thread_id: update.thread_id.or(telegram.thread_message_id),
                };
                let mut job = record.job.clone();
                let dialog = job
                    .data
                    .dialog_data
                    .as_mut()
                    .expect("matched dialog record has dialog data");
                dialog.message_text = params.message_text.clone();
                dialog.original_text = params.original_text.clone();
                dialog.meta = meta;
                let Some(report) = self.queue.update_pending_schedule(
                    record.id,
                    job,
                    TaskQueueSchedule {
                        available_at: record.available_at,
                        debounce_key: record.debounce_key,
                        lane_key: record.lane_key,
                        source_update_ids: vec![update.source_update_id],
                        latest_update_id: Some(update.source_update_id),
                        pending_dialog_inputs: vec![params],
                    },
                ) else {
                    return Ok(false);
                };
                report.task_id
            } else {
                if !self.queue.update_pending_dialog_job_by_message_id(
                    update.chat_id,
                    update.message_id,
                    update.message_text,
                    update.original_text,
                    meta,
                ) {
                    return Ok(false);
                }
                record.id
            };
            self.durability
                .durability_barrier(task_id)
                .await
                .map_err(|message| TaskmanEditedMessageEffectsError::Durability {
                    task_id,
                    message,
                })?;
            Ok(true)
        })
    }

    fn update_pending_image_job<'a>(
        &'a self,
        update: EditedImageJobUpdate<'a>,
    ) -> EditedMessageFuture<'a, bool, Self::Error> {
        Box::pin(async move {
            let meta = serde_json::to_value(update.meta)?;
            Ok(self.queue.update_pending_image_job_by_message_id(
                update.chat_id,
                update.message_id,
                update.prompt,
                update.original_prompt,
                meta,
            ))
        })
    }

    fn should_retry_error(&self, error: &Self::Error) -> bool {
        matches!(error, TaskmanEditedMessageEffectsError::Durability { .. })
    }
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
        source_update_id: update.id,
    };

    let mut suppressed_errors = Vec::new();
    let dialog_debounce_updated = suppress_edited_effect(
        effects.update_dialog_debounce(dialog_update),
        &mut suppressed_errors,
        "update edited dialog debounce",
    )
    .await
    .unwrap_or(false);
    let pending_dialog_updated = match effects.update_pending_dialog_job(dialog_update).await {
        Ok(updated) => updated,
        Err(error) if effects.should_retry_error(&error) => {
            return Err(EditedMessageUpdateError::Durability {
                message: error.to_string(),
            });
        }
        Err(error) => {
            suppress_edited_error(
                error,
                &mut suppressed_errors,
                "update pending dialog job from edited message",
            );
            false
        }
    };

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
            suppress_edited_error(error, suppressed_errors, context);
            None
        }
    }
}

fn suppress_edited_error(
    error: impl fmt::Display,
    suppressed_errors: &mut Vec<String>,
    context: &'static str,
) {
    let message = error.to_string();
    tracing::warn!(message, context, "edited-message effect failed");
    suppressed_errors.push(message);
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        io,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use carapax::types::{Update as TelegramUpdate, User as TelegramUser};
    use openplotva_core::{ChatMessageMeta, ChatState, UserState};
    use openplotva_storage::TelegramFileMetadataUpsert;
    use openplotva_taskman::{
        DEFAULT_PRIORITY, DIALOG_AIFARM_QUEUE_NAME, DialogJobData, DialogJobParams,
        IMAGE_REGULAR_QUEUE_NAME, ImageJobData, InMemoryTaskQueue, JobPayload, JobType,
        StatelessJobItem, TaskQueueSchedule, TelegramData,
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;
    use time::OffsetDateTime;

    use crate::dialog_debounce::{DialogDebounceKey, InMemoryDialogDebounce};
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };

    use super::{
        EditedDialogJobUpdate, EditedImageJobUpdate, EditedMessageEffects, EditedMessageFuture,
        EditedMessageUpdateHandler, EditedMessageUpdateRoute, TaskmanEditedMessageEffects,
        handle_edited_message_update_or_else,
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

    #[tokio::test]
    async fn taskman_edited_effects_update_pending_dialog_and_image_jobs()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            taskman_dialog_job("old dialog", now, 111, 42, 77),
        );
        queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            taskman_image_job("old prompt", now, 111, 42, 77),
        );
        let effects = TaskmanEditedMessageEffects::new(
            queue.clone(),
            Arc::new(crate::dialog_messages::NoopDialogTaskDurabilityBarrier),
        );
        let meta = ChatMessageMeta {
            sender_id: 111,
            sender_type: "user".to_owned(),
            sender_name: "Ada".to_owned(),
            ..ChatMessageMeta::default()
        };

        assert!(
            effects
                .update_pending_dialog_job(EditedDialogJobUpdate {
                    chat_id: 42,
                    message_id: 77,
                    sender_id: 111,
                    thread_id: None,
                    message_text: "\u{200f}new\ttext ",
                    original_text: " original text ",
                    meta: &meta,
                    source_update_id: 200,
                })
                .await?
        );
        assert!(
            effects
                .update_pending_image_job(EditedImageJobUpdate {
                    chat_id: 42,
                    message_id: 77,
                    prompt: "\u{200f}new\tprompt ",
                    original_prompt: " raw prompt ",
                    meta: &meta,
                })
                .await?
        );

        let records = queue.records();
        let dialog = records
            .iter()
            .find_map(|record| record.job.data.dialog_data.as_ref())
            .expect("dialog data");
        assert_eq!(dialog.message_text, "new text");
        assert_eq!(dialog.original_text, "original text");
        assert_eq!(dialog.meta["sender_id"], 111);
        assert_eq!(dialog.meta["sender_type"], "user");

        let image = records
            .iter()
            .find_map(|record| record.job.data.image_data.as_ref())
            .expect("image data");
        assert_eq!(image.prompt, "new prompt");
        assert_eq!(image.original_text, "raw prompt");
        assert!(image.prompt_variants.is_empty());
        assert_eq!(image.meta["sender_name"], "Ada");
        Ok(())
    }

    #[tokio::test]
    async fn taskman_edited_effects_update_the_durable_debounced_task_in_place()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 9,
        };
        let initial_input = DialogJobParams {
            chat_id: 42,
            message_id: 77,
            user_id: 111,
            user_full_name: "Ada".to_owned(),
            message_text: "old dialog".to_owned(),
            original_text: "old dialog".to_owned(),
            meta: json!({}),
            max_output_tokens: 0,
            thread_id: Some(9),
        };
        let scheduled = queue.schedule_or_replace(
            DIALOG_AIFARM_QUEUE_NAME,
            taskman_dialog_job("old dialog", OffsetDateTime::UNIX_EPOCH, 111, 42, 77),
            TaskQueueSchedule {
                available_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5)),
                debounce_key: Some(key.durable_debounce_key()),
                lane_key: Some(key.durable_lane_key()),
                source_update_ids: vec![200],
                latest_update_id: Some(200),
                pending_dialog_inputs: vec![initial_input],
            },
        );
        debounce.track(key, 77, DIALOG_AIFARM_QUEUE_NAME, scheduled.task_id);
        let effects = TaskmanEditedMessageEffects::new(
            Arc::clone(&queue),
            Arc::new(crate::dialog_messages::NoopDialogTaskDurabilityBarrier),
        )
        .with_dialog_debounce(debounce.clone());
        let meta = ChatMessageMeta {
            sender_id: 111,
            sender_type: "user".to_owned(),
            sender_name: "Ada".to_owned(),
            ..ChatMessageMeta::default()
        };

        let update = EditedDialogJobUpdate {
            chat_id: 42,
            message_id: 77,
            sender_id: 111,
            thread_id: Some(9),
            message_text: "\u{200f}new\ttext ",
            original_text: " original text ",
            meta: &meta,
            source_update_id: 201,
        };
        assert!(effects.update_dialog_debounce(update).await?);
        assert!(effects.update_pending_dialog_job(update).await?);

        let records = queue.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, scheduled.task_id);
        assert_eq!(records[0].source_update_ids, vec![200, 201]);
        assert_eq!(records[0].latest_update_id, Some(201));
        assert_eq!(records[0].pending_dialog_inputs.len(), 1);
        assert_eq!(records[0].pending_dialog_inputs[0].message_text, "new text");
        let dialog = records[0].job.data.dialog_data.as_ref().expect("dialog");
        assert_eq!(dialog.message_text, "new text");
        assert_eq!(dialog.original_text, "original text");
        assert_eq!(dialog.meta["sender_name"], "Ada");
        Ok(())
    }

    #[tokio::test]
    async fn edited_dialog_durability_failure_escapes_for_update_retry()
    -> Result<(), Box<dyn Error>> {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };
        let initial_input = DialogJobParams {
            chat_id: 42,
            message_id: 77,
            user_id: 111,
            user_full_name: "Ada".to_owned(),
            message_text: "old".to_owned(),
            original_text: "old".to_owned(),
            meta: json!({}),
            max_output_tokens: 0,
            thread_id: None,
        };
        queue.schedule_or_replace(
            DIALOG_AIFARM_QUEUE_NAME,
            taskman_dialog_job("old", OffsetDateTime::UNIX_EPOCH, 111, 42, 77),
            TaskQueueSchedule {
                available_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(5)),
                debounce_key: Some(key.durable_debounce_key()),
                lane_key: Some(key.durable_lane_key()),
                source_update_ids: vec![100],
                latest_update_id: Some(100),
                pending_dialog_inputs: vec![initial_input],
            },
        );
        let effects = TaskmanEditedMessageEffects::new(queue, Arc::new(FailingDurabilityBarrier));

        let error = handle_edited_message_update_or_else(
            &effects,
            &sample_bot_user()?,
            edited_text_update("new")?,
            |_update| async { Err::<(), _>(io::Error::other("should not delegate")) },
        )
        .await
        .expect_err("durability failure must retry the update");

        assert!(matches!(
            error,
            super::EditedMessageUpdateError::Durability { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_edited_draw_updates_pending_jobs_when_url_is_set()
    -> Result<(), Box<dyn Error>> {
        let Ok(redis_url) = std::env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-edited-draw:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        update_queue
            .enqueue_update(&edited_draw_update_at(now_secs as i64)?)
            .await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded edited draw update"))?;

        let state_store = UpdateStateStoreStub::default();
        let queue = Arc::new(InMemoryTaskQueue::new());
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let dialog_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            taskman_dialog_job("old dialog", created, 111, 42, 77),
        );
        queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            taskman_image_job("old prompt", created, 111, 42, 77),
        );
        let debounce_key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 9,
        };
        debounce.track(debounce_key, 77, DIALOG_AIFARM_QUEUE_NAME, dialog_id);
        let effects = TaskmanEditedMessageEffects::new(
            Arc::clone(&queue),
            Arc::new(crate::dialog_messages::NoopDialogTaskDurabilityBarrier),
        )
        .with_dialog_debounce(debounce.clone());
        let next = UpdateHandlerStub::default();
        let route = Arc::new(Mutex::new(None));
        let captured_route = Arc::clone(&route);

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
            |update| async {
                let handled = handle_edited_message_update_or_else(
                    &effects,
                    &sample_bot_user()?,
                    update,
                    |update| next.handle_update(update),
                )
                .await
                .map_err(|error| io::Error::other(error.to_string()))?;
                *captured_route.lock().expect("route") = Some(handled);
                Ok::<(), io::Error>(())
            },
        )
        .await;

        assert_eq!(report.update_id, 102);
        assert_eq!(report.update_name, "edited_message");
        assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
        assert_eq!(
            report.handle.as_ref().map(|stage| &stage.outcome),
            Some(&UpdateStageOutcome::Completed)
        );
        assert!(!report.skipped_handle);
        assert_eq!(
            state_store.calls(),
            vec![
                "chat:42:private:Ada:".to_owned(),
                "user:111:Ada:ada".to_owned()
            ]
        );
        assert_eq!(
            *route.lock().expect("route"),
            Some(EditedMessageUpdateRoute::Handled {
                dialog_debounce_updated: true,
                pending_dialog_updated: true,
                pending_image_updated: true,
                suppressed_errors: vec![],
            })
        );
        assert_eq!(next.handled_count(), 0);

        let records = queue.records();
        assert_eq!(records.len(), 2);
        let dialog = records
            .iter()
            .find_map(|record| record.job.data.dialog_data.as_ref())
            .expect("dialog data");
        assert_eq!(dialog.message_text, "нарисуй кота");
        assert_eq!(dialog.original_text, "нарисуй кота");
        assert_eq!(dialog.meta["sender_id"], 111);
        let image = records
            .iter()
            .find_map(|record| record.job.data.image_data.as_ref())
            .expect("image data");
        assert_eq!(image.prompt, "кота");
        assert_eq!(image.original_text, "кота");
        assert!(image.prompt_variants.is_empty());
        assert_eq!(image.meta["sender_name"], "Ada");

        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
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
        source_update_id: i64,
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
                source_update_id: update.source_update_id,
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

    struct FailingDurabilityBarrier;

    impl crate::dialog_messages::DialogTaskDurabilityBarrier for FailingDurabilityBarrier {
        fn durability_barrier(
            &self,
            _task_id: i64,
        ) -> crate::dialog_messages::DialogTaskDurabilityFuture<'_> {
            Box::pin(async { Err("database unavailable".to_owned()) })
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
        edited_draw_update_at(1_710_000_000)
    }

    fn edited_draw_update_at(date: i64) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 102,
            "edited_message": {
                "message_id": 77,
                "date": date,
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

    #[derive(Clone, Debug, Default)]
    struct UpdateStateStoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl UpdateStateStoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }
    }

    impl UpdateStateStore for UpdateStateStoreStub {
        type Error = io::Error;

        fn upsert_chat_state<'a>(
            &'a self,
            chat: &'a ChatState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls.lock().expect("calls").push(format!(
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
                self.calls.lock().expect("calls").push(format!(
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
            _params: &'a TelegramFileMetadataUpsert,
        ) -> TelegramFileMetadataStoreFuture<'a, Self::Error> {
            Box::pin(async { Ok(()) })
        }
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

    fn taskman_dialog_job(
        message_text: &str,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> StatelessJobItem {
        StatelessJobItem {
            title: "dialog".to_owned(),
            created,
            priority: DEFAULT_PRIORITY,
            processing_timeout_seconds: 0,
            data: JobPayload {
                job_type: JobType::Dialog,
                telegram_data: Some(taskman_telegram_data(user_id, chat_id, message_id)),
                image_data: None,
                music_data: None,
                dialog_data: Some(DialogJobData {
                    message_text: message_text.to_owned(),
                    ..DialogJobData::default()
                }),
                asr_data: None,
                control_data: None,
                agent_data: None,
            },
        }
    }

    fn taskman_image_job(
        prompt: &str,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> StatelessJobItem {
        StatelessJobItem {
            title: "image_gen".to_owned(),
            created,
            priority: DEFAULT_PRIORITY,
            processing_timeout_seconds: 0,
            data: JobPayload {
                job_type: JobType::ImageGen,
                telegram_data: Some(taskman_telegram_data(user_id, chat_id, message_id)),
                image_data: Some(ImageJobData {
                    prompt: prompt.to_owned(),
                    original_text: prompt.to_owned(),
                    prompt_variants: vec!["enhanced".to_owned()],
                    ..ImageJobData::default()
                }),
                music_data: None,
                dialog_data: None,
                asr_data: None,
                control_data: None,
                agent_data: None,
            },
        }
    }

    fn taskman_telegram_data(user_id: i64, chat_id: i64, message_id: i32) -> TelegramData {
        TelegramData {
            chat_id,
            user_id,
            message_id,
            thread_message_id: None,
            user_full_name: "Ada".to_owned(),
            chat_title: String::new(),
        }
    }
}
