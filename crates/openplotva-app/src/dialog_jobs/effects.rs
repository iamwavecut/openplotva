//! Outbound side effects of a dialog turn: the effects trait and the
//! dispatcher-backed answer sender.

use std::{fmt, sync::Arc};

use openplotva_storage::{
    PostgresTelegramOutboxStore, TelegramDeliveryPolicy, TelegramOutboxBatchInput,
    TelegramOutboxPartInput,
};
use openplotva_taskman::{DEFAULT_PRIORITY, DialogJobParams};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, ReplyMessageRef, RichMessageRequest, TELEGRAM_PARSE_MODE_HTML,
    TelegramOutboundMethod, TextMessageRequest, build_rich_message_method,
    build_text_message_methods, snapshot_outbound_method,
};
use sqlx::Row;
use thiserror::Error;
use time::OffsetDateTime;

use crate::virtual_messages::{
    QueueRichRequest, QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory,
    queue_rich_message, queue_text_message_parts,
};

use super::{
    DialogJobEffectFuture, DialogJobReceiptFuture, DialogJobReceiptLookupFuture,
    dialog_response_requires_rich,
};

/// Identity returned after all final-answer parts were accepted by one queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedBatchReceipt {
    pub batch_id: String,
    pub operation_ids: Vec<String>,
    delivery: QueuedDelivery,
    delivery_complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueuedDelivery {
    Dispatcher,
    DurableOutbox,
}

impl QueuedBatchReceipt {
    pub(crate) fn dispatcher(batch_id: String, operation_ids: Vec<String>) -> Self {
        Self {
            batch_id,
            operation_ids,
            delivery: QueuedDelivery::Dispatcher,
            delivery_complete: false,
        }
    }

    pub(crate) fn durable_outbox(
        batch_id: String,
        operation_ids: Vec<String>,
        delivery_complete: bool,
    ) -> Self {
        Self {
            batch_id,
            operation_ids,
            delivery: QueuedDelivery::DurableOutbox,
            delivery_complete,
        }
    }

    /// Whether taskman must wait for real Telegram receipts before completion.
    #[must_use]
    pub fn requires_delivery_wait(&self) -> bool {
        self.delivery == QueuedDelivery::DurableOutbox
    }

    /// Whether every durable part already has a real Telegram receipt.
    #[must_use]
    pub fn delivery_complete(&self) -> bool {
        self.requires_delivery_wait() && self.delivery_complete
    }
}

/// Side effects performed after a provider returns a dialog answer.
pub trait DialogJobEffects {
    /// Error returned by concrete side effects.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_dialog_answer<'a>(
        &'a self,
        dialog_job_id: i64,
        causation_update_id: Option<i64>,
        params: &'a DialogJobParams,
        answer: &'a str,
    ) -> DialogJobReceiptFuture<'a, Self::Error>;

    /// Return the already committed final-answer batch, if a crash happened
    /// after its Postgres transaction but before taskman entered
    /// `WaitingDelivery`. The default keeps legacy/test effects compatible.
    fn queued_dialog_answer<'a>(
        &'a self,
        _dialog_job_id: i64,
    ) -> DialogJobReceiptLookupFuture<'a, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    /// Queue one intermediate session message (`send_message` tool or text
    /// alongside tool calls). `seq` is the 1-based per-session send counter;
    /// `reply_to_trigger` is true only for the session's first outgoing
    /// message so groups do not get a quote header on every part.
    fn send_dialog_intermediate<'a>(
        &'a self,
        dialog_job_id: i64,
        causation_update_id: Option<i64>,
        params: &'a DialogJobParams,
        text: &'a str,
        seq: u32,
        reply_to_trigger: bool,
    ) -> DialogJobEffectFuture<'a, Self::Error>;
}

#[derive(Clone)]
pub struct DialogDispatcherEffects {
    queue: Arc<DispatcherQueue>,
    next_virtual_id: VirtualIdFactory,
    durable_outbox: Option<DurableDialogOutbox>,
}

#[derive(Clone)]
struct DurableDialogOutbox {
    store: PostgresTelegramOutboxStore,
    bot_id: i64,
}

impl DialogDispatcherEffects {
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>) -> Self {
        Self {
            queue,
            next_virtual_id: monotonic_virtual_id_factory("dialog-vmsg"),
            durable_outbox: None,
        }
    }

    /// Route final answers through the Postgres outbox. Intermediate session
    /// messages remain on the dispatcher until their own durable operation
    /// families are migrated.
    #[must_use]
    pub fn with_durable_outbox(mut self, store: PostgresTelegramOutboxStore, bot_id: i64) -> Self {
        self.durable_outbox = Some(DurableDialogOutbox { store, bot_id });
        self
    }

    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: VirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

/// Concrete dispatch failure.
#[derive(Debug, Error)]
pub enum DialogDispatchEffectError {
    #[error("failed to queue dialog answer: {0}")]
    Queue(#[from] openplotva_telegram::OutboundBuildError),
    #[error("failed to persist dialog answer in Telegram outbox: {0}")]
    Outbox(#[from] openplotva_storage::StorageError),
    #[error("failed to encode dialog answer for Telegram outbox: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("Telegram method {0} cannot be persisted in the durable outbox")]
    UnsupportedMethod(&'static str),
}

impl DialogJobEffects for DialogDispatcherEffects {
    type Error = DialogDispatchEffectError;

    fn send_dialog_answer<'a>(
        &'a self,
        dialog_job_id: i64,
        causation_update_id: Option<i64>,
        params: &'a DialogJobParams,
        answer: &'a str,
    ) -> DialogJobReceiptFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat = ChatRef {
                id: params.chat_id,
                is_forum: params.thread_id.is_some(),
            };
            let reply_to = ReplyMessageRef {
                message_id: i64::from(params.message_id),
                chat,
                is_topic_message: params.thread_id.is_some(),
                message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            };
            if let Some(outbox) = &self.durable_outbox {
                return enqueue_durable_dialog_answer(
                    outbox,
                    dialog_job_id,
                    causation_update_id,
                    params,
                    answer,
                    chat,
                    &reply_to,
                )
                .await;
            }
            // Dialog answers must survive queue trims, and the reply-scoped
            // debounce key stops identical answers to different trigger
            // messages from deduping each other (a duplicate reply to the
            // same message is still suppressed).
            let debounce_key = format!("r{}", params.message_id);
            let report = if dialog_response_requires_rich(answer) {
                let request = RichMessageRequest {
                    chat: Some(chat),
                    message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    html: answer.to_owned(),
                    reply_markup: None,
                };
                queue_rich_message(
                    &self.queue,
                    QueueRichRequest {
                        message: &request,
                        reply_to: Some(&reply_to),
                        immediate: true,
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: None,
                        protected: true,
                        debounce_key: Some(&debounce_key),
                    },
                    || (self.next_virtual_id)(),
                )
                .await?
            } else {
                let request = TextMessageRequest {
                    chat: Some(chat),
                    message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    text: answer.to_owned(),
                    render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                    reply_markup: None,
                };
                queue_text_message_parts(
                    &self.queue,
                    QueueTextRequest {
                        protected: true,
                        debounce_key: Some(&debounce_key),
                        message: &request,
                        reply_to: Some(&reply_to),
                        immediate_first: true,
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: None,
                    },
                    || (self.next_virtual_id)(),
                )
                .await?
            };
            Ok(QueuedBatchReceipt::dispatcher(
                format!("dispatcher:dialog-answer:{dialog_job_id}"),
                report
                    .parts
                    .into_iter()
                    .map(|part| part.virtual_id)
                    .collect(),
            ))
        })
    }

    fn queued_dialog_answer<'a>(
        &'a self,
        dialog_job_id: i64,
    ) -> DialogJobReceiptLookupFuture<'a, Self::Error> {
        Box::pin(async move {
            let Some(outbox) = &self.durable_outbox else {
                return Ok(None);
            };
            let batch_id = dialog_answer_batch_id(outbox.bot_id, dialog_job_id);
            let rows = sqlx::query(
                "SELECT operation_id, state FROM telegram_outbox \
                 WHERE batch_id = $1 AND dialog_job_id = $2 \
                 ORDER BY part_index",
            )
            .bind(&batch_id)
            .bind(dialog_job_id)
            .fetch_all(outbox.store.pool())
            .await
            .map_err(openplotva_storage::StorageError::from)?;
            if rows.is_empty() {
                return Ok(None);
            }
            let delivery_complete = rows.iter().all(|row| {
                matches!(
                    row.try_get::<String, _>("state").as_deref(),
                    Ok("delivered")
                )
            });
            let operation_ids = rows
                .into_iter()
                .map(|row| row.try_get("operation_id"))
                .collect::<Result<Vec<String>, sqlx::Error>>()
                .map_err(openplotva_storage::StorageError::from)?;
            Ok(Some(QueuedBatchReceipt::durable_outbox(
                batch_id,
                operation_ids,
                delivery_complete,
            )))
        })
    }

    fn send_dialog_intermediate<'a>(
        &'a self,
        dialog_job_id: i64,
        causation_update_id: Option<i64>,
        params: &'a DialogJobParams,
        text: &'a str,
        seq: u32,
        reply_to_trigger: bool,
    ) -> DialogJobEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat = ChatRef {
                id: params.chat_id,
                is_forum: params.thread_id.is_some(),
            };
            let reply_to = ReplyMessageRef {
                message_id: i64::from(params.message_id),
                chat,
                is_topic_message: params.thread_id.is_some(),
                message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            };
            let reply_to = reply_to_trigger.then_some(&reply_to);
            if let Some(outbox) = &self.durable_outbox {
                enqueue_durable_dialog_intermediate(
                    outbox,
                    dialog_job_id,
                    causation_update_id,
                    params,
                    text,
                    seq,
                    chat,
                    reply_to,
                )
                .await?;
                return Ok(());
            }
            // Per-send deterministic key: distinct sends of one session never
            // dedupe each other, while a crash replay of the same seq+text is
            // absorbed by the outbound window. Intermediates deliberately go
            // NON-immediate — the dispatcher's dedup and per-chat pacing only
            // apply to non-immediate items, and mid-session messages have no
            // reason to jump the per-chat queue.
            let debounce_key = format!("r{}.s{seq}", params.message_id);
            if dialog_response_requires_rich(text) {
                let request = RichMessageRequest {
                    chat: Some(chat),
                    message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    html: text.to_owned(),
                    reply_markup: None,
                };
                queue_rich_message(
                    &self.queue,
                    QueueRichRequest {
                        message: &request,
                        reply_to,
                        immediate: false,
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: None,
                        protected: true,
                        debounce_key: Some(&debounce_key),
                    },
                    || (self.next_virtual_id)(),
                )
                .await?;
            } else {
                let request = TextMessageRequest {
                    chat: Some(chat),
                    message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    text: text.to_owned(),
                    render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                    reply_markup: None,
                };
                queue_text_message_parts(
                    &self.queue,
                    QueueTextRequest {
                        protected: true,
                        debounce_key: Some(&debounce_key),
                        message: &request,
                        reply_to,
                        immediate_first: false,
                        bypass_chat_restrictions: false,
                        ephemeral_delete_after: None,
                    },
                    || (self.next_virtual_id)(),
                )
                .await?;
            }
            Ok(())
        })
    }
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_durable_dialog_intermediate(
    outbox: &DurableDialogOutbox,
    dialog_job_id: i64,
    causation_update_id: Option<i64>,
    params: &DialogJobParams,
    text: &str,
    seq: u32,
    chat: ChatRef,
    reply_to: Option<&ReplyMessageRef>,
) -> Result<(), DialogDispatchEffectError> {
    let methods = if dialog_response_requires_rich(text) {
        let request = RichMessageRequest {
            chat: Some(chat),
            message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            html: text.to_owned(),
            reply_markup: None,
        };
        vec![TelegramOutboundMethod::from(build_rich_message_method(
            &request, chat, reply_to,
        )?)]
    } else {
        let request = TextMessageRequest {
            chat: Some(chat),
            message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            text: text.to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };
        build_text_message_methods(&request, reply_to)?
            .into_iter()
            .map(TelegramOutboundMethod::from)
            .collect()
    };
    let now = OffsetDateTime::now_utc();
    let mut parts = Vec::with_capacity(methods.len());
    for method in &methods {
        let method_kind = method.method_name();
        let Some((_kind, payload)) = snapshot_outbound_method(method) else {
            return Err(DialogDispatchEffectError::UnsupportedMethod(method_kind));
        };
        parts.push(TelegramOutboxPartInput {
            method_kind: method_kind.to_owned(),
            payload_version: 1,
            payload: serde_json::from_slice(&payload)?,
            blob: None,
            available_at: now,
            expires_at: None,
        });
    }
    let batch = dialog_intermediate_outbox_batch(
        outbox.bot_id,
        dialog_job_id,
        causation_update_id,
        params,
        seq,
        parts,
    );
    outbox.store.enqueue_batch(&batch).await?;
    Ok(())
}

fn dialog_intermediate_outbox_batch(
    bot_id: i64,
    dialog_job_id: i64,
    causation_update_id: Option<i64>,
    params: &DialogJobParams,
    seq: u32,
    parts: Vec<TelegramOutboxPartInput>,
) -> TelegramOutboxBatchInput {
    TelegramOutboxBatchInput {
        batch_id: format!("dialog-intermediate:v1:{bot_id}:{dialog_job_id}:{seq}"),
        bot_id,
        chat_id: Some(params.chat_id),
        thread_id: params.thread_id,
        ordering_key: format!(
            "dialog:{bot_id}:{}:{}",
            params.chat_id,
            params.thread_id.unwrap_or_default()
        ),
        causation_update_id,
        dialog_job_id: Some(dialog_job_id),
        trigger_message_id: Some(i64::from(params.message_id)),
        delivery_policy: TelegramDeliveryPolicy::Create,
        protected: true,
        priority: 0,
        parts,
    }
}

fn dialog_answer_batch_id(bot_id: i64, dialog_job_id: i64) -> String {
    format!("dialog-answer:v1:{bot_id}:{dialog_job_id}")
}

#[allow(clippy::too_many_arguments)]
async fn enqueue_durable_dialog_answer(
    outbox: &DurableDialogOutbox,
    dialog_job_id: i64,
    causation_update_id: Option<i64>,
    params: &DialogJobParams,
    answer: &str,
    chat: ChatRef,
    reply_to: &ReplyMessageRef,
) -> Result<QueuedBatchReceipt, DialogDispatchEffectError> {
    let methods = if dialog_response_requires_rich(answer) {
        let request = RichMessageRequest {
            chat: Some(chat),
            message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            html: answer.to_owned(),
            reply_markup: None,
        };
        vec![TelegramOutboundMethod::from(build_rich_message_method(
            &request,
            chat,
            Some(reply_to),
        )?)]
    } else {
        let request = TextMessageRequest {
            chat: Some(chat),
            message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            text: answer.to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };
        build_text_message_methods(&request, Some(reply_to))?
            .into_iter()
            .map(TelegramOutboundMethod::from)
            .collect()
    };
    let now = OffsetDateTime::now_utc();
    let mut parts = Vec::with_capacity(methods.len());
    for method in &methods {
        let method_kind = method.method_name();
        let Some((_kind, payload)) = snapshot_outbound_method(method) else {
            return Err(DialogDispatchEffectError::UnsupportedMethod(method_kind));
        };
        parts.push(TelegramOutboxPartInput {
            method_kind: method_kind.to_owned(),
            payload_version: 1,
            payload: serde_json::from_slice(&payload)?,
            blob: None,
            available_at: now,
            expires_at: None,
        });
    }
    let batch_id = dialog_answer_batch_id(outbox.bot_id, dialog_job_id);
    let batch = TelegramOutboxBatchInput {
        batch_id: batch_id.clone(),
        bot_id: outbox.bot_id,
        chat_id: Some(params.chat_id),
        thread_id: params.thread_id,
        ordering_key: format!(
            "dialog:{}:{}:{}",
            outbox.bot_id,
            params.chat_id,
            params.thread_id.unwrap_or_default()
        ),
        causation_update_id,
        dialog_job_id: Some(dialog_job_id),
        trigger_message_id: Some(i64::from(params.message_id)),
        delivery_policy: TelegramDeliveryPolicy::Create,
        protected: true,
        priority: DEFAULT_PRIORITY,
        parts,
    };
    let queued = outbox.store.enqueue_batch(&batch).await?;
    let delivery_complete = queued.parts.iter().all(|part| part.state == "delivered");
    Ok(QueuedBatchReceipt::durable_outbox(
        queued.batch_id,
        queued
            .parts
            .into_iter()
            .map(|part| part.operation_id)
            .collect(),
        delivery_complete,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intermediate_outbox_carries_dialog_causation_and_ordering() {
        let params = DialogJobParams {
            chat_id: -100,
            thread_id: Some(7),
            message_id: 11,
            user_id: 42,
            user_full_name: "Ada".to_owned(),
            message_text: "hello".to_owned(),
            original_text: String::new(),
            meta: serde_json::Value::Null,
            max_output_tokens: 0,
        };
        let batch = dialog_intermediate_outbox_batch(42, 123, Some(456), &params, 2, Vec::new());

        assert_eq!(batch.batch_id, "dialog-intermediate:v1:42:123:2");
        assert_eq!(batch.ordering_key, "dialog:42:-100:7");
        assert_eq!(batch.dialog_job_id, Some(123));
        assert_eq!(batch.causation_update_id, Some(456));
        assert_eq!(batch.trigger_message_id, Some(11));
    }
}
