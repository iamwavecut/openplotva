//! Outbound side effects of a dialog turn: the effects trait and the
//! dispatcher-backed answer sender.

use std::{fmt, sync::Arc};

use openplotva_taskman::DialogJobParams;
use openplotva_telegram::{
    ChatRef, DispatcherQueue, ReplyMessageRef, RichMessageRequest, TELEGRAM_PARSE_MODE_HTML,
    TextMessageRequest,
};
use thiserror::Error;

use crate::virtual_messages::{
    QueueRichRequest, QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory,
    queue_rich_message, queue_text_message_parts,
};

use super::{DialogJobEffectFuture, dialog_response_requires_rich};

/// Side effects performed after a provider returns a dialog answer.
pub trait DialogJobEffects {
    /// Error returned by concrete side effects.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_dialog_answer<'a>(
        &'a self,
        params: &'a DialogJobParams,
        answer: &'a str,
    ) -> DialogJobEffectFuture<'a, Self::Error>;

    /// Queue one intermediate session message (`send_message` tool or text
    /// alongside tool calls). `seq` is the 1-based per-session send counter;
    /// `reply_to_trigger` is true only for the session's first outgoing
    /// message so groups do not get a quote header on every part.
    fn send_dialog_intermediate<'a>(
        &'a self,
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
}

impl DialogDispatcherEffects {
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>) -> Self {
        Self {
            queue,
            next_virtual_id: monotonic_virtual_id_factory("dialog-vmsg"),
        }
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
}

impl DialogJobEffects for DialogDispatcherEffects {
    type Error = DialogDispatchEffectError;

    fn send_dialog_answer<'a>(
        &'a self,
        params: &'a DialogJobParams,
        answer: &'a str,
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
            // Dialog answers must survive queue trims, and the reply-scoped
            // debounce key stops identical answers to different trigger
            // messages from deduping each other (a duplicate reply to the
            // same message is still suppressed).
            let debounce_key = format!("r{}", params.message_id);
            if dialog_response_requires_rich(answer) {
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
                .await?;
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
                .await?;
            }
            Ok(())
        })
    }

    fn send_dialog_intermediate<'a>(
        &'a self,
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
