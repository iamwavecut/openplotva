//! Composition-root virtual-message send/edit/delete behavior.

use std::{fmt, future::Future, pin::Pin};

use openplotva_core::{MessageIdMapping, ReadyPendingOp};
use openplotva_telegram::{
    AudioMessageRequest, DispatcherMessage, DispatcherQueue, DispatcherSendStatus,
    DispatcherWorkItem, EditMediaMessageRequest, EditTextMessageRequest, EnqueueOutcome,
    MediaGroupMessageRequest, MediaGroupPhotoItem, MessageFingerprint, OutboundBuildError,
    PENDING_OP_DELETE, PENDING_OP_EDIT, PendingOpBuildError, PhotoMessageRequest, ReplyMessageRef,
    StickerMessageRequest, TELEGRAM_TEXT_MAX_BYTES, TelegramOutboundMethod,
    TelegramOutboundResponse, TextMessageRequest, build_audio_message_method,
    build_audio_message_plan, build_edit_media_message_method, build_edit_media_message_plan,
    build_edit_text_message_method, build_media_group_message_method,
    build_media_group_message_plan, build_pending_op_method, build_photo_message_method,
    build_photo_message_plan, build_sticker_message_method, build_sticker_message_plan,
    build_text_message_method, fingerprint_audio_message_plan, fingerprint_photo_message_plan,
    fingerprint_sticker_message_plan, fingerprint_text_message_part, forum_thread_id, hash_content,
    message_target_chat, split_telegram_text, validate_text_message_text,
};
use serde_json::json;
use thiserror::Error;

use crate::pending_ops::PendingOpHistory;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Storage operations used by Go's virtual send/edit/delete paths.
pub trait VirtualMessageStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load a virtual-message mapping by virtual ID.
    fn get_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>>;

    /// Insert an unresolved virtual-message mapping before queueing a send.
    fn insert_virtual_message<'a>(
        &'a self,
        vmsg_id: String,
        chat_id: i64,
        thread_id: Option<i32>,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// Resolve a virtual message to the real Telegram message ID after send success.
    fn resolve_virtual_message<'a>(
        &'a self,
        vmsg_id: String,
        real_message_id: i32,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    /// Enqueue a pending virtual-message operation.
    fn enqueue_message_op<'a>(
        &'a self,
        vmsg_id: String,
        chat_id: i64,
        op: &'static str,
        payload_json: Option<String>,
    ) -> BoxFuture<'a, Result<i64, Self::Error>>;

    /// Delete a resolved virtual-message mapping.
    fn delete_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

impl VirtualMessageStore for openplotva_storage::PostgresVirtualMessageStore {
    type Error = openplotva_storage::StorageError;

    fn get_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>> {
        Box::pin(async move { self.get_mapping_by_virtual(&vmsg_id).await })
    }

    fn insert_virtual_message<'a>(
        &'a self,
        vmsg_id: String,
        chat_id: i64,
        thread_id: Option<i32>,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            self.insert_virtual_message(&vmsg_id, chat_id, thread_id)
                .await
        })
    }

    fn resolve_virtual_message<'a>(
        &'a self,
        vmsg_id: String,
        real_message_id: i32,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            self.resolve_virtual_message(&vmsg_id, real_message_id, None)
                .await
        })
    }

    fn enqueue_message_op<'a>(
        &'a self,
        vmsg_id: String,
        chat_id: i64,
        op: &'static str,
        payload_json: Option<String>,
    ) -> BoxFuture<'a, Result<i64, Self::Error>> {
        Box::pin(async move {
            self.enqueue_message_op(&vmsg_id, chat_id, op, payload_json.as_deref())
                .await
        })
    }

    fn delete_mapping_by_virtual<'a>(
        &'a self,
        vmsg_id: String,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.delete_mapping_by_virtual(&vmsg_id).await })
    }
}

/// Observable result of a virtual-message edit/delete request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VirtualMessageAction {
    /// The mapping was resolved and a Telegram method was sent immediately.
    SentNow,
    /// The mapping was missing or unresolved, so a pending operation was queued.
    Queued,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualMessageReport {
    /// Whether the operation was sent immediately or queued for later.
    pub action: VirtualMessageAction,
    /// Real Telegram message ID used by immediate sends.
    pub real_message_id: Option<i32>,
    /// Pending operation row ID, when enqueue succeeded.
    pub enqueued_op_id: Option<i64>,
    /// Mapping lookup error ignored by Go before queueing a pending operation.
    pub lookup_error: Option<String>,
    /// Enqueue error ignored by Go after deciding to queue.
    pub enqueue_error: Option<String>,
    /// Number of queued dispatcher items removed by virtual ID.
    pub canceled: usize,
    /// Whether a successful edit was reflected into history.
    pub history_updated: bool,
    /// Whether a successful delete was reflected into history.
    pub history_deleted: bool,
    /// Whether a successful delete removed its virtual-message mapping.
    pub mapping_deleted: bool,
    /// Mapping-delete error ignored by Go after a successful Telegram delete.
    pub delete_mapping_error: Option<String>,
}

impl VirtualMessageReport {
    fn sent_now(real_message_id: i32) -> Self {
        Self {
            action: VirtualMessageAction::SentNow,
            real_message_id: Some(real_message_id),
            enqueued_op_id: None,
            lookup_error: None,
            enqueue_error: None,
            canceled: 0,
            history_updated: false,
            history_deleted: false,
            mapping_deleted: false,
            delete_mapping_error: None,
        }
    }

    fn queued(
        enqueued_op_id: Option<i64>,
        enqueue_error: Option<String>,
        lookup_error: Option<String>,
        canceled: usize,
    ) -> Self {
        Self {
            action: VirtualMessageAction::Queued,
            real_message_id: None,
            enqueued_op_id,
            lookup_error,
            enqueue_error,
            canceled,
            history_updated: false,
            history_deleted: false,
            mapping_deleted: false,
            delete_mapping_error: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualEditRequest<'a> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Virtual message ID.
    pub vmsg_id: &'a str,
    /// New message text.
    pub text: &'a str,
    /// Go parse mode string, such as `HTML`.
    pub parse_mode: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualDeleteRequest<'a> {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Virtual message ID.
    pub vmsg_id: &'a str,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueTextRequest<'a> {
    /// Telegram text request fields.
    pub message: &'a TextMessageRequest,
    /// Optional replied-to message fields.
    pub reply_to: Option<&'a ReplyMessageRef>,
    /// Whether Go would enqueue the first split part in the immediate queue.
    pub immediate_first: bool,
    /// Whether Go `TextMessage.BypassChatRestrictions` was set at enqueue time.
    pub bypass_chat_restrictions: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueStickerRequest<'a> {
    /// Telegram sticker request fields.
    pub message: &'a StickerMessageRequest,
    /// Optional replied-to message fields.
    pub reply_to: Option<&'a ReplyMessageRef>,
    /// Whether Go would enqueue the sticker in the immediate queue.
    pub immediate: bool,
    /// Whether Go `StickerMessage.BypassChatRestrictions` was set at enqueue time.
    pub bypass_chat_restrictions: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueEditTextRequest<'a> {
    /// Telegram edit-text request fields.
    pub message: &'a EditTextMessageRequest,
    /// Whether the edit should enter the immediate queue.
    pub immediate: bool,
    /// Whether Go `TextMessage.BypassChatRestrictions` was set at enqueue time.
    pub bypass_chat_restrictions: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueuePhotoRequest<'a> {
    /// Telegram photo request fields.
    pub message: &'a PhotoMessageRequest,
    /// Whether the photo should enter the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueAudioRequest<'a> {
    /// Telegram audio request fields.
    pub message: &'a AudioMessageRequest,
    /// Whether the audio should enter the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueMediaGroupRequest<'a> {
    /// Telegram media-group request fields.
    pub message: &'a MediaGroupMessageRequest,
    /// Whether the media group should enter the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueEditMediaRequest<'a> {
    /// Telegram edit-media request fields.
    pub message: &'a EditMediaMessageRequest,
    /// Whether the edit should enter the immediate queue.
    pub immediate: bool,
}

/// One queued text part and its virtual-message bookkeeping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedTextPartReport {
    /// Zero-based split part index.
    pub index: usize,
    /// Virtual message ID generated before queueing.
    pub virtual_id: String,
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this part went to the immediate queue.
    pub immediate: bool,
    /// Storage error ignored by Go when inserting the virtual ID row.
    pub insert_error: Option<String>,
}

/// Summary of queueing one Go `SendText` request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueTextReport {
    /// Split text parts that were queued or deduped.
    pub parts: Vec<QueuedTextPartReport>,
}

/// Summary of queueing one Go `SendSticker` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueStickerReport {
    /// Virtual message ID generated before queueing.
    pub virtual_id: String,
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this sticker went to the immediate queue.
    pub immediate: bool,
    /// Storage error ignored by Go when inserting the virtual ID row.
    pub insert_error: Option<String>,
}

/// Summary of queueing one Go direct `sendPhoto` chattable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuePhotoReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this photo went to the immediate queue.
    pub immediate: bool,
}

/// Summary of queueing one Go direct `sendAudio` chattable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueAudioReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this audio went to the immediate queue.
    pub immediate: bool,
}

/// Summary of queueing one Go direct `EditText` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueEditTextReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this edit went to the immediate queue.
    pub immediate: bool,
}

/// Summary of queueing one Go direct `sendMediaGroup` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueMediaGroupReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this media group went to the immediate queue.
    pub immediate: bool,
}

/// Summary of queueing one Go direct `editMessageMedia` call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueEditMediaReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this edit went to the immediate queue.
    pub immediate: bool,
}

impl QueueTextReport {
    /// Number of split parts accepted into a dispatcher queue.
    pub fn enqueued_count(&self) -> usize {
        self.parts
            .iter()
            .filter(|part| part.enqueue_outcome == EnqueueOutcome::Enqueued)
            .count()
    }

    /// Number of split parts suppressed by dispatcher deduplication.
    pub fn deduped_count(&self) -> usize {
        self.parts
            .iter()
            .filter(|part| part.enqueue_outcome == EnqueueOutcome::Deduped)
            .count()
    }
}

/// Report from sending one dispatcher work item and applying Go post-send mapping resolution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchResolveReport {
    pub status: DispatcherSendStatus,
    /// Virtual message ID carried by the dispatcher item.
    pub virtual_id: String,
    /// Real Telegram message ID extracted from a successful send response.
    pub resolved_message_id: Option<i32>,
    /// Whether the dispatcher item had no Telegram method payload to send.
    pub missing_method: bool,
    /// Telegram send error returned by the transport callback.
    pub send_error: Option<String>,
    /// Mapping-resolution error ignored by Go after send success.
    pub resolve_error: Option<String>,
}

/// Recoverable errors from immediate virtual-message handling.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum VirtualMessageError {
    /// Go returns this before trying storage for virtual edits.
    #[error("text is empty")]
    EmptyText,
    /// The resolved operation could not be converted into a Telegram method.
    #[error("failed to build Telegram method: {0}")]
    Build(String),
    /// Telegram rejected the immediate operation.
    #[error("Telegram send failed: {0}")]
    Send(String),
}

/// Queue text parts like Go `SendText`, creating virtual-message rows before dispatcher enqueue.
pub async fn queue_text_message_parts<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    req: QueueTextRequest<'_>,
    mut next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    validate_text_message_text(&req.message.text, &req.message.render_as)?;
    let chat = message_target_chat(req.message.chat.as_ref(), req.reply_to)?;
    let parts = split_telegram_text(
        &req.message.text,
        &req.message.render_as,
        TELEGRAM_TEXT_MAX_BYTES,
    );
    if parts.is_empty() {
        return Err(OutboundBuildError::NoParts);
    }

    let mut report = QueueTextReport::default();
    let total_parts = parts.len();
    for (index, part) in parts.into_iter().enumerate() {
        let virtual_id = next_virtual_id();
        let thread_id = forum_thread_id(chat, req.message.message_thread_id).map(|id| id as i32);
        let insert_error = store
            .insert_virtual_message(virtual_id.clone(), chat.id, thread_id)
            .await
            .err()
            .map(|error| error.to_string());
        let method = build_text_message_method(
            req.message,
            chat,
            req.reply_to,
            part.clone(),
            index + 1 == total_parts,
        )?;
        let dispatcher_message =
            DispatcherMessage::new(fingerprint_text_message_part(chat.id, &part), &virtual_id)
                .with_method(TelegramOutboundMethod::from(method))
                .with_bypass_chat_restrictions(req.bypass_chat_restrictions);
        let immediate = req.immediate_first && index == 0;
        let enqueue_outcome = queue.enqueue(dispatcher_message, immediate);
        report.parts.push(QueuedTextPartReport {
            index,
            virtual_id,
            enqueue_outcome,
            immediate,
            insert_error,
        });
    }

    Ok(report)
}

/// Queue one sticker like Go `SendSticker`, creating a virtual-message row before dispatcher enqueue.
pub async fn queue_sticker_message<S, NextId>(
    store: &S,
    queue: &DispatcherQueue,
    req: QueueStickerRequest<'_>,
    mut next_virtual_id: NextId,
) -> Result<QueueStickerReport, OutboundBuildError>
where
    S: VirtualMessageStore + Sync,
    NextId: FnMut() -> String,
{
    let chat = message_target_chat(req.message.chat.as_ref(), req.reply_to)?;
    let virtual_id = next_virtual_id();
    let thread_id =
        forum_thread_id(chat, req.message.message_thread_id).map(|thread_id| thread_id as i32);
    let insert_error = store
        .insert_virtual_message(virtual_id.clone(), chat.id, thread_id)
        .await
        .err()
        .map(|error| error.to_string());
    let plan = build_sticker_message_plan(req.message, req.reply_to)?;
    let method = build_sticker_message_method(req.message, req.reply_to)?;
    let persistence_payload = plan
        .to_persistence_payload()
        .map_err(|error| OutboundBuildError::PersistencePayload(error.to_string()))?;
    let dispatcher_message =
        DispatcherMessage::new(fingerprint_sticker_message_plan(&plan), &virtual_id)
            .with_method(TelegramOutboundMethod::from(method))
            .with_persistence_payload(persistence_payload)
            .with_bypass_chat_restrictions(req.bypass_chat_restrictions);
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueueStickerReport {
        virtual_id,
        enqueue_outcome,
        immediate: req.immediate,
        insert_error,
    })
}

/// Queue one photo like a Go direct `SendChattable(api.PhotoConfig)` path.
pub fn queue_photo_message(
    queue: &DispatcherQueue,
    req: QueuePhotoRequest<'_>,
) -> Result<QueuePhotoReport, OutboundBuildError> {
    let plan = build_photo_message_plan(req.message)?;
    let method = build_photo_message_method(req.message)?;
    let persistence_payload = plan
        .to_persistence_payload()
        .map_err(|error| OutboundBuildError::PersistencePayload(error.to_string()))?;
    let dispatcher_message = DispatcherMessage::new(fingerprint_photo_message_plan(&plan), "")
        .with_method(TelegramOutboundMethod::from(method))
        .with_persistence_payload(persistence_payload);
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueuePhotoReport {
        enqueue_outcome,
        immediate: req.immediate,
    })
}

/// Queue one audio like a Go direct `SendChattable(api.AudioConfig)` path.
pub fn queue_audio_message(
    queue: &DispatcherQueue,
    req: QueueAudioRequest<'_>,
) -> Result<QueueAudioReport, OutboundBuildError> {
    let plan = build_audio_message_plan(req.message);
    let method = build_audio_message_method(req.message)?;
    let persistence_payload = plan
        .to_persistence_payload()
        .map_err(|error| OutboundBuildError::PersistencePayload(error.to_string()))?;
    let dispatcher_message = DispatcherMessage::new(fingerprint_audio_message_plan(&plan), "")
        .with_method(TelegramOutboundMethod::from(method))
        .with_persistence_payload(persistence_payload);
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueueAudioReport {
        enqueue_outcome,
        immediate: req.immediate,
    })
}

/// Queue one edit-text call like Go `EditText` without virtual-message mapping.
pub fn queue_edit_text_message(
    queue: &DispatcherQueue,
    req: QueueEditTextRequest<'_>,
) -> Result<QueueEditTextReport, OutboundBuildError> {
    let method = build_edit_text_message_method(req.message)?;
    let dispatcher_message =
        DispatcherMessage::new(edit_text_identity_fingerprint(req.message), "")
            .with_method(TelegramOutboundMethod::from(method))
            .with_bypass_chat_restrictions(req.bypass_chat_restrictions);
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueueEditTextReport {
        enqueue_outcome,
        immediate: req.immediate,
    })
}

/// Queue one media group like a Go direct `SendMediaGroup(api.MediaGroupConfig)` path.
pub fn queue_media_group_message(
    queue: &DispatcherQueue,
    req: QueueMediaGroupRequest<'_>,
) -> Result<QueueMediaGroupReport, OutboundBuildError> {
    let plan = build_media_group_message_plan(req.message);
    let method = build_media_group_message_method(req.message)?;
    let persistence_payload = plan
        .to_persistence_payload()
        .map_err(|error| OutboundBuildError::PersistencePayload(error.to_string()))?;
    let dispatcher_message =
        DispatcherMessage::new(media_group_identity_fingerprint(req.message), "")
            .with_method(TelegramOutboundMethod::from(method))
            .with_persistence_payload(persistence_payload);
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueueMediaGroupReport {
        enqueue_outcome,
        immediate: req.immediate,
    })
}

/// Queue one edit-media call like a Go direct `EditMessageMediaWithContext` path.
pub fn queue_edit_media_message(
    queue: &DispatcherQueue,
    req: QueueEditMediaRequest<'_>,
) -> Result<QueueEditMediaReport, OutboundBuildError> {
    let plan = build_edit_media_message_plan(req.message);
    let method = build_edit_media_message_method(req.message)?;
    let persistence_payload = plan
        .to_persistence_payload()
        .map_err(|error| OutboundBuildError::PersistencePayload(error.to_string()))?;
    let dispatcher_message =
        DispatcherMessage::new(edit_media_identity_fingerprint(req.message), "")
            .with_method(TelegramOutboundMethod::from(method))
            .with_persistence_payload(persistence_payload);
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueueEditMediaReport {
        enqueue_outcome,
        immediate: req.immediate,
    })
}

fn media_group_identity_fingerprint(req: &MediaGroupMessageRequest) -> MessageFingerprint {
    let mut content = format!(
        "chat={};thread={};notify={};reply={:?};items=",
        req.chat.id, req.message_thread_id, req.disable_notification, req.reply_parameters
    );
    for item in &req.items {
        content.push_str(&format!("{:p};", item as *const MediaGroupPhotoItem));
    }
    MessageFingerprint {
        chat_id: req.chat.id,
        message_type: "media_group".to_owned(),
        content_hash: hash_content(&content),
        debounce_key: None,
    }
}

fn edit_media_identity_fingerprint(req: &EditMediaMessageRequest) -> MessageFingerprint {
    let content = format!(
        "{}:{}:{:p}",
        std::any::type_name::<EditMediaMessageRequest>(),
        req.message_id,
        &req.media as *const MediaGroupPhotoItem
    );
    MessageFingerprint {
        chat_id: req.chat.id,
        message_type: "unknown".to_owned(),
        content_hash: hash_content(&content),
        debounce_key: None,
    }
}

fn edit_text_identity_fingerprint(req: &EditTextMessageRequest) -> MessageFingerprint {
    let content = format!(
        "{}:{}:{}:{}:{:?}",
        std::any::type_name::<EditTextMessageRequest>(),
        req.message_id,
        req.text,
        req.render_as,
        req.reply_markup
    );
    MessageFingerprint {
        chat_id: req.chat.id,
        message_type: "unknown".to_owned(),
        content_hash: hash_content(&content),
        debounce_key: None,
    }
}

/// Send one dispatcher item and resolve its virtual-message mapping from the Telegram response.
pub async fn send_work_item_and_resolve<S, Send, SendFuture, SendError>(
    store: &S,
    item: DispatcherWorkItem,
    send: Send,
) -> DispatchResolveReport
where
    S: VirtualMessageStore + Sync,
    Send: FnOnce(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<TelegramOutboundResponse, SendError>>,
    SendError: fmt::Display,
{
    let (metadata, method) = item.into_parts();
    let Some(method) = method else {
        return DispatchResolveReport {
            status: DispatcherSendStatus::Failed,
            virtual_id: metadata.virtual_id,
            resolved_message_id: None,
            missing_method: true,
            send_error: None,
            resolve_error: None,
        };
    };

    let response = match send(method).await {
        Ok(response) => response,
        Err(error) => {
            return DispatchResolveReport {
                status: DispatcherSendStatus::Failed,
                virtual_id: metadata.virtual_id,
                resolved_message_id: None,
                missing_method: false,
                send_error: Some(error.to_string()),
                resolve_error: None,
            };
        }
    };

    let mut report = DispatchResolveReport {
        status: DispatcherSendStatus::Sent,
        virtual_id: metadata.virtual_id,
        resolved_message_id: response_message_id(&response),
        missing_method: false,
        send_error: None,
        resolve_error: None,
    };

    if !report.virtual_id.is_empty()
        && let Some(message_id) = report.resolved_message_id
    {
        report.resolve_error = store
            .resolve_virtual_message(report.virtual_id.clone(), message_id)
            .await
            .err()
            .map(|error| error.to_string());
    }

    report
}

pub async fn edit_text_virtual<S, H, Send, SendFuture, SendError, Cancel>(
    store: &S,
    history: &H,
    req: VirtualEditRequest<'_>,
    send: Send,
    cancel: Cancel,
) -> Result<VirtualMessageReport, VirtualMessageError>
where
    S: VirtualMessageStore + Sync,
    H: PendingOpHistory,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Cancel: FnMut(&str) -> usize,
{
    if req.text.is_empty() {
        return Err(VirtualMessageError::EmptyText);
    }

    let payload_json = pending_edit_payload_json(req.text, req.parse_mode);
    let mapping = load_mapping(store, req.vmsg_id).await;
    let Some(real_message_id) = resolved_message_id(&mapping) else {
        return Ok(queue_virtual_message_op(
            store,
            req.vmsg_id,
            req.chat_id,
            PENDING_OP_EDIT,
            Some(payload_json),
            mapping.err().map(|error| error.to_string()),
            cancel,
        )
        .await);
    };

    let op = ready_virtual_op(
        req.vmsg_id,
        req.chat_id,
        PENDING_OP_EDIT,
        payload_json.into_bytes(),
        real_message_id,
    );
    send_ready_virtual_op(&op, send).await?;
    history
        .update_text(req.chat_id, real_message_id, req.text, req.parse_mode)
        .await;

    let mut report = VirtualMessageReport::sent_now(real_message_id);
    report.history_updated = true;
    Ok(report)
}

pub async fn delete_message_virtual<S, H, Send, SendFuture, SendError, Cancel>(
    store: &S,
    history: &H,
    req: VirtualDeleteRequest<'_>,
    send: Send,
    cancel: Cancel,
) -> Result<VirtualMessageReport, VirtualMessageError>
where
    S: VirtualMessageStore + Sync,
    H: PendingOpHistory,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Cancel: FnMut(&str) -> usize,
{
    let mapping = load_mapping(store, req.vmsg_id).await;
    let Some(real_message_id) = resolved_message_id(&mapping) else {
        return Ok(queue_virtual_message_op(
            store,
            req.vmsg_id,
            req.chat_id,
            PENDING_OP_DELETE,
            None,
            mapping.err().map(|error| error.to_string()),
            cancel,
        )
        .await);
    };

    let op = ready_virtual_op(
        req.vmsg_id,
        req.chat_id,
        PENDING_OP_DELETE,
        Vec::new(),
        real_message_id,
    );
    send_ready_virtual_op(&op, send).await?;
    history.delete_message(req.chat_id, real_message_id).await;

    let delete_mapping_result = store
        .delete_mapping_by_virtual(req.vmsg_id.to_owned())
        .await;
    let mut report = VirtualMessageReport::sent_now(real_message_id);
    report.history_deleted = true;
    match delete_mapping_result {
        Ok(()) => report.mapping_deleted = true,
        Err(error) => report.delete_mapping_error = Some(error.to_string()),
    }
    Ok(report)
}

async fn load_mapping<S>(store: &S, vmsg_id: &str) -> Result<Option<MessageIdMapping>, S::Error>
where
    S: VirtualMessageStore + Sync,
{
    store.get_mapping_by_virtual(vmsg_id.to_owned()).await
}

fn resolved_message_id<E>(mapping: &Result<Option<MessageIdMapping>, E>) -> Option<i32> {
    mapping
        .as_ref()
        .ok()
        .and_then(|mapping| mapping.as_ref())
        .and_then(|mapping| mapping.real_message_id)
}

fn response_message_id(response: &TelegramOutboundResponse) -> Option<i32> {
    let raw = match response {
        TelegramOutboundResponse::Message(message) => Some(message.id),
        TelegramOutboundResponse::Messages(messages) => messages.first().map(|message| message.id),
        TelegramOutboundResponse::EditMessage(_) | TelegramOutboundResponse::Boolean(_) => None,
    }?;
    i32::try_from(raw).ok()
}

async fn queue_virtual_message_op<S, Cancel>(
    store: &S,
    vmsg_id: &str,
    chat_id: i64,
    op: &'static str,
    payload_json: Option<String>,
    lookup_error: Option<String>,
    mut cancel: Cancel,
) -> VirtualMessageReport
where
    S: VirtualMessageStore + Sync,
    Cancel: FnMut(&str) -> usize,
{
    let enqueue_result = store
        .enqueue_message_op(vmsg_id.to_owned(), chat_id, op, payload_json)
        .await;
    let (enqueued_op_id, enqueue_error) = match enqueue_result {
        Ok(id) => (Some(id), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let canceled = cancel(vmsg_id);

    VirtualMessageReport::queued(enqueued_op_id, enqueue_error, lookup_error, canceled)
}

async fn send_ready_virtual_op<Send, SendFuture, SendError>(
    op: &ReadyPendingOp,
    mut send: Send,
) -> Result<(), VirtualMessageError>
where
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
{
    let method = build_pending_op_method(op)
        .map_err(|error| VirtualMessageError::Build(pending_build_error_message(error)))?;
    send(method)
        .await
        .map_err(|error| VirtualMessageError::Send(error.to_string()))
}

fn ready_virtual_op(
    vmsg_id: &str,
    chat_id: i64,
    op: &str,
    payload: Vec<u8>,
    real_message_id: i32,
) -> ReadyPendingOp {
    ReadyPendingOp {
        id: 0,
        vmsg_id: vmsg_id.to_owned(),
        chat_id,
        op: op.to_owned(),
        payload,
        real_message_id,
    }
}

fn pending_edit_payload_json(text: &str, parse_mode: &str) -> String {
    json!({
        "parse_mode": parse_mode,
        "text": text,
    })
    .to_string()
}

fn pending_build_error_message(error: PendingOpBuildError) -> String {
    match error {
        PendingOpBuildError::UnknownOp(_) => "unknown op".to_owned(),
        error => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        sync::{Arc, Mutex},
    };

    use openplotva_core::MessageIdMapping;
    use openplotva_telegram::{
        AudioMessageRequest, AudioSource, ChatRef, DispatcherConfig, DispatcherMessage,
        DispatcherQueue, DispatcherSendStatus, EditMediaMessageRequest, EditTextMessageRequest,
        EnqueueOutcome, MediaGroupMessageRequest, MediaGroupPhotoItem, PhotoMessageRequest,
        PhotoSource, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML, TelegramMessage,
        TelegramOutboundMethod, TelegramOutboundMethodKind, TelegramOutboundResponse,
        TextMessageRequest, build_text_message_method, fingerprint_text_message_part,
        persistent_queue_from_drain,
    };
    use serde_json::json;

    use crate::pending_ops::PendingOpHistory;

    use super::{
        PENDING_OP_DELETE, PENDING_OP_EDIT, QueueAudioRequest, QueueEditMediaRequest,
        QueueEditTextRequest, QueueMediaGroupRequest, QueuePhotoRequest, QueueStickerRequest,
        QueueTextRequest, VirtualDeleteRequest, VirtualEditRequest, VirtualMessageAction,
        VirtualMessageError, VirtualMessageReport, VirtualMessageStore, delete_message_virtual,
        edit_text_virtual, queue_audio_message, queue_edit_media_message, queue_edit_text_message,
        queue_media_group_message, queue_photo_message, queue_sticker_message,
        queue_text_message_parts, send_work_item_and_resolve,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StubError(&'static str);

    impl fmt::Display for StubError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for StubError {}

    #[derive(Default)]
    struct StoreState {
        mapping: Option<MessageIdMapping>,
        lookup_error: Option<StubError>,
        insert_error: Option<StubError>,
        enqueue_error: Option<StubError>,
        resolve_error: Option<StubError>,
        delete_mapping_error: Option<StubError>,
        lookup_calls: Vec<String>,
        inserted: Vec<(String, i64, Option<i32>)>,
        resolved: Vec<(String, i32)>,
        enqueued: Vec<(String, i64, &'static str, Option<String>)>,
        deleted_mappings: Vec<String>,
        events: Vec<String>,
    }

    #[derive(Clone, Default)]
    struct StoreStub {
        state: Arc<Mutex<StoreState>>,
    }

    impl StoreStub {
        fn with_state(state: StoreState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn snapshot<T>(&self, inspect: impl FnOnce(&StoreState) -> T) -> T {
            let state = self.state.lock().expect("store state");
            inspect(&state)
        }

        fn history(&self) -> HistoryStub {
            HistoryStub {
                state: Arc::clone(&self.state),
            }
        }
    }

    #[derive(Clone)]
    struct HistoryStub {
        state: Arc<Mutex<StoreState>>,
    }

    impl PendingOpHistory for HistoryStub {
        fn update_text<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            text: &'a str,
            parse_mode: &'a str,
        ) -> super::BoxFuture<'a, ()> {
            self.state.lock().expect("store state").events.push(format!(
                "history:update:{chat_id}:{message_id}:{text}:{parse_mode}"
            ));
            Box::pin(async {})
        }

        fn delete_message<'a>(&'a self, chat_id: i64, message_id: i32) -> super::BoxFuture<'a, ()> {
            self.state
                .lock()
                .expect("store state")
                .events
                .push(format!("history:delete:{chat_id}:{message_id}"));
            Box::pin(async {})
        }
    }

    impl VirtualMessageStore for StoreStub {
        type Error = StubError;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            vmsg_id: String,
        ) -> super::BoxFuture<'a, Result<Option<MessageIdMapping>, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.lookup_calls.push(vmsg_id);
                if let Some(error) = &state.lookup_error {
                    Err(error.clone())
                } else {
                    Ok(state.mapping.clone())
                }
            };
            Box::pin(async move { result })
        }

        fn insert_virtual_message<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            thread_id: Option<i32>,
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.inserted.push((vmsg_id, chat_id, thread_id));
                if let Some(error) = &state.insert_error {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            };
            Box::pin(async move { result })
        }

        fn resolve_virtual_message<'a>(
            &'a self,
            vmsg_id: String,
            real_message_id: i32,
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.resolved.push((vmsg_id, real_message_id));
                if let Some(error) = &state.resolve_error {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            };
            Box::pin(async move { result })
        }

        fn enqueue_message_op<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            op: &'static str,
            payload_json: Option<String>,
        ) -> super::BoxFuture<'a, Result<i64, Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.enqueued.push((vmsg_id, chat_id, op, payload_json));
                if let Some(error) = &state.enqueue_error {
                    Err(error.clone())
                } else {
                    Ok(i64::try_from(state.enqueued.len()).expect("enqueued len fits i64"))
                }
            };
            Box::pin(async move { result })
        }

        fn delete_mapping_by_virtual<'a>(
            &'a self,
            vmsg_id: String,
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.deleted_mappings.push(vmsg_id);
                if let Some(error) = &state.delete_mapping_error {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            };
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn queue_text_message_parts_inserts_virtual_ids_before_dispatch_enqueue()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let text = format!("{}b", "a".repeat(4096));
        let request = text_request(&text);
        let mut ids = ["v1".to_owned(), "v2".to_owned()].into_iter();

        let report = queue_text_message_parts(
            &store,
            &queue,
            QueueTextRequest {
                message: &request,
                reply_to: None,
                immediate_first: true,
                bypass_chat_restrictions: false,
            },
            || ids.next().expect("virtual id"),
        )
        .await?;

        assert_eq!(report.enqueued_count(), 2);
        assert_eq!(report.deduped_count(), 0);
        assert_eq!(report.parts.len(), 2);
        assert_eq!(report.parts[0].virtual_id, "v1");
        assert!(report.parts[0].immediate);
        assert_eq!(report.parts[1].virtual_id, "v2");
        assert!(!report.parts[1].immediate);
        store.snapshot(|state| {
            assert_eq!(
                state.inserted,
                vec![("v1".to_owned(), 42, None), ("v2".to_owned(), 42, None)]
            );
        });
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert_eq!(snapshot.immediate[0].virtual_id, "v1");
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "v2");

        Ok(())
    }

    #[tokio::test]
    async fn queue_text_message_parts_reports_insert_errors_but_still_queues()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            insert_error: Some(StubError("insert failed")),
            ..StoreState::default()
        });
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = text_request("hello");

        let report = queue_text_message_parts(
            &store,
            &queue,
            QueueTextRequest {
                message: &request,
                reply_to: None,
                immediate_first: false,
                bypass_chat_restrictions: false,
            },
            || "v1".to_owned(),
        )
        .await?;

        assert_eq!(report.enqueued_count(), 1);
        assert_eq!(
            report.parts[0].insert_error,
            Some("insert failed".to_owned())
        );
        assert_eq!(queue.snapshot().regular[0].virtual_id, "v1");
        store.snapshot(|state| {
            assert_eq!(state.inserted, vec![("v1".to_owned(), 42, None)]);
        });

        Ok(())
    }

    #[tokio::test]
    async fn queue_text_message_parts_marks_bypass_chat_restrictions_for_dispatcher()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = text_request("settings link");

        let report = queue_text_message_parts(
            &store,
            &queue,
            QueueTextRequest {
                message: &request,
                reply_to: None,
                immediate_first: true,
                bypass_chat_restrictions: true,
            },
            || "v1".to_owned(),
        )
        .await?;

        assert_eq!(report.enqueued_count(), 1);
        let item = queue.dequeue_immediate().expect("queued text item");
        assert!(item.bypasses_chat_restrictions());
        Ok(())
    }

    #[tokio::test]
    async fn queue_sticker_message_inserts_virtual_id_and_enqueues_immediate_like_go()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = openplotva_telegram::StickerMessageRequest {
            chat: Some(ChatRef {
                id: 42,
                is_forum: true,
            }),
            message_thread_id: 7,
            disable_notification: true,
            file_id: "sticker-file-id".to_owned(),
        };

        let report = queue_sticker_message(
            &store,
            &queue,
            QueueStickerRequest {
                message: &request,
                reply_to: None,
                immediate: true,
                bypass_chat_restrictions: false,
            },
            || "sticker-v1".to_owned(),
        )
        .await?;

        assert_eq!(report.virtual_id, "sticker-v1");
        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(report.immediate);
        assert_eq!(report.insert_error, None);
        store.snapshot(|state| {
            assert_eq!(state.inserted, vec![("sticker-v1".to_owned(), 42, Some(7))]);
        });
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert_eq!(snapshot.immediate[0].virtual_id, "sticker-v1");
        assert_eq!(snapshot.immediate[0].fingerprint_key, "42:sticker:abbb8dc3");
        assert!(snapshot.regular.is_empty());

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;
        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        let item = &persisted.items[0];
        assert_eq!(item.message_type, "*api.StickerConfig");
        assert_eq!(item.virtual_id, "sticker-v1");
        assert_eq!(item.chat_id, 42);
        let payload: serde_json::Value = serde_json::from_slice(&item.message)?;
        assert_eq!(payload["ChatID"], 42);
        assert_eq!(payload["MessageThreadID"], 7);
        assert_eq!(payload["File"], "sticker-file-id");

        Ok(())
    }

    #[tokio::test]
    async fn queue_sticker_message_reports_insert_errors_but_still_queues()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            insert_error: Some(StubError("insert failed")),
            ..StoreState::default()
        });
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let reply = ReplyMessageRef {
            chat: ChatRef {
                id: -100,
                is_forum: true,
            },
            message_id: 99,
            is_topic_message: true,
            message_thread_id: 11,
        };
        let request = openplotva_telegram::StickerMessageRequest {
            chat: None,
            message_thread_id: 0,
            disable_notification: false,
            file_id: "reply-sticker".to_owned(),
        };

        let report = queue_sticker_message(
            &store,
            &queue,
            QueueStickerRequest {
                message: &request,
                reply_to: Some(&reply),
                immediate: false,
                bypass_chat_restrictions: false,
            },
            || "sticker-v2".to_owned(),
        )
        .await?;

        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(!report.immediate);
        assert_eq!(report.insert_error, Some("insert failed".to_owned()));
        store.snapshot(|state| {
            assert_eq!(
                state.inserted,
                vec![("sticker-v2".to_owned(), -100, Some(0))]
            );
        });
        let snapshot = queue.snapshot();
        assert!(snapshot.immediate.is_empty());
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "sticker-v2");

        Ok(())
    }

    #[test]
    fn queue_photo_message_enqueues_direct_chattable_without_virtual_mapping_and_keeps_persistence()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = PhotoMessageRequest {
            chat: ChatRef {
                id: 42,
                is_forum: true,
            },
            message_thread_id: 7,
            disable_notification: true,
            photo: PhotoSource::FileId("photo-file-id".to_owned()),
            caption: "<b>done</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            has_spoiler: true,
            reply_parameters: None,
        };

        let report = queue_photo_message(
            &queue,
            QueuePhotoRequest {
                message: &request,
                immediate: false,
            },
        )?;

        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(!report.immediate);
        let snapshot = queue.snapshot();
        assert!(snapshot.immediate.is_empty());
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "");
        assert_eq!(snapshot.regular[0].fingerprint_key, "42:photo:a2fb5546");

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;
        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        let item = &persisted.items[0];
        assert_eq!(item.message_type, "*api.PhotoConfig");
        assert_eq!(item.virtual_id, "");
        assert_eq!(item.chat_id, 42);
        let payload: serde_json::Value = serde_json::from_slice(&item.message)?;
        assert_eq!(payload["ChatID"], 42);
        assert_eq!(payload["MessageThreadID"], 7);
        assert_eq!(payload["File"], "photo-file-id");
        assert_eq!(payload["Caption"], "<b>done</b>");
        assert_eq!(payload["ParseMode"], TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(payload["HasSpoiler"], true);

        Ok(())
    }

    #[test]
    fn queue_audio_message_enqueues_direct_chattable_without_virtual_mapping_and_keeps_persistence()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = AudioMessageRequest {
            chat: ChatRef {
                id: 42,
                is_forum: true,
            },
            message_thread_id: 7,
            disable_notification: true,
            audio: AudioSource::FileId("song-file-id".to_owned()),
            caption: "<b>song</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_parameters: None,
        };

        let report = queue_audio_message(
            &queue,
            QueueAudioRequest {
                message: &request,
                immediate: true,
            },
        )?;

        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(report.immediate);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert_eq!(snapshot.immediate[0].virtual_id, "");
        assert_eq!(snapshot.immediate[0].fingerprint_key, "42:audio:18202a3a");
        assert!(snapshot.regular.is_empty());

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;
        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        let item = &persisted.items[0];
        assert_eq!(item.message_type, "*api.AudioConfig");
        assert_eq!(item.virtual_id, "");
        assert_eq!(item.chat_id, 42);
        let payload: serde_json::Value = serde_json::from_slice(&item.message)?;
        assert_eq!(payload["ChatID"], 42);
        assert_eq!(payload["MessageThreadID"], 7);
        assert_eq!(payload["File"], "song-file-id");
        assert_eq!(payload["Caption"], "<b>song</b>");
        assert_eq!(payload["ParseMode"], TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(payload["Duration"], 0);
        assert_eq!(payload["Performer"], "");
        assert_eq!(payload["Title"], "");

        Ok(())
    }

    #[test]
    fn queue_media_group_message_uses_go_identity_fingerprint_and_keeps_persistence()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig {
            dedupe_config: openplotva_telegram::DebouncerConfig {
                enabled: true,
                ..openplotva_telegram::DebouncerConfig::default()
            },
            ..DispatcherConfig::default()
        });
        let request = media_group_request();
        let same_content = media_group_request();

        let first = queue_media_group_message(
            &queue,
            QueueMediaGroupRequest {
                message: &request,
                immediate: false,
            },
        )?;
        let second = queue_media_group_message(
            &queue,
            QueueMediaGroupRequest {
                message: &same_content,
                immediate: false,
            },
        )?;
        let duplicate = queue_media_group_message(
            &queue,
            QueueMediaGroupRequest {
                message: &request,
                immediate: false,
            },
        )?;

        assert_eq!(first.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert_eq!(second.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert_eq!(duplicate.enqueue_outcome, EnqueueOutcome::Deduped);
        let snapshot = queue.snapshot();
        assert!(snapshot.immediate.is_empty());
        assert_eq!(snapshot.regular.len(), 2);
        assert_eq!(snapshot.regular[0].virtual_id, "");
        assert_eq!(snapshot.regular[1].virtual_id, "");
        assert!(
            snapshot.regular[0]
                .fingerprint_key
                .starts_with("42:media_group:")
        );
        assert!(
            snapshot.regular[1]
                .fingerprint_key
                .starts_with("42:media_group:")
        );
        assert_ne!(
            snapshot.regular[0].fingerprint_key,
            snapshot.regular[1].fingerprint_key
        );

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;
        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 2);
        let item = &persisted.items[0];
        assert_eq!(item.message_type, "*api.MediaGroupConfig");
        assert_eq!(item.virtual_id, "");
        assert_eq!(item.chat_id, 42);
        let payload: serde_json::Value = serde_json::from_slice(&item.message)?;
        assert_eq!(payload["ChatID"], 42);
        assert_eq!(payload["MessageThreadID"], 7);
        assert_eq!(payload["DisableNotification"], true);
        assert_eq!(payload["Media"][0]["media"], "first-photo");
        assert_eq!(payload["Media"][0]["caption"], "<b>album</b>");
        assert_eq!(payload["Media"][0]["parse_mode"], TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(payload["Media"][1]["media"], "second-photo");
        assert_eq!(payload["Media"][1]["has_spoiler"], true);

        Ok(())
    }

    #[test]
    fn queue_edit_media_message_enqueues_direct_edit_without_virtual_mapping_and_keeps_persistence()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = EditMediaMessageRequest {
            chat: ChatRef {
                id: 42,
                is_forum: true,
            },
            message_id: 99,
            media: MediaGroupPhotoItem {
                photo: PhotoSource::FileId("replacement-photo".to_owned()),
                caption: "<b>done</b>".to_owned(),
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                has_spoiler: true,
            },
        };

        let report = queue_edit_media_message(
            &queue,
            QueueEditMediaRequest {
                message: &request,
                immediate: true,
            },
        )?;

        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(report.immediate);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert_eq!(snapshot.immediate[0].virtual_id, "");
        assert!(
            snapshot.immediate[0]
                .fingerprint_key
                .starts_with("42:unknown:")
        );
        assert!(snapshot.regular.is_empty());

        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;
        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        let item = &persisted.items[0];
        assert_eq!(item.message_type, "*api.EditMessageMediaConfig");
        assert_eq!(item.virtual_id, "");
        assert_eq!(item.chat_id, 42);
        let payload: serde_json::Value = serde_json::from_slice(&item.message)?;
        assert_eq!(payload["ChatID"], 42);
        assert_eq!(payload["MessageID"], 99);
        assert_eq!(payload["Media"]["media"], "replacement-photo");
        assert_eq!(payload["Media"]["caption"], "<b>done</b>");
        assert_eq!(payload["Media"]["parse_mode"], TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(payload["Media"]["has_spoiler"], true);

        Ok(())
    }

    #[test]
    fn queue_edit_text_message_enqueues_direct_edit_without_virtual_mapping_and_keeps_persistence()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = EditTextMessageRequest {
            chat: ChatRef {
                id: 42,
                is_forum: false,
            },
            message_id: 99,
            text: "<b>updated</b>".to_owned(),
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        };

        let report = queue_edit_text_message(
            &queue,
            QueueEditTextRequest {
                message: &request,
                immediate: true,
                bypass_chat_restrictions: true,
            },
        )?;

        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(report.immediate);
        let item = queue.dequeue_immediate().expect("queued edit item");
        assert_eq!(item.metadata().virtual_id, "");
        assert!(item.metadata().fingerprint_key.starts_with("42:unknown:"));
        assert!(item.bypasses_chat_restrictions());
        let (_, method, _) = item.into_persistence_parts();
        assert_eq!(
            method.as_ref().map(TelegramOutboundMethod::kind),
            Some(TelegramOutboundMethodKind::EditMessageText)
        );

        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let _ = queue_edit_text_message(
            &queue,
            QueueEditTextRequest {
                message: &request,
                immediate: false,
                bypass_chat_restrictions: false,
            },
        )?;
        let persisted = persistent_queue_from_drain(queue.drain_for_shutdown(), 100)?;
        assert_eq!(persisted.skipped, 0);
        assert_eq!(persisted.items.len(), 1);
        let item = &persisted.items[0];
        assert_eq!(item.message_type, "*api.EditMessageTextConfig");
        assert_eq!(item.virtual_id, "");
        assert_eq!(item.chat_id, 42);
        let payload: serde_json::Value = serde_json::from_slice(&item.message)?;
        assert_eq!(payload["chat_id"], 42);
        assert_eq!(payload["message_id"], 99);
        assert_eq!(payload["text"], "<b>updated</b>");
        assert_eq!(payload["parse_mode"], TELEGRAM_PARSE_MODE_HTML);

        Ok(())
    }

    #[tokio::test]
    async fn send_work_item_and_resolve_records_mapping_from_successful_message_response()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let item = queued_text_item("v1");

        let report = send_work_item_and_resolve(&store, item, |_| async {
            Ok::<_, StubError>(TelegramOutboundResponse::Message(Box::new(
                telegram_message(42, 77),
            )))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Sent);
        assert_eq!(report.virtual_id, "v1");
        assert_eq!(report.resolved_message_id, Some(77));
        assert_eq!(report.resolve_error, None);
        store.snapshot(|state| {
            assert_eq!(state.resolved, vec![("v1".to_owned(), 77)]);
        });

        Ok(())
    }

    #[tokio::test]
    async fn send_work_item_and_resolve_keeps_send_success_when_mapping_resolution_fails()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            resolve_error: Some(StubError("resolve failed")),
            ..StoreState::default()
        });
        let item = queued_text_item("v1");

        let report = send_work_item_and_resolve(&store, item, |_| async {
            Ok::<_, StubError>(TelegramOutboundResponse::Message(Box::new(
                telegram_message(42, 77),
            )))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Sent);
        assert_eq!(report.resolved_message_id, Some(77));
        assert_eq!(report.resolve_error, Some("resolve failed".to_owned()));
        store.snapshot(|state| {
            assert_eq!(state.resolved, vec![("v1".to_owned(), 77)]);
        });

        Ok(())
    }

    #[tokio::test]
    async fn send_work_item_and_resolve_does_not_resolve_after_send_failure() {
        let store = StoreStub::default();
        let item = queued_text_item("v1");

        let report = send_work_item_and_resolve(&store, item, |_| async {
            Err::<TelegramOutboundResponse, _>(StubError("telegram failed"))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Failed);
        assert_eq!(report.send_error, Some("telegram failed".to_owned()));
        assert_eq!(report.resolved_message_id, None);
        store.snapshot(|state| {
            assert!(state.resolved.is_empty());
        });
    }

    #[tokio::test]
    async fn edit_text_virtual_sends_now_when_mapping_is_resolved() -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::resolved("v1", 42, 77)),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();
        let mut canceled = Vec::new();

        let report = edit_text_virtual(
            &store,
            &history,
            VirtualEditRequest {
                chat_id: 42,
                vmsg_id: "v1",
                text: "<b>edited</b>",
                parse_mode: "HTML",
            },
            |method| {
                sent.push(method_payload(method));
                async { Ok::<(), StubError>(()) }
            },
            |vmsg_id| {
                canceled.push(vmsg_id.to_owned());
                0
            },
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::SentNow,
                real_message_id: Some(77),
                history_updated: true,
                enqueued_op_id: None,
                lookup_error: None,
                enqueue_error: None,
                canceled: 0,
                history_deleted: false,
                mapping_deleted: false,
                delete_mapping_error: None,
            }
        );
        assert_eq!(
            sent,
            vec![(
                TelegramOutboundMethodKind::EditMessageText,
                json!({
                    "chat_id": 42,
                    "message_id": 77,
                    "parse_mode": "HTML",
                    "text": "<b>edited</b>",
                })
            )]
        );
        assert!(canceled.is_empty());
        store.snapshot(|state| {
            assert_eq!(state.lookup_calls, vec!["v1"]);
            assert!(state.enqueued.is_empty());
            assert_eq!(
                state.events,
                vec!["history:update:42:77:<b>edited</b>:HTML"]
            );
        });

        Ok(())
    }

    #[tokio::test]
    async fn edit_text_virtual_rejects_empty_text_like_go() {
        let store = StoreStub::default();
        let history = store.history();

        let error = edit_text_virtual(
            &store,
            &history,
            VirtualEditRequest {
                chat_id: 42,
                vmsg_id: "v1",
                text: "",
                parse_mode: "",
            },
            |_| async { Ok::<(), StubError>(()) },
            |_| 0,
        )
        .await
        .expect_err("empty virtual edit should fail");

        assert_eq!(error, VirtualMessageError::EmptyText);
        assert_eq!(error.to_string(), "text is empty");
        store.snapshot(|state| {
            assert!(state.lookup_calls.is_empty());
            assert!(state.enqueued.is_empty());
        });
    }

    #[tokio::test]
    async fn edit_text_virtual_queues_and_cancels_when_mapping_is_unresolved()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::unresolved("v1", 42)),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();
        let mut canceled = Vec::new();

        let report = edit_text_virtual(
            &store,
            &history,
            VirtualEditRequest {
                chat_id: 42,
                vmsg_id: "v1",
                text: "edited",
                parse_mode: "HTML",
            },
            |method| {
                sent.push(method.kind());
                async { Ok::<(), StubError>(()) }
            },
            |vmsg_id| {
                canceled.push(vmsg_id.to_owned());
                1
            },
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::Queued,
                enqueued_op_id: Some(1),
                canceled: 1,
                real_message_id: None,
                lookup_error: None,
                enqueue_error: None,
                history_updated: false,
                history_deleted: false,
                mapping_deleted: false,
                delete_mapping_error: None,
            }
        );
        assert!(sent.is_empty());
        assert_eq!(canceled, vec!["v1"]);
        store.snapshot(|state| {
            assert_eq!(state.lookup_calls, vec!["v1"]);
            assert_eq!(state.enqueued.len(), 1);
            let (vmsg_id, chat_id, op, payload) = &state.enqueued[0];
            assert_eq!(vmsg_id, "v1");
            assert_eq!(*chat_id, 42);
            assert_eq!(*op, PENDING_OP_EDIT);
            let payload = payload.as_deref().expect("edit payload");
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(payload).expect("payload json"),
                json!({
                    "parse_mode": "HTML",
                    "text": "edited",
                })
            );
            assert!(state.events.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn delete_message_virtual_sends_now_updates_history_and_deletes_mapping()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::resolved("v2", 42, 78)),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();

        let report = delete_message_virtual(
            &store,
            &history,
            VirtualDeleteRequest {
                chat_id: 42,
                vmsg_id: "v2",
            },
            |method| {
                sent.push(method_payload(method));
                async { Ok::<(), StubError>(()) }
            },
            |_| 0,
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::SentNow,
                real_message_id: Some(78),
                history_deleted: true,
                mapping_deleted: true,
                enqueued_op_id: None,
                lookup_error: None,
                enqueue_error: None,
                canceled: 0,
                history_updated: false,
                delete_mapping_error: None,
            }
        );
        assert_eq!(
            sent,
            vec![(
                TelegramOutboundMethodKind::DeleteMessage,
                json!({
                    "chat_id": 42,
                    "message_id": 78,
                })
            )]
        );
        store.snapshot(|state| {
            assert_eq!(state.lookup_calls, vec!["v2"]);
            assert_eq!(state.deleted_mappings, vec!["v2"]);
            assert_eq!(state.events, vec!["history:delete:42:78"]);
            assert!(state.enqueued.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn delete_message_virtual_queues_after_mapping_lookup_failure()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::with_state(StoreState {
            lookup_error: Some(StubError("db lookup")),
            enqueue_error: Some(StubError("enqueue failed")),
            ..StoreState::default()
        });
        let history = store.history();
        let mut sent = Vec::new();
        let mut canceled = Vec::new();

        let report = delete_message_virtual(
            &store,
            &history,
            VirtualDeleteRequest {
                chat_id: 42,
                vmsg_id: "v2",
            },
            |method| {
                sent.push(method.kind());
                async { Ok::<(), StubError>(()) }
            },
            |vmsg_id| {
                canceled.push(vmsg_id.to_owned());
                2
            },
        )
        .await?;

        assert_eq!(
            report,
            VirtualMessageReport {
                action: VirtualMessageAction::Queued,
                lookup_error: Some("db lookup".to_owned()),
                enqueue_error: Some("enqueue failed".to_owned()),
                canceled: 2,
                real_message_id: None,
                enqueued_op_id: None,
                history_updated: false,
                history_deleted: false,
                mapping_deleted: false,
                delete_mapping_error: None,
            }
        );
        assert!(sent.is_empty());
        assert_eq!(canceled, vec!["v2"]);
        store.snapshot(|state| {
            assert_eq!(
                state.enqueued,
                vec![("v2".to_owned(), 42, PENDING_OP_DELETE, None)]
            );
            assert!(state.events.is_empty());
            assert!(state.deleted_mappings.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn immediate_send_error_returns_before_history_and_mapping_delete() {
        let store = StoreStub::with_state(StoreState {
            mapping: Some(MessageIdMapping::resolved("v2", 42, 78)),
            ..StoreState::default()
        });
        let history = store.history();

        let error = delete_message_virtual(
            &store,
            &history,
            VirtualDeleteRequest {
                chat_id: 42,
                vmsg_id: "v2",
            },
            |_| async { Err::<(), StubError>(StubError("telegram failed")) },
            |_| 0,
        )
        .await
        .expect_err("Telegram delete failure should propagate");

        assert_eq!(
            error,
            VirtualMessageError::Send("telegram failed".to_owned())
        );
        store.snapshot(|state| {
            assert!(state.events.is_empty());
            assert!(state.deleted_mappings.is_empty());
            assert!(state.enqueued.is_empty());
        });
    }

    fn method_payload(
        method: TelegramOutboundMethod,
    ) -> (TelegramOutboundMethodKind, serde_json::Value) {
        let kind = method.kind();
        let payload = match method {
            TelegramOutboundMethod::EditMessageText(method) => {
                serde_json::to_value(method.as_ref()).expect("edit payload")
            }
            TelegramOutboundMethod::DeleteMessage(method) => {
                serde_json::to_value(method.as_ref()).expect("delete payload")
            }
            other => panic!("unexpected method kind: {:?}", other.kind()),
        };
        (kind, payload)
    }

    fn text_request(text: &str) -> TextMessageRequest {
        TextMessageRequest {
            chat: Some(ChatRef {
                id: 42,
                is_forum: false,
            }),
            message_thread_id: 0,
            disable_notification: false,
            allow_sending_without_reply: None,
            text: text.to_owned(),
            render_as: String::new(),
            reply_markup: None,
        }
    }

    fn media_group_request() -> MediaGroupMessageRequest {
        MediaGroupMessageRequest {
            chat: ChatRef {
                id: 42,
                is_forum: true,
            },
            message_thread_id: 7,
            disable_notification: true,
            items: vec![
                MediaGroupPhotoItem {
                    photo: PhotoSource::FileId("first-photo".to_owned()),
                    caption: "<b>album</b>".to_owned(),
                    render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                    has_spoiler: false,
                },
                MediaGroupPhotoItem {
                    photo: PhotoSource::FileId("second-photo".to_owned()),
                    caption: String::new(),
                    render_as: String::new(),
                    has_spoiler: true,
                },
            ],
            reply_parameters: Some(openplotva_telegram::ReplyParametersPlan {
                message_id: 99,
                chat_id: 42,
                allow_sending_without_reply: true,
            }),
        }
    }

    fn queued_text_item(virtual_id: &str) -> openplotva_telegram::DispatcherWorkItem {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let method = build_text_message_method(
            &text_request("hello"),
            ChatRef {
                id: 42,
                is_forum: false,
            },
            None,
            "hello",
            true,
        )
        .expect("text method");
        assert_eq!(
            queue.enqueue(
                DispatcherMessage::new(fingerprint_text_message_part(42, "hello"), virtual_id)
                    .with_method(TelegramOutboundMethod::from(method)),
                false,
            ),
            EnqueueOutcome::Enqueued
        );
        queue.dequeue_regular().expect("queued work item")
    }

    fn telegram_message(chat_id: i64, message_id: i64) -> TelegramMessage {
        serde_json::from_value(json!({
            "message_id": message_id,
            "date": 0,
            "chat": {
                "type": "private",
                "id": chat_id,
                "first_name": "Plotva",
            },
            "from": {
                "id": 1,
                "is_bot": true,
                "first_name": "Plotva",
            },
        }))
        .expect("telegram message")
    }
}
