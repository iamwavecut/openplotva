//! Composition-root virtual-message send/edit/delete behavior.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use openplotva_telegram::{
    AudioMessageRequest, DeleteMessageRequest, DispatcherMessage, DispatcherQueue,
    DispatcherSendStatus, DispatcherWorkItem, EditMediaMessageRequest, EditTextMessageRequest,
    EnqueueOutcome, MediaGroupMessageRequest, MediaGroupPhotoItem, MessageFingerprint,
    OutboundBuildError, PhotoMessageRequest, ReplyMessageRef, RichMessageRequest,
    StickerMessageRequest, TELEGRAM_TEXT_MAX_BYTES, TelegramOutboundMethod,
    TelegramOutboundResponse, TextMessageRequest, build_audio_message_method,
    build_audio_message_plan, build_delete_message_method, build_edit_media_message_method,
    build_edit_media_message_plan, build_edit_text_message_method,
    build_media_group_message_method, build_media_group_message_plan, build_photo_message_method,
    build_photo_message_plan, build_rich_message_method, build_sticker_message_method,
    build_sticker_message_plan, build_text_message_method, fingerprint_audio_message_plan,
    fingerprint_photo_message_plan, fingerprint_rich_message, fingerprint_sticker_message_plan,
    fingerprint_text_message_part, hash_content, message_target_chat, split_telegram_text,
    validate_text_message_text,
};
use time::OffsetDateTime;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Shared generator for virtual-message IDs used by app-level producers.
pub type VirtualIdFactory = Arc<dyn Fn() -> String + Send + Sync>;

/// Build a process-local monotonic virtual-message ID factory.
#[must_use]
pub fn monotonic_virtual_id_factory(prefix: &'static str) -> VirtualIdFactory {
    let next_id = Arc::new(AtomicU64::new(1));
    let process_id = std::process::id();
    let started_at = OffsetDateTime::now_utc().unix_timestamp_nanos();
    Arc::new(move || {
        let id = next_id.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}-{process_id:x}-{started_at:x}-{id:x}")
    })
}

pub trait EphemeralMessageTracker {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Track a real Telegram message for later deletion.
    fn track_ephemeral_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        delete_after: Duration,
        now: OffsetDateTime,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

impl EphemeralMessageTracker for openplotva_storage::RedisEphemeralMessageStore {
    type Error = openplotva_storage::StorageError;

    fn track_ephemeral_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        delete_after: Duration,
        now: OffsetDateTime,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            let expires_at =
                now + time::Duration::try_from(delete_after).unwrap_or(time::Duration::MAX);
            let message = openplotva_storage::EphemeralMessage {
                chat_id,
                message_id: i64::from(message_id),
                expires_at,
            };
            self.set_ephemeral_message(
                &message,
                openplotva_storage::ephemeral_redis_ttl(
                    delete_after,
                    openplotva_storage::EPHEMERAL_DEFAULT_CLEANUP_INTERVAL,
                ),
            )
            .await
        })
    }
}

pub trait EphemeralCleanupStore {
    /// Store error type.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load all tracked ephemeral messages.
    fn ephemeral_messages<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Vec<openplotva_storage::EphemeralMessage>, Self::Error>>;

    /// Delete tracked ephemeral-message records after Telegram delete attempts.
    fn delete_ephemeral_messages<'a>(
        &'a self,
        messages: &'a [openplotva_storage::EphemeralMessage],
    ) -> BoxFuture<'a, Result<(), Self::Error>>;
}

impl EphemeralCleanupStore for openplotva_storage::RedisEphemeralMessageStore {
    type Error = openplotva_storage::StorageError;

    fn ephemeral_messages<'a>(
        &'a self,
    ) -> BoxFuture<'a, Result<Vec<openplotva_storage::EphemeralMessage>, Self::Error>> {
        Box::pin(async move { self.ephemeral_messages().await })
    }

    fn delete_ephemeral_messages<'a>(
        &'a self,
        messages: &'a [openplotva_storage::EphemeralMessage],
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move { self.delete_ephemeral_messages(messages).await })
    }
}

/// Ephemeral tracker for tests and runtime paths that do not own ephemeral sends.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopEphemeralMessageTracker;

impl EphemeralMessageTracker for NoopEphemeralMessageTracker {
    type Error = std::convert::Infallible;

    fn track_ephemeral_message<'a>(
        &'a self,
        _chat_id: i64,
        _message_id: i32,
        _delete_after: Duration,
        _now: OffsetDateTime,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
}

/// History side effect triggered after a successful dispatcher edit.
pub trait EditHistorySink {
    /// Record an edited message's new text in chat history.
    fn update_text<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        text: &'a str,
        parse_mode: &'a str,
    ) -> BoxFuture<'a, ()>;
}

/// No-op history sink for call sites that intentionally skip persistence.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoopEditHistorySink;

impl EditHistorySink for NoopEditHistorySink {
    fn update_text<'a>(
        &'a self,
        _chat_id: i64,
        _message_id: i32,
        _text: &'a str,
        _parse_mode: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async {})
    }
}

impl EditHistorySink for openplotva_storage::PostgresHistoryStore {
    fn update_text<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        text: &'a str,
        _parse_mode: &'a str,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if let Err(error) = self.update_text_entry(chat_id, message_id, text).await {
                tracing::warn!(
                    chat_id,
                    message_id,
                    %error,
                    "failed to update chat history entry after Telegram edit"
                );
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueTextRequest<'a> {
    /// Telegram text request fields.
    pub message: &'a TextMessageRequest,
    /// Optional replied-to message fields.
    pub reply_to: Option<&'a ReplyMessageRef>,
    pub immediate_first: bool,
    pub bypass_chat_restrictions: bool,
    pub ephemeral_delete_after: Option<Duration>,
    /// Protect the queued parts from dispatcher queue-overflow trimming.
    pub protected: bool,
    /// Namespace the dedupe fingerprint (e.g. reply-scoped `r{message_id}`)
    /// so identical text in different contexts is not deduped away.
    pub debounce_key: Option<&'a str>,
}

pub struct QueueRichRequest<'a> {
    pub message: &'a RichMessageRequest,
    pub reply_to: Option<&'a ReplyMessageRef>,
    pub immediate: bool,
    pub bypass_chat_restrictions: bool,
    pub ephemeral_delete_after: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueStickerRequest<'a> {
    /// Telegram sticker request fields.
    pub message: &'a StickerMessageRequest,
    /// Optional replied-to message fields.
    pub reply_to: Option<&'a ReplyMessageRef>,
    pub immediate: bool,
    pub bypass_chat_restrictions: bool,
    pub ephemeral_delete_after: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueueEditTextRequest<'a> {
    /// Telegram edit-text request fields.
    pub message: &'a EditTextMessageRequest,
    /// Whether the edit should enter the immediate queue.
    pub immediate: bool,
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
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueTextReport {
    /// Split text parts that were queued or deduped.
    pub parts: Vec<QueuedTextPartReport>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueStickerReport {
    /// Virtual message ID generated before queueing.
    pub virtual_id: String,
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this sticker went to the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuePhotoReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this photo went to the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueAudioReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this audio went to the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueEditTextReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this edit went to the immediate queue.
    pub immediate: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueueMediaGroupReport {
    pub enqueue_outcome: EnqueueOutcome,
    /// Whether this media group went to the immediate queue.
    pub immediate: bool,
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchSendReport {
    pub status: DispatcherSendStatus,
    /// Virtual message ID carried by the dispatcher item.
    pub virtual_id: String,
    /// Real Telegram message ID extracted from a successful send response.
    pub sent_message_id: Option<i32>,
    /// Telegram send error returned by the transport callback.
    pub send_error: Option<String>,
    pub ephemeral_track_error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EphemeralCleanupReport {
    /// Number of tracked records loaded from the store.
    pub loaded: usize,
    pub expired: usize,
    /// Number of Telegram delete requests attempted.
    pub telegram_delete_attempted: usize,
    /// Number of Telegram delete requests that failed.
    pub telegram_delete_failed: usize,
    /// Last Telegram delete error observed in this tick.
    pub telegram_delete_error: Option<String>,
    /// Number of expired records removed from the store.
    pub store_deleted: usize,
    /// Number of cleanup batches whose store deletion failed.
    pub store_delete_failed_batches: usize,
    /// Store list error, if the tick could not load records.
    pub list_error: Option<String>,
    /// Last store deletion error, if any cleanup batch failed to delete records.
    pub store_delete_error: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EphemeralCleanupWorkerReport {
    /// Number of cleanup ticks processed before stop.
    pub ticks: usize,
    /// Total records loaded from the store.
    pub loaded: usize,
    /// Total expired records found.
    pub expired: usize,
    /// Total Telegram delete requests attempted.
    pub telegram_delete_attempted: usize,
    /// Total Telegram delete requests that failed.
    pub telegram_delete_failed: usize,
    /// Last Telegram delete error observed by the worker.
    pub last_telegram_delete_error: Option<String>,
    /// Total expired records removed from the store.
    pub store_deleted: usize,
    /// Total store deletion batches that failed.
    pub store_delete_failed_batches: usize,
    /// Number of ticks that failed while listing store records.
    pub list_errors: usize,
    /// Last list error observed by the worker.
    pub last_list_error: Option<String>,
    /// Last store deletion error observed by the worker.
    pub last_store_delete_error: Option<String>,
}

impl EphemeralCleanupWorkerReport {
    fn record_tick(&mut self, tick: &EphemeralCleanupReport) {
        self.ticks += 1;
        self.loaded += tick.loaded;
        self.expired += tick.expired;
        self.telegram_delete_attempted += tick.telegram_delete_attempted;
        self.telegram_delete_failed += tick.telegram_delete_failed;
        if let Some(error) = &tick.telegram_delete_error {
            self.last_telegram_delete_error = Some(error.clone());
        }
        self.store_deleted += tick.store_deleted;
        self.store_delete_failed_batches += tick.store_delete_failed_batches;
        if let Some(error) = &tick.list_error {
            self.list_errors += 1;
            self.last_list_error = Some(error.clone());
        }
        if let Some(error) = &tick.store_delete_error {
            self.last_store_delete_error = Some(error.clone());
        }
    }
}

pub async fn process_ephemeral_cleanup_once_at<S, Send, SendFuture, SendError>(
    store: &S,
    now: OffsetDateTime,
    mut send_delete: Send,
) -> EphemeralCleanupReport
where
    S: EphemeralCleanupStore + Sync,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
{
    let messages = match store.ephemeral_messages().await {
        Ok(messages) => messages,
        Err(error) => {
            return EphemeralCleanupReport {
                list_error: Some(error.to_string()),
                ..EphemeralCleanupReport::default()
            };
        }
    };

    let expired = openplotva_storage::expired_ephemeral_messages_at(&messages, now);
    let mut report = EphemeralCleanupReport {
        loaded: messages.len(),
        expired: expired.len(),
        ..EphemeralCleanupReport::default()
    };

    for batch in expired.chunks(openplotva_storage::EPHEMERAL_CLEANUP_BATCH_SIZE) {
        for message in batch {
            let Ok(method) = build_delete_message_method(&DeleteMessageRequest {
                chat_id: message.chat_id,
                message_id: message.message_id,
            }) else {
                continue;
            };
            report.telegram_delete_attempted += 1;
            if let Err(error) = send_delete(TelegramOutboundMethod::from(method)).await {
                report.telegram_delete_failed += 1;
                report.telegram_delete_error = Some(error.to_string());
            }
        }

        match store.delete_ephemeral_messages(batch).await {
            Ok(()) => {
                report.store_deleted += batch.len();
            }
            Err(error) => {
                report.store_delete_failed_batches += 1;
                report.store_delete_error = Some(error.to_string());
            }
        }
    }

    report
}

pub async fn run_ephemeral_cleanup_worker_until<S, Send, SendFuture, SendError, Stop>(
    store: &S,
    send_delete: Send,
    stop: Stop,
) -> EphemeralCleanupWorkerReport
where
    S: EphemeralCleanupStore + Sync,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Stop: Future<Output = ()>,
{
    run_ephemeral_cleanup_worker_every_until(
        store,
        send_delete,
        openplotva_storage::EPHEMERAL_DEFAULT_CLEANUP_INTERVAL,
        stop,
    )
    .await
}

pub async fn run_ephemeral_cleanup_worker_every_until<S, Send, SendFuture, SendError, Stop>(
    store: &S,
    mut send_delete: Send,
    interval: Duration,
    stop: Stop,
) -> EphemeralCleanupWorkerReport
where
    S: EphemeralCleanupStore + Sync,
    Send: FnMut(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<(), SendError>>,
    SendError: fmt::Display,
    Stop: Future<Output = ()>,
{
    let mut report = EphemeralCleanupWorkerReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                let tick = process_ephemeral_cleanup_once_at(
                    store,
                    OffsetDateTime::now_utc(),
                    &mut send_delete,
                )
                .await;
                trace_ephemeral_cleanup_tick(&tick);
                report.record_tick(&tick);
            }
        }
    }

    report
}

fn trace_ephemeral_cleanup_tick(tick: &EphemeralCleanupReport) {
    if tick.loaded == 0 && tick.list_error.is_none() {
        return;
    }

    tracing::debug!(
        loaded = tick.loaded,
        expired = tick.expired,
        telegram_delete_attempted = tick.telegram_delete_attempted,
        telegram_delete_failed = tick.telegram_delete_failed,
        telegram_delete_error = tick.telegram_delete_error.as_deref(),
        store_deleted = tick.store_deleted,
        store_delete_failed_batches = tick.store_delete_failed_batches,
        list_error = tick.list_error.as_deref(),
        store_delete_error = tick.store_delete_error.as_deref(),
        "processed ephemeral message cleanup tick"
    );
}

pub async fn queue_text_message_parts<NextId>(
    queue: &DispatcherQueue,
    req: QueueTextRequest<'_>,
    mut next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
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
        let method = build_text_message_method(
            req.message,
            chat,
            req.reply_to,
            part.clone(),
            index + 1 == total_parts,
        )?;
        let mut dispatcher_message =
            DispatcherMessage::new(fingerprint_text_message_part(chat.id, &part), &virtual_id)
                .with_method(TelegramOutboundMethod::from(method))
                .with_bypass_chat_restrictions(req.bypass_chat_restrictions)
                .with_protected(req.protected);
        if let Some(debounce_key) = req.debounce_key {
            dispatcher_message = dispatcher_message.with_debounce_key(debounce_key);
        }
        if index == 0
            && let Some(delete_after) = req.ephemeral_delete_after
        {
            dispatcher_message = dispatcher_message.with_ephemeral_delete_after(delete_after);
        }
        let immediate = req.immediate_first && index == 0;
        let enqueue_outcome = queue.enqueue(dispatcher_message, immediate);
        report.parts.push(QueuedTextPartReport {
            index,
            virtual_id,
            enqueue_outcome,
            immediate,
        });
    }

    Ok(report)
}

pub async fn queue_rich_message<NextId>(
    queue: &DispatcherQueue,
    req: QueueRichRequest<'_>,
    mut next_virtual_id: NextId,
) -> Result<QueueTextReport, OutboundBuildError>
where
    NextId: FnMut() -> String,
{
    let chat = message_target_chat(req.message.chat.as_ref(), req.reply_to)?;
    let method = build_rich_message_method(req.message, chat, req.reply_to)?;
    let virtual_id = next_virtual_id();
    let mut dispatcher_message =
        DispatcherMessage::new(fingerprint_rich_message(chat.id, &method.html), &virtual_id)
            .with_method(TelegramOutboundMethod::from(method))
            .with_bypass_chat_restrictions(req.bypass_chat_restrictions);
    if let Some(delete_after) = req.ephemeral_delete_after {
        dispatcher_message = dispatcher_message.with_ephemeral_delete_after(delete_after);
    }
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);
    let mut report = QueueTextReport::default();
    report.parts.push(QueuedTextPartReport {
        index: 0,
        virtual_id,
        enqueue_outcome,
        immediate: req.immediate,
    });
    Ok(report)
}

pub async fn queue_sticker_message<NextId>(
    queue: &DispatcherQueue,
    req: QueueStickerRequest<'_>,
    mut next_virtual_id: NextId,
) -> Result<QueueStickerReport, OutboundBuildError>
where
    NextId: FnMut() -> String,
{
    message_target_chat(req.message.chat.as_ref(), req.reply_to)?;
    let virtual_id = next_virtual_id();
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
    let dispatcher_message = if let Some(delete_after) = req.ephemeral_delete_after {
        dispatcher_message.with_ephemeral_delete_after(delete_after)
    } else {
        dispatcher_message
    };
    let enqueue_outcome = queue.enqueue(dispatcher_message, req.immediate);

    Ok(QueueStickerReport {
        virtual_id,
        enqueue_outcome,
        immediate: req.immediate,
    })
}

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

/// Send one dispatcher item and report the real Telegram message ID from the response.
pub async fn send_work_item<Send, SendFuture, SendError>(
    item: DispatcherWorkItem,
    send: Send,
) -> DispatchSendReport
where
    Send: FnOnce(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<TelegramOutboundResponse, SendError>>,
    SendError: fmt::Display,
{
    send_work_item_inner(
        None::<&NoopEditHistorySink>,
        None::<&NoopEphemeralMessageTracker>,
        item,
        OffsetDateTime::now_utc(),
        send,
    )
    .await
}

/// Send one dispatcher item, report its real message ID, and track ephemeral sends.
pub async fn send_work_item_with_ephemeral<E, Send, SendFuture, SendError>(
    ephemeral: &E,
    item: DispatcherWorkItem,
    now: OffsetDateTime,
    send: Send,
) -> DispatchSendReport
where
    E: EphemeralMessageTracker + Sync,
    Send: FnOnce(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<TelegramOutboundResponse, SendError>>,
    SendError: fmt::Display,
{
    send_work_item_inner(
        None::<&NoopEditHistorySink>,
        Some(ephemeral),
        item,
        now,
        send,
    )
    .await
}

/// Send one dispatcher item, update history on direct edits, and track ephemeral sends.
pub async fn send_work_item_with_history_and_ephemeral<H, E, Send, SendFuture, SendError>(
    history: &H,
    ephemeral: &E,
    item: DispatcherWorkItem,
    now: OffsetDateTime,
    send: Send,
) -> DispatchSendReport
where
    H: EditHistorySink,
    E: EphemeralMessageTracker + Sync,
    Send: FnOnce(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<TelegramOutboundResponse, SendError>>,
    SendError: fmt::Display,
{
    send_work_item_inner(Some(history), Some(ephemeral), item, now, send).await
}

async fn send_work_item_inner<H, E, Send, SendFuture, SendError>(
    history: Option<&H>,
    ephemeral: Option<&E>,
    item: DispatcherWorkItem,
    now: OffsetDateTime,
    send: Send,
) -> DispatchSendReport
where
    H: EditHistorySink,
    E: EphemeralMessageTracker + Sync,
    Send: FnOnce(TelegramOutboundMethod) -> SendFuture,
    SendFuture: Future<Output = Result<TelegramOutboundResponse, SendError>>,
    SendError: fmt::Display,
{
    let (metadata, method) = item.into_parts();
    let ephemeral_delete_after = metadata.ephemeral_delete_after;
    let chat_id = metadata.chat_id;
    let Some(method) = method else {
        return DispatchSendReport {
            status: DispatcherSendStatus::Failed,
            virtual_id: metadata.virtual_id,
            sent_message_id: None,
            send_error: None,
            ephemeral_track_error: None,
        };
    };
    let direct_edit_history =
        direct_edit_text_history_update(metadata.chat_id, &metadata.virtual_id, &method);

    let response = match send(method).await {
        Ok(response) => response,
        Err(error) => {
            return DispatchSendReport {
                status: DispatcherSendStatus::Failed,
                virtual_id: metadata.virtual_id,
                sent_message_id: None,
                send_error: Some(error.to_string()),
                ephemeral_track_error: None,
            };
        }
    };

    let mut report = DispatchSendReport {
        status: DispatcherSendStatus::Sent,
        virtual_id: metadata.virtual_id,
        sent_message_id: response_message_id(&response),
        send_error: None,
        ephemeral_track_error: None,
    };

    if let (Some(ephemeral), Some(delete_after), Some(message_id)) =
        (ephemeral, ephemeral_delete_after, report.sent_message_id)
    {
        report.ephemeral_track_error = ephemeral
            .track_ephemeral_message(chat_id, message_id, delete_after, now)
            .await
            .err()
            .map(|error| error.to_string());
    }

    if let (Some(history), Some(edit)) = (history, direct_edit_history) {
        history
            .update_text(edit.chat_id, edit.message_id, &edit.text, &edit.parse_mode)
            .await;
    }

    report
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectEditTextHistoryUpdate {
    chat_id: i64,
    message_id: i32,
    text: String,
    parse_mode: String,
}

fn direct_edit_text_history_update(
    chat_id: i64,
    virtual_id: &str,
    method: &TelegramOutboundMethod,
) -> Option<DirectEditTextHistoryUpdate> {
    if !virtual_id.is_empty() {
        return None;
    }
    let TelegramOutboundMethod::EditMessageText(method) = method else {
        return None;
    };
    let payload = serde_json::to_value(method.as_ref()).ok()?;
    let chat_id = if chat_id == 0 {
        payload.get("chat_id")?.as_i64()?
    } else {
        chat_id
    };
    let message_id = payload
        .get("message_id")?
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())?;
    let text = payload.get("text")?.as_str()?.to_owned();
    let parse_mode = payload
        .get("parse_mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned();

    Some(DirectEditTextHistoryUpdate {
        chat_id,
        message_id,
        text,
        parse_mode,
    })
}

fn response_message_id(response: &TelegramOutboundResponse) -> Option<i32> {
    let raw = match response {
        TelegramOutboundResponse::Message(message) => Some(message.id),
        TelegramOutboundResponse::Messages(messages) => messages.first().map(|message| message.id),
        TelegramOutboundResponse::EditMessage(_)
        | TelegramOutboundResponse::Boolean(_)
        | TelegramOutboundResponse::SentGuestMessage(_)
        | TelegramOutboundResponse::String(_) => None,
    }?;
    i32::try_from(raw).ok()
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fmt,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use carapax::types::EditMessageResult;
    use openplotva_storage::EphemeralMessage;
    use openplotva_telegram::{
        AudioMessageRequest, AudioSource, ChatRef, DispatcherConfig, DispatcherMessage,
        DispatcherQueue, DispatcherSendStatus, EditMediaMessageRequest, EditTextMessageRequest,
        EnqueueOutcome, MediaGroupMessageRequest, MediaGroupPhotoItem, PhotoMessageRequest,
        PhotoSource, TELEGRAM_PARSE_MODE_HTML, TelegramMessage, TelegramOutboundMethod,
        TelegramOutboundMethodKind, TelegramOutboundResponse, TextMessageRequest,
        build_text_message_method, fingerprint_text_message_part, persistent_queue_from_drain,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    use super::EditHistorySink;

    use super::{
        QueueAudioRequest, QueueEditMediaRequest, QueueEditTextRequest, QueueMediaGroupRequest,
        QueuePhotoRequest, QueueStickerRequest, QueueTextRequest, queue_audio_message,
        queue_edit_media_message, queue_edit_text_message, queue_media_group_message,
        queue_photo_message, queue_sticker_message, queue_text_message_parts, send_work_item,
        send_work_item_with_ephemeral, send_work_item_with_history_and_ephemeral,
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
        ephemeral_error: Option<StubError>,
        ephemeral_list_error: Option<StubError>,
        ephemeral_delete_error: Option<StubError>,
        ephemeral_messages: Vec<EphemeralMessage>,
        ephemeral_tracked: Vec<(i64, i32, Duration, OffsetDateTime)>,
        ephemeral_deleted: Vec<Vec<EphemeralMessage>>,
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

    impl EditHistorySink for HistoryStub {
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
    }

    impl super::EphemeralMessageTracker for StoreStub {
        type Error = StubError;

        fn track_ephemeral_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            delete_after: Duration,
            now: OffsetDateTime,
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            let result = {
                let mut state = self.state.lock().expect("store state");
                state
                    .ephemeral_tracked
                    .push((chat_id, message_id, delete_after, now));
                if let Some(error) = &state.ephemeral_error {
                    Err(error.clone())
                } else {
                    Ok(())
                }
            };
            Box::pin(async move { result })
        }
    }

    impl super::EphemeralCleanupStore for StoreStub {
        type Error = StubError;

        fn ephemeral_messages<'a>(
            &'a self,
        ) -> super::BoxFuture<'a, Result<Vec<EphemeralMessage>, Self::Error>> {
            let result = {
                let state = self.state.lock().expect("store state");
                if let Some(error) = &state.ephemeral_list_error {
                    Err(error.clone())
                } else {
                    Ok(state.ephemeral_messages.clone())
                }
            };
            Box::pin(async move { result })
        }

        fn delete_ephemeral_messages<'a>(
            &'a self,
            messages: &'a [EphemeralMessage],
        ) -> super::BoxFuture<'a, Result<(), Self::Error>> {
            let messages = messages.to_vec();
            let result = {
                let mut state = self.state.lock().expect("store state");
                state.ephemeral_deleted.push(messages);
                if let Some(error) = &state.ephemeral_delete_error {
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
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let text = format!("{}b", "a".repeat(4096));
        let request = text_request(&text);
        let mut ids = ["v1".to_owned(), "v2".to_owned()].into_iter();

        let report = queue_text_message_parts(
            &queue,
            QueueTextRequest {
                protected: false,
                debounce_key: None,
                message: &request,
                reply_to: None,
                immediate_first: true,
                bypass_chat_restrictions: false,
                ephemeral_delete_after: None,
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
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.immediate.len(), 1);
        assert_eq!(snapshot.immediate[0].virtual_id, "v1");
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "v2");

        Ok(())
    }

    #[tokio::test]
    async fn queue_text_message_parts_marks_bypass_chat_restrictions_for_dispatcher()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let request = text_request("settings link");

        let report = queue_text_message_parts(
            &queue,
            QueueTextRequest {
                protected: false,
                debounce_key: None,
                message: &request,
                reply_to: None,
                immediate_first: true,
                bypass_chat_restrictions: true,
                ephemeral_delete_after: None,
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
    async fn queue_text_message_parts_marks_only_first_part_ephemeral_like_go()
    -> Result<(), Box<dyn Error>> {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let text = format!("{}b", "a".repeat(4096));
        let request = text_request(&text);
        let mut ids = ["v1".to_owned(), "v2".to_owned()].into_iter();

        let report = queue_text_message_parts(
            &queue,
            QueueTextRequest {
                protected: false,
                debounce_key: None,
                message: &request,
                reply_to: None,
                immediate_first: true,
                bypass_chat_restrictions: false,
                ephemeral_delete_after: Some(Duration::from_secs(60)),
            },
            || ids.next().expect("virtual id"),
        )
        .await?;

        assert_eq!(report.enqueued_count(), 2);
        let snapshot = queue.snapshot();
        assert_eq!(
            snapshot.immediate[0].ephemeral_delete_after,
            Some(Duration::from_secs(60))
        );
        assert_eq!(snapshot.regular[0].ephemeral_delete_after, None);
        Ok(())
    }

    #[tokio::test]
    async fn queue_sticker_message_inserts_virtual_id_and_enqueues_immediate_like_go()
    -> Result<(), Box<dyn Error>> {
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
            &queue,
            QueueStickerRequest {
                message: &request,
                reply_to: None,
                immediate: true,
                bypass_chat_restrictions: false,
                ephemeral_delete_after: None,
            },
            || "sticker-v1".to_owned(),
        )
        .await?;

        assert_eq!(report.virtual_id, "sticker-v1");
        assert_eq!(report.enqueue_outcome, EnqueueOutcome::Enqueued);
        assert!(report.immediate);
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
        let (_, method, _, _) = item.into_persistence_parts();
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
    async fn send_work_item_records_mapping_from_successful_message_response()
    -> Result<(), Box<dyn Error>> {
        let item = queued_text_item("v1");

        let report = send_work_item(item, |_| async {
            Ok::<_, StubError>(TelegramOutboundResponse::Message(Box::new(
                telegram_message(42, 77),
            )))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Sent);
        assert_eq!(report.virtual_id, "v1");
        assert_eq!(report.sent_message_id, Some(77));

        Ok(())
    }

    #[tokio::test]
    async fn send_work_item_tracks_ephemeral_after_successful_first_message()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let item = queued_ephemeral_text_item("v1", Duration::from_secs(60));

        let report = send_work_item_with_ephemeral(&store, item, now, |_| async {
            Ok::<_, StubError>(TelegramOutboundResponse::Message(Box::new(
                telegram_message(42, 77),
            )))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Sent);
        assert_eq!(report.sent_message_id, Some(77));
        assert_eq!(report.ephemeral_track_error, None);
        store.snapshot(|state| {
            assert_eq!(
                state.ephemeral_tracked,
                vec![(42, 77, Duration::from_secs(60), now)]
            );
        });

        Ok(())
    }

    #[tokio::test]
    async fn send_work_item_does_not_track_ephemeral_after_send_failure()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let item = queued_ephemeral_text_item("v1", Duration::from_secs(60));

        let report = send_work_item_with_ephemeral(&store, item, now, |_| async {
            Err::<TelegramOutboundResponse, _>(StubError("telegram failed"))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Failed);
        store.snapshot(|state| {
            assert!(state.ephemeral_tracked.is_empty());
        });

        Ok(())
    }

    #[tokio::test]
    async fn ephemeral_cleanup_deletes_expired_messages_in_go_batches_and_removes_store_records()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let mut messages = (1..=11)
            .map(|message_id| EphemeralMessage {
                chat_id: -10042,
                message_id,
                expires_at: now - time::Duration::seconds(1),
            })
            .collect::<Vec<_>>();
        messages.push(EphemeralMessage {
            chat_id: -10042,
            message_id: 12,
            expires_at: now,
        });
        messages.push(EphemeralMessage {
            chat_id: -10042,
            message_id: 13,
            expires_at: now + time::Duration::seconds(1),
        });
        let store = StoreStub::with_state(StoreState {
            ephemeral_messages: messages,
            ..StoreState::default()
        });
        let mut delete_payloads = Vec::new();

        let report = super::process_ephemeral_cleanup_once_at(&store, now, |method| {
            let (kind, payload) = method_payload(method);
            assert_eq!(kind, TelegramOutboundMethodKind::DeleteMessage);
            delete_payloads.push((payload["chat_id"].clone(), payload["message_id"].clone()));
            async { Ok::<_, StubError>(()) }
        })
        .await;

        assert_eq!(report.loaded, 13);
        assert_eq!(report.expired, 11);
        assert_eq!(report.telegram_delete_attempted, 11);
        assert_eq!(report.telegram_delete_failed, 0);
        assert_eq!(report.store_deleted, 11);
        assert_eq!(report.store_delete_failed_batches, 0);
        assert_eq!(report.list_error, None);
        assert_eq!(delete_payloads.len(), 11);
        assert_eq!(delete_payloads[0], (json!(-10042), json!(1)));
        assert_eq!(delete_payloads[10], (json!(-10042), json!(11)));
        store.snapshot(|state| {
            assert_eq!(state.ephemeral_deleted.len(), 2);
            assert_eq!(state.ephemeral_deleted[0].len(), 10);
            assert_eq!(state.ephemeral_deleted[1].len(), 1);
        });

        Ok(())
    }

    #[tokio::test]
    async fn ephemeral_cleanup_removes_store_records_even_when_telegram_delete_fails()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_710_000_000)?;
        let message = EphemeralMessage {
            chat_id: 42,
            message_id: 77,
            expires_at: now - time::Duration::seconds(1),
        };
        let store = StoreStub::with_state(StoreState {
            ephemeral_messages: vec![message.clone()],
            ..StoreState::default()
        });

        let report = super::process_ephemeral_cleanup_once_at(&store, now, |_| async {
            Err::<(), _>(StubError("telegram delete failed"))
        })
        .await;

        assert_eq!(report.loaded, 1);
        assert_eq!(report.expired, 1);
        assert_eq!(report.telegram_delete_attempted, 1);
        assert_eq!(report.telegram_delete_failed, 1);
        assert_eq!(
            report.telegram_delete_error,
            Some("telegram delete failed".to_owned())
        );
        assert_eq!(report.store_deleted, 1);
        assert_eq!(report.store_delete_failed_batches, 0);
        store.snapshot(|state| {
            assert_eq!(state.ephemeral_deleted, vec![vec![message]]);
        });

        Ok(())
    }

    #[tokio::test]
    async fn ephemeral_cleanup_worker_ticks_until_stop_and_accumulates_report()
    -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::now_utc();
        let store = StoreStub::with_state(StoreState {
            ephemeral_messages: vec![EphemeralMessage {
                chat_id: 42,
                message_id: 77,
                expires_at: now - time::Duration::seconds(1),
            }],
            ..StoreState::default()
        });
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let mut stop_tx = Some(stop_tx);

        let report = super::run_ephemeral_cleanup_worker_every_until(
            &store,
            |_| {
                if let Some(stop_tx) = stop_tx.take() {
                    let _ = stop_tx.send(());
                }
                async { Ok::<_, StubError>(()) }
            },
            Duration::from_millis(5),
            async {
                let _ = stop_rx.await;
            },
        )
        .await;

        assert_eq!(report.ticks, 1);
        assert_eq!(report.loaded, 1);
        assert_eq!(report.expired, 1);
        assert_eq!(report.telegram_delete_attempted, 1);
        assert_eq!(report.store_deleted, 1);
        Ok(())
    }

    #[tokio::test]
    async fn send_work_item_does_not_resolve_after_send_failure() {
        let item = queued_text_item("v1");

        let report = send_work_item(item, |_| async {
            Err::<TelegramOutboundResponse, _>(StubError("telegram failed"))
        })
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Failed);
        assert_eq!(report.send_error, Some("telegram failed".to_owned()));
        assert_eq!(report.sent_message_id, None);
    }

    #[tokio::test]
    async fn send_work_item_with_history_updates_direct_edit_text_after_success()
    -> Result<(), Box<dyn Error>> {
        let store = StoreStub::default();
        let history = store.history();
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
        queue_edit_text_message(
            &queue,
            QueueEditTextRequest {
                message: &request,
                immediate: false,
                bypass_chat_restrictions: false,
            },
        )?;
        let item = queue.dequeue_regular().expect("queued edit item");

        let report = send_work_item_with_history_and_ephemeral(
            &history,
            &super::NoopEphemeralMessageTracker,
            item,
            OffsetDateTime::now_utc(),
            |_| async {
                Ok::<_, StubError>(TelegramOutboundResponse::EditMessage(
                    EditMessageResult::Message(Box::new(telegram_message(42, 99))),
                ))
            },
        )
        .await;

        assert_eq!(report.status, DispatcherSendStatus::Sent);
        assert_eq!(report.sent_message_id, None);
        store.snapshot(|state| {
            assert_eq!(
                state.events,
                vec!["history:update:42:99:<b>updated</b>:HTML"]
            );
        });

        Ok(())
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

    fn queued_ephemeral_text_item(
        virtual_id: &str,
        delete_after: Duration,
    ) -> openplotva_telegram::DispatcherWorkItem {
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
                    .with_method(TelegramOutboundMethod::from(method))
                    .with_ephemeral_delete_after(delete_after),
                true,
            ),
            EnqueueOutcome::Enqueued
        );
        queue.dequeue_immediate().expect("queued work item")
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
