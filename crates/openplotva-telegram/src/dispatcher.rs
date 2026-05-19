use std::{
    collections::VecDeque,
    future::Future,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{ChatLimiters, Debouncer, DebouncerConfig, MessageFingerprint, TelegramOutboundMethod};

/// Go outbound dispatcher queue settings currently ported to Rust.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DispatcherConfig {
    /// Maximum items retained in each queue. `0` means unbounded, matching Go.
    pub max_queue_size: usize,
    /// Regular-queue duplicate suppression settings.
    pub dedupe_config: DebouncerConfig,
}

/// Outbound message metadata needed by the queue layer.
#[derive(Debug)]
pub struct DispatcherMessage {
    /// Fingerprint used for regular-queue deduplication.
    pub fingerprint: MessageFingerprint,
    /// Virtual message ID used for cancellation and future real-ID mapping.
    pub virtual_id: String,
    method: Option<TelegramOutboundMethod>,
}

impl DispatcherMessage {
    /// Build queue metadata for an outbound message.
    pub fn new(fingerprint: MessageFingerprint, virtual_id: impl Into<String>) -> Self {
        Self {
            fingerprint,
            virtual_id: virtual_id.into(),
            method: None,
        }
    }

    /// Attach the concrete Telegram method that the worker should send.
    pub fn with_method(mut self, method: TelegramOutboundMethod) -> Self {
        self.method = Some(method);
        self
    }
}

/// Result of trying to enqueue a message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    /// Message entered the target queue.
    Enqueued,
    /// Regular message was suppressed by the debouncer.
    Deduped,
}

/// Result of taking a regular queue item for worker processing.
#[derive(Debug)]
pub enum RegularDequeueOutcome {
    /// The regular queue was empty.
    Empty,
    /// The item is ready to send.
    Ready(DispatcherWorkItem),
    /// The item was requeued at the front because the chat limiter is not ready.
    RateLimited { retry_after: Duration },
}

/// Result returned by an outbound transport after trying to send a queued item.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatcherSendStatus {
    /// Transport accepted the message and Go would increment processed stats.
    Sent,
    /// Transport returned an error and Go would leave processed stats unchanged.
    Failed,
}

impl DispatcherSendStatus {
    fn is_sent(self) -> bool {
        matches!(self, Self::Sent)
    }
}

/// Result of one async dispatcher worker step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DispatcherWorkerOutcome {
    /// No item was available in the worker's queue.
    Empty,
    /// Regular worker deferred the front item until the per-chat limiter is ready.
    RateLimited { retry_after: Duration },
    /// The transport reported success for a queued item.
    Sent { virtual_id: String, immediate: bool },
    /// The transport reported failure for a queued item.
    SendFailed { virtual_id: String, immediate: bool },
}

/// Inspectable queue item matching the Go dispatcher's persisted item metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherQueuedMessage {
    /// Telegram chat ID used by the regular dispatch rate limiter.
    pub chat_id: i64,
    /// The dedupe key string captured when the item was enqueued.
    pub fingerprint_key: String,
    /// Virtual message ID used for cancellation and future real-ID mapping.
    pub virtual_id: String,
    /// Whether the item belongs to the immediate queue.
    pub immediate: bool,
    /// Enqueue time used for oldest-age statistics.
    pub added_at: Instant,
}

/// Worker-owned queue item containing cloneable metadata plus the send payload.
#[derive(Debug)]
pub struct DispatcherWorkItem {
    metadata: DispatcherQueuedMessage,
    method: Option<TelegramOutboundMethod>,
}

impl DispatcherWorkItem {
    pub fn metadata(&self) -> &DispatcherQueuedMessage {
        &self.metadata
    }

    /// Consume the worker item and return only the concrete Telegram method payload.
    pub fn into_method(self) -> Option<TelegramOutboundMethod> {
        self.method
    }

    /// Consume the worker item and return both metadata and the concrete payload.
    pub fn into_parts(self) -> (DispatcherQueuedMessage, Option<TelegramOutboundMethod>) {
        (self.metadata, self.method)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueSnapshot {
    /// Regular outbound queue, oldest first.
    pub regular: Vec<DispatcherQueuedMessage>,
    /// Immediate outbound queue, oldest first.
    pub immediate: Vec<DispatcherQueuedMessage>,
}

/// Go dispatcher statistics ported for the queue layer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DispatcherStats {
    /// Number of queued regular messages.
    pub regular_queue_size: usize,
    /// Number of queued immediate messages.
    pub immediate_queue_size: usize,
    /// Number of successfully processed messages.
    pub processed_total: i64,
    /// Go's combined dispatcher and debouncer dedupe count.
    pub deduped_total: i64,
    /// Age of the oldest regular queue item.
    pub oldest_regular_age: Duration,
    /// Age of the oldest immediate queue item.
    pub oldest_immediate_age: Duration,
}

#[derive(Debug)]
pub struct DispatcherQueue {
    config: DispatcherConfig,
    debouncer: Debouncer,
    state: Mutex<DispatcherQueueState>,
}

#[derive(Debug, Default)]
struct DispatcherQueueState {
    regular: VecDeque<DispatcherQueueItem>,
    immediate: VecDeque<DispatcherQueueItem>,
    processed_total: i64,
    deduped_total: i64,
}

#[derive(Debug)]
struct DispatcherQueueItem {
    metadata: DispatcherQueuedMessage,
    method: Option<TelegramOutboundMethod>,
}

impl DispatcherQueueItem {
    fn into_work_item(self) -> DispatcherWorkItem {
        DispatcherWorkItem {
            metadata: self.metadata,
            method: self.method,
        }
    }
}

impl DispatcherQueue {
    /// Build an empty dispatcher queue.
    pub fn new(config: DispatcherConfig) -> Self {
        let debouncer = Debouncer::new(config.dedupe_config.clone());
        Self {
            config,
            debouncer,
            state: Mutex::new(DispatcherQueueState::default()),
        }
    }

    /// Enqueue an outbound message in the regular or immediate queue.
    pub fn enqueue(&self, message: DispatcherMessage, immediate: bool) -> EnqueueOutcome {
        self.enqueue_at_inner(message, immediate, Instant::now())
    }

    /// Remove queued messages by virtual ID from both queues.
    pub fn cancel(&self, virtual_id: &str) -> usize {
        if virtual_id.is_empty() {
            return 0;
        }

        let mut state = self.state();
        let regular_before = state.regular.len();
        state
            .regular
            .retain(|item| item.metadata.virtual_id != virtual_id);
        let immediate_before = state.immediate.len();
        state
            .immediate
            .retain(|item| item.metadata.virtual_id != virtual_id);

        (regular_before - state.regular.len()) + (immediate_before - state.immediate.len())
    }

    /// Remove and return the oldest immediate item.
    pub fn dequeue_immediate(&self) -> Option<DispatcherWorkItem> {
        self.state()
            .immediate
            .pop_front()
            .map(DispatcherQueueItem::into_work_item)
    }

    /// Remove and return the oldest regular item.
    pub fn dequeue_regular(&self) -> Option<DispatcherWorkItem> {
        self.state()
            .regular
            .pop_front()
            .map(DispatcherQueueItem::into_work_item)
    }

    /// Take the oldest regular item if its chat limiter is ready.
    pub fn dequeue_regular_with_limiter(&self, limiters: &ChatLimiters) -> RegularDequeueOutcome {
        self.dequeue_regular_with_limiter_at(limiters, Instant::now())
    }

    /// Process one immediate queue item with an async transport callback.
    pub async fn process_immediate_once<F, Fut>(&self, send: F) -> DispatcherWorkerOutcome
    where
        F: FnOnce(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        let Some(item) = self.dequeue_immediate() else {
            return DispatcherWorkerOutcome::Empty;
        };
        self.process_ready_item(item, send).await
    }

    /// Process one regular queue item with limiter checks and an async transport callback.
    pub async fn process_regular_once<F, Fut>(
        &self,
        limiters: &ChatLimiters,
        send: F,
    ) -> DispatcherWorkerOutcome
    where
        F: FnOnce(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        self.process_regular_once_at(limiters, Instant::now(), send)
            .await
    }

    /// Put a deferred regular item back at the front, matching Go wait-error handling.
    pub fn requeue_regular_front(&self, message: DispatcherWorkItem) {
        self.state().regular.push_front(DispatcherQueueItem {
            metadata: message.metadata,
            method: message.method,
        });
    }

    /// Record one successfully sent queue item.
    pub fn record_processed(&self) {
        let mut state = self.state();
        state.processed_total += 1;
    }

    /// Record a send attempt, incrementing processed stats only on success like Go.
    pub fn record_send_result(&self, sent: bool) {
        if sent {
            self.record_processed();
        }
    }

    /// Return current queue statistics.
    pub fn stats(&self) -> DispatcherStats {
        self.stats_at_inner(Instant::now())
    }

    /// Return a clone of queued metadata, oldest first in each queue.
    pub fn snapshot(&self) -> QueueSnapshot {
        let state = self.state();
        QueueSnapshot {
            regular: state
                .regular
                .iter()
                .map(|item| item.metadata.clone())
                .collect(),
            immediate: state
                .immediate
                .iter()
                .map(|item| item.metadata.clone())
                .collect(),
        }
    }

    #[cfg(test)]
    fn enqueue_at(
        &self,
        message: DispatcherMessage,
        immediate: bool,
        now: Instant,
    ) -> EnqueueOutcome {
        self.enqueue_at_inner(message, immediate, now)
    }

    fn enqueue_at_inner(
        &self,
        message: DispatcherMessage,
        immediate: bool,
        now: Instant,
    ) -> EnqueueOutcome {
        let DispatcherMessage {
            fingerprint,
            virtual_id,
            method,
        } = message;

        if !immediate && !self.debouncer.should_process_at(&fingerprint, now) {
            self.state().deduped_total += 1;
            return EnqueueOutcome::Deduped;
        }

        let queued = DispatcherQueueItem {
            metadata: DispatcherQueuedMessage {
                chat_id: fingerprint.chat_id,
                fingerprint_key: fingerprint.to_string(),
                virtual_id,
                immediate,
                added_at: now,
            },
            method,
        };

        {
            let mut state = self.state();
            if immediate {
                state.immediate.push_back(queued);
                trim_queue(&mut state.immediate, self.config.max_queue_size);
            } else {
                state.regular.push_back(queued);
                trim_queue(&mut state.regular, self.config.max_queue_size);
            }
        }

        if !immediate {
            self.debouncer.record_sent_at(&fingerprint, now);
        }
        EnqueueOutcome::Enqueued
    }

    pub(crate) fn dequeue_regular_with_limiter_at(
        &self,
        limiters: &ChatLimiters,
        now: Instant,
    ) -> RegularDequeueOutcome {
        let Some(item) = self.dequeue_regular() else {
            return RegularDequeueOutcome::Empty;
        };
        let chat_id = item.metadata().chat_id;
        if chat_id != 0 && !limiters.allow_at(chat_id, now) {
            let retry_after = limiters.retry_after_at(chat_id, now);
            self.requeue_regular_front(item);
            return RegularDequeueOutcome::RateLimited { retry_after };
        }
        RegularDequeueOutcome::Ready(item)
    }

    pub(crate) async fn process_regular_once_at<F, Fut>(
        &self,
        limiters: &ChatLimiters,
        now: Instant,
        send: F,
    ) -> DispatcherWorkerOutcome
    where
        F: FnOnce(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        match self.dequeue_regular_with_limiter_at(limiters, now) {
            RegularDequeueOutcome::Empty => DispatcherWorkerOutcome::Empty,
            RegularDequeueOutcome::RateLimited { retry_after } => {
                DispatcherWorkerOutcome::RateLimited { retry_after }
            }
            RegularDequeueOutcome::Ready(item) => self.process_ready_item(item, send).await,
        }
    }

    async fn process_ready_item<F, Fut>(
        &self,
        item: DispatcherWorkItem,
        send: F,
    ) -> DispatcherWorkerOutcome
    where
        F: FnOnce(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        let virtual_id = item.metadata().virtual_id.clone();
        let immediate = item.metadata().immediate;
        let status = send(item).await;
        self.record_send_result(status.is_sent());
        if status.is_sent() {
            DispatcherWorkerOutcome::Sent {
                virtual_id,
                immediate,
            }
        } else {
            DispatcherWorkerOutcome::SendFailed {
                virtual_id,
                immediate,
            }
        }
    }

    #[cfg(test)]
    fn stats_at(&self, now: Instant) -> DispatcherStats {
        self.stats_at_inner(now)
    }

    fn stats_at_inner(&self, now: Instant) -> DispatcherStats {
        let state = self.state();
        DispatcherStats {
            regular_queue_size: state.regular.len(),
            immediate_queue_size: state.immediate.len(),
            processed_total: state.processed_total,
            deduped_total: state.deduped_total + self.debouncer.deduped_count(),
            oldest_regular_age: oldest_age(&state.regular, now),
            oldest_immediate_age: oldest_age(&state.immediate, now),
        }
    }

    fn state(&self) -> MutexGuard<'_, DispatcherQueueState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn trim_queue(queue: &mut VecDeque<DispatcherQueueItem>, max_queue_size: usize) {
    if max_queue_size == 0 {
        return;
    }
    while queue.len() > max_queue_size {
        queue.pop_front();
    }
}

fn oldest_age(queue: &VecDeque<DispatcherQueueItem>, now: Instant) -> Duration {
    queue
        .front()
        .map(|item| now.saturating_duration_since(item.metadata.added_at))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        DispatcherConfig, DispatcherMessage, DispatcherQueue, DispatcherSendStatus,
        DispatcherWorkerOutcome, EnqueueOutcome, QueueSnapshot, RegularDequeueOutcome,
    };
    use crate::{
        ChatLimiters, DebouncerConfig, MESSAGE_TYPE_TEXT, MessageFingerprint,
        TelegramOutboundMethod, TelegramOutboundMethodKind, hash_content,
    };

    fn text_message(chat_id: i64, text: &str, virtual_id: &str) -> DispatcherMessage {
        DispatcherMessage::new(
            MessageFingerprint {
                chat_id,
                message_type: MESSAGE_TYPE_TEXT.to_owned(),
                content_hash: hash_content(text),
                debounce_key: None,
            },
            virtual_id,
        )
    }

    fn text_method(chat_id: i64, text: &str) -> TelegramOutboundMethod {
        TelegramOutboundMethod::from(carapax::types::SendMessage::new(chat_id, text))
    }

    fn virtual_ids(snapshot: QueueSnapshot) -> (Vec<String>, Vec<String>) {
        (
            snapshot
                .regular
                .into_iter()
                .map(|item| item.virtual_id)
                .collect(),
            snapshot
                .immediate
                .into_iter()
                .map(|item| item.virtual_id)
                .collect(),
        )
    }

    #[test]
    fn snapshot_remains_metadata_only_when_queue_carries_method_payloads() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.enqueue_at(
            text_message(42, "regular", "regular-1").with_method(text_method(42, "payload body")),
            false,
            now,
        );

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "regular-1");
        assert_eq!(snapshot.regular[0].chat_id, 42);

        let item = queue.dequeue_regular().expect("regular item");
        assert_eq!(item.metadata().virtual_id, "regular-1");
        assert_eq!(
            item.into_method().map(|method| method.kind()),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
    }

    #[tokio::test]
    async fn worker_receives_owned_method_payload_once() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();
        let seen = Arc::new(Mutex::new(Vec::new()));

        queue.enqueue_at(
            text_message(42, "immediate", "immediate-1")
                .with_method(text_method(42, "payload body")),
            true,
            now,
        );

        let outcome = queue
            .process_immediate_once({
                let seen = Arc::clone(&seen);
                move |item| async move {
                    let virtual_id = item.metadata().virtual_id.clone();
                    let kind = item.into_method().map(|method| method.kind());
                    seen.lock().expect("seen lock").push((virtual_id, kind));
                    DispatcherSendStatus::Sent
                }
            })
            .await;

        assert_eq!(
            outcome,
            DispatcherWorkerOutcome::Sent {
                virtual_id: "immediate-1".to_owned(),
                immediate: true,
            }
        );
        assert_eq!(
            *seen.lock().expect("seen lock"),
            vec![(
                "immediate-1".to_owned(),
                Some(TelegramOutboundMethodKind::SendMessage)
            )]
        );
        assert!(queue.dequeue_immediate().is_none());
    }

    #[test]
    fn dequeue_immediate_and_regular_remove_oldest_items() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "regular", "regular-1"), false, now);
        queue.enqueue_at(text_message(42, "immediate", "immediate-1"), true, now);

        let immediate = queue.dequeue_immediate().expect("immediate item");
        assert_eq!(immediate.metadata().virtual_id, "immediate-1");
        assert_eq!(immediate.metadata().chat_id, 42);

        let regular = queue.dequeue_regular().expect("regular item");
        assert_eq!(regular.metadata().virtual_id, "regular-1");
        assert_eq!(regular.metadata().chat_id, 42);

        assert!(queue.dequeue_immediate().is_none());
        assert!(queue.dequeue_regular().is_none());
        assert_eq!(queue.stats_at(now).regular_queue_size, 0);
        assert_eq!(queue.stats_at(now).immediate_queue_size, 0);
    }

    #[test]
    fn requeue_regular_front_preserves_go_wait_error_order() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "first", "regular-1"), false, now);
        queue.enqueue_at(
            text_message(42, "second", "regular-2"),
            false,
            now + Duration::from_secs(1),
        );

        let first = queue.dequeue_regular().expect("regular item");
        queue.requeue_regular_front(first);

        assert_eq!(
            virtual_ids(queue.snapshot()).0,
            vec!["regular-1".to_owned(), "regular-2".to_owned()]
        );
    }

    #[test]
    fn regular_dequeue_with_limiter_requeues_front_until_chat_is_ready() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let limiters = ChatLimiters::new(crate::DEFAULT_DISPATCH_INTERVAL);
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "first", "regular-1"), false, now);
        queue.enqueue_at(
            text_message(42, "second", "regular-2"),
            false,
            now + Duration::from_secs(1),
        );

        assert!(matches!(
            queue.dequeue_regular_with_limiter_at(&limiters, now),
            RegularDequeueOutcome::Ready(item) if item.metadata().virtual_id == "regular-1"
        ));
        assert!(matches!(
            queue.dequeue_regular_with_limiter_at(&limiters, now),
            RegularDequeueOutcome::RateLimited { retry_after }
                if retry_after == crate::DEFAULT_DISPATCH_INTERVAL
        ));
        assert_eq!(
            virtual_ids(queue.snapshot()).0,
            vec!["regular-2".to_owned()]
        );
        assert!(matches!(
            queue.dequeue_regular_with_limiter_at(&limiters, now + crate::DEFAULT_DISPATCH_INTERVAL),
            RegularDequeueOutcome::Ready(item) if item.metadata().virtual_id == "regular-2"
        ));
    }

    #[test]
    fn send_result_stats_match_go_success_only_dequeued_count() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.record_send_result(false);
        assert_eq!(queue.stats_at(now).processed_total, 0);

        queue.record_send_result(true);
        assert_eq!(queue.stats_at(now).processed_total, 1);
    }

    #[tokio::test]
    async fn immediate_worker_awaits_sender_and_records_success() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();
        let seen = Arc::new(Mutex::new(Vec::new()));

        queue.enqueue_at(text_message(42, "immediate", "immediate-1"), true, now);

        let outcome = queue
            .process_immediate_once({
                let seen = Arc::clone(&seen);
                move |item| async move {
                    seen.lock()
                        .expect("seen lock")
                        .push(item.metadata().virtual_id.clone());
                    DispatcherSendStatus::Sent
                }
            })
            .await;

        assert_eq!(
            outcome,
            DispatcherWorkerOutcome::Sent {
                virtual_id: "immediate-1".to_owned(),
                immediate: true,
            }
        );
        assert_eq!(
            *seen.lock().expect("seen lock"),
            vec!["immediate-1".to_owned()]
        );
        assert_eq!(queue.stats_at(now).processed_total, 1);
        assert_eq!(queue.stats_at(now).immediate_queue_size, 0);
    }

    #[tokio::test]
    async fn regular_worker_requeues_rate_limited_item_without_calling_sender() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let limiters = ChatLimiters::new(crate::DEFAULT_DISPATCH_INTERVAL);
        let now = std::time::Instant::now();
        let sends = Arc::new(Mutex::new(0usize));

        queue.enqueue_at(text_message(42, "first", "regular-1"), false, now);
        queue.enqueue_at(
            text_message(42, "second", "regular-2"),
            false,
            now + Duration::from_secs(1),
        );

        let sent = queue
            .process_regular_once_at(&limiters, now, {
                let sends = Arc::clone(&sends);
                move |_| async move {
                    *sends.lock().expect("send count lock") += 1;
                    DispatcherSendStatus::Sent
                }
            })
            .await;
        assert!(matches!(
            sent,
            DispatcherWorkerOutcome::Sent {
                virtual_id,
                immediate: false
            } if virtual_id == "regular-1"
        ));

        let limited = queue
            .process_regular_once_at(&limiters, now, {
                let sends = Arc::clone(&sends);
                move |_| async move {
                    *sends.lock().expect("send count lock") += 1;
                    DispatcherSendStatus::Sent
                }
            })
            .await;

        assert_eq!(
            limited,
            DispatcherWorkerOutcome::RateLimited {
                retry_after: crate::DEFAULT_DISPATCH_INTERVAL,
            }
        );
        assert_eq!(*sends.lock().expect("send count lock"), 1);
        assert_eq!(
            virtual_ids(queue.snapshot()).0,
            vec!["regular-2".to_owned()]
        );
        assert_eq!(queue.stats_at(now).processed_total, 1);
    }

    #[tokio::test]
    async fn worker_send_failure_does_not_increment_processed_total() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "immediate", "immediate-1"), true, now);

        let outcome = queue
            .process_immediate_once(|_| async { DispatcherSendStatus::Failed })
            .await;

        assert_eq!(
            outcome,
            DispatcherWorkerOutcome::SendFailed {
                virtual_id: "immediate-1".to_owned(),
                immediate: true,
            }
        );
        assert_eq!(queue.stats_at(now).processed_total, 0);
        assert_eq!(queue.stats_at(now).immediate_queue_size, 0);
    }

    #[test]
    fn dispatcher_stats_include_queue_sizes_and_oldest_ages() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        assert_eq!(
            queue.enqueue_at(text_message(42, "regular", "regular-1"), false, now),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            queue.enqueue_at(
                text_message(42, "immediate", "immediate-1"),
                true,
                now + Duration::from_secs(5),
            ),
            EnqueueOutcome::Enqueued
        );

        let stats = queue.stats_at(now + Duration::from_secs(12));

        assert_eq!(stats.regular_queue_size, 1);
        assert_eq!(stats.immediate_queue_size, 1);
        assert_eq!(stats.processed_total, 0);
        assert_eq!(stats.deduped_total, 0);
        assert!(stats.oldest_regular_age >= Duration::from_secs(12));
        assert!(stats.oldest_immediate_age >= Duration::from_secs(7));
    }

    #[test]
    fn regular_enqueue_dedupes_but_immediate_enqueue_bypasses_dedupe() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            max_queue_size: 0,
            dedupe_config: DebouncerConfig {
                enabled: true,
                default_window: Duration::from_secs(30),
                max_cache_size: 1000,
                per_chat_settings: HashMap::new(),
            },
        });
        let now = std::time::Instant::now();

        assert_eq!(
            queue.enqueue_at(text_message(42, "same", "regular-1"), false, now),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            queue.enqueue_at(
                text_message(42, "same", "regular-2"),
                false,
                now + Duration::from_secs(1),
            ),
            EnqueueOutcome::Deduped
        );
        assert_eq!(
            queue.enqueue_at(
                text_message(42, "same", "immediate-1"),
                true,
                now + Duration::from_secs(2),
            ),
            EnqueueOutcome::Enqueued
        );

        let stats = queue.stats_at(now + Duration::from_secs(3));
        assert_eq!(stats.regular_queue_size, 1);
        assert_eq!(stats.immediate_queue_size, 1);
        assert_eq!(stats.deduped_total, 2);
        assert_eq!(
            virtual_ids(queue.snapshot()),
            (vec!["regular-1".to_owned()], vec!["immediate-1".to_owned()])
        );
    }

    #[test]
    fn max_queue_size_trims_oldest_per_queue() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            max_queue_size: 1,
            dedupe_config: DebouncerConfig::default(),
        });
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "regular-1", "regular-1"), false, now);
        queue.enqueue_at(
            text_message(42, "regular-2", "regular-2"),
            false,
            now + Duration::from_secs(1),
        );
        queue.enqueue_at(text_message(42, "immediate-1", "immediate-1"), true, now);
        queue.enqueue_at(
            text_message(42, "immediate-2", "immediate-2"),
            true,
            now + Duration::from_secs(1),
        );

        assert_eq!(
            virtual_ids(queue.snapshot()),
            (vec!["regular-2".to_owned()], vec!["immediate-2".to_owned()])
        );
    }

    #[test]
    fn cancel_removes_matching_virtual_ids_from_both_queues() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "regular", "shared"), false, now);
        queue.enqueue_at(text_message(42, "immediate", "shared"), true, now);
        queue.enqueue_at(text_message(42, "kept", "kept"), false, now);

        assert_eq!(queue.cancel(""), 0);
        assert_eq!(queue.cancel("shared"), 2);
        assert_eq!(queue.cancel("shared"), 0);
        assert_eq!(
            virtual_ids(queue.snapshot()),
            (vec!["kept".to_owned()], vec![])
        );
    }
}
