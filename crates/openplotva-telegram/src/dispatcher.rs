use std::{
    collections::VecDeque,
    future::Future,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant, SystemTime},
};

use carapax::api::Client;
use tokio::{
    sync::Notify,
    time::{sleep, timeout},
};

use crate::rate_limit::DEFAULT_RATE_LIMITER_MAX_IDLE;
use crate::{
    ChatLimiters, Debouncer, DebouncerConfig, MessageFingerprint, RichApiClient,
    TelegramOutboundMethod, TelegramOutboundMethodKind, send_telegram_method_status,
    send_telegram_method_status_with_rich,
};

pub const DEFAULT_DISPATCHER_CLEANUP_INTERVAL: Duration = Duration::from_secs(10 * 60);
pub const DEFAULT_DISPATCHER_SEND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DispatcherConfig {
    pub max_queue_size: usize,
    /// Regular-queue duplicate suppression settings.
    pub dedupe_config: DebouncerConfig,
    /// Maximum time a worker may spend on one outbound item.
    pub send_timeout: Duration,
}

impl DispatcherConfig {
    fn send_timeout(&self) -> Duration {
        if self.send_timeout.is_zero() {
            DEFAULT_DISPATCHER_SEND_TIMEOUT
        } else {
            self.send_timeout
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatcherRuntimeConfig {
    /// Interval between idle per-chat limiter cleanup passes.
    pub cleanup_interval: Duration,
    /// Idle age after which a per-chat limiter can be removed.
    pub rate_limiter_max_idle: Duration,
}

impl Default for DispatcherRuntimeConfig {
    fn default() -> Self {
        Self {
            cleanup_interval: DEFAULT_DISPATCHER_CLEANUP_INTERVAL,
            rate_limiter_max_idle: DEFAULT_RATE_LIMITER_MAX_IDLE,
        }
    }
}

impl DispatcherRuntimeConfig {
    pub fn cleanup_interval(self) -> Duration {
        if self.cleanup_interval.is_zero() {
            DEFAULT_DISPATCHER_CLEANUP_INTERVAL
        } else {
            self.cleanup_interval
        }
    }

    pub fn rate_limiter_max_idle(self) -> Duration {
        if self.rate_limiter_max_idle.is_zero() {
            DEFAULT_RATE_LIMITER_MAX_IDLE
        } else {
            self.rate_limiter_max_idle
        }
    }
}

/// Outbound message metadata needed by the queue layer.
#[derive(Debug)]
pub struct DispatcherMessage {
    /// Fingerprint used for regular-queue deduplication.
    pub fingerprint: MessageFingerprint,
    /// Virtual message ID used for cancellation and future real-ID mapping.
    pub virtual_id: String,
    method: Option<TelegramOutboundMethod>,
    persistence_payload: Option<DispatcherPersistencePayload>,
    bypass_chat_restrictions: bool,
    ephemeral_delete_after: Option<Duration>,
}

impl DispatcherMessage {
    /// Build queue metadata for an outbound message.
    pub fn new(fingerprint: MessageFingerprint, virtual_id: impl Into<String>) -> Self {
        Self {
            fingerprint,
            virtual_id: virtual_id.into(),
            method: None,
            persistence_payload: None,
            bypass_chat_restrictions: false,
            ephemeral_delete_after: None,
        }
    }

    /// Attach the concrete Telegram method that the worker should send.
    pub fn with_method(mut self, method: TelegramOutboundMethod) -> Self {
        self.method = Some(method);
        self
    }

    pub fn with_persistence_payload(mut self, payload: DispatcherPersistencePayload) -> Self {
        self.persistence_payload = Some(payload);
        self
    }

    pub fn with_bypass_chat_restrictions(mut self, bypass: bool) -> Self {
        self.bypass_chat_restrictions = bypass;
        self
    }

    pub fn with_ephemeral_delete_after(mut self, duration: Duration) -> Self {
        self.ephemeral_delete_after = Some(duration);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherPersistencePayload {
    pub message_type: String,
    /// JSON-encoded Telegram message/method payload.
    pub message: Vec<u8>,
}

impl DispatcherPersistencePayload {
    /// Build a persistence payload from raw JSON bytes.
    pub fn new(message_type: impl Into<String>, message: impl Into<Vec<u8>>) -> Self {
        Self {
            message_type: message_type.into(),
            message: message.into(),
        }
    }

    /// Build a persistence payload from a JSON value.
    pub fn from_json_value(
        message_type: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::new(message_type, serde_json::to_vec(&value)?))
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
    Sent,
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

/// Result returned when a long-running dispatcher worker loop exits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatcherWorkerLoopOutcome {
    /// The caller's stop future completed.
    Stopped,
}

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
    pub enqueued_at: SystemTime,
    pub ephemeral_delete_after: Option<Duration>,
}

/// Worker-owned queue item containing cloneable metadata plus the send payload.
#[derive(Debug)]
pub struct DispatcherWorkItem {
    metadata: DispatcherQueuedMessage,
    method: Option<TelegramOutboundMethod>,
    persistence_payload: Option<DispatcherPersistencePayload>,
    bypass_chat_restrictions: bool,
    ephemeral_delete_after: Option<Duration>,
}

impl DispatcherWorkItem {
    pub fn metadata(&self) -> &DispatcherQueuedMessage {
        &self.metadata
    }

    /// Return the Telegram method kind without consuming the queued payload.
    pub fn method_kind(&self) -> Option<TelegramOutboundMethodKind> {
        self.method.as_ref().map(TelegramOutboundMethod::kind)
    }

    /// Return whether this queued item should skip chat permission settings.
    pub fn bypasses_chat_restrictions(&self) -> bool {
        self.bypass_chat_restrictions
    }

    pub fn ephemeral_delete_after(&self) -> Option<Duration> {
        self.ephemeral_delete_after
    }

    /// Consume the worker item and return only the concrete Telegram method payload.
    pub fn into_method(self) -> Option<TelegramOutboundMethod> {
        self.method
    }

    /// Consume the worker item and return both metadata and the concrete payload.
    pub fn into_parts(self) -> (DispatcherQueuedMessage, Option<TelegramOutboundMethod>) {
        (self.metadata, self.method)
    }

    /// Consume the worker item and return metadata, payload, and persistence payload.
    pub fn into_persistence_parts(
        self,
    ) -> (
        DispatcherQueuedMessage,
        Option<TelegramOutboundMethod>,
        Option<DispatcherPersistencePayload>,
        Option<Duration>,
    ) {
        (
            self.metadata,
            self.method,
            self.persistence_payload,
            self.ephemeral_delete_after,
        )
    }
}

#[derive(Debug)]
pub struct DispatcherRestoredMessage {
    pub fingerprint: MessageFingerprint,
    /// Persisted dedupe key string to keep future shutdown snapshots stable.
    pub fingerprint_key: String,
    /// Virtual message ID used for cancellation and future real-ID mapping.
    pub virtual_id: String,
    /// Whether the item belongs to the immediate queue.
    pub immediate: bool,
    /// Original enqueue wall-clock time from persistent storage.
    pub enqueued_at: SystemTime,
    /// Concrete Telegram method to replay.
    pub method: TelegramOutboundMethod,
    pub persistence_payload: Option<DispatcherPersistencePayload>,
    pub bypass_chat_restrictions: bool,
    pub ephemeral_delete_after: Option<Duration>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueueSnapshot {
    /// Regular outbound queue, oldest first.
    pub regular: Vec<DispatcherQueuedMessage>,
    /// Immediate outbound queue, oldest first.
    pub immediate: Vec<DispatcherQueuedMessage>,
}

#[derive(Debug, Default)]
pub struct DispatcherDrain {
    pub immediate: Vec<DispatcherWorkItem>,
    /// Regular queue items, oldest first.
    pub regular: Vec<DispatcherWorkItem>,
}

impl DispatcherDrain {
    /// Total number of drained queue items.
    pub fn len(&self) -> usize {
        self.immediate.len() + self.regular.len()
    }

    /// Return whether no items were drained.
    pub fn is_empty(&self) -> bool {
        self.immediate.is_empty() && self.regular.is_empty()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DispatcherStats {
    /// Number of queued regular messages.
    pub regular_queue_size: usize,
    /// Number of queued immediate messages.
    pub immediate_queue_size: usize,
    /// Number of successfully processed messages.
    pub processed_total: i64,
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
    immediate_notify: Notify,
    regular_notify: Notify,
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
    persistence_payload: Option<DispatcherPersistencePayload>,
    bypass_chat_restrictions: bool,
    ephemeral_delete_after: Option<Duration>,
}

impl DispatcherQueueItem {
    fn into_work_item(self) -> DispatcherWorkItem {
        DispatcherWorkItem {
            metadata: self.metadata,
            method: self.method,
            persistence_payload: self.persistence_payload,
            bypass_chat_restrictions: self.bypass_chat_restrictions,
            ephemeral_delete_after: self.ephemeral_delete_after,
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
            immediate_notify: Notify::new(),
            regular_notify: Notify::new(),
        }
    }

    /// Enqueue an outbound message in the regular or immediate queue.
    pub fn enqueue(&self, message: DispatcherMessage, immediate: bool) -> EnqueueOutcome {
        self.enqueue_at_inner(message, immediate, Instant::now())
    }

    /// Restore a message loaded from persistent queue storage.
    pub fn restore(&self, message: DispatcherRestoredMessage) -> EnqueueOutcome {
        self.restore_at_inner(message, Instant::now(), SystemTime::now())
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

    /// Drain queued work for graceful shutdown persistence.
    pub fn drain_for_shutdown(&self) -> DispatcherDrain {
        let drained = {
            let mut state = self.state();
            DispatcherDrain {
                immediate: state
                    .immediate
                    .drain(..)
                    .map(DispatcherQueueItem::into_work_item)
                    .collect(),
                regular: state
                    .regular
                    .drain(..)
                    .map(DispatcherQueueItem::into_work_item)
                    .collect(),
            }
        };

        self.immediate_notify.notify_waiters();
        self.regular_notify.notify_waiters();
        drained
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

    /// Process one immediate item by sending its concrete Telegram method payload.
    pub async fn process_immediate_method_once<F, Fut>(
        &self,
        send_method: F,
    ) -> DispatcherWorkerOutcome
    where
        F: FnOnce(TelegramOutboundMethod) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        self.process_immediate_once(|item| send_work_item_method_status(item, send_method))
            .await
    }

    /// Process one immediate item through a real `carapax` client.
    pub async fn process_immediate_telegram_once(
        &self,
        client: &Client,
    ) -> DispatcherWorkerOutcome {
        self.process_immediate_method_once(|method| send_telegram_method_status(client, method))
            .await
    }

    /// Process one immediate item through Telegram and rich-message clients.
    pub async fn process_immediate_telegram_once_with_rich(
        &self,
        client: &Client,
        rich: &RichApiClient,
    ) -> DispatcherWorkerOutcome {
        self.process_immediate_method_once(|method| {
            send_telegram_method_status_with_rich(client, rich, method)
        })
        .await
    }

    /// Run the immediate worker loop until the provided stop future completes.
    pub async fn run_immediate_method_worker_until<F, Fut, Stop>(
        &self,
        stop: Stop,
        send_method: F,
    ) -> DispatcherWorkerLoopOutcome
    where
        F: Fn(TelegramOutboundMethod) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
        Stop: Future<Output = ()>,
    {
        self.run_immediate_worker_until(stop, |item| {
            send_work_item_method_status(item, &send_method)
        })
        .await
    }

    /// Run the immediate worker loop until the provided stop future completes.
    pub async fn run_immediate_worker_until<F, Fut, Stop>(
        &self,
        stop: Stop,
        send: F,
    ) -> DispatcherWorkerLoopOutcome
    where
        F: Fn(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
        Stop: Future<Output = ()>,
    {
        tokio::pin!(stop);

        loop {
            tokio::select! {
                () = &mut stop => return DispatcherWorkerLoopOutcome::Stopped,
                () = self.wait_for_immediate_item() => {}
            }

            loop {
                tokio::select! {
                    biased;
                    () = &mut stop => return DispatcherWorkerLoopOutcome::Stopped,
                    () = std::future::ready(()) => {}
                }

                let outcome = self.process_immediate_once(&send).await;
                if matches!(outcome, DispatcherWorkerOutcome::Empty) {
                    break;
                }
            }
        }
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

    /// Process one regular item by sending its concrete Telegram method payload.
    pub async fn process_regular_method_once<F, Fut>(
        &self,
        limiters: &ChatLimiters,
        send_method: F,
    ) -> DispatcherWorkerOutcome
    where
        F: FnOnce(TelegramOutboundMethod) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        self.process_regular_method_once_at(limiters, Instant::now(), send_method)
            .await
    }

    /// Process one regular item through a real `carapax` client.
    pub async fn process_regular_telegram_once(
        &self,
        limiters: &ChatLimiters,
        client: &Client,
    ) -> DispatcherWorkerOutcome {
        self.process_regular_method_once(limiters, |method| {
            send_telegram_method_status(client, method)
        })
        .await
    }

    /// Process one regular item through Telegram and rich-message clients.
    pub async fn process_regular_telegram_once_with_rich(
        &self,
        limiters: &ChatLimiters,
        client: &Client,
        rich: &RichApiClient,
    ) -> DispatcherWorkerOutcome {
        self.process_regular_method_once(limiters, |method| {
            send_telegram_method_status_with_rich(client, rich, method)
        })
        .await
    }

    /// Run the regular worker loop until the provided stop future completes.
    pub async fn run_regular_method_worker_until<F, Fut, Stop>(
        &self,
        limiters: &ChatLimiters,
        stop: Stop,
        send_method: F,
    ) -> DispatcherWorkerLoopOutcome
    where
        F: Fn(TelegramOutboundMethod) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
        Stop: Future<Output = ()>,
    {
        self.run_regular_worker_until(limiters, stop, |item| {
            send_work_item_method_status(item, &send_method)
        })
        .await
    }

    /// Run the regular worker loop until the provided stop future completes.
    pub async fn run_regular_worker_until<F, Fut, Stop>(
        &self,
        limiters: &ChatLimiters,
        stop: Stop,
        send: F,
    ) -> DispatcherWorkerLoopOutcome
    where
        F: Fn(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
        Stop: Future<Output = ()>,
    {
        tokio::pin!(stop);

        loop {
            tokio::select! {
                () = &mut stop => return DispatcherWorkerLoopOutcome::Stopped,
                () = self.wait_for_regular_item() => {}
            }

            loop {
                tokio::select! {
                    biased;
                    () = &mut stop => return DispatcherWorkerLoopOutcome::Stopped,
                    () = std::future::ready(()) => {}
                }

                match self.process_regular_once(limiters, &send).await {
                    DispatcherWorkerOutcome::Empty => break,
                    DispatcherWorkerOutcome::RateLimited { retry_after } => {
                        tokio::select! {
                            () = &mut stop => return DispatcherWorkerLoopOutcome::Stopped,
                            () = sleep(retry_after) => {}
                        }
                    }
                    DispatcherWorkerOutcome::Sent { .. }
                    | DispatcherWorkerOutcome::SendFailed { .. } => {}
                }
            }
        }
    }

    /// Run immediate and regular method workers together until `stop` completes.
    pub async fn run_method_workers_until<F, Fut, Stop>(
        &self,
        limiters: &ChatLimiters,
        stop: Stop,
        send_method: F,
    ) -> DispatcherWorkerLoopOutcome
    where
        F: Fn(TelegramOutboundMethod) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
        Stop: Future<Output = ()>,
    {
        self.run_workers_until(limiters, stop, |item| {
            send_work_item_method_status(item, &send_method)
        })
        .await
    }

    /// Run immediate and regular workers together until `stop` completes.
    pub async fn run_workers_until<F, Fut, Stop>(
        &self,
        limiters: &ChatLimiters,
        stop: Stop,
        send: F,
    ) -> DispatcherWorkerLoopOutcome
    where
        F: Fn(DispatcherWorkItem) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
        Stop: Future<Output = ()>,
    {
        tokio::select! {
            () = stop => DispatcherWorkerLoopOutcome::Stopped,
            _ = async {
                tokio::join!(
                    self.run_immediate_worker_until(std::future::pending::<()>(), &send),
                    self.run_regular_worker_until(limiters, std::future::pending::<()>(), &send),
                );
            } => DispatcherWorkerLoopOutcome::Stopped,
        }
    }

    /// Run both method workers against a real `carapax` client until `stop` completes.
    pub async fn run_telegram_workers_until<Stop>(
        &self,
        limiters: &ChatLimiters,
        client: &Client,
        stop: Stop,
    ) -> DispatcherWorkerLoopOutcome
    where
        Stop: Future<Output = ()>,
    {
        self.run_method_workers_until(limiters, stop, |method| {
            send_telegram_method_status(client, method)
        })
        .await
    }

    /// Run both method workers against Telegram and rich-message clients until `stop` completes.
    pub async fn run_telegram_workers_until_with_rich<Stop>(
        &self,
        limiters: &ChatLimiters,
        client: &Client,
        rich: &RichApiClient,
        stop: Stop,
    ) -> DispatcherWorkerLoopOutcome
    where
        Stop: Future<Output = ()>,
    {
        self.run_method_workers_until(limiters, stop, |method| {
            send_telegram_method_status_with_rich(client, rich, method)
        })
        .await
    }

    pub fn requeue_regular_front(&self, message: DispatcherWorkItem) {
        self.state().regular.push_front(DispatcherQueueItem {
            metadata: message.metadata,
            method: message.method,
            persistence_payload: message.persistence_payload,
            bypass_chat_restrictions: message.bypass_chat_restrictions,
            ephemeral_delete_after: message.ephemeral_delete_after,
        });
        self.regular_notify.notify_one();
    }

    /// Record one successfully sent queue item.
    pub fn record_processed(&self) {
        let mut state = self.state();
        state.processed_total += 1;
    }

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
            persistence_payload,
            bypass_chat_restrictions,
            ephemeral_delete_after,
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
                enqueued_at: SystemTime::now(),
                ephemeral_delete_after,
            },
            method,
            persistence_payload,
            bypass_chat_restrictions,
            ephemeral_delete_after,
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

        if immediate {
            self.immediate_notify.notify_one();
        } else {
            self.regular_notify.notify_one();
        }

        if !immediate {
            self.debouncer.record_sent_at(&fingerprint, now);
        }
        EnqueueOutcome::Enqueued
    }

    fn restore_at_inner(
        &self,
        message: DispatcherRestoredMessage,
        now: Instant,
        wall_now: SystemTime,
    ) -> EnqueueOutcome {
        let DispatcherRestoredMessage {
            fingerprint,
            fingerprint_key,
            virtual_id,
            immediate,
            enqueued_at,
            method,
            persistence_payload,
            bypass_chat_restrictions,
            ephemeral_delete_after,
        } = message;

        if !self.debouncer.should_process_at(&fingerprint, now) {
            return EnqueueOutcome::Deduped;
        }

        let queued = DispatcherQueueItem {
            metadata: DispatcherQueuedMessage {
                chat_id: fingerprint.chat_id,
                fingerprint_key: if fingerprint_key.is_empty() {
                    fingerprint.to_string()
                } else {
                    fingerprint_key
                },
                virtual_id,
                immediate,
                added_at: added_instant_for_enqueued_at(enqueued_at, now, wall_now),
                enqueued_at,
                ephemeral_delete_after,
            },
            method: Some(method),
            persistence_payload,
            bypass_chat_restrictions,
            ephemeral_delete_after,
        };

        self.push_queue_item(queued);

        EnqueueOutcome::Enqueued
    }

    fn push_queue_item(&self, queued: DispatcherQueueItem) {
        let immediate = queued.metadata.immediate;
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

        if immediate {
            self.immediate_notify.notify_one();
        } else {
            self.regular_notify.notify_one();
        }
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

    pub(crate) async fn process_regular_method_once_at<F, Fut>(
        &self,
        limiters: &ChatLimiters,
        now: Instant,
        send_method: F,
    ) -> DispatcherWorkerOutcome
    where
        F: FnOnce(TelegramOutboundMethod) -> Fut,
        Fut: Future<Output = DispatcherSendStatus>,
    {
        self.process_regular_once_at(limiters, now, |item| {
            send_work_item_method_status(item, send_method)
        })
        .await
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
        let chat_id = item.metadata().chat_id;
        let send_timeout = self.config.send_timeout();
        let status = match timeout(send_timeout, send(item)).await {
            Ok(status) => status,
            Err(_) => {
                tracing::warn!(
                    virtual_id = %virtual_id,
                    chat_id,
                    immediate,
                    timeout_ms = send_timeout.as_millis() as u64,
                    "dispatcher send timed out"
                );
                DispatcherSendStatus::Failed
            }
        };
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

    fn has_immediate_items(&self) -> bool {
        !self.state().immediate.is_empty()
    }

    fn has_regular_items(&self) -> bool {
        !self.state().regular.is_empty()
    }

    async fn wait_for_immediate_item(&self) {
        loop {
            let notified = self.immediate_notify.notified();
            if self.has_immediate_items() {
                return;
            }
            notified.await;
        }
    }

    async fn wait_for_regular_item(&self) {
        loop {
            let notified = self.regular_notify.notified();
            if self.has_regular_items() {
                return;
            }
            notified.await;
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

fn added_instant_for_enqueued_at(
    enqueued_at: SystemTime,
    now: Instant,
    wall_now: SystemTime,
) -> Instant {
    match wall_now.duration_since(enqueued_at) {
        Ok(age) => now.checked_sub(age).unwrap_or(now),
        Err(_) => now,
    }
}

async fn send_work_item_method_status<F, Fut>(
    item: DispatcherWorkItem,
    send_method: F,
) -> DispatcherSendStatus
where
    F: FnOnce(TelegramOutboundMethod) -> Fut,
    Fut: Future<Output = DispatcherSendStatus>,
{
    let Some(method) = item.into_method() else {
        return DispatcherSendStatus::Failed;
    };
    send_method(method).await
}

pub async fn run_limiter_cleanup_until<Stop>(
    limiters: &ChatLimiters,
    config: DispatcherRuntimeConfig,
    stop: Stop,
) -> DispatcherWorkerLoopOutcome
where
    Stop: Future<Output = ()>,
{
    let cleanup_interval = config.cleanup_interval();
    let rate_limiter_max_idle = config.rate_limiter_max_idle();
    tokio::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => return DispatcherWorkerLoopOutcome::Stopped,
            () = sleep(cleanup_interval) => {
                limiters.cleanup(rate_limiter_max_idle);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        future::pending,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::{
        DEFAULT_DISPATCHER_CLEANUP_INTERVAL, DispatcherConfig, DispatcherMessage, DispatcherQueue,
        DispatcherRestoredMessage, DispatcherRuntimeConfig, DispatcherSendStatus,
        DispatcherWorkerLoopOutcome, DispatcherWorkerOutcome, EnqueueOutcome, QueueSnapshot,
        RegularDequeueOutcome, run_limiter_cleanup_until,
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

    fn restored_text(
        chat_id: i64,
        text: &str,
        virtual_id: &str,
        immediate: bool,
    ) -> DispatcherRestoredMessage {
        let fingerprint = MessageFingerprint {
            chat_id,
            message_type: MESSAGE_TYPE_TEXT.to_owned(),
            content_hash: hash_content(text),
            debounce_key: None,
        };
        DispatcherRestoredMessage {
            fingerprint: fingerprint.clone(),
            fingerprint_key: fingerprint.to_string(),
            virtual_id: virtual_id.to_owned(),
            immediate,
            enqueued_at: std::time::SystemTime::now(),
            method: text_method(chat_id, text),
            persistence_payload: None,
            bypass_chat_restrictions: false,
            ephemeral_delete_after: None,
        }
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

    async fn wait_until(mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("condition became true");
    }

    #[test]
    fn dispatcher_runtime_config_defaults_match_go_cleanup_settings() {
        let config = DispatcherRuntimeConfig::default();

        assert_eq!(
            config.cleanup_interval(),
            DEFAULT_DISPATCHER_CLEANUP_INTERVAL
        );
        assert_eq!(
            config.rate_limiter_max_idle(),
            crate::DEFAULT_RATE_LIMITER_MAX_IDLE
        );
        assert_eq!(
            DispatcherRuntimeConfig {
                cleanup_interval: Duration::ZERO,
                rate_limiter_max_idle: Duration::ZERO,
            }
            .cleanup_interval(),
            DEFAULT_DISPATCHER_CLEANUP_INTERVAL
        );
        assert_eq!(
            DispatcherRuntimeConfig {
                cleanup_interval: Duration::ZERO,
                rate_limiter_max_idle: Duration::ZERO,
            }
            .rate_limiter_max_idle(),
            crate::DEFAULT_RATE_LIMITER_MAX_IDLE
        );
    }

    #[tokio::test]
    async fn limiter_cleanup_loop_removes_idle_limiters_until_stop() {
        let limiters = Arc::new(ChatLimiters::new(Duration::ZERO));
        assert!(limiters.allow(42));
        assert_eq!(limiters.active_len(), 1);

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn({
            let limiters = Arc::clone(&limiters);
            async move {
                run_limiter_cleanup_until(
                    &limiters,
                    DispatcherRuntimeConfig {
                        cleanup_interval: Duration::from_millis(5),
                        rate_limiter_max_idle: Duration::from_millis(1),
                    },
                    async {
                        let _ = stop_rx.await;
                    },
                )
                .await
            }
        });

        wait_until(|| limiters.active_len() == 0).await;

        stop_tx.send(()).expect("stop signal sent");
        let outcome = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("cleanup worker stopped")
            .expect("cleanup worker task");
        assert_eq!(outcome, DispatcherWorkerLoopOutcome::Stopped);
    }

    #[test]
    fn drain_for_shutdown_preserves_go_queue_order_and_payloads() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();

        queue.enqueue_at(
            text_message(42, "regular first", "regular-1")
                .with_method(text_method(42, "regular first payload")),
            false,
            now,
        );
        queue.enqueue_at(
            text_message(43, "immediate first", "immediate-1")
                .with_method(text_method(43, "immediate first payload")),
            true,
            now + Duration::from_secs(1),
        );
        queue.enqueue_at(
            text_message(44, "regular second", "regular-2")
                .with_method(text_method(44, "regular second payload")),
            false,
            now + Duration::from_secs(2),
        );

        let drained = queue.drain_for_shutdown();

        assert_eq!(drained.len(), 3);
        assert!(!drained.is_empty());
        assert_eq!(drained.immediate.len(), 1);
        assert_eq!(drained.regular.len(), 2);
        assert_eq!(drained.immediate[0].metadata().virtual_id, "immediate-1");
        assert_eq!(drained.regular[0].metadata().virtual_id, "regular-1");
        assert_eq!(drained.regular[1].metadata().virtual_id, "regular-2");
        assert_eq!(
            drained.immediate[0]
                .metadata()
                .added_at
                .saturating_duration_since(now),
            Duration::from_secs(1)
        );
        assert_eq!(
            drained.regular[1]
                .metadata()
                .added_at
                .saturating_duration_since(now),
            Duration::from_secs(2)
        );
        assert_eq!(
            drained.immediate[0]
                .method_kind()
                .expect("immediate method kind"),
            TelegramOutboundMethodKind::SendMessage
        );
        assert_eq!(
            drained.regular[1]
                .method_kind()
                .expect("regular method kind"),
            TelegramOutboundMethodKind::SendMessage
        );
        assert_eq!(queue.stats_at(now).regular_queue_size, 0);
        assert_eq!(queue.stats_at(now).immediate_queue_size, 0);
        assert_eq!(virtual_ids(queue.snapshot()), (vec![], vec![]));
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

    #[tokio::test]
    async fn immediate_method_worker_sends_owned_payload_through_transport_callback() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();
        let sent_methods = Arc::new(Mutex::new(Vec::new()));

        queue.enqueue_at(
            text_message(42, "immediate", "immediate-1")
                .with_method(text_method(42, "payload body")),
            true,
            now,
        );

        let outcome = queue
            .process_immediate_method_once({
                let sent_methods = Arc::clone(&sent_methods);
                move |method| async move {
                    sent_methods
                        .lock()
                        .expect("sent methods lock")
                        .push(method.kind());
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
            *sent_methods.lock().expect("sent methods lock"),
            vec![TelegramOutboundMethodKind::SendMessage]
        );
        assert_eq!(queue.stats_at(now).processed_total, 1);
    }

    #[tokio::test]
    async fn regular_method_worker_respects_limiter_before_transport_callback() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let limiters = ChatLimiters::new(crate::DEFAULT_DISPATCH_INTERVAL);
        let now = std::time::Instant::now();
        let sent_methods = Arc::new(Mutex::new(Vec::new()));

        queue.enqueue_at(
            text_message(42, "first", "regular-1").with_method(text_method(42, "first payload")),
            false,
            now,
        );
        queue.enqueue_at(
            text_message(42, "second", "regular-2").with_method(text_method(42, "second payload")),
            false,
            now + Duration::from_secs(1),
        );

        let first = queue
            .process_regular_method_once_at(&limiters, now, {
                let sent_methods = Arc::clone(&sent_methods);
                move |method| async move {
                    sent_methods
                        .lock()
                        .expect("sent methods lock")
                        .push(method.kind());
                    DispatcherSendStatus::Sent
                }
            })
            .await;
        assert!(matches!(
            first,
            DispatcherWorkerOutcome::Sent {
                virtual_id,
                immediate: false
            } if virtual_id == "regular-1"
        ));

        let limited = queue
            .process_regular_method_once_at(&limiters, now, {
                let sent_methods = Arc::clone(&sent_methods);
                move |method| async move {
                    sent_methods
                        .lock()
                        .expect("sent methods lock")
                        .push(method.kind());
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
        assert_eq!(
            *sent_methods.lock().expect("sent methods lock"),
            vec![TelegramOutboundMethodKind::SendMessage]
        );
        assert_eq!(
            virtual_ids(queue.snapshot()).0,
            vec!["regular-2".to_owned()]
        );
        assert_eq!(queue.stats_at(now).processed_total, 1);
    }

    #[tokio::test]
    async fn immediate_worker_loop_waits_for_enqueue_and_stops_on_signal() {
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let sent_methods = Arc::new(Mutex::new(Vec::new()));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let worker = tokio::spawn({
            let queue = Arc::clone(&queue);
            let sent_methods = Arc::clone(&sent_methods);
            async move {
                queue
                    .run_immediate_method_worker_until(
                        async {
                            let _ = stop_rx.await;
                        },
                        move |method| {
                            let sent_methods = Arc::clone(&sent_methods);
                            async move {
                                sent_methods
                                    .lock()
                                    .expect("sent methods lock")
                                    .push(method.kind());
                                DispatcherSendStatus::Sent
                            }
                        },
                    )
                    .await
            }
        });

        tokio::task::yield_now().await;
        assert!(sent_methods.lock().expect("sent methods lock").is_empty());

        queue.enqueue(
            text_message(42, "immediate", "immediate-1")
                .with_method(text_method(42, "payload body")),
            true,
        );

        wait_until(|| sent_methods.lock().expect("sent methods lock").len() == 1).await;
        assert_eq!(
            *sent_methods.lock().expect("sent methods lock"),
            vec![TelegramOutboundMethodKind::SendMessage]
        );
        assert_eq!(queue.stats().processed_total, 1);

        stop_tx.send(()).expect("stop signal sent");
        let outcome = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker stopped")
            .expect("worker task");
        assert_eq!(outcome, DispatcherWorkerLoopOutcome::Stopped);
    }

    #[tokio::test]
    async fn regular_worker_loop_waits_sleeps_for_limiter_and_stops_on_signal() {
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let limiters = Arc::new(ChatLimiters::new(crate::DEFAULT_DISPATCH_INTERVAL));
        let sent_methods = Arc::new(Mutex::new(Vec::new()));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let worker = tokio::spawn({
            let queue = Arc::clone(&queue);
            let limiters = Arc::clone(&limiters);
            let sent_methods = Arc::clone(&sent_methods);
            async move {
                queue
                    .run_regular_method_worker_until(
                        &limiters,
                        async {
                            let _ = stop_rx.await;
                        },
                        move |method| {
                            let sent_methods = Arc::clone(&sent_methods);
                            async move {
                                sent_methods
                                    .lock()
                                    .expect("sent methods lock")
                                    .push(method.kind());
                                DispatcherSendStatus::Sent
                            }
                        },
                    )
                    .await
            }
        });

        tokio::task::yield_now().await;
        queue.enqueue(
            text_message(42, "first", "regular-1").with_method(text_method(42, "first payload")),
            false,
        );
        queue.enqueue(
            text_message(42, "second", "regular-2").with_method(text_method(42, "second payload")),
            false,
        );

        wait_until(|| sent_methods.lock().expect("sent methods lock").len() == 2).await;
        assert_eq!(
            *sent_methods.lock().expect("sent methods lock"),
            vec![
                TelegramOutboundMethodKind::SendMessage,
                TelegramOutboundMethodKind::SendMessage
            ]
        );
        assert_eq!(queue.stats().processed_total, 2);

        stop_tx.send(()).expect("stop signal sent");
        let outcome = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker stopped")
            .expect("worker task");
        assert_eq!(outcome, DispatcherWorkerLoopOutcome::Stopped);
    }

    #[tokio::test]
    async fn combined_worker_runner_processes_regular_and_immediate_until_stop() {
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let limiters = Arc::new(ChatLimiters::new(crate::DEFAULT_DISPATCH_INTERVAL));
        let sent_methods = Arc::new(Mutex::new(Vec::new()));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let worker = tokio::spawn({
            let queue = Arc::clone(&queue);
            let limiters = Arc::clone(&limiters);
            let sent_methods = Arc::clone(&sent_methods);
            async move {
                queue
                    .run_method_workers_until(
                        &limiters,
                        async {
                            let _ = stop_rx.await;
                        },
                        move |method| {
                            let sent_methods = Arc::clone(&sent_methods);
                            async move {
                                sent_methods
                                    .lock()
                                    .expect("sent methods lock")
                                    .push(method.kind());
                                DispatcherSendStatus::Sent
                            }
                        },
                    )
                    .await
            }
        });

        tokio::task::yield_now().await;
        queue.enqueue(
            text_message(42, "regular", "regular-1").with_method(text_method(42, "regular body")),
            false,
        );
        queue.enqueue(
            text_message(43, "immediate", "immediate-1")
                .with_method(text_method(43, "immediate body")),
            true,
        );

        wait_until(|| sent_methods.lock().expect("sent methods lock").len() == 2).await;
        assert_eq!(
            *sent_methods.lock().expect("sent methods lock"),
            vec![
                TelegramOutboundMethodKind::SendMessage,
                TelegramOutboundMethodKind::SendMessage
            ]
        );
        assert_eq!(queue.stats().processed_total, 2);
        assert_eq!(virtual_ids(queue.snapshot()), (vec![], vec![]));

        stop_tx.send(()).expect("stop signal sent");
        let outcome = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker stopped")
            .expect("worker task");
        assert_eq!(outcome, DispatcherWorkerLoopOutcome::Stopped);
    }

    #[tokio::test]
    async fn generic_worker_runner_exposes_work_item_metadata_until_stop() {
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let limiters = Arc::new(ChatLimiters::new(crate::DEFAULT_DISPATCH_INTERVAL));
        let sent_virtual_ids = Arc::new(Mutex::new(Vec::new()));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let worker = tokio::spawn({
            let queue = Arc::clone(&queue);
            let limiters = Arc::clone(&limiters);
            let sent_virtual_ids = Arc::clone(&sent_virtual_ids);
            async move {
                queue
                    .run_workers_until(
                        &limiters,
                        async {
                            let _ = stop_rx.await;
                        },
                        move |item| {
                            let sent_virtual_ids = Arc::clone(&sent_virtual_ids);
                            async move {
                                sent_virtual_ids
                                    .lock()
                                    .expect("sent virtual ids lock")
                                    .push((item.metadata().virtual_id.clone(), item.method_kind()));
                                DispatcherSendStatus::Sent
                            }
                        },
                    )
                    .await
            }
        });

        tokio::task::yield_now().await;
        queue.enqueue(
            text_message(42, "regular", "regular-1").with_method(text_method(42, "regular body")),
            false,
        );
        queue.enqueue(
            text_message(43, "immediate", "immediate-1")
                .with_method(text_method(43, "immediate body")),
            true,
        );

        wait_until(|| {
            sent_virtual_ids
                .lock()
                .expect("sent virtual ids lock")
                .len()
                == 2
        })
        .await;
        let mut sent = sent_virtual_ids
            .lock()
            .expect("sent virtual ids lock")
            .clone();
        sent.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            sent,
            vec![
                (
                    "immediate-1".to_owned(),
                    Some(TelegramOutboundMethodKind::SendMessage)
                ),
                (
                    "regular-1".to_owned(),
                    Some(TelegramOutboundMethodKind::SendMessage)
                ),
            ]
        );
        assert_eq!(queue.stats().processed_total, 2);

        stop_tx.send(()).expect("stop signal sent");
        let outcome = tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .expect("worker stopped")
            .expect("worker task");
        assert_eq!(outcome, DispatcherWorkerLoopOutcome::Stopped);
    }

    #[tokio::test]
    async fn method_worker_fails_missing_payload_without_calling_transport() {
        let queue = DispatcherQueue::new(DispatcherConfig::default());
        let now = std::time::Instant::now();
        let sends = Arc::new(Mutex::new(0usize));

        queue.enqueue_at(text_message(42, "immediate", "immediate-1"), true, now);

        let outcome = queue
            .process_immediate_method_once({
                let sends = Arc::clone(&sends);
                move |_| async move {
                    *sends.lock().expect("send count lock") += 1;
                    DispatcherSendStatus::Sent
                }
            })
            .await;

        assert_eq!(
            outcome,
            DispatcherWorkerOutcome::SendFailed {
                virtual_id: "immediate-1".to_owned(),
                immediate: true,
            }
        );
        assert_eq!(*sends.lock().expect("send count lock"), 0);
        assert_eq!(queue.stats_at(now).processed_total, 0);
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

    #[tokio::test]
    async fn stuck_immediate_send_times_out_so_worker_can_process_next_item() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            send_timeout: Duration::from_millis(10),
            ..DispatcherConfig::default()
        });
        let now = std::time::Instant::now();

        queue.enqueue_at(text_message(42, "stuck", "immediate-1"), true, now);
        queue.enqueue_at(
            text_message(42, "next", "immediate-2"),
            true,
            now + Duration::from_millis(1),
        );

        let timeout_outcome = queue
            .process_immediate_once(|_| pending::<DispatcherSendStatus>())
            .await;

        assert_eq!(
            timeout_outcome,
            DispatcherWorkerOutcome::SendFailed {
                virtual_id: "immediate-1".to_owned(),
                immediate: true,
            }
        );
        assert_eq!(queue.stats_at(now).processed_total, 0);
        assert_eq!(queue.stats_at(now).immediate_queue_size, 1);

        let next_outcome = queue
            .process_immediate_once(|_| async { DispatcherSendStatus::Sent })
            .await;

        assert_eq!(
            next_outcome,
            DispatcherWorkerOutcome::Sent {
                virtual_id: "immediate-2".to_owned(),
                immediate: true,
            }
        );
        assert_eq!(queue.stats_at(now).processed_total, 1);
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
            ..DispatcherConfig::default()
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
    fn restore_checks_existing_debouncer_for_immediate_items_like_go_start() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            max_queue_size: 0,
            dedupe_config: DebouncerConfig {
                enabled: true,
                default_window: Duration::from_secs(30),
                max_cache_size: 1000,
                per_chat_settings: HashMap::new(),
            },
            ..DispatcherConfig::default()
        });
        let now = std::time::Instant::now();

        assert_eq!(
            queue.enqueue_at(text_message(42, "same", "regular-1"), false, now),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            queue.restore(restored_text(42, "same", "immediate-2", true)),
            EnqueueOutcome::Deduped
        );

        let stats = queue.stats();
        assert_eq!(stats.regular_queue_size, 1);
        assert_eq!(stats.immediate_queue_size, 0);
        assert_eq!(stats.deduped_total, 1);
        assert_eq!(
            virtual_ids(queue.snapshot()),
            (vec!["regular-1".to_owned()], Vec::new())
        );
    }

    #[test]
    fn restore_does_not_record_loaded_items_into_debouncer_like_go_start() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            max_queue_size: 0,
            dedupe_config: DebouncerConfig {
                enabled: true,
                default_window: Duration::from_secs(30),
                max_cache_size: 1000,
                per_chat_settings: HashMap::new(),
            },
            ..DispatcherConfig::default()
        });

        assert_eq!(
            queue.restore(restored_text(42, "same", "regular-1", false)),
            EnqueueOutcome::Enqueued
        );
        assert_eq!(
            queue.restore(restored_text(42, "same", "regular-2", false)),
            EnqueueOutcome::Enqueued
        );

        let stats = queue.stats();
        assert_eq!(stats.regular_queue_size, 2);
        assert_eq!(stats.immediate_queue_size, 0);
        assert_eq!(stats.deduped_total, 0);
        assert_eq!(
            virtual_ids(queue.snapshot()),
            (
                vec!["regular-1".to_owned(), "regular-2".to_owned()],
                Vec::new()
            )
        );
    }

    #[test]
    fn max_queue_size_trims_oldest_per_queue() {
        let queue = DispatcherQueue::new(DispatcherConfig {
            max_queue_size: 1,
            dedupe_config: DebouncerConfig::default(),
            ..DispatcherConfig::default()
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
