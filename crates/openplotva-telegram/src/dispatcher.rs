use std::{
    collections::VecDeque,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{Debouncer, DebouncerConfig, MessageFingerprint};

/// Go outbound dispatcher queue settings currently ported to Rust.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DispatcherConfig {
    /// Maximum items retained in each queue. `0` means unbounded, matching Go.
    pub max_queue_size: usize,
    /// Regular-queue duplicate suppression settings.
    pub dedupe_config: DebouncerConfig,
}

/// Outbound message metadata needed by the queue layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherMessage {
    /// Fingerprint used for regular-queue deduplication.
    pub fingerprint: MessageFingerprint,
    /// Virtual message ID used for cancellation and future real-ID mapping.
    pub virtual_id: String,
}

impl DispatcherMessage {
    /// Build queue metadata for an outbound message.
    pub fn new(fingerprint: MessageFingerprint, virtual_id: impl Into<String>) -> Self {
        Self {
            fingerprint,
            virtual_id: virtual_id.into(),
        }
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

/// Inspectable queue item matching the Go dispatcher's persisted item metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatcherQueuedMessage {
    /// The dedupe key string captured when the item was enqueued.
    pub fingerprint_key: String,
    /// Virtual message ID used for cancellation and future real-ID mapping.
    pub virtual_id: String,
    /// Whether the item belongs to the immediate queue.
    pub immediate: bool,
    /// Enqueue time used for oldest-age statistics.
    pub added_at: Instant,
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
    regular: VecDeque<DispatcherQueuedMessage>,
    immediate: VecDeque<DispatcherQueuedMessage>,
    processed_total: i64,
    deduped_total: i64,
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
        state.regular.retain(|item| item.virtual_id != virtual_id);
        let immediate_before = state.immediate.len();
        state.immediate.retain(|item| item.virtual_id != virtual_id);

        (regular_before - state.regular.len()) + (immediate_before - state.immediate.len())
    }

    /// Record one successfully sent queue item.
    pub fn record_processed(&self) {
        let mut state = self.state();
        state.processed_total += 1;
    }

    /// Return current queue statistics.
    pub fn stats(&self) -> DispatcherStats {
        self.stats_at_inner(Instant::now())
    }

    /// Return a clone of queued metadata, oldest first in each queue.
    pub fn snapshot(&self) -> QueueSnapshot {
        let state = self.state();
        QueueSnapshot {
            regular: state.regular.iter().cloned().collect(),
            immediate: state.immediate.iter().cloned().collect(),
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
        if !immediate && !self.debouncer.should_process_at(&message.fingerprint, now) {
            self.state().deduped_total += 1;
            return EnqueueOutcome::Deduped;
        }

        let queued = DispatcherQueuedMessage {
            fingerprint_key: message.fingerprint.to_string(),
            virtual_id: message.virtual_id,
            immediate,
            added_at: now,
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
            self.debouncer.record_sent_at(&message.fingerprint, now);
        }
        EnqueueOutcome::Enqueued
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

fn trim_queue(queue: &mut VecDeque<DispatcherQueuedMessage>, max_queue_size: usize) {
    if max_queue_size == 0 {
        return;
    }
    while queue.len() > max_queue_size {
        queue.pop_front();
    }
}

fn oldest_age(queue: &VecDeque<DispatcherQueuedMessage>, now: Instant) -> Duration {
    queue
        .front()
        .map(|item| now.saturating_duration_since(item.added_at))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Duration};

    use super::{
        DispatcherConfig, DispatcherMessage, DispatcherQueue, EnqueueOutcome, QueueSnapshot,
    };
    use crate::{DebouncerConfig, MESSAGE_TYPE_TEXT, MessageFingerprint, hash_content};

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
