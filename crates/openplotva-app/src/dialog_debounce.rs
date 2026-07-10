use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use openplotva_taskman::{
    InMemoryTaskQueue, StatelessJobItem, TaskQueueSchedule, TaskQueueScheduleDisposition,
};
use tokio::task::JoinHandle;

use crate::edited::EditedDialogJobUpdate;

pub const GO_DIALOG_DEBOUNCE_INTERVAL: Duration = Duration::from_secs(5);
pub type DialogDebounceAssignObserverFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

pub trait DialogDebounceAssignObserver: Send + Sync {
    fn assigned<'a>(
        &'a self,
        key: DialogDebounceKey,
        queue_name: &'a str,
        task_id: i64,
    ) -> DialogDebounceAssignObserverFuture<'a>;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DialogDebounceKey {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Sender/user ID.
    pub user_id: i64,
    /// Forum topic/thread ID, or zero when absent.
    pub thread_id: i64,
}

/// One scheduled dialog debounce entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogDebounceEntry {
    /// Message that can still edit the debounced dialog job.
    pub message_id: i32,
    /// Queue name selected by the dialog planner.
    pub queue_name: String,
    pub task_id: i64,
    generation: u64,
}

/// Report returned after scheduling a debounced dialog job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogDebounceScheduleReport {
    /// Effective delay before the durable task becomes dequeueable.
    pub delay: Duration,
    /// Whether an older entry or timer was replaced for the same chat/user/thread key.
    pub replaced: bool,
    /// Whether the source update was already represented by a durable task.
    pub reused: bool,
    pub task_id: i64,
    generation: Option<u64>,
}

struct DialogDebounceTimer {
    generation: u64,
    handle: JoinHandle<()>,
}

pub struct InMemoryDialogDebounce {
    entries: Mutex<HashMap<DialogDebounceKey, DialogDebounceEntry>>,
    timers: Mutex<HashMap<DialogDebounceKey, DialogDebounceTimer>>,
    next_generation: AtomicU64,
}

impl fmt::Debug for InMemoryDialogDebounce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InMemoryDialogDebounce")
            .field("entries", &self.len())
            .field("timers", &self.timer_len())
            .finish()
    }
}

impl Default for InMemoryDialogDebounce {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            timers: Mutex::new(HashMap::new()),
            next_generation: AtomicU64::new(1),
        }
    }
}

impl InMemoryDialogDebounce {
    /// Build empty debounce state.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn delay_or_default(delay: Duration) -> Duration {
        if delay.is_zero() {
            GO_DIALOG_DEBOUNCE_INTERVAL
        } else {
            delay
        }
    }

    /// Track one already-durable scheduled task for edited-message lookup and
    /// timer wakeup. The task payload remains owned by taskman.
    pub fn track(
        &self,
        key: DialogDebounceKey,
        message_id: i32,
        queue_name: impl Into<String>,
        task_id: i64,
    ) -> (bool, u64) {
        let replaced_timer = self.cancel_timer(key);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let replaced_entry = self.entries().insert(
            key,
            DialogDebounceEntry {
                message_id,
                queue_name: queue_name.into(),
                task_id,
                generation,
            },
        );
        (replaced_timer || replaced_entry.is_some(), generation)
    }

    /// Persist a delayed task immediately. `available_at` controls dequeue;
    /// this in-memory state only indexes the durable task and arms a wake timer.
    #[allow(clippy::too_many_arguments)]
    pub fn schedule(
        self: &Arc<Self>,
        key: DialogDebounceKey,
        message_id: i32,
        queue_name: impl Into<String>,
        job: StatelessJobItem,
        task_schedule: TaskQueueSchedule,
        queue: InMemoryTaskQueue,
        delay: Duration,
    ) -> DialogDebounceScheduleReport {
        let delay = Self::delay_or_default(delay);
        let queue_name = queue_name.into();
        let task_report = queue.schedule_or_replace(queue_name.clone(), job, task_schedule);
        if task_report.disposition == TaskQueueScheduleDisposition::Reused {
            return DialogDebounceScheduleReport {
                delay,
                replaced: false,
                reused: true,
                task_id: task_report.task_id,
                generation: None,
            };
        }
        let (tracked_replacement, generation) =
            self.track(key, message_id, queue_name, task_report.task_id);
        DialogDebounceScheduleReport {
            delay,
            replaced: tracked_replacement
                || task_report.disposition == TaskQueueScheduleDisposition::Replaced,
            reused: false,
            task_id: task_report.task_id,
            generation: Some(generation),
        }
    }

    /// Arm the non-durable wake/index cleanup only after the task's durability
    /// barrier has completed. Returns false when a newer schedule won the key.
    pub fn arm_timer(
        self: &Arc<Self>,
        key: DialogDebounceKey,
        report: &DialogDebounceScheduleReport,
        assign_observer: Option<Arc<dyn DialogDebounceAssignObserver>>,
    ) -> bool {
        let Some(generation) = report.generation else {
            return false;
        };
        let debounce = Arc::clone(self);
        let delay = report.delay;
        let (start_sender, start_receiver) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if start_receiver.await.is_err() {
                return;
            }
            tokio::time::sleep(delay).await;
            if let Some(entry) = debounce.release_from_timer(key, generation) {
                tracing::debug!(
                    chat_id = key.chat_id,
                    user_id = key.user_id,
                    thread_id = key.thread_id,
                    queue_name = %entry.queue_name,
                    task_id = entry.task_id,
                    "released durable dialog job after debounce"
                );
                if let Some(assign_observer) = assign_observer.as_deref() {
                    assign_observer
                        .assigned(key, &entry.queue_name, entry.task_id)
                        .await;
                }
            }
        });
        let mut timers = self.timers();
        let entries = self.entries();
        if !entries
            .get(&key)
            .is_some_and(|entry| entry.generation == generation && entry.task_id == report.task_id)
        {
            drop(entries);
            drop(timers);
            handle.abort();
            return false;
        }
        if let Some(previous) = timers.insert(key, DialogDebounceTimer { generation, handle }) {
            previous.handle.abort();
        }
        drop(entries);
        drop(timers);
        let _ = start_sender.send(());
        true
    }

    /// Check whether the RAM index still points at this edited message. Payload
    /// mutation is performed against the durable taskman record by the caller.
    pub fn update(&self, update: EditedDialogJobUpdate<'_>) -> bool {
        let key = DialogDebounceKey::from_update(update);
        self.entries()
            .get(&key)
            .is_some_and(|entry| entry.message_id == update.message_id)
    }

    /// Remove an entry without assigning it.
    pub fn remove(&self, key: DialogDebounceKey) -> Option<DialogDebounceEntry> {
        self.cancel_timer(key);
        self.entries().remove(&key)
    }

    /// Stop wake timers and clear RAM indexes without touching durable tasks.
    pub fn stop_all(&self) -> usize {
        let mut timers = self.timers();
        for (_key, timer) in timers.drain() {
            timer.handle.abort();
        }
        drop(timers);

        let mut entries = self.entries();
        let stopped = entries.len();
        entries.clear();
        stopped
    }

    /// Current active debounce entry count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries().len()
    }

    /// Whether no debounce entries are active.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn release_from_timer(
        &self,
        key: DialogDebounceKey,
        generation: u64,
    ) -> Option<DialogDebounceEntry> {
        let timer_matches = {
            let mut timers = self.timers();
            match timers.get(&key) {
                Some(timer) if timer.generation == generation => {
                    timers.remove(&key);
                    true
                }
                _ => false,
            }
        };
        if !timer_matches {
            return None;
        }
        let mut entries = self.entries();
        if entries
            .get(&key)
            .is_none_or(|entry| entry.generation != generation)
        {
            return None;
        }
        entries.remove(&key)
    }

    fn cancel_timer(&self, key: DialogDebounceKey) -> bool {
        let Some(timer) = self.timers().remove(&key) else {
            return false;
        };
        timer.handle.abort();
        true
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<DialogDebounceKey, DialogDebounceEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn timers(&self) -> MutexGuard<'_, HashMap<DialogDebounceKey, DialogDebounceTimer>> {
        self.timers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn timer_len(&self) -> usize {
        self.timers().len()
    }
}

impl DialogDebounceKey {
    #[must_use]
    pub fn durable_debounce_key(self) -> String {
        format!(
            "dialog:{}:{}:{}",
            self.chat_id, self.user_id, self.thread_id
        )
    }

    #[must_use]
    pub fn durable_lane_key(self) -> String {
        format!("dialog:{}:{}", self.chat_id, self.thread_id)
    }

    /// Build a debounce key from an edited-message update payload.
    #[must_use]
    pub fn from_update(update: EditedDialogJobUpdate<'_>) -> Self {
        Self {
            chat_id: update.chat_id,
            user_id: update.sender_id,
            thread_id: update.thread_id.unwrap_or_default().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use openplotva_core::ChatMessageMeta;
    use openplotva_taskman::{
        DIALOG_AIFARM_QUEUE_NAME, DialogJobParams, InMemoryTaskQueue, JobStatus, TaskQueueSchedule,
        new_dialog_job_at,
    };
    use time::{Duration as TimeDuration, OffsetDateTime};

    use crate::edited::EditedDialogJobUpdate;

    use super::{DialogDebounceKey, GO_DIALOG_DEBOUNCE_INTERVAL, InMemoryDialogDebounce};

    #[test]
    fn debounce_delay_uses_go_default_for_zero_only() {
        assert_eq!(
            InMemoryDialogDebounce::delay_or_default(Duration::ZERO),
            GO_DIALOG_DEBOUNCE_INTERVAL
        );
        assert_eq!(
            InMemoryDialogDebounce::delay_or_default(Duration::from_millis(150)),
            Duration::from_millis(150)
        );
    }

    #[test]
    fn debounce_key_formats_are_stable_and_ascii() {
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 9,
        };
        assert_eq!(key.durable_debounce_key(), "dialog:42:111:9");
        assert_eq!(key.durable_lane_key(), "dialog:42:9");
    }

    #[test]
    fn debounce_index_matches_edits_without_owning_the_payload() {
        let debounce = InMemoryDialogDebounce::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 9,
        };
        debounce.track(key, 77, DIALOG_AIFARM_QUEUE_NAME, 501);
        let meta = meta_sender(111);

        assert!(debounce.update(EditedDialogJobUpdate {
            chat_id: 42,
            message_id: 77,
            sender_id: 111,
            thread_id: Some(9),
            message_text: "new text",
            original_text: "new text",
            meta: &meta,
            source_update_id: 12,
        }));
        assert_eq!(debounce.len(), 1);
    }

    #[tokio::test]
    async fn durable_schedule_survives_restart_before_availability() {
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let queue = InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };
        let now = OffsetDateTime::now_utc();
        let input = dialog_params("scheduled", 111, 42, 1);

        let report = debounce.schedule(
            key,
            1,
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(input.clone(), now),
            task_schedule(key, now + TimeDuration::milliseconds(20), 10, input),
            queue.clone(),
            Duration::from_millis(20),
        );

        assert_eq!(report.delay, Duration::from_millis(20));
        assert!(!report.replaced);
        assert_eq!(
            queue.records().len(),
            1,
            "assignment is immediate and durable"
        );
        let restored = InMemoryTaskQueue::from_snapshot(queue.snapshot());
        assert!(
            restored
                .dequeue(DIALOG_AIFARM_QUEUE_NAME, "worker", now)
                .is_none()
        );
        assert_eq!(
            restored
                .dequeue(
                    DIALOG_AIFARM_QUEUE_NAME,
                    "worker",
                    now + TimeDuration::milliseconds(20),
                )
                .map(|work| work.id),
            Some(report.task_id)
        );
    }

    #[tokio::test]
    async fn debounce_replaces_in_place_and_arms_only_the_latest_timer() {
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let queue = InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };
        let now = OffsetDateTime::now_utc();
        let old_input = dialog_params("old", 111, 42, 1);

        let old = debounce.schedule(
            key,
            1,
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(old_input.clone(), now),
            task_schedule(key, now + TimeDuration::milliseconds(60), 10, old_input),
            queue.clone(),
            Duration::from_millis(60),
        );
        assert!(debounce.arm_timer(key, &old, None));
        let new_input = dialog_params("new", 111, 42, 2);
        let replacement = debounce.schedule(
            key,
            2,
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(new_input.clone(), now),
            task_schedule(key, now + TimeDuration::milliseconds(10), 11, new_input),
            queue.clone(),
            Duration::from_millis(10),
        );
        assert_eq!(old.task_id, replacement.task_id);
        assert!(debounce.arm_timer(key, &replacement, None));

        assert!(replacement.replaced);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(debounce.is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].source_update_ids, vec![10, 11]);
        assert_eq!(records[0].pending_dialog_inputs.len(), 2);
        assert_eq!(
            records[0]
                .job
                .data
                .dialog_data
                .as_ref()
                .expect("dialog")
                .message_text,
            "new"
        );
    }

    #[tokio::test]
    async fn debounce_stop_all_keeps_the_durable_scheduled_task() {
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let queue = InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };
        let now = OffsetDateTime::now_utc();
        let input = dialog_params("scheduled", 111, 42, 1);

        let report = debounce.schedule(
            key,
            1,
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(input.clone(), now),
            task_schedule(key, now + TimeDuration::milliseconds(20), 10, input),
            queue.clone(),
            Duration::from_millis(20),
        );
        assert!(debounce.arm_timer(key, &report, None));

        assert_eq!(debounce.stop_all(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(debounce.is_empty());
        assert_eq!(queue.records().len(), 1);
        assert_eq!(queue.records()[0].status, JobStatus::Pending);
    }

    fn dialog_params(
        message_text: &str,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> DialogJobParams {
        DialogJobParams {
            chat_id,
            message_id,
            user_id,
            user_full_name: "Ada".to_owned(),
            message_text: message_text.to_owned(),
            original_text: message_text.to_owned(),
            meta: serde_json::json!({"sender_id": user_id}),
            max_output_tokens: 0,
            thread_id: None,
        }
    }

    fn task_schedule(
        key: DialogDebounceKey,
        available_at: OffsetDateTime,
        update_id: i64,
        input: DialogJobParams,
    ) -> TaskQueueSchedule {
        TaskQueueSchedule {
            available_at: Some(available_at),
            debounce_key: Some(key.durable_debounce_key()),
            lane_key: Some(key.durable_lane_key()),
            source_update_ids: vec![update_id],
            latest_update_id: Some(update_id),
            pending_dialog_inputs: vec![input],
        }
    }

    fn meta_sender(sender_id: i64) -> ChatMessageMeta {
        ChatMessageMeta {
            sender_id,
            sender_type: "user".to_owned(),
            sender_name: "Ada".to_owned(),
            ..ChatMessageMeta::default()
        }
    }
}
