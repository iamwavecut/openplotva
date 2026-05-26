use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use openplotva_taskman::{InMemoryTaskQueue, StatelessJobItem, clean_unicode_non_printables};
use tokio::task::JoinHandle;

use crate::edited::EditedDialogJobUpdate;

pub const GO_DIALOG_DEBOUNCE_INTERVAL: Duration = Duration::from_secs(5);

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
    /// Job assigned after the debounce delay elapses.
    pub job: StatelessJobItem,
}

/// Report returned after assigning a debounced dialog job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogDebounceAssignReport {
    /// Queue used for the assignment.
    pub queue_name: String,
    /// New taskman ID.
    pub task_id: i64,
}

/// Report returned after scheduling a debounced dialog job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DialogDebounceScheduleReport {
    /// Effective delay before assignment.
    pub delay: Duration,
    /// Whether an older entry or timer was replaced for the same chat/user/thread key.
    pub replaced: bool,
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

    /// Register one debounced dialog job, replacing any existing entry for the key.
    pub fn register(
        &self,
        key: DialogDebounceKey,
        message_id: i32,
        queue_name: impl Into<String>,
        job: StatelessJobItem,
    ) -> bool {
        let replaced_timer = self.cancel_timer(key);
        let replaced_entry = self.entries().insert(
            key,
            DialogDebounceEntry {
                message_id,
                queue_name: queue_name.into(),
                job,
            },
        );
        replaced_timer || replaced_entry.is_some()
    }

    /// Register one debounced dialog job and assign it to taskman after the delay.
    pub fn schedule(
        self: &Arc<Self>,
        key: DialogDebounceKey,
        message_id: i32,
        queue_name: impl Into<String>,
        job: StatelessJobItem,
        queue: InMemoryTaskQueue,
        delay: Duration,
    ) -> DialogDebounceScheduleReport {
        let delay = Self::delay_or_default(delay);
        let queue_name = queue_name.into();
        let replaced = self.register(key, message_id, queue_name, job);
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let debounce = Arc::clone(self);
        let handle = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Some(report) = debounce.assign_due_from_timer(key, generation, &queue) {
                tracing::debug!(
                    chat_id = key.chat_id,
                    user_id = key.user_id,
                    thread_id = key.thread_id,
                    queue_name = %report.queue_name,
                    task_id = report.task_id,
                    "assigned dialog job after debounce"
                );
            }
        });

        self.timers()
            .insert(key, DialogDebounceTimer { generation, handle });
        DialogDebounceScheduleReport { delay, replaced }
    }

    /// Update an existing debounced dialog job after a Telegram edited-message update.
    pub fn update(&self, update: EditedDialogJobUpdate<'_>) -> Result<bool, serde_json::Error> {
        let key = DialogDebounceKey::from_update(update);
        let mut entries = self.entries();
        let Some(entry) = entries.get_mut(&key) else {
            return Ok(false);
        };
        if entry.message_id != update.message_id {
            return Ok(false);
        }
        let Some(dialog_data) = entry.job.data.dialog_data.as_mut() else {
            return Ok(false);
        };
        dialog_data.message_text = clean_unicode_non_printables(update.message_text);
        dialog_data.original_text = update.original_text.trim().to_owned();
        dialog_data.meta = serde_json::to_value(update.meta)?;
        Ok(true)
    }

    /// Assign and remove an entry when the debounce timer fires.
    pub fn assign_due(
        &self,
        key: DialogDebounceKey,
        queue: &InMemoryTaskQueue,
    ) -> Option<DialogDebounceAssignReport> {
        self.cancel_timer(key);
        self.assign_due_without_timer_cancel(key, queue)
    }

    /// Remove an entry without assigning it.
    pub fn remove(&self, key: DialogDebounceKey) -> Option<DialogDebounceEntry> {
        self.cancel_timer(key);
        self.entries().remove(&key)
    }

    /// Stop all pending debounce timers and clear all unassigned entries.
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

    fn assign_due_from_timer(
        &self,
        key: DialogDebounceKey,
        generation: u64,
        queue: &InMemoryTaskQueue,
    ) -> Option<DialogDebounceAssignReport> {
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
        self.assign_due_without_timer_cancel(key, queue)
    }

    fn assign_due_without_timer_cancel(
        &self,
        key: DialogDebounceKey,
        queue: &InMemoryTaskQueue,
    ) -> Option<DialogDebounceAssignReport> {
        let entry = self.entries().remove(&key)?;
        let queue_name = entry.queue_name;
        let task_id = queue.assign(queue_name.clone(), entry.job);
        Some(DialogDebounceAssignReport {
            queue_name,
            task_id,
        })
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
        DEFAULT_PRIORITY, DIALOG_AIFARM_QUEUE_NAME, DialogJobData, JobPayload, JobType,
        StatelessJobItem,
    };
    use time::OffsetDateTime;

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
    fn debounce_updates_matching_message_then_assigns_job() {
        let debounce = InMemoryDialogDebounce::new();
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 9,
        };
        debounce.register(
            key,
            77,
            DIALOG_AIFARM_QUEUE_NAME,
            dialog_job("old text", 111, 42, 77),
        );
        let meta = meta_sender(111);

        assert!(
            debounce
                .update(EditedDialogJobUpdate {
                    chat_id: 42,
                    message_id: 77,
                    sender_id: 111,
                    thread_id: Some(9),
                    message_text: "\u{200f}new\ttext ",
                    original_text: " original text ",
                    meta: &meta,
                })
                .expect("update debounced dialog")
        );
        let report = debounce.assign_due(key, &queue).expect("assigned");

        assert_eq!(report.queue_name, DIALOG_AIFARM_QUEUE_NAME);
        assert_eq!(debounce.len(), 0);
        let records = queue.records();
        assert_eq!(records[0].id, report.task_id);
        let dialog = records[0].job.data.dialog_data.as_ref().expect("dialog");
        assert_eq!(dialog.message_text, "new text");
        assert_eq!(dialog.original_text, "original text");
        assert_eq!(dialog.meta["sender_id"], 111);
    }

    #[test]
    fn debounce_replaces_existing_key_like_go_register() {
        let debounce = InMemoryDialogDebounce::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };
        debounce.register(key, 1, "old", dialog_job("old", 111, 42, 1));
        debounce.register(key, 2, "new", dialog_job("new", 111, 42, 2));

        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let report = debounce.assign_due(key, &queue).expect("assigned");
        assert_eq!(report.queue_name, "new");
        assert_eq!(
            queue.records()[0]
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
    async fn debounce_schedule_assigns_after_delay() {
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };

        let report = debounce.schedule(
            key,
            1,
            "text",
            dialog_job("scheduled", 111, 42, 1),
            queue.clone(),
            Duration::from_millis(10),
        );

        assert_eq!(report.delay, Duration::from_millis(10));
        assert!(!report.replaced);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(debounce.is_empty());
        let records = queue.records();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0]
                .job
                .data
                .dialog_data
                .as_ref()
                .expect("dialog")
                .message_text,
            "scheduled"
        );
    }

    #[tokio::test]
    async fn debounce_schedule_replaces_and_cancels_previous_timer() {
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };

        debounce.schedule(
            key,
            1,
            "text",
            dialog_job("old", 111, 42, 1),
            queue.clone(),
            Duration::from_millis(60),
        );
        let replacement = debounce.schedule(
            key,
            2,
            "text",
            dialog_job("new", 111, 42, 2),
            queue.clone(),
            Duration::from_millis(10),
        );

        assert!(replacement.replaced);
        tokio::time::sleep(Duration::from_millis(90)).await;
        let records = queue.records();
        assert_eq!(records.len(), 1);
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
    async fn debounce_stop_all_cancels_timers_and_clears_entries() {
        let debounce = Arc::new(InMemoryDialogDebounce::new());
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let key = DialogDebounceKey {
            chat_id: 42,
            user_id: 111,
            thread_id: 0,
        };

        debounce.schedule(
            key,
            1,
            "text",
            dialog_job("scheduled", 111, 42, 1),
            queue.clone(),
            Duration::from_millis(20),
        );

        assert_eq!(debounce.stop_all(), 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(debounce.is_empty());
        assert!(queue.records().is_empty());
    }

    fn dialog_job(
        message_text: &str,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> StatelessJobItem {
        StatelessJobItem {
            title: "dialog".to_owned(),
            created: OffsetDateTime::UNIX_EPOCH,
            priority: DEFAULT_PRIORITY,
            processing_timeout_seconds: 0,
            data: JobPayload {
                job_type: JobType::Dialog,
                telegram_data: Some(openplotva_taskman::TelegramData {
                    chat_id,
                    user_id,
                    message_id,
                    thread_message_id: None,
                    user_full_name: "Ada".to_owned(),
                    chat_title: String::new(),
                }),
                dialog_data: Some(DialogJobData {
                    message_text: message_text.to_owned(),
                    ..DialogJobData::default()
                }),
                image_data: None,
                music_data: None,
                control_data: None,
            },
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
