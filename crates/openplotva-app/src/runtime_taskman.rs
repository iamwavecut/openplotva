use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use openplotva_server::{
    RuntimeTaskmanDiagnosticsData, RuntimeTaskmanInspector, RuntimeTaskmanJobData,
    RuntimeTaskmanJobDetailsData, RuntimeTaskmanJobListEntryData, RuntimeTaskmanJobListResultData,
    RuntimeTaskmanJobSummaryData, RuntimeTaskmanJobsFilter, RuntimeTaskmanQueueDiagnosticsData,
};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, InMemoryTaskQueue, JobPayload, JobStatus, JobType, TaskQueueRecord,
    fallback_queue_time_estimate,
};
use serde_json::json;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[derive(Clone, Default)]
pub(crate) struct RuntimeTaskmanInspectorHandle {
    shared_queue: Arc<Mutex<Option<Arc<InMemoryTaskQueue>>>>,
    worker_counts: Arc<Mutex<BTreeMap<String, i32>>>,
}

impl RuntimeTaskmanInspectorHandle {
    pub(crate) fn is_configured(&self) -> bool {
        self.records().is_some()
    }

    pub(crate) fn set_shared_queue(
        &self,
        queue: Arc<InMemoryTaskQueue>,
        worker_counts: BTreeMap<String, i32>,
    ) {
        *self
            .shared_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(queue);
        self.worker_counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(worker_counts);
    }

    fn records(&self) -> Option<Vec<RuntimeTaskmanRecord>> {
        // Control jobs ride the same shared queue, so the shared queue is the single
        // source of truth for diagnostics.
        let shared_records = self
            .shared_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|queue| queue.records())?;
        Some(
            shared_records
                .into_iter()
                .map(RuntimeTaskmanRecord::new)
                .collect(),
        )
    }

    fn worker_count(&self, queue_name: &str) -> i32 {
        self.worker_counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(queue_name)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn cancel_job(&self, id: i64) -> Result<(), String> {
        self.shared_queue()?
            .cancel(id, "cancelled by admin", OffsetDateTime::now_utc())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn restart_job(&self, id: i64) -> Result<i64, String> {
        self.shared_queue()?
            .restart(id, OffsetDateTime::now_utc())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn delete_jobs(
        &self,
        filter: RuntimeTaskmanJobsFilter,
    ) -> Result<RuntimeTaskmanDeleteResult, String> {
        let Some(records) = self.records() else {
            return Err("task manager not configured".to_owned());
        };
        let filter = NormalizedTaskmanFilter::try_from(filter)?;
        let mut result = RuntimeTaskmanDeleteResult::default();
        for record in records {
            if !filter.matches(&record) {
                continue;
            }
            result.matched += 1;
            if record.record.status.is_active() {
                result.deleted_active += 1;
            }
            self.delete_record(&record)?;
            result.deleted += 1;
        }
        Ok(result)
    }

    fn delete_record(&self, record: &RuntimeTaskmanRecord) -> Result<(), String> {
        self.shared_queue()?
            .delete(record.diagnostic_id)
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn shared_queue(&self) -> Result<Arc<InMemoryTaskQueue>, String> {
        self.shared_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
            .ok_or_else(|| "task manager not configured".to_owned())
    }
}

#[derive(Clone, Debug)]
struct RuntimeTaskmanRecord {
    diagnostic_id: i64,
    record: TaskQueueRecord,
}

impl RuntimeTaskmanRecord {
    fn new(record: TaskQueueRecord) -> Self {
        Self {
            diagnostic_id: record.id,
            record,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeTaskmanDeleteResult {
    pub(crate) matched: i32,
    pub(crate) deleted: i32,
    pub(crate) deleted_active: i32,
    pub(crate) skipped_active: i32,
}

impl RuntimeTaskmanInspector for RuntimeTaskmanInspectorHandle {
    fn list_jobs(
        &self,
        filter: RuntimeTaskmanJobsFilter,
    ) -> Result<RuntimeTaskmanJobListResultData, String> {
        let Some(mut records) = self.records() else {
            return Ok(empty_taskman_job_list());
        };
        let filter = NormalizedTaskmanFilter::try_from(filter)?;
        records.retain(|record| filter.matches(record));

        let total = records.len() as i32;
        let summary = taskman_summary(&records);
        sort_taskman_records(&mut records, &filter.sort_by, &filter.sort_dir);

        let offset = filter.offset.min(total.max(0)) as usize;
        let limit = filter.limit as usize;
        let items = records
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(taskman_list_entry_from_record)
            .collect();

        Ok(RuntimeTaskmanJobListResultData {
            total,
            offset: filter.offset,
            limit: filter.limit,
            summary,
            items,
        })
    }

    fn job(&self, id: i64) -> Result<Option<RuntimeTaskmanJobDetailsData>, String> {
        let Some(record) = self.shared_queue().ok().and_then(|queue| queue.record(id)) else {
            return Ok(None);
        };
        let record = RuntimeTaskmanRecord::new(record);
        Ok(Some(RuntimeTaskmanJobDetailsData {
            job: taskman_job_from_record(&record),
            messages: taskman_messages_from_record(&record),
            events: taskman_events_from_record(&record),
        }))
    }

    fn queue_diagnostics(
        &self,
        queues: Vec<String>,
        priority: i32,
    ) -> Result<RuntimeTaskmanDiagnosticsData, String> {
        let Some(records) = self.records() else {
            return Ok(RuntimeTaskmanDiagnosticsData::default());
        };
        let queue_names = taskman_queue_names(&records, queues);
        let now = OffsetDateTime::now_utc();
        let active = records
            .iter()
            .filter(|record| record.record.status == JobStatus::Processing)
            .count() as i32;
        let started1m = records
            .iter()
            .filter(|record| recent(record.record.started_at, now))
            .count() as i32;
        let completed1m = records
            .iter()
            .filter(|record| recent(record.record.completed_at, now))
            .count() as i32;
        let queues = queue_names
            .into_iter()
            .map(|queue_name| {
                let pending_or_higher = count_pending_or_higher(&records, &queue_name, priority);
                RuntimeTaskmanQueueDiagnosticsData {
                    pending: count_pending_exact(&records, &queue_name, priority),
                    pending_or_higher,
                    active: count_processing(&records, &queue_name),
                    worker_count: self.worker_count(&queue_name),
                    eta_seconds: fallback_eta_seconds(&queue_name, pending_or_higher),
                    priority,
                    queue_name,
                }
            })
            .collect::<Vec<_>>();
        let worker_count = queues.iter().map(|queue| queue.worker_count).sum();

        Ok(RuntimeTaskmanDiagnosticsData {
            running: true,
            active,
            started1m,
            completed1m,
            worker_count,
            queue_signal_count: 0,
            slow_job_count: 0,
            queues,
        })
    }
}

#[derive(Clone, Debug)]
struct NormalizedTaskmanFilter {
    q: String,
    status: BTreeSet<String>,
    queue: BTreeSet<String>,
    user_id: Option<i64>,
    chat_id: Option<i64>,
    time_field: String,
    from: Option<OffsetDateTime>,
    to: Option<OffsetDateTime>,
    sort_by: String,
    sort_dir: String,
    offset: i32,
    limit: i32,
}

impl TryFrom<RuntimeTaskmanJobsFilter> for NormalizedTaskmanFilter {
    type Error = String;

    fn try_from(filter: RuntimeTaskmanJobsFilter) -> Result<Self, Self::Error> {
        let time_field = defaulted_choice(
            &filter.time_field,
            "created_at",
            &["created_at", "started_at", "completed_at"],
            "invalid time_field",
        )?;
        let sort_by = defaulted_choice(
            &filter.sort_by,
            "created_at",
            &["id", "priority", "created_at", "started_at", "completed_at"],
            "invalid sort_by",
        )?;
        let sort_dir = defaulted_choice(
            &filter.sort_dir,
            "desc",
            &["asc", "desc"],
            "invalid sort_dir",
        )?;
        let status = filter
            .status
            .into_iter()
            .map(|status| {
                let status = status.trim().to_owned();
                if matches!(
                    status.as_str(),
                    "pending" | "processing" | "completed" | "failed" | "cancelled"
                ) {
                    Ok(status)
                } else {
                    Err("invalid status".to_owned())
                }
            })
            .collect::<Result<BTreeSet<_>, _>>()?;

        Ok(Self {
            q: filter.q.trim().to_lowercase(),
            status,
            queue: filter
                .queue
                .into_iter()
                .map(|queue| queue.trim().to_owned())
                .filter(|queue| !queue.is_empty())
                .collect(),
            user_id: filter.user_id,
            chat_id: filter.chat_id,
            time_field,
            from: parse_optional_time(&filter.from, "invalid from")?,
            to: parse_optional_time(&filter.to, "invalid to")?,
            sort_by,
            sort_dir,
            offset: filter.offset.max(0),
            limit: if filter.limit <= 0 {
                200
            } else {
                filter.limit.min(1000)
            },
        })
    }
}

impl NormalizedTaskmanFilter {
    fn matches(&self, record: &RuntimeTaskmanRecord) -> bool {
        if !self.status.is_empty() && !self.status.contains(record.record.status.as_str()) {
            return false;
        }
        if !self.queue.is_empty() && !self.queue.contains(&record.record.queue_name) {
            return false;
        }
        if self
            .user_id
            .is_some_and(|user_id| user_id != record_user_id(record))
        {
            return false;
        }
        if self
            .chat_id
            .is_some_and(|chat_id| chat_id != record_chat_id(record))
        {
            return false;
        }
        if !self.matches_time(record) {
            return false;
        }
        self.q.is_empty() || taskman_search_haystack(record).contains(&self.q)
    }

    fn matches_time(&self, record: &RuntimeTaskmanRecord) -> bool {
        if self.from.is_none() && self.to.is_none() {
            return true;
        }
        let Some(value) = record_time_field(record, &self.time_field) else {
            return false;
        };
        if self.from.is_some_and(|from| value < from) {
            return false;
        }
        self.to.is_none_or(|to| value <= to)
    }
}

fn empty_taskman_job_list() -> RuntimeTaskmanJobListResultData {
    RuntimeTaskmanJobListResultData {
        total: 0,
        offset: 0,
        limit: 0,
        summary: RuntimeTaskmanJobSummaryData {
            by_status: json!({}),
            by_queue: json!({}),
        },
        items: Vec::new(),
    }
}

fn defaulted_choice(
    value: &str,
    default: &str,
    allowed: &[&str],
    error: &str,
) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(default.to_owned());
    }
    if allowed.contains(&value) {
        Ok(value.to_owned())
    } else {
        Err(error.to_owned())
    }
}

fn parse_optional_time(value: &str, error: &str) -> Result<Option<OffsetDateTime>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .map(Some)
        .map_err(|_| error.to_owned())
}

fn taskman_summary(records: &[RuntimeTaskmanRecord]) -> RuntimeTaskmanJobSummaryData {
    let mut by_status = BTreeMap::<String, i32>::new();
    let mut by_queue = BTreeMap::<String, i32>::new();
    for record in records {
        *by_status
            .entry(record.record.status.as_str().to_owned())
            .or_default() += 1;
        *by_queue
            .entry(record.record.queue_name.clone())
            .or_default() += 1;
    }
    RuntimeTaskmanJobSummaryData {
        by_status: json!(by_status),
        by_queue: json!(by_queue),
    }
}

fn sort_taskman_records(records: &mut [RuntimeTaskmanRecord], sort_by: &str, sort_dir: &str) {
    records.sort_by(|left, right| compare_taskman_records(left, right, sort_by, sort_dir));
}

fn compare_taskman_records(
    left: &RuntimeTaskmanRecord,
    right: &RuntimeTaskmanRecord,
    sort_by: &str,
    sort_dir: &str,
) -> Ordering {
    match sort_by {
        "id" => compare_i64(left.diagnostic_id, right.diagnostic_id, sort_dir),
        "priority" => compare_i32_then_id(
            left.record.job.priority,
            right.record.job.priority,
            left.diagnostic_id,
            right.diagnostic_id,
            sort_dir,
        ),
        "started_at" => compare_optional_time(
            left.record.started_at,
            right.record.started_at,
            left.diagnostic_id,
            right.diagnostic_id,
            sort_dir,
        ),
        "completed_at" => compare_optional_time(
            left.record.completed_at,
            right.record.completed_at,
            left.diagnostic_id,
            right.diagnostic_id,
            sort_dir,
        ),
        _ => compare_time(
            left.record.job.created,
            right.record.job.created,
            left.diagnostic_id,
            right.diagnostic_id,
            sort_dir,
        ),
    }
}

fn compare_i32_then_id(
    left: i32,
    right: i32,
    left_id: i64,
    right_id: i64,
    sort_dir: &str,
) -> Ordering {
    match compare_i32(left, right, sort_dir) {
        Ordering::Equal => compare_i64(left_id, right_id, sort_dir),
        ordering => ordering,
    }
}

fn compare_i32(left: i32, right: i32, sort_dir: &str) -> Ordering {
    if sort_dir == "asc" {
        left.cmp(&right)
    } else {
        right.cmp(&left)
    }
}

fn compare_i64(left: i64, right: i64, sort_dir: &str) -> Ordering {
    if sort_dir == "asc" {
        left.cmp(&right)
    } else {
        right.cmp(&left)
    }
}

fn compare_optional_time(
    left: Option<OffsetDateTime>,
    right: Option<OffsetDateTime>,
    left_id: i64,
    right_id: i64,
    sort_dir: &str,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => compare_time(left, right, left_id, right_id, sort_dir),
        (None, None) => compare_i64(left_id, right_id, sort_dir),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
    }
}

fn compare_time(
    left: OffsetDateTime,
    right: OffsetDateTime,
    left_id: i64,
    right_id: i64,
    sort_dir: &str,
) -> Ordering {
    let ordering = if sort_dir == "asc" {
        left.cmp(&right)
    } else {
        right.cmp(&left)
    };
    match ordering {
        Ordering::Equal => compare_i64(left_id, right_id, sort_dir),
        ordering => ordering,
    }
}

fn taskman_list_entry_from_record(record: RuntimeTaskmanRecord) -> RuntimeTaskmanJobListEntryData {
    let telegram = telegram_data(&record);
    let user_id = telegram.map_or(0, |data| data.user_id);
    let chat_id = telegram.map_or(0, |data| data.chat_id);
    let trigger_message_id = telegram.map_or(0, |data| data.message_id);
    let thread_message_id = telegram.and_then(|data| data.thread_message_id);
    let job_type = job_type_name(record.record.job.data.job_type).to_owned();
    let preview = job_preview(&record.record.job.data);
    let created_at = format_time(record.record.job.created);
    let started_at = record.record.started_at.map(format_time);
    let completed_at = record.record.completed_at.map(format_time);
    RuntimeTaskmanJobListEntryData {
        id: record.diagnostic_id,
        queue_name: record.record.queue_name,
        priority: record.record.job.priority,
        title: record.record.job.title,
        job_type,
        status: record.record.status.as_str().to_owned(),
        user_id,
        chat_id,
        trigger_message_id,
        thread_message_id,
        progress_message_id: record.record.progress_message_id,
        queue_position_message_id: record.record.queue_position_message_id,
        result_message_id: record.record.result_message_id,
        worker_id: record.record.worker_id,
        created_at,
        started_at,
        completed_at,
        error_message: record.record.error,
        processing_timeout_seconds: record.record.job.processing_timeout_seconds,
        prompt_hash: None,
        estimated_processing_time: None,
        actual_processing_time: None,
        preview,
    }
}

fn taskman_job_from_record(record: &RuntimeTaskmanRecord) -> RuntimeTaskmanJobData {
    RuntimeTaskmanJobData {
        id: record.diagnostic_id,
        queue_name: record.record.queue_name.clone(),
        priority: record.record.job.priority,
        title: record.record.job.title.clone(),
        payload: serde_json::to_value(&record.record.job.data).ok(),
        status: record.record.status.as_str().to_owned(),
        user_id: telegram_data(record).map_or(0, |data| data.user_id),
        chat_id: telegram_data(record).map_or(0, |data| data.chat_id),
        trigger_message_id: telegram_data(record).map_or(0, |data| data.message_id),
        thread_message_id: telegram_data(record).and_then(|data| data.thread_message_id),
        progress_message_id: record.record.progress_message_id,
        queue_position_message_id: record.record.queue_position_message_id,
        result_message_id: record.record.result_message_id,
        worker_id: record.record.worker_id.clone(),
        created_at: format_time(record.record.job.created),
        started_at: record.record.started_at.map(format_time),
        completed_at: record.record.completed_at.map(format_time),
        error_message: record.record.error.clone(),
        processing_timeout_seconds: record.record.job.processing_timeout_seconds,
        prompt_hash: None,
        estimated_processing_time: None,
        actual_processing_time: None,
    }
}

fn taskman_messages_from_record(
    record: &RuntimeTaskmanRecord,
) -> Vec<openplotva_server::RuntimeTaskmanJobMessageData> {
    let mut messages = record.record.messages.clone();
    messages.sort_by_key(|message| std::cmp::Reverse(message.created_at));
    messages
        .into_iter()
        .map(|message| openplotva_server::RuntimeTaskmanJobMessageData {
            id: message.id,
            job_id: record.diagnostic_id,
            message_type: message.message_type,
            chat_id: message.chat_id,
            message_id: message.message_id,
            created_at: format_time(message.created_at),
            status: message.status,
        })
        .collect()
}

fn taskman_events_from_record(record: &RuntimeTaskmanRecord) -> Option<serde_json::Value> {
    if record.record.events.is_empty() {
        None
    } else {
        serde_json::to_value(&record.record.events).ok()
    }
}

fn format_time(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

fn telegram_data(record: &RuntimeTaskmanRecord) -> Option<&openplotva_taskman::TelegramData> {
    record.record.job.data.telegram_data.as_ref()
}

fn record_user_id(record: &RuntimeTaskmanRecord) -> i64 {
    telegram_data(record).map_or(0, |data| data.user_id)
}

fn record_chat_id(record: &RuntimeTaskmanRecord) -> i64 {
    telegram_data(record).map_or(0, |data| data.chat_id)
}

fn record_time_field(record: &RuntimeTaskmanRecord, field: &str) -> Option<OffsetDateTime> {
    match field {
        "started_at" => record.record.started_at,
        "completed_at" => record.record.completed_at,
        _ => Some(record.record.job.created),
    }
}

fn taskman_search_haystack(record: &RuntimeTaskmanRecord) -> String {
    [
        record.record.queue_name.as_str(),
        record.record.job.title.as_str(),
        job_type_name(record.record.job.data.job_type),
        record.record.status.as_str(),
        job_preview(&record.record.job.data)
            .as_deref()
            .unwrap_or_default(),
        telegram_data(record)
            .map(|data| data.user_full_name.as_str())
            .unwrap_or_default(),
    ]
    .join(" ")
    .to_lowercase()
}

fn job_type_name(job_type: JobType) -> &'static str {
    match job_type {
        JobType::Dialog => "dialog",
        JobType::ImageGen => "image_gen",
        JobType::ImageEdit => "image_edit",
        JobType::MusicGen => "music_gen",
        JobType::Translation => "translation",
        JobType::MemoryConsolidation => "memory_consolidation",
        JobType::Control => "control",
        JobType::Agent => "agent",
    }
}

fn job_preview(payload: &JobPayload) -> Option<String> {
    match payload.job_type {
        JobType::Control => payload.control_data.as_ref().and_then(|data| {
            first_preview([
                data.text.as_str(),
                control_kind_name(data.kind).unwrap_or_default(),
            ])
        }),
        JobType::ImageGen | JobType::ImageEdit => payload
            .image_data
            .as_ref()
            .and_then(|data| first_preview([data.prompt.as_str(), data.original_text.as_str()])),
        JobType::MusicGen => payload
            .music_data
            .as_ref()
            .and_then(|data| first_preview([data.topic.as_str(), data.lyrics.as_str()])),
        JobType::Dialog | JobType::Translation => payload.dialog_data.as_ref().and_then(|data| {
            first_preview([data.message_text.as_str(), data.original_text.as_str()])
        }),
        JobType::MemoryConsolidation => payload
            .telegram_data
            .as_ref()
            .and_then(|data| first_preview([data.user_full_name.as_str(), ""])),
        JobType::Agent => payload
            .agent_data
            .as_ref()
            .and_then(|data| first_preview([data.goal.as_str(), data.profile_id.as_str()])),
    }
}

fn control_kind_name(kind: openplotva_taskman::ControlKind) -> Option<&'static str> {
    match kind {
        openplotva_taskman::ControlKind::Unknown => None,
        openplotva_taskman::ControlKind::Translate => Some("translate"),
        openplotva_taskman::ControlKind::GroupSettings => Some("group_settings"),
        openplotva_taskman::ControlKind::VipInvoice => Some("vip_invoice"),
        openplotva_taskman::ControlKind::DonateInvoice => Some("donate_invoice"),
        openplotva_taskman::ControlKind::SuccessfulPayment => Some("successful_payment"),
        openplotva_taskman::ControlKind::Checkin => Some("checkin"),
        openplotva_taskman::ControlKind::ChatAdminsSync => Some("chat_admins_sync"),
        openplotva_taskman::ControlKind::ChatMemberSync => Some("chat_member_sync"),
        openplotva_taskman::ControlKind::NewMembersFollowup => Some("new_members_followup"),
    }
}

fn first_preview<const N: usize>(fields: [&str; N]) -> Option<String> {
    fields
        .into_iter()
        .map(str::trim)
        .find(|field| !field.is_empty())
        .map(shrink_preview)
}

fn shrink_preview(value: &str) -> String {
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    trim_preview_runes(&compact, 160)
}

fn trim_preview_runes(value: &str, limit: usize) -> String {
    let Some((index, _)) = value.char_indices().nth(limit) else {
        return value.to_owned();
    };
    format!("{}...", &value[..index])
}

fn taskman_queue_names(records: &[RuntimeTaskmanRecord], queues: Vec<String>) -> Vec<String> {
    let mut queue_names = queues
        .into_iter()
        .map(|queue| queue.trim().to_owned())
        .filter(|queue| !queue.is_empty())
        .collect::<Vec<_>>();
    if queue_names.is_empty() {
        queue_names = records
            .iter()
            .map(|record| record.record.queue_name.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
    }
    if queue_names.is_empty() {
        queue_names.push(CONTROL_QUEUE_NAME.to_owned());
    }
    queue_names
}

fn recent(value: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    value.is_some_and(|value| {
        let age = now - value;
        !age.is_negative() && age <= time::Duration::minutes(1)
    })
}

fn count_pending_exact(records: &[RuntimeTaskmanRecord], queue_name: &str, priority: i32) -> i32 {
    records
        .iter()
        .filter(|record| {
            record.record.queue_name == queue_name
                && record.record.status == JobStatus::Pending
                && record.record.job.priority == priority
        })
        .count() as i32
}

fn count_pending_or_higher(
    records: &[RuntimeTaskmanRecord],
    queue_name: &str,
    priority: i32,
) -> i32 {
    records
        .iter()
        .filter(|record| {
            record.record.queue_name == queue_name
                && record.record.status == JobStatus::Pending
                && record.record.job.priority >= priority
        })
        .count() as i32
}

fn fallback_eta_seconds(queue_name: &str, pending_or_higher: i32) -> i32 {
    let depth = usize::try_from(pending_or_higher.max(0)).unwrap_or(usize::MAX);
    let seconds = fallback_queue_time_estimate(queue_name, depth).as_secs();
    i32::try_from(seconds).unwrap_or(i32::MAX)
}

fn count_processing(records: &[RuntimeTaskmanRecord], queue_name: &str) -> i32 {
    records
        .iter()
        .filter(|record| {
            record.record.queue_name == queue_name && record.record.status == JobStatus::Processing
        })
        .count() as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payments::{PaymentControlJobQueue, PersistentPaymentControlJobQueue};
    use openplotva_taskman::{
        CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, DialogJobParams,
        HIGH_PRIORITY, IMAGE_REGULAR_QUEUE_NAME, ImageGenJobParams, InMemoryTaskQueue,
        MESSAGE_STATUS_COMPLETED, MESSAGE_STATUS_PLACEHOLDER, MESSAGE_TYPE_RESULT, TEXT_QUEUE_NAME,
        TaskQueueJobEvent, TaskQueueJobMessageParams, new_control_job_at, new_dialog_job_at,
        new_image_gen_job_at,
    };

    #[tokio::test]
    async fn runtime_taskman_inspector_lists_live_persistent_control_queue() {
        let shared = Arc::new(InMemoryTaskQueue::new());
        let queue = PersistentPaymentControlJobQueue::from_task_queue((*shared).clone());
        let created = OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339).expect("created");
        let job = new_control_job_at(
            ControlJobParams {
                chat_id: 9,
                message_id: 11,
                user_id: 7,
                user_full_name: "Wave Cut".to_owned(),
                thread_id: Some(12),
                data: ControlJobData {
                    kind: ControlKind::VipInvoice,
                    ..ControlJobData::default()
                },
            },
            created,
        )
        .with_name("vip invoice")
        .with_priority(HIGH_PRIORITY);
        queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, job)
            .await
            .expect("assign job");

        let inspector = RuntimeTaskmanInspectorHandle::default();
        inspector.set_shared_queue(shared, BTreeMap::from([(CONTROL_QUEUE_NAME.to_owned(), 1)]));
        let result = inspector
            .list_jobs(RuntimeTaskmanJobsFilter {
                q: "vip".to_owned(),
                queue: vec![CONTROL_QUEUE_NAME.to_owned()],
                ..RuntimeTaskmanJobsFilter::default()
            })
            .expect("list jobs");

        assert_eq!(result.total, 1);
        assert_eq!(result.limit, 200);
        assert_eq!(result.summary.by_status["pending"], 1);
        assert_eq!(result.items[0].id, 1);
        assert_eq!(result.items[0].user_id, 7);
        assert_eq!(result.items[0].chat_id, 9);
        assert_eq!(result.items[0].trigger_message_id, 11);
        assert_eq!(result.items[0].thread_message_id, Some(12));
        assert_eq!(result.items[0].preview.as_deref(), Some("vip_invoice"));

        let diagnostics = inspector
            .queue_diagnostics(vec![CONTROL_QUEUE_NAME.to_owned()], HIGH_PRIORITY)
            .expect("diagnostics");
        assert!(diagnostics.running);
        assert_eq!(diagnostics.queues[0].pending, 1);
        assert_eq!(diagnostics.queues[0].pending_or_higher, 1);
        assert_eq!(diagnostics.queues[0].worker_count, 1);
        assert_eq!(diagnostics.queues[0].eta_seconds, 30);

        let details = inspector.job(1).expect("job lookup").expect("job");
        assert_eq!(details.job.payload.expect("payload")["type"], "control");
    }

    #[test]
    fn runtime_taskman_inspector_lists_shared_dialog_and_image_queue_records() {
        let queue = Arc::new(InMemoryTaskQueue::new());
        let created = OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339).expect("created");
        let dialog_id = queue.assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(
                DialogJobParams {
                    chat_id: 100,
                    message_id: 10,
                    user_id: 7,
                    user_full_name: "Wave Cut".to_owned(),
                    message_text: "hello".to_owned(),
                    original_text: "hello".to_owned(),
                    meta: json!({}),
                    max_output_tokens: 512,
                    thread_id: None,
                },
                created,
            ),
        );
        let image_id = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: 100,
                    message_id: 11,
                    user_id: 7,
                    user_full_name: "Wave Cut".to_owned(),
                    prompt: "cat".to_owned(),
                    original_text: "draw cat".to_owned(),
                    ..ImageGenJobParams::default()
                },
                created,
            )
            .with_name("image"),
        );
        assert_eq!(dialog_id, 1);
        assert_eq!(image_id, 2);
        queue
            .update_job_messages(image_id, Some(31), Some(32), Some(33))
            .expect("update job messages");
        let older = created - time::Duration::seconds(5);
        let first_message = queue
            .create_job_message(TaskQueueJobMessageParams {
                job_id: image_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 100,
                message_id: 33,
                created_at: older,
                status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
            })
            .expect("first message");
        let second_message = queue
            .create_job_message(TaskQueueJobMessageParams {
                job_id: image_id,
                message_type: MESSAGE_TYPE_RESULT.to_owned(),
                chat_id: 100,
                message_id: 34,
                created_at: created,
                status: MESSAGE_STATUS_COMPLETED.to_owned(),
            })
            .expect("second message");
        queue
            .append_job_event(
                image_id,
                TaskQueueJobEvent {
                    stage: "provider_retry".to_owned(),
                    provider: "aifarm".to_owned(),
                    message: "retrying image".to_owned(),
                    ..TaskQueueJobEvent::default()
                },
                created,
            )
            .expect("append event");

        let inspector = RuntimeTaskmanInspectorHandle::default();
        inspector.set_shared_queue(
            Arc::clone(&queue),
            BTreeMap::from([
                (TEXT_QUEUE_NAME.to_owned(), 1),
                (IMAGE_REGULAR_QUEUE_NAME.to_owned(), 1),
            ]),
        );

        let result = inspector
            .list_jobs(RuntimeTaskmanJobsFilter {
                queue: vec![
                    TEXT_QUEUE_NAME.to_owned(),
                    IMAGE_REGULAR_QUEUE_NAME.to_owned(),
                ],
                sort_by: "id".to_owned(),
                sort_dir: "asc".to_owned(),
                ..RuntimeTaskmanJobsFilter::default()
            })
            .expect("list jobs");
        assert_eq!(result.total, 2);
        assert_eq!(result.summary.by_queue[TEXT_QUEUE_NAME], 1);
        assert_eq!(result.summary.by_queue[IMAGE_REGULAR_QUEUE_NAME], 1);
        assert_eq!(result.items[0].id, dialog_id);
        assert_eq!(result.items[0].queue_name, TEXT_QUEUE_NAME);
        assert_eq!(result.items[0].job_type, "dialog");
        assert_eq!(result.items[0].processing_timeout_seconds, 0);
        assert_eq!(result.items[0].preview.as_deref(), Some("hello"));
        assert_eq!(result.items[1].id, image_id);
        assert_eq!(result.items[1].queue_name, IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(result.items[1].job_type, "image_gen");
        assert_eq!(result.items[1].preview.as_deref(), Some("cat"));

        let diagnostics = inspector
            .queue_diagnostics(
                vec![
                    TEXT_QUEUE_NAME.to_owned(),
                    IMAGE_REGULAR_QUEUE_NAME.to_owned(),
                ],
                0,
            )
            .expect("diagnostics");
        assert_eq!(diagnostics.worker_count, 2);
        assert_eq!(diagnostics.queues[0].queue_name, TEXT_QUEUE_NAME);
        assert_eq!(diagnostics.queues[0].worker_count, 1);
        assert_eq!(diagnostics.queues[0].eta_seconds, 30);
        assert_eq!(diagnostics.queues[1].queue_name, IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(diagnostics.queues[1].worker_count, 1);
        assert_eq!(diagnostics.queues[1].eta_seconds, 48);

        let details = inspector
            .job(image_id)
            .expect("job lookup")
            .expect("shared job");
        assert_eq!(details.job.id, image_id);
        assert_eq!(details.job.queue_name, IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(details.job.progress_message_id, Some(31));
        assert_eq!(details.job.queue_position_message_id, Some(32));
        assert_eq!(details.job.result_message_id, Some(33));
        assert_eq!(details.messages.len(), 2);
        assert_eq!(details.messages[0].id, second_message);
        assert_eq!(details.messages[0].job_id, image_id);
        assert_eq!(details.messages[0].message_id, 34);
        assert_eq!(details.messages[0].status, MESSAGE_STATUS_COMPLETED);
        assert_eq!(details.messages[1].id, first_message);
        let events = details.events.expect("events");
        assert_eq!(events[0]["stage"], "provider_retry");
        assert_eq!(events[0]["provider"], "aifarm");
    }

    #[tokio::test]
    async fn runtime_taskman_inspector_uses_one_manager_id_namespace() {
        let shared_queue = Arc::new(InMemoryTaskQueue::new());
        let control_queue =
            PersistentPaymentControlJobQueue::from_task_queue((*shared_queue).clone());
        let created = OffsetDateTime::parse("2026-05-21T10:00:00Z", &Rfc3339).expect("created");
        let control_job = new_control_job_at(
            ControlJobParams {
                chat_id: 9,
                message_id: 11,
                user_id: 7,
                user_full_name: "Wave Cut".to_owned(),
                thread_id: None,
                data: ControlJobData {
                    kind: ControlKind::VipInvoice,
                    ..ControlJobData::default()
                },
            },
            created,
        );
        control_queue
            .assign_payment_control_job(CONTROL_QUEUE_NAME, control_job)
            .await
            .expect("assign control job");

        let shared_id = shared_queue.assign(
            TEXT_QUEUE_NAME,
            new_dialog_job_at(
                DialogJobParams {
                    chat_id: 100,
                    message_id: 10,
                    user_id: 7,
                    user_full_name: "Wave Cut".to_owned(),
                    message_text: "hello".to_owned(),
                    original_text: "hello".to_owned(),
                    meta: json!({}),
                    max_output_tokens: 512,
                    thread_id: None,
                },
                created,
            ),
        );

        let inspector = RuntimeTaskmanInspectorHandle::default();
        inspector.set_shared_queue(
            Arc::clone(&shared_queue),
            BTreeMap::from([
                (TEXT_QUEUE_NAME.to_owned(), 1),
                (CONTROL_QUEUE_NAME.to_owned(), 1),
            ]),
        );

        let result = inspector
            .list_jobs(RuntimeTaskmanJobsFilter {
                sort_by: "id".to_owned(),
                sort_dir: "asc".to_owned(),
                ..RuntimeTaskmanJobsFilter::default()
            })
            .expect("list jobs");
        assert_eq!(
            result.items.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(shared_id, 2);

        let restarted = inspector.restart_job(shared_id).expect("restart shared");
        assert_eq!(restarted, 3);
        assert!(inspector.job(restarted).expect("lookup").is_some());
    }
}
