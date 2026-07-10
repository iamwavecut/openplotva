//! Observable and recoverable background task orchestration.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use serde::{Deserialize, Serialize};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "taskman";

pub const CONTROL_QUEUE_NAME: &str = "control";

pub type Priority = i32;

pub const DEFAULT_PRIORITY: Priority = 0;
pub const HIGH_PRIORITY: Priority = 2;
pub const HIGHEST_PRIORITY: Priority = 4;
pub const ASR_PRIORITY: Priority = HIGHEST_PRIORITY + 2;
pub const LOW_PRIORITY: Priority = -2;
pub const LOWEST_PRIORITY: Priority = -4;

pub const TEXT_QUEUE_NAME: &str = "text";
pub const IMAGE_REGULAR_QUEUE_NAME: &str = "image-regular";
pub const IMAGE_VIP_QUEUE_NAME: &str = "image-vip";
pub const MUSIC_VIP_QUEUE_NAME: &str = "music-vip";
pub const ASR_GPU1_QUEUE_NAME: &str = "asr-gpu1";
pub const DIALOG_AIFARM_QUEUE_NAME: &str = "dialog-aifarm";
pub const MEMORY_CONSOLIDATION_QUEUE_NAME: &str = "memory-consolidation";
/// Dedicated queue for agent-loop runs routed to the single-slot Qwen reasoner.
pub const AGENT_QWEN_QUEUE_NAME: &str = "agent-qwen";

pub const LLM_JOB_RETRY_STAGE: &str = "llm_job_retry";
pub const LLM_JOB_RETRY_EXHAUSTED_STAGE: &str = "llm_job_retry_exhausted";
pub const DEFAULT_LLM_JOB_MAX_ATTEMPTS: i32 = 5;
pub const ASR_PROCESSING_TIMEOUT_SECONDS: i32 = 180;
/// Audit-only event stage written once per committed agent step.
pub const AGENT_STEP_STAGE: &str = "agent_step";
/// Serialized agent-state codec marker stored in `AgentRunStateBlob.format`.
pub const AGENT_RUN_STATE_FORMAT: &str = "openplotva.agent-run-state.v1+json";

/// Version marker for the approved Rust-native taskman snapshot codec.
pub const TASK_QUEUE_SNAPSHOT_FORMAT: &str = "openplotva.taskman-queue.v1+json";
/// Version marker for the approved Rust-native taskman WAL codec.
pub const TASK_QUEUE_WAL_FORMAT: &str = "openplotva.taskman-queue-wal.v1+jsonl";
/// Rust-native WAL operation that upserts one full job record.
pub const TASK_QUEUE_WAL_UPSERT_JOB: &str = "upsert_job";
/// Rust-native WAL operation that deletes one job record.
pub const TASK_QUEUE_WAL_DELETE_JOB: &str = "delete_job";
pub const MAX_JOB_EVENTS: usize = 400;
pub const MESSAGE_TYPE_RESULT: &str = "result";
pub const MESSAGE_STATUS_PLACEHOLDER: &str = "placeholder";
pub const MESSAGE_STATUS_COMPLETED: &str = "completed";
pub const MESSAGE_STATUS_FAILED: &str = "failed";
pub const STUCK_JOB_ERROR_MESSAGE: &str = "Job stuck in processing state for too long";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    Dialog,
    Asr,
    ImageGen,
    ImageEdit,
    MusicGen,
    Translation,
    MemoryConsolidation,
    Control,
    Agent,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Job waits in its queue.
    #[default]
    Pending,
    /// Job has been dequeued by a worker.
    Processing,
    /// Job completed without executor failure.
    Completed,
    /// Job failed and carries an error string.
    Failed,
    /// Job was cancelled before completion.
    Cancelled,
}

impl JobStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Processing)
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    #[default]
    #[serde(rename = "")]
    Unknown,
    Translate,
    GroupSettings,
    VipInvoice,
    DonateInvoice,
    SuccessfulPayment,
    Checkin,
    ChatAdminsSync,
    ChatMemberSync,
    NewMembersFollowup,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobPayload {
    #[serde(rename = "type")]
    pub job_type: JobType,
    /// Telegram routing metadata used by task executors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub telegram_data: Option<TelegramData>,
    /// Image-job-specific payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_data: Option<ImageJobData>,
    /// Music-job-specific payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub music_data: Option<MusicJobData>,
    /// Dialog-job-specific payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialog_data: Option<DialogJobData>,
    /// Voice-transcription-specific payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asr_data: Option<AsrJobData>,
    /// Control-job-specific payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_data: Option<ControlJobData>,
    /// Agent-loop-job-specific payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_data: Option<AgentJobData>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct AsrJobData {
    pub file_unique_id: String,
    pub duration_seconds: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TelegramData {
    pub chat_id: i64,
    pub user_id: i64,
    pub message_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_message_id: Option<i32>,
    pub user_full_name: String,
    pub chat_title: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct ImageJobData {
    pub original_text: String,
    pub author: String,
    pub width: i32,
    pub height: i32,
    pub prompt: String,
    #[serde(default = "empty_json_object")]
    pub meta: serde_json::Value,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub negative_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aspect_ratio: Option<[i32; 2]>,
    #[serde(skip_serializing_if = "is_default_i32")]
    pub seed: i32,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub image_file_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub image_urls: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    pub is_image_edit: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub prompt_variants: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    pub is_nsfw: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub raw_negative_prompt: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub raw_aspect_ratio: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub raw_seed: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct MusicJobData {
    pub topic: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub lyrics: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub style: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub vocal_language: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reference_file_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reference_file_unique_id: String,
    #[serde(default = "empty_json_object")]
    pub meta: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct DialogJobData {
    pub message_text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub original_text: String,
    #[serde(default = "empty_json_object")]
    pub meta: serde_json::Value,
    #[serde(skip_serializing_if = "is_default_i32")]
    pub max_output_tokens: i32,
}

impl Default for ImageJobData {
    fn default() -> Self {
        Self {
            original_text: String::new(),
            author: String::new(),
            width: 0,
            height: 0,
            prompt: String::new(),
            meta: empty_json_object(),
            negative_prompt: String::new(),
            aspect_ratio: None,
            seed: 0,
            image_file_id: String::new(),
            image_urls: Vec::new(),
            is_image_edit: false,
            prompt_variants: Vec::new(),
            is_nsfw: false,
            raw_negative_prompt: String::new(),
            raw_aspect_ratio: String::new(),
            raw_seed: String::new(),
        }
    }
}

impl Default for MusicJobData {
    fn default() -> Self {
        Self {
            topic: String::new(),
            lyrics: String::new(),
            style: String::new(),
            vocal_language: String::new(),
            reference_file_id: String::new(),
            reference_file_unique_id: String::new(),
            meta: empty_json_object(),
        }
    }
}

impl Default for DialogJobData {
    fn default() -> Self {
        Self {
            message_text: String::new(),
            original_text: String::new(),
            meta: empty_json_object(),
            max_output_tokens: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct AgentJobData {
    /// Agent profile id (e.g. "search") resolved against the configured registry.
    pub profile_id: String,
    /// Natural-language goal the agent loop must satisfy.
    pub goal: String,
    /// Reasoner provider name; empty means resolve from the profile config.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reasoner_provider: String,
    /// Writer provider name; empty means resolve from the profile config.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub writer_provider: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct ControlJobData {
    pub kind: ControlKind,
    #[serde(skip_serializing_if = "is_default_i64")]
    pub amount: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub target_lang: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub theme: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub reply_text: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub chat_type: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub user_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub first_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub last_name: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub language_code: String,
    #[serde(skip_serializing_if = "is_false")]
    pub is_premium: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub callback_query_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub new_chat_member_ids: Vec<i64>,
    #[serde(skip_serializing_if = "is_false")]
    pub bot_was_added: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment: Option<ControlPayment>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct ControlPayment {
    pub currency: String,
    pub total_amount: i32,
    pub invoice_payload: String,
    pub telegram_payment_charge_id: String,
    pub provider_payment_charge_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_period_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_expiration_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_recurring: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_first_recurring: Option<bool>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub thread_id: Option<i32>,
    pub data: ControlJobData,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DialogJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub message_text: String,
    pub original_text: String,
    pub meta: serde_json::Value,
    pub max_output_tokens: i32,
    pub thread_id: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AsrJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub thread_id: Option<i32>,
    pub file_unique_id: String,
    pub duration_seconds: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AgentJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub thread_id: Option<i32>,
    pub profile_id: String,
    pub goal: String,
    pub reasoner_provider: String,
    pub writer_provider: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ImageGenJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub prompt: String,
    pub original_text: String,
    pub meta: serde_json::Value,
    pub prompt_variants: Vec<String>,
    pub is_nsfw: bool,
    pub negative_prompt: String,
    pub aspect_ratio: String,
    pub seed: String,
    pub thread_id: Option<i32>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageEditJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub prompt: String,
    pub photo_file_id: String,
    pub photo_urls: Vec<String>,
    pub thread_id: Option<i32>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MusicGenJobParams {
    pub chat_id: i64,
    pub message_id: i32,
    pub user_id: i64,
    pub user_full_name: String,
    pub topic: String,
    pub lyrics: String,
    pub style: String,
    pub vocal_language: String,
    pub reference_file_id: String,
    pub reference_file_unique_id: String,
    pub meta: serde_json::Value,
    pub thread_id: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StatelessJobItem {
    pub title: String,
    pub created: OffsetDateTime,
    pub priority: Priority,
    #[serde(default, skip_serializing_if = "is_default_i32")]
    pub processing_timeout_seconds: i32,
    pub data: JobPayload,
}

/// Durable, opaque-to-taskman checkpoint of an agent run, persisted on the job
/// record and journaled by the existing WAL upsert path. `state` is the serialized
/// engine `AgentState` JSON; taskman never interprets it.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AgentRunStateBlob {
    pub format: String,
    pub committed_step: i32,
    pub state: String,
    pub resume_count: i32,
}

/// Durable Rust-native taskman row used by the generic queue core.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskQueueRecord {
    /// Taskman job ID.
    pub id: i64,
    /// Queue name, such as `control`, `text`, or `dialog-aifarm`.
    pub queue_name: String,
    pub status: JobStatus,
    pub job: StatelessJobItem,
    /// Worker ID that dequeued this job.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    /// Processing start time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_started_at: Option<OffsetDateTime>,
    /// Completion/failure/cancellation time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<OffsetDateTime>,
    /// Failure or cancellation reason.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Telegram progress placeholder message ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress_message_id: Option<i32>,
    /// Telegram result message ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_message_id: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<TaskQueueJobMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<TaskQueueJobEvent>,
    /// Durable agent-run checkpoint for `JobType::Agent` rows; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_state: Option<AgentRunStateBlob>,
}

/// One pending image job's Telegram coordinates and queue-position placeholder,
/// used to keep the "waiting in queue" message current as the queue drains.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingImageQueueEntry {
    pub job_id: i64,
    pub chat_id: i64,
    pub thread_message_id: Option<i32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskQueueJobMessage {
    pub id: i64,
    pub job_id: i64,
    pub message_type: String,
    pub chat_id: i64,
    pub message_id: i32,
    pub created_at: OffsetDateTime,
    pub status: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskQueueJobMessageParams {
    pub job_id: i64,
    pub message_type: String,
    pub chat_id: i64,
    pub message_id: i32,
    pub created_at: OffsetDateTime,
    pub status: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskQueueJobEvent {
    pub at: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub level: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stage: String,
    #[serde(default, skip_serializing_if = "is_default_i32")]
    pub attempt: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub data: BTreeMap<String, String>,
}

/// Work item dequeued by a worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskQueueWorkItem {
    /// Taskman job ID.
    pub id: i64,
    pub job: StatelessJobItem,
    pub events: Vec<TaskQueueJobEvent>,
}

/// Work item for the agent worker: like `TaskQueueWorkItem` plus the durable
/// checkpoint and whether this claim resumed an orphaned run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskQueueAgentWorkItem {
    pub id: i64,
    pub job: StatelessJobItem,
    pub events: Vec<TaskQueueJobEvent>,
    pub agent_state: Option<AgentRunStateBlob>,
    pub resumed: bool,
}

/// Outcome of `release_orphaned_processing_for_startup`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StartupReleaseReport {
    /// Non-agent jobs requeued to `Pending`.
    pub requeued: usize,
    /// Agent jobs kept `Processing` for resume (worker_id cleared).
    pub agent_kept: usize,
}

/// Versioned Rust-native snapshot of the generic taskman queue.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskQueueSnapshot {
    /// Snapshot codec marker.
    pub format: String,
    /// Next taskman job ID to assign after restore.
    pub next_id: i64,
    /// Next taskman message ID to assign after restore.
    #[serde(default = "one_i64")]
    pub next_message_id: i64,
    /// ID-ordered queue records.
    pub records: Vec<TaskQueueRecord>,
}

/// One Rust-native taskman WAL line.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskQueueWalRecord {
    /// WAL codec marker.
    pub format: String,
    /// Operation name.
    pub op: String,
    /// Taskman job ID affected by this line.
    pub job_id: i64,
    /// Full job record for upsert operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record: Option<TaskQueueRecord>,
}

/// Append-only journal boundary used by durable queue runtimes.
pub trait TaskQueueWalSink: Send + Sync {
    /// Append one Rust-native WAL line. Implementations may log and drop write failures.
    fn append_task_queue_wal_record(&self, record: &TaskQueueWalRecord);
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskQueueStatus {
    pub regular_queue_depth: usize,
    pub vip_queue_depth: usize,
    /// Active regular + VIP image jobs.
    pub active_jobs_count: usize,
    /// Active regular + VIP image jobs for the requesting user.
    pub user_active_jobs: usize,
    pub user_pending_jobs: usize,
    /// Estimated wait for the regular image queue.
    pub estimated_wait: std::time::Duration,
}

impl TaskQueueStatus {
    #[must_use]
    pub fn go_estimated_wait_string(&self) -> String {
        go_duration_string(self.estimated_wait)
    }
}

/// Error returned by the generic taskman queue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskQueueError {
    /// Requeue targeted an empty queue name.
    QueueNameRequired,
    /// Status mutation targeted a missing job ID.
    JobNotFound(i64),
    /// Assignment would exceed the active per-user job limit.
    UserActiveLimitReached {
        /// Queue name where the limit was checked.
        queue_name: String,
        /// User ID counted for active jobs.
        user_id: i64,
    },
}

/// Error returned while decoding the Rust-native taskman snapshot.
#[derive(Debug)]
pub enum TaskQueueSnapshotError {
    /// JSON decoding failed.
    Json(serde_json::Error),
    /// Snapshot uses another codec family.
    UnsupportedFormat(String),
}

impl fmt::Display for TaskQueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueNameRequired => write!(f, "queue name is required"),
            Self::JobNotFound(job_id) => write!(f, "taskman job {job_id} not found"),
            Self::UserActiveLimitReached {
                queue_name,
                user_id,
            } => write!(
                f,
                "active taskman job limit reached for queue {queue_name} and user {user_id}"
            ),
        }
    }
}

impl Error for TaskQueueError {}

impl fmt::Display for TaskQueueSnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(error) => write!(f, "decode taskman queue snapshot: {error}"),
            Self::UnsupportedFormat(format) => {
                write!(f, "unsupported taskman queue snapshot format: {format}")
            }
        }
    }
}

impl Error for TaskQueueSnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            Self::UnsupportedFormat(_) => None,
        }
    }
}

impl From<serde_json::Error> for TaskQueueSnapshotError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

#[derive(Clone, Debug)]
struct TaskQueueState {
    next_id: i64,
    next_message_id: i64,
    records: Vec<TaskQueueRecord>,
    worker_heartbeats: BTreeMap<String, OffsetDateTime>,
}

#[derive(Clone, Debug)]
struct TaskQueueIdAllocatorState {
    next_id: i64,
    next_message_id: i64,
}

/// Shared taskman ID allocator for a runtime-owned manager namespace.
///
/// queues can share this allocator to preserve one externally visible ID space
/// without coupling their persistence formats.
#[derive(Clone, Debug)]
pub struct TaskQueueIdAllocator {
    state: Arc<Mutex<TaskQueueIdAllocatorState>>,
}

impl TaskQueueIdAllocator {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TaskQueueIdAllocatorState {
                next_id: 1,
                next_message_id: 1,
            })),
        }
    }

    /// Advance allocator cursors so restored snapshot records stay unique.
    pub fn reserve_snapshot(&self, snapshot: &TaskQueueSnapshot) {
        let next_id_from_records = snapshot
            .records
            .iter()
            .map(|record| record.id.saturating_add(1))
            .max()
            .unwrap_or(1);
        let next_message_id_from_records = snapshot
            .records
            .iter()
            .flat_map(|record| {
                record
                    .messages
                    .iter()
                    .map(|message| message.id.saturating_add(1))
            })
            .max()
            .unwrap_or(1);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.next_id = state
            .next_id
            .max(snapshot.next_id)
            .max(next_id_from_records)
            .max(1);
        state.next_message_id = state
            .next_message_id
            .max(snapshot.next_message_id)
            .max(next_message_id_from_records)
            .max(1);
    }

    /// Raise the allocator high-water to at least the given next ids. Seeded from
    /// the durable Postgres id sequences on startup so issued ids never regress
    /// below a value handed out before a restart, even after rows were purged.
    pub fn seed_high_water(&self, next_id: i64, next_message_id: i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.next_id = state.next_id.max(next_id).max(1);
        state.next_message_id = state.next_message_id.max(next_message_id).max(1);
    }

    fn allocate_job_id(&self) -> (i64, i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = state.next_id.max(1);
        state.next_id = id.saturating_add(1).max(1);
        (id, state.next_id)
    }

    fn allocate_message_id(&self) -> (i64, i64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = state.next_message_id.max(1);
        state.next_message_id = id.saturating_add(1).max(1);
        (id, state.next_message_id)
    }

    fn next_ids(&self) -> (i64, i64) {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (state.next_id.max(1), state.next_message_id.max(1))
    }
}

impl Default for TaskQueueIdAllocator {
    fn default() -> Self {
        Self::new()
    }
}

/// Rust-native in-memory taskman queue core.
///
/// then older creation time, then lower job ID; pending/processing are active
/// for per-user limits; processing jobs can be requeued on startup.
#[derive(Clone)]
pub struct InMemoryTaskQueue {
    state: Arc<Mutex<TaskQueueState>>,
    ids: Option<TaskQueueIdAllocator>,
    journal: Option<Arc<dyn TaskQueueWalSink>>,
}

impl fmt::Debug for InMemoryTaskQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryTaskQueue")
            .field("state", &self.state)
            .field("ids", &self.ids)
            .field("journal", &self.journal.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl InMemoryTaskQueue {
    /// Build an empty taskman queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(TaskQueueState {
                next_id: 1,
                next_message_id: 1,
                records: Vec::new(),
                worker_heartbeats: BTreeMap::new(),
            })),
            ids: None,
            journal: None,
        }
    }

    /// Build an empty taskman queue with a Rust-native WAL sink.
    #[must_use]
    pub fn new_with_journal(journal: Arc<dyn TaskQueueWalSink>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TaskQueueState {
                next_id: 1,
                next_message_id: 1,
                records: Vec::new(),
                worker_heartbeats: BTreeMap::new(),
            })),
            ids: None,
            journal: Some(journal),
        }
    }

    /// Build an empty taskman queue using a shared manager ID namespace.
    #[must_use]
    pub fn new_with_id_allocator(ids: TaskQueueIdAllocator) -> Self {
        let (next_id, next_message_id) = ids.next_ids();
        Self {
            state: Arc::new(Mutex::new(TaskQueueState {
                next_id,
                next_message_id,
                records: Vec::new(),
                worker_heartbeats: BTreeMap::new(),
            })),
            ids: Some(ids),
            journal: None,
        }
    }

    /// Build an empty taskman queue using a shared ID namespace and WAL sink.
    #[must_use]
    pub fn new_with_id_allocator_and_journal(
        ids: TaskQueueIdAllocator,
        journal: Arc<dyn TaskQueueWalSink>,
    ) -> Self {
        let (next_id, next_message_id) = ids.next_ids();
        Self {
            state: Arc::new(Mutex::new(TaskQueueState {
                next_id,
                next_message_id,
                records: Vec::new(),
                worker_heartbeats: BTreeMap::new(),
            })),
            ids: Some(ids),
            journal: Some(journal),
        }
    }

    /// Restore a queue from a decoded Rust-native snapshot.
    #[must_use]
    pub fn from_snapshot(snapshot: TaskQueueSnapshot) -> Self {
        Self::from_snapshot_inner(snapshot, None, None)
    }

    /// Restore a queue from a snapshot with a Rust-native WAL sink.
    #[must_use]
    pub fn from_snapshot_with_journal(
        snapshot: TaskQueueSnapshot,
        journal: Arc<dyn TaskQueueWalSink>,
    ) -> Self {
        Self::from_snapshot_inner(snapshot, None, Some(journal))
    }

    /// Restore a queue from a snapshot using a shared manager ID namespace.
    #[must_use]
    pub fn from_snapshot_with_id_allocator(
        snapshot: TaskQueueSnapshot,
        ids: TaskQueueIdAllocator,
    ) -> Self {
        ids.reserve_snapshot(&snapshot);
        Self::from_snapshot_inner(snapshot, Some(ids), None)
    }

    /// Restore a queue from a snapshot using a shared manager ID namespace and WAL sink.
    #[must_use]
    pub fn from_snapshot_with_id_allocator_and_journal(
        snapshot: TaskQueueSnapshot,
        ids: TaskQueueIdAllocator,
        journal: Arc<dyn TaskQueueWalSink>,
    ) -> Self {
        ids.reserve_snapshot(&snapshot);
        Self::from_snapshot_inner(snapshot, Some(ids), Some(journal))
    }

    fn from_snapshot_inner(
        snapshot: TaskQueueSnapshot,
        ids: Option<TaskQueueIdAllocator>,
        journal: Option<Arc<dyn TaskQueueWalSink>>,
    ) -> Self {
        let next_id_from_records = snapshot
            .records
            .iter()
            .map(|record| record.id.saturating_add(1))
            .max()
            .unwrap_or(1);
        let next_message_id_from_records = snapshot
            .records
            .iter()
            .flat_map(|record| {
                record
                    .messages
                    .iter()
                    .map(|message| message.id.saturating_add(1))
            })
            .max()
            .unwrap_or(1);
        let (shared_next_id, shared_next_message_id) = ids
            .as_ref()
            .map(TaskQueueIdAllocator::next_ids)
            .unwrap_or((1, 1));
        Self {
            state: Arc::new(Mutex::new(TaskQueueState {
                next_id: snapshot
                    .next_id
                    .max(next_id_from_records)
                    .max(shared_next_id)
                    .max(1),
                next_message_id: snapshot
                    .next_message_id
                    .max(next_message_id_from_records)
                    .max(shared_next_message_id)
                    .max(1),
                records: snapshot.records,
                worker_heartbeats: BTreeMap::new(),
            })),
            ids,
            journal,
        }
    }

    /// Assign a job to a named queue and return its taskman ID.
    pub fn assign(&self, queue_name: impl Into<String>, job: StatelessJobItem) -> i64 {
        let mut state = self.lock();
        let id = self.next_job_id(&mut state);
        let record = TaskQueueRecord {
            id,
            queue_name: queue_name.into(),
            status: JobStatus::Pending,
            job,
            worker_id: None,
            started_at: None,
            execution_started_at: None,
            completed_at: None,
            error: None,
            progress_message_id: None,
            result_message_id: None,
            messages: Vec::new(),
            events: Vec::new(),
            agent_state: None,
        };
        state.records.push(record.clone());
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        id
    }

    /// Assign a job only when the user has fewer than `max_active_jobs` active jobs.
    pub fn assign_with_user_limit(
        &self,
        queue_name: impl Into<String>,
        job: StatelessJobItem,
        max_active_jobs: usize,
    ) -> Result<Option<i64>, TaskQueueError> {
        let queue_name = queue_name.into();
        if max_active_jobs == 0 {
            return Ok(Some(self.assign(queue_name, job)));
        }
        let user_id = job_user_id(&job);
        if self.user_active_count(&queue_name, user_id) >= max_active_jobs {
            return Ok(None);
        }
        Ok(Some(self.assign(queue_name, job)))
    }

    /// Assign a job with a queue-position message send pending.
    pub fn dequeue(
        &self,
        queue_name: &str,
        worker_id: impl Into<String>,
        started_at: OffsetDateTime,
    ) -> Option<TaskQueueWorkItem> {
        let mut state = self.lock();
        let index = next_pending_index(&state.records, queue_name)?;
        let worker_id = worker_id.into();
        let (id, job, events, record) = {
            let record = &mut state.records[index];
            record.status = JobStatus::Processing;
            record.worker_id = Some(worker_id.clone());
            record.started_at = Some(started_at);
            record.execution_started_at = None;
            record.completed_at = None;
            record.error = None;
            (
                record.id,
                record.job.clone(),
                record.events.clone(),
                record.clone(),
            )
        };
        state.worker_heartbeats.insert(worker_id, started_at);
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Some(TaskQueueWorkItem { id, job, events })
    }

    pub fn dequeue_matching(
        &self,
        queue_name: &str,
        worker_id: impl Into<String>,
        started_at: OffsetDateTime,
        mut predicate: impl FnMut(&StatelessJobItem) -> bool,
    ) -> Option<TaskQueueWorkItem> {
        let mut state = self.lock();
        let index = next_pending_index_matching(&state.records, queue_name, |job| predicate(job))?;
        let worker_id = worker_id.into();
        let (id, job, events, record) = {
            let record = &mut state.records[index];
            record.status = JobStatus::Processing;
            record.worker_id = Some(worker_id.clone());
            record.started_at = Some(started_at);
            record.execution_started_at = None;
            record.completed_at = None;
            record.error = None;
            (
                record.id,
                record.job.clone(),
                record.events.clone(),
                record.clone(),
            )
        };
        state.worker_heartbeats.insert(worker_id, started_at);
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Some(TaskQueueWorkItem { id, job, events })
    }

    /// Claim a fresh `Pending` agent job, or adopt an orphaned `Processing` agent
    /// run (one whose owning worker died and was cleared at startup). Adopting
    /// keeps the durable checkpoint and bumps its resume counter.
    pub fn dequeue_or_adopt_agent(
        &self,
        queue_name: &str,
        worker_id: impl Into<String>,
        started_at: OffsetDateTime,
    ) -> Option<TaskQueueAgentWorkItem> {
        let mut state = self.lock();
        let worker_id = worker_id.into();
        let (index, resumed) = match next_pending_index(&state.records, queue_name) {
            Some(index) => (index, false),
            None => (next_orphaned_agent_index(&state.records, queue_name)?, true),
        };
        let (id, job, events, agent_state, record) = {
            let record = &mut state.records[index];
            record.status = JobStatus::Processing;
            record.worker_id = Some(worker_id.clone());
            if resumed {
                if let Some(blob) = record.agent_state.as_mut() {
                    blob.resume_count += 1;
                }
            } else {
                record.started_at = Some(started_at);
                record.execution_started_at = None;
                record.completed_at = None;
                record.error = None;
            }
            (
                record.id,
                record.job.clone(),
                record.events.clone(),
                record.agent_state.clone(),
                record.clone(),
            )
        };
        state.worker_heartbeats.insert(worker_id, started_at);
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Some(TaskQueueAgentWorkItem {
            id,
            job,
            events,
            agent_state,
            resumed,
        })
    }

    /// Persist an agent-run checkpoint onto the job record. The whole record is
    /// WAL-upserted, so the blob rides the existing durability path.
    pub fn checkpoint_agent_state(
        &self,
        job_id: i64,
        blob: AgentRunStateBlob,
    ) -> Result<(), TaskQueueError> {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        record.agent_state = Some(blob);
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    /// Startup recovery that preserves agent runs: non-agent `Processing` jobs are
    /// requeued to `Pending` (they have no checkpoint), while agent `Processing`
    /// jobs keep their status and checkpoint and only drop the dead `worker_id`,
    /// so a live worker re-adopts and resumes them mid-loop.
    pub fn release_orphaned_processing_for_startup(&self) -> StartupReleaseReport {
        let mut state = self.lock();
        let mut report = StartupReleaseReport::default();
        let mut ids = Vec::new();
        for record in &mut state.records {
            if record.status != JobStatus::Processing {
                continue;
            }
            if is_agent_record(record) {
                record.worker_id = None;
                report.agent_kept += 1;
            } else {
                record.status = JobStatus::Pending;
                record.worker_id = None;
                record.started_at = None;
                record.execution_started_at = None;
                record.completed_at = None;
                record.error = None;
                report.requeued += 1;
            }
            ids.push(record.id);
        }
        let records = records_by_ids(&state.records, &ids);
        self.append_wal_records(records.into_iter().map(task_queue_wal_upsert));
        drop(state);
        report
    }

    /// Mark a job completed.
    pub fn complete(
        &self,
        job_id: i64,
        completed_at: OffsetDateTime,
    ) -> Result<(), TaskQueueError> {
        self.finalize(job_id, JobStatus::Completed, None, completed_at)
    }

    /// Mark a job failed.
    pub fn fail(
        &self,
        job_id: i64,
        error: impl Into<String>,
        completed_at: OffsetDateTime,
    ) -> Result<(), TaskQueueError> {
        self.finalize(job_id, JobStatus::Failed, Some(error.into()), completed_at)
    }

    /// Mark one job cancelled.
    pub fn cancel(
        &self,
        job_id: i64,
        reason: impl Into<String>,
        completed_at: OffsetDateTime,
    ) -> Result<(), TaskQueueError> {
        self.finalize(
            job_id,
            JobStatus::Cancelled,
            Some(reason.into()),
            completed_at,
        )
    }

    /// Move one job back to pending status in a target queue.
    pub fn requeue_job_to_queue(
        &self,
        job_id: i64,
        queue_name: impl Into<String>,
    ) -> Result<(), TaskQueueError> {
        let queue_name = queue_name.into();
        if queue_name.is_empty() {
            return Err(TaskQueueError::QueueNameRequired);
        }
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        record.queue_name = queue_name;
        record.status = JobStatus::Pending;
        record.worker_id = None;
        record.started_at = None;
        record.execution_started_at = None;
        record.completed_at = None;
        record.error = None;
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    /// Delete one job and return the removed record.
    pub fn delete(&self, job_id: i64) -> Result<TaskQueueRecord, TaskQueueError> {
        let mut state = self.lock();
        let index = state
            .records
            .iter()
            .position(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        let record = state.records.remove(index);
        self.append_wal_record(task_queue_wal_delete(job_id));
        drop(state);
        Ok(record)
    }

    /// Recreate one job with a fresh ID, preserving queue, priority, title, timeout, and payload.
    pub fn restart(&self, job_id: i64, created_at: OffsetDateTime) -> Result<i64, TaskQueueError> {
        let mut state = self.lock();
        let source = state
            .records
            .iter()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        let mut job = source.job.clone();
        let queue_name = source.queue_name.clone();
        job.created = created_at;

        let id = self.next_job_id(&mut state);
        let record = TaskQueueRecord {
            id,
            queue_name,
            status: JobStatus::Pending,
            job,
            worker_id: None,
            started_at: None,
            execution_started_at: None,
            completed_at: None,
            error: None,
            progress_message_id: None,
            result_message_id: None,
            messages: Vec::new(),
            events: Vec::new(),
            agent_state: None,
        };
        state.records.push(record.clone());
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(id)
    }

    /// Return one queue record by ID.
    #[must_use]
    pub fn record(&self, job_id: i64) -> Option<TaskQueueRecord> {
        self.lock()
            .records
            .iter()
            .find(|record| record.id == job_id)
            .cloned()
    }

    /// Cancel all active jobs for one user/chat pair.
    pub fn cancel_user_jobs(
        &self,
        user_id: i64,
        chat_id: i64,
        reason: impl Into<String>,
        completed_at: OffsetDateTime,
    ) -> Vec<i64> {
        let reason = reason.into();
        let mut state = self.lock();
        let mut cancelled = Vec::new();
        let mut records = Vec::new();
        for record in &mut state.records {
            if !record.status.is_active() || job_user_id(&record.job) != user_id {
                continue;
            }
            if job_chat_id(&record.job) != chat_id {
                continue;
            }
            record.status = JobStatus::Cancelled;
            record.completed_at = Some(completed_at);
            record.error = Some(reason.clone());
            cancelled.push(record.id);
            records.push(record.clone());
        }
        self.append_wal_records(records.into_iter().map(task_queue_wal_upsert));
        drop(state);
        cancelled
    }

    /// Requeue processing jobs during startup recovery.
    pub fn requeue_processing_for_startup(&self) -> usize {
        let mut state = self.lock();
        let ids = state
            .records
            .iter()
            .filter(|record| record.status == JobStatus::Processing)
            .map(|record| record.id)
            .collect::<Vec<_>>();
        let requeued = requeue_processing_records(&mut state.records);
        let records = records_by_ids(&state.records, &ids);
        self.append_wal_records(records.into_iter().map(task_queue_wal_upsert));
        drop(state);
        requeued
    }

    pub fn set_execution_started(
        &self,
        job_id: i64,
        started_at: OffsetDateTime,
    ) -> Result<bool, TaskQueueError> {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        if record.execution_started_at.is_some() {
            return Ok(false);
        }
        record.execution_started_at = Some(started_at);
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(true)
    }

    pub fn requeue_expired_processing(&self, now: OffsetDateTime) -> Vec<i64> {
        let mut state = self.lock();
        let ids = requeue_expired_processing_records(&mut state.records, now);
        let records = records_by_ids(&state.records, &ids);
        self.append_wal_records(records.into_iter().map(task_queue_wal_upsert));
        drop(state);
        ids
    }

    /// Mark processing jobs stuck for too long as failed.
    pub fn fail_stuck_processing(
        &self,
        now: OffsetDateTime,
        stuck_duration: std::time::Duration,
    ) -> Vec<i64> {
        let mut state = self.lock();
        let ids = fail_stuck_processing_records(&mut state.records, now, stuck_duration);
        let records = records_by_ids(&state.records, &ids);
        self.append_wal_records(records.into_iter().map(task_queue_wal_upsert));
        drop(state);
        ids
    }

    pub fn prune_terminal_before(&self, cutoff: OffsetDateTime) -> Vec<i64> {
        let mut state = self.lock();
        let mut ids = Vec::new();
        state.records.retain(|record| {
            let prune = task_queue_record_terminal_before(record, cutoff);
            if prune {
                ids.push(record.id);
            }
            !prune
        });
        self.append_wal_records(ids.iter().copied().map(task_queue_wal_delete));
        drop(state);
        ids
    }

    /// Update one worker heartbeat timestamp.
    pub fn update_worker_heartbeat(&self, worker_id: impl Into<String>, at: OffsetDateTime) {
        self.lock().worker_heartbeats.insert(worker_id.into(), at);
    }

    #[must_use]
    pub fn active_workers(&self) -> BTreeMap<String, i32> {
        self.lock()
            .worker_heartbeats
            .keys()
            .map(|worker_id| (worker_id.clone(), 1))
            .collect()
    }

    /// Return the last heartbeat timestamp for one worker.
    #[must_use]
    pub fn worker_heartbeat_at(&self, worker_id: &str) -> Option<OffsetDateTime> {
        self.lock().worker_heartbeats.get(worker_id).copied()
    }

    #[must_use]
    pub fn active_worker_count_for_queue(&self, queue_name: &str) -> usize {
        let prefix = format!("{queue_name}-worker-");
        self.lock()
            .worker_heartbeats
            .keys()
            .filter(|worker_id| worker_id.starts_with(&prefix))
            .count()
            .max(1)
    }

    /// Count pending jobs with exactly this priority.
    #[must_use]
    pub fn queue_depth(&self, queue_name: &str, priority: Priority) -> usize {
        self.lock()
            .records
            .iter()
            .filter(|record| {
                record.queue_name == queue_name
                    && record.status == JobStatus::Pending
                    && record.job.priority == priority
            })
            .count()
    }

    /// Count pending jobs with this priority or higher.
    #[must_use]
    pub fn queue_depth_for_priority_or_higher(
        &self,
        queue_name: &str,
        priority: Priority,
    ) -> usize {
        self.lock()
            .records
            .iter()
            .filter(|record| {
                record.queue_name == queue_name
                    && record.status == JobStatus::Pending
                    && record.job.priority >= priority
            })
            .count()
    }

    /// Count active jobs in one queue.
    #[must_use]
    pub fn active_count(&self, queue_name: &str) -> usize {
        self.lock()
            .records
            .iter()
            .filter(|record| record.queue_name == queue_name && record.status.is_active())
            .count()
    }

    /// Find one active job in a queue without cloning the full taskman snapshot.
    #[must_use]
    pub fn active_job_id_matching(
        &self,
        queue_name: &str,
        mut predicate: impl FnMut(&StatelessJobItem) -> bool,
    ) -> Option<i64> {
        self.lock()
            .records
            .iter()
            .find(|record| {
                record.queue_name == queue_name
                    && record.status.is_active()
                    && predicate(&record.job)
            })
            .map(|record| record.id)
    }

    /// Count active jobs in one queue for one user.
    #[must_use]
    pub fn user_active_count(&self, queue_name: &str, user_id: i64) -> usize {
        self.lock()
            .records
            .iter()
            .filter(|record| {
                record.queue_name == queue_name
                    && record.status.is_active()
                    && job_user_id(&record.job) == user_id
            })
            .count()
    }

    #[must_use]
    pub fn image_queue_status(&self, user_id: i64) -> TaskQueueStatus {
        let regular_queue_depth = self.queue_depth(IMAGE_REGULAR_QUEUE_NAME, DEFAULT_PRIORITY);
        let vip_queue_depth = self.queue_depth(IMAGE_VIP_QUEUE_NAME, DEFAULT_PRIORITY);
        let active_regular = self.active_count(IMAGE_REGULAR_QUEUE_NAME);
        let active_vip = self.active_count(IMAGE_VIP_QUEUE_NAME);
        let user_active_regular = self.user_active_count(IMAGE_REGULAR_QUEUE_NAME, user_id);
        let user_active_vip = self.user_active_count(IMAGE_VIP_QUEUE_NAME, user_id);
        let user_active_jobs = user_active_regular + user_active_vip;
        let pending_regular =
            self.queue_depth_for_priority_or_higher(IMAGE_REGULAR_QUEUE_NAME, LOWEST_PRIORITY);
        let pending_vip =
            self.queue_depth_for_priority_or_higher(IMAGE_VIP_QUEUE_NAME, LOWEST_PRIORITY);
        let user_pending_jobs = if (regular_queue_depth > 0 || vip_queue_depth > 0)
            && (pending_regular > 0 || pending_vip > 0)
        {
            user_active_jobs
        } else {
            0
        };
        let estimated_wait = if regular_queue_depth == 0 {
            std::time::Duration::ZERO
        } else {
            fallback_queue_time_estimate(IMAGE_REGULAR_QUEUE_NAME, regular_queue_depth)
        };
        let estimated_wait = if estimated_wait < std::time::Duration::from_secs(1) {
            std::time::Duration::ZERO
        } else {
            estimated_wait
        };

        TaskQueueStatus {
            regular_queue_depth,
            vip_queue_depth,
            active_jobs_count: active_regular + active_vip,
            user_active_jobs,
            user_pending_jobs,
            estimated_wait,
        }
    }

    pub fn update_pending_dialog_job_by_message_id(
        &self,
        chat_id: i64,
        message_id: i32,
        message_text: &str,
        original_text: &str,
        meta: serde_json::Value,
    ) -> bool {
        let mut state = self.lock();
        let mut updated = None;
        for record in &mut state.records {
            if !pending_record_matches_message(record, chat_id, message_id) {
                continue;
            }
            if record.job.data.job_type != JobType::Dialog {
                continue;
            }
            let Some(dialog_data) = record.job.data.dialog_data.as_mut() else {
                continue;
            };
            dialog_data.message_text = clean_unicode_non_printables(message_text);
            dialog_data.original_text = original_text.trim().to_owned();
            dialog_data.meta = meta;
            updated = Some(record.clone());
            break;
        }
        if let Some(record) = updated {
            self.append_wal_record(task_queue_wal_upsert(record));
            drop(state);
            true
        } else {
            false
        }
    }

    pub fn update_pending_image_job_by_message_id(
        &self,
        chat_id: i64,
        message_id: i32,
        prompt: &str,
        original_text: &str,
        meta: serde_json::Value,
    ) -> bool {
        let mut state = self.lock();
        let mut updated = None;
        for record in &mut state.records {
            if !pending_record_matches_message(record, chat_id, message_id) {
                continue;
            }
            if record.job.data.job_type != JobType::ImageGen {
                continue;
            }
            let Some(image_data) = record.job.data.image_data.as_mut() else {
                continue;
            };
            image_data.prompt = clean_unicode_non_printables(prompt);
            image_data.original_text = clean_unicode_non_printables(original_text.trim());
            image_data.meta = meta;
            image_data.prompt_variants.clear();
            updated = Some(record.clone());
            break;
        }
        if let Some(record) = updated {
            self.append_wal_record(task_queue_wal_upsert(record));
            drop(state);
            true
        } else {
            false
        }
    }

    pub fn set_job_image_urls(
        &self,
        job_id: i64,
        image_urls: Vec<String>,
    ) -> Result<(), TaskQueueError> {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        if let Some(image_data) = record.job.data.image_data.as_mut() {
            image_data.image_urls = image_urls;
        }
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    pub fn update_job_messages(
        &self,
        job_id: i64,
        progress_message_id: Option<i32>,
        result_message_id: Option<i32>,
    ) -> Result<(), TaskQueueError> {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        record.progress_message_id = progress_message_id;
        record.result_message_id = result_message_id;
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    /// Pending image jobs on one queue in dispatch order (front of queue first).
    /// The `ahead` count for entry `i` is exactly `i`.
    #[must_use]
    pub fn pending_image_queue_entries(&self, queue_name: &str) -> Vec<PendingImageQueueEntry> {
        let state = self.lock();
        let mut pending: Vec<&TaskQueueRecord> = state
            .records
            .iter()
            .filter(|record| {
                record.queue_name == queue_name
                    && record.status == JobStatus::Pending
                    && matches!(
                        record.job.data.job_type,
                        JobType::ImageGen | JobType::ImageEdit
                    )
            })
            .collect();
        pending.sort_by(|left, right| compare_go_queue_records(left, right));
        pending
            .into_iter()
            .map(|record| {
                let telegram = record.job.data.telegram_data.as_ref();
                PendingImageQueueEntry {
                    job_id: record.id,
                    chat_id: telegram.map_or(0, |telegram| telegram.chat_id),
                    thread_message_id: telegram.and_then(|telegram| telegram.thread_message_id),
                }
            })
            .collect()
    }

    pub fn update_job_result_message(
        &self,
        job_id: i64,
        result_message_id: Option<i32>,
    ) -> Result<(), TaskQueueError> {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        record.result_message_id = result_message_id;
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    pub fn create_job_message(
        &self,
        params: TaskQueueJobMessageParams,
    ) -> Result<i64, TaskQueueError> {
        let mut state = self.lock();
        let record_index = state
            .records
            .iter()
            .position(|record| record.id == params.job_id)
            .ok_or(TaskQueueError::JobNotFound(params.job_id))?;
        let id = self.next_message_id(&mut state);
        let record = &mut state.records[record_index];
        record.messages.push(TaskQueueJobMessage {
            id,
            job_id: params.job_id,
            message_type: params.message_type,
            chat_id: params.chat_id,
            message_id: params.message_id,
            created_at: params.created_at,
            status: params.status,
        });
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(id)
    }

    #[must_use]
    pub fn job_messages(&self, job_id: i64) -> Vec<TaskQueueJobMessage> {
        let mut messages = self
            .lock()
            .records
            .iter()
            .find(|record| record.id == job_id)
            .map(|record| record.messages.clone())
            .unwrap_or_default();
        sort_job_messages_newest_first(&mut messages);
        messages
    }

    pub fn update_job_message_status(
        &self,
        message_id: i64,
        status: impl Into<String>,
    ) -> Result<(), TaskQueueError> {
        let status = status.into();
        let mut state = self.lock();
        let mut updated = None;
        for record in &mut state.records {
            if let Some(message) = record
                .messages
                .iter_mut()
                .find(|message| message.id == message_id)
            {
                message.status = status;
                updated = Some(record.clone());
                break;
            }
        }
        if let Some(record) = updated {
            self.append_wal_record(task_queue_wal_upsert(record));
            drop(state);
            Ok(())
        } else {
            Err(TaskQueueError::JobNotFound(message_id))
        }
    }

    #[must_use]
    pub fn stale_placeholder_messages(
        &self,
        now: OffsetDateTime,
        max_age: std::time::Duration,
    ) -> Vec<TaskQueueJobMessage> {
        let state = self.lock();
        let max_age = TimeDuration::seconds(max_age.as_secs().min(i64::MAX as u64) as i64);
        state
            .records
            .iter()
            .flat_map(|record| record.messages.iter())
            .filter(|message| message.status == MESSAGE_STATUS_PLACEHOLDER)
            .filter(|message| {
                message
                    .created_at
                    .checked_add(max_age)
                    .is_some_and(|deadline| deadline < now)
            })
            .cloned()
            .collect()
    }

    /// Delete one taskman message by global message row ID.
    pub fn delete_job_message(&self, message_id: i64) -> bool {
        let mut state = self.lock();
        for record in &mut state.records {
            let Some(index) = record
                .messages
                .iter()
                .position(|message| message.id == message_id)
            else {
                continue;
            };
            record.messages.remove(index);
            let record = record.clone();
            self.append_wal_record(task_queue_wal_upsert(record));
            drop(state);
            return true;
        }
        false
    }

    /// Delete all taskman messages for one job.
    pub fn delete_job_messages_by_job_id(&self, job_id: i64) -> usize {
        let mut state = self.lock();
        let Some(record) = state.records.iter_mut().find(|record| record.id == job_id) else {
            return 0;
        };
        let deleted = record.messages.len();
        record.messages.clear();
        let record = record.clone();
        if deleted > 0 {
            self.append_wal_record(task_queue_wal_upsert(record));
        }
        drop(state);
        deleted
    }

    pub fn append_job_event(
        &self,
        job_id: i64,
        mut event: TaskQueueJobEvent,
        at: OffsetDateTime,
    ) -> Result<(), TaskQueueError> {
        if event.at.trim().is_empty() {
            event.at = format_time(at);
        }
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        record.events.push(event);
        if record.events.len() > MAX_JOB_EVENTS {
            let remove_count = record.events.len() - MAX_JOB_EVENTS;
            record.events.drain(..remove_count);
        }
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    /// Return a stable ID-ordered snapshot of queue records.
    #[must_use]
    pub fn records(&self) -> Vec<TaskQueueRecord> {
        self.lock().records.clone()
    }

    /// Return a versioned Rust-native snapshot.
    #[must_use]
    pub fn snapshot(&self) -> TaskQueueSnapshot {
        let state = self.lock();
        self.snapshot_locked(&state)
    }

    /// Build one snapshot while holding the queue state lock.
    ///
    /// This is intentionally synchronous: durable runtimes use it to save a
    /// snapshot and compact the WAL without allowing a mutation between those
    /// two filesystem operations.
    pub fn with_locked_snapshot<T>(&self, f: impl FnOnce(&TaskQueueSnapshot) -> T) -> T {
        let state = self.lock();
        let snapshot = self.snapshot_locked(&state);
        f(&snapshot)
    }

    fn snapshot_locked(&self, state: &TaskQueueState) -> TaskQueueSnapshot {
        let (next_id, next_message_id) = self
            .ids
            .as_ref()
            .map(TaskQueueIdAllocator::next_ids)
            .unwrap_or((state.next_id, state.next_message_id));
        TaskQueueSnapshot {
            format: TASK_QUEUE_SNAPSHOT_FORMAT.to_owned(),
            next_id,
            next_message_id,
            records: state.records.clone(),
        }
    }

    /// Replay Rust-native WAL lines into the in-memory queue without journaling them again.
    pub fn replay_wal_records(
        &self,
        records: impl IntoIterator<Item = TaskQueueWalRecord>,
    ) -> usize {
        let mut state = self.lock();
        let mut applied = 0;
        for record in records {
            if apply_task_queue_wal_record_locked(&mut state, record) {
                applied += 1;
            }
        }
        applied
    }

    fn next_job_id(&self, state: &mut TaskQueueState) -> i64 {
        if let Some(ids) = &self.ids {
            let (id, next_id) = ids.allocate_job_id();
            state.next_id = next_id;
            id
        } else {
            let id = state.next_id.max(1);
            state.next_id = id.saturating_add(1).max(1);
            id
        }
    }

    fn next_message_id(&self, state: &mut TaskQueueState) -> i64 {
        if let Some(ids) = &self.ids {
            let (id, next_message_id) = ids.allocate_message_id();
            state.next_message_id = next_message_id;
            id
        } else {
            let id = state.next_message_id.max(1);
            state.next_message_id = id.saturating_add(1).max(1);
            id
        }
    }

    fn append_wal_record(&self, record: TaskQueueWalRecord) {
        if let Some(journal) = &self.journal {
            journal.append_task_queue_wal_record(&record);
        }
    }

    fn append_wal_records(&self, records: impl IntoIterator<Item = TaskQueueWalRecord>) {
        if let Some(journal) = &self.journal {
            for record in records {
                journal.append_task_queue_wal_record(&record);
            }
        }
    }

    fn finalize(
        &self,
        job_id: i64,
        status: JobStatus,
        error: Option<String>,
        completed_at: OffsetDateTime,
    ) -> Result<(), TaskQueueError> {
        let mut state = self.lock();
        let record = state
            .records
            .iter_mut()
            .find(|record| record.id == job_id)
            .ok_or(TaskQueueError::JobNotFound(job_id))?;
        record.status = status;
        record.completed_at = Some(completed_at);
        record.error = error;
        let record = record.clone();
        self.append_wal_record(task_queue_wal_upsert(record));
        drop(state);
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, TaskQueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for InMemoryTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlJobPayloadError {
    /// The payload is not a control job.
    InvalidJobType {
        actual: JobType,
    },
    MissingTelegramData,
    MissingControlData,
}

impl fmt::Display for ControlJobPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobType { actual } => {
                write!(f, "invalid job type for control job executor: {actual:?}")
            }
            Self::MissingTelegramData => f.write_str("missing Telegram data for control job"),
            Self::MissingControlData => f.write_str("missing control data for control job"),
        }
    }
}

impl Error for ControlJobPayloadError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DialogJobPayloadError {
    /// The payload is not a dialog job.
    InvalidJobType {
        actual: JobType,
    },
    MissingTelegramData,
    MissingDialogData,
}

impl fmt::Display for DialogJobPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobType { actual } => {
                write!(f, "invalid job type for dialog job executor: {actual:?}")
            }
            Self::MissingTelegramData => f.write_str("missing Telegram data for dialog job"),
            Self::MissingDialogData => f.write_str("missing dialog data for dialog job"),
        }
    }
}

impl Error for DialogJobPayloadError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AsrJobPayloadError {
    InvalidJobType { actual: JobType },
    MissingTelegramData,
    MissingAsrData,
}

impl fmt::Display for AsrJobPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobType { actual } => {
                write!(f, "invalid job type for ASR executor: {actual:?}")
            }
            Self::MissingTelegramData => f.write_str("missing Telegram data for ASR job"),
            Self::MissingAsrData => f.write_str("missing ASR data for ASR job"),
        }
    }
}

impl Error for AsrJobPayloadError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageGenJobPayloadError {
    /// The payload is not an image generation job.
    InvalidJobType {
        actual: JobType,
    },
    MissingTelegramData,
    MissingImageData,
}

impl fmt::Display for ImageGenJobPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobType { actual } => {
                write!(
                    f,
                    "invalid job type for image generation executor: {actual:?}"
                )
            }
            Self::MissingTelegramData => f.write_str("missing Telegram data for image job"),
            Self::MissingImageData => f.write_str("missing image data for image job"),
        }
    }
}

impl Error for ImageGenJobPayloadError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageEditJobPayloadError {
    /// The payload is not an image edit job.
    InvalidJobType {
        actual: JobType,
    },
    MissingTelegramData,
    MissingImageData,
}

impl fmt::Display for ImageEditJobPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobType { actual } => {
                write!(f, "invalid job type for image edit executor: {actual:?}")
            }
            Self::MissingTelegramData => f.write_str("missing Telegram data for image edit job"),
            Self::MissingImageData => f.write_str("missing image data for image edit job"),
        }
    }
}

impl Error for ImageEditJobPayloadError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MusicGenJobPayloadError {
    /// The payload is not a music generation job.
    InvalidJobType {
        actual: JobType,
    },
    MissingTelegramData,
    MissingMusicData,
}

impl fmt::Display for MusicGenJobPayloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobType { actual } => {
                write!(
                    f,
                    "invalid job type for music generation executor: {actual:?}"
                )
            }
            Self::MissingTelegramData => f.write_str("missing Telegram data for music job"),
            Self::MissingMusicData => f.write_str("missing music data for music job"),
        }
    }
}

impl Error for MusicGenJobPayloadError {}

/// Encode a generic taskman queue snapshot as Rust-native JSON bytes.
pub fn encode_task_queue_snapshot(
    snapshot: &TaskQueueSnapshot,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(snapshot)
}

/// Decode a generic taskman queue snapshot from Rust-native JSON bytes.
pub fn decode_task_queue_snapshot(
    bytes: &[u8],
) -> Result<TaskQueueSnapshot, TaskQueueSnapshotError> {
    let snapshot: TaskQueueSnapshot = serde_json::from_slice(bytes)?;
    if snapshot.format != TASK_QUEUE_SNAPSHOT_FORMAT {
        return Err(TaskQueueSnapshotError::UnsupportedFormat(snapshot.format));
    }
    Ok(snapshot)
}

/// Build an empty Rust-native taskman snapshot.
#[must_use]
pub fn empty_task_queue_snapshot() -> TaskQueueSnapshot {
    TaskQueueSnapshot {
        format: TASK_QUEUE_SNAPSHOT_FORMAT.to_owned(),
        next_id: 1,
        next_message_id: 1,
        records: Vec::new(),
    }
}

/// Encode one Rust-native taskman WAL line.
pub fn encode_task_queue_wal_record(
    record: &TaskQueueWalRecord,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(record)
}

/// Decode one Rust-native taskman WAL line.
pub fn decode_task_queue_wal_record(bytes: &[u8]) -> Result<TaskQueueWalRecord, serde_json::Error> {
    serde_json::from_slice(bytes)
}

/// Replay Rust-native WAL lines over a decoded snapshot and return the recovered snapshot.
#[must_use]
pub fn replay_task_queue_wal_records(
    snapshot: TaskQueueSnapshot,
    records: impl IntoIterator<Item = TaskQueueWalRecord>,
) -> TaskQueueSnapshot {
    let queue = InMemoryTaskQueue::from_snapshot(snapshot);
    queue.replay_wal_records(records);
    queue.snapshot()
}

#[must_use]
pub fn new_control_job_at(params: ControlJobParams, created: OffsetDateTime) -> StatelessJobItem {
    StatelessJobItem {
        title: "control".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::Control,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_unicode_non_printables(&params.user_full_name),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: Some(params.data),
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_dialog_job_at(params: DialogJobParams, created: OffsetDateTime) -> StatelessJobItem {
    StatelessJobItem {
        title: "dialog".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::Dialog,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_unicode_non_printables(&params.user_full_name),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: None,
            dialog_data: Some(DialogJobData {
                message_text: clean_unicode_non_printables(&params.message_text),
                original_text: params.original_text.trim().to_owned(),
                meta: non_null_json_object(params.meta),
                max_output_tokens: params.max_output_tokens,
            }),
            asr_data: None,
            control_data: None,
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_dialog_job(params: DialogJobParams) -> StatelessJobItem {
    new_dialog_job_at(params, OffsetDateTime::now_utc())
}

#[must_use]
pub fn new_asr_job_at(params: AsrJobParams, created: OffsetDateTime) -> StatelessJobItem {
    StatelessJobItem {
        title: "asr".to_owned(),
        created,
        priority: ASR_PRIORITY,
        processing_timeout_seconds: ASR_PROCESSING_TIMEOUT_SECONDS,
        data: JobPayload {
            job_type: JobType::Asr,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_unicode_non_printables(&params.user_full_name),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: Some(AsrJobData {
                file_unique_id: params.file_unique_id.trim().to_owned(),
                duration_seconds: params.duration_seconds.max(0),
            }),
            control_data: None,
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_asr_job(params: AsrJobParams) -> StatelessJobItem {
    new_asr_job_at(params, OffsetDateTime::now_utc())
}

#[must_use]
pub fn new_agent_job_at(params: AgentJobParams, created: OffsetDateTime) -> StatelessJobItem {
    StatelessJobItem {
        title: "agent".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::Agent,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_unicode_non_printables(&params.user_full_name),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: Some(AgentJobData {
                profile_id: params.profile_id,
                goal: clean_unicode_non_printables(&params.goal),
                reasoner_provider: params.reasoner_provider,
                writer_provider: params.writer_provider,
            }),
        },
    }
}

#[must_use]
pub fn new_agent_job(params: AgentJobParams) -> StatelessJobItem {
    new_agent_job_at(params, OffsetDateTime::now_utc())
}

#[must_use]
pub fn new_image_gen_job_at(
    params: ImageGenJobParams,
    created: OffsetDateTime,
) -> StatelessJobItem {
    let clean_user_full_name = clean_unicode_non_printables(&params.user_full_name);
    let clean_prompt = clean_unicode_non_printables(&params.prompt);
    let original_text = if params.original_text.trim().is_empty() {
        clean_prompt.clone()
    } else {
        clean_unicode_non_printables(params.original_text.trim())
    };
    StatelessJobItem {
        title: "image_gen".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::ImageGen,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_user_full_name.clone(),
                chat_title: String::new(),
            }),
            image_data: Some(ImageJobData {
                original_text,
                author: clean_user_full_name,
                prompt: clean_prompt,
                meta: non_null_json_object(params.meta),
                prompt_variants: params.prompt_variants,
                is_nsfw: params.is_nsfw,
                raw_negative_prompt: params.negative_prompt,
                raw_aspect_ratio: params.aspect_ratio,
                raw_seed: params.seed,
                ..ImageJobData::default()
            }),
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_image_gen_job(params: ImageGenJobParams) -> StatelessJobItem {
    new_image_gen_job_at(params, OffsetDateTime::now_utc())
}

#[must_use]
pub fn new_image_edit_job_at(
    params: ImageEditJobParams,
    created: OffsetDateTime,
) -> StatelessJobItem {
    let clean_user_full_name = clean_unicode_non_printables(&params.user_full_name);
    let clean_prompt = clean_unicode_non_printables(&params.prompt);
    StatelessJobItem {
        title: "image_edit".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::ImageEdit,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_user_full_name.clone(),
                chat_title: String::new(),
            }),
            image_data: Some(ImageJobData {
                original_text: clean_prompt.clone(),
                author: clean_user_full_name,
                prompt: clean_prompt,
                image_file_id: params.photo_file_id,
                image_urls: params.photo_urls,
                is_image_edit: true,
                ..ImageJobData::default()
            }),
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_image_edit_job(params: ImageEditJobParams) -> StatelessJobItem {
    new_image_edit_job_at(params, OffsetDateTime::now_utc())
}

#[must_use]
pub fn new_music_gen_job_at(
    params: MusicGenJobParams,
    created: OffsetDateTime,
) -> StatelessJobItem {
    let clean_user_full_name = clean_unicode_non_printables(&params.user_full_name);
    let clean_topic = clean_unicode_non_printables(&params.topic);
    StatelessJobItem {
        title: "music_gen".to_owned(),
        created,
        priority: DEFAULT_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::MusicGen,
            telegram_data: Some(TelegramData {
                chat_id: params.chat_id,
                user_id: params.user_id,
                message_id: params.message_id,
                thread_message_id: params.thread_id,
                user_full_name: clean_user_full_name.clone(),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: Some(MusicJobData {
                topic: clean_topic,
                lyrics: clean_unicode_non_printables(&params.lyrics),
                style: clean_unicode_non_printables(&params.style),
                vocal_language: clean_unicode_non_printables(&params.vocal_language),
                reference_file_id: params.reference_file_id.trim().to_owned(),
                reference_file_unique_id: params.reference_file_unique_id.trim().to_owned(),
                meta: non_null_json_object(params.meta),
            }),
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_music_gen_job(params: MusicGenJobParams) -> StatelessJobItem {
    new_music_gen_job_at(params, OffsetDateTime::now_utc())
}

#[must_use]
pub fn new_memory_consolidation_job_at(created: OffsetDateTime) -> StatelessJobItem {
    StatelessJobItem {
        title: "memory_consolidation".to_owned(),
        created,
        priority: LOWEST_PRIORITY,
        processing_timeout_seconds: 0,
        data: JobPayload {
            job_type: JobType::MemoryConsolidation,
            telegram_data: None,
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
        },
    }
}

#[must_use]
pub fn new_memory_consolidation_job() -> StatelessJobItem {
    new_memory_consolidation_job_at(OffsetDateTime::now_utc())
}

pub fn control_job_params_from_stateless_job(
    job: &StatelessJobItem,
) -> Result<ControlJobParams, ControlJobPayloadError> {
    control_job_params_from_payload(&job.data)
}

pub fn control_job_params_from_payload(
    payload: &JobPayload,
) -> Result<ControlJobParams, ControlJobPayloadError> {
    if payload.job_type != JobType::Control {
        return Err(ControlJobPayloadError::InvalidJobType {
            actual: payload.job_type,
        });
    }
    let telegram_data = payload
        .telegram_data
        .as_ref()
        .ok_or(ControlJobPayloadError::MissingTelegramData)?;
    let control_data = payload
        .control_data
        .as_ref()
        .ok_or(ControlJobPayloadError::MissingControlData)?;

    Ok(ControlJobParams {
        chat_id: telegram_data.chat_id,
        message_id: telegram_data.message_id,
        user_id: telegram_data.user_id,
        user_full_name: telegram_data.user_full_name.clone(),
        thread_id: telegram_data.thread_message_id,
        data: control_data.clone(),
    })
}

pub fn image_gen_job_params_from_stateless_job(
    job: &StatelessJobItem,
) -> Result<ImageGenJobParams, ImageGenJobPayloadError> {
    image_gen_job_params_from_payload(&job.data)
}

pub fn image_gen_job_params_from_payload(
    payload: &JobPayload,
) -> Result<ImageGenJobParams, ImageGenJobPayloadError> {
    if payload.job_type != JobType::ImageGen {
        return Err(ImageGenJobPayloadError::InvalidJobType {
            actual: payload.job_type,
        });
    }
    let telegram_data = payload
        .telegram_data
        .as_ref()
        .ok_or(ImageGenJobPayloadError::MissingTelegramData)?;
    let image_data = payload
        .image_data
        .as_ref()
        .ok_or(ImageGenJobPayloadError::MissingImageData)?;

    Ok(ImageGenJobParams {
        chat_id: telegram_data.chat_id,
        message_id: telegram_data.message_id,
        user_id: telegram_data.user_id,
        user_full_name: telegram_data.user_full_name.clone(),
        prompt: image_data.prompt.clone(),
        original_text: image_data.original_text.clone(),
        meta: image_data.meta.clone(),
        prompt_variants: image_data.prompt_variants.clone(),
        is_nsfw: image_data.is_nsfw,
        negative_prompt: image_data.raw_negative_prompt.clone(),
        aspect_ratio: image_data.raw_aspect_ratio.clone(),
        seed: image_data.raw_seed.clone(),
        thread_id: telegram_data.thread_message_id,
    })
}

pub fn image_edit_job_params_from_stateless_job(
    job: &StatelessJobItem,
) -> Result<ImageEditJobParams, ImageEditJobPayloadError> {
    image_edit_job_params_from_payload(&job.data)
}

pub fn image_edit_job_params_from_payload(
    payload: &JobPayload,
) -> Result<ImageEditJobParams, ImageEditJobPayloadError> {
    if payload.job_type != JobType::ImageEdit {
        return Err(ImageEditJobPayloadError::InvalidJobType {
            actual: payload.job_type,
        });
    }
    let telegram_data = payload
        .telegram_data
        .as_ref()
        .ok_or(ImageEditJobPayloadError::MissingTelegramData)?;
    let image_data = payload
        .image_data
        .as_ref()
        .ok_or(ImageEditJobPayloadError::MissingImageData)?;

    Ok(ImageEditJobParams {
        chat_id: telegram_data.chat_id,
        message_id: telegram_data.message_id,
        user_id: telegram_data.user_id,
        user_full_name: telegram_data.user_full_name.clone(),
        prompt: image_data.prompt.clone(),
        photo_file_id: image_data.image_file_id.clone(),
        photo_urls: image_data.image_urls.clone(),
        thread_id: telegram_data.thread_message_id,
    })
}

pub fn music_gen_job_params_from_stateless_job(
    job: &StatelessJobItem,
) -> Result<MusicGenJobParams, MusicGenJobPayloadError> {
    music_gen_job_params_from_payload(&job.data)
}

pub fn music_gen_job_params_from_payload(
    payload: &JobPayload,
) -> Result<MusicGenJobParams, MusicGenJobPayloadError> {
    if payload.job_type != JobType::MusicGen {
        return Err(MusicGenJobPayloadError::InvalidJobType {
            actual: payload.job_type,
        });
    }
    let telegram_data = payload
        .telegram_data
        .as_ref()
        .ok_or(MusicGenJobPayloadError::MissingTelegramData)?;
    let music_data = payload
        .music_data
        .as_ref()
        .ok_or(MusicGenJobPayloadError::MissingMusicData)?;

    Ok(MusicGenJobParams {
        chat_id: telegram_data.chat_id,
        message_id: telegram_data.message_id,
        user_id: telegram_data.user_id,
        user_full_name: telegram_data.user_full_name.clone(),
        topic: music_data.topic.clone(),
        lyrics: music_data.lyrics.clone(),
        style: music_data.style.clone(),
        vocal_language: music_data.vocal_language.clone(),
        reference_file_id: music_data.reference_file_id.clone(),
        reference_file_unique_id: music_data.reference_file_unique_id.clone(),
        meta: music_data.meta.clone(),
        thread_id: telegram_data.thread_message_id,
    })
}

pub fn dialog_job_params_from_stateless_job(
    job: &StatelessJobItem,
) -> Result<DialogJobParams, DialogJobPayloadError> {
    dialog_job_params_from_payload(&job.data)
}

pub fn dialog_job_params_from_payload(
    payload: &JobPayload,
) -> Result<DialogJobParams, DialogJobPayloadError> {
    if payload.job_type != JobType::Dialog {
        return Err(DialogJobPayloadError::InvalidJobType {
            actual: payload.job_type,
        });
    }
    let telegram_data = payload
        .telegram_data
        .as_ref()
        .ok_or(DialogJobPayloadError::MissingTelegramData)?;
    let dialog_data = payload
        .dialog_data
        .as_ref()
        .ok_or(DialogJobPayloadError::MissingDialogData)?;

    Ok(DialogJobParams {
        chat_id: telegram_data.chat_id,
        message_id: telegram_data.message_id,
        user_id: telegram_data.user_id,
        user_full_name: telegram_data.user_full_name.clone(),
        message_text: dialog_data.message_text.clone(),
        original_text: dialog_data.original_text.clone(),
        meta: dialog_data.meta.clone(),
        max_output_tokens: dialog_data.max_output_tokens,
        thread_id: telegram_data.thread_message_id,
    })
}

pub fn asr_job_params_from_stateless_job(
    job: &StatelessJobItem,
) -> Result<AsrJobParams, AsrJobPayloadError> {
    asr_job_params_from_payload(&job.data)
}

pub fn asr_job_params_from_payload(
    payload: &JobPayload,
) -> Result<AsrJobParams, AsrJobPayloadError> {
    if payload.job_type != JobType::Asr {
        return Err(AsrJobPayloadError::InvalidJobType {
            actual: payload.job_type,
        });
    }
    let telegram_data = payload
        .telegram_data
        .as_ref()
        .ok_or(AsrJobPayloadError::MissingTelegramData)?;
    let asr_data = payload
        .asr_data
        .as_ref()
        .ok_or(AsrJobPayloadError::MissingAsrData)?;

    Ok(AsrJobParams {
        chat_id: telegram_data.chat_id,
        message_id: telegram_data.message_id,
        user_id: telegram_data.user_id,
        user_full_name: telegram_data.user_full_name.clone(),
        thread_id: telegram_data.thread_message_id,
        file_unique_id: asr_data.file_unique_id.clone(),
        duration_seconds: asr_data.duration_seconds,
    })
}

impl StatelessJobItem {
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.title = name.into();
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub fn with_processing_timeout_seconds(mut self, timeout_seconds: i32) -> Self {
        if timeout_seconds > 0 {
            self.processing_timeout_seconds = timeout_seconds;
        }
        self
    }

    #[must_use]
    pub fn with_dialog_max_output_tokens(mut self, max_tokens: i32) -> Self {
        if max_tokens > 0
            && let Some(dialog_data) = self.data.dialog_data.as_mut()
        {
            dialog_data.max_output_tokens = max_tokens;
        }
        self
    }
}

fn is_default_i64(value: &i64) -> bool {
    *value == 0
}

const fn one_i64() -> i64 {
    1
}

fn is_default_i32(value: &i32) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn empty_json_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

fn non_null_json_object(value: serde_json::Value) -> serde_json::Value {
    if value.is_null() {
        empty_json_object()
    } else {
        value
    }
}

fn format_time(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

fn sort_job_messages_newest_first(messages: &mut [TaskQueueJobMessage]) {
    messages.sort_by_key(|message| std::cmp::Reverse(message.created_at));
}

fn task_queue_wal_upsert(record: TaskQueueRecord) -> TaskQueueWalRecord {
    TaskQueueWalRecord {
        format: TASK_QUEUE_WAL_FORMAT.to_owned(),
        op: TASK_QUEUE_WAL_UPSERT_JOB.to_owned(),
        job_id: record.id,
        record: Some(record),
    }
}

fn task_queue_wal_delete(job_id: i64) -> TaskQueueWalRecord {
    TaskQueueWalRecord {
        format: TASK_QUEUE_WAL_FORMAT.to_owned(),
        op: TASK_QUEUE_WAL_DELETE_JOB.to_owned(),
        job_id,
        record: None,
    }
}

fn apply_task_queue_wal_record_locked(
    state: &mut TaskQueueState,
    wal_record: TaskQueueWalRecord,
) -> bool {
    if wal_record.format != TASK_QUEUE_WAL_FORMAT {
        return false;
    }
    match wal_record.op.as_str() {
        TASK_QUEUE_WAL_UPSERT_JOB => {
            let Some(record) = wal_record.record else {
                return false;
            };
            upsert_task_queue_record_locked(state, record);
            true
        }
        TASK_QUEUE_WAL_DELETE_JOB => {
            let before = state.records.len();
            state
                .records
                .retain(|record| record.id != wal_record.job_id);
            before != state.records.len()
        }
        _ => false,
    }
}

fn upsert_task_queue_record_locked(state: &mut TaskQueueState, record: TaskQueueRecord) {
    let next_id = record.id.saturating_add(1).max(1);
    let next_message_id = record
        .messages
        .iter()
        .map(|message| message.id.saturating_add(1))
        .max()
        .unwrap_or(1);
    state.next_id = state.next_id.max(next_id);
    state.next_message_id = state.next_message_id.max(next_message_id);
    if let Some(existing) = state
        .records
        .iter_mut()
        .find(|existing| existing.id == record.id)
    {
        *existing = record;
    } else {
        state.records.push(record);
    }
}

fn records_by_ids(records: &[TaskQueueRecord], ids: &[i64]) -> Vec<TaskQueueRecord> {
    records
        .iter()
        .filter(|record| ids.contains(&record.id))
        .cloned()
        .collect()
}

#[must_use]
pub fn clean_unicode_non_printables(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }

    let mut result = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        let Some(ch) = clean_unicode_char(ch) else {
            continue;
        };
        if ch == ' ' || ch == '\t' {
            pending_space = true;
            continue;
        }
        if pending_space {
            result.push(' ');
            pending_space = false;
        }
        result.push(ch);
    }
    result.trim().to_owned()
}

fn clean_unicode_char(ch: char) -> Option<char> {
    match ch {
        '\u{00a0}' | '\u{2000}' | '\u{2001}' | '\u{2002}' | '\u{2003}' | '\u{2004}'
        | '\u{2005}' | '\u{2006}' | '\u{2007}' | '\u{2008}' | '\u{2009}' | '\u{200a}' => Some(' '),
        '\u{200b}' | '\u{200c}' | '\u{200d}' | '\u{200e}' | '\u{200f}' | '\u{feff}' => None,
        _ if ch.is_control() && !matches!(ch, '\t' | '\n' | '\r') => None,
        _ => Some(ch),
    }
}

fn next_pending_index(records: &[TaskQueueRecord], queue_name: &str) -> Option<usize> {
    next_pending_index_matching(records, queue_name, |_| true)
}

fn is_agent_record(record: &TaskQueueRecord) -> bool {
    record.job.data.job_type == JobType::Agent
}

/// Find the best orphaned (worker-less) `Processing` agent job in a queue, so a
/// live worker can re-adopt and resume it after a crash/restart.
fn next_orphaned_agent_index(records: &[TaskQueueRecord], queue_name: &str) -> Option<usize> {
    records
        .iter()
        .enumerate()
        .filter(|(_index, record)| {
            record.queue_name == queue_name
                && record.status == JobStatus::Processing
                && record.worker_id.is_none()
                && is_agent_record(record)
        })
        .min_by(|(_left_index, left), (_right_index, right)| compare_go_queue_records(left, right))
        .map(|(index, _record)| index)
}

fn next_pending_index_matching(
    records: &[TaskQueueRecord],
    queue_name: &str,
    mut predicate: impl FnMut(&StatelessJobItem) -> bool,
) -> Option<usize> {
    let lane_blockers =
        (queue_name == DIALOG_AIFARM_QUEUE_NAME).then(|| dialog_lane_blockers(records));
    records
        .iter()
        .enumerate()
        .filter(|(_index, record)| {
            record.queue_name == queue_name && record.status == JobStatus::Pending
        })
        .filter(|(index, record)| {
            !dialog_lane_blocked(lane_blockers.as_ref(), records, *index, record)
        })
        .filter(|(_index, record)| !image_work_blocked_by_active_asr(records, record))
        .filter(|(_index, record)| predicate(&record.job))
        .min_by(|(_left_index, left), (_right_index, right)| compare_go_queue_records(left, right))
        .map(|(index, _record)| index)
}

/// Preserve GPU-1 priority across separate ASR and draw worker queues. This is
/// evaluated while the queue state is locked, so a draw claim cannot race an
/// already assigned ASR job.
fn image_work_blocked_by_active_asr(
    records: &[TaskQueueRecord],
    candidate: &TaskQueueRecord,
) -> bool {
    if !matches!(
        candidate.job.data.job_type,
        JobType::ImageGen | JobType::ImageEdit
    ) {
        return false;
    }
    records.iter().any(|record| {
        record.status.is_active()
            && record.job.data.job_type == JobType::Asr
            && record.job.priority > candidate.job.priority
    })
}

fn compare_go_queue_records(left: &TaskQueueRecord, right: &TaskQueueRecord) -> std::cmp::Ordering {
    right
        .job
        .priority
        .cmp(&left.job.priority)
        .then_with(|| left.job.created.cmp(&right.job.created))
        .then_with(|| left.id.cmp(&right.id))
}

fn dialog_lane_blocked(
    lane_blockers: Option<&BTreeMap<(i64, i32), DialogLaneBlocker>>,
    records: &[TaskQueueRecord],
    candidate_index: usize,
    candidate: &TaskQueueRecord,
) -> bool {
    if !indexes_dialog_lane(candidate) {
        return false;
    }
    let Some(lane_blockers) = lane_blockers else {
        return false;
    };
    let Some(blocker) = lane_blockers.get(&dialog_lane_key(candidate)) else {
        return false;
    };
    if blocker.processing {
        return true;
    }
    let Some(pending_index) = blocker.best_pending_index else {
        return false;
    };
    pending_index != candidate_index
        && compare_go_queue_records(&records[pending_index], candidate).is_lt()
}

#[derive(Clone, Copy, Debug, Default)]
struct DialogLaneBlocker {
    processing: bool,
    best_pending_index: Option<usize>,
}

fn dialog_lane_blockers(records: &[TaskQueueRecord]) -> BTreeMap<(i64, i32), DialogLaneBlocker> {
    let mut blockers: BTreeMap<(i64, i32), DialogLaneBlocker> = BTreeMap::new();
    for (index, record) in records.iter().enumerate() {
        if !indexes_dialog_lane(record) {
            continue;
        }
        let blocker = blockers.entry(dialog_lane_key(record)).or_default();
        match record.status {
            JobStatus::Processing => blocker.processing = true,
            JobStatus::Pending => {
                let replace = blocker.best_pending_index.is_none_or(|best_index| {
                    compare_go_queue_records(record, &records[best_index]).is_lt()
                });
                if replace {
                    blocker.best_pending_index = Some(index);
                }
            }
            _ => {}
        }
    }
    blockers
}

fn indexes_dialog_lane(record: &TaskQueueRecord) -> bool {
    record.queue_name == DIALOG_AIFARM_QUEUE_NAME
        && record.job.data.job_type == JobType::Dialog
        && record.status.is_active()
}

fn dialog_lane_key(record: &TaskQueueRecord) -> (i64, i32) {
    let Some(telegram) = record.job.data.telegram_data.as_ref() else {
        return (0, 0);
    };
    (
        telegram.chat_id,
        telegram.thread_message_id.unwrap_or_default(),
    )
}

fn requeue_processing_records(records: &mut [TaskQueueRecord]) -> usize {
    let mut requeued = 0;
    for record in records {
        if record.status == JobStatus::Processing {
            record.status = JobStatus::Pending;
            record.worker_id = None;
            record.started_at = None;
            record.execution_started_at = None;
            record.completed_at = None;
            record.error = None;
            requeued += 1;
        }
    }
    requeued
}

fn requeue_expired_processing_records(
    records: &mut [TaskQueueRecord],
    now: OffsetDateTime,
) -> Vec<i64> {
    let mut requeued = Vec::new();
    for record in records {
        if !processing_record_expired(record, now) {
            continue;
        }
        record.status = JobStatus::Pending;
        record.worker_id = None;
        record.started_at = None;
        record.execution_started_at = None;
        record.completed_at = None;
        record.error = None;
        requeued.push(record.id);
    }
    requeued
}

fn processing_record_expired(record: &TaskQueueRecord, now: OffsetDateTime) -> bool {
    if record.status != JobStatus::Processing
        || record.job.processing_timeout_seconds <= 0
        || is_agent_record(record)
    {
        return false;
    }
    let Some(started_at) = processing_record_timeout_start_at(record) else {
        return false;
    };
    started_at
        .checked_add(TimeDuration::seconds(i64::from(
            record.job.processing_timeout_seconds,
        )))
        .is_some_and(|deadline| deadline < now)
}

fn processing_record_timeout_start_at(record: &TaskQueueRecord) -> Option<OffsetDateTime> {
    match record.job.data.job_type {
        JobType::Asr | JobType::ImageGen | JobType::ImageEdit | JobType::MusicGen => {
            record.execution_started_at
        }
        _ => record.started_at,
    }
}

fn fail_stuck_processing_records(
    records: &mut [TaskQueueRecord],
    now: OffsetDateTime,
    stuck_duration: std::time::Duration,
) -> Vec<i64> {
    let mut failed = Vec::new();
    let stuck_duration =
        TimeDuration::seconds(stuck_duration.as_secs().min(i64::MAX as u64) as i64);
    for record in records {
        if record.status != JobStatus::Processing {
            continue;
        }
        if is_agent_record(record) {
            continue;
        }
        let Some(started_at) = processing_record_timeout_start_at(record) else {
            continue;
        };
        let is_stuck = started_at
            .checked_add(stuck_duration)
            .is_some_and(|deadline| deadline < now);
        if !is_stuck {
            continue;
        }
        record.status = JobStatus::Failed;
        record.completed_at = Some(now);
        record.error = Some(STUCK_JOB_ERROR_MESSAGE.to_owned());
        failed.push(record.id);
    }
    failed
}

fn task_queue_record_terminal_before(record: &TaskQueueRecord, cutoff: OffsetDateTime) -> bool {
    if record.status.is_active() {
        return false;
    }
    match record.completed_at {
        Some(completed_at) => completed_at < cutoff,
        None => false,
    }
}

#[must_use]
pub fn fallback_queue_time_estimate(queue_name: &str, depth: usize) -> std::time::Duration {
    if depth == 0 {
        return std::time::Duration::ZERO;
    }
    let base_secs = match queue_name {
        IMAGE_REGULAR_QUEUE_NAME | IMAGE_VIP_QUEUE_NAME | MUSIC_VIP_QUEUE_NAME => 8 * 60,
        CONTROL_QUEUE_NAME | TEXT_QUEUE_NAME => 2 * 60,
        _ => 5 * 60,
    };
    let estimated_secs = (base_secs * depth as u64 / 10).max(30);
    std::time::Duration::from_secs(estimated_secs)
}

fn go_duration_string(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;
    if hours > 0 {
        return format!("{hours}h{minutes}m{secs}s");
    }
    if minutes > 0 {
        return format!("{minutes}m{secs}s");
    }
    format!("{secs}s")
}

#[must_use]
pub fn format_go_duration(duration: std::time::Duration) -> String {
    go_duration_string(duration)
}

fn job_telegram_data(job: &StatelessJobItem) -> Option<&TelegramData> {
    job.data.telegram_data.as_ref()
}

fn job_user_id(job: &StatelessJobItem) -> i64 {
    job_telegram_data(job).map_or(0, |data| data.user_id)
}

fn job_chat_id(job: &StatelessJobItem) -> i64 {
    job_telegram_data(job).map_or(0, |data| data.chat_id)
}

fn pending_record_matches_message(record: &TaskQueueRecord, chat_id: i64, message_id: i32) -> bool {
    if record.status != JobStatus::Pending {
        return false;
    }
    let Some(telegram_data) = record.job.data.telegram_data.as_ref() else {
        return false;
    };
    telegram_data.chat_id == chat_id && telegram_data.message_id == message_id
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use serde_json::json;
    use time::{Duration as TimeDuration, OffsetDateTime};

    use super::{
        AGENT_QWEN_QUEUE_NAME, ASR_GPU1_QUEUE_NAME, ASR_PRIORITY, ASR_PROCESSING_TIMEOUT_SECONDS,
        AgentJobParams, AgentRunStateBlob, AsrJobParams, CONTROL_QUEUE_NAME, ControlJobData,
        ControlJobParams, ControlJobPayloadError, ControlKind, DEFAULT_PRIORITY,
        DIALOG_AIFARM_QUEUE_NAME, DialogJobData, DialogJobParams, DialogJobPayloadError,
        HIGH_PRIORITY, HIGHEST_PRIORITY, IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME,
        ImageEditJobParams, ImageEditJobPayloadError, ImageGenJobParams, ImageGenJobPayloadError,
        ImageJobData, InMemoryTaskQueue, JobPayload, JobStatus, JobType, LOWEST_PRIORITY,
        MAX_JOB_EVENTS, MESSAGE_STATUS_COMPLETED, MESSAGE_STATUS_FAILED,
        MESSAGE_STATUS_PLACEHOLDER, MESSAGE_TYPE_RESULT, MUSIC_VIP_QUEUE_NAME, MusicGenJobParams,
        MusicGenJobPayloadError, PendingImageQueueEntry, STUCK_JOB_ERROR_MESSAGE,
        TASK_QUEUE_SNAPSHOT_FORMAT, TEXT_QUEUE_NAME, TaskQueueError, TaskQueueJobEvent,
        TaskQueueJobMessageParams, TaskQueueWalRecord, TaskQueueWalSink,
        asr_job_params_from_stateless_job, decode_task_queue_snapshot, encode_task_queue_snapshot,
        image_edit_job_params_from_stateless_job, image_gen_job_params_from_stateless_job,
        music_gen_job_params_from_stateless_job, new_agent_job_at, new_asr_job_at,
        new_control_job_at, new_dialog_job_at, new_image_edit_job_at, new_image_gen_job_at,
        new_memory_consolidation_job_at, new_music_gen_job_at, replay_task_queue_wal_records,
    };

    #[derive(Default)]
    struct WalSinkStub {
        records: Mutex<Vec<TaskQueueWalRecord>>,
    }

    impl WalSinkStub {
        fn records(&self) -> Vec<TaskQueueWalRecord> {
            self.records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl TaskQueueWalSink for WalSinkStub {
        fn append_task_queue_wal_record(&self, record: &TaskQueueWalRecord) {
            self.records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(record.clone());
        }
    }

    #[test]
    fn control_job_payload_matches_go_shape_for_vip_invoice_queueing()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_control_job_at(
            ControlJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: " Alice\u{200f}\tSmith ".to_owned(),
                thread_id: Some(40),
                data: ControlJobData {
                    kind: ControlKind::VipInvoice,
                    amount: 300,
                    user_name: "Alice".to_owned(),
                    first_name: "Alice".to_owned(),
                    ..ControlJobData::default()
                },
            },
            created,
        )
        .with_name("vip invoice")
        .with_priority(HIGH_PRIORITY);

        assert_eq!(CONTROL_QUEUE_NAME, "control");
        assert_eq!(job.title, "vip invoice");
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.data.job_type, JobType::Control);
        let telegram_data = job
            .data
            .telegram_data
            .as_ref()
            .expect("control job should include Telegram data");
        assert_eq!(telegram_data.user_full_name, "Alice Smith");
        assert_eq!(telegram_data.thread_message_id, Some(40));

        let payload = serde_json::to_value(&job.data)?;
        assert_eq!(
            payload,
            json!({
                "type": "control",
                "telegram_data": {
                    "chat_id": 10,
                    "user_id": 30,
                    "message_id": 20,
                    "thread_message_id": 40,
                    "user_full_name": "Alice Smith",
                    "chat_title": ""
                },
                "control_data": {
                    "kind": "vip_invoice",
                    "amount": 300,
                    "user_name": "Alice",
                    "first_name": "Alice"
                }
            })
        );
        Ok(())
    }

    #[test]
    fn dialog_job_payload_matches_go_shape_and_builders() -> Result<(), Box<dyn std::error::Error>>
    {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_dialog_job_at(
            DialogJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: " Alice\u{200f}\tSmith ".to_owned(),
                message_text: "\u{200f}hello\tthere".to_owned(),
                original_text: " original text ".to_owned(),
                meta: json!({"type": "text", "annotation": "note"}),
                max_output_tokens: 128,
                thread_id: Some(40),
            },
            created,
        )
        .with_name("dialog trigger")
        .with_priority(HIGH_PRIORITY)
        .with_processing_timeout_seconds(600)
        .with_dialog_max_output_tokens(2048);

        assert_eq!(job.title, "dialog trigger");
        assert_eq!(job.priority, HIGH_PRIORITY);
        assert_eq!(job.processing_timeout_seconds, 600);
        let payload = serde_json::to_value(&job)?;
        assert_eq!(payload["title"], "dialog trigger");
        assert_eq!(payload["priority"], 2);
        assert_eq!(payload["processing_timeout_seconds"], 600);
        assert_eq!(
            payload["data"],
            json!({
                "type": "dialog",
                "telegram_data": {
                    "chat_id": 10,
                    "user_id": 30,
                    "message_id": 20,
                    "thread_message_id": 40,
                    "user_full_name": "Alice Smith",
                    "chat_title": ""
                },
                "dialog_data": {
                    "message_text": "hello there",
                    "original_text": "original text",
                    "meta": {
                        "type": "text",
                        "annotation": "note"
                    },
                    "max_output_tokens": 2048
                }
            })
        );
        Ok(())
    }

    #[test]
    fn dialog_job_params_decode_go_executor_input_from_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_dialog_job_at(
            DialogJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice Smith".to_owned(),
                message_text: "hello".to_owned(),
                original_text: " original ".to_owned(),
                meta: json!({"type": "text"}),
                max_output_tokens: 512,
                thread_id: Some(40),
            },
            created,
        );

        let params = super::dialog_job_params_from_stateless_job(&job)?;

        assert_eq!(
            params,
            DialogJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice Smith".to_owned(),
                message_text: "hello".to_owned(),
                original_text: "original".to_owned(),
                meta: json!({"type": "text"}),
                max_output_tokens: 512,
                thread_id: Some(40),
            }
        );
        Ok(())
    }

    #[test]
    fn image_gen_job_payload_matches_go_shape_for_draw_assignment()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_image_gen_job_at(
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: " Alice\u{200f}\tSmith ".to_owned(),
                prompt: "  neon\u{200f}\tcastle  ".to_owned(),
                original_text: "  neon castle  ".to_owned(),
                meta: json!({
                    "vision_description": "old image",
                    "attachments": [{"kind": "image", "content": "style ref"}],
                }),
                prompt_variants: vec!["variant one".to_owned(), "variant two".to_owned()],
                is_nsfw: true,
                negative_prompt: "blur".to_owned(),
                aspect_ratio: "16:9".to_owned(),
                seed: "42".to_owned(),
                thread_id: Some(40),
            },
            created,
        )
        .with_name("image")
        .with_priority(HIGHEST_PRIORITY);

        assert_eq!(job.title, "image");
        assert_eq!(job.priority, HIGHEST_PRIORITY);
        assert_eq!(job.created, created);
        let payload = serde_json::to_value(&job.data)?;
        assert_eq!(
            payload,
            json!({
                "type": "image_gen",
                "telegram_data": {
                    "chat_id": -100,
                    "user_id": 30,
                    "message_id": 20,
                    "thread_message_id": 40,
                    "user_full_name": "Alice Smith",
                    "chat_title": "",
                },
                "image_data": {
                    "original_text": "neon castle",
                    "author": "Alice Smith",
                    "width": 0,
                    "height": 0,
                    "prompt": "neon castle",
                    "meta": {
                        "vision_description": "old image",
                        "attachments": [{"kind": "image", "content": "style ref"}],
                    },
                    "prompt_variants": ["variant one", "variant two"],
                    "is_nsfw": true,
                    "raw_negative_prompt": "blur",
                    "raw_aspect_ratio": "16:9",
                    "raw_seed": "42",
                },
            })
        );

        let decoded = image_gen_job_params_from_stateless_job(&job)?;
        assert_eq!(decoded.chat_id, -100);
        assert_eq!(decoded.message_id, 20);
        assert_eq!(decoded.user_id, 30);
        assert_eq!(decoded.user_full_name, "Alice Smith");
        assert_eq!(decoded.prompt, "neon castle");
        assert_eq!(decoded.original_text, "neon castle");
        assert_eq!(decoded.prompt_variants, ["variant one", "variant two"]);
        assert!(decoded.is_nsfw);
        assert_eq!(decoded.negative_prompt, "blur");
        assert_eq!(decoded.aspect_ratio, "16:9");
        assert_eq!(decoded.seed, "42");
        assert_eq!(decoded.thread_id, Some(40));

        Ok(())
    }

    #[test]
    fn image_edit_job_payload_matches_go_shape_for_kontext_assignment()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_image_edit_job_at(
            ImageEditJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: " Alice\u{200f}\tSmith ".to_owned(),
                prompt: " make\u{200f}\tit night ".to_owned(),
                photo_file_id: "photo-file".to_owned(),
                photo_urls: vec![
                    "https://telegram.test/a.png".to_owned(),
                    "https://telegram.test/b.png".to_owned(),
                ],
                thread_id: Some(40),
            },
            created,
        )
        .with_name("image_edit")
        .with_priority(HIGHEST_PRIORITY);

        assert_eq!(job.title, "image_edit");
        assert_eq!(job.priority, HIGHEST_PRIORITY);
        assert_eq!(job.created, created);
        let payload = serde_json::to_value(&job.data)?;
        assert_eq!(
            payload,
            json!({
                "type": "image_edit",
                "telegram_data": {
                    "chat_id": -100,
                    "user_id": 30,
                    "message_id": 20,
                    "thread_message_id": 40,
                    "user_full_name": "Alice Smith",
                    "chat_title": "",
                },
                "image_data": {
                    "original_text": "make it night",
                    "author": "Alice Smith",
                    "width": 0,
                    "height": 0,
                    "prompt": "make it night",
                    "meta": {},
                    "image_file_id": "photo-file",
                    "image_urls": [
                        "https://telegram.test/a.png",
                        "https://telegram.test/b.png"
                    ],
                    "is_image_edit": true,
                },
            })
        );

        let decoded = image_edit_job_params_from_stateless_job(&job)?;
        assert_eq!(
            decoded,
            ImageEditJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice Smith".to_owned(),
                prompt: "make it night".to_owned(),
                photo_file_id: "photo-file".to_owned(),
                photo_urls: vec![
                    "https://telegram.test/a.png".to_owned(),
                    "https://telegram.test/b.png".to_owned(),
                ],
                thread_id: Some(40),
            }
        );
        Ok(())
    }

    #[test]
    fn music_gen_job_payload_matches_go_shape_for_vip_assignment()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_music_gen_job_at(
            MusicGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: " Alice\u{200f}\tSmith ".to_owned(),
                topic: " synth\u{200f}\twave ".to_owned(),
                lyrics: "verse".to_owned(),
                style: "electropop".to_owned(),
                vocal_language: "ru".to_owned(),
                reference_file_id: " file-id ".to_owned(),
                reference_file_unique_id: " unique-id ".to_owned(),
                meta: json!({"type": "audio", "annotation": "ref"}),
                thread_id: Some(40),
            },
            created,
        )
        .with_name("music")
        .with_priority(HIGHEST_PRIORITY);

        assert_eq!(MUSIC_VIP_QUEUE_NAME, "music-vip");
        assert_eq!(job.title, "music");
        assert_eq!(job.priority, HIGHEST_PRIORITY);
        assert_eq!(job.created, created);
        let payload = serde_json::to_value(&job.data)?;
        assert_eq!(
            payload,
            json!({
                "type": "music_gen",
                "telegram_data": {
                    "chat_id": -100,
                    "user_id": 30,
                    "message_id": 20,
                    "thread_message_id": 40,
                    "user_full_name": "Alice Smith",
                    "chat_title": "",
                },
                "music_data": {
                    "topic": "synth wave",
                    "lyrics": "verse",
                    "style": "electropop",
                    "vocal_language": "ru",
                    "reference_file_id": "file-id",
                    "reference_file_unique_id": "unique-id",
                    "meta": {"type": "audio", "annotation": "ref"},
                },
            })
        );

        let decoded = music_gen_job_params_from_stateless_job(&job)?;
        assert_eq!(
            decoded,
            MusicGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice Smith".to_owned(),
                topic: "synth wave".to_owned(),
                lyrics: "verse".to_owned(),
                style: "electropop".to_owned(),
                vocal_language: "ru".to_owned(),
                reference_file_id: "file-id".to_owned(),
                reference_file_unique_id: "unique-id".to_owned(),
                meta: json!({"type": "audio", "annotation": "ref"}),
                thread_id: Some(40),
            }
        );
        Ok(())
    }

    #[test]
    fn memory_consolidation_job_matches_go_shape() -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_memory_consolidation_job_at(created);

        assert_eq!(job.title, "memory_consolidation");
        assert_eq!(job.created, created);
        assert_eq!(job.priority, LOWEST_PRIORITY);
        assert_eq!(job.processing_timeout_seconds, 0);
        assert_eq!(job.data.job_type, JobType::MemoryConsolidation);
        assert!(job.data.telegram_data.is_none());
        assert!(job.data.dialog_data.is_none());
        assert!(job.data.image_data.is_none());
        assert!(job.data.music_data.is_none());
        assert!(job.data.control_data.is_none());
        Ok(())
    }

    #[test]
    fn asr_job_payload_is_durable_and_higher_priority_than_draw()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let params = AsrJobParams {
            chat_id: -100,
            message_id: 20,
            user_id: 30,
            user_full_name: "Alice".to_owned(),
            thread_id: Some(7),
            file_unique_id: "voice-u".to_owned(),
            duration_seconds: 4,
        };

        let job = new_asr_job_at(params.clone(), created);

        assert_eq!(job.data.job_type, JobType::Asr);
        assert_eq!(job.priority, ASR_PRIORITY);
        assert!(job.priority > HIGHEST_PRIORITY);
        assert_eq!(
            job.processing_timeout_seconds,
            ASR_PROCESSING_TIMEOUT_SECONDS
        );
        assert_eq!(asr_job_params_from_stateless_job(&job)?, params);
        let payload = serde_json::to_value(&job)?;
        assert_eq!(payload["data"]["type"], "asr");
        assert_eq!(payload["data"]["asr_data"]["file_unique_id"], "voice-u");
        Ok(())
    }

    #[test]
    fn image_gen_job_decoder_rejects_non_image_payload() {
        let job = new_dialog_job_at(
            DialogJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                message_text: "hello".to_owned(),
                original_text: String::new(),
                meta: serde_json::Value::Null,
                max_output_tokens: 0,
                thread_id: None,
            },
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(
            image_gen_job_params_from_stateless_job(&job),
            Err(ImageGenJobPayloadError::InvalidJobType {
                actual: JobType::Dialog,
            })
        );
    }

    #[test]
    fn image_edit_job_decoder_rejects_non_edit_payload() {
        let job = new_image_gen_job_at(
            ImageGenJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "draw".to_owned(),
                ..ImageGenJobParams::default()
            },
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(
            image_edit_job_params_from_stateless_job(&job),
            Err(ImageEditJobPayloadError::InvalidJobType {
                actual: JobType::ImageGen,
            })
        );
    }

    #[test]
    fn music_gen_job_decoder_rejects_non_music_payload() {
        let job = new_image_gen_job_at(
            ImageGenJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "draw".to_owned(),
                ..ImageGenJobParams::default()
            },
            OffsetDateTime::UNIX_EPOCH,
        );

        assert_eq!(
            music_gen_job_params_from_stateless_job(&job),
            Err(MusicGenJobPayloadError::InvalidJobType {
                actual: JobType::ImageGen,
            })
        );
    }

    #[test]
    fn dialog_job_params_reject_missing_go_required_payload_sections() {
        let payload_without_telegram = JobPayload {
            job_type: JobType::Dialog,
            telegram_data: None,
            image_data: None,
            music_data: None,
            dialog_data: Some(DialogJobData {
                message_text: "hello".to_owned(),
                ..DialogJobData::default()
            }),
            asr_data: None,
            control_data: None,
            agent_data: None,
        };
        assert_eq!(
            super::dialog_job_params_from_payload(&payload_without_telegram),
            Err(DialogJobPayloadError::MissingTelegramData)
        );

        let payload_without_dialog = JobPayload {
            job_type: JobType::Dialog,
            telegram_data: Some(super::TelegramData {
                chat_id: 10,
                user_id: 30,
                message_id: 20,
                thread_message_id: None,
                user_full_name: "Alice".to_owned(),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
        };
        assert_eq!(
            super::dialog_job_params_from_payload(&payload_without_dialog),
            Err(DialogJobPayloadError::MissingDialogData)
        );
    }

    #[test]
    fn control_job_data_serializes_donation_and_successful_payment_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let donation = ControlJobData {
            kind: ControlKind::DonateInvoice,
            amount: 600,
            ..ControlJobData::default()
        };
        assert_eq!(
            serde_json::to_value(donation)?,
            json!({
                "kind": "donate_invoice",
                "amount": 600
            })
        );

        let successful_payment = ControlJobData {
            kind: ControlKind::SuccessfulPayment,
            payment: Some(super::ControlPayment {
                currency: "XTR".to_owned(),
                total_amount: 300,
                invoice_payload: "subscription_42".to_owned(),
                telegram_payment_charge_id: "telegram-charge".to_owned(),
                provider_payment_charge_id: "provider-charge".to_owned(),
                ..super::ControlPayment::default()
            }),
            ..ControlJobData::default()
        };
        assert_eq!(
            serde_json::to_value(successful_payment)?,
            json!({
                "kind": "successful_payment",
                "payment": {
                    "currency": "XTR",
                    "total_amount": 300,
                    "invoice_payload": "subscription_42",
                    "telegram_payment_charge_id": "telegram-charge",
                    "provider_payment_charge_id": "provider-charge"
                }
            })
        );
        Ok(())
    }

    #[test]
    fn control_payment_preserves_recurring_subscription_metadata()
    -> Result<(), Box<dyn std::error::Error>> {
        let payment: super::ControlPayment = serde_json::from_value(json!({
            "currency": "XTR",
            "total_amount": 300,
            "invoice_payload": "subscription_42",
            "telegram_payment_charge_id": "telegram-charge",
            "provider_payment_charge_id": "provider-charge",
            "paid_at": "2026-06-17T16:14:02Z",
            "subscription_period_seconds": 2592000,
            "subscription_expiration_date": "2026-07-17T16:14:02Z",
            "is_recurring": true,
            "is_first_recurring": false
        }))?;

        assert_eq!(payment.paid_at.as_deref(), Some("2026-06-17T16:14:02Z"));
        assert_eq!(payment.subscription_period_seconds, Some(2_592_000));
        assert_eq!(
            payment.subscription_expiration_date.as_deref(),
            Some("2026-07-17T16:14:02Z")
        );
        assert_eq!(payment.is_recurring, Some(true));
        assert_eq!(payment.is_first_recurring, Some(false));

        let legacy: super::ControlPayment = serde_json::from_value(json!({
            "currency": "XTR",
            "total_amount": 300,
            "invoice_payload": "subscription_42",
            "telegram_payment_charge_id": "telegram-charge",
            "provider_payment_charge_id": "provider-charge"
        }))?;
        assert_eq!(legacy.paid_at, None);
        assert_eq!(legacy.subscription_period_seconds, None);
        assert_eq!(legacy.subscription_expiration_date, None);
        assert_eq!(legacy.is_recurring, None);
        assert_eq!(legacy.is_first_recurring, None);

        Ok(())
    }

    #[test]
    fn control_job_data_deserializes_sparse_go_payloads_with_defaults()
    -> Result<(), Box<dyn std::error::Error>> {
        let data: ControlJobData = serde_json::from_value(json!({
            "kind": "donate_invoice",
            "amount": 600
        }))?;

        assert_eq!(
            data,
            ControlJobData {
                kind: ControlKind::DonateInvoice,
                amount: 600,
                ..ControlJobData::default()
            }
        );
        Ok(())
    }

    #[test]
    fn control_job_params_decode_go_executor_input_from_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let job = new_control_job_at(
            ControlJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice Smith".to_owned(),
                thread_id: Some(40),
                data: ControlJobData {
                    kind: ControlKind::DonateInvoice,
                    amount: 600,
                    ..ControlJobData::default()
                },
            },
            created,
        );

        let params = super::control_job_params_from_stateless_job(&job)?;

        assert_eq!(
            params,
            ControlJobParams {
                chat_id: 10,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice Smith".to_owned(),
                thread_id: Some(40),
                data: ControlJobData {
                    kind: ControlKind::DonateInvoice,
                    amount: 600,
                    ..ControlJobData::default()
                },
            }
        );
        Ok(())
    }

    #[test]
    fn control_job_params_reject_missing_go_required_payload_sections() {
        let payload_without_telegram = JobPayload {
            job_type: JobType::Control,
            telegram_data: None,
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: Some(ControlJobData {
                kind: ControlKind::VipInvoice,
                ..ControlJobData::default()
            }),
            agent_data: None,
        };
        assert_eq!(
            super::control_job_params_from_payload(&payload_without_telegram),
            Err(ControlJobPayloadError::MissingTelegramData)
        );

        let payload_without_control = JobPayload {
            job_type: JobType::Control,
            telegram_data: Some(super::TelegramData {
                chat_id: 10,
                user_id: 30,
                message_id: 20,
                thread_message_id: None,
                user_full_name: "Alice".to_owned(),
                chat_title: String::new(),
            }),
            image_data: None,
            music_data: None,
            dialog_data: None,
            asr_data: None,
            control_data: None,
            agent_data: None,
        };
        assert_eq!(
            super::control_job_params_from_payload(&payload_without_control),
            Err(ControlJobPayloadError::MissingControlData)
        );
    }

    #[test]
    fn in_memory_task_queue_dequeues_by_go_priority_created_and_id_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - time::Duration::seconds(10);

        let low = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("low", DEFAULT_PRIORITY, old, 1, 100, 10),
        );
        let high_new = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("high-new", HIGH_PRIORITY, now, 2, 100, 11),
        );
        let high_old = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("high-old", HIGH_PRIORITY, old, 3, 100, 12),
        );
        let highest = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("highest", HIGHEST_PRIORITY, now, 4, 100, 13),
        );

        assert_eq!((low, high_new, high_old, highest), (1, 2, 3, 4));

        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker", now)
                .map(|item| item.id),
            Some(highest)
        );
        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker", now)
                .map(|item| item.id),
            Some(high_old)
        );
        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker", now)
                .map(|item| item.id),
            Some(high_new)
        );
        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker", now)
                .map(|item| item.id),
            Some(low)
        );
        assert!(queue.dequeue(TEXT_QUEUE_NAME, "worker", now).is_none());
        Ok(())
    }

    #[test]
    fn active_asr_blocks_new_vip_and_regular_draw_claims() -> Result<(), Box<dyn std::error::Error>>
    {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let regular = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: 1,
                    message_id: 10,
                    user_id: 2,
                    prompt: "regular".to_owned(),
                    ..ImageGenJobParams::default()
                },
                now,
            ),
        );
        let vip = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_edit_job_at(
                ImageEditJobParams {
                    chat_id: 1,
                    message_id: 11,
                    user_id: 2,
                    prompt: "vip".to_owned(),
                    photo_file_id: "photo".to_owned(),
                    ..ImageEditJobParams::default()
                },
                now,
            )
            .with_priority(HIGHEST_PRIORITY),
        );
        let asr = queue.assign(
            ASR_GPU1_QUEUE_NAME,
            new_asr_job_at(
                AsrJobParams {
                    chat_id: 1,
                    message_id: 12,
                    user_id: 2,
                    file_unique_id: "voice".to_owned(),
                    duration_seconds: 4,
                    ..AsrJobParams::default()
                },
                now,
            ),
        );

        assert!(
            queue
                .dequeue_matching(IMAGE_REGULAR_QUEUE_NAME, "regular-worker", now, |job| {
                    job.data.job_type == JobType::ImageGen
                })
                .is_none()
        );
        assert!(
            queue
                .dequeue_matching(IMAGE_VIP_QUEUE_NAME, "vip-worker", now, |job| {
                    job.data.job_type == JobType::ImageEdit
                })
                .is_none()
        );
        assert_eq!(
            queue
                .dequeue_matching(ASR_GPU1_QUEUE_NAME, "asr-worker", now, |job| {
                    job.data.job_type == JobType::Asr
                })
                .map(|work| work.id),
            Some(asr)
        );
        assert!(
            queue
                .dequeue_matching(IMAGE_REGULAR_QUEUE_NAME, "regular-worker", now, |job| {
                    job.data.job_type == JobType::ImageGen
                })
                .is_none(),
            "processing ASR must keep blocking draw"
        );

        queue.complete(asr, now)?;
        assert_eq!(
            queue
                .dequeue_matching(IMAGE_REGULAR_QUEUE_NAME, "regular-worker", now, |job| {
                    job.data.job_type == JobType::ImageGen
                })
                .map(|work| work.id),
            Some(regular)
        );
        assert_eq!(
            queue
                .dequeue_matching(IMAGE_VIP_QUEUE_NAME, "vip-worker", now, |job| {
                    job.data.job_type == JobType::ImageEdit
                })
                .map(|work| work.id),
            Some(vip)
        );
        Ok(())
    }

    #[test]
    fn processing_draw_does_not_block_asr_claim() -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let draw = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: 1,
                    message_id: 10,
                    user_id: 2,
                    prompt: "already drawing".to_owned(),
                    ..ImageGenJobParams::default()
                },
                now,
            ),
        );
        assert_eq!(
            queue
                .dequeue_matching(IMAGE_REGULAR_QUEUE_NAME, "draw-worker", now, |job| {
                    job.data.job_type == JobType::ImageGen
                })
                .map(|work| work.id),
            Some(draw)
        );
        let asr = queue.assign(
            ASR_GPU1_QUEUE_NAME,
            new_asr_job_at(
                AsrJobParams {
                    chat_id: 1,
                    message_id: 11,
                    user_id: 2,
                    file_unique_id: "voice-during-draw".to_owned(),
                    duration_seconds: 4,
                    ..AsrJobParams::default()
                },
                now,
            ),
        );

        assert_eq!(
            queue
                .dequeue_matching(ASR_GPU1_QUEUE_NAME, "asr-worker", now, |job| {
                    job.data.job_type == JobType::Asr
                })
                .map(|work| work.id),
            Some(asr)
        );
        assert_eq!(
            queue.record(draw).map(|record| record.status),
            Some(JobStatus::Processing),
            "ASR priority must not cancel an already-running draw"
        );
        Ok(())
    }

    #[test]
    fn pending_image_queue_entries_track_positions_as_queue_drains()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let image_job = |chat: i64, thread: Option<i32>, created| {
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: chat,
                    message_id: 20,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "draw".to_owned(),
                    thread_id: thread,
                    ..ImageGenJobParams::default()
                },
                created,
            )
            .with_name("image")
            .with_priority(DEFAULT_PRIORITY)
        };

        let first = queue.assign(IMAGE_REGULAR_QUEUE_NAME, image_job(-100, Some(7), now));
        let second = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            image_job(-200, None, now + time::Duration::seconds(1)),
        );
        let third = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            image_job(-300, None, now + time::Duration::seconds(2)),
        );

        assert_eq!(
            queue.pending_image_queue_entries(IMAGE_REGULAR_QUEUE_NAME),
            vec![
                PendingImageQueueEntry {
                    job_id: first,
                    chat_id: -100,
                    thread_message_id: Some(7),
                },
                PendingImageQueueEntry {
                    job_id: second,
                    chat_id: -200,
                    thread_message_id: None,
                },
                PendingImageQueueEntry {
                    job_id: third,
                    chat_id: -300,
                    thread_message_id: None,
                },
            ]
        );

        assert_eq!(
            queue
                .dequeue_matching(IMAGE_REGULAR_QUEUE_NAME, "worker", now, |job| job
                    .data
                    .job_type
                    == JobType::ImageGen)
                .map(|item| item.id),
            Some(first)
        );
        let entries = queue.pending_image_queue_entries(IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(
            entries.iter().map(|entry| entry.job_id).collect::<Vec<_>>(),
            vec![second, third]
        );
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_dequeue_matching_skips_unowned_jobs_without_reordering()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let high_unowned = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("high-unowned", HIGH_PRIORITY, now, 7, 10, 1),
        );
        let default_owned = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("default-owned", DEFAULT_PRIORITY, now, 7, 10, 2),
        );

        let item = queue
            .dequeue_matching(TEXT_QUEUE_NAME, "worker", now, |job| {
                job.title == "default-owned"
            })
            .expect("owned job");

        assert_eq!(item.id, default_owned);
        let statuses: HashMap<i64, JobStatus> = queue
            .records()
            .into_iter()
            .map(|record| (record.id, record.status))
            .collect();
        assert_eq!(statuses.get(&high_unowned), Some(&JobStatus::Pending));
        assert_eq!(statuses.get(&default_owned), Some(&JobStatus::Processing));
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_blocks_same_thread_dialog_aifarm_jobs_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let first = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            dialog_aifarm_job_at("first", now, 10, 100, 1, Some(7)),
        );
        let same_thread = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            dialog_aifarm_job_at("same thread", now, 11, 100, 2, Some(7)),
        );
        let other_thread = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            dialog_aifarm_job_at("other thread", now, 12, 100, 3, Some(8)),
        );

        assert_eq!(
            queue
                .dequeue(DIALOG_AIFARM_QUEUE_NAME, "dialog-aifarm-0", now)
                .map(|item| item.id),
            Some(first)
        );
        assert_eq!(
            queue
                .dequeue(DIALOG_AIFARM_QUEUE_NAME, "dialog-aifarm-1", now)
                .map(|item| item.id),
            Some(other_thread)
        );
        assert_eq!(
            queue
                .dequeue(DIALOG_AIFARM_QUEUE_NAME, "dialog-aifarm-2", now)
                .map(|item| item.id),
            None
        );

        queue.complete(first, now)?;
        assert_eq!(
            queue
                .dequeue(DIALOG_AIFARM_QUEUE_NAME, "dialog-aifarm-2", now)
                .map(|item| item.id),
            Some(same_thread)
        );
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_counts_user_limits_and_active_statuses()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let first = queue.assign_with_user_limit(
            TEXT_QUEUE_NAME,
            job_at("first", HIGH_PRIORITY, now, 7, 10, 1),
            2,
        )?;
        let second = queue.assign_with_user_limit(
            TEXT_QUEUE_NAME,
            job_at("second", HIGH_PRIORITY, now, 7, 10, 2),
            2,
        )?;
        assert_eq!(first, Some(1));
        assert_eq!(second, Some(2));
        assert_eq!(
            queue.assign_with_user_limit(
                TEXT_QUEUE_NAME,
                job_at("third", HIGH_PRIORITY, now, 7, 10, 3),
                2
            )?,
            None
        );
        assert_eq!(queue.active_count(TEXT_QUEUE_NAME), 2);
        assert_eq!(queue.user_active_count(TEXT_QUEUE_NAME, 7), 2);
        assert_eq!(queue.queue_depth(TEXT_QUEUE_NAME, HIGH_PRIORITY), 2);
        assert_eq!(
            queue.queue_depth_for_priority_or_higher(TEXT_QUEUE_NAME, DEFAULT_PRIORITY),
            2
        );

        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker", now)
                .map(|item| item.id),
            first
        );
        assert_eq!(queue.user_active_count(TEXT_QUEUE_NAME, 7), 2);
        queue.complete(first.unwrap_or_default(), now)?;
        assert_eq!(queue.user_active_count(TEXT_QUEUE_NAME, 7), 1);
        queue.fail(second.unwrap_or_default(), "boom", now)?;
        assert_eq!(queue.active_count(TEXT_QUEUE_NAME), 0);
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_requeues_processing_jobs_on_startup()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let id = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, now, 7, 10, 1),
        );

        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker-a", now)
                .map(|item| item.id),
            Some(id)
        );
        assert_eq!(queue.requeue_processing_for_startup(), 1);
        let records = queue.records();
        assert_eq!(records[0].status, JobStatus::Pending);
        assert_eq!(records[0].worker_id, None);
        assert_eq!(records[0].started_at, None);
        assert_eq!(records[0].execution_started_at, None);

        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker-b", now)
                .map(|item| item.id),
            Some(id)
        );
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_requeues_expired_processing_jobs_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let deadline = start + time::Duration::seconds(60);
        let expired = deadline + time::Duration::seconds(1);
        let id = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, start, 7, 10, 1).with_processing_timeout_seconds(60),
        );

        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "dialog-worker", start)
                .map(|item| item.id),
            Some(id)
        );
        assert_eq!(
            queue.requeue_expired_processing(deadline),
            Vec::<i64>::new()
        );
        assert_eq!(queue.requeue_expired_processing(expired), vec![id]);
        let record = queue.record(id).expect("requeued record");
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.worker_id, None);
        assert_eq!(record.started_at, None);
        assert_eq!(record.execution_started_at, None);
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_uses_execution_start_for_image_music_timeouts()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let execution_start = start + time::Duration::seconds(10);
        let id = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: 10,
                    message_id: 1,
                    user_id: 7,
                    user_full_name: "Wave".to_owned(),
                    prompt: "draw".to_owned(),
                    original_text: "draw".to_owned(),
                    ..ImageGenJobParams::default()
                },
                start,
            )
            .with_processing_timeout_seconds(60),
        );

        assert_eq!(
            queue
                .dequeue_matching(IMAGE_VIP_QUEUE_NAME, "image-worker", start, |job| {
                    job.data.job_type == JobType::ImageGen
                })
                .map(|item| item.id),
            Some(id)
        );
        assert_eq!(
            queue.requeue_expired_processing(start + time::Duration::seconds(600)),
            Vec::<i64>::new()
        );
        assert!(queue.set_execution_started(id, execution_start)?);
        assert!(!queue.set_execution_started(id, execution_start + time::Duration::seconds(5))?);
        assert_eq!(
            queue.requeue_expired_processing(execution_start + time::Duration::seconds(60)),
            Vec::<i64>::new()
        );
        assert_eq!(
            queue.requeue_expired_processing(execution_start + time::Duration::seconds(61)),
            vec![id]
        );
        let record = queue.record(id).expect("requeued record");
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.execution_started_at, None);
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_fails_stuck_processing_jobs_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let deadline = start + time::Duration::hours(4);
        let stuck = deadline + time::Duration::seconds(1);
        let id = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, start, 7, 10, 1),
        );
        queue.dequeue(TEXT_QUEUE_NAME, "dialog-worker", start);

        assert_eq!(
            queue.fail_stuck_processing(deadline, std::time::Duration::from_secs(4 * 3600)),
            Vec::<i64>::new()
        );
        assert_eq!(
            queue.fail_stuck_processing(stuck, std::time::Duration::from_secs(4 * 3600)),
            vec![id]
        );
        let record = queue.record(id).expect("failed record");
        assert_eq!(record.status, JobStatus::Failed);
        assert_eq!(record.worker_id.as_deref(), Some("dialog-worker"));
        assert_eq!(record.started_at, Some(start));
        assert_eq!(record.completed_at, Some(stuck));
        assert_eq!(record.error.as_deref(), Some(STUCK_JOB_ERROR_MESSAGE));
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_uses_execution_start_for_stuck_image_jobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let execution_start = start + time::Duration::minutes(3);
        let id = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_edit_job_at(
                ImageEditJobParams {
                    chat_id: 10,
                    message_id: 1,
                    user_id: 7,
                    user_full_name: "Wave".to_owned(),
                    prompt: "edit".to_owned(),
                    photo_file_id: "file".to_owned(),
                    ..ImageEditJobParams::default()
                },
                start,
            ),
        );
        queue.dequeue_matching(IMAGE_VIP_QUEUE_NAME, "image-worker", start, |job| {
            job.data.job_type == JobType::ImageEdit
        });
        assert!(queue.set_execution_started(id, execution_start)?);

        assert_eq!(
            queue.fail_stuck_processing(
                start + time::Duration::hours(4) + time::Duration::seconds(1),
                std::time::Duration::from_secs(4 * 3600)
            ),
            Vec::<i64>::new()
        );
        assert_eq!(
            queue.fail_stuck_processing(
                execution_start + time::Duration::hours(4) + time::Duration::seconds(1),
                std::time::Duration::from_secs(4 * 3600)
            ),
            vec![id]
        );
        let record = queue.record(id).expect("failed record");
        assert_eq!(record.status, JobStatus::Failed);
        assert_eq!(record.execution_started_at, Some(execution_start));
        assert_eq!(record.error.as_deref(), Some(STUCK_JOB_ERROR_MESSAGE));
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_prunes_old_terminal_records() -> Result<(), Box<dyn std::error::Error>>
    {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - TimeDuration::days(2);
        let recent = now - TimeDuration::hours(1);
        let cutoff = now - TimeDuration::days(1);

        let old_done = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("old", DEFAULT_PRIORITY, old, 1, 1, 1),
        );
        let recent_done = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("recent", DEFAULT_PRIORITY, recent, 2, 1, 2),
        );
        let active = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("active", DEFAULT_PRIORITY, old, 3, 1, 3),
        );

        queue.complete(old_done, old)?;
        queue.fail(recent_done, "recent failure", recent)?;

        assert_eq!(queue.prune_terminal_before(cutoff), vec![old_done]);
        let statuses: HashMap<i64, JobStatus> = queue
            .records()
            .into_iter()
            .map(|record| (record.id, record.status))
            .collect();
        assert_eq!(statuses.get(&old_done), None);
        assert_eq!(statuses.get(&recent_done), Some(&JobStatus::Failed));
        assert_eq!(statuses.get(&active), Some(&JobStatus::Pending));
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_tracks_worker_heartbeats_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let start = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let heartbeat = start + time::Duration::seconds(30);
        queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, start, 7, 10, 1),
        );
        queue.dequeue(TEXT_QUEUE_NAME, "text-worker-0", start);

        assert_eq!(queue.worker_heartbeat_at("text-worker-0"), Some(start));
        assert_eq!(queue.active_workers()["text-worker-0"], 1);
        assert_eq!(queue.active_worker_count_for_queue(TEXT_QUEUE_NAME), 1);

        queue.update_worker_heartbeat("text-worker-0", heartbeat);
        queue.update_worker_heartbeat("image-regular-worker-0", heartbeat);
        queue.update_worker_heartbeat("image-regular-worker-1", heartbeat);
        assert_eq!(queue.worker_heartbeat_at("text-worker-0"), Some(heartbeat));
        assert_eq!(
            queue.active_worker_count_for_queue(IMAGE_REGULAR_QUEUE_NAME),
            2
        );
        assert_eq!(queue.active_worker_count_for_queue(IMAGE_VIP_QUEUE_NAME), 1);
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_requeues_job_to_target_queue_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let id = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, now, 7, 10, 1),
        );
        queue.append_job_event(
            id,
            TaskQueueJobEvent {
                stage: "before_retry".to_owned(),
                ..TaskQueueJobEvent::default()
            },
            now,
        )?;

        let item = queue
            .dequeue(TEXT_QUEUE_NAME, "worker-a", now)
            .expect("work");
        assert_eq!(item.id, id);
        assert_eq!(item.events.len(), 1);
        queue.fail(id, "temporary", now)?;
        queue.requeue_job_to_queue(id, DIALOG_AIFARM_QUEUE_NAME)?;

        let record = queue.record(id).expect("job");
        assert_eq!(record.queue_name, DIALOG_AIFARM_QUEUE_NAME);
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.worker_id, None);
        assert_eq!(record.started_at, None);
        assert_eq!(record.execution_started_at, None);
        assert_eq!(record.completed_at, None);
        assert_eq!(record.error, None);
        assert_eq!(record.events.len(), 1);
        assert_eq!(
            queue
                .dequeue(DIALOG_AIFARM_QUEUE_NAME, "worker-b", now)
                .map(|item| item.id),
            Some(id)
        );
        assert_eq!(
            queue.requeue_job_to_queue(id, ""),
            Err(TaskQueueError::QueueNameRequired)
        );
        assert_eq!(
            queue.requeue_job_to_queue(999, TEXT_QUEUE_NAME),
            Err(TaskQueueError::JobNotFound(999))
        );
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_cancels_matching_active_user_jobs()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let first = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("first", DEFAULT_PRIORITY, now, 7, 10, 1),
        );
        let second = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("second", DEFAULT_PRIORITY, now, 7, 10, 2),
        );
        let other_user = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("other-user", DEFAULT_PRIORITY, now, 8, 10, 3),
        );
        let other_chat = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("other-chat", DEFAULT_PRIORITY, now, 7, 11, 4),
        );

        assert_eq!(
            queue
                .dequeue(TEXT_QUEUE_NAME, "worker", now)
                .map(|item| item.id),
            Some(first)
        );

        let cancelled = queue.cancel_user_jobs(7, 10, "cancelled by user", now);
        assert_eq!(cancelled, vec![first, second]);

        let statuses: HashMap<i64, JobStatus> = queue
            .records()
            .into_iter()
            .map(|record| (record.id, record.status))
            .collect();
        assert_eq!(statuses.get(&first), Some(&JobStatus::Cancelled));
        assert_eq!(statuses.get(&second), Some(&JobStatus::Cancelled));
        assert_eq!(statuses.get(&other_user), Some(&JobStatus::Pending));
        assert_eq!(statuses.get(&other_chat), Some(&JobStatus::Pending));
        Ok(())
    }

    #[test]
    fn in_memory_task_queue_deletes_and_restarts_jobs_for_admin_controls()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let restarted_at = now + time::Duration::minutes(5);
        let first = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("first", HIGH_PRIORITY, now, 7, 10, 1),
        );
        let second = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            job_at("second", DEFAULT_PRIORITY, now, 8, 11, 2),
        );

        let new_id = queue.restart(first, restarted_at)?;
        assert_eq!(new_id, 3);
        let restarted = queue.record(new_id).expect("restarted job");
        assert_eq!(restarted.queue_name, TEXT_QUEUE_NAME);
        assert_eq!(restarted.status, JobStatus::Pending);
        assert_eq!(restarted.job.title, "first");
        assert_eq!(restarted.job.created, restarted_at);

        let deleted = queue.delete(second)?;
        assert_eq!(deleted.id, second);
        assert!(queue.record(second).is_none());
        assert_eq!(queue.delete(404), Err(TaskQueueError::JobNotFound(404)));
        Ok(())
    }

    #[test]
    fn image_queue_status_matches_go_tool_adapter_counts_and_fallback_eta()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            job_at("regular-default", DEFAULT_PRIORITY, now, 100, 42, 10),
        );
        queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            job_at("regular-low", LOWEST_PRIORITY, now, 100, 42, 11),
        );
        queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            job_at("vip-default", DEFAULT_PRIORITY, now, 100, 42, 12),
        );
        queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            job_at("other-vip", DEFAULT_PRIORITY, now, 103, 77, 13),
        );
        queue.dequeue(IMAGE_REGULAR_QUEUE_NAME, "image-regular-worker-1", now);

        let status = queue.image_queue_status(100);

        assert_eq!(status.regular_queue_depth, 0);
        assert_eq!(status.vip_queue_depth, 2);
        assert_eq!(status.active_jobs_count, 4);
        assert_eq!(status.user_active_jobs, 3);
        assert_eq!(status.user_pending_jobs, 3);
        assert_eq!(status.estimated_wait, std::time::Duration::ZERO);
        assert_eq!(status.go_estimated_wait_string(), "0s");
        Ok(())
    }

    #[test]
    fn image_queue_status_uses_go_fallback_eta_for_regular_default_depth()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        for id in 0..10 {
            queue.assign(
                IMAGE_REGULAR_QUEUE_NAME,
                job_at(
                    "regular-default",
                    DEFAULT_PRIORITY,
                    now,
                    200 + id,
                    42,
                    10 + id as i32,
                ),
            );
        }

        let status = queue.image_queue_status(42);

        assert_eq!(status.regular_queue_depth, 10);
        assert_eq!(status.go_estimated_wait_string(), "8m0s");
        Ok(())
    }

    #[test]
    fn pending_dialog_job_update_matches_go_message_index_and_cleaning()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let completed = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            dialog_job_at("completed", now, 7, 100, 51),
        );
        queue.complete(completed, now)?;
        let pending = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            dialog_job_at("pending", now, 7, 100, 51),
        );
        let meta = json!({"sender_id": 7, "sender_type": "user"});

        let updated = queue.update_pending_dialog_job_by_message_id(
            100,
            51,
            "\u{200f}new\tindexed ",
            " raw text ",
            meta.clone(),
        );

        assert!(updated);
        let records: HashMap<i64, super::TaskQueueRecord> = queue
            .records()
            .into_iter()
            .map(|record| (record.id, record))
            .collect();
        assert_eq!(
            records
                .get(&completed)
                .and_then(|record| record.job.data.dialog_data.as_ref())
                .map(|data| data.message_text.as_str()),
            Some("completed")
        );
        let data = records
            .get(&pending)
            .and_then(|record| record.job.data.dialog_data.as_ref())
            .expect("pending dialog payload");
        assert_eq!(data.message_text, "new indexed");
        assert_eq!(data.original_text, "raw text");
        assert_eq!(data.meta, meta);
        Ok(())
    }

    #[test]
    fn pending_image_job_update_clears_variants_and_skips_non_pending()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let completed = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            image_job_at("completed", now, 8, 100, 52),
        );
        queue.complete(completed, now)?;
        let pending = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            image_job_at("pending", now, 8, 100, 52),
        );
        let meta = json!({"sender_id": 8, "sender_type": "user"});

        let updated = queue.update_pending_image_job_by_message_id(
            100,
            52,
            "\u{200f}new\tprompt ",
            " raw prompt ",
            meta.clone(),
        );

        assert!(updated);
        let records: HashMap<i64, super::TaskQueueRecord> = queue
            .records()
            .into_iter()
            .map(|record| (record.id, record))
            .collect();
        assert_eq!(
            records
                .get(&completed)
                .and_then(|record| record.job.data.image_data.as_ref())
                .map(|data| data.prompt.as_str()),
            Some("completed")
        );
        let data = records
            .get(&pending)
            .and_then(|record| record.job.data.image_data.as_ref())
            .expect("pending image payload");
        assert_eq!(data.prompt, "new prompt");
        assert_eq!(data.original_text, "raw prompt");
        assert_eq!(data.meta, meta);
        assert!(data.prompt_variants.is_empty());
        Ok(())
    }

    #[test]
    fn set_job_image_urls_updates_matching_image_payload_only() -> Result<(), TaskQueueError> {
        let queue = InMemoryTaskQueue::new();
        let image_id = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            image_job_at("image", OffsetDateTime::UNIX_EPOCH, 7, 10, 20),
        );
        let control_id = queue.assign(
            CONTROL_QUEUE_NAME,
            job_at(
                "control",
                DEFAULT_PRIORITY,
                OffsetDateTime::UNIX_EPOCH,
                7,
                10,
                21,
            ),
        );

        queue.set_job_image_urls(image_id, vec!["https://img.test/1.png".to_owned()])?;
        queue.set_job_image_urls(control_id, vec!["ignored".to_owned()])?;

        let records = queue.records();
        assert_eq!(
            records[0]
                .job
                .data
                .image_data
                .as_ref()
                .expect("image data")
                .image_urls,
            vec!["https://img.test/1.png"]
        );
        assert!(records[1].job.data.image_data.is_none());
        assert_eq!(
            queue.set_job_image_urls(999, Vec::new()),
            Err(TaskQueueError::JobNotFound(999))
        );
        Ok(())
    }

    #[test]
    fn task_queue_tracks_go_job_messages_and_events() -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let older = OffsetDateTime::from_unix_timestamp(1_779_193_700)?;
        let job_id = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, created, 7, 10, 20),
        );

        queue.update_job_messages(job_id, Some(101), Some(103))?;
        let first_message = queue.create_job_message(TaskQueueJobMessageParams {
            job_id,
            message_type: MESSAGE_TYPE_RESULT.to_owned(),
            chat_id: 10,
            message_id: 103,
            created_at: older,
            status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
        })?;
        let second_message = queue.create_job_message(TaskQueueJobMessageParams {
            job_id,
            message_type: MESSAGE_TYPE_RESULT.to_owned(),
            chat_id: 10,
            message_id: 104,
            created_at: created,
            status: MESSAGE_STATUS_COMPLETED.to_owned(),
        })?;
        queue.update_job_message_status(first_message, MESSAGE_STATUS_FAILED)?;

        for index in 0..405 {
            queue.append_job_event(
                job_id,
                TaskQueueJobEvent {
                    stage: format!("stage-{index}"),
                    ..TaskQueueJobEvent::default()
                },
                created,
            )?;
        }

        let record = queue
            .records()
            .into_iter()
            .find(|record| record.id == job_id)
            .expect("job record");
        assert_eq!(record.progress_message_id, Some(101));
        assert_eq!(record.result_message_id, Some(103));
        assert_eq!(record.events.len(), MAX_JOB_EVENTS);
        assert_eq!(record.events[0].stage, "stage-5");
        assert_eq!(record.events[399].stage, "stage-404");
        assert_eq!(record.events[0].at, "2026-05-19T12:30:00Z");

        let messages = queue.job_messages(job_id);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].id, second_message);
        assert_eq!(messages[0].status, MESSAGE_STATUS_COMPLETED);
        assert_eq!(messages[1].id, first_message);
        assert_eq!(messages[1].status, MESSAGE_STATUS_FAILED);
        Ok(())
    }

    #[test]
    fn task_queue_finds_and_deletes_stale_placeholders_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let old = now - time::Duration::seconds(121);
        let boundary = now - time::Duration::seconds(120);
        let job_id = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("dialog", DEFAULT_PRIORITY, now, 7, 10, 20),
        );
        let stale = queue.create_job_message(TaskQueueJobMessageParams {
            job_id,
            message_type: MESSAGE_TYPE_RESULT.to_owned(),
            chat_id: 10,
            message_id: 103,
            created_at: old,
            status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
        })?;
        let fresh_boundary = queue.create_job_message(TaskQueueJobMessageParams {
            job_id,
            message_type: MESSAGE_TYPE_RESULT.to_owned(),
            chat_id: 10,
            message_id: 104,
            created_at: boundary,
            status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
        })?;
        let completed = queue.create_job_message(TaskQueueJobMessageParams {
            job_id,
            message_type: MESSAGE_TYPE_RESULT.to_owned(),
            chat_id: 10,
            message_id: 105,
            created_at: old,
            status: MESSAGE_STATUS_COMPLETED.to_owned(),
        })?;

        let stale_messages =
            queue.stale_placeholder_messages(now, std::time::Duration::from_secs(120));
        assert_eq!(
            stale_messages
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            vec![stale]
        );
        assert!(queue.delete_job_message(stale));
        assert!(!queue.delete_job_message(999));
        let remaining_ids = queue
            .job_messages(job_id)
            .into_iter()
            .map(|message| message.id)
            .collect::<Vec<_>>();
        assert_eq!(remaining_ids, vec![fresh_boundary, completed]);
        assert_eq!(queue.delete_job_messages_by_job_id(job_id), 2);
        assert!(queue.job_messages(job_id).is_empty());
        Ok(())
    }

    #[test]
    fn task_queue_snapshot_round_trips_and_rejects_other_formats()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let first = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("first", DEFAULT_PRIORITY, now, 7, 10, 1),
        );
        queue.fail(first, "network", now)?;

        let snapshot = queue.snapshot();
        assert_eq!(snapshot.format, TASK_QUEUE_SNAPSHOT_FORMAT);
        let bytes = encode_task_queue_snapshot(&snapshot)?;
        let decoded = decode_task_queue_snapshot(&bytes)?;
        assert_eq!(decoded, snapshot);

        let restored = InMemoryTaskQueue::from_snapshot(decoded);
        let next = restored.assign(
            TEXT_QUEUE_NAME,
            job_at("second", DEFAULT_PRIORITY, now, 7, 10, 2),
        );
        assert_eq!(next, first + 1);

        let wrong = br#"{"format":"go-gob","next_id":1,"records":[]}"#;
        assert!(matches!(
            decode_task_queue_snapshot(wrong),
            Err(super::TaskQueueSnapshotError::UnsupportedFormat(format)) if format == "go-gob"
        ));
        Ok(())
    }

    #[test]
    fn task_queue_wal_replays_final_job_state_without_gob_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = Arc::new(WalSinkStub::default());
        let queue = InMemoryTaskQueue::new_with_journal(journal.clone());
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;
        let keep = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("keep", DEFAULT_PRIORITY, now, 7, 10, 1),
        );
        let delete = queue.assign(
            TEXT_QUEUE_NAME,
            job_at("delete", DEFAULT_PRIORITY, now, 8, 10, 2),
        );
        let work = queue
            .dequeue(TEXT_QUEUE_NAME, "text-worker-0", now)
            .expect("work");
        assert_eq!(work.id, keep);
        queue.update_job_messages(keep, Some(101), Some(103))?;
        queue.create_job_message(TaskQueueJobMessageParams {
            job_id: keep,
            message_type: MESSAGE_TYPE_RESULT.to_owned(),
            chat_id: 10,
            message_id: 103,
            created_at: now,
            status: MESSAGE_STATUS_PLACEHOLDER.to_owned(),
        })?;
        queue.append_job_event(
            keep,
            TaskQueueJobEvent {
                stage: "provider".to_owned(),
                message: "queued".to_owned(),
                ..TaskQueueJobEvent::default()
            },
            now,
        )?;
        queue.complete(keep, now)?;
        queue.delete(delete)?;

        let recovered =
            replay_task_queue_wal_records(super::empty_task_queue_snapshot(), journal.records());

        assert_eq!(recovered.records.len(), 1);
        let record = &recovered.records[0];
        assert_eq!(record.id, keep);
        assert_eq!(record.status, JobStatus::Completed);
        assert_eq!(record.progress_message_id, Some(101));
        assert_eq!(record.messages.len(), 1);
        assert_eq!(record.events.len(), 1);
        assert_eq!(record.events[0].stage, "provider");
        assert_eq!(recovered.next_id, 3);
        assert_eq!(recovered.next_message_id, 2);
        Ok(())
    }

    #[test]
    fn task_queue_status_strings_match_go_status_constants() {
        assert_eq!(JobStatus::Pending.as_str(), "pending");
        assert_eq!(JobStatus::Processing.as_str(), "processing");
        assert_eq!(JobStatus::Completed.as_str(), "completed");
        assert_eq!(JobStatus::Failed.as_str(), "failed");
        assert_eq!(JobStatus::Cancelled.as_str(), "cancelled");
        assert!(JobStatus::Pending.is_active());
        assert!(JobStatus::Processing.is_active());
        assert!(!JobStatus::Completed.is_active());
    }

    #[test]
    fn in_memory_task_queue_missing_finalize_is_loud() -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        assert_eq!(
            queue.complete(404, now),
            Err(TaskQueueError::JobNotFound(404))
        );
        Ok(())
    }

    fn job_at(
        title: &str,
        priority: i32,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> super::StatelessJobItem {
        new_control_job_at(
            ControlJobParams {
                chat_id,
                message_id,
                user_id,
                user_full_name: format!("User {user_id}"),
                thread_id: None,
                data: ControlJobData {
                    kind: ControlKind::Translate,
                    text: title.to_owned(),
                    ..ControlJobData::default()
                },
            },
            created,
        )
        .with_name(title)
        .with_priority(priority)
    }

    fn dialog_job_at(
        message_text: &str,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> super::StatelessJobItem {
        super::StatelessJobItem {
            title: "dialog".to_owned(),
            created,
            priority: DEFAULT_PRIORITY,
            processing_timeout_seconds: 0,
            data: JobPayload {
                job_type: JobType::Dialog,
                telegram_data: Some(super::TelegramData {
                    chat_id,
                    user_id,
                    message_id,
                    thread_message_id: None,
                    user_full_name: format!("User {user_id}"),
                    chat_title: String::new(),
                }),
                image_data: None,
                music_data: None,
                dialog_data: Some(DialogJobData {
                    message_text: message_text.to_owned(),
                    ..DialogJobData::default()
                }),
                asr_data: None,
                control_data: None,
                agent_data: None,
            },
        }
    }

    fn dialog_aifarm_job_at(
        message_text: &str,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
        thread_id: Option<i32>,
    ) -> super::StatelessJobItem {
        new_dialog_job_at(
            DialogJobParams {
                chat_id,
                message_id,
                user_id,
                user_full_name: format!("User {user_id}"),
                message_text: message_text.to_owned(),
                original_text: message_text.to_owned(),
                meta: serde_json::Value::Null,
                max_output_tokens: 0,
                thread_id,
            },
            created,
        )
    }

    fn image_job_at(
        prompt: &str,
        created: OffsetDateTime,
        user_id: i64,
        chat_id: i64,
        message_id: i32,
    ) -> super::StatelessJobItem {
        super::StatelessJobItem {
            title: "image_gen".to_owned(),
            created,
            priority: DEFAULT_PRIORITY,
            processing_timeout_seconds: 0,
            data: JobPayload {
                job_type: JobType::ImageGen,
                telegram_data: Some(super::TelegramData {
                    chat_id,
                    user_id,
                    message_id,
                    thread_message_id: None,
                    user_full_name: format!("User {user_id}"),
                    chat_title: String::new(),
                }),
                image_data: Some(ImageJobData {
                    prompt: prompt.to_owned(),
                    original_text: prompt.to_owned(),
                    prompt_variants: vec!["enhanced".to_owned(), "clip".to_owned()],
                    ..ImageJobData::default()
                }),
                music_data: None,
                dialog_data: None,
                asr_data: None,
                control_data: None,
                agent_data: None,
            },
        }
    }

    fn agent_job(goal: &str) -> super::StatelessJobItem {
        new_agent_job_at(
            AgentJobParams {
                chat_id: 1,
                message_id: 2,
                user_id: 3,
                user_full_name: "User".to_owned(),
                thread_id: None,
                profile_id: "search".to_owned(),
                goal: goal.to_owned(),
                reasoner_provider: "qwen-reasoner".to_owned(),
                writer_provider: "conversational".to_owned(),
            },
            OffsetDateTime::now_utc(),
        )
    }

    fn blob(committed_step: i32, state: &str) -> AgentRunStateBlob {
        AgentRunStateBlob {
            format: "openplotva.agent-run-state.v1+json".to_owned(),
            committed_step,
            state: state.to_owned(),
            resume_count: 0,
        }
    }

    #[test]
    fn agent_checkpoint_round_trips_through_snapshot() {
        let queue = InMemoryTaskQueue::new();
        let id = queue.assign(AGENT_QWEN_QUEUE_NAME, agent_job("find rust news"));
        queue
            .checkpoint_agent_state(id, blob(2, "{\"step\":2}"))
            .expect("checkpoint");

        let restored = InMemoryTaskQueue::from_snapshot(queue.snapshot());
        let record = restored
            .records()
            .into_iter()
            .find(|record| record.id == id)
            .expect("record present after restore");
        let agent_state = record.agent_state.expect("agent_state survives snapshot");
        assert_eq!(agent_state.committed_step, 2);
        assert_eq!(agent_state.state, "{\"step\":2}");
    }

    #[test]
    fn agent_run_resumes_from_checkpoint_after_orphan_release() {
        let now = OffsetDateTime::now_utc();
        let queue = InMemoryTaskQueue::new();
        let id = queue.assign(AGENT_QWEN_QUEUE_NAME, agent_job("research"));

        let first = queue
            .dequeue_or_adopt_agent(AGENT_QWEN_QUEUE_NAME, "worker-a", now)
            .expect("fresh claim");
        assert!(!first.resumed);
        queue
            .checkpoint_agent_state(id, blob(1, "{}"))
            .expect("checkpoint");

        // Crash: the owning worker dies; startup recovery releases the orphan.
        let report = queue.release_orphaned_processing_for_startup();
        assert_eq!(report.agent_kept, 1);
        assert_eq!(report.requeued, 0);

        // With no fresh Pending job, a live worker must re-adopt the orphan.
        let second = queue
            .dequeue_or_adopt_agent(AGENT_QWEN_QUEUE_NAME, "worker-b", now)
            .expect("re-adopt orphan");
        assert!(second.resumed);
        assert_eq!(second.id, id);
        let resumed = second.agent_state.expect("checkpoint preserved");
        assert_eq!(resumed.committed_step, 1);
        assert_eq!(resumed.resume_count, 1);
    }

    #[test]
    fn startup_release_keeps_agent_processing_and_requeues_dialog() {
        let now = OffsetDateTime::now_utc();
        let queue = InMemoryTaskQueue::new();
        let agent_id = queue.assign(AGENT_QWEN_QUEUE_NAME, agent_job("long run"));
        let dialog_id = queue.assign(
            DIALOG_AIFARM_QUEUE_NAME,
            new_dialog_job_at(
                DialogJobParams {
                    chat_id: 1,
                    message_id: 2,
                    user_id: 3,
                    user_full_name: "User".to_owned(),
                    message_text: "hi".to_owned(),
                    original_text: String::new(),
                    meta: json!({}),
                    max_output_tokens: 0,
                    thread_id: None,
                },
                now,
            ),
        );
        queue
            .dequeue_or_adopt_agent(AGENT_QWEN_QUEUE_NAME, "w", now)
            .expect("claim agent");
        queue
            .dequeue(DIALOG_AIFARM_QUEUE_NAME, "w", now)
            .expect("claim dialog");

        let report = queue.release_orphaned_processing_for_startup();
        assert_eq!(report.agent_kept, 1);
        assert_eq!(report.requeued, 1);

        let records = queue.records();
        let agent_record = records
            .iter()
            .find(|r| r.id == agent_id)
            .expect("agent record should exist");
        assert_eq!(agent_record.status, JobStatus::Processing);
        assert!(agent_record.worker_id.is_none());
        let dialog_record = records
            .iter()
            .find(|r| r.id == dialog_id)
            .expect("dialog record should exist");
        assert_eq!(dialog_record.status, JobStatus::Pending);
    }

    #[test]
    fn stuck_failer_excludes_agent_jobs() {
        let now = OffsetDateTime::now_utc();
        let queue = InMemoryTaskQueue::new();
        let agent_id = queue.assign(AGENT_QWEN_QUEUE_NAME, agent_job("never stuck"));
        queue
            .dequeue_or_adopt_agent(AGENT_QWEN_QUEUE_NAME, "w", now)
            .expect("claim agent");

        let failed = queue.fail_stuck_processing(
            now + TimeDuration::hours(1),
            std::time::Duration::from_secs(1),
        );
        assert!(!failed.contains(&agent_id));
        let record = queue
            .records()
            .into_iter()
            .find(|r| r.id == agent_id)
            .expect("agent record should exist");
        assert_eq!(record.status, JobStatus::Processing);
    }
}
