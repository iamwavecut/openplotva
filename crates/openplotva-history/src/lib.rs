//! Chat history and summary cascade behavior.

use std::{collections::HashSet, error::Error, fmt};

use openplotva_core::{ChatMessageMeta, MessageSender, ToolCall};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "history";

pub const SUMMARY_PROMPT_VERSION: &str = "chat_history_summary_v1";
pub const MAX_SUMMARY_MESSAGES: i32 = 500;
pub const MIN_EDGE_RAW_MESSAGES: i32 = 20;
pub const SUMMARY_WINDOW_TTL_HOURS: i64 = 24;
pub const DEFAULT_SUMMARY_MAX_INPUT_TOKENS: i32 = 10_000;
pub const DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS: i32 = 600;
pub const HISTORY_SUMMARY_GENERATE_MAX_ATTEMPTS: usize = 2;
pub const HISTORY_SUMMARY_GENERATE_RETRY_DELAY_SECONDS: u64 = 2;
pub const MODEL_GEMINI_FLASH_FALLBACK: &str = "gemini-2.5-flash-lite";
pub const AIFARM_DEFAULT_HISTORY_SUMMARY_MODEL: &str = "default";

pub const MESSAGE_KIND_TEXT: &str = "text";
pub const MESSAGE_KIND_TOOL_REQUEST: &str = "tool_request";
pub const MESSAGE_KIND_TOOL_RESPONSE: &str = "tool_response";
pub const ROLE_TOOL: &str = "tool";
pub const ROLE_USER: &str = "user";

pub const SENDER_TYPE_USER: &str = "user";
pub const SENDER_TYPE_CHANNEL: &str = "channel";
pub const SENDER_TYPE_SAME_CHAT: &str = "same_chat";
pub const SENDER_TYPE_SYSTEM: &str = "system";

const HISTORY_SUMMARY_RETRYABLE_ERROR_FRAGMENTS: &[&str] = &[
    "429",
    "rate limit",
    "too many requests",
    "temporarily unavailable",
    "timeout",
    "timed out",
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryScope {
    /// Empty/unknown scope before normalization.
    #[default]
    #[serde(rename = "")]
    Unknown,
    /// Whole chat.
    Chat,
    /// Current forum thread.
    Thread,
}

impl SummaryScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "",
            Self::Chat => "chat",
            Self::Thread => "thread",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SummaryWindow {
    /// Empty/unknown window before normalization.
    #[default]
    #[serde(rename = "")]
    Unknown,
    /// Day window.
    Day,
    /// Last N hours.
    Hours,
    /// Last N messages.
    Messages,
    /// Since timestamp.
    Since,
}

impl SummaryWindow {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "",
            Self::Day => "day",
            Self::Hours => "hours",
            Self::Messages => "messages",
            Self::Since => "since",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SummaryRequest {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Target thread ID, or zero for whole-chat scope.
    pub thread_id: i32,
    /// Requesting Telegram user ID.
    pub requested_by_user_id: i64,
    /// Summary scope.
    pub scope: SummaryScope,
    /// Summary window.
    pub window: SummaryWindow,
    /// Requested hours.
    pub hours: i32,
    /// Requested message count.
    pub message_count: i32,
    /// Since timestamp for `since` window.
    pub since: Option<OffsetDateTime>,
    /// Current time, injected for tests.
    pub now: Option<OffsetDateTime>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct SummaryMessageEntry {
    /// Stored history entry ID.
    #[serde(default)]
    pub entry_id: String,
    /// Dialog role.
    #[serde(default)]
    pub role: String,
    /// Message kind.
    #[serde(default)]
    pub kind: String,
    /// Stored timestamp.
    #[serde(default, deserialize_with = "deserialize_optional_rfc3339")]
    pub timestamp: Option<OffsetDateTime>,
    /// Telegram message ID from embedded `api.Message`.
    #[serde(default)]
    pub message_id: i32,
    /// Telegram Unix date from embedded `api.Message`.
    #[serde(default)]
    pub date: i64,
    /// Telegram forum topic ID from embedded `api.Message`.
    #[serde(default)]
    pub message_thread_id: i32,
    /// Telegram text.
    #[serde(default)]
    pub text: String,
    /// Telegram caption.
    #[serde(default)]
    pub caption: String,
    /// Original text before fallback normalization.
    #[serde(default)]
    pub original_text: String,
    #[serde(default)]
    pub meta: ChatMessageMeta,
    /// Optional tool call.
    #[serde(default)]
    pub tool_call: Option<ToolCall>,
    /// Sender user.
    #[serde(default)]
    pub from: Option<SummaryTelegramUser>,
    /// Sender chat/channel.
    #[serde(default)]
    pub sender_chat: Option<SummaryTelegramChat>,
    /// Message chat.
    #[serde(default)]
    pub chat: Option<SummaryTelegramChat>,
    /// Forward origin marker from embedded `api.Message`.
    #[serde(default)]
    pub forward_origin: Option<SummaryForwardOrigin>,
    /// Via bot from embedded `api.Message`.
    #[serde(default)]
    pub via_bot: Option<SummaryTelegramUser>,
    /// Telegram automatic-forward flag.
    #[serde(default)]
    pub is_automatic_forward: bool,
}

/// Minimal Telegram forward-origin subset used by memory consolidation noise filtering.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SummaryForwardOrigin {
    /// Telegram forward origin type.
    #[serde(default, rename = "type")]
    pub origin_type: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SummaryInputAssembly {
    /// Ordered raw-message and reused-summary prompt items.
    pub items: Vec<SummaryInputItem>,
    /// Ordered coverage source rows.
    pub sources: Vec<SummarySource>,
    /// Raw plus reused-summary covered message count.
    pub covered_message_count: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SummaryCoverageRange {
    /// Covered range start.
    pub start: OffsetDateTime,
    /// Covered range end.
    pub end: OffsetDateTime,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SummaryTelegramUser {
    /// Telegram user ID.
    #[serde(default)]
    pub id: i64,
    /// First name.
    #[serde(default)]
    pub first_name: String,
    /// Last name.
    #[serde(default)]
    pub last_name: String,
    /// Username.
    #[serde(default, alias = "user_name")]
    pub username: String,
    /// Bot flag.
    #[serde(default)]
    pub is_bot: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SummaryTelegramChat {
    /// Telegram chat ID.
    #[serde(default)]
    pub id: i64,
    /// Chat type.
    #[serde(default, rename = "type")]
    pub chat_type: String,
    /// Title.
    #[serde(default)]
    pub title: String,
    /// First name.
    #[serde(default)]
    pub first_name: String,
    /// Last name.
    #[serde(default)]
    pub last_name: String,
    /// Username.
    #[serde(default, alias = "user_name")]
    pub username: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SummaryInput {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Target thread ID, or zero for whole-chat scope.
    pub thread_id: i32,
    /// Summary scope.
    pub scope: SummaryScope,
    /// Covered range start.
    pub range_start_at: OffsetDateTime,
    /// Covered range end.
    pub range_end_at: OffsetDateTime,
    /// First Telegram message ID in the covered range.
    pub first_message_id: i32,
    /// Last Telegram message ID in the covered range.
    pub last_message_id: i32,
    /// First history entry ID.
    pub first_entry_id: String,
    /// Last history entry ID.
    pub last_entry_id: String,
    /// Raw message count represented by the input.
    pub raw_message_count: i32,
    /// Raw plus reused-summary covered message count.
    pub covered_message_count: i32,
    /// Number of reused summaries in this input.
    pub reused_summary_count: i32,
    /// Message count omitted while fitting context.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub omitted_message_count: i32,
    /// Source summary IDs.
    #[serde(default)]
    pub source_summary_ids: Vec<i64>,
    /// Requesting Telegram user ID.
    pub requested_by_user_id: i64,
    /// Stable input hash.
    pub input_hash: String,
    /// Estimated input tokens.
    pub input_token_estimate: i32,
    /// Summary cascade depth.
    pub cascade_depth: i32,
    /// Source coverage rows.
    #[serde(default)]
    pub sources: Vec<SummarySource>,
    /// Ordered raw and summary items.
    #[serde(default)]
    pub items: Vec<SummaryInputItem>,
}

impl Default for SummaryInput {
    fn default() -> Self {
        Self {
            chat_id: 0,
            thread_id: 0,
            scope: SummaryScope::default(),
            range_start_at: go_zero_time(),
            range_end_at: go_zero_time(),
            first_message_id: 0,
            last_message_id: 0,
            first_entry_id: String::new(),
            last_entry_id: String::new(),
            raw_message_count: 0,
            covered_message_count: 0,
            reused_summary_count: 0,
            omitted_message_count: 0,
            source_summary_ids: Vec::new(),
            requested_by_user_id: 0,
            input_hash: String::new(),
            input_token_estimate: 0,
            cascade_depth: 0,
            sources: Vec::new(),
            items: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SummaryInputItem {
    /// Item kind, for example `message` or `summary`.
    pub kind: String,
    /// Item timestamp.
    pub at: OffsetDateTime,
    /// Telegram message ID.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub message_id: i32,
    /// History entry ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entry_id: String,
    /// Dialog role string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub role: String,
    /// Sender display name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_name: String,
    /// Sender username.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_username: String,
    /// Sender type.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sender_type: String,
    /// Message text.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    /// Original text before fallback normalization.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub original_text: String,
    /// Metadata type.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub meta_type: String,
    /// Annotation text.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub annotation: String,
    /// Vision description text.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub vision_description: String,
    /// Reused summary ID.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub summary_id: i64,
    /// Reused summary range start.
    pub range_start_at: OffsetDateTime,
    /// Reused summary range end.
    pub range_end_at: OffsetDateTime,
    /// Reused summary rendered HTML.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary_html: String,
    /// Reused summary structured content.
    pub summary_json: SummaryContent,
}

impl Default for SummaryInputItem {
    fn default() -> Self {
        Self {
            kind: String::new(),
            at: go_zero_time(),
            message_id: 0,
            entry_id: String::new(),
            role: String::new(),
            sender_name: String::new(),
            sender_username: String::new(),
            sender_type: String::new(),
            text: String::new(),
            original_text: String::new(),
            meta_type: String::new(),
            annotation: String::new(),
            vision_description: String::new(),
            summary_id: 0,
            range_start_at: go_zero_time(),
            range_end_at: go_zero_time(),
            summary_html: String::new(),
            summary_json: SummaryContent::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SummarySourceType {
    /// Reused stored summary.
    Summary,
    /// Raw message range.
    MessageRange,
}

impl SummarySourceType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Summary => "summary",
            Self::MessageRange => "message_range",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SummarySource {
    /// One-based source order after normalization.
    pub source_order: i32,
    /// Source type.
    pub source_type: SummarySourceType,
    /// Stored source summary ID.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub source_summary_id: i64,
    /// Source range start.
    pub range_start_at: OffsetDateTime,
    /// Source range end.
    pub range_end_at: OffsetDateTime,
    /// First Telegram message ID in source.
    pub first_message_id: i32,
    /// Last Telegram message ID in source.
    pub last_message_id: i32,
    /// First history entry ID in source.
    pub first_entry_id: String,
    /// Last history entry ID in source.
    pub last_entry_id: String,
    /// Raw message count.
    pub raw_message_count: i32,
    /// Covered message count.
    pub covered_message_count: i32,
}

impl Default for SummarySource {
    fn default() -> Self {
        Self {
            source_order: 0,
            source_type: SummarySourceType::MessageRange,
            source_summary_id: 0,
            range_start_at: go_zero_time(),
            range_end_at: go_zero_time(),
            first_message_id: 0,
            last_message_id: 0,
            first_entry_id: String::new(),
            last_entry_id: String::new(),
            raw_message_count: 0,
            covered_message_count: 0,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SummaryDocument {
    /// Structured summary content.
    pub content: SummaryContent,
    /// Rendered Telegram HTML.
    pub html: String,
    /// Provider model label.
    pub model: String,
    /// Prompt version.
    pub prompt_version: String,
    /// Prompt hash.
    pub prompt_hash: String,
    /// Input hash override.
    pub input_hash: String,
    /// Input token estimate override.
    pub input_token_estimate: i32,
    /// Output token estimate override.
    pub output_token_estimate: i32,
    /// Cascade depth override.
    pub cascade_depth: i32,
    /// Quality score override.
    pub quality_score: f64,
    pub quality_notes: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StoredSummary {
    /// Summary ID.
    pub id: i64,
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Target thread ID.
    pub thread_id: i32,
    /// Summary scope.
    pub scope: SummaryScope,
    /// Requesting Telegram user ID.
    pub requested_by_user_id: i64,
    /// Covered range start.
    pub range_start_at: OffsetDateTime,
    /// Covered range end.
    pub range_end_at: OffsetDateTime,
    /// First Telegram message ID in range.
    pub first_message_id: i32,
    /// Last Telegram message ID in range.
    pub last_message_id: i32,
    /// First history entry ID.
    pub first_entry_id: String,
    /// Last history entry ID.
    pub last_entry_id: String,
    /// Raw message count.
    pub raw_message_count: i32,
    /// Covered message count.
    pub covered_message_count: i32,
    /// Source summary IDs.
    #[serde(default)]
    pub source_summary_ids: Vec<i64>,
    /// Structured summary JSON.
    pub summary_json: SummaryContent,
    /// Rendered Telegram HTML.
    pub summary_html: String,
    /// Provider model label.
    pub model: String,
    /// Prompt version.
    pub prompt_version: String,
    /// Stable input hash.
    pub input_hash: String,
    /// Prompt hash.
    pub prompt_hash: String,
    /// Estimated input tokens.
    pub input_token_estimate: i32,
    /// Estimated output tokens.
    pub output_token_estimate: i32,
    /// Cascade depth.
    pub cascade_depth: i32,
    /// Quality score.
    pub quality_score: f64,
    pub quality_notes: String,
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
}

impl Default for StoredSummary {
    fn default() -> Self {
        Self {
            id: 0,
            chat_id: 0,
            thread_id: 0,
            scope: SummaryScope::default(),
            requested_by_user_id: 0,
            range_start_at: go_zero_time(),
            range_end_at: go_zero_time(),
            first_message_id: 0,
            last_message_id: 0,
            first_entry_id: String::new(),
            last_entry_id: String::new(),
            raw_message_count: 0,
            covered_message_count: 0,
            source_summary_ids: Vec::new(),
            summary_json: SummaryContent::default(),
            summary_html: String::new(),
            model: String::new(),
            prompt_version: String::new(),
            input_hash: String::new(),
            prompt_hash: String::new(),
            input_token_estimate: 0,
            output_token_estimate: 0,
            cascade_depth: 0,
            quality_score: 0.0,
            quality_notes: String::new(),
            created_at: go_zero_time(),
        }
    }
}

/// Prepared summary row plus source rows for storage adapters.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedStoredSummary {
    /// Stored summary row values.
    pub stored: StoredSummary,
    /// Compact JSON representation of `stored.summary_json`.
    pub summary_json: Vec<u8>,
    /// Normalized source rows.
    pub sources: Vec<SummarySource>,
}

pub trait SummaryTokenEstimator {
    /// Estimate tokens for a text fragment.
    fn estimate_text(&self, text: &str) -> i32;
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SummaryInputFitStats {
    /// Original input token estimate including system prompt.
    pub original_input_token_estimate: i32,
    /// Final input token estimate including system prompt.
    pub final_input_token_estimate: i32,
    /// Number of message items dropped.
    pub dropped_message_count: i32,
    /// Dropped Telegram message IDs in drop order.
    pub dropped_message_ids: Vec<i32>,
    /// Original message item count.
    pub original_message_item_count: i32,
    /// Final message item count.
    pub final_message_item_count: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HeuristicSummaryTokenEstimator;

impl SummaryTokenEstimator for HeuristicSummaryTokenEstimator {
    fn estimate_text(&self, text: &str) -> i32 {
        estimate_summary_text_tokens(text)
    }
}

/// Error returned by `prepare_stored_summary`.
#[derive(Debug, Eq, PartialEq)]
pub enum PrepareStoredSummaryError {
    /// Input chat ID is required.
    MissingChatId,
    /// Rendered HTML is empty after trimming.
    EmptySummaryHtml,
    /// Summary JSON could not be serialized.
    MarshalSummaryJson(String),
}

impl fmt::Display for PrepareStoredSummaryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingChatId => f.write_str("chat_id is required"),
            Self::EmptySummaryHtml => f.write_str("summary_html is empty"),
            Self::MarshalSummaryJson(err) => write!(f, "marshal summary json: {err}"),
        }
    }
}

impl Error for PrepareStoredSummaryError {}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredSummaryEvent {
    /// One-based event order.
    pub source_order: i32,
    /// Event row content.
    pub event: SummaryEvent,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SummaryContent {
    /// Short event titles.
    #[serde(default)]
    pub events: Vec<String>,
    /// Structured event details.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_details: Vec<SummaryEvent>,
    /// Actors mentioned in the summary.
    #[serde(default)]
    pub actors: Vec<SummaryActor>,
    /// Summary recap text.
    #[serde(default)]
    pub recap: String,
    /// Open questions.
    #[serde(default)]
    pub open_questions: Vec<String>,
    /// Source style label.
    #[serde(default)]
    pub source_style: String,
    /// Optional quality score.
    #[serde(
        default,
        deserialize_with = "deserialize_f64_loose",
        skip_serializing_if = "is_zero_f64"
    )]
    pub quality_score: f64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub quality_notes: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SummaryActor {
    /// Actor display name.
    #[serde(default)]
    pub name: String,
    /// Actor description.
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SummaryEvent {
    /// Event title.
    #[serde(default)]
    pub title: String,
    /// Event description.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    /// Event actors.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actors: Vec<String>,
    /// Event timestamp label.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub occurred_at: String,
    /// Optional confidence score.
    #[serde(
        default,
        deserialize_with = "deserialize_f64_loose",
        skip_serializing_if = "is_zero_f64"
    )]
    pub confidence: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct HistorySummaryLlmResponse {
    /// Rendered Telegram HTML.
    #[serde(default)]
    pub summary_html: String,
    /// Structured summary content.
    #[serde(default)]
    pub summary_json: SummaryContent,
}

/// Error returned by history-summary response decoding.
#[derive(Debug)]
pub enum HistorySummaryDecodeError {
    Json(serde_json::Error),
    /// Decoded response contains no useful summary content.
    EmptySummaryJson,
}

/// Error returned by `parse_history_summary_since`.
#[derive(Debug)]
pub struct HistorySummarySinceParseError {
    source: time::error::Parse,
}

/// Error returned by summary-entry payload decoding.
#[derive(Debug)]
pub enum SummaryEntryDecodeError {
    Json(serde_json::Error),
}

impl fmt::Display for SummaryEntryDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(err) => write!(f, "decode summary entry payload: {err}"),
        }
    }
}

impl Error for SummaryEntryDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(err) => Some(err),
        }
    }
}

impl fmt::Display for HistorySummarySinceParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid since timestamp: {}", self.source)
    }
}

impl Error for HistorySummarySinceParseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

impl fmt::Display for HistorySummaryDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(err) => write!(f, "decode history summary response: {err}"),
            Self::EmptySummaryJson => f.write_str("summary_json is empty"),
        }
    }
}

impl Error for HistorySummaryDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(err) => Some(err),
            Self::EmptySummaryJson => None,
        }
    }
}

#[must_use]
pub const fn history_summary_known_keys() -> &'static [&'static str] {
    &[
        "summary_html",
        "summary_json",
        "events",
        "event_details",
        "title",
        "actors",
        "recap",
        "open_questions",
        "source_style",
        "quality_score",
        "quality_notes",
        "name",
        "description",
        "occurred_at",
        "confidence",
    ]
}

pub fn decode_history_summary_response(
    raw: &str,
) -> Result<HistorySummaryLlmResponse, HistorySummaryDecodeError> {
    let value = decode_history_summary_value(raw)?;
    let mut decoded: HistorySummaryLlmResponse =
        decode_response_value(value.clone()).map_err(HistorySummaryDecodeError::Json)?;
    if !summary_content_has_payload(&decoded.summary_json)
        && let Ok(direct) = decode_summary_content_value(value)
        && summary_content_has_payload(&direct)
    {
        decoded.summary_json = direct;
    }
    decoded.summary_json = normalize_generated_summary_content(decoded.summary_json);
    if !summary_content_has_payload(&decoded.summary_json) {
        return Err(HistorySummaryDecodeError::EmptySummaryJson);
    }
    decoded.summary_html = render_history_summary_html(&decoded.summary_json);
    Ok(decoded)
}

fn decode_history_summary_value(raw: &str) -> Result<Value, HistorySummaryDecodeError> {
    let trimmed = raw.trim();
    let strict_error = match serde_json::from_str::<Value>(trimmed) {
        Ok(value) => return Ok(value),
        Err(err) => err,
    };
    if let Some(value) =
        openplotva_dialog::salvage_json_object_value(trimmed, history_summary_known_keys())
    {
        return Ok(value);
    }
    Err(HistorySummaryDecodeError::Json(strict_error))
}

fn decode_response_value(value: Value) -> Result<HistorySummaryLlmResponse, serde_json::Error> {
    serde_json::from_value(value)
}

fn decode_summary_content_value(value: Value) -> Result<SummaryContent, serde_json::Error> {
    serde_json::from_value(value)
}

#[must_use]
pub fn history_summary_provider(raw_provider: &str) -> &'static str {
    let provider = raw_provider.trim();
    if provider.eq_ignore_ascii_case("genkit") {
        return "genkit";
    }
    "aifarm"
}

#[must_use]
pub fn history_summary_model(
    explicit_model: &str,
    provider: &str,
    memory_consolidation_model: &str,
    runtime_default_model: Option<&str>,
) -> String {
    let model = explicit_model.trim();
    if !model.is_empty() {
        return model.to_owned();
    }
    if history_summary_provider(provider) == "aifarm" {
        let model = memory_consolidation_model.trim();
        if !model.is_empty() {
            return model.to_owned();
        }
        return AIFARM_DEFAULT_HISTORY_SUMMARY_MODEL.to_owned();
    }
    runtime_default_model
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or(MODEL_GEMINI_FLASH_FALLBACK)
        .to_owned()
}

#[must_use]
pub const fn history_summary_timeout_seconds(configured_seconds: i32) -> i32 {
    if configured_seconds > 0 {
        configured_seconds
    } else {
        DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS
    }
}

#[must_use]
pub fn history_summary_generate_error_retryable(
    message: &str,
    context_done: bool,
    error_cancelled: bool,
    llm_retryable_reason: Option<&str>,
) -> bool {
    if context_done || error_cancelled {
        return false;
    }
    if let Some(reason) = llm_retryable_reason {
        return !reason.is_empty();
    }
    contains_any_ascii_fold(message, HISTORY_SUMMARY_RETRYABLE_ERROR_FRAGMENTS)
}

#[must_use]
pub fn resolve_summary_scope(scope: &str, current_thread_id: i32) -> (SummaryScope, i32) {
    match normalized_summary_scope(scope) {
        SummaryScope::Chat => (SummaryScope::Chat, 0),
        SummaryScope::Thread => {
            if current_thread_id != 0 {
                (SummaryScope::Thread, current_thread_id)
            } else {
                (SummaryScope::Chat, 0)
            }
        }
        SummaryScope::Unknown => {
            if current_thread_id != 0 {
                (SummaryScope::Thread, current_thread_id)
            } else {
                (SummaryScope::Chat, 0)
            }
        }
    }
}

#[must_use]
pub fn history_summary_scope(scope: &str, context_thread_id: Option<i32>) -> (SummaryScope, i32) {
    resolve_summary_scope(scope, context_thread_id.unwrap_or_default())
}

pub fn parse_history_summary_since(
    value: &str,
) -> Result<Option<OffsetDateTime>, HistorySummarySinceParseError> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .map(Some)
        .map_err(|source| HistorySummarySinceParseError { source })
}

#[must_use]
pub fn normalize_summary_request(mut request: SummaryRequest) -> SummaryRequest {
    (request.scope, request.thread_id) =
        resolve_summary_scope(request.scope.as_str(), request.thread_id);
    request.window = match request.window {
        SummaryWindow::Hours | SummaryWindow::Messages | SummaryWindow::Since => request.window,
        SummaryWindow::Unknown | SummaryWindow::Day => SummaryWindow::Day,
    };
    if request.hours <= 0 {
        request.hours = 24;
    }
    request.hours = request.hours.min(24);
    if request.message_count <= 0 {
        request.message_count = 100;
    }
    request.message_count = request.message_count.min(MAX_SUMMARY_MESSAGES);
    let now = request.now.unwrap_or_else(OffsetDateTime::now_utc);
    request.now = Some(now.to_offset(time::UtcOffset::UTC));
    request.since = request
        .since
        .map(|since| since.to_offset(time::UtcOffset::UTC));
    request
}

#[must_use]
pub fn normalized_summary_scope(scope: &str) -> SummaryScope {
    let scope = scope.trim();
    if scope.eq_ignore_ascii_case(SummaryScope::Chat.as_str()) {
        return SummaryScope::Chat;
    }
    if scope.eq_ignore_ascii_case(SummaryScope::Thread.as_str()) {
        return SummaryScope::Thread;
    }
    SummaryScope::Unknown
}

#[must_use]
pub fn normalized_summary_window(window: &str) -> SummaryWindow {
    let window = window.trim();
    if window.eq_ignore_ascii_case(SummaryWindow::Hours.as_str()) {
        return SummaryWindow::Hours;
    }
    if window.eq_ignore_ascii_case(SummaryWindow::Messages.as_str()) {
        return SummaryWindow::Messages;
    }
    if window.eq_ignore_ascii_case(SummaryWindow::Since.as_str()) {
        return SummaryWindow::Since;
    }
    if window.eq_ignore_ascii_case(SummaryWindow::Day.as_str()) {
        return SummaryWindow::Day;
    }
    SummaryWindow::Unknown
}

#[must_use]
pub fn summary_requested_range(
    request: &SummaryRequest,
    reset_at: Option<OffsetDateTime>,
) -> (OffsetDateTime, OffsetDateTime) {
    let range_end = request
        .now
        .unwrap_or_else(OffsetDateTime::now_utc)
        .to_offset(time::UtcOffset::UTC);
    let earliest = range_end - time::Duration::hours(SUMMARY_WINDOW_TTL_HOURS);
    let mut range_start = match request.window {
        SummaryWindow::Hours => range_end - time::Duration::hours(i64::from(request.hours)),
        SummaryWindow::Since => request
            .since
            .map(|since| since.to_offset(time::UtcOffset::UTC))
            .unwrap_or(earliest),
        SummaryWindow::Unknown | SummaryWindow::Day | SummaryWindow::Messages => earliest,
    };
    if range_start < earliest {
        range_start = earliest;
    }
    if range_start > range_end {
        range_start = range_end;
    }
    if let Some(reset_at) = reset_at.map(|value| value.to_offset(time::UtcOffset::UTC))
        && reset_at > range_start
    {
        range_start = reset_at;
    }
    (range_start, range_end)
}

#[must_use]
pub fn summary_reset_at(
    chat_reset_at: Option<OffsetDateTime>,
    scope: SummaryScope,
    thread_id: i32,
    thread_reset_at: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    if scope != SummaryScope::Thread || thread_id == 0 {
        return chat_reset_at.map(|value| value.to_offset(time::UtcOffset::UTC));
    }
    max_optional_time_utc(chat_reset_at, thread_reset_at)
}

pub fn prepare_stored_summary(
    input: &SummaryInput,
    doc: &SummaryDocument,
) -> Result<PreparedStoredSummary, PrepareStoredSummaryError> {
    if input.chat_id == 0 {
        return Err(PrepareStoredSummaryError::MissingChatId);
    }
    let html = doc.html.trim().to_owned();
    if html.is_empty() {
        return Err(PrepareStoredSummaryError::EmptySummaryHtml);
    }
    let content = normalize_summary_content(doc.content.clone());
    let summary_json = serde_json::to_vec(&content)
        .map_err(|err| PrepareStoredSummaryError::MarshalSummaryJson(err.to_string()))?;
    let sources = summary_sources_for_storage(input);
    let source_summary_ids = summary_source_ids_for_storage(input, &sources);
    let stored = StoredSummary {
        chat_id: input.chat_id,
        thread_id: input.thread_id,
        scope: input.scope,
        requested_by_user_id: input.requested_by_user_id,
        range_start_at: input.range_start_at,
        range_end_at: input.range_end_at,
        first_message_id: input.first_message_id,
        last_message_id: input.last_message_id,
        first_entry_id: input.first_entry_id.clone(),
        last_entry_id: input.last_entry_id.clone(),
        raw_message_count: input.raw_message_count,
        covered_message_count: input.covered_message_count,
        source_summary_ids,
        summary_json: content.clone(),
        summary_html: html.clone(),
        model: summary_model(doc),
        prompt_version: summary_prompt_version(doc),
        input_hash: summary_input_hash(input, doc),
        prompt_hash: doc.prompt_hash.trim().to_owned(),
        input_token_estimate: summary_input_token_estimate(input, doc),
        output_token_estimate: summary_output_token_estimate(&html, &content, doc),
        cascade_depth: summary_cascade_depth(input, doc),
        quality_score: summary_quality_score(&content, doc),
        quality_notes: summary_quality_notes(&content, doc),
        ..StoredSummary::default()
    };

    Ok(PreparedStoredSummary {
        stored,
        summary_json,
        sources,
    })
}

#[must_use]
pub fn summary_sources_for_storage(input: &SummaryInput) -> Vec<SummarySource> {
    let sources = if input.sources.is_empty() {
        vec![summary_source_from_input_range(input)]
    } else {
        input.sources.clone()
    };
    normalize_summary_sources(sources)
}

#[must_use]
pub fn summary_source_ids_for_storage(input: &SummaryInput, sources: &[SummarySource]) -> Vec<i64> {
    let source_ids = summary_ids_from_sources(sources);
    if source_ids.is_empty() && !input.source_summary_ids.is_empty() {
        return input.source_summary_ids.clone();
    }
    source_ids
}

#[must_use]
pub fn summary_source_from_stored_summary(summary: &StoredSummary) -> SummarySource {
    SummarySource {
        source_type: SummarySourceType::Summary,
        source_summary_id: summary.id,
        range_start_at: summary.range_start_at,
        range_end_at: summary.range_end_at,
        first_message_id: summary.first_message_id,
        last_message_id: summary.last_message_id,
        first_entry_id: summary.first_entry_id.clone(),
        last_entry_id: summary.last_entry_id.clone(),
        raw_message_count: summary.raw_message_count,
        covered_message_count: summary.covered_message_count.max(summary.raw_message_count),
        ..SummarySource::default()
    }
}

#[must_use]
pub fn summary_source_from_input_range(input: &SummaryInput) -> SummarySource {
    SummarySource {
        source_type: SummarySourceType::MessageRange,
        range_start_at: input.range_start_at,
        range_end_at: input.range_end_at,
        first_message_id: input.first_message_id,
        last_message_id: input.last_message_id,
        first_entry_id: input.first_entry_id.clone(),
        last_entry_id: input.last_entry_id.clone(),
        raw_message_count: input.raw_message_count,
        covered_message_count: input.covered_message_count,
        ..SummarySource::default()
    }
}

#[must_use]
pub fn summary_input_item_from_stored_summary(summary: &StoredSummary) -> SummaryInputItem {
    SummaryInputItem {
        kind: "summary".to_owned(),
        at: summary.range_start_at,
        summary_id: summary.id,
        range_start_at: summary.range_start_at,
        range_end_at: summary.range_end_at,
        summary_html: summary.summary_html.clone(),
        summary_json: summary.summary_json.clone(),
        ..SummaryInputItem::default()
    }
}

pub fn decode_summary_message_entry_payload(
    payload: &[u8],
) -> Result<SummaryMessageEntry, SummaryEntryDecodeError> {
    serde_json::from_slice(payload).map_err(SummaryEntryDecodeError::Json)
}

pub fn decode_summary_message_entry_payloads(
    payloads: &[Vec<u8>],
) -> Result<Vec<SummaryMessageEntry>, SummaryEntryDecodeError> {
    payloads
        .iter()
        .map(|payload| decode_summary_message_entry_payload(payload))
        .collect()
}

#[must_use]
pub fn filter_summary_entries_with_content(
    entries: &[SummaryMessageEntry],
) -> Vec<SummaryMessageEntry> {
    entries
        .iter()
        .filter(|entry| summary_entry_has_content(entry))
        .cloned()
        .collect()
}

#[must_use]
pub fn summary_input_items_from_message_entries(
    entries: &[SummaryMessageEntry],
) -> Vec<SummaryInputItem> {
    filter_summary_entries_with_content(entries)
        .iter()
        .map(summary_input_item_from_message_entry)
        .collect()
}

#[must_use]
pub fn summary_input_item_from_message_entry(entry: &SummaryMessageEntry) -> SummaryInputItem {
    let sender = summary_message_sender(entry);
    let role = if entry.role.trim().is_empty() {
        ROLE_USER
    } else {
        entry.role.trim()
    };
    let sender_name = non_empty_trimmed_or(&entry.meta.sender_name, || sender.display_name());
    let sender_username = non_empty_trimmed_or(&entry.meta.sender_username, || {
        sender.username.trim().to_owned()
    });
    let sender_type = non_empty_trimmed_or(&entry.meta.sender_type, || {
        sender.sender_type.trim().to_owned()
    });
    let at = summary_message_entry_timestamp(entry);

    SummaryInputItem {
        kind: "message".to_owned(),
        at,
        message_id: entry.message_id,
        entry_id: summary_message_entry_key(entry),
        role: role.to_owned(),
        sender_name,
        sender_username,
        sender_type,
        text: summary_entry_text(entry),
        original_text: entry.original_text.trim().to_owned(),
        meta_type: entry.meta.message_type.trim().to_owned(),
        annotation: entry.meta.annotation.trim().to_owned(),
        vision_description: entry.meta.vision_description.trim().to_owned(),
        ..SummaryInputItem::default()
    }
}

#[must_use]
pub fn build_summary_input(
    request: &SummaryRequest,
    range_start_at: OffsetDateTime,
    range_end_at: OffsetDateTime,
    entries: &[SummaryMessageEntry],
    selected_summaries: &[StoredSummary],
    assembly: SummaryInputAssembly,
) -> Option<SummaryInput> {
    let first = entries.first()?;
    let last = entries.last()?;
    Some(SummaryInput {
        chat_id: request.chat_id,
        thread_id: request.thread_id,
        scope: request.scope,
        range_start_at,
        range_end_at,
        first_message_id: first.message_id,
        last_message_id: last.message_id,
        first_entry_id: summary_message_entry_key(first),
        last_entry_id: summary_message_entry_key(last),
        raw_message_count: i32::try_from(entries.len()).unwrap_or(i32::MAX),
        covered_message_count: assembly.covered_message_count,
        reused_summary_count: i32::try_from(selected_summaries.len()).unwrap_or(i32::MAX),
        source_summary_ids: summary_ids_from_stored_summaries(selected_summaries),
        requested_by_user_id: request.requested_by_user_id,
        cascade_depth: next_cascade_depth(selected_summaries),
        sources: assembly.sources,
        items: assembly.items,
        ..SummaryInput::default()
    })
}

#[must_use]
pub fn sorted_summary_coverage_ranges(summaries: &[StoredSummary]) -> Vec<SummaryCoverageRange> {
    if summaries.is_empty() {
        return Vec::new();
    }
    let mut ranges = Vec::with_capacity(summaries.len());
    for summary in summaries {
        let start = summary.range_start_at.to_offset(time::UtcOffset::UTC);
        let end = summary.range_end_at.to_offset(time::UtcOffset::UTC);
        if end < start {
            continue;
        }
        if ranges
            .last()
            .is_none_or(|range: &SummaryCoverageRange| start > range.end)
        {
            ranges.push(SummaryCoverageRange { start, end });
            continue;
        }
        if let Some(range) = ranges.last_mut()
            && end > range.end
        {
            range.end = end;
        }
    }
    ranges
}

#[must_use]
pub fn build_ordered_summary_input_assembly(
    entries: &[SummaryMessageEntry],
    summaries: &[StoredSummary],
) -> SummaryInputAssembly {
    let coverage = sorted_summary_coverage_ranges(summaries);
    build_ordered_summary_input_assembly_with_coverage(entries, summaries, &coverage)
}

#[must_use]
pub fn summary_entry_text(entry: &SummaryMessageEntry) -> String {
    let text = entry.text.trim();
    if text.is_empty() {
        entry.caption.trim().to_owned()
    } else {
        text.to_owned()
    }
}

#[must_use]
pub fn summary_message_entry_timestamp(entry: &SummaryMessageEntry) -> OffsetDateTime {
    if let Some(timestamp) = entry.timestamp
        && timestamp != go_zero_time()
    {
        return timestamp.to_offset(time::UtcOffset::UTC);
    }
    if let Some(timestamp) = entry
        .tool_call
        .as_ref()
        .and_then(|call| parse_optional_go_time(call.at.as_deref()))
    {
        return timestamp.to_offset(time::UtcOffset::UTC);
    }
    summary_message_date_time(entry.date)
}

#[must_use]
pub fn summary_message_entry_key(entry: &SummaryMessageEntry) -> String {
    let entry_id = entry.entry_id.trim();
    if !entry_id.is_empty() {
        return entry_id.to_owned();
    }
    let kind = normalize_summary_message_kind(entry);
    if kind != MESSAGE_KIND_TEXT
        && let Some(call) = &entry.tool_call
    {
        return tool_summary_entry_id(kind, entry.message_id, 0, call);
    }
    if entry.message_id != 0 {
        return format!("msg:{}", entry.message_id);
    }
    let sender = summary_message_sender(entry);
    anonymous_summary_entry_id(kind, sender.id, summary_message_entry_timestamp(entry))
}

#[must_use]
pub fn normalize_summary_message_kind(entry: &SummaryMessageEntry) -> &str {
    let kind = entry.kind.trim();
    if !kind.is_empty() {
        return kind;
    }
    if entry.tool_call.is_some() {
        if entry.role.trim() == ROLE_TOOL {
            return MESSAGE_KIND_TOOL_RESPONSE;
        }
        return MESSAGE_KIND_TOOL_REQUEST;
    }
    MESSAGE_KIND_TEXT
}

#[must_use]
pub fn normalize_summary_sources(mut sources: Vec<SummarySource>) -> Vec<SummarySource> {
    sources.sort_by(|left, right| {
        left.range_start_at
            .cmp(&right.range_start_at)
            .then_with(|| {
                summary_source_type_order(left.source_type)
                    .cmp(&summary_source_type_order(right.source_type))
            })
            .then_with(|| left.range_end_at.cmp(&right.range_end_at))
            .then_with(|| left.source_summary_id.cmp(&right.source_summary_id))
    });
    for (index, source) in sources.iter_mut().enumerate() {
        source.source_order = (index + 1) as i32;
    }
    sources
}

#[must_use]
pub fn summary_ids_from_sources(sources: &[SummarySource]) -> Vec<i64> {
    let mut out = Vec::new();
    for source in sources {
        if source.source_type != SummarySourceType::Summary || source.source_summary_id == 0 {
            continue;
        }
        if out.contains(&source.source_summary_id) {
            continue;
        }
        out.push(source.source_summary_id);
    }
    out
}

#[must_use]
pub fn summary_ids_from_stored_summaries(summaries: &[StoredSummary]) -> Vec<i64> {
    summaries.iter().map(|summary| summary.id).collect()
}

#[must_use]
pub const fn summary_source_id_for_storage(source: &SummarySource) -> Option<i64> {
    if matches!(source.source_type, SummarySourceType::Summary) && source.source_summary_id != 0 {
        Some(source.source_summary_id)
    } else {
        None
    }
}

#[must_use]
pub fn summary_model(doc: &SummaryDocument) -> String {
    let model = doc.model.trim();
    if model.is_empty() {
        "unknown".to_owned()
    } else {
        model.to_owned()
    }
}

#[must_use]
pub fn summary_prompt_version(doc: &SummaryDocument) -> String {
    let prompt_version = doc.prompt_version.trim();
    if prompt_version.is_empty() {
        SUMMARY_PROMPT_VERSION.to_owned()
    } else {
        prompt_version.to_owned()
    }
}

#[must_use]
pub fn summary_input_hash(input: &SummaryInput, doc: &SummaryDocument) -> String {
    let input_hash = doc.input_hash.trim();
    if !input_hash.is_empty() {
        return input_hash.to_owned();
    }
    let input_hash = input.input_hash.trim();
    if !input_hash.is_empty() {
        return input_hash.to_owned();
    }
    hash_json(&summary_input_for_hash(input))
}

#[must_use]
pub fn summary_input_token_estimate(input: &SummaryInput, doc: &SummaryDocument) -> i32 {
    if doc.input_token_estimate > 0 {
        return doc.input_token_estimate;
    }
    if input.input_token_estimate > 0 {
        return input.input_token_estimate;
    }
    estimate_summary_tokens(input)
}

#[must_use]
pub fn summary_output_token_estimate(
    html: &str,
    content: &SummaryContent,
    doc: &SummaryDocument,
) -> i32 {
    if doc.output_token_estimate > 0 {
        return doc.output_token_estimate;
    }
    summary_payload_token_estimate(html, content)
}

/// Fetcher `historyOutputTokenEstimate`.
#[must_use]
pub fn history_output_token_estimate(llm: &HistorySummaryLlmResponse) -> i32 {
    summary_payload_token_estimate(&llm.summary_html, &llm.summary_json)
}

#[must_use]
pub fn next_cascade_depth(summaries: &[StoredSummary]) -> i32 {
    summaries
        .iter()
        .map(|summary| summary.cascade_depth + 1)
        .max()
        .unwrap_or_default()
}

#[must_use]
pub fn select_reusable_summary_spans(summaries: &[StoredSummary]) -> Vec<StoredSummary> {
    if summaries.len() <= 1 {
        return summaries.to_vec();
    }
    let mut sorted = summaries.to_vec();
    sorted.sort_by(|left, right| {
        left.range_end_at
            .cmp(&right.range_end_at)
            .then_with(|| left.range_start_at.cmp(&right.range_start_at))
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut best = vec![SummarySelectionState::default(); sorted.len() + 1];
    for (index, candidate) in sorted.iter().enumerate() {
        let base_index = sorted[..index]
            .partition_point(|summary| summary.range_end_at <= candidate.range_start_at);
        let base = best[base_index];
        let take = SummarySelectionState {
            score: base.score + summary_selection_score(candidate),
            count: base.count + 1,
            newest_unix_nanos: base
                .newest_unix_nanos
                .max(candidate.created_at.unix_timestamp_nanos()),
            take: true,
            prev: base_index,
        };
        let skip = SummarySelectionState {
            take: false,
            prev: index,
            ..best[index]
        };
        best[index + 1] = if better_summary_selection(take, skip) {
            take
        } else {
            skip
        };
    }
    compact_selected_summary_spans(&sorted, &best)
}

#[must_use]
pub fn fit_summary_input_to_token_limit(
    input: SummaryInput,
    estimator: Option<&dyn SummaryTokenEstimator>,
    max_input_tokens: i32,
    system_prompt: &str,
) -> (SummaryInput, SummaryInputFitStats) {
    let mut fitted = input.clone();
    let original_tokens = estimate_summary_input_tokens(&fitted, estimator, system_prompt);
    let mut stats = SummaryInputFitStats {
        original_input_token_estimate: original_tokens,
        final_input_token_estimate: original_tokens,
        original_message_item_count: count_summary_message_items(&fitted.items),
        final_message_item_count: count_summary_message_items(&fitted.items),
        ..SummaryInputFitStats::default()
    };
    stamp_summary_input_metadata_with_token_estimate(&mut fitted, original_tokens);
    if max_input_tokens <= 0 || original_tokens <= max_input_tokens {
        return (fitted, stats);
    }

    let candidates = summary_input_drop_candidates(&fitted.items, estimator);
    if candidates.is_empty() {
        return (fitted, stats);
    }

    let mut dropped = Vec::<usize>::with_capacity(candidates.len());
    let base_omitted = fitted.omitted_message_count;
    for candidate in candidates {
        dropped.push(candidate.index);
        stats.dropped_message_count += 1;
        stats.dropped_message_ids.push(candidate.message_id);

        fitted.items = summary_items_without_indexes(&input.items, &dropped);
        fitted.omitted_message_count = base_omitted + stats.dropped_message_count;
        let tokens = estimate_summary_input_tokens(&fitted, estimator, system_prompt);
        stats.final_input_token_estimate = tokens;
        stats.final_message_item_count = count_summary_message_items(&fitted.items);
        stamp_summary_input_metadata_with_token_estimate(&mut fitted, tokens);
        if tokens <= max_input_tokens {
            break;
        }
    }
    (fitted, stats)
}

#[must_use]
pub fn estimate_summary_input_tokens(
    input: &SummaryInput,
    estimator: Option<&dyn SummaryTokenEstimator>,
    system_prompt: &str,
) -> i32 {
    let prompt_tokens = estimate_with(estimator, system_prompt);
    let payload = match serde_json::to_string_pretty(&summary_input_for_hash(input)) {
        Ok(payload) => payload,
        Err(_) => return prompt_tokens,
    };
    prompt_tokens + estimate_with(estimator, &payload)
}

pub fn stamp_summary_input_metadata(input: &mut SummaryInput) {
    let token_estimate = estimate_summary_tokens(&summary_input_for_hash(input));
    stamp_summary_input_metadata_with_token_estimate(input, token_estimate);
}

pub fn stamp_summary_input_metadata_with_token_estimate(
    input: &mut SummaryInput,
    token_estimate: i32,
) {
    input.input_token_estimate = token_estimate;
    input.input_hash = hash_json(&summary_input_for_hash(input));
}

#[must_use]
pub fn build_edge_raw_summary_input(
    parent: &SummaryInput,
    min_messages: i32,
) -> Option<SummaryInput> {
    if parent.reused_summary_count == 0 || parent.items.is_empty() {
        return None;
    }
    let min_messages = if min_messages <= 0 {
        MIN_EDGE_RAW_MESSAGES
    } else {
        min_messages
    };
    let group = edge_raw_message_group(&parent.items)?;
    if i32::try_from(group.len()).ok()? < min_messages {
        return None;
    }
    let first = group.first()?;
    let last = group.last()?;
    let raw_count = i32::try_from(group.len()).ok()?;
    let mut input = SummaryInput {
        chat_id: parent.chat_id,
        thread_id: parent.thread_id,
        scope: parent.scope,
        range_start_at: first.at,
        range_end_at: last.at,
        first_message_id: first.message_id,
        last_message_id: last.message_id,
        first_entry_id: first.entry_id.clone(),
        last_entry_id: last.entry_id.clone(),
        raw_message_count: raw_count,
        covered_message_count: raw_count,
        requested_by_user_id: parent.requested_by_user_id,
        sources: normalize_summary_sources(vec![SummarySource {
            source_type: SummarySourceType::MessageRange,
            range_start_at: first.at,
            range_end_at: last.at,
            first_message_id: first.message_id,
            last_message_id: last.message_id,
            first_entry_id: first.entry_id.clone(),
            last_entry_id: last.entry_id.clone(),
            raw_message_count: raw_count,
            covered_message_count: raw_count,
            ..SummarySource::default()
        }]),
        items: group,
        ..SummaryInput::default()
    };
    stamp_summary_input_metadata(&mut input);
    Some(input)
}

#[must_use]
pub fn merge_edge_summary_input(
    parent: &SummaryInput,
    raw_input: &SummaryInput,
    stored: &StoredSummary,
) -> SummaryInput {
    let mut merged = parent.clone();
    merged.items = merge_edge_summary_items(
        &parent.items,
        &raw_message_entry_id_set(&raw_input.items),
        summary_input_item_from_stored_summary(stored),
    );
    merged.sources = replace_raw_source_with_summary(&parent.sources, raw_input, stored);
    merged.source_summary_ids = summary_ids_from_sources(&merged.sources);
    merged.reused_summary_count =
        i32::try_from(merged.source_summary_ids.len()).unwrap_or(i32::MAX);
    merged.cascade_depth = parent.cascade_depth.max(stored.cascade_depth + 1);
    stamp_summary_input_metadata(&mut merged);
    merged
}

#[must_use]
pub fn summary_events_for_storage(content: &SummaryContent) -> Vec<StoredSummaryEvent> {
    let content = normalize_summary_content(content.clone());
    let mut out = Vec::with_capacity(content.event_details.len());
    for (index, mut event) in content.event_details.into_iter().enumerate() {
        if event.confidence == 0.0 {
            event.confidence = content.quality_score;
        }
        out.push(StoredSummaryEvent {
            source_order: (index + 1) as i32,
            event,
        });
    }
    out
}

#[must_use]
pub fn parse_summary_event_time(value: &str) -> Option<OffsetDateTime> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|value| value.to_offset(time::UtcOffset::UTC))
        .ok()
}

#[must_use]
pub const fn summary_cascade_depth(input: &SummaryInput, doc: &SummaryDocument) -> i32 {
    if doc.cascade_depth > 0 {
        doc.cascade_depth
    } else {
        input.cascade_depth
    }
}

#[must_use]
pub fn summary_quality_score(content: &SummaryContent, doc: &SummaryDocument) -> f64 {
    let score = clamp_quality_score(doc.quality_score);
    if score != 0.0 {
        return score;
    }
    clamp_quality_score(content.quality_score)
}

#[must_use]
pub fn summary_quality_notes(content: &SummaryContent, doc: &SummaryDocument) -> String {
    let notes = doc.quality_notes.trim();
    if notes.is_empty() {
        content.quality_notes.trim().to_owned()
    } else {
        notes.to_owned()
    }
}

#[must_use]
pub fn summary_input_for_hash(input: &SummaryInput) -> SummaryInput {
    let mut input = input.clone();
    input.input_hash.clear();
    input.input_token_estimate = 0;
    input
}

#[must_use]
pub fn hash_text(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

#[must_use]
pub fn estimate_summary_tokens<T: Serialize>(value: &T) -> i32 {
    let payload = match serde_json::to_string(value) {
        Ok(payload) => payload,
        Err(_) => return 0,
    };
    estimate_summary_text_tokens(&payload)
}

#[must_use]
pub fn estimate_summary_text_tokens(value: &str) -> i32 {
    let runes = value.chars().count();
    if runes == 0 {
        return 0;
    }
    (runes / 4).max(1) as i32
}

#[must_use]
pub fn summary_content_has_payload(content: &SummaryContent) -> bool {
    !content.recap.trim().is_empty()
        || !content.events.is_empty()
        || !content.event_details.is_empty()
}

#[must_use]
pub fn normalize_generated_summary_content(content: SummaryContent) -> SummaryContent {
    let events = trim_summary_strings(content.events);
    let event_details = normalize_generated_summary_events(content.event_details, &events);
    SummaryContent {
        events,
        event_details,
        actors: normalize_generated_summary_actors(content.actors),
        recap: content.recap.trim().to_owned(),
        open_questions: trim_summary_strings(content.open_questions),
        source_style: content.source_style.trim().to_owned(),
        quality_score: content.quality_score,
        quality_notes: content.quality_notes.trim().to_owned(),
    }
}

#[must_use]
pub fn normalize_summary_content(content: SummaryContent) -> SummaryContent {
    let events = trim_summary_strings(content.events);
    let mut event_details = normalize_summary_events_for_storage(content.event_details);
    if event_details.is_empty() {
        event_details = summary_events_from_titles(&events);
    }
    let events = if events.is_empty() {
        event_details
            .iter()
            .map(|event| event.title.clone())
            .collect()
    } else {
        events
    };
    SummaryContent {
        events,
        event_details,
        actors: normalize_generated_summary_actors(content.actors),
        recap: content.recap.trim().to_owned(),
        open_questions: trim_summary_strings(content.open_questions),
        source_style: content.source_style.trim().to_owned(),
        quality_score: clamp_quality_score(content.quality_score),
        quality_notes: content.quality_notes.trim().to_owned(),
    }
}

#[must_use]
pub fn render_history_summary_html(content: &SummaryContent) -> String {
    let mut out = String::new();
    if content.events.is_empty() {
        for event in &content.event_details {
            let title = event.title.trim();
            if !title.is_empty() {
                push_summary_event(&mut out, title);
            }
        }
    } else {
        for event in &content.events {
            let event = event.trim();
            if !event.is_empty() {
                push_summary_event(&mut out, event);
            }
        }
    }
    let recap = content.recap.trim();
    if !recap.is_empty() {
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&escape_go_html(recap));
    }
    out
}

fn normalize_generated_summary_actors(input: Vec<SummaryActor>) -> Vec<SummaryActor> {
    let mut actors = Vec::with_capacity(input.len());
    for mut actor in input {
        actor.name = actor.name.trim().to_owned();
        actor.description = actor.description.trim().to_owned();
        if actor.name.is_empty() && actor.description.is_empty() {
            continue;
        }
        actors.push(actor);
    }
    actors
}

fn normalize_generated_summary_events(
    input: Vec<SummaryEvent>,
    fallback_titles: &[String],
) -> Vec<SummaryEvent> {
    let mut event_details = Vec::with_capacity(input.len());
    for mut event in input {
        event.title = event.title.trim().to_owned();
        event.description = event.description.trim().to_owned();
        event.occurred_at = event.occurred_at.trim().to_owned();
        event.actors = trim_summary_strings(event.actors);
        if event.title.is_empty() {
            continue;
        }
        event_details.push(event);
    }
    if event_details.is_empty() {
        return summary_events_from_titles(fallback_titles);
    }
    event_details
}

fn normalize_summary_events_for_storage(input: Vec<SummaryEvent>) -> Vec<SummaryEvent> {
    let mut event_details = Vec::with_capacity(input.len());
    for mut event in input {
        event.title = event.title.trim().to_owned();
        event.description = event.description.trim().to_owned();
        event.occurred_at = event.occurred_at.trim().to_owned();
        event.actors = trim_summary_strings(event.actors);
        event.confidence = clamp_quality_score(event.confidence);
        if event.title.is_empty() {
            continue;
        }
        event_details.push(event);
    }
    event_details
}

fn summary_events_from_titles(titles: &[String]) -> Vec<SummaryEvent> {
    let mut event_details = Vec::with_capacity(titles.len());
    for title in titles {
        if !title.is_empty() {
            event_details.push(SummaryEvent {
                title: title.clone(),
                ..SummaryEvent::default()
            });
        }
    }
    event_details
}

fn trim_summary_strings(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            out.push(trimmed.to_owned());
        }
    }
    out
}

fn summary_payload_token_estimate(html: &str, content: &SummaryContent) -> i32 {
    #[derive(Serialize)]
    struct SummaryOutputTokenPayload<'a> {
        html: &'a str,
        content: &'a SummaryContent,
    }

    estimate_summary_tokens(&SummaryOutputTokenPayload { html, content })
}

fn estimate_with(estimator: Option<&dyn SummaryTokenEstimator>, text: &str) -> i32 {
    match estimator {
        Some(estimator) => estimator.estimate_text(text),
        None => HeuristicSummaryTokenEstimator.estimate_text(text),
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SummarySelectionState {
    score: f64,
    newest_unix_nanos: i128,
    count: i32,
    take: bool,
    prev: usize,
}

fn compact_selected_summary_spans(
    summaries: &[StoredSummary],
    best: &[SummarySelectionState],
) -> Vec<StoredSummary> {
    let mut selected = Vec::new();
    let mut state_index = summaries.len();
    while state_index > 0 {
        let state = best[state_index];
        if state.take {
            selected.push(summaries[state_index - 1].clone());
        }
        state_index = state.prev;
    }
    selected.reverse();
    selected
}

fn summary_selection_score(summary: &StoredSummary) -> f64 {
    let coverage = summary
        .covered_message_count
        .max(summary.raw_message_count)
        .max(0);
    let duration_minutes = (summary.range_end_at - summary.range_start_at)
        .whole_minutes()
        .max(0);
    f64::from(coverage * 1000) + duration_minutes as f64 + summary.quality_score * 50.0
        - f64::from(summary.cascade_depth * 25)
}

fn better_summary_selection(left: SummarySelectionState, right: SummarySelectionState) -> bool {
    if left.score != right.score {
        return left.score > right.score;
    }
    if left.newest_unix_nanos != right.newest_unix_nanos {
        return left.newest_unix_nanos > right.newest_unix_nanos;
    }
    left.count < right.count
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SummaryInputDropCandidate {
    index: usize,
    message_id: i32,
    tokens: i32,
}

fn summary_input_drop_candidates(
    items: &[SummaryInputItem],
    estimator: Option<&dyn SummaryTokenEstimator>,
) -> Vec<SummaryInputDropCandidate> {
    let mut candidates = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if item.kind != "message" {
            continue;
        }
        let mut tokens = estimate_with(estimator, &summary_message_sizing_text(item));
        if tokens <= 0 {
            tokens = estimate_with(estimator, &must_marshal_summary_input_item(item));
        }
        candidates.push(SummaryInputDropCandidate {
            index,
            message_id: item.message_id,
            tokens,
        });
    }
    candidates.sort_by(|left, right| {
        right
            .tokens
            .cmp(&left.tokens)
            .then_with(|| left.message_id.cmp(&right.message_id))
            .then_with(|| left.index.cmp(&right.index))
    });
    candidates
}

fn summary_message_sizing_text(item: &SummaryInputItem) -> String {
    if item.original_text.trim().is_empty()
        && item.annotation.trim().is_empty()
        && item.vision_description.trim().is_empty()
    {
        return item.text.trim().to_owned();
    }
    [(
        item.text.as_str(),
        item.original_text.as_str(),
        item.annotation.as_str(),
        item.vision_description.as_str(),
    )]
    .into_iter()
    .flat_map(|parts| [parts.0, parts.1, parts.2, parts.3])
    .collect::<Vec<_>>()
    .join("\n")
    .trim()
    .to_owned()
}

fn must_marshal_summary_input_item(item: &SummaryInputItem) -> String {
    serde_json::to_string(item).unwrap_or_default()
}

fn summary_items_without_indexes(
    items: &[SummaryInputItem],
    dropped: &[usize],
) -> Vec<SummaryInputItem> {
    items
        .iter()
        .enumerate()
        .filter(|(index, _)| !dropped.contains(index))
        .map(|(_, item)| item.clone())
        .collect()
}

fn count_summary_message_items(items: &[SummaryInputItem]) -> i32 {
    items
        .iter()
        .filter(|item| item.kind == "message")
        .count()
        .try_into()
        .unwrap_or(i32::MAX)
}

fn edge_raw_message_group(items: &[SummaryInputItem]) -> Option<Vec<SummaryInputItem>> {
    let raw_count = raw_message_item_count(items);
    if raw_count == 0 || raw_count == items.len() {
        return None;
    }
    edge_raw_prefix(items, raw_count).or_else(|| edge_raw_suffix(items, raw_count))
}

fn raw_message_item_count(items: &[SummaryInputItem]) -> usize {
    items.iter().filter(|item| item.kind == "message").count()
}

fn edge_raw_prefix(items: &[SummaryInputItem], raw_count: usize) -> Option<Vec<SummaryInputItem>> {
    let prefix_end = items
        .iter()
        .take_while(|item| item.kind == "message")
        .count();
    (prefix_end == raw_count).then(|| items[..prefix_end].to_vec())
}

fn edge_raw_suffix(items: &[SummaryInputItem], raw_count: usize) -> Option<Vec<SummaryInputItem>> {
    let suffix_start = items
        .iter()
        .rposition(|item| item.kind != "message")
        .map_or(0, |index| index + 1);
    (items.len() - suffix_start == raw_count).then(|| items[suffix_start..].to_vec())
}

fn raw_message_entry_id_set(items: &[SummaryInputItem]) -> HashSet<String> {
    items
        .iter()
        .filter(|item| item.kind == "message" && !item.entry_id.is_empty())
        .map(|item| item.entry_id.clone())
        .collect()
}

fn merge_edge_summary_items(
    parent_items: &[SummaryInputItem],
    raw_ids: &HashSet<String>,
    stored_item: SummaryInputItem,
) -> Vec<SummaryInputItem> {
    let mut out = Vec::with_capacity(parent_items.len().saturating_sub(raw_ids.len()) + 1);
    let mut inserted = false;
    for item in parent_items {
        if item.kind == "message" && raw_ids.contains(&item.entry_id) {
            if !inserted {
                out.push(stored_item.clone());
                inserted = true;
            }
            continue;
        }
        out.push(item.clone());
    }
    if !inserted {
        out.push(stored_item);
    }
    sort_summary_input_items(&mut out);
    out
}

fn sort_summary_input_items(items: &mut [SummaryInputItem]) {
    items.sort_by(|left, right| {
        left.at
            .cmp(&right.at)
            .then_with(|| match (left.kind.as_str(), right.kind.as_str()) {
                (left_kind, right_kind) if left_kind == right_kind => {
                    left.message_id.cmp(&right.message_id)
                }
                ("summary", _) => std::cmp::Ordering::Less,
                (_, "summary") => std::cmp::Ordering::Greater,
                _ => left.message_id.cmp(&right.message_id),
            })
    });
}

fn replace_raw_source_with_summary(
    sources: &[SummarySource],
    raw_input: &SummaryInput,
    stored: &StoredSummary,
) -> Vec<SummarySource> {
    let mut out = Vec::with_capacity(sources.len() + 1);
    let mut replaced = false;
    for source in sources {
        if !replaced
            && source.source_type == SummarySourceType::MessageRange
            && source.first_entry_id == raw_input.first_entry_id
            && source.last_entry_id == raw_input.last_entry_id
        {
            out.push(summary_source_from_stored_summary(stored));
            replaced = true;
            continue;
        }
        out.push(source.clone());
    }
    if !replaced {
        out.push(summary_source_from_stored_summary(stored));
    }
    normalize_summary_sources(out)
}

fn max_optional_time_utc(
    left: Option<OffsetDateTime>,
    right: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    match (
        left.map(|value| value.to_offset(time::UtcOffset::UTC)),
        right.map(|value| value.to_offset(time::UtcOffset::UTC)),
    ) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn build_ordered_summary_input_assembly_with_coverage(
    entries: &[SummaryMessageEntry],
    summaries: &[StoredSummary],
    coverage: &[SummaryCoverageRange],
) -> SummaryInputAssembly {
    let mut builder = SummaryInputAssemblyBuilder::new(entries, summaries);
    let mut entry_index = 0;
    let mut cursor = SummaryCoverageCursor::new(coverage);
    for summary in summaries {
        entry_index =
            builder.append_until(entries, entry_index, summary.range_start_at, &mut cursor);
        builder
            .items
            .push(summary_input_item_from_stored_summary(summary));
        builder.append_summary(summary);
    }
    builder.append_remaining(entries, entry_index, &mut cursor);
    builder.result(summaries)
}

#[derive(Clone, Debug, Default)]
struct SummaryInputAssemblyBuilder {
    items: Vec<SummaryInputItem>,
    sources: Vec<SummarySource>,
    pending_summary: Option<SummarySource>,
    extra_pending_summaries: Vec<SummarySource>,
    first: Option<SummaryMessageEntry>,
    last: Option<SummaryMessageEntry>,
    group_count: i32,
    uncovered: i32,
}

impl SummaryInputAssemblyBuilder {
    fn new(entries: &[SummaryMessageEntry], summaries: &[StoredSummary]) -> Self {
        Self {
            items: Vec::with_capacity(entries.len() + summaries.len()),
            sources: Vec::with_capacity(summaries.len() + 1),
            ..Self::default()
        }
    }

    fn append_source(&mut self, mut source: SummarySource) {
        source.source_order = i32::try_from(self.sources.len() + 1).unwrap_or(i32::MAX);
        self.sources.push(source);
    }

    fn append_pending_summary(&mut self, source: SummarySource) {
        if self.pending_summary.is_none() {
            self.pending_summary = Some(source);
            return;
        }
        self.extra_pending_summaries.push(source);
    }

    fn flush_pending_summaries(&mut self) {
        if let Some(source) = self.pending_summary.take() {
            self.append_source(source);
        }
        let extra = std::mem::take(&mut self.extra_pending_summaries);
        for source in extra {
            self.append_source(source);
        }
    }

    fn flush_group(&mut self) {
        if self.group_count > 0 {
            if let (Some(first), Some(last)) = (&self.first, &self.last) {
                self.append_source(SummarySource {
                    source_type: SummarySourceType::MessageRange,
                    range_start_at: summary_message_entry_timestamp(first),
                    range_end_at: summary_message_entry_timestamp(last),
                    first_message_id: first.message_id,
                    last_message_id: last.message_id,
                    first_entry_id: summary_message_entry_key(first),
                    last_entry_id: summary_message_entry_key(last),
                    raw_message_count: self.group_count,
                    covered_message_count: self.group_count,
                    ..SummarySource::default()
                });
            }
            self.group_count = 0;
            self.first = None;
            self.last = None;
        }
        self.flush_pending_summaries();
    }

    fn append_summary(&mut self, summary: &StoredSummary) {
        let source = summary_source_from_stored_summary(summary);
        if self.group_count > 0 {
            self.append_pending_summary(source);
            return;
        }
        self.append_source(source);
    }

    fn append_uncovered_entry(&mut self, entry: &SummaryMessageEntry) {
        self.items
            .push(summary_input_item_from_message_entry(entry));
        if self.group_count == 0 {
            self.first = Some(entry.clone());
        }
        self.last = Some(entry.clone());
        self.group_count += 1;
        self.uncovered += 1;
    }

    fn append_until(
        &mut self,
        entries: &[SummaryMessageEntry],
        mut entry_index: usize,
        before: OffsetDateTime,
        cursor: &mut SummaryCoverageCursor,
    ) -> usize {
        let before = before.to_offset(time::UtcOffset::UTC);
        while entry_index < entries.len()
            && summary_message_entry_timestamp(&entries[entry_index]) < before
        {
            self.append_entry(&entries[entry_index], cursor);
            entry_index += 1;
        }
        entry_index
    }

    fn append_remaining(
        &mut self,
        entries: &[SummaryMessageEntry],
        entry_index: usize,
        cursor: &mut SummaryCoverageCursor,
    ) {
        for entry in &entries[entry_index..] {
            self.append_entry(entry, cursor);
        }
    }

    fn append_entry(&mut self, entry: &SummaryMessageEntry, cursor: &mut SummaryCoverageCursor) {
        if cursor.covers(summary_message_entry_timestamp(entry)) {
            self.flush_group();
            return;
        }
        self.append_uncovered_entry(entry);
    }

    fn result(mut self, summaries: &[StoredSummary]) -> SummaryInputAssembly {
        self.flush_group();
        SummaryInputAssembly {
            items: self.items,
            sources: self.sources,
            covered_message_count: covered_message_count_from_uncovered(self.uncovered, summaries),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SummaryCoverageCursor {
    ranges: Vec<SummaryCoverageRange>,
    index: usize,
}

impl SummaryCoverageCursor {
    fn new(ranges: &[SummaryCoverageRange]) -> Self {
        Self {
            ranges: ranges.to_vec(),
            index: 0,
        }
    }

    fn covers(&mut self, at: OffsetDateTime) -> bool {
        if self.ranges.is_empty() {
            return false;
        }
        let at = at.to_offset(time::UtcOffset::UTC);
        while self.index < self.ranges.len() && self.ranges[self.index].end < at {
            self.index += 1;
        }
        self.index < self.ranges.len() && at >= self.ranges[self.index].start
    }
}

fn covered_message_count_from_uncovered(uncovered: i32, summaries: &[StoredSummary]) -> i32 {
    summaries.iter().fold(uncovered, |covered, summary| {
        covered
            + if summary.covered_message_count > 0 {
                summary.covered_message_count
            } else {
                summary.raw_message_count
            }
    })
}

fn summary_entry_has_content(entry: &SummaryMessageEntry) -> bool {
    normalize_summary_message_kind(entry) == MESSAGE_KIND_TEXT
        && (!summary_entry_text(entry).is_empty()
            || !entry.meta.annotation.trim().is_empty()
            || !entry.meta.vision_description.trim().is_empty()
            || !entry.meta.attachments.is_empty())
}

fn summary_message_sender(entry: &SummaryMessageEntry) -> MessageSender {
    if let Some(sender_chat) = &entry.sender_chat {
        let sender_type = if entry
            .chat
            .as_ref()
            .is_some_and(|chat| chat.id == sender_chat.id)
        {
            SENDER_TYPE_SAME_CHAT
        } else {
            SENDER_TYPE_CHANNEL
        };
        let mut full_name = summary_chat_full_name(sender_chat);
        if !full_name.is_empty() {
            full_name = format!("📣 {full_name}");
        }
        return MessageSender {
            sender_type: sender_type.to_owned(),
            id: sender_chat.id,
            full_name,
            username: sender_chat.username.trim().to_owned(),
            is_bot: false,
        };
    }
    if let Some(from) = &entry.from {
        return MessageSender {
            sender_type: SENDER_TYPE_USER.to_owned(),
            id: from.id,
            full_name: summary_user_full_name(from),
            username: from.username.trim().to_owned(),
            is_bot: from.is_bot,
        };
    }
    MessageSender::system()
}

fn summary_user_full_name(user: &SummaryTelegramUser) -> String {
    let name = format!("{} {}", user.first_name, user.last_name)
        .trim()
        .to_owned();
    if name.is_empty() {
        user.username.clone()
    } else {
        name
    }
}

fn summary_chat_full_name(chat: &SummaryTelegramChat) -> String {
    if !chat.title.is_empty() {
        return chat.title.clone();
    }
    let name = format!("{} {}", chat.first_name, chat.last_name)
        .trim()
        .to_owned();
    if name.is_empty() {
        chat.username.clone()
    } else {
        name
    }
}

fn non_empty_trimmed_or<F>(value: &str, fallback: F) -> String
where
    F: FnOnce() -> String,
{
    let value = value.trim();
    if value.is_empty() {
        fallback()
    } else {
        value.to_owned()
    }
}

fn tool_summary_entry_id(kind: &str, message_id: i32, index: i32, call: &ToolCall) -> String {
    let name = call.name.trim();
    let reference = call.r#ref.trim();
    if reference.is_empty() {
        format!("{kind}:{message_id}:{name}:{index}")
    } else {
        format!("{kind}:{message_id}:{name}:{reference}")
    }
}

fn anonymous_summary_entry_id(kind: &str, sender_id: i64, at: OffsetDateTime) -> String {
    format!("anon:{kind}:{sender_id}:{}", at.unix_timestamp_nanos())
}

fn summary_message_date_time(date: i64) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(date)
        .unwrap_or_else(|_| go_zero_time())
        .to_offset(time::UtcOffset::UTC)
}

fn parse_optional_go_time(value: Option<&str>) -> Option<OffsetDateTime> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let parsed = OffsetDateTime::parse(value, &Rfc3339).ok()?;
    (parsed != go_zero_time()).then(|| parsed.to_offset(time::UtcOffset::UTC))
}

fn deserialize_optional_rfc3339<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                return Ok(None);
            }
            let parsed = OffsetDateTime::parse(value, &Rfc3339).map_err(de::Error::custom)?;
            Ok((parsed != go_zero_time()).then(|| parsed.to_offset(time::UtcOffset::UTC)))
        }
        other => Err(de::Error::custom(format!(
            "expected RFC3339 timestamp string or null, got {other}"
        ))),
    }
}

fn hash_json<T: Serialize>(value: &T) -> String {
    let payload = match serde_json::to_string(value) {
        Ok(payload) => payload,
        Err(_) => return String::new(),
    };
    hash_text(&payload)
}

fn summary_source_type_order(source_type: SummarySourceType) -> i32 {
    match source_type {
        SummarySourceType::Summary => 0,
        SummarySourceType::MessageRange => 1,
    }
}

fn clamp_quality_score(score: f64) -> f64 {
    score.clamp(0.0, 1.0)
}

fn go_zero_time() -> OffsetDateTime {
    let date = match time::Date::from_calendar_date(1, time::Month::January, 1) {
        Ok(date) => date,
        Err(_) => unreachable!("year 1 January 1 is representable"),
    };
    time::PrimitiveDateTime::new(date, time::Time::MIDNIGHT).assume_utc()
}

fn push_summary_event(out: &mut String, event: &str) {
    out.push_str("• ");
    out.push_str(&escape_go_html(event));
    out.push('\n');
}

fn escape_go_html(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            _ => out.push(ch),
        }
    }
    out
}

fn contains_any_ascii_fold(haystack: &str, needles: &[&str]) -> bool {
    needles
        .iter()
        .any(|needle| contains_ascii_case_insensitive(haystack, needle))
}

fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| ascii_eq_ignore_case(window, needle))
}

fn ascii_eq_ignore_case(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn deserialize_f64_loose<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(0.0);
    };
    Ok(match value {
        Value::Number(number) => number.as_f64().unwrap_or_default(),
        Value::Bool(true) => 1.0,
        Value::Bool(false) | Value::Null => 0.0,
        Value::String(value) => value.trim().parse::<f64>().unwrap_or_default(),
        Value::Array(_) | Value::Object(_) => 0.0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_history_summary_response_accepts_wrapped_summary_json() {
        let got = decode_history_summary_response(
            r#"{
                "summary_json": {
                    "events": [" shipped "],
                    "actors": [{"name": " Alice ", "description": " drove the build "}],
                    "recap": " Everyone survived <barely>.",
                    "open_questions": [],
                    "source_style": "incident report"
                }
            }"#,
        )
        .expect("decode wrapped summary");

        assert_eq!(got.summary_json.events, vec!["shipped"]);
        assert_eq!(
            got.summary_json.actors,
            vec![SummaryActor {
                name: "Alice".to_owned(),
                description: "drove the build".to_owned(),
            }]
        );
        assert!(got.summary_html.contains("• shipped"));
        assert!(
            got.summary_html
                .contains("Everyone survived &lt;barely&gt;.")
        );
    }

    #[test]
    fn decode_history_summary_response_accepts_direct_summary_json() {
        let got = decode_history_summary_response(
            r#"{
                "events": ["edge gap collapsed"],
                "actors": [],
                "recap": "The fresh messages became their own little recap.",
                "open_questions": [],
                "source_style": "chronicle"
            }"#,
        )
        .expect("decode direct summary");

        assert_eq!(got.summary_json.events, vec!["edge gap collapsed"]);
        assert!(
            got.summary_html
                .contains("The fresh messages became their own little recap.")
        );
    }

    #[test]
    fn decode_history_summary_response_rerenders_html_and_rejects_empty() {
        let got = decode_history_summary_response(
            r#"{
                "summary_html": "<b>stale</b>",
                "summary_json": {
                    "event_details": [{"title": "  deploy done  "}],
                    "recap": " ok "
                }
            }"#,
        )
        .expect("decode summary");

        assert_eq!(got.summary_html, "• deploy done\n\nok");

        let err = decode_history_summary_response(r#"{"summary_json":{"events":[" "]}}"#)
            .expect_err("empty summary");
        assert!(matches!(err, HistorySummaryDecodeError::EmptySummaryJson));
        assert_eq!(err.to_string(), "summary_json is empty");
    }

    #[test]
    fn decode_history_summary_response_salvages_loose_wrapped_json() {
        let got = decode_history_summary_response(
            r#"model said:
            {summary_json:{events:[" shipped "],recap:"ok",quality_score:"0.75"}}
            thanks"#,
        )
        .expect("salvage wrapped summary");

        assert_eq!(got.summary_json.events, vec!["shipped"]);
        assert_eq!(got.summary_json.recap, "ok");
        assert_eq!(got.summary_json.quality_score, 0.75);
        assert_eq!(got.summary_html, "• shipped\n\nok");
    }

    #[test]
    fn decode_history_summary_response_salvages_loose_direct_json() {
        let got = decode_history_summary_response(
            r#"{events:["edge"],event_details:[{title:" ignored "}],actors:[{name:" Bob "}],recap:"done"}"#,
        )
        .expect("salvage direct summary");

        assert_eq!(got.summary_json.events, vec!["edge"]);
        assert_eq!(got.summary_json.event_details[0].title, "ignored");
        assert_eq!(got.summary_json.actors[0].name, "Bob");
        assert_eq!(got.summary_html, "• edge\n\ndone");
    }

    #[test]
    fn history_summary_known_keys_match_go_catalog() {
        assert_eq!(
            history_summary_known_keys(),
            &[
                "summary_html",
                "summary_json",
                "events",
                "event_details",
                "title",
                "actors",
                "recap",
                "open_questions",
                "source_style",
                "quality_score",
                "quality_notes",
                "name",
                "description",
                "occurred_at",
                "confidence",
            ]
        );
    }

    #[test]
    fn history_summary_provider_and_model_match_go_defaults() {
        assert_eq!(history_summary_provider(" GENKIT "), "genkit");
        assert_eq!(history_summary_provider(""), "aifarm");
        assert_eq!(history_summary_provider("weird"), "aifarm");
        assert_eq!(
            history_summary_model(" explicit ", "aifarm", "memory", Some("runtime")),
            "explicit"
        );
        assert_eq!(
            history_summary_model("", "aifarm", " memory ", Some("runtime")),
            "memory"
        );
        assert_eq!(
            history_summary_model("", "aifarm", "", Some("runtime")),
            AIFARM_DEFAULT_HISTORY_SUMMARY_MODEL
        );
        assert_eq!(
            history_summary_model("", "genkit", "memory", Some(" runtime ")),
            "runtime"
        );
        assert_eq!(
            history_summary_model("", "genkit", "memory", None),
            MODEL_GEMINI_FLASH_FALLBACK
        );
    }

    #[test]
    fn history_summary_timeout_and_retry_rules_match_go() {
        assert_eq!(history_summary_timeout_seconds(42), 42);
        assert_eq!(
            history_summary_timeout_seconds(0),
            DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS
        );
        assert!(history_summary_generate_error_retryable(
            "wrapped deadline exceeded",
            false,
            false,
            Some("provider_timeout")
        ));
        assert!(history_summary_generate_error_retryable(
            "openrouter 429 rate limit",
            false,
            false,
            None
        ));
        assert!(history_summary_generate_error_retryable(
            "Client.Timeout exceeded while awaiting headers",
            false,
            false,
            None
        ));
        assert!(history_summary_generate_error_retryable(
            "OpenRouter TEMPORARILY UNAVAILABLE",
            false,
            false,
            None
        ));
        assert!(!history_summary_generate_error_retryable(
            "deadline exceeded",
            true,
            false,
            None
        ));
        assert!(!history_summary_generate_error_retryable(
            "context canceled",
            false,
            true,
            None
        ));
        assert!(!history_summary_generate_error_retryable(
            "provider_timeout",
            false,
            false,
            Some("")
        ));
    }

    #[test]
    fn summary_scope_defaults_to_current_thread_like_go() {
        assert_eq!(resolve_summary_scope("", 77), (SummaryScope::Thread, 77));
        assert_eq!(resolve_summary_scope("chat", 77), (SummaryScope::Chat, 0));
        assert_eq!(resolve_summary_scope("thread", 0), (SummaryScope::Chat, 0));
        assert_eq!(
            history_summary_scope(" THREAD ", Some(77)),
            (SummaryScope::Thread, 77)
        );
    }

    #[test]
    fn normalize_summary_request_caps_window_hours_and_messages() {
        let now = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("now");
        let since = OffsetDateTime::parse("2026-05-19T10:00:00+02:00", &Rfc3339).expect("since");
        let request = normalize_summary_request(SummaryRequest {
            scope: SummaryScope::Thread,
            thread_id: 42,
            window: SummaryWindow::Messages,
            hours: 72,
            message_count: 900,
            since: Some(since),
            now: Some(now),
            ..SummaryRequest::default()
        });

        assert_eq!(request.scope, SummaryScope::Thread);
        assert_eq!(request.thread_id, 42);
        assert_eq!(request.window, SummaryWindow::Messages);
        assert_eq!(request.hours, 24);
        assert_eq!(request.message_count, MAX_SUMMARY_MESSAGES);
        assert_eq!(request.now, Some(now));
        assert_eq!(
            request.since,
            Some(OffsetDateTime::parse("2026-05-19T08:00:00Z", &Rfc3339).expect("since utc"))
        );

        let defaulted = normalize_summary_request(SummaryRequest::default());
        assert_eq!(defaulted.scope, SummaryScope::Chat);
        assert_eq!(defaulted.window, SummaryWindow::Day);
        assert_eq!(defaulted.hours, 24);
        assert_eq!(defaulted.message_count, 100);
        assert!(defaulted.now.is_some());
    }

    #[test]
    fn parse_history_summary_since_matches_go_rfc3339_rules() {
        assert_eq!(parse_history_summary_since(" ").expect("blank"), None);
        assert_eq!(
            parse_history_summary_since(" 2026-05-20T10:00:00Z ")
                .expect("since")
                .expect("value"),
            OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("expected")
        );
        let err = parse_history_summary_since("2026-05-20").expect_err("invalid");
        assert!(err.to_string().starts_with("invalid since timestamp:"));
    }

    #[test]
    fn summary_requested_range_clamps_to_ttl_and_reset_like_go() {
        let now = OffsetDateTime::parse("2026-04-23T12:00:00Z", &Rfc3339).expect("now");
        let reset_at = now - time::Duration::hours(3);
        let request = normalize_summary_request(SummaryRequest {
            window: SummaryWindow::Since,
            since: Some(now - time::Duration::hours(48)),
            now: Some(now),
            ..SummaryRequest::default()
        });

        let (start, end) = summary_requested_range(&request, Some(reset_at));

        assert_eq!(start, reset_at);
        assert_eq!(end, now);

        let future_since = normalize_summary_request(SummaryRequest {
            window: SummaryWindow::Since,
            since: Some(now + time::Duration::hours(1)),
            now: Some(now),
            ..SummaryRequest::default()
        });
        assert_eq!(summary_requested_range(&future_since, None).0, now);

        let hours = normalize_summary_request(SummaryRequest {
            window: SummaryWindow::Hours,
            hours: 2,
            now: Some(now),
            ..SummaryRequest::default()
        });
        assert_eq!(
            summary_requested_range(&hours, None),
            (now - time::Duration::hours(2), now)
        );

        let messages = normalize_summary_request(SummaryRequest {
            window: SummaryWindow::Messages,
            now: Some(now),
            ..SummaryRequest::default()
        });
        assert_eq!(
            summary_requested_range(&messages, None),
            (now - time::Duration::hours(SUMMARY_WINDOW_TTL_HOURS), now)
        );
    }

    #[test]
    fn summary_reset_at_matches_go_chat_thread_precedence() {
        let chat = OffsetDateTime::parse("2026-04-23T10:00:00+02:00", &Rfc3339).expect("chat");
        let thread = OffsetDateTime::parse("2026-04-23T11:00:00Z", &Rfc3339).expect("thread");

        assert_eq!(
            summary_reset_at(Some(chat), SummaryScope::Chat, 42, Some(thread)),
            Some(OffsetDateTime::parse("2026-04-23T08:00:00Z", &Rfc3339).expect("chat utc"))
        );
        assert_eq!(
            summary_reset_at(Some(chat), SummaryScope::Thread, 0, Some(thread)),
            Some(OffsetDateTime::parse("2026-04-23T08:00:00Z", &Rfc3339).expect("chat utc"))
        );
        assert_eq!(
            summary_reset_at(Some(chat), SummaryScope::Thread, 42, Some(thread)),
            Some(thread)
        );
        assert_eq!(
            summary_reset_at(None, SummaryScope::Thread, 42, Some(thread)),
            Some(thread)
        );
        assert_eq!(summary_reset_at(None, SummaryScope::Chat, 0, None), None);
    }

    #[test]
    fn summary_message_entry_decode_and_input_item_match_go_mapping() {
        let entry = decode_summary_message_entry_payload(
            br#"{
                "entry_id": " msg:7 ",
                "timestamp": "2026-05-20T10:00:00+02:00",
                "message_id": 7,
                "date": 1779271200,
                "caption": " caption fallback ",
                "original_text": " original ",
                "from": {"id": 42, "first_name": " Alice ", "last_name": " Wave ", "username": "alice"},
                "chat": {"id": 100, "type": "private"},
                "meta": {
                    "type": " image ",
                    "annotation": " note ",
                    "vision_description": " seen ",
                    "sender_name": " Meta Name ",
                    "sender_username": " meta_user ",
                    "sender_type": " meta_type "
                }
            }"#,
        )
        .expect("decode entry");

        let item = summary_input_item_from_message_entry(&entry);

        assert_eq!(item.kind, "message");
        assert_eq!(
            item.at,
            OffsetDateTime::parse("2026-05-20T08:00:00Z", &Rfc3339).expect("utc")
        );
        assert_eq!(item.message_id, 7);
        assert_eq!(item.entry_id, "msg:7");
        assert_eq!(item.role, ROLE_USER);
        assert_eq!(item.sender_name, "Meta Name");
        assert_eq!(item.sender_username, "meta_user");
        assert_eq!(item.sender_type, "meta_type");
        assert_eq!(item.text, "caption fallback");
        assert_eq!(item.original_text, "original");
        assert_eq!(item.meta_type, "image");
        assert_eq!(item.annotation, "note");
        assert_eq!(item.vision_description, "seen");
    }

    #[test]
    fn summary_message_entry_sender_fallbacks_match_go_resolution() {
        let user_entry = SummaryMessageEntry {
            message_id: 9,
            from: Some(SummaryTelegramUser {
                id: 55,
                username: "fallback_user".to_owned(),
                ..SummaryTelegramUser::default()
            }),
            text: "hello".to_owned(),
            ..SummaryMessageEntry::default()
        };
        let user_item = summary_input_item_from_message_entry(&user_entry);
        assert_eq!(user_item.sender_name, "fallback_user");
        assert_eq!(user_item.sender_username, "fallback_user");
        assert_eq!(user_item.sender_type, SENDER_TYPE_USER);

        let same_chat = SummaryMessageEntry {
            message_id: 10,
            sender_chat: Some(SummaryTelegramChat {
                id: -100,
                title: "Channel Title".to_owned(),
                username: "channel".to_owned(),
                ..SummaryTelegramChat::default()
            }),
            chat: Some(SummaryTelegramChat {
                id: -100,
                ..SummaryTelegramChat::default()
            }),
            text: "announcement".to_owned(),
            ..SummaryMessageEntry::default()
        };
        let same_chat_item = summary_input_item_from_message_entry(&same_chat);
        assert_eq!(same_chat_item.sender_name, "📣 Channel Title");
        assert_eq!(same_chat_item.sender_username, "channel");
        assert_eq!(same_chat_item.sender_type, SENDER_TYPE_SAME_CHAT);

        let channel = SummaryMessageEntry {
            sender_chat: Some(SummaryTelegramChat {
                id: -200,
                first_name: "Fallback".to_owned(),
                last_name: "Chat".to_owned(),
                ..SummaryTelegramChat::default()
            }),
            chat: Some(SummaryTelegramChat {
                id: -100,
                ..SummaryTelegramChat::default()
            }),
            text: "announcement".to_owned(),
            ..SummaryMessageEntry::default()
        };
        assert_eq!(
            summary_input_item_from_message_entry(&channel).sender_type,
            SENDER_TYPE_CHANNEL
        );
        assert_eq!(
            summary_input_item_from_message_entry(&channel).sender_name,
            "📣 Fallback Chat"
        );
    }

    #[test]
    fn summary_entry_filter_keeps_go_visible_content_only() {
        let blank = SummaryMessageEntry {
            text: " ".to_owned(),
            ..SummaryMessageEntry::default()
        };
        let annotation = SummaryMessageEntry {
            meta: ChatMessageMeta {
                annotation: " note ".to_owned(),
                ..ChatMessageMeta::default()
            },
            ..SummaryMessageEntry::default()
        };
        let attachment = SummaryMessageEntry {
            meta: ChatMessageMeta {
                attachments: vec![openplotva_core::ChatAttachment {
                    kind: "image".to_owned(),
                    ..openplotva_core::ChatAttachment::default()
                }],
                ..ChatMessageMeta::default()
            },
            ..SummaryMessageEntry::default()
        };
        let tool = SummaryMessageEntry {
            role: ROLE_TOOL.to_owned(),
            tool_call: Some(ToolCall {
                name: "lookup".to_owned(),
                ..ToolCall::default()
            }),
            text: "tool text".to_owned(),
            ..SummaryMessageEntry::default()
        };

        let got = filter_summary_entries_with_content(&[
            blank.clone(),
            annotation.clone(),
            attachment.clone(),
            tool,
        ]);

        assert_eq!(got, vec![annotation, attachment]);
        assert_eq!(
            summary_input_items_from_message_entries(&got)
                .iter()
                .map(|item| item.kind.as_str())
                .collect::<Vec<_>>(),
            vec!["message", "message"]
        );
    }

    #[test]
    fn summary_entry_timestamp_and_keys_match_go_fallbacks() {
        let timestamp = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("ts");
        let with_timestamp = SummaryMessageEntry {
            timestamp: Some(timestamp),
            date: 1,
            ..SummaryMessageEntry::default()
        };
        assert_eq!(summary_message_entry_timestamp(&with_timestamp), timestamp);

        let tool_time = SummaryMessageEntry {
            role: ROLE_TOOL.to_owned(),
            message_id: 77,
            tool_call: Some(ToolCall {
                name: " lookup ".to_owned(),
                r#ref: " ref-1 ".to_owned(),
                at: Some("2026-05-20T11:00:00+02:00".to_owned()),
                ..ToolCall::default()
            }),
            ..SummaryMessageEntry::default()
        };
        assert_eq!(
            summary_message_entry_timestamp(&tool_time),
            OffsetDateTime::parse("2026-05-20T09:00:00Z", &Rfc3339).expect("tool utc")
        );
        assert_eq!(
            normalize_summary_message_kind(&tool_time),
            MESSAGE_KIND_TOOL_RESPONSE
        );
        assert_eq!(
            summary_message_entry_key(&tool_time),
            "tool_response:77:lookup:ref-1"
        );

        let text = SummaryMessageEntry {
            message_id: 8,
            date: 1,
            ..SummaryMessageEntry::default()
        };
        assert_eq!(summary_message_entry_key(&text), "msg:8");

        let anonymous = SummaryMessageEntry {
            date: 1,
            from: Some(SummaryTelegramUser {
                id: 5,
                ..SummaryTelegramUser::default()
            }),
            ..SummaryMessageEntry::default()
        };
        assert_eq!(
            summary_message_entry_key(&anonymous),
            "anon:text:5:1000000000"
        );
    }

    #[test]
    fn ordered_summary_input_assembly_matches_go_pending_source_order() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let entries = vec![
            summary_message_entry(1, base, "before"),
            summary_message_entry(2, base + time::Duration::minutes(10), "covered start"),
            summary_message_entry(3, base + time::Duration::minutes(20), "covered end"),
            summary_message_entry(4, base + time::Duration::minutes(30), "after"),
        ];
        let summary = StoredSummary {
            id: 44,
            range_start_at: base + time::Duration::minutes(10),
            range_end_at: base + time::Duration::minutes(20),
            first_message_id: 2,
            last_message_id: 3,
            first_entry_id: "msg:2".to_owned(),
            last_entry_id: "msg:3".to_owned(),
            raw_message_count: 2,
            covered_message_count: 5,
            summary_html: "summary".to_owned(),
            ..StoredSummary::default()
        };

        let assembly =
            build_ordered_summary_input_assembly(&entries, std::slice::from_ref(&summary));

        assert_eq!(
            assembly
                .items
                .iter()
                .map(|item| (item.kind.as_str(), item.message_id, item.summary_id))
                .collect::<Vec<_>>(),
            vec![("message", 1, 0), ("summary", 0, 44), ("message", 4, 0)]
        );
        assert_eq!(
            assembly
                .sources
                .iter()
                .map(|source| (
                    source.source_order,
                    source.source_type,
                    source.first_message_id,
                    source.last_message_id,
                    source.source_summary_id,
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, SummarySourceType::MessageRange, 1, 1, 0),
                (2, SummarySourceType::Summary, 2, 3, 44),
                (3, SummarySourceType::MessageRange, 4, 4, 0),
            ]
        );
        assert_eq!(assembly.covered_message_count, 7);
    }

    #[test]
    fn sorted_summary_coverage_ranges_merge_like_go_cursor() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let ranges = sorted_summary_coverage_ranges(&[
            stored_summary(1, base, (10, 20), 1, 0, 0.0, 0),
            stored_summary(2, base, (20, 30), 1, 0, 0.0, 0),
            stored_summary(3, base, (40, 35), 1, 0, 0.0, 0),
        ]);

        assert_eq!(
            ranges,
            vec![SummaryCoverageRange {
                start: base + time::Duration::minutes(10),
                end: base + time::Duration::minutes(30),
            }]
        );

        let mut cursor = SummaryCoverageCursor::new(&ranges);
        assert!(!cursor.covers(base + time::Duration::minutes(9)));
        assert!(cursor.covers(base + time::Duration::minutes(10)));
        assert!(cursor.covers(base + time::Duration::minutes(30)));
        assert!(!cursor.covers(base + time::Duration::minutes(31)));
    }

    #[test]
    fn build_summary_input_matches_go_field_selection() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let entries = vec![
            summary_message_entry(1, base, "one"),
            summary_message_entry(2, base + time::Duration::minutes(1), "two"),
        ];
        let summary = StoredSummary {
            id: 9,
            cascade_depth: 3,
            covered_message_count: 4,
            raw_message_count: 2,
            ..StoredSummary::default()
        };
        let assembly = SummaryInputAssembly {
            items: vec![summary_message_item(1, base, "one")],
            sources: vec![SummarySource {
                source_order: 1,
                source_type: SummarySourceType::Summary,
                source_summary_id: 9,
                ..SummarySource::default()
            }],
            covered_message_count: 5,
        };
        let request = SummaryRequest {
            chat_id: 100,
            thread_id: 7,
            scope: SummaryScope::Thread,
            requested_by_user_id: 55,
            ..SummaryRequest::default()
        };

        let mut input = build_summary_input(
            &request,
            base,
            base + time::Duration::minutes(1),
            &entries,
            &[summary],
            assembly,
        )
        .expect("input");
        stamp_summary_input_metadata(&mut input);

        assert_eq!(input.chat_id, 100);
        assert_eq!(input.thread_id, 7);
        assert_eq!(input.scope, SummaryScope::Thread);
        assert_eq!(input.first_message_id, 1);
        assert_eq!(input.last_message_id, 2);
        assert_eq!(input.first_entry_id, "msg:1");
        assert_eq!(input.last_entry_id, "msg:2");
        assert_eq!(input.raw_message_count, 2);
        assert_eq!(input.covered_message_count, 5);
        assert_eq!(input.reused_summary_count, 1);
        assert_eq!(input.source_summary_ids, vec![9]);
        assert_eq!(input.requested_by_user_id, 55);
        assert_eq!(input.cascade_depth, 4);
        assert!(!input.input_hash.is_empty());
        assert!(input.input_token_estimate > 0);
    }

    #[test]
    fn normalize_generated_summary_content_matches_go_fetcher() {
        let got = normalize_generated_summary_content(SummaryContent {
            events: vec![" shipped ".to_owned(), " ".to_owned(), "fixed".to_owned()],
            event_details: vec![SummaryEvent {
                title: " ".to_owned(),
                ..SummaryEvent::default()
            }],
            actors: vec![
                SummaryActor {
                    name: " Alice ".to_owned(),
                    description: " owner ".to_owned(),
                },
                SummaryActor {
                    name: " ".to_owned(),
                    description: " ".to_owned(),
                },
            ],
            open_questions: vec![" next? ".to_owned(), String::new()],
            recap: " recap ".to_owned(),
            source_style: " style ".to_owned(),
            quality_notes: " notes ".to_owned(),
            ..SummaryContent::default()
        });

        assert_eq!(got.events, vec!["shipped", "fixed"]);
        assert_eq!(
            got.event_details,
            vec![
                SummaryEvent {
                    title: "shipped".to_owned(),
                    ..SummaryEvent::default()
                },
                SummaryEvent {
                    title: "fixed".to_owned(),
                    ..SummaryEvent::default()
                },
            ]
        );
        assert_eq!(
            got.actors,
            vec![SummaryActor {
                name: "Alice".to_owned(),
                description: "owner".to_owned(),
            }]
        );
        assert_eq!(got.open_questions, vec!["next?"]);
        assert_eq!(got.recap, "recap");
        assert_eq!(got.source_style, "style");
        assert_eq!(got.quality_notes, "notes");
    }

    #[test]
    fn render_history_summary_html_escapes_events_and_recap() {
        let got = render_history_summary_html(&SummaryContent {
            events: vec![" shipped ".to_owned(), String::new()],
            recap: " Everyone survived <barely> & \"ok\".".to_owned(),
            ..SummaryContent::default()
        });

        assert_eq!(
            got,
            "• shipped\n\nEveryone survived &lt;barely&gt; &amp; &#34;ok&#34;."
        );
    }

    #[test]
    fn render_history_summary_html_falls_back_to_event_details() {
        let got = render_history_summary_html(&SummaryContent {
            event_details: vec![
                SummaryEvent {
                    title: " ".to_owned(),
                    ..SummaryEvent::default()
                },
                SummaryEvent {
                    title: "edge gap collapsed".to_owned(),
                    ..SummaryEvent::default()
                },
            ],
            ..SummaryContent::default()
        });

        assert_eq!(got, "• edge gap collapsed\n");
    }

    #[test]
    fn summary_content_payload_matches_go_gate() {
        assert!(!summary_content_has_payload(&SummaryContent::default()));
        assert!(summary_content_has_payload(&SummaryContent {
            recap: " x ".to_owned(),
            ..SummaryContent::default()
        }));
        assert!(summary_content_has_payload(&SummaryContent {
            events: vec![String::new()],
            ..SummaryContent::default()
        }));
        assert!(summary_content_has_payload(&SummaryContent {
            event_details: vec![SummaryEvent::default()],
            ..SummaryContent::default()
        }));
    }

    #[test]
    fn prepare_stored_summary_matches_go_defaults_and_source_order() {
        let start = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("start");
        let end = OffsetDateTime::parse("2026-05-20T11:00:00Z", &Rfc3339).expect("end");
        let later = OffsetDateTime::parse("2026-05-20T12:00:00Z", &Rfc3339).expect("later");
        let input = SummaryInput {
            chat_id: 10,
            thread_id: 20,
            scope: SummaryScope::Thread,
            range_start_at: start,
            range_end_at: later,
            first_message_id: 1,
            last_message_id: 9,
            first_entry_id: "msg:1".to_owned(),
            last_entry_id: "msg:9".to_owned(),
            raw_message_count: 7,
            covered_message_count: 11,
            source_summary_ids: vec![99],
            requested_by_user_id: 55,
            input_hash: " input-hash ".to_owned(),
            input_token_estimate: 321,
            cascade_depth: 2,
            sources: vec![
                SummarySource {
                    source_type: SummarySourceType::MessageRange,
                    range_start_at: start,
                    range_end_at: later,
                    raw_message_count: 3,
                    covered_message_count: 3,
                    ..SummarySource::default()
                },
                SummarySource {
                    source_type: SummarySourceType::Summary,
                    source_summary_id: 44,
                    range_start_at: start,
                    range_end_at: end,
                    raw_message_count: 4,
                    covered_message_count: 4,
                    ..SummarySource::default()
                },
                SummarySource {
                    source_type: SummarySourceType::Summary,
                    source_summary_id: 44,
                    range_start_at: start,
                    range_end_at: later,
                    raw_message_count: 4,
                    covered_message_count: 4,
                    ..SummarySource::default()
                },
            ],
            ..SummaryInput::default()
        };
        let doc = SummaryDocument {
            content: SummaryContent {
                event_details: vec![SummaryEvent {
                    title: " shipped ".to_owned(),
                    confidence: 2.5,
                    ..SummaryEvent::default()
                }],
                recap: " recap ".to_owned(),
                quality_score: 0.5,
                quality_notes: " content notes ".to_owned(),
                ..SummaryContent::default()
            },
            html: " <b>ok</b> ".to_owned(),
            prompt_hash: " prompt-hash ".to_owned(),
            quality_score: 1.5,
            quality_notes: " doc notes ".to_owned(),
            ..SummaryDocument::default()
        };

        let got = prepare_stored_summary(&input, &doc).expect("prepare summary");

        assert_eq!(got.stored.summary_html, "<b>ok</b>");
        assert_eq!(got.stored.model, "unknown");
        assert_eq!(got.stored.prompt_version, SUMMARY_PROMPT_VERSION);
        assert_eq!(got.stored.input_hash, "input-hash");
        assert_eq!(got.stored.prompt_hash, "prompt-hash");
        assert_eq!(got.stored.input_token_estimate, 321);
        assert_eq!(got.stored.cascade_depth, 2);
        assert_eq!(got.stored.quality_score, 1.0);
        assert_eq!(got.stored.quality_notes, "doc notes");
        assert_eq!(got.stored.summary_json.events, vec!["shipped"]);
        assert_eq!(got.stored.summary_json.event_details[0].confidence, 1.0);
        assert_eq!(got.stored.source_summary_ids, vec![44]);
        assert_eq!(
            got.sources
                .iter()
                .map(|source| (
                    source.source_order,
                    source.source_type,
                    source.source_summary_id
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, SummarySourceType::Summary, 44),
                (2, SummarySourceType::Summary, 44),
                (3, SummarySourceType::MessageRange, 0),
            ]
        );
        assert_eq!(
            String::from_utf8(got.summary_json).expect("summary json"),
            r#"{"events":["shipped"],"event_details":[{"title":"shipped","confidence":1.0}],"actors":[],"recap":"recap","open_questions":[],"source_style":"","quality_score":0.5,"quality_notes":"content notes"}"#
        );
    }

    #[test]
    fn summary_source_fallbacks_and_token_estimates_match_go() {
        let start = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("start");
        let input = SummaryInput {
            chat_id: 1,
            range_start_at: start,
            range_end_at: start,
            first_message_id: 10,
            last_message_id: 12,
            first_entry_id: "msg:10".to_owned(),
            last_entry_id: "msg:12".to_owned(),
            raw_message_count: 3,
            covered_message_count: 3,
            source_summary_ids: vec![7, 8],
            ..SummaryInput::default()
        };
        let sources = summary_sources_for_storage(&input);

        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].source_order, 1);
        assert_eq!(sources[0].source_type, SummarySourceType::MessageRange);
        assert_eq!(summary_source_ids_for_storage(&input, &sources), vec![7, 8]);

        let llm = HistorySummaryLlmResponse {
            summary_html: "hello".to_owned(),
            summary_json: SummaryContent {
                events: vec!["world".to_owned()],
                recap: "done".to_owned(),
                ..SummaryContent::default()
            },
        };
        let compact = serde_json::to_string(&serde_json::json!({
            "html": llm.summary_html,
            "content": llm.summary_json,
        }))
        .expect("token payload");
        assert_eq!(
            history_output_token_estimate(&llm),
            (compact.chars().count() / 4).max(1) as i32
        );
    }

    #[test]
    fn fit_summary_input_to_token_limit_drops_largest_messages_first() {
        let start = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("start");
        let input = SummaryInput {
            chat_id: 1,
            omitted_message_count: 4,
            items: vec![
                summary_message_item(1, start, "short"),
                SummaryInputItem {
                    kind: "summary".to_owned(),
                    summary_id: 9,
                    summary_html: "kept".to_owned(),
                    at: start,
                    range_start_at: start,
                    range_end_at: start,
                    ..SummaryInputItem::default()
                },
                summary_message_item(
                    2,
                    start,
                    "long long long long long long long long long long long long",
                ),
                SummaryInputItem {
                    kind: "message".to_owned(),
                    message_id: 3,
                    text: "visible".to_owned(),
                    original_text: "original original".to_owned(),
                    annotation: "annotation".to_owned(),
                    vision_description: "vision".to_owned(),
                    at: start,
                    range_start_at: start,
                    range_end_at: start,
                    ..SummaryInputItem::default()
                },
            ],
            ..SummaryInput::default()
        };

        let (got, stats) = fit_summary_input_to_token_limit(input, None, 1, "system");

        assert_eq!(stats.original_message_item_count, 3);
        assert_eq!(stats.final_message_item_count, 0);
        assert_eq!(stats.dropped_message_count, 3);
        assert_eq!(stats.dropped_message_ids, vec![2, 3, 1]);
        assert_eq!(got.omitted_message_count, 7);
        assert_eq!(got.items.len(), 1);
        assert_eq!(got.items[0].kind, "summary");
        assert_eq!(got.input_token_estimate, stats.final_input_token_estimate);
        assert!(!got.input_hash.is_empty());
    }

    #[test]
    fn estimate_and_stamp_summary_input_metadata_match_go_shape() {
        let start = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("start");
        let mut input = SummaryInput {
            chat_id: 1,
            input_hash: "stale".to_owned(),
            input_token_estimate: 999,
            items: vec![summary_message_item(1, start, "hello")],
            ..SummaryInput::default()
        };
        let original = estimate_summary_input_tokens(&input, None, "prompt");
        let (fitted, stats) =
            fit_summary_input_to_token_limit(input.clone(), None, original, "prompt");

        assert_eq!(stats.original_input_token_estimate, original);
        assert_eq!(stats.dropped_message_count, 0);
        assert_eq!(fitted.items.len(), 1);
        assert_eq!(fitted.input_token_estimate, original);
        assert_ne!(fitted.input_hash, "stale");

        stamp_summary_input_metadata(&mut input);
        assert_eq!(
            input.input_token_estimate,
            estimate_summary_tokens(&summary_input_for_hash(&input))
        );
        assert_ne!(input.input_hash, hash_text("should not match"));
    }

    #[test]
    fn summary_events_for_storage_apply_go_order_confidence_and_time_parse() {
        let events = summary_events_for_storage(&SummaryContent {
            events: vec!["fallback".to_owned()],
            event_details: vec![
                SummaryEvent {
                    title: " first ".to_owned(),
                    occurred_at: "2026-05-20T10:00:00+02:00".to_owned(),
                    confidence: 0.0,
                    ..SummaryEvent::default()
                },
                SummaryEvent {
                    title: " second ".to_owned(),
                    confidence: 0.25,
                    ..SummaryEvent::default()
                },
            ],
            quality_score: 0.75,
            ..SummaryContent::default()
        });

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].source_order, 1);
        assert_eq!(events[0].event.title, "first");
        assert_eq!(events[0].event.confidence, 0.75);
        assert_eq!(events[1].source_order, 2);
        assert_eq!(events[1].event.confidence, 0.25);
        assert_eq!(
            parse_summary_event_time(&events[0].event.occurred_at),
            Some(OffsetDateTime::parse("2026-05-20T08:00:00Z", &Rfc3339).expect("utc"))
        );
        assert_eq!(parse_summary_event_time(""), None);
        assert_eq!(parse_summary_event_time("not a timestamp"), None);
    }

    #[test]
    fn select_reusable_summary_spans_match_go_weighted_selection_rules() {
        let base = OffsetDateTime::parse("2026-04-23T10:00:00Z", &Rfc3339).expect("base");
        let selected = select_reusable_summary_spans(&[
            stored_summary(1, base, (0, 60), 0, 120, 0.0, 0),
            stored_summary(2, base, (10, 50), 0, 180, 0.0, 0),
            stored_summary(3, base, (90, 120), 0, 180, 0.0, 0),
        ]);

        assert_eq!(summary_ids_from_stored_summaries(&selected), vec![1, 3]);

        let newest = select_reusable_summary_spans(&[
            stored_summary(1, base, (0, 10), 1, 60, 0.0, 0),
            stored_summary(2, base, (0, 10), 1, 120, 0.0, 0),
        ]);
        assert_eq!(summary_ids_from_stored_summaries(&newest), vec![2]);

        let fewer = select_reusable_summary_spans(&[
            stored_summary(1, base, (0, 10), 1, 60, 0.0, 0),
            stored_summary(2, base, (10, 20), 1, 60, 0.0, 0),
            stored_summary(3, base, (0, 20), 2, 60, 0.0, 0),
        ]);
        assert_eq!(summary_ids_from_stored_summaries(&fewer), vec![3]);

        let touching = select_reusable_summary_spans(&[
            stored_summary(1, base, (0, 30), 5, 0, 0.0, 0),
            stored_summary(2, base, (30, 50), 5, 0, 0.0, 0),
            stored_summary(3, base, (0, 50), 9, 0, 0.0, 0),
        ]);
        assert_eq!(summary_ids_from_stored_summaries(&touching), vec![1, 2]);
    }

    #[test]
    fn summary_selection_score_uses_quality_and_cascade_penalty() {
        let base = OffsetDateTime::parse("2026-04-23T10:00:00Z", &Rfc3339).expect("base");
        let selected = select_reusable_summary_spans(&[
            stored_summary(1, base, (0, 10), 1, 60, 0.0, 0),
            stored_summary(2, base, (0, 10), 1, 60, 1.0, 0),
        ]);
        assert_eq!(summary_ids_from_stored_summaries(&selected), vec![2]);

        let selected = select_reusable_summary_spans(&[
            stored_summary(1, base, (0, 10), 1, 60, 0.0, 0),
            stored_summary(2, base, (0, 10), 1, 60, 0.0, 1),
        ]);
        assert_eq!(summary_ids_from_stored_summaries(&selected), vec![1]);
        assert_eq!(next_cascade_depth(&selected), 1);
    }

    #[test]
    fn build_edge_raw_summary_input_and_merge_match_go() {
        let base = OffsetDateTime::parse("2026-04-23T10:00:00Z", &Rfc3339).expect("base");
        let reused = StoredSummary {
            id: 10,
            range_start_at: base + time::Duration::minutes(10),
            range_end_at: base + time::Duration::minutes(20),
            first_message_id: 10,
            last_message_id: 20,
            first_entry_id: "msg:10".to_owned(),
            last_entry_id: "msg:20".to_owned(),
            raw_message_count: 11,
            covered_message_count: 11,
            summary_html: "old summary".to_owned(),
            ..StoredSummary::default()
        };
        let parent = SummaryInput {
            chat_id: 100,
            thread_id: 7,
            scope: SummaryScope::Thread,
            range_start_at: base,
            range_end_at: reused.range_end_at,
            first_message_id: 1,
            last_message_id: 20,
            first_entry_id: "msg:1".to_owned(),
            last_entry_id: "msg:20".to_owned(),
            raw_message_count: 13,
            covered_message_count: 13,
            reused_summary_count: 1,
            source_summary_ids: vec![10],
            sources: normalize_summary_sources(vec![
                SummarySource {
                    source_type: SummarySourceType::MessageRange,
                    range_start_at: base,
                    range_end_at: base + time::Duration::minutes(1),
                    first_message_id: 1,
                    last_message_id: 2,
                    first_entry_id: "msg:1".to_owned(),
                    last_entry_id: "msg:2".to_owned(),
                    raw_message_count: 2,
                    covered_message_count: 2,
                    ..SummarySource::default()
                },
                summary_source_from_stored_summary(&reused),
            ]),
            items: vec![
                summary_message_item(1, base, "first"),
                summary_message_item(2, base + time::Duration::minutes(1), "second"),
                summary_input_item_from_stored_summary(&reused),
            ],
            ..SummaryInput::default()
        };

        let raw_input = build_edge_raw_summary_input(&parent, 2).expect("edge raw input");

        assert_eq!(raw_input.items.len(), 2);
        assert_eq!(raw_input.first_entry_id, "msg:1");
        assert_eq!(raw_input.last_entry_id, "msg:2");
        assert_eq!(raw_input.raw_message_count, 2);
        assert!(!raw_input.input_hash.is_empty());

        let edge_summary = StoredSummary {
            id: 20,
            range_start_at: raw_input.range_start_at,
            range_end_at: raw_input.range_end_at,
            first_message_id: raw_input.first_message_id,
            last_message_id: raw_input.last_message_id,
            first_entry_id: raw_input.first_entry_id.clone(),
            last_entry_id: raw_input.last_entry_id.clone(),
            raw_message_count: raw_input.raw_message_count,
            covered_message_count: raw_input.covered_message_count,
            summary_html: "edge summary".to_owned(),
            ..StoredSummary::default()
        };
        let merged = merge_edge_summary_input(&parent, &raw_input, &edge_summary);

        assert_eq!(merged.items.len(), 2);
        assert_eq!(merged.items[0].summary_id, 20);
        assert_eq!(merged.items[1].summary_id, 10);
        assert_eq!(merged.source_summary_ids, vec![20, 10]);
        assert_eq!(merged.reused_summary_count, 2);
        assert_ne!(parent.input_hash, merged.input_hash);
    }

    #[test]
    fn build_edge_raw_summary_input_requires_single_edge_gap() {
        let base = OffsetDateTime::parse("2026-04-23T10:00:00Z", &Rfc3339).expect("base");
        let parent = SummaryInput {
            reused_summary_count: 1,
            items: vec![
                summary_message_item(1, base, "first"),
                SummaryInputItem {
                    kind: "summary".to_owned(),
                    at: base + time::Duration::minutes(1),
                    summary_id: 10,
                    ..SummaryInputItem::default()
                },
                summary_message_item(2, base + time::Duration::minutes(2), "second"),
            ],
            ..SummaryInput::default()
        };

        assert_eq!(build_edge_raw_summary_input(&parent, 1), None);
    }

    fn summary_message_item(message_id: i32, at: OffsetDateTime, text: &str) -> SummaryInputItem {
        SummaryInputItem {
            kind: "message".to_owned(),
            message_id,
            entry_id: format!("msg:{message_id}"),
            text: text.to_owned(),
            at,
            range_start_at: at,
            range_end_at: at,
            ..SummaryInputItem::default()
        }
    }

    fn summary_message_entry(
        message_id: i32,
        timestamp: OffsetDateTime,
        text: &str,
    ) -> SummaryMessageEntry {
        SummaryMessageEntry {
            message_id,
            entry_id: format!("msg:{message_id}"),
            timestamp: Some(timestamp),
            date: timestamp.unix_timestamp(),
            text: text.to_owned(),
            from: Some(SummaryTelegramUser {
                id: i64::from(message_id),
                first_name: format!("User {message_id}"),
                ..SummaryTelegramUser::default()
            }),
            ..SummaryMessageEntry::default()
        }
    }

    fn stored_summary(
        id: i64,
        base: OffsetDateTime,
        range_minutes: (i64, i64),
        covered_message_count: i32,
        created_minutes: i64,
        quality_score: f64,
        cascade_depth: i32,
    ) -> StoredSummary {
        StoredSummary {
            id,
            range_start_at: base + time::Duration::minutes(range_minutes.0),
            range_end_at: base + time::Duration::minutes(range_minutes.1),
            covered_message_count,
            raw_message_count: covered_message_count,
            created_at: base + time::Duration::minutes(created_minutes),
            quality_score,
            cascade_depth,
            ..StoredSummary::default()
        }
    }
}
