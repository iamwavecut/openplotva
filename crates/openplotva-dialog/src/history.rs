use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use openplotva_core::{ChatAttachment, ChatMessageMeta, ToolCall};

pub const DEFAULT_CONTEXT_HISTORY_LIMIT: usize = 15;

const REPLY_CHAIN_EXTRA_DEPTH: usize = 4;

pub const ROLE_USER: &str = "user";
pub const ROLE_MODEL: &str = "model";
pub const ROLE_TOOL: &str = "tool";

pub type MessageKind = &'static str;
pub const MESSAGE_KIND_TEXT: MessageKind = "text";
pub const MESSAGE_KIND_TOOL_REQUEST: MessageKind = "tool_request";
pub const MESSAGE_KIND_TOOL_RESPONSE: MessageKind = "tool_response";

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct HistoryMessage {
    /// History entry ID.
    #[serde(default, rename = "EntryID", skip_serializing_if = "String::is_empty")]
    pub entry_id: String,
    /// Dialog role.
    #[serde(default, rename = "Role", skip_serializing_if = "String::is_empty")]
    pub role: String,
    /// Message kind.
    #[serde(default, rename = "Kind", skip_serializing_if = "String::is_empty")]
    pub kind: String,
    /// Sender name.
    #[serde(default, rename = "Name", skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Visible text.
    #[serde(default, rename = "Text", skip_serializing_if = "String::is_empty")]
    pub text: String,
    /// Original pre-normalized text.
    #[serde(
        default,
        rename = "OriginalText",
        skip_serializing_if = "String::is_empty"
    )]
    pub original_text: String,
    #[serde(default, rename = "Timestamp", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<OffsetDateTime>,
    /// Telegram message ID.
    #[serde(default, rename = "MessageID", skip_serializing_if = "is_zero_i32")]
    pub message_id: i32,
    /// Telegram topic/thread ID.
    #[serde(default, rename = "ThreadID", skip_serializing_if = "is_zero_i32")]
    pub thread_id: i32,
    /// Telegram user ID.
    #[serde(default, rename = "UserID", skip_serializing_if = "is_zero_i64")]
    pub user_id: i64,
    /// Reply target message ID.
    #[serde(default, rename = "ReplyToID", skip_serializing_if = "is_zero_i32")]
    pub reply_to_id: i32,
    /// Reply target sender name.
    #[serde(
        default,
        rename = "ReplyToName",
        skip_serializing_if = "String::is_empty"
    )]
    pub reply_to_name: String,
    #[serde(default, rename = "Meta")]
    pub meta: ChatMessageMeta,
    #[serde(default, rename = "ToolCall", skip_serializing_if = "Option::is_none")]
    pub tool_call: Option<ToolCall>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DialogMessage {
    /// Telegram message ID.
    #[serde(default, rename = "ID")]
    pub id: i32,
    /// Current text.
    #[serde(default, rename = "Text")]
    pub text: String,
    /// Normalized text.
    #[serde(default, rename = "Normalized")]
    pub normalized: String,
    /// Original pre-normalized text.
    #[serde(default, rename = "OriginalText")]
    pub original_text: String,
    /// Message timestamp.
    #[serde(default, rename = "Timestamp", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<OffsetDateTime>,
    /// Reply target message ID.
    #[serde(default, rename = "ReplyToID")]
    pub reply_to_id: i32,
    /// Reply target display name.
    #[serde(default, rename = "ReplyToName")]
    pub reply_to_name: String,
    #[serde(default, rename = "Meta")]
    pub meta: ChatMessageMeta,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Persona {
    /// Mood label.
    #[serde(default, rename = "Mood")]
    pub mood: String,
    /// Custom persona text.
    #[serde(default, rename = "CustomPersona")]
    pub custom_persona: String,
    /// Daily persona fields.
    #[serde(default, rename = "Persona", skip_serializing_if = "Option::is_none")]
    pub persona: Option<DailyPersona>,
    /// Whether profanity is allowed.
    #[serde(default, rename = "Profanity")]
    pub profanity: bool,
    /// Whether obscenifier is enabled.
    #[serde(default, rename = "Obscenifier")]
    pub obscenifier: bool,
    /// Reactivity score.
    #[serde(default, rename = "Reactivity")]
    pub reactivity: i32,
    /// Proactivity score.
    #[serde(default, rename = "Proactivity")]
    pub proactivity: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DailyPersona {
    /// Persona name.
    #[serde(default, rename = "Name")]
    pub name: String,
    /// Persona tone.
    #[serde(default, rename = "Tone")]
    pub tone: String,
    /// Persona background.
    #[serde(default, rename = "Background")]
    pub background: String,
    /// Persona boundaries.
    #[serde(default, rename = "Boundaries")]
    pub boundaries: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DialogContext {
    /// Telegram chat ID.
    #[serde(default, rename = "ChatID")]
    pub chat_id: i64,
    /// Telegram topic/thread ID.
    #[serde(default, rename = "ThreadID", skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<i32>,
    /// Chat title.
    #[serde(default, rename = "ChatTitle")]
    pub chat_title: String,
    /// Bot display name.
    #[serde(default, rename = "BotName")]
    pub bot_name: String,
    /// Locale.
    #[serde(default, rename = "Locale")]
    pub locale: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DialogUser {
    /// Telegram user ID.
    #[serde(default, rename = "ID")]
    pub id: i64,
    /// Full display name.
    #[serde(default, rename = "FullName")]
    pub full_name: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct MultimodalImage {
    /// Telegram file unique ID.
    #[serde(default, rename = "FileUniqueID")]
    pub file_unique_id: String,
    /// Source label.
    #[serde(default, rename = "Source")]
    pub source: String,
    /// Prompt label.
    #[serde(default, rename = "Label")]
    pub label: String,
    /// Media caption.
    #[serde(default, rename = "Caption")]
    pub caption: String,
    /// Data URL.
    #[serde(default, rename = "DataURL")]
    pub data_url: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DialogInput {
    /// Chat context.
    #[serde(default, rename = "Context")]
    pub context: DialogContext,
    /// Requesting user.
    #[serde(default, rename = "User")]
    pub user: DialogUser,
    /// Current message.
    #[serde(default, rename = "Message")]
    pub message: DialogMessage,
    /// Persona inputs.
    #[serde(default, rename = "Persona")]
    pub persona: Persona,
    /// Selected history.
    #[serde(default, rename = "History", skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<HistoryMessage>,
    /// Multimodal image inputs.
    #[serde(
        default,
        rename = "MultimodalImages",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub multimodal_images: Vec<MultimodalImage>,
    /// Reference context snippets.
    #[serde(
        default,
        rename = "ReferenceContext",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub reference_context: Vec<String>,
    /// Shield context text.
    #[serde(
        default,
        rename = "ShieldContext",
        skip_serializing_if = "String::is_empty"
    )]
    pub shield_context: String,
    /// Request timestamp.
    #[serde(default, rename = "Timestamp", skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<OffsetDateTime>,
    /// Requested model.
    #[serde(default, rename = "Model", skip_serializing_if = "String::is_empty")]
    pub model: String,
    /// Maximum output token count.
    #[serde(
        default,
        rename = "MaxOutputTokens",
        skip_serializing_if = "is_zero_i32"
    )]
    pub max_output_tokens: i32,
    /// Guest mode flag.
    #[serde(default, rename = "GuestMode", skip_serializing_if = "is_false")]
    pub guest_mode: bool,
    /// Whether tools are disabled.
    #[serde(default, rename = "DisableTools", skip_serializing_if = "is_false")]
    pub disable_tools: bool,
    /// Per-request thinking override (routing-assignment scoped); providers
    /// fall back to their configured default when unset.
    #[serde(
        default,
        rename = "EnableThinking",
        skip_serializing_if = "Option::is_none"
    )]
    pub enable_thinking: Option<bool>,
    /// Capture-only X-ray of what fed this turn (memories recalled, persona,
    /// settings). Never serialized into the LLM request; the runtime lifts it
    /// onto the in-memory run record for the admin "Context X-ray".
    #[serde(skip)]
    pub context_capture: Option<TurnContextArtifact>,
}

/// One memory card that surfaced during recall for a turn — the scored skeleton
/// kept for the admin X-ray, not the full card body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CapturedMemory {
    pub card_id: i64,
    pub salience: f64,
    pub confidence: f64,
    pub card_type: String,
    pub competing: bool,
    pub preview: String,
}

/// The persona resolved for a turn.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PersonaSnapshot {
    pub name: String,
    pub mood: String,
    pub custom: bool,
    pub profanity: bool,
    pub obscenifier: bool,
}

/// One applied chat customization, as a display label/value pair.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SettingKv {
    pub label: String,
    pub value: String,
}

/// Light, in-memory X-ray of everything that shaped one dialog turn's request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TurnContextArtifact {
    pub memories: Vec<CapturedMemory>,
    pub persona: Option<PersonaSnapshot>,
    pub settings: Vec<SettingKv>,
    pub history_len: i32,
    pub tools_offered: bool,
    pub shield_on: bool,
    pub reference_context_chars: i32,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DialogOutput {
    /// Provider name.
    #[serde(default, rename = "Provider", skip_serializing_if = "String::is_empty")]
    pub provider: String,
    /// Provider this response fell back from.
    #[serde(
        default,
        rename = "FallbackFrom",
        skip_serializing_if = "String::is_empty"
    )]
    pub fallback_from: String,
    /// Fallback error text.
    #[serde(
        default,
        rename = "FallbackError",
        skip_serializing_if = "String::is_empty"
    )]
    pub fallback_error: String,
    /// Raw provider response.
    #[serde(default, rename = "Response", skip_serializing_if = "String::is_empty")]
    pub response: String,
    /// Final answer.
    #[serde(default, rename = "Answer", skip_serializing_if = "String::is_empty")]
    pub answer: String,
    /// Tool calls.
    #[serde(default, rename = "ToolCalls", skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    /// Provider trace artifacts for runtime diagnostics. This is intentionally not part of
    #[serde(default, skip)]
    pub trace: Option<DialogTraceArtifacts>,
    /// Provider trace artifacts for every model request that happened while producing this
    #[serde(default, skip)]
    pub trace_events: Vec<DialogTraceArtifacts>,
}

/// Provider trace artifacts used by the runtime diagnostic API.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DialogTraceArtifacts {
    /// Provider name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider: String,
    /// Provider request kind.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub flow: String,
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub iteration: i32,
    /// Provider model.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
    /// Raw provider request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_request: Option<Value>,
    /// Raw provider response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_response: Option<Value>,
    /// Provider-side cache material resolved before the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_cache_content: Option<Value>,
    /// Provider transport snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<Value>,
    /// Provider inference parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inference_params: Option<Value>,
    /// Provider token usage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<DialogTraceUsage>,
    /// Provider timing counters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timings: Option<Value>,
    /// Provider error text.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// Raw provider prompt size.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub prompt_chars: i32,
    /// Raw provider prompt message count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub prompt_messages: i32,
    /// Reference docs character count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub docs_chars: i32,
}

#[derive(Clone, Debug, Default, Eq, Deserialize, PartialEq, Serialize)]
pub struct DialogTraceUsage {
    /// Prompt/input token count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub input_tokens: i32,
    /// Candidate/output token count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub output_tokens: i32,
    /// Total token count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub total_tokens: i32,
    /// Cached-content token count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub cached_tokens: i32,
    /// Reasoning/thought token count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub thoughts_tokens: i32,
    /// Tool-use prompt token count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub tool_use_prompt_tokens: i32,
    /// Provider traffic type label.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub traffic_type: String,
}

#[must_use]
pub fn clone_history_messages(history: &[HistoryMessage]) -> Vec<HistoryMessage> {
    history.to_vec()
}

#[must_use]
pub fn conversation_projection(history: &[HistoryMessage]) -> Vec<HistoryMessage> {
    history
        .iter()
        .filter(|item| normalize_history_message_kind(item) == MESSAGE_KIND_TEXT)
        .cloned()
        .collect()
}

#[must_use]
pub fn normalize_history_message(mut item: HistoryMessage) -> HistoryMessage {
    if item.kind.is_empty() {
        item.kind = normalize_history_message_kind(&item).to_owned();
    }
    item
}

#[must_use]
pub fn normalize_history_message_kind(item: &HistoryMessage) -> &str {
    if !item.kind.is_empty() {
        return item.kind.as_str();
    }
    if item.tool_call.is_some() {
        if item.role == ROLE_TOOL {
            return MESSAGE_KIND_TOOL_RESPONSE;
        }
        return MESSAGE_KIND_TOOL_REQUEST;
    }
    MESSAGE_KIND_TEXT
}

#[must_use]
pub fn select_history_messages_for_context(
    turns: &[HistoryMessage],
    limit: usize,
    trigger_message_id: i32,
    thread_id: i32,
) -> Vec<HistoryMessage> {
    let mut selected = select_history_messages_excluding(turns, limit, trigger_message_id);
    if thread_id != 0 {
        selected = select_history_messages_with_thread_priority(
            turns,
            limit,
            trigger_message_id,
            thread_id,
        );
    }
    if !should_include_reply_ancestors(&selected, trigger_message_id) {
        return selected;
    }

    let by_id = index_history_messages_by_id(turns);
    let ancestors =
        collect_history_reply_ancestors(&by_id, trigger_message_id, REPLY_CHAIN_EXTRA_DEPTH);
    if ancestors.is_empty() {
        return selected;
    }

    sort_history_messages_by_timeline(
        turns,
        &append_missing_history_messages(&selected, &ancestors),
    )
}

#[must_use]
pub fn select_llm_history_messages_for_context(
    turns: &[HistoryMessage],
    limit: usize,
    trigger_message_id: i32,
    thread_id: i32,
) -> Vec<HistoryMessage> {
    if turns.is_empty() || limit == 0 {
        return Vec::new();
    }

    let visible = conversation_projection(turns);
    let selected_visible =
        select_history_messages_for_context(&visible, limit, trigger_message_id, thread_id);
    if selected_visible.is_empty() {
        return Vec::new();
    }

    let allowed_message_ids = selected_visible
        .iter()
        .filter_map(|turn| (turn.message_id != 0).then_some(turn.message_id))
        .collect::<HashSet<_>>();
    let mut selected = turns
        .iter()
        .filter(|turn| turn.message_id != 0 && allowed_message_ids.contains(&turn.message_id))
        .cloned()
        .collect::<Vec<_>>();
    selected.reverse();
    selected
}

fn select_history_messages_excluding(
    turns: &[HistoryMessage],
    limit: usize,
    exclude_message_id: i32,
) -> Vec<HistoryMessage> {
    select_history_messages_filtered(turns, limit, exclude_message_id, None)
}

fn select_history_messages_with_thread_priority(
    turns: &[HistoryMessage],
    limit: usize,
    exclude_message_id: i32,
    thread_id: i32,
) -> Vec<HistoryMessage> {
    select_history_messages_filtered(turns, limit, exclude_message_id, Some(thread_id))
}

fn select_history_messages_filtered(
    turns: &[HistoryMessage],
    limit: usize,
    exclude_message_id: i32,
    thread_filter: Option<i32>,
) -> Vec<HistoryMessage> {
    if turns.is_empty() || limit == 0 {
        return Vec::new();
    }

    let trigger = turns
        .iter()
        .find(|turn| {
            turn.message_id == exclude_message_id
                && normalize_history_message_kind(turn) == MESSAGE_KIND_TEXT
        })
        .or_else(|| {
            turns
                .iter()
                .find(|turn| turn.message_id == exclude_message_id)
        });
    let mut seen = HashMap::<i32, usize>::new();
    let mut selected = Vec::<HistoryMessage>::with_capacity(limit);
    for turn in turns {
        let Some(turn) =
            history_selection_candidate(turn, exclude_message_id, thread_filter, trigger)
        else {
            continue;
        };
        if let Some(idx) = seen.get(&turn.message_id).copied() {
            selected[idx] = merge_history_messages(selected[idx].clone(), turn);
            continue;
        }
        seen.insert(turn.message_id, selected.len());
        selected.push(turn);
        if selected.len() >= limit {
            break;
        }
    }
    selected.reverse();
    selected
}

fn history_selection_candidate(
    turn: &HistoryMessage,
    exclude_message_id: i32,
    thread_filter: Option<i32>,
    trigger: Option<&HistoryMessage>,
) -> Option<HistoryMessage> {
    if turn.message_id == 0 || turn.message_id == exclude_message_id {
        return None;
    }
    if thread_filter.is_some_and(|thread_id| turn.thread_id != thread_id) {
        return None;
    }
    if trigger.is_some_and(|trigger| history_message_is_after(turn, trigger)) {
        return None;
    }
    let mut turn = turn.clone();
    turn.text = turn.text.trim().to_owned();
    turn.original_text = turn.original_text.trim().to_owned();
    if turn.original_text == turn.text {
        turn.original_text.clear();
    }
    history_message_has_context_content(&turn).then_some(turn)
}

fn history_message_is_after(message: &HistoryMessage, trigger: &HistoryMessage) -> bool {
    if let (Some(message_at), Some(trigger_at)) = (message.timestamp, trigger.timestamp)
        && message_at != trigger_at
    {
        return message_at > trigger_at;
    }
    message.message_id > trigger.message_id
}

fn should_include_reply_ancestors(selected: &[HistoryMessage], trigger_message_id: i32) -> bool {
    !selected.is_empty() && trigger_message_id != 0 && REPLY_CHAIN_EXTRA_DEPTH > 0
}

fn append_missing_history_messages(
    selected: &[HistoryMessage],
    extras: &[HistoryMessage],
) -> Vec<HistoryMessage> {
    let mut seen = selected
        .iter()
        .filter_map(|turn| (turn.message_id != 0).then_some(turn.message_id))
        .collect::<HashSet<_>>();
    let mut out = Vec::with_capacity(selected.len() + extras.len());
    out.extend_from_slice(selected);
    for turn in extras {
        if turn.message_id == 0 || !seen.insert(turn.message_id) {
            continue;
        }
        out.push(turn.clone());
    }
    out
}

fn index_history_messages_by_id(turns: &[HistoryMessage]) -> HashMap<i32, HistoryMessage> {
    let mut indexed = HashMap::<i32, HistoryMessage>::with_capacity(turns.len());
    for turn in turns {
        if turn.message_id == 0 {
            continue;
        }
        let mut turn = turn.clone();
        turn.text = turn.text.trim().to_owned();
        turn.original_text = turn.original_text.trim().to_owned();
        if turn.original_text == turn.text {
            turn.original_text.clear();
        }
        indexed
            .entry(turn.message_id)
            .and_modify(|existing| {
                *existing = merge_history_messages(existing.clone(), turn.clone())
            })
            .or_insert(turn);
    }
    indexed
}

fn collect_history_reply_ancestors(
    by_id: &HashMap<i32, HistoryMessage>,
    trigger_message_id: i32,
    max_depth: usize,
) -> Vec<HistoryMessage> {
    if by_id.is_empty() || trigger_message_id == 0 || max_depth == 0 {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(max_depth);
    let mut current_id = trigger_message_id;
    while out.len() < max_depth {
        let Some(current) = by_id.get(&current_id) else {
            break;
        };
        if current.reply_to_id == 0 {
            break;
        }
        let Some(parent) = by_id.get(&current.reply_to_id) else {
            break;
        };
        out.push(parent.clone());
        current_id = current.reply_to_id;
    }
    out.reverse();
    out
}

fn sort_history_messages_by_timeline(
    turns: &[HistoryMessage],
    selected: &[HistoryMessage],
) -> Vec<HistoryMessage> {
    if selected.len() <= 1 {
        return selected.to_vec();
    }

    let mut positions = HashMap::with_capacity(turns.len());
    for (idx, turn) in turns.iter().enumerate() {
        if turn.message_id == 0 {
            continue;
        }
        positions.entry(turn.message_id).or_insert(idx);
    }

    let mut out = selected.to_vec();
    out.sort_by(|left, right| history_message_timeline_order(&positions, left, right));
    out
}

fn history_message_timeline_order(
    positions: &HashMap<i32, usize>,
    left: &HistoryMessage,
    right: &HistoryMessage,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    let left_index = positions.get(&left.message_id);
    let right_index = positions.get(&right.message_id);
    if let (Some(left_index), Some(right_index)) = (left_index, right_index)
        && left_index != right_index
    {
        return right_index.cmp(left_index);
    }
    if let (Some(left_ts), Some(right_ts)) = (left.timestamp, right.timestamp)
        && left_ts != right_ts
    {
        return left_ts.cmp(&right_ts);
    }
    match left.message_id.cmp(&right.message_id) {
        Ordering::Equal => left.user_id.cmp(&right.user_id),
        other => other,
    }
}

fn merge_history_messages(mut current: HistoryMessage, next: HistoryMessage) -> HistoryMessage {
    current.timestamp = latest_history_timestamp(current.timestamp, next.timestamp);
    if current.reply_to_id == 0 && next.reply_to_id != 0 {
        current.reply_to_id = next.reply_to_id;
        current.reply_to_name = next.reply_to_name;
    }
    current.text = fill_blank_history_string(&current.text, &next.text);
    current.original_text = fill_blank_history_string(&current.original_text, &next.original_text);
    if history_meta_has_context_content(&next.meta, &next.text, &next.original_text)
        && !history_meta_has_context_content(&current.meta, &current.text, &current.original_text)
    {
        current.meta = next.meta;
    }
    if current.user_id == 0 && next.user_id != 0 {
        current.user_id = next.user_id;
    }
    current.name = fill_blank_history_string(&current.name, &next.name);
    current
}

fn latest_history_timestamp(
    current: Option<OffsetDateTime>,
    next: Option<OffsetDateTime>,
) -> Option<OffsetDateTime> {
    match (current, next) {
        (None, next) => next,
        (current, None) => current,
        (Some(current), Some(next)) if next > current => Some(next),
        (current, _) => current,
    }
}

fn fill_blank_history_string(current: &str, next: &str) -> String {
    if current.trim().is_empty() && !next.trim().is_empty() {
        return next.to_owned();
    }
    current.to_owned()
}

#[must_use]
pub fn history_message_has_context_content(turn: &HistoryMessage) -> bool {
    let text = turn.text.trim();
    let original = turn.original_text.trim();
    if !text.is_empty() || !original.is_empty() {
        return true;
    }
    history_meta_has_context_content(&turn.meta, text, original)
}

#[must_use]
pub fn history_meta_has_context_content(
    meta: &ChatMessageMeta,
    text: &str,
    original: &str,
) -> bool {
    let mut vision_description = meta.vision_description.trim();
    let mut message_type = meta.message_type.trim();
    let annotation = meta.annotation.trim();
    if message_type == "text" {
        message_type = "";
    }
    if !vision_description.is_empty()
        && (vision_description == text || vision_description == original)
    {
        vision_description = "";
    }
    if !message_type.is_empty() || !vision_description.is_empty() || !annotation.is_empty() {
        return true;
    }
    meta.attachments
        .iter()
        .any(|attachment| history_attachment_has_context_content(attachment, text, original))
}

fn history_attachment_has_context_content(
    attachment: &ChatAttachment,
    text: &str,
    original: &str,
) -> bool {
    let caption = unique_history_text(&attachment.caption, &[text, original]);
    let content = unique_history_text(&attachment.content, &[&caption, text, original]);
    has_any_history_text(&[
        attachment.kind.as_str(),
        attachment.source.as_str(),
        content.as_str(),
        attachment.file_unique_id.as_str(),
        attachment.file_name.as_str(),
        attachment.mime_type.as_str(),
        caption.as_str(),
        attachment.performer.as_str(),
        attachment.title.as_str(),
        attachment.phone.as_str(),
        attachment.first_name.as_str(),
        attachment.last_name.as_str(),
    ]) || attachment.latitude.is_some()
        || attachment.longitude.is_some()
        || attachment.duration_seconds != 0
}

#[must_use]
pub fn unique_history_text(value: &str, duplicates: &[&str]) -> String {
    let value = value.trim();
    if duplicates.contains(&value) {
        return String::new();
    }
    value.to_owned()
}

fn has_any_history_text(values: &[&str]) -> bool {
    values.iter().any(|value| !value.trim().is_empty())
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(seconds: i64) -> Option<OffsetDateTime> {
        OffsetDateTime::from_unix_timestamp(seconds).ok()
    }

    #[test]
    fn select_history_messages_for_context_includes_reply_ancestor_beyond_limit() {
        let turns = vec![
            HistoryMessage {
                message_id: 5,
                thread_id: 10,
                text: "trigger".to_owned(),
                reply_to_id: 3,
                timestamp: ts(105),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 4,
                thread_id: 10,
                text: "recent".to_owned(),
                timestamp: ts(104),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 3,
                thread_id: 10,
                text: " parent ".to_owned(),
                original_text: " parent ".to_owned(),
                timestamp: ts(103),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 2,
                thread_id: 20,
                text: "other thread".to_owned(),
                timestamp: ts(102),
                ..HistoryMessage::default()
            },
        ];

        let got = select_history_messages_for_context(&turns, 1, 5, 10);

        assert_eq!(
            got.iter().map(|turn| turn.message_id).collect::<Vec<_>>(),
            vec![3, 4]
        );
        assert_eq!(got[0].original_text, "");
    }

    #[test]
    fn select_history_messages_for_context_includes_non_topic_reply_root() {
        let turns = vec![
            HistoryMessage {
                message_id: 5,
                thread_id: 3,
                text: "trigger".to_owned(),
                reply_to_id: 3,
                timestamp: ts(105),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 4,
                thread_id: 3,
                text: "recent".to_owned(),
                timestamp: ts(104),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 3,
                thread_id: 0,
                text: "video root".to_owned(),
                timestamp: ts(103),
                ..HistoryMessage::default()
            },
        ];

        let got = select_history_messages_for_context(&turns, 1, 5, 3);

        assert_eq!(
            got.iter().map(|turn| turn.message_id).collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn select_history_messages_for_context_excludes_messages_after_trigger() {
        let turns = vec![
            HistoryMessage {
                message_id: 12,
                text: "arrived while trigger was debounced".to_owned(),
                timestamp: ts(112),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 11,
                text: "trigger".to_owned(),
                timestamp: ts(111),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 10,
                text: "prior context".to_owned(),
                timestamp: ts(110),
                ..HistoryMessage::default()
            },
        ];

        let got = select_history_messages_for_context(&turns, 10, 11, 0);

        assert_eq!(
            got.iter().map(|turn| turn.message_id).collect::<Vec<_>>(),
            vec![10],
            "the model must never see post-trigger chat events before the current message"
        );
    }

    #[test]
    fn select_history_messages_for_context_uses_message_id_for_equal_timestamps() {
        let turns = vec![
            HistoryMessage {
                message_id: 12,
                text: "same-second future".to_owned(),
                timestamp: ts(111),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 11,
                text: "trigger".to_owned(),
                timestamp: ts(111),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 10,
                text: "same-second prior".to_owned(),
                timestamp: ts(111),
                ..HistoryMessage::default()
            },
        ];

        let got = select_history_messages_for_context(&turns, 10, 11, 0);

        assert_eq!(
            got.iter().map(|turn| turn.message_id).collect::<Vec<_>>(),
            vec![10]
        );
    }

    #[test]
    fn llm_selection_keeps_tool_turns_for_selected_visible_message_ids() {
        let turns = vec![
            HistoryMessage {
                message_id: 3,
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "new".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 2,
                role: ROLE_TOOL.to_owned(),
                tool_call: Some(ToolCall {
                    name: "web_search".to_owned(),
                    ..ToolCall::default()
                }),
                text: "tool response".to_owned(),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                message_id: 2,
                kind: MESSAGE_KIND_TEXT.to_owned(),
                text: "visible".to_owned(),
                ..HistoryMessage::default()
            },
        ];

        let got = select_llm_history_messages_for_context(&turns, 1, 3, 0);

        assert_eq!(got.len(), 2);
        assert_eq!(got[0].text, "visible");
        assert_eq!(got[1].text, "tool response");
    }

    #[test]
    fn history_message_has_context_content_uses_attachment_metadata() {
        let duplicate_only = ChatMessageMeta {
            attachments: vec![ChatAttachment {
                caption: "same".to_owned(),
                content: "same".to_owned(),
                ..ChatAttachment::default()
            }],
            ..ChatMessageMeta::default()
        };
        assert!(!history_meta_has_context_content(
            &duplicate_only,
            "same",
            "same"
        ));

        let mut with_metadata = duplicate_only.clone();
        with_metadata.attachments[0].file_name = "photo.jpg".to_owned();
        assert!(history_meta_has_context_content(
            &with_metadata,
            "same",
            "same"
        ));

        let mut with_distinct_content = duplicate_only;
        with_distinct_content.attachments[0].content = "distinct".to_owned();
        assert!(history_meta_has_context_content(
            &with_distinct_content,
            "same",
            "same"
        ));
    }

    #[test]
    fn history_message_omits_nil_tool_call_in_json() -> Result<(), serde_json::Error> {
        let payload = serde_json::to_string(&HistoryMessage {
            message_id: 42,
            text: "hello".to_owned(),
            ..HistoryMessage::default()
        })?;

        assert!(!payload.contains("\"ToolCall\":null"));
        assert!(payload.contains("\"MessageID\":42"));
        Ok(())
    }

    #[test]
    fn dialog_output_trace_artifacts_do_not_change_go_json_shape() -> Result<(), serde_json::Error>
    {
        let payload = serde_json::to_string(&DialogOutput {
            provider: "genkit".to_owned(),
            answer: "ok".to_owned(),
            trace: Some(DialogTraceArtifacts {
                request_kind: "gemini.generateContent".to_owned(),
                raw_request: Some(serde_json::json!({"secret": true})),
                usage: Some(DialogTraceUsage {
                    input_tokens: 10,
                    ..DialogTraceUsage::default()
                }),
                ..DialogTraceArtifacts::default()
            }),
            trace_events: vec![DialogTraceArtifacts {
                provider: "aifarm".to_owned(),
                raw_request: Some(serde_json::json!({"hidden": true})),
                ..DialogTraceArtifacts::default()
            }],
            ..DialogOutput::default()
        })?;

        assert!(payload.contains("\"Provider\":\"genkit\""));
        assert!(payload.contains("\"Answer\":\"ok\""));
        assert!(!payload.contains("gemini.generateContent"));
        assert!(!payload.contains("aifarm"));
        assert!(!payload.contains("secret"));
        assert!(!payload.contains("hidden"));
        assert!(!payload.contains("input_tokens"));
        Ok(())
    }

    #[test]
    fn normalize_history_message_kind_matches_go_tool_rules() {
        assert_eq!(
            normalize_history_message_kind(&HistoryMessage::default()),
            MESSAGE_KIND_TEXT
        );
        assert_eq!(
            normalize_history_message_kind(&HistoryMessage {
                role: ROLE_TOOL.to_owned(),
                tool_call: Some(ToolCall::default()),
                ..HistoryMessage::default()
            }),
            MESSAGE_KIND_TOOL_RESPONSE
        );
        assert_eq!(
            normalize_history_message_kind(&HistoryMessage {
                role: ROLE_MODEL.to_owned(),
                tool_call: Some(ToolCall::default()),
                ..HistoryMessage::default()
            }),
            MESSAGE_KIND_TOOL_REQUEST
        );
    }
}
