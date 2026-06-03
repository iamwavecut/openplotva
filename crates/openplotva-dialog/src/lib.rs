//! Provider-neutral dialog contracts and tool parsing.

use std::{error::Error, fmt, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

mod history;
mod json_codec;
mod persona;
pub mod tool_telemetry;

pub use history::{
    DEFAULT_CONTEXT_HISTORY_LIMIT, DailyPersona, DialogContext, DialogInput, DialogMessage,
    DialogOutput, DialogTraceArtifacts, DialogTraceUsage, DialogUser, HistoryMessage,
    MESSAGE_KIND_TEXT, MESSAGE_KIND_TOOL_REQUEST, MESSAGE_KIND_TOOL_RESPONSE, MessageKind,
    MultimodalImage, Persona, ROLE_MODEL, ROLE_TOOL, ROLE_USER, clone_history_messages,
    conversation_projection, history_message_has_context_content, history_meta_has_context_content,
    normalize_history_message, normalize_history_message_kind, select_history_messages_for_context,
    select_llm_history_messages_for_context, unique_history_text,
};
pub use json_codec::{
    PlotvaFinalResponse, coerce_bool_value, coerce_f64_value, coerce_i64_value,
    coerce_string_value, coerce_u64_value, decode_plotva_final_response_with_salvage,
    find_json_key_value, parse_json_object_tolerant, plotva_known_keys, salvage_json_object_text,
    salvage_json_object_value,
};
pub use persona::{daily_persona_for_day, daily_persona_for_unix_timestamp};

/// Human-readable crate purpose used by scaffold tests and docs.
pub const PURPOSE: &str = "dialog";

pub const PROVIDER_GENKIT: &str = "genkit";
pub const PROVIDER_AIFARM: &str = "aifarm";
pub const PROVIDER_NVIDIA: &str = "nvidia";
pub const PROVIDER_VMLX: &str = "vmlx";

pub const TOOL_RESULT_STATUS_OK: &str = "ok";
pub const TOOL_RESULT_STATUS_QUEUED: &str = "queued";
pub const TOOL_RESULT_STATUS_FAILED: &str = "failed";
pub const TOOL_RESULT_STATUS_NOOP: &str = "noop";
pub const TOOL_RESULT_STATUS_EXECUTED: &str = "executed";

pub const IMAGE_GENERATION_NOT_SCHEDULED_MESSAGE: &str = "Не удалось запустить генерацию изображения. Проверьте лимиты, доступ по подписке или настройки чата.";
pub const SONG_GENERATION_NOT_SCHEDULED_MESSAGE: &str = "Не удалось запустить генерацию песни. Проверьте лимиты, доступ по подписке или настройки чата.";

/// Error wrapper carrying hidden provider trace events without changing the visible error text.
#[derive(Debug)]
pub struct DialogTraceError {
    source: Box<dyn Error + Send + Sync + 'static>,
    trace_events: Vec<DialogTraceArtifacts>,
}

impl DialogTraceError {
    /// Wrap a source error with trace events.
    #[must_use]
    pub fn new(
        source: Box<dyn Error + Send + Sync + 'static>,
        trace_events: Vec<DialogTraceArtifacts>,
    ) -> Self {
        Self {
            source,
            trace_events,
        }
    }

    /// Hidden trace events collected before the error.
    #[must_use]
    pub fn trace_events(&self) -> &[DialogTraceArtifacts] {
        &self.trace_events
    }
}

impl fmt::Display for DialogTraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl Error for DialogTraceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref() as &(dyn Error + 'static))
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ToolContext {
    /// Telegram chat ID.
    #[serde(default)]
    pub chat_id: i64,
    /// Forum topic ID, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<i32>,
    /// Source Telegram message ID.
    #[serde(default)]
    pub message_id: i32,
    /// Caller Telegram user ID.
    #[serde(default)]
    pub user_id: i64,
    /// Caller display name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_full_name: String,
    /// Source message text.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message_text: String,
    /// History/dialog message metadata.
    #[serde(default)]
    pub message_meta: openplotva_core::ChatMessageMeta,
}

#[derive(Clone, Debug, Default, Eq, Deserialize, PartialEq, Serialize)]
pub struct ToolSideEffect {
    /// Side-effect kind, such as `image_generation_job`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    /// Optional side-effect ticket ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ticket_id: String,
    /// Optional estimated completion time.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub eta: String,
    /// Side-effect state, such as `queued`, `failed`, or `noop`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state: String,
}

#[derive(Clone, Debug, Eq, Deserialize, PartialEq, Serialize)]
pub struct ToolError {
    /// Stable error code.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    /// Human-readable reason.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    /// Whether the caller may retry.
    #[serde(default, skip_serializing_if = "is_false")]
    pub retryable: bool,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ToolResult {
    /// Result status string.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    /// Result message.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
    /// Whether the dialog should not reply.
    #[serde(default, skip_serializing_if = "is_false")]
    pub no_reply: bool,
    /// Structured side effect produced by the tool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_effect: Option<ToolSideEffect>,
    /// Tool-specific data payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Structured tool error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ToolError>,
}

impl ToolResult {
    #[must_use]
    pub fn failed(code: impl Into<String>, reason: impl Into<String>) -> Self {
        let code = code.into();
        let reason = reason.into();
        Self {
            status: TOOL_RESULT_STATUS_FAILED.to_owned(),
            message: reason.clone(),
            no_reply: false,
            side_effect: None,
            data: None,
            error: Some(ToolError {
                code,
                reason,
                retryable: true,
            }),
        }
    }
}

#[must_use]
pub fn user_facing_not_scheduled_message(message: &str, fallback: &str) -> String {
    let message = message.trim();
    let fallback = fallback.trim();
    if is_internal_not_scheduled_instruction(message) {
        return fallback.to_owned();
    }
    if message.is_empty() {
        fallback.to_owned()
    } else {
        message.to_owned()
    }
}

#[must_use]
pub fn is_internal_not_scheduled_instruction(message: &str) -> bool {
    let message = message.trim();
    message
        == "Запрос не поставлен в очередь. Пользователь уже получил причину (лимиты, доступ или настройки)."
        || message
            == "Запрос не поставлен в очередь. Пользователь уже получил причину (лимиты, доступ или настройки). Не утверждай, что генерация запущена."
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DrawRequest {
    /// Tool context.
    pub context: ToolContext,
    /// Image prompt.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prompt: String,
    /// Negative prompt.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub negative_prompt: String,
    /// Aspect ratio.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub aspect_ratio: String,
    /// Seed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub seed: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct SongRequest {
    /// Tool context.
    pub context: ToolContext,
    /// Song topic.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub topic: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct VisionRequest {
    /// Tool context.
    pub context: ToolContext,
    /// Vision attachment handle.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub file_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct HistorySummaryRequest {
    /// Tool context.
    pub context: ToolContext,
    /// Summary window.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub window: String,
    /// Window hours.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub hours: i32,
    /// Message count.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub message_count: i32,
    /// Since timestamp.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub since: String,
    /// Scope.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
}

/// Dialog toolbox error.
#[derive(Debug)]
pub struct DialogToolboxError {
    message: String,
}

impl DialogToolboxError {
    fn unsupported(tool: &'static str) -> Self {
        Self {
            message: format!("dialog toolbox method not implemented: {tool}"),
        }
    }
}

impl fmt::Display for DialogToolboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for DialogToolboxError {}

/// Boxed dialog toolbox error.
pub type ToolboxError = Box<dyn Error + Send + Sync + 'static>;

/// Boxed async dialog toolbox future.
pub type ToolboxFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ToolResult, ToolboxError>> + Send + 'a>>;

pub trait DialogToolbox: Send + Sync {
    /// Currency rates tool.
    fn currency_rates<'a>(&'a self, _meta: ToolContext) -> ToolboxFuture<'a> {
        unsupported_tool_future("currency_rates")
    }

    /// Draw image tool.
    fn draw_image<'a>(&'a self, _req: DrawRequest) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_DRAW_IMAGE)
    }

    /// Generate song tool.
    fn generate_song<'a>(&'a self, _req: SongRequest) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_GENERATE_SONG)
    }

    /// Vision image tool.
    fn vision_image<'a>(&'a self, _req: VisionRequest) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_VISION_IMAGE)
    }

    /// Web search tool.
    fn web_search<'a>(&'a self, _query: String) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_WEB_SEARCH)
    }

    /// Crawl URL tool.
    fn crawl_url<'a>(&'a self, _url: String) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_CRAWL_URL)
    }

    /// YouTube summary tool.
    fn youtube_summary<'a>(&'a self, _video: String) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_YOUTUBE_SUMMARY)
    }

    /// Queue status tool.
    fn queue_status<'a>(&'a self, _user_id: i64) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_QUEUE_STATUS)
    }

    /// Cancel drawing tool.
    fn cancel_drawing<'a>(&'a self, _user_id: i64, _chat_id: i64) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_CANCEL_DRAWING)
    }

    /// Translate text tool.
    fn translate_text<'a>(&'a self, _text: String, _target_lang: String) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_TRANSLATE_TEXT)
    }

    /// Chat history summary tool.
    fn chat_history_summary<'a>(&'a self, _req: HistorySummaryRequest) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_CHAT_HISTORY_SUMMARY)
    }
}

fn unsupported_tool_future<'a>(tool: &'static str) -> ToolboxFuture<'a> {
    Box::pin(async move { Err(Box::new(DialogToolboxError::unsupported(tool)) as ToolboxError) })
}

pub const STEP_DRAW_IMAGE: &str = "draw_image";
pub const STEP_GENERATE_SONG: &str = "generate_song";
pub const STEP_VISION_IMAGE: &str = "vision_image";
pub const STEP_CURRENCY_RATES: &str = "currency_rates";
pub const STEP_WEB_SEARCH: &str = "web_search";
pub const STEP_CRAWL_URL: &str = "crawl_url";
pub const STEP_YOUTUBE_SUMMARY: &str = "youtube_summary";
pub const STEP_QUEUE_STATUS: &str = "queue_status";
pub const STEP_CANCEL_DRAWING: &str = "cancel_drawing";
pub const STEP_TRANSLATE_TEXT: &str = "translate_text";
pub const STEP_CHAT_HISTORY_SUMMARY: &str = "chat_history_summary";

const ALL_STEPS: &[&str] = &[
    STEP_DRAW_IMAGE,
    STEP_GENERATE_SONG,
    STEP_VISION_IMAGE,
    STEP_CURRENCY_RATES,
    STEP_WEB_SEARCH,
    STEP_CRAWL_URL,
    STEP_YOUTUBE_SUMMARY,
    STEP_QUEUE_STATUS,
    STEP_CANCEL_DRAWING,
    STEP_TRANSLATE_TEXT,
    STEP_CHAT_HISTORY_SUMMARY,
];

const INLINE_TOOL_ARG_KEYS: &[&str] = &[
    "prompt",
    "topic",
    "file_id",
    "query",
    "url",
    "video",
    "text",
    "target_lang",
    "window",
    "hours",
    "message_count",
    "since",
    "scope",
    "negative_prompt",
    "aspect_ratio",
    "seed",
];

const INLINE_ARG_END_MARKERS: &[&str] = &[
    "}<tool_call|>",
    "</|tool_call>",
    "</|tool_call|>",
    "</tool_call>",
    "<|tool_call>",
    "<|tool_call|>",
    "<|tool_call:",
    "<tool_call|>",
    "</tool>",
    "<tool ",
    "<tool:",
];

const TOOL_PROTOCOL_ARTIFACT_MARKERS: &[&str] = &[
    "<|tool_call",
    "<tool_call|>",
    "</|tool_call",
    "</tool_call",
    "<|tool_response",
    "<tool_response|>",
    "</|tool_response",
    "</tool_response",
    "<|channel>",
    "<channel|>",
    "</|channel>",
    "TOOL_REQUEST\n",
    "TOOL_RESULT\n",
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolArgSpec {
    /// Argument name.
    pub name: &'static str,
    /// Whether the argument is required.
    pub required: bool,
    /// Tool prompt description.
    pub description: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolSpec {
    /// Tool name.
    pub name: &'static str,
    /// Short summary.
    pub summary: &'static str,
    /// When the model should call this tool.
    pub when_to_use: &'static str,
    /// Expected result.
    pub result: &'static str,
    /// Tool arguments.
    pub args: &'static [ToolArgSpec],
}

const DRAW_IMAGE_ARGS: &[ToolArgSpec] = &[
    ToolArgSpec {
        name: "prompt",
        required: true,
        description: "Image prompt. Prefer concrete visual instructions.",
    },
    ToolArgSpec {
        name: "negative_prompt",
        required: false,
        description: "Optional exclusions or undesired details.",
    },
    ToolArgSpec {
        name: "aspect_ratio",
        required: false,
        description: "Optional target aspect ratio such as 1:1, 4:3, 16:9.",
    },
    ToolArgSpec {
        name: "seed",
        required: false,
        description: "Optional deterministic seed.",
    },
];

const GENERATE_SONG_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "topic",
    required: true,
    description: "Song topic or concise idea to turn into a song.",
}];

const VISION_IMAGE_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "file_id",
    required: false,
    description: "Image attachment handle from attachments, for example message_123_image_1. Prefer the handle over opaque Telegram file_unique_id values. Omit only when the latest message has exactly one image.",
}];

const WEB_SEARCH_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "query",
    required: true,
    description: "Search query string.",
}];

const CRAWL_URL_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "url",
    required: true,
    description: "Full URL to fetch.",
}];

const YOUTUBE_SUMMARY_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "video",
    required: true,
    description: "YouTube URL or video ID.",
}];

const TRANSLATE_TEXT_ARGS: &[ToolArgSpec] = &[
    ToolArgSpec {
        name: "text",
        required: true,
        description: "Source text to translate.",
    },
    ToolArgSpec {
        name: "target_lang",
        required: false,
        description: "Target language code such as ru, en, de, fr, es. Default is ru.",
    },
];

const CHAT_HISTORY_SUMMARY_ARGS: &[ToolArgSpec] = &[
    ToolArgSpec {
        name: "window",
        required: false,
        description: "Optional window: day, hours, messages, or since. Default is day.",
    },
    ToolArgSpec {
        name: "hours",
        required: false,
        description: "Optional number of hours for window=hours, 1-24.",
    },
    ToolArgSpec {
        name: "message_count",
        required: false,
        description: "Optional recent message count for window=messages, maximum 500.",
    },
    ToolArgSpec {
        name: "since",
        required: false,
        description: "Optional RFC3339 timestamp for window=since.",
    },
    ToolArgSpec {
        name: "scope",
        required: false,
        description: "Optional scope: thread or chat. Default is current thread when present, otherwise chat.",
    },
];

const ALTERNATIVE_DIALOG_TOOL_CATALOG: &[ToolSpec] = &[
    ToolSpec {
        name: STEP_DRAW_IMAGE,
        summary: "Create or edit an image from the user's request.",
        when_to_use: "Use when the latest user message asks to draw, generate, create, redraw, or edit an image.",
        result: "Schedules image generation; the final image is delivered asynchronously.",
        args: DRAW_IMAGE_ARGS,
    },
    ToolSpec {
        name: STEP_GENERATE_SONG,
        summary: "Generate a song from a topic or idea.",
        when_to_use: "Use when the latest user message asks for a song, track, lyrics-based song, or music generation.",
        result: "Schedules music generation; the result is delivered asynchronously.",
        args: GENERATE_SONG_ARGS,
    },
    ToolSpec {
        name: STEP_VISION_IMAGE,
        summary: "Describe an image from chat context.",
        when_to_use: "Use when the latest user message asks what is shown in an image or requires understanding an attached image.",
        result: "Returns a text description of the image.",
        args: VISION_IMAGE_ARGS,
    },
    ToolSpec {
        name: STEP_CURRENCY_RATES,
        summary: "Fetch current fiat and crypto exchange rates.",
        when_to_use: "Use when the latest user message asks about exchange rates or recent currency movement.",
        result: "Returns fresh rate data and may queue a rates side-effect message.",
        args: &[],
    },
    ToolSpec {
        name: STEP_WEB_SEARCH,
        summary: "Search the web for current or external information.",
        when_to_use: "Use when the latest user message asks for current facts, news, recent events, or anything outside reliable memory.",
        result: "Returns aggregated search results for grounding the answer.",
        args: WEB_SEARCH_ARGS,
    },
    ToolSpec {
        name: STEP_CRAWL_URL,
        summary: "Fetch and extract readable text from a URL.",
        when_to_use: "Use when the latest user message asks to inspect, summarize, or quote a specific webpage.",
        result: "Returns extracted page content.",
        args: CRAWL_URL_ARGS,
    },
    ToolSpec {
        name: STEP_YOUTUBE_SUMMARY,
        summary: "Fetch a YouTube transcript and summary.",
        when_to_use: "Use when the latest user message asks to summarize, explain, or review a YouTube video.",
        result: "Returns a transcript-based summary or transcript text.",
        args: YOUTUBE_SUMMARY_ARGS,
    },
    ToolSpec {
        name: STEP_QUEUE_STATUS,
        summary: "Check the image-generation queue state.",
        when_to_use: "Use when the latest user message asks about queue position, current load, or wait time for image generation.",
        result: "Returns queue depth, active jobs, and estimated wait time.",
        args: &[],
    },
    ToolSpec {
        name: STEP_CANCEL_DRAWING,
        summary: "Cancel the user's active or queued image-generation jobs.",
        when_to_use: "Use when the latest user message asks to stop, cancel, or discard pending image generation.",
        result: "Cancels active jobs for the requesting user in the current chat.",
        args: &[],
    },
    ToolSpec {
        name: STEP_TRANSLATE_TEXT,
        summary: "Translate text into a target language.",
        when_to_use: "Use when the latest user message asks to translate a word, phrase, sentence, or passage.",
        result: "Returns the translated text with language information.",
        args: TRANSLATE_TEXT_ARGS,
    },
    ToolSpec {
        name: STEP_CHAT_HISTORY_SUMMARY,
        summary: "Summarize recent conversation history for the current chat or Telegram thread.",
        when_to_use: "Use when the latest user message asks what people discussed, what happened recently, or asks for a recap of the last day, hours, or messages.",
        result: "Returns a saved summary with event bullets and an artistic recap of actors and events.",
        args: CHAT_HISTORY_SUMMARY_ARGS,
    },
];

#[must_use]
pub fn alternative_dialog_tools() -> Vec<ToolSpec> {
    ALTERNATIVE_DIALOG_TOOL_CATALOG.to_vec()
}

#[must_use]
pub fn alternative_dialog_tool_names() -> Vec<&'static str> {
    ALTERNATIVE_DIALOG_TOOL_CATALOG
        .iter()
        .map(|spec| spec.name)
        .collect()
}

/// Return whether a tool call should be omitted from dialog history.
#[must_use]
pub fn is_dialog_history_noise_tool_call_name(name: &str) -> bool {
    let name = name.trim();
    name.eq_ignore_ascii_case(STEP_TRANSLATE_TEXT)
        || name.eq_ignore_ascii_case(STEP_CHAT_HISTORY_SUMMARY)
}

#[must_use]
pub fn filter_dialog_tool_calls_for_history(
    calls: &[openplotva_core::ToolCall],
) -> Vec<openplotva_core::ToolCall> {
    openplotva_core::filter_non_terminator_tool_calls(calls)
        .into_iter()
        .filter(|call| !is_dialog_history_noise_tool_call_name(&call.name))
        .collect()
}

/// OpenAI-compatible chat-completion tool definition used by AIFarm.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ChatCompletionTool {
    /// Tool type, always `function`.
    #[serde(rename = "type")]
    pub tool_type: String,
    /// Function schema.
    pub function: ChatCompletionFunction,
}

/// OpenAI-compatible chat-completion function definition.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ChatCompletionFunction {
    /// Function name.
    pub name: String,
    /// Function description.
    pub description: String,
    /// JSON-schema parameter object.
    pub parameters: Value,
}

#[must_use]
pub fn chat_completion_tools_for_names(names: &[&str]) -> Vec<ChatCompletionTool> {
    ALTERNATIVE_DIALOG_TOOL_CATALOG
        .iter()
        .filter(|spec| names.contains(&spec.name))
        .map(|spec| ChatCompletionTool {
            tool_type: "function".to_owned(),
            function: ChatCompletionFunction {
                name: spec.name.to_owned(),
                description: join_tool_description(spec),
                parameters: tool_parameters_schema(spec),
            },
        })
        .collect()
}

fn join_tool_description(spec: &ToolSpec) -> String {
    let mut parts = Vec::with_capacity(3);
    if !spec.summary.trim().is_empty() {
        parts.push(spec.summary.trim().to_owned());
    }
    if !spec.when_to_use.trim().is_empty() {
        parts.push(format!("When to use: {}", spec.when_to_use.trim()));
    }
    if !spec.result.trim().is_empty() {
        parts.push(format!("Result: {}", spec.result.trim()));
    }
    parts.join(" ")
}

fn tool_parameters_schema(spec: &ToolSpec) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for arg in spec.args {
        properties.insert(arg.name.to_owned(), tool_argument_schema(arg));
        if arg.required {
            required.push(Value::String(arg.name.to_owned()));
        }
    }

    let mut schema = Map::new();
    schema.insert("type".to_owned(), Value::String("object".to_owned()));
    schema.insert("additionalProperties".to_owned(), Value::Bool(false));
    schema.insert("properties".to_owned(), Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".to_owned(), Value::Array(required));
    }
    Value::Object(schema)
}

fn tool_argument_schema(arg: &ToolArgSpec) -> Value {
    let mut schema = Map::new();
    match arg.name {
        "hours" => {
            schema.insert("type".to_owned(), Value::String("integer".to_owned()));
            schema.insert("minimum".to_owned(), Value::from(1));
            schema.insert("maximum".to_owned(), Value::from(24));
        }
        "message_count" => {
            schema.insert("type".to_owned(), Value::String("integer".to_owned()));
            schema.insert("minimum".to_owned(), Value::from(1));
            schema.insert("maximum".to_owned(), Value::from(500));
        }
        _ => {
            schema.insert("type".to_owned(), Value::String("string".to_owned()));
            if arg.required {
                schema.insert("minLength".to_owned(), Value::from(1));
            }
        }
    }
    match arg.name {
        "scope" => {
            schema.insert("enum".to_owned(), json!(["thread", "chat"]));
        }
        "window" => {
            schema.insert(
                "enum".to_owned(),
                json!(["day", "hours", "messages", "since"]),
            );
        }
        _ => {}
    }
    if !arg.description.trim().is_empty() {
        schema.insert(
            "description".to_owned(),
            Value::String(arg.description.trim().to_owned()),
        );
    }
    Value::Object(schema)
}

#[must_use]
pub fn sanitize_tool_text(value: &str) -> String {
    let mut cleaned = value.trim();
    if cleaned.is_empty() {
        return String::new();
    }
    let mut cut_at = None;
    for marker in TOOL_PROTOCOL_ARTIFACT_MARKERS {
        if let Some(idx) = cleaned.find(marker)
            && cut_at.is_none_or(|prev| idx < prev)
        {
            cut_at = Some(idx);
        }
    }
    if let Some(idx) = cut_at {
        cleaned = cleaned[..idx].trim();
        return trim_protocol_boundary_suffix(cleaned);
    }
    cleaned.to_owned()
}

#[must_use]
pub fn sanitize_final_text(value: &str) -> String {
    let cleaned = sanitize_tool_text(value);
    if has_leading_plain_context_message(&cleaned) {
        return String::new();
    }
    let stripped = strip_leading_context_messages(&cleaned);
    if !stripped.trim().is_empty() {
        return stripped;
    }
    recover_from_assistant_envelope(&cleaned).unwrap_or(stripped)
}

fn recover_from_assistant_envelope(value: &str) -> Option<String> {
    let cleaned = value.trim();
    if !cleaned.starts_with("<assistant_message") {
        return None;
    }
    let close_tag = "</assistant_message>";
    let end = cleaned.find(close_tag)?;
    let envelope_end = end + close_tag.len();
    if !cleaned[envelope_end..].trim().is_empty() {
        return None;
    }
    let inner = &cleaned[..end];
    let open_tag = "<text>";
    let last_text_open = inner.rfind(open_tag)?;
    let body = &inner[last_text_open + open_tag.len()..];
    let close = body.find("</text>")?;
    let answer = body[..close].trim();
    (!answer.is_empty()).then(|| answer.to_owned())
}

#[must_use]
pub fn has_leading_context_message(value: &str) -> bool {
    let cleaned = sanitize_tool_text(value);
    let cleaned = cleaned.trim();
    has_prefix_fold(cleaned, "<message")
        || has_prefix_fold(cleaned, "<assistant_message")
        || has_prefix_fold(cleaned, "<last_message")
        || has_leading_plain_context_message(cleaned)
}

fn has_leading_plain_context_message(value: &str) -> bool {
    let cleaned = value.trim();
    has_prefix_fold(cleaned, "previous bot message")
        || has_prefix_fold(cleaned, "предыдущая реплика")
}

fn strip_leading_context_messages(value: &str) -> String {
    let mut cleaned = value.trim();
    loop {
        let end_tag = if cleaned.starts_with("<message") {
            "</message>"
        } else if cleaned.starts_with("<assistant_message") {
            "</assistant_message>"
        } else if cleaned.starts_with("<last_message") {
            "</last_message>"
        } else {
            return cleaned.to_owned();
        };
        let Some(end) = cleaned.find(end_tag) else {
            return String::new();
        };
        cleaned = cleaned[end + end_tag.len()..].trim();
    }
}

fn trim_protocol_boundary_suffix(value: &str) -> String {
    let mut cleaned = value.trim().to_owned();
    loop {
        let mut next = cleaned.trim().to_owned();
        for suffix in [r#"\"\"\"}"#, r#""""}"#, "'''}", "`}"] {
            next = next.trim_end_matches(suffix).trim().to_owned();
        }
        for suffix in [r#"\"\"\""#, r#"""""#, "'''", "```"] {
            next = next.trim_end_matches(suffix).trim().to_owned();
        }
        if next == cleaned {
            return cleaned;
        }
        cleaned = next;
    }
}

/// Recursively sanitize strings inside a decoded tool value.
#[must_use]
pub fn sanitize_tool_value(value: Value) -> Value {
    match value {
        Value::String(value) => Value::String(sanitize_tool_text(&value)),
        Value::Array(values) => Value::Array(values.into_iter().map(sanitize_tool_value).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, sanitize_tool_value(value)))
                .collect(),
        ),
        other => other,
    }
}

/// A parsed dialog tool step.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolStep {
    /// Tool name.
    #[serde(default, rename = "step")]
    pub step: String,
    /// Image prompt.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub prompt: String,
    /// Song topic.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub topic: String,
    /// Vision attachment handle.
    #[serde(default, rename = "file_id", skip_serializing_if = "String::is_empty")]
    pub file_id: String,
    /// Web search query.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub query: String,
    /// Crawl URL.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub url: String,
    /// YouTube URL or ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub video: String,
    /// Text to translate.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    /// Target language.
    #[serde(
        default,
        rename = "target_lang",
        skip_serializing_if = "String::is_empty"
    )]
    pub target_lang: String,
    /// History-summary window.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub window: String,
    /// History-summary hours.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub hours: i32,
    /// History-summary message count.
    #[serde(default, rename = "message_count", skip_serializing_if = "is_zero_i32")]
    pub message_count: i32,
    /// History-summary since timestamp.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub since: String,
    /// History-summary scope.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub scope: String,
    /// Image negative prompt.
    #[serde(
        default,
        rename = "negative_prompt",
        skip_serializing_if = "String::is_empty"
    )]
    pub negative_prompt: String,
    /// Image aspect ratio.
    #[serde(
        default,
        rename = "aspect_ratio",
        skip_serializing_if = "String::is_empty"
    )]
    pub aspect_ratio: String,
    /// Image seed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub seed: String,
}

/// OpenAI-compatible native tool call.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeToolCall {
    /// Provider tool-call ID/ref.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Tool call type.
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    pub call_type: String,
    /// Function call payload.
    pub function: NativeToolFunction,
}

/// OpenAI-compatible native function call.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct NativeToolFunction {
    /// Function name.
    #[serde(default)]
    pub name: String,
    /// Function arguments, either object or JSON string.
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ToolParseDecision {
    /// Parser form.
    pub form: String,
    /// Parsed or ignored tool.
    pub tool: String,
    /// Outcome: detected, ignored, none, or error.
    pub outcome: String,
    /// Reason for non-detected outcome.
    pub reason: String,
}

/// Tool parse error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolParseError {
    message: String,
}

impl ToolParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ToolParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for ToolParseError {}

pub fn parse_native_tool_step(calls: &[NativeToolCall]) -> Result<ToolStep, ToolParseError> {
    if calls.len() != 1 {
        return Err(ToolParseError::new(format!(
            "expected exactly one tool call, got {}",
            calls.len()
        )));
    }
    let call = &calls[0];
    if !call.call_type.is_empty() && call.call_type != "function" {
        return Err(ToolParseError::new(format!(
            "unsupported tool call type {:?}",
            call.call_type
        )));
    }
    let name = call.function.name.trim();
    if !is_known_step(name) {
        return Err(ToolParseError::new(format!("unknown step {name:?}")));
    }
    decode_tool_call_arguments(name, &call.function.arguments)
        .map_err(|error| ToolParseError::new(format!("decode {name} tool arguments: {error}")))
}

/// Parse model content for XML-ish, inline, normalized, JSON, or bare tool calls.
pub fn extract_content_tool_step(
    raw_content: &str,
) -> Result<(Option<ToolStep>, ToolParseDecision), ToolParseError> {
    let raw_content = raw_content.trim();
    for parser in [
        parse_xmlish_tool_step_attempt,
        parse_inline_tool_step_attempt,
        parse_normalized_tool_step_attempt,
        parse_bare_tool_step_attempt,
    ] {
        let (step, ok, form) = parser(raw_content)?;
        if ok {
            let decision = ToolParseDecision {
                form,
                tool: step.step.clone(),
                outcome: "detected".to_owned(),
                reason: String::new(),
            };
            return Ok((Some(step), decision));
        }
    }
    if let Some(decision) = detect_ignored_tool_call(raw_content) {
        return Ok((None, decision));
    }
    Ok((
        None,
        ToolParseDecision {
            outcome: "none".to_owned(),
            reason: "no_tool_call".to_owned(),
            ..ToolParseDecision::default()
        },
    ))
}

fn parse_xmlish_tool_step_attempt(raw: &str) -> Result<(ToolStep, bool, String), ToolParseError> {
    parse_xmlish_tool_call_step(raw).map(|(step, ok)| (step, ok, "xmlish".to_owned()))
}

fn parse_inline_tool_step_attempt(raw: &str) -> Result<(ToolStep, bool, String), ToolParseError> {
    parse_inline_tool_call_step(raw).map(|(step, ok)| (step, ok, "inline".to_owned()))
}

fn parse_normalized_tool_step_attempt(
    raw: &str,
) -> Result<(ToolStep, bool, String), ToolParseError> {
    parse_normalized_tool_call_step(raw)
}

fn parse_bare_tool_step_attempt(raw: &str) -> Result<(ToolStep, bool, String), ToolParseError> {
    parse_bare_tool_call_step(raw).map(|(step, ok)| (step, ok, classify_bare_tool_call_form(raw)))
}

fn decode_tool_call_arguments(name: &str, raw: &Value) -> Result<ToolStep, ToolParseError> {
    let mut step = ToolStep {
        step: name.to_owned(),
        ..ToolStep::default()
    };
    match raw {
        Value::Null => {}
        Value::String(encoded) => populate_jsonish_or_inline_args(encoded, &mut step)?,
        Value::Object(map) => populate_json_map_args(map, &mut step),
        other => populate_jsonish_or_inline_args(&other.to_string(), &mut step)?,
    }
    normalize_and_validate_step(step)
}

fn populate_jsonish_or_inline_args(raw: &str, step: &mut ToolStep) -> Result<(), ToolParseError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(());
    }
    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        match value {
            Value::Object(map) => populate_json_map_args(&map, step),
            Value::String(inner) => populate_jsonish_or_inline_args(&inner, step)?,
            _ => {}
        }
        return Ok(());
    }
    let body = trimmed
        .strip_prefix('{')
        .unwrap_or(trimmed)
        .strip_suffix('}')
        .unwrap_or(trimmed);
    populate_inline_tool_args(body, step);
    Ok(())
}

fn populate_json_map_args(map: &Map<String, Value>, step: &mut ToolStep) {
    for (key, value) in map {
        match (key.as_str(), value) {
            ("step", Value::String(value)) => step.step = value.trim().to_owned(),
            ("prompt", Value::String(value)) => step.prompt = value.clone(),
            ("topic", Value::String(value)) => step.topic = value.clone(),
            ("file_id", Value::String(value)) => step.file_id = value.clone(),
            ("query", Value::String(value)) => step.query = value.clone(),
            ("url", Value::String(value)) => step.url = value.clone(),
            ("video", Value::String(value)) => step.video = value.clone(),
            ("text", Value::String(value)) => step.text = value.clone(),
            ("target_lang", Value::String(value)) => step.target_lang = value.clone(),
            ("window", Value::String(value)) => step.window = value.clone(),
            ("hours", Value::Number(value)) => {
                step.hours = value
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())
                    .unwrap_or(0);
            }
            ("message_count", Value::Number(value)) => {
                step.message_count = value
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())
                    .unwrap_or(0);
            }
            ("since", Value::String(value)) => step.since = value.clone(),
            ("scope", Value::String(value)) => step.scope = value.clone(),
            ("negative_prompt", Value::String(value)) => step.negative_prompt = value.clone(),
            ("aspect_ratio", Value::String(value)) => step.aspect_ratio = value.clone(),
            ("seed", Value::String(value)) => step.seed = value.clone(),
            _ => {}
        }
    }
}

fn parse_inline_tool_call_step(raw: &str) -> Result<(ToolStep, bool), ToolParseError> {
    let Some((start, marker_len)) = find_inline_tool_call_start(raw) else {
        return Ok((ToolStep::default(), false));
    };
    let mut rest = raw[start + marker_len..].trim();
    rest = rest.strip_prefix("call:").unwrap_or(rest);
    let Some(name_end) = rest.find('{') else {
        return Err(ToolParseError::new("inline tool call is missing arguments"));
    };
    let name = rest[..name_end].trim();
    if !is_known_step(name) {
        return Ok((ToolStep::default(), false));
    }
    let mut args = &rest[name_end + 1..];
    if let Some(end) = args.find("}<tool_call|>") {
        args = &args[..end];
    }
    let mut step = ToolStep {
        step: name.to_owned(),
        ..ToolStep::default()
    };
    populate_inline_tool_args(args, &mut step);
    normalize_and_validate_step(step).map(|step| (step, true))
}

fn parse_bare_tool_call_step(raw: &str) -> Result<(ToolStep, bool), ToolParseError> {
    let raw = raw.trim();
    let start = bare_tool_call_start(raw);
    if start < 0 {
        return Ok((ToolStep::default(), false));
    }
    let raw = raw[start as usize..].trim();
    let Some(open) = raw.find('{') else {
        return Ok((ToolStep::default(), false));
    };
    if open == 0 {
        return Ok((ToolStep::default(), false));
    }
    let name = raw[..open].trim();
    let name = name.strip_prefix("call:").unwrap_or(name).trim();
    if !is_known_step(name) {
        return Ok((ToolStep::default(), false));
    }
    let mut step = ToolStep {
        step: name.to_owned(),
        ..ToolStep::default()
    };
    populate_inline_tool_args(&raw[open + 1..], &mut step);
    normalize_and_validate_step(step).map(|step| (step, true))
}

fn classify_bare_tool_call_form(raw: &str) -> String {
    if bare_tool_call_start(raw.trim()) <= 0 {
        "bare_start".to_owned()
    } else {
        "bare_block".to_owned()
    }
}

fn bare_tool_call_start(raw: &str) -> isize {
    let mut best = -1;
    for step in ALL_STEPS {
        let idx = first_bare_tool_name_start(raw, step);
        if idx >= 0 && (best < 0 || idx < best) {
            best = idx;
        }
    }
    best
}

fn first_bare_tool_name_start(raw: &str, name: &str) -> isize {
    let mut best = -1;
    for needle in [format!("{name}{{"), format!("call:{name}{{")] {
        let mut offset = 0;
        while let Some(idx) = raw[offset..].find(&needle) {
            let start = offset + idx;
            if is_bare_tool_call_boundary(raw, start) && (best < 0 || (start as isize) < best) {
                best = start as isize;
            }
            offset = start + needle.len();
            if offset >= raw.len() {
                break;
            }
        }
    }
    best
}

fn is_bare_tool_call_boundary(raw: &str, start: usize) -> bool {
    if start == 0 {
        return true;
    }
    let prev = raw.as_bytes()[start - 1];
    if prev == b'\n' || prev == b'\r' {
        return true;
    }
    if prev == b' ' || prev == b'\t' {
        let line_start = raw[..start].rfind(['\n', '\r']).map_or(0, |idx| idx + 1);
        return raw[line_start..start].trim().is_empty();
    }
    false
}

fn parse_normalized_tool_call_step(raw: &str) -> Result<(ToolStep, bool, String), ToolParseError> {
    for candidate in normalized_tool_call_candidates(raw) {
        if candidate.payload.is_empty() {
            continue;
        }
        if let (step, true) = parse_json_tool_call_step(&candidate.payload)? {
            return Ok((step, true, candidate.form));
        }
        if let (step, true) = parse_bare_tool_call_step(&candidate.payload)? {
            let form = if candidate.form == "json" {
                classify_bare_tool_call_form(&candidate.payload)
            } else {
                candidate.form
            };
            return Ok((step, true, form));
        }
    }
    Ok((ToolStep::default(), false, String::new()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NormalizedToolCallCandidate {
    form: String,
    payload: String,
}

fn normalized_tool_call_candidates(raw: &str) -> Vec<NormalizedToolCallCandidate> {
    let raw = raw.trim();
    let mut candidates = Vec::with_capacity(8);
    for block in fenced_code_blocks(raw) {
        candidates.push(NormalizedToolCallCandidate {
            form: "fenced".to_owned(),
            payload: block,
        });
    }
    candidates.push(NormalizedToolCallCandidate {
        form: "json".to_owned(),
        payload: raw.to_owned(),
    });
    for line in tool_call_candidate_lines(raw) {
        if let Some(payload) = normalize_tool_prefix_call(&line) {
            candidates.push(NormalizedToolCallCandidate {
                form: "tool_prefix".to_owned(),
                payload,
            });
        }
        if let Some(payload) = normalize_function_style_call(&line) {
            candidates.push(NormalizedToolCallCandidate {
                form: "function_args".to_owned(),
                payload,
            });
        }
        if let Some(payload) = normalize_colon_style_call(&line) {
            candidates.push(NormalizedToolCallCandidate {
                form: "colon_args".to_owned(),
                payload,
            });
        }
    }
    candidates
}

fn fenced_code_blocks(raw: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut offset = 0;
    while let Some(start_rel) = raw[offset..].find("```") {
        let start = offset + start_rel + 3;
        let Some(line_end_rel) = raw[start..].find(['\n', '\r']) else {
            return blocks;
        };
        let body_start = start + line_end_rel + 1;
        let Some(end_rel) = raw[body_start..].find("```") else {
            return blocks;
        };
        let body_end = body_start + end_rel;
        let block = raw[body_start..body_end].trim();
        if !block.is_empty() {
            blocks.push(block.to_owned());
        }
        offset = body_end + 3;
        if offset >= raw.len() {
            break;
        }
    }
    blocks
}

fn tool_call_candidate_lines(raw: &str) -> Vec<String> {
    raw.lines()
        .map(|line| line.trim().trim_end_matches('\r').trim())
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_tool_prefix_call(line: &str) -> Option<String> {
    line.trim()
        .strip_prefix("tool:")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_function_style_call(line: &str) -> Option<String> {
    for step in ALL_STEPS {
        for prefix in [format!("{step}({{"), format!("call:{step}({{")] {
            if line.starts_with(&prefix) {
                return Some(line.replacen("({", "{", 1));
            }
        }
    }
    None
}

fn normalize_colon_style_call(line: &str) -> Option<String> {
    for step in ALL_STEPS {
        for prefix in [step.to_string(), format!("call:{step}")] {
            if let Some(payload) = normalize_colon_style_call_for_prefix(line, &prefix) {
                return Some(payload);
            }
        }
    }
    None
}

fn normalize_colon_style_call_for_prefix(line: &str, prefix: &str) -> Option<String> {
    let rest = line.strip_prefix(prefix)?.trim();
    let rest = rest.strip_prefix(':')?.trim();
    if !rest.starts_with('{') {
        return None;
    }
    Some(format!("{prefix}{rest}"))
}

fn parse_json_tool_call_step(raw: &str) -> Result<(ToolStep, bool), ToolParseError> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('{') {
        return Ok((ToolStep::default(), false));
    }
    let Ok(Value::Object(payload)) = serde_json::from_str::<Value>(trimmed) else {
        return Ok((ToolStep::default(), false));
    };
    let name = json_tool_name(&payload);
    if name.is_empty() || !is_known_step(&name) {
        return Ok((ToolStep::default(), false));
    }
    let mut step = ToolStep {
        step: name.clone(),
        ..ToolStep::default()
    };
    if let Some(raw_args) = first_json_raw(&payload, &["arguments", "args", "input"]) {
        step = decode_tool_call_arguments(&name, raw_args).map_err(|error| {
            ToolParseError::new(format!("decode {name} tool arguments: {error}"))
        })?;
    } else {
        populate_json_map_args(&payload, &mut step);
        step.step = name;
        step = normalize_and_validate_step(step)?;
    }
    Ok((step, true))
}

fn json_tool_name(payload: &Map<String, Value>) -> String {
    for key in ["tool", "tool_name", "name", "function", "step"] {
        if let Some(Value::String(value)) = payload.get(key) {
            return value.trim().to_owned();
        }
    }
    String::new()
}

fn first_json_raw<'a>(payload: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| payload.get(*key))
}

fn detect_ignored_tool_call(raw: &str) -> Option<ToolParseDecision> {
    detect_ignored_inline_tool_call(raw)
        .or_else(|| detect_ignored_json_tool_call(raw))
        .or_else(|| detect_ignored_bare_tool_call(raw))
}

fn detect_ignored_inline_tool_call(raw: &str) -> Option<ToolParseDecision> {
    let (start, marker_len) = find_inline_tool_call_start(raw)?;
    let mut rest = raw[start + marker_len..].trim();
    rest = rest.strip_prefix("call:").unwrap_or(rest);
    let Some(name_end) = rest.find('{') else {
        return Some(ToolParseDecision {
            form: "inline".to_owned(),
            outcome: "ignored".to_owned(),
            reason: "missing_arguments".to_owned(),
            ..ToolParseDecision::default()
        });
    };
    let name = rest[..name_end].trim();
    if name.is_empty() || is_known_step(name) {
        return None;
    }
    Some(ToolParseDecision {
        form: "inline".to_owned(),
        tool: name.to_owned(),
        outcome: "ignored".to_owned(),
        reason: "unknown_tool".to_owned(),
    })
}

fn detect_ignored_json_tool_call(raw: &str) -> Option<ToolParseDecision> {
    for candidate in normalized_tool_call_candidates(raw) {
        let trimmed = candidate.payload.trim();
        if !trimmed.starts_with('{') {
            continue;
        }
        let Ok(Value::Object(payload)) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        let name = json_tool_name(&payload);
        if !name.is_empty() && !is_known_step(&name) {
            return Some(ToolParseDecision {
                form: candidate.form,
                tool: name,
                outcome: "ignored".to_owned(),
                reason: "unknown_tool".to_owned(),
            });
        }
    }
    None
}

fn detect_ignored_bare_tool_call(raw: &str) -> Option<ToolParseDecision> {
    for line in tool_call_candidate_lines(raw) {
        if let Some(name) = bare_tool_name_from_line(&line)
            && !name.is_empty()
            && !is_known_step(&name)
        {
            return Some(ToolParseDecision {
                form: "bare".to_owned(),
                tool: name,
                outcome: "ignored".to_owned(),
                reason: "unknown_tool".to_owned(),
            });
        }
    }
    None
}

fn bare_tool_name_from_line(line: &str) -> Option<String> {
    let line = line.trim().strip_prefix("call:").unwrap_or(line.trim());
    let open = line.find('{')?;
    if open == 0 {
        return None;
    }
    let name = line[..open].trim();
    is_tool_like_name(name).then(|| name.to_owned())
}

fn is_tool_like_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

fn parse_xmlish_tool_call_step(raw: &str) -> Result<(ToolStep, bool), ToolParseError> {
    let Some(tag) = first_xmlish_tool_tag(raw) else {
        return Ok((ToolStep::default(), false));
    };
    if let Some(call) = xmlish_tool_attr(&tag, "call") {
        return parse_bare_tool_call_step(&call);
    }
    let trimmed = tag.trim();
    if trimmed.starts_with("<tool:") {
        let payload = trimmed
            .trim_start_matches("<tool:")
            .trim_end_matches('>')
            .trim_end_matches('/')
            .trim();
        return parse_bare_tool_call_step(payload);
    }
    if let Some(payload) = first_xmlish_tool_body(raw) {
        let (nested_step, nested_ok) = parse_xmlish_tool_call_step(&payload)?;
        if nested_ok {
            return Ok((nested_step, true));
        }
        return parse_bare_tool_call_step(&payload);
    }
    let Some(name) = xmlish_tool_attr(&tag, "name") else {
        return Ok((ToolStep::default(), false));
    };
    let name = name.trim();
    if !is_known_step(name) {
        return Ok((ToolStep::default(), false));
    }
    let mut step = ToolStep {
        step: name.to_owned(),
        ..ToolStep::default()
    };
    populate_xmlish_tool_attrs(&tag, &mut step);
    normalize_and_validate_step(step).map(|step| (step, true))
}

fn first_xmlish_tool_tag(raw: &str) -> Option<String> {
    let mut offset = 0;
    while let Some(idx) = raw[offset..].find("<tool") {
        let start = offset + idx;
        let after = start + "<tool".len();
        if after < raw.len() && !is_xmlish_tool_marker(raw.as_bytes()[after]) {
            offset = after;
            continue;
        }
        let end = raw[start..].find('>')?;
        return Some(raw[start..start + end + 1].to_owned());
    }
    None
}

fn is_xmlish_tool_marker(byte: u8) -> bool {
    matches!(byte, b':' | b'_' | b' ' | b'\t' | b'\n' | b'\r' | b'>')
}

fn first_xmlish_tool_body(raw: &str) -> Option<String> {
    let mut offset = 0;
    while let Some(idx) = raw[offset..].find("<tool") {
        let start = offset + idx;
        let after = start + "<tool".len();
        if after < raw.len() && !is_xmlish_tool_marker(raw.as_bytes()[after]) {
            offset = after;
            continue;
        }
        let open_end = raw[start..].find('>')? + start;
        let tag = &raw[start..open_end + 1];
        if tag.trim().ends_with("/>") {
            offset = open_end + 1;
            continue;
        }
        let name = xmlish_tool_tag_name(tag);
        if name.is_empty() {
            offset = open_end + 1;
            continue;
        }
        let close_tag = format!("</{name}>");
        let body_start = open_end + 1;
        let Some(body_end) = index_fold(&raw[body_start..], &close_tag) else {
            offset = body_start;
            continue;
        };
        return Some(unescape_xmlish(
            raw[body_start..body_start + body_end].trim(),
        ));
    }
    None
}

fn xmlish_tool_tag_name(tag: &str) -> String {
    let tag = tag.trim().trim_start_matches('<');
    let Some(end) = tag.find([' ', '\t', '\n', '\r', '>', '/']) else {
        return String::new();
    };
    tag[..end].trim().to_owned()
}

fn index_fold(value: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    value.char_indices().find_map(|(start, _)| {
        let end = start.checked_add(needle.len())?;
        value
            .get(start..end)
            .filter(|candidate| candidate.eq_ignore_ascii_case(needle))
            .map(|_| start)
    })
}

fn xmlish_tool_attr(tag: &str, name: &str) -> Option<String> {
    let mut offset = 0;
    while let Some(idx) = tag[offset..].find(name) {
        let start = offset + idx;
        let name_end = start + name.len();
        if (start == 0 || is_xmlish_attr_boundary(tag.as_bytes()[start - 1]))
            && name_end < tag.len()
        {
            let rest = tag[name_end..].trim_start_matches([' ', '\t', '\r', '\n']);
            if let Some(value) = rest.strip_prefix('=') {
                return parse_xmlish_attr_value(value.trim_start_matches([' ', '\t', '\r', '\n']));
            }
        }
        offset = name_end;
    }
    None
}

fn is_xmlish_attr_boundary(byte: u8) -> bool {
    matches!(byte, b'<' | b' ' | b'\t' | b'\n' | b'\r')
}

fn parse_xmlish_attr_value(value: &str) -> Option<String> {
    let first = value.as_bytes().first().copied()?;
    if first == b'"' || first == b'\'' {
        return parse_quoted_xmlish_attr_value(value, first);
    }
    let end = value
        .find([' ', '\t', '\n', '\r', '>', '/'])
        .unwrap_or(value.len());
    Some(unescape_xmlish(value[..end].trim()))
}

fn parse_quoted_xmlish_attr_value(value: &str, quote: u8) -> Option<String> {
    for idx in 1..value.len() {
        if value.as_bytes()[idx] == quote {
            return Some(unescape_xmlish(value[1..idx].trim()));
        }
    }
    None
}

fn unescape_xmlish(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#34;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
}

fn find_inline_tool_call_start(raw: &str) -> Option<(usize, usize)> {
    ["<|tool_call>", "<|tool_call|>", "<|tool_call:"]
        .iter()
        .filter_map(|marker| raw.find(marker).map(|idx| (idx, marker.len())))
        .min_by_key(|(idx, _)| *idx)
}

fn populate_xmlish_tool_attrs(tag: &str, step: &mut ToolStep) {
    populate_tool_args(|key| xmlish_tool_attr(tag, key), step);
}

fn populate_inline_tool_args(args: &str, step: &mut ToolStep) {
    populate_tool_args(|key| extract_inline_tool_arg(args, key), step);
}

fn populate_tool_args(mut lookup: impl FnMut(&str) -> Option<String>, step: &mut ToolStep) {
    if let Some(value) = lookup("prompt") {
        step.prompt = value;
    }
    if let Some(value) = lookup("topic") {
        step.topic = value;
    }
    if let Some(value) = lookup("file_id") {
        step.file_id = value;
    }
    if let Some(value) = lookup("query") {
        step.query = value;
    }
    if let Some(value) = lookup("url") {
        step.url = value;
    }
    if let Some(value) = lookup("video") {
        step.video = value;
    }
    if let Some(value) = lookup("text") {
        step.text = value;
    }
    if let Some(value) = lookup("target_lang") {
        step.target_lang = value;
    }
    if let Some(value) = lookup("window") {
        step.window = value;
    }
    if let Some(value) = lookup("hours").and_then(|value| value.trim().parse::<i32>().ok()) {
        step.hours = value;
    }
    if let Some(value) = lookup("message_count").and_then(|value| value.trim().parse::<i32>().ok())
    {
        step.message_count = value;
    }
    if let Some(value) = lookup("since") {
        step.since = value;
    }
    if let Some(value) = lookup("scope") {
        step.scope = value;
    }
    if let Some(value) = lookup("negative_prompt") {
        step.negative_prompt = value;
    }
    if let Some(value) = lookup("aspect_ratio") {
        step.aspect_ratio = value;
    }
    if let Some(value) = lookup("seed") {
        step.seed = value;
    }
}

fn extract_inline_tool_arg(args: &str, key: &str) -> Option<String> {
    let value_start = inline_tool_arg_value_start(args, key)?;
    let mut value = args[value_start..].trim();
    const QUOTE_MARKER: &str = r#"<|"|>"#;
    if let Some(rest) = value.strip_prefix(QUOTE_MARKER) {
        value = rest;
        return Some(value[..inline_arg_end(value, true)].trim().to_owned());
    }
    if let Some(parsed) = parse_regular_quoted_inline_arg(value) {
        return Some(parsed);
    }
    Some(value[..inline_arg_end(value, false)].trim().to_owned())
}

fn is_inline_arg_boundary(byte: u8) -> bool {
    matches!(
        byte,
        b',' | b'{' | b' ' | b'\n' | b'\r' | b'\t' | b'"' | b'\''
    )
}

fn inline_tool_arg_value_start(args: &str, key: &str) -> Option<usize> {
    for needle in [
        format!("{key}:"),
        format!(r#""{key}":"#),
        format!("'{key}':"),
    ] {
        let mut offset = 0;
        while let Some(idx) = args[offset..].find(&needle) {
            let pos = offset + idx;
            if pos == 0 || is_inline_arg_boundary(args.as_bytes()[pos - 1]) {
                return Some(pos + needle.len());
            }
            offset = pos + needle.len();
            if offset >= args.len() {
                break;
            }
        }
    }
    None
}

fn parse_regular_quoted_inline_arg(value: &str) -> Option<String> {
    let quote = value.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let mut out = String::new();
    let mut escaped = false;
    for (idx, ch) in value.char_indices().skip(1) {
        if escaped {
            out.push(unescape_inline_arg_char(ch));
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == quote && quoted_arg_can_end(&value[idx + 1..]) {
            return Some(out.trim().to_owned());
        }
        out.push(ch);
    }
    let unterminated = value[1..].trim();
    Some(
        unterminated[..inline_arg_end(unterminated, false)]
            .trim()
            .to_owned(),
    )
}

fn quoted_arg_can_end(rest: &str) -> bool {
    let rest = rest.trim_start();
    rest.is_empty()
        || rest.starts_with('}')
        || INLINE_ARG_END_MARKERS
            .iter()
            .any(|marker| rest.starts_with(marker))
        || rest
            .strip_prefix(',')
            .is_some_and(|after| inline_arg_starts_with_known_key(after.trim_start()))
}

fn unescape_inline_arg_char(value: char) -> char {
    match value {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        other => other,
    }
}

fn inline_arg_end(value: &str, quoted: bool) -> usize {
    let mut end = value.len();
    if quoted && let Some(idx) = value.find(r#"<|"|>"#) {
        end = end.min(idx);
    }
    for marker in INLINE_ARG_END_MARKERS {
        if let Some(idx) = value.find(marker) {
            end = end.min(idx);
        }
    }
    if !quoted {
        if let Some(idx) = value.find('}') {
            end = end.min(idx);
        }
        if let Some(idx) = inline_arg_separator_index(value) {
            end = end.min(idx);
        }
    }
    end
}

fn inline_arg_separator_index(value: &str) -> Option<usize> {
    let mut offset = 0;
    while let Some(idx) = value[offset..].find(',') {
        let pos = offset + idx;
        let rest = value[pos + 1..].trim_start_matches([' ', '\t', '\r', '\n']);
        if inline_arg_starts_with_known_key(rest) {
            return Some(pos);
        }
        offset = pos + 1;
        if offset >= value.len() {
            break;
        }
    }
    None
}

fn inline_arg_starts_with_known_key(rest: &str) -> bool {
    INLINE_TOOL_ARG_KEYS.iter().any(|key| {
        rest.starts_with(&format!("{key}:"))
            || rest.starts_with(&format!(r#""{key}":"#))
            || rest.starts_with(&format!("'{key}':"))
    })
}

fn normalize_and_validate_step(mut step: ToolStep) -> Result<ToolStep, ToolParseError> {
    step.step = step.step.trim().to_owned();
    step.prompt = sanitize_tool_text(&step.prompt);
    step.topic = sanitize_tool_text(&step.topic);
    step.file_id = sanitize_tool_text(&step.file_id);
    step.query = sanitize_tool_text(&step.query);
    step.url = sanitize_tool_text(&step.url);
    step.video = sanitize_tool_text(&step.video);
    step.text = sanitize_tool_text(&step.text);
    step.target_lang = sanitize_tool_text(&step.target_lang);
    step.window = sanitize_tool_text(&step.window);
    step.since = sanitize_tool_text(&step.since);
    step.scope = sanitize_tool_text(&step.scope);
    step.negative_prompt = sanitize_tool_text(&step.negative_prompt);
    step.aspect_ratio = sanitize_tool_text(&step.aspect_ratio);
    step.seed = sanitize_tool_text(&step.seed);

    if !is_known_step(&step.step) {
        return Err(ToolParseError::new(format!("unknown step {:?}", step.step)));
    }
    validate_required_step_arguments(&step)?;
    if step_contains_protocol_sentinel_argument(&step) {
        return Err(ToolParseError::new(format!(
            "{} tool argument contains protocol sentinel",
            step.step
        )));
    }
    Ok(step)
}

fn validate_required_step_arguments(step: &ToolStep) -> Result<(), ToolParseError> {
    match step.step.as_str() {
        STEP_DRAW_IMAGE => require_step_argument(&step.step, "prompt", &step.prompt),
        STEP_GENERATE_SONG => require_step_argument(&step.step, "topic", &step.topic),
        STEP_WEB_SEARCH => require_step_argument(&step.step, "query", &step.query),
        STEP_CRAWL_URL => require_step_argument(&step.step, "url", &step.url),
        STEP_YOUTUBE_SUMMARY => require_step_argument(&step.step, "video", &step.video),
        STEP_TRANSLATE_TEXT => require_step_argument(&step.step, "text", &step.text),
        _ => Ok(()),
    }
}

fn require_step_argument(step: &str, name: &str, value: &str) -> Result<(), ToolParseError> {
    if value.trim().is_empty() {
        return Err(ToolParseError::new(format!("{step} {name} is empty")));
    }
    Ok(())
}

fn step_contains_protocol_sentinel_argument(step: &ToolStep) -> bool {
    [
        step.prompt.as_str(),
        step.topic.as_str(),
        step.file_id.as_str(),
        step.query.as_str(),
        step.url.as_str(),
        step.video.as_str(),
        step.text.as_str(),
        step.target_lang.as_str(),
        step.window.as_str(),
        step.since.as_str(),
        step.scope.as_str(),
        step.negative_prompt.as_str(),
        step.aspect_ratio.as_str(),
        step.seed.as_str(),
    ]
    .iter()
    .any(|value| is_protocol_sentinel_value(value))
}

fn is_protocol_sentinel_value(value: &str) -> bool {
    let cleaned = value.trim();
    if cleaned.is_empty() {
        return false;
    }
    has_prefix_fold(cleaned, "<|")
        || has_prefix_fold(cleaned, "call:")
        || cleaned.eq_ignore_ascii_case("final_response")
        || is_known_step_fold(cleaned)
}

fn is_known_step(step: &str) -> bool {
    ALL_STEPS.contains(&step)
}

fn is_known_step_fold(step: &str) -> bool {
    ALL_STEPS
        .iter()
        .any(|known| known.eq_ignore_ascii_case(step))
}

fn has_prefix_fold(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_catalog_matches_go_names_and_schema() {
        let names = alternative_dialog_tool_names();
        assert_eq!(
            names,
            vec![
                STEP_DRAW_IMAGE,
                STEP_GENERATE_SONG,
                STEP_VISION_IMAGE,
                STEP_CURRENCY_RATES,
                STEP_WEB_SEARCH,
                STEP_CRAWL_URL,
                STEP_YOUTUBE_SUMMARY,
                STEP_QUEUE_STATUS,
                STEP_CANCEL_DRAWING,
                STEP_TRANSLATE_TEXT,
                STEP_CHAT_HISTORY_SUMMARY,
            ]
        );

        let tools = chat_completion_tools_for_names(&names);
        assert_eq!(tools.len(), names.len());
        let draw = tools
            .iter()
            .find(|tool| tool.function.name == STEP_DRAW_IMAGE)
            .expect("draw tool");
        assert_eq!(draw.tool_type, "function");
        assert!(draw.function.description.contains("When to use:"));
        let properties = draw.function.parameters["properties"]
            .as_object()
            .expect("props");
        assert!(properties.contains_key("prompt"));
        assert!(properties.contains_key("negative_prompt"));
        assert_eq!(draw.function.parameters["required"], json!(["prompt"]));

        let translate = tools
            .iter()
            .find(|tool| tool.function.name == STEP_TRANSLATE_TEXT)
            .expect("translate tool");
        assert_eq!(translate.function.parameters["required"], json!(["text"]));
        assert!(
            tools
                .iter()
                .all(|tool| tool.function.name != "final_response")
        );
    }

    #[test]
    fn dialog_history_noise_filter_matches_go() {
        assert!(is_dialog_history_noise_tool_call_name(" Translate_Text "));
        assert!(is_dialog_history_noise_tool_call_name(
            "chat_history_summary"
        ));
        assert!(!is_dialog_history_noise_tool_call_name(""));
        assert!(!is_dialog_history_noise_tool_call_name("web_search"));

        let filtered = filter_dialog_tool_calls_for_history(&[
            openplotva_core::ToolCall {
                name: "web_search".to_owned(),
                ..openplotva_core::ToolCall::default()
            },
            openplotva_core::ToolCall {
                name: " translate_text ".to_owned(),
                ..openplotva_core::ToolCall::default()
            },
            openplotva_core::ToolCall {
                name: "final_response".to_owned(),
                ..openplotva_core::ToolCall::default()
            },
            openplotva_core::ToolCall {
                name: "chat_history_summary".to_owned(),
                ..openplotva_core::ToolCall::default()
            },
            openplotva_core::ToolCall {
                name: "draw_image".to_owned(),
                ..openplotva_core::ToolCall::default()
            },
        ]);

        assert_eq!(
            filtered
                .iter()
                .map(|call| call.name.as_str())
                .collect::<Vec<_>>(),
            vec!["web_search", "draw_image"]
        );
    }

    #[test]
    fn sanitizer_cuts_protocol_leaks_and_context_messages() {
        let raw = r#"A cozy, evening atmosphere. High quality digital art style."""}<tool_call|><|tool_response><|channel>thought"#;
        assert_eq!(
            sanitize_tool_text(raw),
            "A cozy, evening atmosphere. High quality digital art style."
        );

        let final_text = "<message id=\"1\"><text>old</text></message>\nНовый ответ.";
        assert_eq!(sanitize_final_text(final_text), "Новый ответ.");
        assert!(has_leading_context_message(final_text));
        assert_eq!(
            sanitize_final_text("Previous bot message\nmessage_id: 1\ntext: old answer"),
            ""
        );
        assert_eq!(
            sanitize_final_text(
                r#"<assistant_message id="7"><meta>copied</meta><text>Ответ восстановлен.</text></assistant_message>"#
            ),
            "Ответ восстановлен."
        );

        assert_eq!(
            sanitize_tool_value(json!({
                "prompt": "cat<|tool_call>call:draw_image{prompt:",
                "nested": ["ok", "bad<|tool_response>ignored"],
            })),
            json!({"prompt": "cat", "nested": ["ok", "bad"]})
        );
    }

    #[test]
    fn content_parser_salvages_go_tool_call_forms() -> Result<(), ToolParseError> {
        let cases: Vec<(String, ToolStep, &str)> = vec![
            (
                r#"<|tool_call>call:draw_image{negative_prompt:<|"|>blur<|"|>,prompt:<|"|>cat<|"|>}<tool_call|>"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat".to_owned(),
                    negative_prompt: "blur".to_owned(),
                    ..ToolStep::default()
                },
                "inline",
            ),
            (
                r#"draw_image{prompt:<|"|>cat in space<|"|>}"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    ..ToolStep::default()
                },
                "bare_start",
            ),
            (
                "Сначала текст.\n\nvision_image{file_id:\"message_177154_image_1\"}".to_owned(),
                ToolStep {
                    step: STEP_VISION_IMAGE.to_owned(),
                    file_id: "message_177154_image_1".to_owned(),
                    ..ToolStep::default()
                },
                "bare_block",
            ),
            (
                "```tool\n".to_owned() + r#"draw_image{prompt:"cat in space"}"# + "\n```",
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    ..ToolStep::default()
                },
                "fenced",
            ),
            (
                r#"draw_image({prompt:"cat in space"})"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    ..ToolStep::default()
                },
                "function_args",
            ),
            (
                r#"draw_image: {prompt:"cat in space"}"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    ..ToolStep::default()
                },
                "colon_args",
            ),
            (
                r#"tool: draw_image{prompt:"cat in space"}"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    ..ToolStep::default()
                },
                "tool_prefix",
            ),
            (
                r#"{"tool":"draw_image","prompt":"cat in space"}"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    ..ToolStep::default()
                },
                "json",
            ),
            (
                r#"<tool call="vision_image{file_id: 'message_11989713_image_1'}"/>"#.to_owned(),
                ToolStep {
                    step: STEP_VISION_IMAGE.to_owned(),
                    file_id: "message_11989713_image_1".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<tool:vision_image{file_id:"message_177164_image_1"}>"#.to_owned(),
                ToolStep {
                    step: STEP_VISION_IMAGE.to_owned(),
                    file_id: "message_177164_image_1".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<tool_call>chat_history_summary{window: "hours", hours: 6, message_count: 120, scope: "thread"}</tool_call>"#.to_owned(),
                ToolStep {
                    step: STEP_CHAT_HISTORY_SUMMARY.to_owned(),
                    window: "hours".to_owned(),
                    hours: 6,
                    message_count: 120,
                    scope: "thread".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<tool_calls><tool_call>web_search{query: "weather St. Petersburg June 2026 forecast"}</tool_call></tool_calls>"#.to_owned(),
                ToolStep {
                    step: STEP_WEB_SEARCH.to_owned(),
                    query: "weather St. Petersburg June 2026 forecast".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                "<tool_calls>\n  <tool_call>web_search{query: \"weather St. Petersburg June 2026 forecast\"}</tool_call>\n</tool_calls>".to_owned(),
                ToolStep {
                    step: STEP_WEB_SEARCH.to_owned(),
                    query: "weather St. Petersburg June 2026 forecast".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                " <tool_calls>\n\n  <tool_call>\n    web_search{query: \"weather St. Petersburg June 2026 forecast\"}\n  </tool_call>\n</tool_calls> ".to_owned(),
                ToolStep {
                    step: STEP_WEB_SEARCH.to_owned(),
                    query: "weather St. Petersburg June 2026 forecast".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
        ];

        for (raw, want, form) in cases {
            let (step, decision) = extract_content_tool_step(raw.as_ref())?;
            assert_eq!(step, Some(want));
            assert_eq!(decision.form, form);
            assert_eq!(decision.outcome, "detected");
        }
        Ok(())
    }

    #[test]
    fn content_parser_ignores_unknown_and_mid_sentence_bare_calls() -> Result<(), ToolParseError> {
        let (step, decision) =
            extract_content_tool_step(r#"Это пример draw_image{prompt:"cat"}, не вызов."#)?;
        assert_eq!(step, None);
        assert_eq!(decision.outcome, "none");

        for raw in [
            r#"do_magic{prompt:"cat"}"#,
            r#"{"tool":"do_magic","prompt":"cat"}"#,
            r#"<|tool_call>call:do_magic{prompt:<|"|>cat<|"|>}<tool_call|>"#,
        ] {
            let (step, decision) = extract_content_tool_step(raw)?;
            assert_eq!(step, None);
            assert_eq!(decision.outcome, "ignored");
            assert_eq!(decision.reason, "unknown_tool");
        }
        Ok(())
    }

    #[test]
    fn native_parser_salvages_quoted_prompt_and_rejects_sentinel() -> Result<(), ToolParseError> {
        let step = parse_native_tool_step(&[NativeToolCall {
            id: String::new(),
            call_type: "function".to_owned(),
            function: NativeToolFunction {
                name: STEP_DRAW_IMAGE.to_owned(),
                arguments: Value::String(r#"{"prompt":"кот "в шляпе""}"#.to_owned()),
            },
        }])?;
        assert_eq!(step.prompt, r#"кот "в шляпе""#);

        let error = parse_native_tool_step(&[NativeToolCall {
            id: String::new(),
            call_type: "function".to_owned(),
            function: NativeToolFunction {
                name: STEP_DRAW_IMAGE.to_owned(),
                arguments: json!({"prompt": "final_response"}),
            },
        }])
        .expect_err("sentinel rejected");
        assert!(error.to_string().contains("protocol sentinel"));
        Ok(())
    }

    #[test]
    fn parser_helpers_preserve_loose_go_syntax() {
        assert!(is_tool_like_name("draw-image_2"));
        assert!(!is_tool_like_name("draw image"));
        assert!(!is_tool_like_name(""));

        assert_eq!(
            parse_xmlish_attr_value(r#""draw_image &amp; more" other="x""#),
            Some("draw_image & more".to_owned())
        );
        assert_eq!(
            parse_xmlish_attr_value("draw_image/>"),
            Some("draw_image".to_owned())
        );
        assert_eq!(
            parse_regular_quoted_inline_arg(r#""cat\nwith\tmood""#),
            Some("cat\nwith\tmood".to_owned())
        );
        assert_eq!(
            parse_regular_quoted_inline_arg(r#""cat", prompt:"next""#),
            Some("cat".to_owned())
        );
        assert!(is_protocol_sentinel_value(" FINAL_RESPONSE "));
        assert!(is_protocol_sentinel_value(" CALL:draw_image "));
        assert!(is_protocol_sentinel_value(" DRAW_IMAGE "));
        assert!(!is_protocol_sentinel_value("обычный текст"));
    }
}
