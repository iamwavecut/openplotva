//! Provider-neutral dialog contracts and tool parsing.

use std::{error::Error, fmt, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

mod dispatch;
mod history;
mod json_codec;
mod persona;
pub mod tool_telemetry;
pub mod transcript;
pub mod turn;

pub use dispatch::dispatch_dialog_tool;

pub use history::{
    CapturedMemory, DEFAULT_CONTEXT_HISTORY_LIMIT, DailyPersona, DialogContext, DialogInput,
    DialogMessage, DialogOutput, DialogTraceArtifacts, DialogTraceUsage, DialogUser,
    HistoryMessage, MESSAGE_KIND_TEXT, MESSAGE_KIND_TOOL_REQUEST, MESSAGE_KIND_TOOL_RESPONSE,
    MessageKind, MultimodalImage, Persona, PersonaSnapshot, ROLE_MODEL, ROLE_TOOL, ROLE_USER,
    SettingKv, TurnContextArtifact, clone_history_messages, conversation_projection,
    history_message_has_context_content, history_meta_has_context_content,
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
pub use transcript::{
    ChatStepOutput, ChatStepRequest, ChatStepToolCall, SessionMessage, SessionToolCall, ToolsMode,
    dialog_tool_context,
};

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
pub struct RatesRequest {
    /// Tool context.
    pub context: ToolContext,
    /// Optional requested rate pairs/symbols, comma or space separated.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pairs: String,
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

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct HistorySearchRequest {
    /// Tool context.
    pub context: ToolContext,
    /// Keyword query for chat history search.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub query: String,
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
    fn currency_rates<'a>(&'a self, _req: RatesRequest) -> ToolboxFuture<'a> {
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

    /// Understand a supported visual, video, voice, or audio attachment.
    fn understand_media<'a>(&'a self, _req: VisionRequest) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_UNDERSTAND_MEDIA)
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

    /// Chat history keyword search tool.
    fn history_search<'a>(&'a self, _req: HistorySearchRequest) -> ToolboxFuture<'a> {
        unsupported_tool_future(STEP_HISTORY_SEARCH)
    }
}

fn unsupported_tool_future<'a>(tool: &'static str) -> ToolboxFuture<'a> {
    Box::pin(async move { Err(Box::new(DialogToolboxError::unsupported(tool)) as ToolboxError) })
}

pub const STEP_DRAW_IMAGE: &str = "draw_image";
pub const STEP_GENERATE_SONG: &str = "generate_song";
pub const STEP_UNDERSTAND_MEDIA: &str = "understand_media";
const LEGACY_STEP_VISION_IMAGE: &str = "vision_image";
pub const STEP_CURRENCY_RATES: &str = "currency_rates";
pub const STEP_WEB_SEARCH: &str = "web_search";
pub const STEP_CRAWL_URL: &str = "crawl_url";
pub const STEP_YOUTUBE_SUMMARY: &str = "youtube_summary";
pub const STEP_QUEUE_STATUS: &str = "queue_status";
pub const STEP_CANCEL_DRAWING: &str = "cancel_drawing";
pub const STEP_TRANSLATE_TEXT: &str = "translate_text";
pub const STEP_CHAT_HISTORY_SUMMARY: &str = "chat_history_summary";
pub const STEP_HISTORY_SEARCH: &str = "history_search";
pub const STEP_MEMORY_SEARCH: &str = "memory_search";

/// Session-engine tool: post an intermediate chat message without ending the
/// turn. Never part of the shared catalog — the legacy provider loop must not
/// advertise it (rollback safety); the session engine injects it itself.
pub const STEP_SEND_MESSAGE: &str = "send_message";

/// Session-engine tool: set a semantic emoji reaction on a chat message.
/// Never part of the shared catalog, same as [`STEP_SEND_MESSAGE`].
pub const STEP_REACT_TO_MESSAGE: &str = "react_to_message";

const ALL_STEPS: &[&str] = &[
    STEP_DRAW_IMAGE,
    STEP_GENERATE_SONG,
    STEP_UNDERSTAND_MEDIA,
    LEGACY_STEP_VISION_IMAGE,
    STEP_CURRENCY_RATES,
    STEP_WEB_SEARCH,
    STEP_CRAWL_URL,
    STEP_YOUTUBE_SUMMARY,
    STEP_QUEUE_STATUS,
    STEP_CANCEL_DRAWING,
    STEP_TRANSLATE_TEXT,
    STEP_CHAT_HISTORY_SUMMARY,
    STEP_HISTORY_SEARCH,
    STEP_MEMORY_SEARCH,
    STEP_SEND_MESSAGE,
    STEP_REACT_TO_MESSAGE,
];

const INLINE_TOOL_ARG_KEYS: &[&str] = &[
    "prompt",
    "topic",
    "file_id",
    "query",
    "url",
    "video",
    "pairs",
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
    "emoji",
    "chat_id",
    "thread_id",
    "message_id",
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
    "<tool_call",
    "</|tool_call",
    "</tool_call",
    "<|tool_response",
    "<tool_response",
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
        description: "Optional. Target aspect ratio, one of 1:1, 2:3, 3:2, 3:4, 4:3, 9:16, 16:9, 1:2, 2:1. Omit unless the user asked for a specific shape or orientation; the image pipeline infers a ratio otherwise.",
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
    description: "Supported image, raster/video sticker, animation, video, video-note, voice, audio, or MIME-typed document handle from the rendered attachments block, for example message_123_image_1 or message_123_video_1. Video media returns visual analysis and spoken-audio transcription together; voice and audio return transcription without visual decoding. Prefer the handle over opaque Telegram file_unique_id values. Omit only when the latest message has exactly one supported media attachment. Oversized files rejected by Telegram and unsupported media are permanent errors and must not be retried.",
}];

const CURRENCY_RATES_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "pairs",
    required: false,
    description: "Optional comma-separated known pairs or symbols, for example USD/RUB, BTC/USD, gold, Brent. Omit for the default set.",
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

const HISTORY_SEARCH_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "query",
    required: true,
    description: "Keywords to find relevant earlier messages in THIS chat's history.",
}];

const MEMORY_SEARCH_ARGS: &[ToolArgSpec] = &[ToolArgSpec {
    name: "query",
    required: true,
    description: "What to recall from long-term memory about the user or chat.",
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
        result: "Schedules image generation and ENDS your turn; the image arrives asynchronously. \
                 Anything you want to tell the chat, say it before or together with this call.",
        args: DRAW_IMAGE_ARGS,
    },
    ToolSpec {
        name: STEP_GENERATE_SONG,
        summary: "Generate a song from a topic or idea.",
        when_to_use: "Use when the latest user message asks for a song, track, lyrics-based song, or music generation.",
        result: "Schedules music generation and ENDS your turn; the song arrives asynchronously. \
                 Anything you want to tell the chat, say it before or together with this call.",
        args: GENERATE_SONG_ARGS,
    },
    ToolSpec {
        name: STEP_UNDERSTAND_MEDIA,
        summary: "Understand supported visual media, voice, and audio from chat context.",
        when_to_use: "Use when the latest user message asks about an image, supported sticker, animation, video, video note, voice, audio, or media document, including what is shown or what is said.",
        result: "Returns visual description for images and video, transcription for voice and audio, and both together for video. A missing audio track is an explicit note rather than a visual failure. Partial modality failures, unsupported media, missing references, and Telegram's permanent oversized-file download limit are returned with stable retryability.",
        args: VISION_IMAGE_ARGS,
    },
    ToolSpec {
        name: STEP_CURRENCY_RATES,
        summary: "Fetch current fiat and crypto exchange rates.",
        when_to_use: "Use when the latest user message asks about exchange rates or recent currency movement.",
        result: "Returns fresh rate data and may queue a rates side-effect message.",
        args: CURRENCY_RATES_ARGS,
    },
    ToolSpec {
        name: STEP_WEB_SEARCH,
        summary: "Search the web for current or external information.",
        when_to_use: "Use when the latest user message asks for current facts, news, recent events, or anything outside reliable memory.",
        result: "Returns search results with links; follow the promising ones with crawl_url when \
                 the snippets are not enough.",
        args: WEB_SEARCH_ARGS,
    },
    ToolSpec {
        name: STEP_CRAWL_URL,
        summary: "Fetch and extract readable text from a URL.",
        when_to_use: "Use to read a page found via web_search, or when the latest user message asks \
                      to inspect, summarize, or quote a specific webpage.",
        result: "Returns extracted page content — the natural follow-up to web_search links.",
        args: CRAWL_URL_ARGS,
    },
    ToolSpec {
        name: STEP_HISTORY_SEARCH,
        summary: "Search earlier messages in THIS chat by keywords.",
        when_to_use: "Use to ground on what was said before — recall references, in-jokes, decisions, names, or context the user implies from past conversation.",
        result: "Returns matching past messages (sender, time, text).",
        args: HISTORY_SEARCH_ARGS,
    },
    ToolSpec {
        name: STEP_MEMORY_SEARCH,
        summary: "Search long-term memory for facts about the user or chat.",
        when_to_use: "Use to personalize or ground on durable facts (preferences, identity, ongoing topics) the bot has remembered.",
        result: "Returns relevant remembered facts and recent episode summaries.",
        args: MEMORY_SEARCH_ARGS,
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

/// Tools exposed only to the reasoning agent (via an explicit allow-list), not to
/// the conversational model. Kept in the catalog so the agent can resolve their
/// schemas, but filtered out of the conversational tool list.
pub const AGENT_ONLY_TOOL_NAMES: &[&str] = &[STEP_MEMORY_SEARCH];

#[must_use]
pub fn alternative_dialog_tool_names() -> Vec<&'static str> {
    ALTERNATIVE_DIALOG_TOOL_CATALOG
        .iter()
        .map(|spec| spec.name)
        .filter(|name| !AGENT_ONLY_TOOL_NAMES.contains(name))
        .collect()
}

/// Return whether a tool call should be omitted from dialog history.
#[must_use]
pub fn is_dialog_history_noise_tool_call_name(name: &str) -> bool {
    let name = name.trim();
    name.eq_ignore_ascii_case(STEP_TRANSLATE_TEXT)
        || name.eq_ignore_ascii_case(STEP_CHAT_HISTORY_SUMMARY)
        // Session mechanics: a sent message is already real history, and a
        // reaction result carries nothing worth re-reading next turn.
        || name.eq_ignore_ascii_case(STEP_SEND_MESSAGE)
        || name.eq_ignore_ascii_case(STEP_REACT_TO_MESSAGE)
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
    chat_completion_tools_for_specs(
        &ALTERNATIVE_DIALOG_TOOL_CATALOG
            .iter()
            .filter(|spec| names.contains(&spec.name))
            .copied()
            .collect::<Vec<_>>(),
    )
}

/// Build native tool definitions from explicit specs — the session engine
/// uses this to add its own tools without putting them in the shared catalog.
#[must_use]
pub fn chat_completion_tools_for_specs(specs: &[ToolSpec]) -> Vec<ChatCompletionTool> {
    specs
        .iter()
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

/// Emoji the `react_to_message` tool may set: the Bot API bot-reaction list
/// minus the service ones (👀 queued, ✍ generating, 🤔 failure) so a semantic
/// reaction can never be mistaken for a lifecycle signal. Must stay in sync
/// with the list embedded in [`SESSION_REACT_TO_MESSAGE_SPEC`].
pub const SESSION_REACTION_ALLOWED_EMOJI: &[&str] = &[
    "❤",
    "👍",
    "👎",
    "🔥",
    "🥰",
    "👏",
    "😁",
    "🤯",
    "😱",
    "🤬",
    "😢",
    "🎉",
    "🤩",
    "🤮",
    "💩",
    "🙏",
    "👌",
    "🕊",
    "🤡",
    "🥱",
    "🥴",
    "😍",
    "🐳",
    "❤‍🔥",
    "🌚",
    "🌭",
    "💯",
    "🤣",
    "⚡",
    "🍌",
    "🏆",
    "💔",
    "🤨",
    "😐",
    "🍓",
    "🍾",
    "💋",
    "🖕",
    "😈",
    "😴",
    "😭",
    "🤓",
    "👻",
    "👨‍💻",
    "🎃",
    "🙈",
    "😇",
    "😨",
    "🤝",
    "🤗",
    "🫡",
    "🎅",
    "🎄",
    "☃",
    "💅",
    "🤪",
    "🗿",
    "🆒",
    "💘",
    "🙉",
    "🦄",
    "😘",
    "💊",
    "🙊",
    "😎",
    "👾",
    "🤷‍♂",
    "🤷",
    "🤷‍♀",
    "😡",
];

/// `send_message` spec, injected by the session engine only.
pub const SESSION_SEND_MESSAGE_SPEC: ToolSpec = ToolSpec {
    name: STEP_SEND_MESSAGE,
    summary: "Send an intermediate message to the chat NOW, without ending your turn.",
    when_to_use: "Use it for a short heads-up before slow work (a search, reading pages), or to \
                  split a long reply into several messages sent back to back. Plain assistant \
                  text WITHOUT tool calls always ends your turn — call send_message when you \
                  intend to continue working or writing afterwards. Never repeat a message you \
                  already sent this turn.",
    result: "Delivers the message immediately; returns ok, or an error when the per-turn message \
             limit is reached, the text duplicates an already-sent message, or the text is empty \
             after sanitization.",
    args: &[ToolArgSpec {
        name: "text",
        required: true,
        description: "Message text (Telegram HTML, same format as your normal replies).",
    }],
};

/// `react_to_message` spec, injected by the session engine only.
pub const SESSION_REACT_TO_MESSAGE_SPEC: ToolSpec = ToolSpec {
    name: STEP_REACT_TO_MESSAGE,
    summary: "Set one emoji reaction on a chat message — usually the message you are answering, \
              or another message visible in the conversation.",
    when_to_use: "A semantic, occasional gesture: react when an emoji genuinely fits the message \
                  (something funny, impressive, sad, or deserving a thumbs-up). Do not overuse \
                  it. Exactly ONE emoji per message, and there is no point calling this more \
                  than once for the same message — a new reaction only replaces the previous \
                  one. Allowed emoji (nothing else is accepted): ❤ 👍 👎 🔥 🥰 👏 😁 🤯 😱 🤬 😢 \
                  🎉 🤩 🤮 💩 🙏 👌 🕊 🤡 🥱 🥴 😍 🐳 ❤‍🔥 🌚 🌭 💯 🤣 ⚡ 🍌 🏆 💔 🤨 😐 🍓 🍾 💋 \
                  🖕 😈 😴 😭 🤓 👻 👨‍💻 🎃 🙈 😇 😨 🤝 🤗 🫡 🎅 🎄 ☃ 💅 🤪 🗿 🆒 💘 🙉 🦄 😘 💊 \
                  🙊 😎 👾 🤷‍♂ 🤷 🤷‍♀ 😡",
    result: "Sets the reaction; returns ok, or an error for a disallowed emoji or a message you \
             already reacted to this turn.",
    args: &[
        ToolArgSpec {
            name: "chat_id",
            required: true,
            description: "Chat id of the target message (the current chat).",
        },
        ToolArgSpec {
            name: "thread_id",
            required: false,
            description: "Forum thread id, when the chat uses topics.",
        },
        ToolArgSpec {
            name: "message_id",
            required: true,
            description: "Id of the message to react to.",
        },
        ToolArgSpec {
            name: "emoji",
            required: true,
            description: "One emoji from the allowed list.",
        },
    ],
};

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
    sanitize_assistant_text(value).text
}

/// Provider-visible assistant text after known reasoning and message protocol
/// scaffolding has been removed.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SanitizedAssistantText {
    /// Text that may be shown to the user.
    pub text: String,
    /// Whether known model protocol scaffolding was removed.
    pub removed_scaffolding: bool,
    /// Whether an ambiguous protocol fragment remained.
    pub residual_protocol: bool,
}

/// Extract only user-visible assistant text. This deliberately recognizes a
/// small protocol vocabulary instead of treating arbitrary HTML as model
/// metadata.
#[must_use]
pub fn sanitize_assistant_text(value: &str) -> SanitizedAssistantText {
    let raw = value.trim();
    if raw.is_empty() {
        return SanitizedAssistantText::default();
    }
    let decoded = unescape_xmlish(raw);
    let original = if decoded.trim() != raw && starts_with_known_protocol(decoded.trim()) {
        decoded.trim()
    } else {
        raw
    };

    let channel_stripped = strip_reasoning_channels(original);
    let mut cleaned = channel_stripped.trim().to_owned();
    let mut removed = cleaned != original;

    while let Some((rest, did_remove, malformed)) = strip_one_leading_reasoning_block(&cleaned) {
        removed |= did_remove;
        if malformed {
            return SanitizedAssistantText {
                removed_scaffolding: true,
                residual_protocol: true,
                ..SanitizedAssistantText::default()
            };
        }
        cleaned = rest;
    }

    if cleaned.is_empty() || has_leading_plain_context_message(&cleaned) {
        return SanitizedAssistantText {
            removed_scaffolding: removed || !original.is_empty(),
            ..SanitizedAssistantText::default()
        };
    }

    let (without_context, context_removed, context_malformed) =
        strip_leading_user_context_envelopes(&cleaned);
    removed |= context_removed;
    if context_malformed {
        return SanitizedAssistantText {
            removed_scaffolding: true,
            residual_protocol: true,
            ..SanitizedAssistantText::default()
        };
    }
    cleaned = without_context;

    if starts_with_message_scaffolding(&cleaned) {
        let text = extract_protocol_text_bodies(&cleaned);
        return SanitizedAssistantText {
            residual_protocol: text.is_none(),
            text: text.unwrap_or_default(),
            removed_scaffolding: true,
        };
    }

    let cleaned = sanitize_tool_text(&cleaned);
    let residual_protocol = reply_has_residual_leak(&cleaned);
    SanitizedAssistantText {
        text: if residual_protocol {
            String::new()
        } else {
            cleaned
        },
        removed_scaffolding: removed,
        residual_protocol,
    }
}

fn starts_with_known_protocol(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    [
        "<|channel",
        "<channel|",
        "<type:",
        "<tool_call",
        "<|tool_call",
    ]
    .into_iter()
    .any(|prefix| lower.starts_with(prefix))
        || [
            "thought",
            "analysis",
            "eigen_thought",
            "message",
            "last_message",
            "chat_context",
            "reference_context",
            "attach_id",
            "call",
            "assistant_message",
            "assistants_message",
            "reply",
            "assistant",
            "message_type",
            "text",
        ]
        .into_iter()
        .any(|tag| starts_with_xml_tag(&lower, tag))
}

fn strip_one_leading_reasoning_block(value: &str) -> Option<(String, bool, bool)> {
    let trimmed = value.trim_start();
    let lower = trimmed.to_ascii_lowercase();
    let tag = ["thought", "analysis", "eigen_thought"]
        .into_iter()
        .find(|tag| starts_with_xml_tag(&lower, tag))?;
    let open_end = trimmed.find('>')? + 1;
    let close = format!("</{tag}>");
    let Some(close_start) = lower[open_end..].find(&close).map(|idx| open_end + idx) else {
        return Some((String::new(), true, true));
    };
    let rest = trimmed[close_start + close.len()..].trim_start().to_owned();
    Some((rest, true, false))
}

fn strip_leading_user_context_envelopes(value: &str) -> (String, bool, bool) {
    let mut cleaned = value.trim().to_owned();
    let mut removed = false;
    loop {
        let lower = cleaned.to_ascii_lowercase();
        let tag = if starts_with_xml_tag(&lower, "message") {
            "message"
        } else if starts_with_xml_tag(&lower, "last_message") {
            "last_message"
        } else if starts_with_xml_tag(&lower, "chat_context") {
            "chat_context"
        } else if starts_with_xml_tag(&lower, "reference_context") {
            "reference_context"
        } else {
            return (cleaned, removed, false);
        };
        let close = format!("</{tag}>");
        let Some(end) = lower.find(&close) else {
            return (String::new(), true, true);
        };
        cleaned = cleaned[end + close.len()..].trim_start().to_owned();
        removed = true;
    }
}

fn starts_with_xml_tag(value: &str, tag: &str) -> bool {
    let prefix = format!("<{tag}");
    let Some(rest) = value.strip_prefix(&prefix) else {
        return false;
    };
    rest.chars()
        .next()
        .is_some_and(|ch| matches!(ch, '>' | '/') || ch.is_whitespace())
}

fn starts_with_message_scaffolding(value: &str) -> bool {
    let lower = value.trim_start().to_ascii_lowercase();
    [
        "assistant_message",
        "assistants_message",
        "reply",
        "assistant",
        "message_type",
        "text",
    ]
    .into_iter()
    .any(|tag| starts_with_xml_tag(&lower, tag))
}

fn extract_protocol_text_bodies(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    let mut from = 0;
    let mut bodies = Vec::new();
    while let Some(relative_open) = lower[from..].find("<text>") {
        let body_start = from + relative_open + "<text>".len();
        if let Some(relative_close) = lower[body_start..].find("</text>") {
            let body_end = body_start + relative_close;
            let body = value[body_start..body_end].trim();
            if !body.is_empty() {
                bodies.push(body.to_owned());
            }
            from = body_end + "</text>".len();
            continue;
        }
        let body = value[body_start..].trim();
        if !body.is_empty() && !body.contains('<') {
            bodies.push(body.to_owned());
        }
        break;
    }
    (!bodies.is_empty()).then(|| bodies.join("\n\n"))
}

#[must_use]
pub fn has_leading_context_message(value: &str) -> bool {
    let cleaned = sanitize_tool_text(value);
    let cleaned = cleaned.trim();
    let lower = cleaned.to_ascii_lowercase();
    starts_with_xml_tag(&lower, "message")
        || starts_with_xml_tag(&lower, "attach_id")
        || starts_with_xml_tag(&lower, "assistant_message")
        || starts_with_xml_tag(&lower, "assistants_message")
        || starts_with_xml_tag(&lower, "last_message")
        || has_leading_plain_context_message(cleaned)
}

fn has_leading_plain_context_message(value: &str) -> bool {
    let cleaned = value.trim();
    has_prefix_fold(cleaned, "previous bot message")
        || has_prefix_fold(cleaned, "предыдущая реплика")
}

// Harmony/channel framing (qwen3.6, Gemma variants) labels its reasoning channel
// "thought"; when the framing collapses the label/reasoning leaks ahead of the answer.
const REASONING_CHANNEL_MARKERS: &[&str] =
    &["<|channel|>", "<|channel>", "<channel|>", "</|channel>"];

const REASONING_CHANNEL_LABELS: &[&str] = &["thought", "analysis", "commentary"];

const REPLY_LEAK_MARKERS: &[&str] = &[
    "<|channel",
    "<channel|",
    "</|channel",
    "<think",
    "</think",
    "<|tool_call",
    "<tool_call",
    "<|tool_response",
    "<tool_response",
    "tool_request\n",
    "tool_result\n",
    "<chat_context",
    "<reference_context",
    "relevant memory (read-only",
    "<message id=",
    "<last_message",
    "<type:",
];

const REPLY_LEAK_TAGS: &[&str] = &[
    "thought",
    "analysis",
    "eigen_thought",
    "assistant_message",
    "assistants_message",
    "reply",
    "assistant",
    "message_type",
    "attach_id",
    "call",
];

#[must_use]
pub fn strip_reasoning_channels(content: &str) -> String {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let mut last_end: Option<usize> = None;
    for marker in REASONING_CHANNEL_MARKERS {
        let mut from = 0;
        while let Some(rel) = trimmed[from..].find(marker) {
            let end = from + rel + marker.len();
            last_end = Some(last_end.map_or(end, |prev| prev.max(end)));
            from = end;
        }
    }
    if let Some(end) = last_end {
        return strip_leading_channel_label(trimmed[end..].trim()).to_owned();
    }
    // A leading bare reasoning label with no answer channel is a reasoning-only leak.
    if strip_leading_channel_label(trimmed).len() != trimmed.len() {
        return String::new();
    }
    content.to_owned()
}

fn strip_leading_channel_label(value: &str) -> &str {
    let value = value.trim_start();
    for label in REASONING_CHANNEL_LABELS {
        let Some(head) = value.get(..label.len()) else {
            continue;
        };
        if head.eq_ignore_ascii_case(label) {
            let rest = &value[label.len()..];
            // Only when the label is on its own line — "thought" mid-reply stays.
            if rest.is_empty() || rest.starts_with('\n') || rest.starts_with('\r') {
                return rest.trim_start();
            }
        }
    }
    value
}

#[must_use]
pub fn reply_has_residual_leak(value: &str) -> bool {
    // Only a leak that *opens* the reply — a tag quoted mid-text is fine.
    let lower = value.trim_start().to_lowercase();
    REPLY_LEAK_MARKERS
        .iter()
        .any(|marker| lower.starts_with(marker))
        || REPLY_LEAK_TAGS
            .iter()
            .any(|tag| starts_with_xml_tag(&lower, tag))
}

fn is_cyrillic(ch: char) -> bool {
    ('\u{0400}'..='\u{052f}').contains(&ch)
        || ('\u{2de0}'..='\u{2dff}').contains(&ch)
        || ('\u{a640}'..='\u{a69f}').contains(&ch)
}

#[must_use]
pub fn pathological_final_answer_reason(value: &str) -> Option<String> {
    let mut max_run = 0;
    let mut current_run = 0;
    let mut previous = '\0';
    let mut cyrillic = 0;
    let mut spaces = 0;
    for ch in value.trim().chars() {
        if ch.is_whitespace() {
            spaces += 1;
        }
        if is_cyrillic(ch) {
            cyrillic += 1;
        }
        if ch == previous && ch.is_alphanumeric() {
            current_run += 1;
        } else {
            current_run = 1;
            previous = ch;
        }
        max_run = max_run.max(current_run);
    }
    if max_run >= 24 {
        return Some("repeated character run".to_owned());
    }
    if cyrillic >= 80 && spaces <= 1 {
        return Some("missing word spaces".to_owned());
    }
    let mut repeated_segments = std::collections::HashMap::<String, usize>::new();
    for segment in value.split(['\n', '.', '!', '?']) {
        let normalized = segment
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase();
        if normalized.chars().count() < 8 {
            continue;
        }
        let count = repeated_segments.entry(normalized).or_default();
        *count += 1;
        if *count >= 3 {
            return Some("repeated phrase".to_owned());
        }
    }
    None
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DialogReplySuppression {
    Empty,
    ContextLeak,
    ProtocolOnly,
    ReasoningLeak,
    Pathological(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DialogReplyOutcome {
    Reply(String),
    Suppressed(DialogReplySuppression),
}

// Provider-agnostic reply finalization: every LLM provider funnels raw assistant
// content through here to recover the answer and refuse to emit internal leaks.
#[must_use]
pub fn finalize_dialog_reply(content: &str) -> DialogReplyOutcome {
    if content.trim().is_empty() {
        return DialogReplyOutcome::Suppressed(DialogReplySuppression::Empty);
    }
    let sanitized = sanitize_assistant_text(content);
    let answer = sanitized.text;
    if answer.trim().is_empty() {
        if has_leading_context_message(content) {
            return DialogReplyOutcome::Suppressed(DialogReplySuppression::ContextLeak);
        }
        if sanitized.residual_protocol {
            return DialogReplyOutcome::Suppressed(DialogReplySuppression::ReasoningLeak);
        }
        return DialogReplyOutcome::Suppressed(DialogReplySuppression::ProtocolOnly);
    }
    if reply_has_residual_leak(&answer) {
        return DialogReplyOutcome::Suppressed(DialogReplySuppression::ReasoningLeak);
    }
    if let Some(reason) = pathological_final_answer_reason(&answer) {
        return DialogReplyOutcome::Suppressed(DialogReplySuppression::Pathological(reason));
    }
    DialogReplyOutcome::Reply(answer)
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
    /// Optional requested currency/market pairs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pairs: String,
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
    /// Reaction emoji (`react_to_message`).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub emoji: String,
    /// Reaction target chat (`react_to_message`; the engine clamps it to the
    /// session's chat regardless of what the model passes).
    #[serde(default, rename = "chat_id", skip_serializing_if = "is_zero_i64")]
    pub target_chat_id: i64,
    /// Reaction target thread (`react_to_message`).
    #[serde(default, rename = "thread_id", skip_serializing_if = "is_zero_i64")]
    pub target_thread_id: i64,
    /// Reaction target message (`react_to_message`).
    #[serde(default, rename = "message_id", skip_serializing_if = "is_zero_i64")]
    pub target_message_id: i64,
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
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

/// Typed interpretation of one assistant content string.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedAssistantContent {
    /// User-visible text after protocol sanitization.
    pub text: String,
    /// Ordered pseudo-tool calls recovered from the content channel.
    pub tool_steps: Vec<ToolStep>,
    /// Parser decision used for observability.
    pub decision: ToolParseDecision,
    /// Whether known reasoning or message scaffolding was removed.
    pub removed_scaffolding: bool,
    /// Whether ambiguous protocol markup remains.
    pub residual_protocol: bool,
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

/// Parse pseudo-tool calls before sanitizing the remaining assistant text.
pub fn parse_assistant_content(
    raw_content: &str,
) -> Result<ParsedAssistantContent, ToolParseError> {
    let (tool_steps, decision) = extract_content_tool_steps(raw_content)?;
    let sanitized = if tool_steps.is_empty() {
        sanitize_assistant_text(raw_content)
    } else {
        let Some(remainder) = content_without_pseudo_tool_calls(raw_content, &decision) else {
            return Ok(ParsedAssistantContent {
                tool_steps,
                decision,
                removed_scaffolding: true,
                residual_protocol: true,
                ..ParsedAssistantContent::default()
            });
        };
        let mut sanitized = sanitize_assistant_text(&remainder);
        sanitized.removed_scaffolding = true;
        sanitized
    };
    Ok(ParsedAssistantContent {
        text: sanitized.text,
        tool_steps,
        decision,
        removed_scaffolding: sanitized.removed_scaffolding,
        residual_protocol: sanitized.residual_protocol,
    })
}

fn content_without_pseudo_tool_calls(raw: &str, decision: &ToolParseDecision) -> Option<String> {
    let decoded = unescape_xmlish(raw);
    let content = if decision.form == "xmlish" && !raw.contains('<') && decoded.contains('<') {
        decoded.as_str()
    } else {
        raw
    };
    match decision.form.as_str() {
        "xmlish" => remove_leading_xmlish_named_call_protocol(content)
            .or_else(|| remove_xmlish_tool_protocol(content)),
        "inline" => remove_inline_tool_protocol(content),
        "bare_start" | "bare_block" => remove_bare_tool_protocol(content),
        "fenced" => remove_fenced_tool_protocol(content),
        "json" => Some(String::new()),
        "tool_prefix" | "function_args" | "colon_args" => {
            remove_tool_protocol_line(content, &decision.tool)
        }
        _ => None,
    }
}

fn remove_leading_xmlish_named_call_protocol(raw: &str) -> Option<String> {
    let protocol = strip_reasoning_channels(raw);
    let protocol = protocol.trim_start();
    if !starts_with_xml_tag(&protocol.to_ascii_lowercase(), "call") {
        return None;
    }
    let mut offset = raw.rfind(protocol)?;
    let mut spans = Vec::new();
    loop {
        offset += raw[offset..].len() - raw[offset..].trim_start().len();
        let lower = raw[offset..].to_ascii_lowercase();
        if !starts_with_xml_tag(&lower, "call") {
            break;
        }
        let open_end = raw[offset..].find('>')? + offset + 1;
        let close = "</call>";
        let close_start = index_fold(&raw[open_end..], close)? + open_end;
        let end = close_start + close.len();
        spans.push((offset, end));
        offset = end;
    }
    (!spans.is_empty()).then(|| remove_content_spans(raw, &spans))
}

fn remove_xmlish_tool_protocol(raw: &str) -> Option<String> {
    let mut spans = Vec::new();
    let mut offset = 0;
    while let Some(relative_start) = raw[offset..].find('<') {
        let start = offset + relative_start;
        if raw[start..].starts_with("</") {
            offset = start + 2;
            continue;
        }
        let relative_open_end = raw[start..].find('>')?;
        let open_end = start + relative_open_end + 1;
        let tag = &raw[start..open_end];
        let name = xmlish_tool_tag_name(tag);
        let recognized = matches!(name.as_str(), "tool" | "tool_call" | "tool_calls")
            || name.starts_with("tool:")
            || canonical_known_step(&name).is_some();
        if !recognized {
            offset = open_end;
            continue;
        }
        if tag.trim_end().ends_with("/>")
            || name.starts_with("tool:")
            || canonical_known_step(&name)
                .is_some_and(|step| xmlish_direct_tool_tag_has_args(tag, step))
        {
            spans.push((start, open_end));
            offset = open_end;
            continue;
        }
        let close = format!("</{name}>");
        let relative_close = index_fold(&raw[open_end..], &close)?;
        let end = open_end + relative_close + close.len();
        spans.push((start, end));
        offset = end;
    }
    (!spans.is_empty()).then(|| remove_content_spans(raw, &spans))
}

fn remove_inline_tool_protocol(raw: &str) -> Option<String> {
    let (start, marker_len) = find_inline_tool_call_start(raw)?;
    let call_start = start + marker_len;
    let end = [
        "<tool_call|>",
        "</tool_call>",
        "</|tool_call>",
        "</|tool_call|>",
    ]
    .into_iter()
    .filter_map(|marker| {
        raw[call_start..]
            .find(marker)
            .map(|relative| call_start + relative + marker.len())
    })
    .min()
    .unwrap_or(raw.len());
    Some(remove_content_spans(raw, &[(start, end)]))
}

fn remove_bare_tool_protocol(raw: &str) -> Option<String> {
    let start = usize::try_from(bare_tool_call_start(raw)).ok()?;
    let open = raw[start..].find('{')? + start;
    let end = matching_brace_end(raw, open)?;
    Some(remove_content_spans(raw, &[(start, end)]))
}

fn matching_brace_end(raw: &str, open: usize) -> Option<usize> {
    let mut depth = 0_u32;
    let mut quote = None;
    let mut escaped = false;
    for (relative, ch) in raw[open..].char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' && quote.is_some() {
            escaped = true;
            continue;
        }
        if matches!(ch, '\'' | '"') {
            if quote == Some(ch) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(ch);
            }
            continue;
        }
        if quote.is_some() {
            continue;
        }
        match ch {
            '{' => depth += 1,
            '}' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(open + relative + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

fn remove_fenced_tool_protocol(raw: &str) -> Option<String> {
    let mut offset = 0;
    while let Some(relative_start) = raw[offset..].find("```") {
        let start = offset + relative_start;
        let header_end = raw[start + 3..].find(['\n', '\r'])? + start + 3;
        let body_start = header_end + 1;
        let relative_end = raw[body_start..].find("```")?;
        let end = body_start + relative_end + 3;
        if detect_tool_step_in(raw[body_start..body_start + relative_end].trim())
            .ok()
            .flatten()
            .is_some()
        {
            return Some(remove_content_spans(raw, &[(start, end)]));
        }
        offset = end;
    }
    None
}

fn remove_tool_protocol_line(raw: &str, tool: &str) -> Option<String> {
    let mut start = 0;
    for line in raw.split_inclusive('\n') {
        let trimmed = line.trim();
        if detect_tool_step_in(trimmed)
            .ok()
            .flatten()
            .is_some_and(|(step, _)| step.step == tool)
        {
            return Some(remove_content_spans(raw, &[(start, start + line.len())]));
        }
        start += line.len();
    }
    None
}

fn remove_content_spans(raw: &str, spans: &[(usize, usize)]) -> String {
    let mut output = String::with_capacity(raw.len());
    let mut cursor = 0;
    for &(start, end) in spans {
        if start < cursor || end < start || end > raw.len() {
            continue;
        }
        output.push_str(&raw[cursor..start]);
        cursor = end;
    }
    output.push_str(&raw[cursor..]);
    output.trim().to_owned()
}

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
    let (steps, decision) = extract_content_tool_steps(raw_content)?;
    Ok((steps.into_iter().next(), decision))
}

/// Parse model content for one or more XML-ish, inline, normalized, JSON, or
/// bare tool calls.
pub fn extract_content_tool_steps(
    raw_content: &str,
) -> Result<(Vec<ToolStep>, ToolParseDecision), ToolParseError> {
    let raw_content = raw_content.trim();
    if let Some((steps, decision)) = detect_tool_steps_in(raw_content)? {
        return Ok((steps, decision));
    }
    // Some backends leak a tool call as HTML-entity-encoded markup in the
    // assistant text (e.g. `&lt;tool_calls&gt;web_search{...}&lt;/tool_calls&gt;`).
    // Decoding lets the same parsers recognize and execute it instead of letting
    // the markup reach the send boundary, where sanitization strips it to empty.
    let decoded = unescape_xmlish(raw_content);
    if decoded.trim() != raw_content
        && let Some((steps, decision)) = detect_tool_steps_in(decoded.trim())?
    {
        return Ok((steps, decision));
    }
    if let Some(decision) = detect_ignored_tool_call(raw_content) {
        return Ok((Vec::new(), decision));
    }
    Ok((
        Vec::new(),
        ToolParseDecision {
            outcome: "none".to_owned(),
            reason: "no_tool_call".to_owned(),
            ..ToolParseDecision::default()
        },
    ))
}

fn detect_tool_steps_in(
    content: &str,
) -> Result<Option<(Vec<ToolStep>, ToolParseDecision)>, ToolParseError> {
    let named_call_steps = parse_xmlish_named_call_steps(content)?;
    if !named_call_steps.is_empty() {
        let decision = ToolParseDecision {
            form: "xmlish".to_owned(),
            tool: named_call_steps
                .iter()
                .map(|step| step.step.as_str())
                .collect::<Vec<_>>()
                .join(","),
            outcome: "detected".to_owned(),
            reason: String::new(),
        };
        return Ok(Some((named_call_steps, decision)));
    }

    let direct_steps = parse_xmlish_direct_tool_tag_steps(content)?;
    if !direct_steps.is_empty() {
        let decision = ToolParseDecision {
            form: "xmlish".to_owned(),
            tool: direct_steps
                .iter()
                .map(|step| step.step.as_str())
                .collect::<Vec<_>>()
                .join(","),
            outcome: "detected".to_owned(),
            reason: String::new(),
        };
        return Ok(Some((direct_steps, decision)));
    }

    detect_tool_step_in(content)
        .map(|maybe_step| maybe_step.map(|(step, decision)| (vec![step], decision)))
}

fn parse_xmlish_named_call_steps(raw: &str) -> Result<Vec<ToolStep>, ToolParseError> {
    let protocol = strip_reasoning_channels(raw);
    let mut remaining = protocol.trim_start();
    if !starts_with_xml_tag(&remaining.to_ascii_lowercase(), "call") {
        return Ok(Vec::new());
    }

    let mut steps = Vec::new();
    loop {
        let lower = remaining.to_ascii_lowercase();
        if !starts_with_xml_tag(&lower, "call") {
            break;
        }
        let open_end = remaining
            .find('>')
            .ok_or_else(|| ToolParseError::new("unterminated XML-ish call tag"))?
            + 1;
        let close = "</call>";
        let body_end = index_fold(&remaining[open_end..], close)
            .map(|offset| open_end + offset)
            .ok_or_else(|| ToolParseError::new("unterminated XML-ish call element"))?;
        let body = &remaining[open_end..body_end];
        let raw_name = xmlish_child_text(body, "tool_name")
            .ok_or_else(|| ToolParseError::new("XML-ish call has no tool_name"))?;
        let name = canonical_known_step(&raw_name)
            .ok_or_else(|| ToolParseError::new(format!("unknown step {raw_name:?}")))?;
        let arguments_body = xmlish_child_text(body, "arguments").unwrap_or_default();
        let mut arguments = serde_json::Map::new();
        for key in INLINE_TOOL_ARG_KEYS {
            if let Some(value) = xmlish_child_text(&arguments_body, key) {
                arguments.insert((*key).to_owned(), Value::String(value));
            }
        }
        steps.push(decode_tool_call_arguments(name, &Value::Object(arguments))?);

        remaining = remaining[body_end + close.len()..].trim_start();
    }
    Ok(steps)
}

fn detect_tool_step_in(
    content: &str,
) -> Result<Option<(ToolStep, ToolParseDecision)>, ToolParseError> {
    for parser in [
        parse_xmlish_tool_step_attempt,
        parse_inline_tool_step_attempt,
        parse_normalized_tool_step_attempt,
        parse_bare_tool_step_attempt,
    ] {
        let (step, ok, form) = parser(content)?;
        if ok {
            let decision = ToolParseDecision {
                form,
                tool: step.step.clone(),
                outcome: "detected".to_owned(),
                reason: String::new(),
            };
            return Ok(Some((step, decision)));
        }
    }
    Ok(None)
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
    if let Some(Value::String(value)) = map.get("step") {
        step.step = value.trim().to_owned();
    }
    populate_tool_args(|key| json_scalar_arg(map.get(key)), step);
}

fn json_scalar_arg(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
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
    if let Some(step) = parse_xmlish_direct_tool_tag(raw)? {
        return Ok((step, true));
    }
    let Some(tag) = first_xmlish_tool_tag(raw) else {
        return Ok((ToolStep::default(), false));
    };
    if let Some(call) = xmlish_tool_attr(&tag, "call") {
        return parse_bare_tool_call_step(&call);
    }
    if let Some(step) = parse_xmlish_tool_attrs(&tag)? {
        return Ok((step, true));
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
    Ok((ToolStep::default(), false))
}

fn parse_xmlish_tool_attrs(tag: &str) -> Result<Option<ToolStep>, ToolParseError> {
    let Some(name) = xmlish_tool_attr(tag, "name") else {
        return Ok(None);
    };
    let name = name.trim();
    if !is_known_step(name) {
        return Ok(None);
    }
    let mut step = ToolStep {
        step: name.to_owned(),
        ..ToolStep::default()
    };
    populate_xmlish_tool_attrs(tag, &mut step)?;
    normalize_and_validate_step(step).map(Some)
}

fn parse_xmlish_direct_tool_tag(raw: &str) -> Result<Option<ToolStep>, ToolParseError> {
    let Some(tag) = first_xmlish_direct_tool_tag(raw) else {
        return Ok(None);
    };
    parse_xmlish_direct_tool_tag_step(&tag)
}

fn parse_xmlish_direct_tool_tag_steps(raw: &str) -> Result<Vec<ToolStep>, ToolParseError> {
    let mut steps = Vec::new();
    for (tag, body) in xmlish_direct_tool_elements(raw)? {
        if let Some(step) = parse_xmlish_direct_tool_tag_step(&tag)? {
            steps.push(step);
            continue;
        }
        let Some(name) = canonical_known_step(&xmlish_tool_tag_name(&tag)) else {
            continue;
        };
        let Some(body) = body else {
            continue;
        };
        let mut arguments = serde_json::Map::new();
        for key in INLINE_TOOL_ARG_KEYS {
            if let Some(value) = xmlish_child_text(&body, key) {
                arguments.insert((*key).to_owned(), Value::String(value));
            }
        }
        steps.push(decode_tool_call_arguments(name, &Value::Object(arguments))?);
    }
    Ok(steps)
}

fn xmlish_direct_tool_elements(raw: &str) -> Result<Vec<(String, Option<String>)>, ToolParseError> {
    let mut elements = Vec::new();
    let mut offset = 0;
    while let Some(idx) = raw[offset..].find('<') {
        let start = offset + idx;
        if raw[start..].starts_with("</") {
            offset = start + 2;
            continue;
        }
        let Some(relative_end) = raw[start..].find('>') else {
            return Err(ToolParseError::new("unterminated XML-ish tool tag"));
        };
        let open_end = start + relative_end;
        let tag = raw[start..=open_end].to_owned();
        let name = xmlish_tool_tag_name(&tag);
        if canonical_known_step(&name).is_none() {
            offset = open_end + 1;
            continue;
        }
        if tag.trim_end().ends_with("/>") || xmlish_direct_tool_tag_has_args(&tag, &name) {
            elements.push((tag, None));
            offset = open_end + 1;
            continue;
        }
        let close_tag = format!("</{name}>");
        let body_start = open_end + 1;
        let Some(relative_body_end) = index_fold(&raw[body_start..], &close_tag) else {
            return Err(ToolParseError::new(format!(
                "unterminated XML-ish {name} tool element"
            )));
        };
        let body_end = body_start + relative_body_end;
        elements.push((tag, Some(unescape_xmlish(raw[body_start..body_end].trim()))));
        offset = body_end + close_tag.len();
    }
    Ok(elements)
}

fn xmlish_child_text(body: &str, name: &str) -> Option<String> {
    let open = format!("<{name}>");
    let start = index_fold(body, &open)? + open.len();
    let close = format!("</{name}>");
    let end = index_fold(&body[start..], &close)? + start;
    let value = unescape_xmlish(body[start..end].trim());
    if value.is_empty() { None } else { Some(value) }
}

fn parse_xmlish_direct_tool_tag_step(tag: &str) -> Result<Option<ToolStep>, ToolParseError> {
    let Some(name) = canonical_known_step(&xmlish_tool_tag_name(tag)) else {
        return Ok(None);
    };
    if !xmlish_direct_tool_tag_has_args(tag, name) {
        return Ok(None);
    }
    let mut step = ToolStep {
        step: name.to_owned(),
        ..ToolStep::default()
    };
    populate_xmlish_tool_attrs(tag, &mut step)?;
    normalize_and_validate_step(step).map(Some)
}

fn first_xmlish_direct_tool_tag(raw: &str) -> Option<String> {
    xmlish_direct_tool_tags(raw).into_iter().next()
}

fn xmlish_direct_tool_tags(raw: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut offset = 0;
    while let Some(idx) = raw[offset..].find('<') {
        let start = offset + idx;
        if raw[start..].starts_with("</") {
            offset = start + 2;
            continue;
        }
        let Some(end) = raw[start..].find('>') else {
            break;
        };
        let tag = &raw[start..start + end + 1];
        if canonical_known_step(&xmlish_tool_tag_name(tag)).is_some() {
            tags.push(tag.to_owned());
        }
        offset = start + end + 1;
    }
    tags
}

fn xmlish_direct_tool_tag_has_args(tag: &str, step: &str) -> bool {
    matches!(step, STEP_QUEUE_STATUS | STEP_CANCEL_DRAWING)
        || INLINE_TOOL_ARG_KEYS
            .iter()
            .any(|key| xmlish_tool_attr(tag, key).is_some())
        || xmlish_tool_attr(tag, "arg").is_some()
        || xmlish_tool_attr(tag, "args").is_some()
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

fn populate_xmlish_tool_attrs(tag: &str, step: &mut ToolStep) -> Result<(), ToolParseError> {
    populate_tool_args(|key| xmlish_tool_attr(tag, key), step);
    if let Some(arg) = xmlish_tool_attr(tag, "arg") {
        populate_jsonish_or_inline_args(&arg, step)?;
    }
    if let Some(args) = xmlish_tool_attr(tag, "args") {
        populate_jsonish_or_inline_args(&args, step)?;
    }
    Ok(())
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
    if let Some(value) = lookup("pairs") {
        step.pairs = value;
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
    if let Some(value) = lookup("emoji") {
        step.emoji = value;
    }
    if let Some(value) = lookup("chat_id").and_then(|value| value.trim().parse::<i64>().ok()) {
        step.target_chat_id = value;
    }
    if let Some(value) = lookup("thread_id").and_then(|value| value.trim().parse::<i64>().ok()) {
        step.target_thread_id = value;
    }
    if let Some(value) = lookup("message_id").and_then(|value| value.trim().parse::<i64>().ok()) {
        step.target_message_id = value;
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
    if step.step.eq_ignore_ascii_case(LEGACY_STEP_VISION_IMAGE) {
        step.step = STEP_UNDERSTAND_MEDIA.to_owned();
    }
    step.prompt = sanitize_tool_text(&step.prompt);
    step.topic = sanitize_tool_text(&step.topic);
    step.file_id = sanitize_tool_text(&step.file_id);
    step.query = sanitize_tool_text(&step.query);
    step.url = sanitize_tool_text(&step.url);
    step.video = sanitize_tool_text(&step.video);
    step.pairs = sanitize_tool_text(&step.pairs);
    step.text = sanitize_tool_text(&step.text);
    step.target_lang = sanitize_tool_text(&step.target_lang);
    step.window = sanitize_tool_text(&step.window);
    step.since = sanitize_tool_text(&step.since);
    step.scope = sanitize_tool_text(&step.scope);
    step.negative_prompt = sanitize_tool_text(&step.negative_prompt);
    step.aspect_ratio = sanitize_tool_text(&step.aspect_ratio);
    step.seed = sanitize_tool_text(&step.seed);
    step.emoji = step.emoji.trim().to_owned();

    if !is_known_step(&step.step) {
        return Err(ToolParseError::new(format!("unknown step {:?}", step.step)));
    }
    if step_contains_protocol_sentinel_argument(&step) {
        return Err(ToolParseError::new(format!(
            "{} tool argument contains protocol sentinel",
            step.step
        )));
    }
    Ok(step)
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
        step.emoji.as_str(),
        step.pairs.as_str(),
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

fn canonical_known_step(step: &str) -> Option<&'static str> {
    ALL_STEPS
        .iter()
        .copied()
        .find(|known| known.eq_ignore_ascii_case(step.trim()))
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
    fn finalize_dialog_reply_recovers_channels_and_suppresses_leaks() {
        use DialogReplyOutcome::{Reply, Suppressed};
        use DialogReplySuppression::{
            ContextLeak, Empty, Pathological, ProtocolOnly, ReasoningLeak,
        };

        // Harmony/channel framing: the answer is the final channel; the leaked
        // "thought" reasoning label must not survive.
        assert_eq!(
            finalize_dialog_reply("thought\n<channel|>😂"),
            Reply("😂".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply("<|channel>thought\n<channel|>Как это понимать?"),
            Reply("Как это понимать?".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply("<channel|><|channel>thought\n<channel|>ответ"),
            Reply("ответ".to_owned())
        );

        // Bare reasoning label with no answer channel → suppressed for regen.
        assert_eq!(
            finalize_dialog_reply("thought\n\n"),
            Suppressed(ProtocolOnly)
        );
        assert_eq!(
            finalize_dialog_reply("thought\nМне нужно просто ответить, но я зациклился."),
            Suppressed(ProtocolOnly)
        );
        // The reasoning label is matched case-insensitively.
        assert_eq!(
            finalize_dialog_reply("Thought\n\n"),
            Suppressed(ProtocolOnly)
        );
        assert_eq!(
            finalize_dialog_reply("ANALYSIS\nСначала подумаю."),
            Suppressed(ProtocolOnly)
        );

        // Closed known reasoning blocks are removed while their answer survives.
        assert_eq!(
            finalize_dialog_reply("<chat_context><reference_context>leaked</reference_context>"),
            Suppressed(ReasoningLeak)
        );
        assert_eq!(
            finalize_dialog_reply("<thought>сначала рассуждение</thought>\nОтвет"),
            Reply("Ответ".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply("<type:analysis>внутренний протокол\nОтвет"),
            Suppressed(ReasoningLeak)
        );
        // Persona terms in prose must NOT be suppressed (false-positive guard).
        assert_eq!(
            finalize_dialog_reply("Моя кастомная персона важнее, base_voice не при чём."),
            Reply("Моя кастомная персона важнее, base_voice не при чём.".to_owned())
        );
        // A tag quoted mid-reply is not a leak — the marker must open the reply.
        assert_eq!(
            finalize_dialog_reply("Оберни мысли в <think> теги, чтобы их скрыть."),
            Reply("Оберни мысли в <think> теги, чтобы их скрыть.".to_owned())
        );

        // Copied context echo → context leak; blank → empty.
        assert_eq!(
            finalize_dialog_reply("<message id=\"1\"><text>old</text></message>"),
            Suppressed(ContextLeak)
        );
        assert_eq!(
            finalize_dialog_reply(
                r#"<attach_id>171363</attach_id>
<message id="171363" thread_id="171360" timestamp="2026-07-13T14:49:25Z">
  <user username="Плотва" type="bot"></user>
  <message_type>text</message_type>
  <text>*шепотом* ...они уже начали ГОВОРИТЬ в эфире...</text>
</message>"#
            ),
            Suppressed(ContextLeak)
        );
        assert_eq!(finalize_dialog_reply("   "), Suppressed(Empty));

        // Exact production message envelope: only the text node is visible.
        assert_eq!(
            finalize_dialog_reply(
                r#"<reply to_id="6877"><to_user>WaveCut</to_user></reply>
<assistant id="6878">Плотва</assistant>
<message_type>text</message_type>
<text>Да, это уже нормальный ответ.</text>"#
            ),
            Reply("Да, это уже нормальный ответ.".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply(
                "&lt;reply&gt;&lt;to_user&gt;Ada&lt;/to_user&gt;&lt;/reply&gt;\n&lt;message_type&gt;text&lt;/message_type&gt;\n&lt;text&gt;entity answer&lt;/text&gt;"
            ),
            Reply("entity answer".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply(
                "<message_type>text</message_type><text>first</text><text>second</text>"
            ),
            Reply("first\n\nsecond".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply("<reply></reply><text>malformed but clear"),
            Reply("malformed but clear".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply("```xml\n<reply><to_user>Ada</to_user></reply>\n```"),
            Reply("```xml\n<reply><to_user>Ada</to_user></reply>\n```".to_owned())
        );
        assert_eq!(
            sanitize_assistant_text("<assistantship>fiction</assistantship>").text,
            "<assistantship>fiction</assistantship>"
        );

        // Degenerate loop → pathological.
        assert!(matches!(
            finalize_dialog_reply(&"а".repeat(30)),
            Suppressed(Pathological(_))
        ));
        assert_eq!(
            finalize_dialog_reply(
                "Ничего не вижу на экране. Ничего не вижу на экране. Ничего не вижу на экране."
            ),
            Suppressed(Pathological("repeated phrase".to_owned()))
        );

        // Real replies are untouched — including ones that merely mention
        // "thought" mid-sentence.
        assert_eq!(
            finalize_dialog_reply("Логично."),
            Reply("Логично.".to_owned())
        );
        assert_eq!(
            finalize_dialog_reply("I thought so too."),
            Reply("I thought so too.".to_owned())
        );
    }

    #[test]
    fn tool_catalog_matches_go_names_and_schema() {
        let names = alternative_dialog_tool_names();
        assert_eq!(
            names,
            vec![
                STEP_DRAW_IMAGE,
                STEP_GENERATE_SONG,
                STEP_UNDERSTAND_MEDIA,
                STEP_CURRENCY_RATES,
                STEP_WEB_SEARCH,
                STEP_CRAWL_URL,
                STEP_HISTORY_SEARCH,
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
    fn currency_rates_tool_accepts_optional_pairs() -> Result<(), ToolParseError> {
        let tools = chat_completion_tools_for_names(&[STEP_CURRENCY_RATES]);
        let rates = tools.first().expect("rates tool");
        assert!(rates.function.parameters["properties"]["pairs"].is_object());
        assert!(rates.function.parameters.get("required").is_none());

        let (step, decision) =
            extract_content_tool_step(r#"<tool_call>currency_rates{pairs:"btc eth"}</tool_call>"#)?;
        let step = step.expect("rates tool step");
        assert_eq!(decision.outcome, "detected");
        assert_eq!(step.step, STEP_CURRENCY_RATES);
        assert_eq!(step.pairs, "btc eth");

        let error = extract_content_tool_step(r#"<currency_rates pairs="final_response" />"#)
            .expect_err("pairs sentinel rejected");
        assert!(error.to_string().contains("protocol sentinel"));
        Ok(())
    }

    #[test]
    fn tool_schema_args_are_recognized_by_inline_and_xmlish_arg_scanners() {
        let mut missing = Vec::new();
        for spec in alternative_dialog_tools()
            .into_iter()
            .chain([SESSION_SEND_MESSAGE_SPEC, SESSION_REACT_TO_MESSAGE_SPEC])
        {
            for arg in spec.args {
                if !INLINE_TOOL_ARG_KEYS.contains(&arg.name) {
                    missing.push(format!("{}.{}", spec.name, arg.name));
                }
            }
        }

        assert!(
            missing.is_empty(),
            "tool schema args missing from inline/xmlish scanner keys: {missing:?}"
        );
    }

    #[test]
    fn tool_argument_decoders_keep_json_xmlish_and_native_shapes_in_parity()
    -> Result<(), ToolParseError> {
        for raw in [
            r#"<currency_rates pairs="btc eth" />"#,
            r#"{"tool":"currency_rates","pairs":"btc eth"}"#,
            r#"{"tool":"currency_rates","arguments":{"pairs":"btc eth"}}"#,
        ] {
            let (step, decision) = extract_content_tool_step(raw)?;
            let step = step.expect("currency_rates tool step");
            assert_eq!(decision.outcome, "detected");
            assert_eq!(step.step, STEP_CURRENCY_RATES);
            assert_eq!(step.pairs, "btc eth", "raw={raw}");
        }

        let native_rates = parse_native_tool_step(&[NativeToolCall {
            id: "call-rates".to_owned(),
            call_type: "function".to_owned(),
            function: NativeToolFunction {
                name: STEP_CURRENCY_RATES.to_owned(),
                arguments: json!({"pairs": "btc eth"}),
            },
        }])?;
        assert_eq!(native_rates.pairs, "btc eth");

        let (summary, _) = extract_content_tool_step(
            r#"{"tool":"chat_history_summary","arguments":{"window":"hours","hours":"6","message_count":"120"}}"#,
        )?;
        let summary = summary.expect("summary tool step");
        assert_eq!(summary.step, STEP_CHAT_HISTORY_SUMMARY);
        assert_eq!(summary.window, "hours");
        assert_eq!(summary.hours, 6);
        assert_eq!(summary.message_count, 120);

        let native_summary = parse_native_tool_step(&[NativeToolCall {
            id: "call-summary".to_owned(),
            call_type: "function".to_owned(),
            function: NativeToolFunction {
                name: STEP_CHAT_HISTORY_SUMMARY.to_owned(),
                arguments: json!({
                    "window": "hours",
                    "hours": "6",
                    "message_count": "120"
                }),
            },
        }])?;
        assert_eq!(native_summary.window, "hours");
        assert_eq!(native_summary.hours, 6);
        assert_eq!(native_summary.message_count, 120);
        Ok(())
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
            sanitize_final_text(
                r#"<assistants_message><text>Ответ из production-варианта.</text></assistants_message>"#
            ),
            "Ответ из production-варианта."
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
                "Сначала текст.\n\nunderstand_media{file_id:\"message_177154_image_1\"}".to_owned(),
                ToolStep {
                    step: STEP_UNDERSTAND_MEDIA.to_owned(),
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
                r#"<tool call="understand_media{file_id: 'message_11989713_image_1'}"/>"#.to_owned(),
                ToolStep {
                    step: STEP_UNDERSTAND_MEDIA.to_owned(),
                    file_id: "message_11989713_image_1".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<tool:understand_media{file_id:"message_177164_image_1"}>"#.to_owned(),
                ToolStep {
                    step: STEP_UNDERSTAND_MEDIA.to_owned(),
                    file_id: "message_177164_image_1".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<draw_image prompt="cat in space" negative_prompt="blur" />"#.to_owned(),
                ToolStep {
                    step: STEP_DRAW_IMAGE.to_owned(),
                    prompt: "cat in space".to_owned(),
                    negative_prompt: "blur".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<send_message text="Working on it." />"#.to_owned(),
                ToolStep {
                    step: STEP_SEND_MESSAGE.to_owned(),
                    text: "Working on it.".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                r#"<react_to_message chat_id="-1001680667629" emoji="🤣" message_id="316691" />"#.to_owned(),
                ToolStep {
                    step: STEP_REACT_TO_MESSAGE.to_owned(),
                    emoji: "🤣".to_owned(),
                    target_chat_id: -1001680667629,
                    target_message_id: 316691,
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
            (
                "<tool_calls>\n  <tool_call name=\"history_search\" arg=\"query: CherryCherry123\"></tool_call>\n</tool_calls>".to_owned(),
                ToolStep {
                    step: STEP_HISTORY_SEARCH.to_owned(),
                    query: "CherryCherry123".to_owned(),
                    ..ToolStep::default()
                },
                "xmlish",
            ),
            (
                "<tool_call name=\"history_search\" args='{\"query\":\"CherryCherry123\"}' />".to_owned(),
                ToolStep {
                    step: STEP_HISTORY_SEARCH.to_owned(),
                    query: "CherryCherry123".to_owned(),
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
    fn content_parser_preserves_nested_preamble_then_video_tool_order() -> Result<(), ToolParseError>
    {
        let raw = "<send_message><text>Так, сейчас гляну, что там за видео</text></send_message><understand_media><file_id>message_1110901_video_1</file_id></understand_media>";

        let (steps, decision) = extract_content_tool_steps(raw)?;

        assert_eq!(decision.outcome, "detected");
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].step, STEP_SEND_MESSAGE);
        assert_eq!(steps[0].text, "Так, сейчас гляну, что там за видео");
        assert_eq!(steps[1].step, STEP_UNDERSTAND_MEDIA);
        assert_eq!(steps[1].file_id, "message_1110901_video_1");
        Ok(())
    }

    #[test]
    fn typed_content_parser_removes_only_recognized_tool_spans() -> Result<(), ToolParseError> {
        let bare = parse_assistant_content(
            "Сейчас проверю.\n\nunderstand_media{file_id:\"message_177154_image_1\"}",
        )?;
        assert_eq!(bare.text, "Сейчас проверю.");
        assert_eq!(bare.tool_steps.len(), 1);
        assert!(!bare.residual_protocol);

        let xml = parse_assistant_content(
            "<b>Проверяю</b><send_message><text>Уже смотрю</text></send_message><understand_media><file_id>message_1110901_video_1</file_id></understand_media>",
        )?;
        assert_eq!(xml.text, "<b>Проверяю</b>");
        assert_eq!(xml.tool_steps.len(), 2);
        assert!(!xml.residual_protocol);

        let wrapper = parse_assistant_content(
            "Секунду.<tool call=\"understand_media{file_id: 'message_1_video_1'}\"/>",
        )?;
        assert_eq!(wrapper.text, "Секунду.");
        assert_eq!(wrapper.tool_steps.len(), 1);
        assert!(!wrapper.residual_protocol);
        Ok(())
    }

    #[test]
    fn typed_content_parser_recovers_production_named_calls() -> Result<(), ToolParseError> {
        let raw = r#"<|channel>thought
<channel|><call>
  <tool_name>react_to_message</tool_name>
  <arguments>
    <emoji>🤣</emoji>
    <message_id>999948</message_id>
  </arguments>
</call>
<call>
  <tool_name>react_to_message</tool_name>
  <arguments>
    <emoji>😂</emoji>
    <message_id>999949</message_id>
  </arguments>
</call>

Ну и за что тебе такое наказание божье?"#;

        let parsed = parse_assistant_content(raw)?;

        assert_eq!(parsed.text, "Ну и за что тебе такое наказание божье?");
        assert_eq!(parsed.tool_steps.len(), 2);
        assert_eq!(parsed.tool_steps[0].step, STEP_REACT_TO_MESSAGE);
        assert_eq!(parsed.tool_steps[0].emoji, "🤣");
        assert_eq!(parsed.tool_steps[0].target_message_id, 999948);
        assert_eq!(parsed.tool_steps[1].step, STEP_REACT_TO_MESSAGE);
        assert_eq!(parsed.tool_steps[1].emoji, "😂");
        assert_eq!(parsed.tool_steps[1].target_message_id, 999949);
        assert!(!parsed.residual_protocol);
        Ok(())
    }

    #[test]
    fn named_call_protocol_is_leading_only_and_malformed_calls_fail_loud()
    -> Result<(), ToolParseError> {
        for raw in [
            "Это XML-пример: <call><tool_name>queue_status</tool_name></call>.",
            "```xml\n<call><tool_name>queue_status</tool_name></call>\n```",
        ] {
            let parsed = parse_assistant_content(raw)?;
            assert!(parsed.tool_steps.is_empty());
            assert_eq!(parsed.text, raw);
        }

        let mixed = parse_assistant_content(
            "<call><tool_name>queue_status</tool_name></call>\n\nПример остаётся: <call><tool_name>queue_status</tool_name></call>.",
        )?;
        assert_eq!(mixed.tool_steps.len(), 1);
        assert_eq!(mixed.tool_steps[0].step, STEP_QUEUE_STATUS);
        assert_eq!(
            mixed.text,
            "Пример остаётся: <call><tool_name>queue_status</tool_name></call>."
        );

        let error = parse_assistant_content(
            "<call><tool_name>react_to_message</tool_name><arguments><emoji>😂</emoji>",
        )
        .expect_err("unterminated leading call must be a provider protocol error");
        assert!(error.to_string().contains("unterminated XML-ish call"));
        Ok(())
    }

    #[test]
    fn legacy_vision_image_call_canonicalizes_to_understand_media() -> Result<(), ToolParseError> {
        let (step, decision) = extract_content_tool_step(
            "<vision_image><file_id>message_1110901_video_1</file_id></vision_image>",
        )?;

        assert_eq!(decision.outcome, "detected");
        assert_eq!(step.expect("legacy call").step, STEP_UNDERSTAND_MEDIA);
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
    fn content_parser_decodes_entity_encoded_tool_call() -> Result<(), ToolParseError> {
        let (step, decision) = extract_content_tool_step(
            r#"&lt;tool_calls&gt;&lt;tool_call&gt;web_search{query: "rust async runtime"}&lt;/tool_call&gt;&lt;/tool_calls&gt;"#,
        )?;
        assert_eq!(
            step,
            Some(ToolStep {
                step: STEP_WEB_SEARCH.to_owned(),
                query: "rust async runtime".to_owned(),
                ..ToolStep::default()
            })
        );
        assert_eq!(decision.outcome, "detected");
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
