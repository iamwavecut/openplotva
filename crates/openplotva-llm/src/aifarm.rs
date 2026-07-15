//! AIFarm/OpenAI-compatible dialog message formatting.

use std::error::Error;
use std::fmt::Write as _;
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

use openplotva_core::{ChatAttachment, ChatMessageMeta};
use openplotva_dialog::{
    ChatStepOutput, ChatStepRequest, ChatStepToolCall, DEFAULT_CONTEXT_HISTORY_LIMIT, DialogInput,
    DialogTraceArtifacts, DialogTraceError, DialogTraceUsage, HistoryMessage, MESSAGE_KIND_TEXT,
    MESSAGE_KIND_TOOL_REQUEST, MESSAGE_KIND_TOOL_RESPONSE, NativeToolCall, PROVIDER_AIFARM,
    PROVIDER_NVIDIA, PROVIDER_VMLX, ROLE_MODEL, ROLE_USER, STEP_SEND_MESSAGE, SessionMessage,
    ToolParseDecision, ToolSpec, ToolStep, ToolsMode, alternative_dialog_tool_names,
    alternative_dialog_tools, clone_history_messages, decode_plotva_final_response_with_salvage,
    is_dialog_history_noise_tool_call_name, normalize_history_message, parse_assistant_content,
    parse_native_tool_step, sanitize_tool_text, select_llm_history_messages_for_context,
    tool_telemetry,
};
use openplotva_history::{
    AIFARM_DEFAULT_HISTORY_SUMMARY_MODEL, HistorySummaryDecodeError, SummaryDocument, SummaryInput,
    decode_history_summary_response, hash_text, history_output_token_estimate,
};
use openplotva_memory::{
    DEFAULT_MEMORY_MAX_OUTPUT_TOKENS, ExtractInput, ExtractOutput, MemoryExtractor,
    MemoryExtractorFuture, decode_extraction_json, estimate_memory_tokens,
};

use crate::retry::{FailureReason, ProviderError, retryable_reason};

const DEFAULT_TIMEOUT_MS: i32 = 600_000;
const DEFAULT_AIFARM_BASE_URL: &str = "http://127.0.0.1:50051";
const DEFAULT_SERVICE_NAME: &str = "llm-openai";
const DEFAULT_ENDPOINT_NAME: &str = "chat_completions";
const DEFAULT_MODEL_NAME: &str = "Gemma 4 26B Heretic";
const DEFAULT_HISTORY_SUMMARY_MAX_OUTPUT_TOKENS: i32 = 1024;
const DEFAULT_VRAM_CLOUD_TEMPERATURE: f64 = 0.7;
const DEFAULT_VRAM_CLOUD_TOP_P: f64 = 0.8;
const DEFAULT_VRAM_CLOUD_TOP_K: f64 = 20.0;
const DEFAULT_VRAM_CLOUD_PRESENCE_PENALTY: f64 = 1.5;
const DEFAULT_VRAM_CLOUD_REPETITION_PENALTY: f64 = 1.0;
const DEFAULT_VRAM_CLOUD_MAX_TOKENS: i32 = 768;

pub const DISCOVERY_PRIORITY_INTERACTIVE: i32 = 0;
pub const DISCOVERY_PRIORITY_MEMORY: i32 = 10;

pub const AIFARM_WORKLOAD_DIALOG: &str = "dialog";
pub const AIFARM_WORKLOAD_STRUCTURED: &str = "structured";
pub const AIFARM_WORKLOAD_SUMMARY: &str = "summary";
pub const AIFARM_WORKLOAD_MEMORY: &str = "memory";

/// AIFarm formatter error.
#[derive(Debug, Error)]
pub enum AifarmMessageError {
    /// Prompt rendering failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// JSON formatting failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// AIFarm chat message.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct ChatMessage {
    /// Chat role.
    pub role: String,
    /// Plain string content.
    pub content: String,
    /// Multimodal content parts. When non-empty these replace `content` at serialization time.
    #[serde(default, skip_serializing, skip_deserializing)]
    pub content_parts: Vec<ChatContentPart>,
    /// Native tool calls of an in-session assistant message (pre-encoded
    /// OpenAI `tool_calls` array).
    #[serde(default)]
    pub tool_calls: Option<Value>,
    /// Tool-call id linking a `role: "tool"` result message to its call.
    #[serde(default)]
    pub tool_call_id: Option<String>,
    /// Tool name of a `role: "tool"` result message.
    #[serde(default)]
    pub name: Option<String>,
}

impl Serialize for ChatMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let extra_fields = usize::from(self.tool_calls.is_some())
            + usize::from(self.tool_call_id.is_some())
            + usize::from(self.name.is_some());
        let mut state = serializer.serialize_struct("ChatMessage", 2 + extra_fields)?;
        state.serialize_field("role", &self.role)?;
        if self.content_parts.is_empty() {
            state.serialize_field("content", &self.content)?;
        } else {
            state.serialize_field("content", &self.content_parts)?;
        }
        if let Some(tool_calls) = &self.tool_calls {
            state.serialize_field("tool_calls", tool_calls)?;
        }
        if let Some(tool_call_id) = &self.tool_call_id {
            state.serialize_field("tool_call_id", tool_call_id)?;
        }
        if let Some(name) = &self.name {
            state.serialize_field("name", name)?;
        }
        state.end()
    }
}

/// AIFarm multimodal content part.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChatContentPart {
    /// Part type.
    #[serde(rename = "type")]
    pub part_type: String,
    /// Text content.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    /// Image URL part.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_url: Option<ChatImageUrlPart>,
    /// Video URL part.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video_url: Option<ChatVideoUrlPart>,
}

/// AIFarm image URL payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChatImageUrlPart {
    /// Data URL or HTTP URL.
    pub url: String,
    /// Image detail mode.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub detail: String,
}

/// OpenAI-compatible video URL payload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChatVideoUrlPart {
    /// Data URL or HTTP URL.
    pub url: String,
}

/// OpenAI-compatible chat completion request subset used by AIFarm routing.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChatCompletionRequest {
    /// Model name.
    #[serde(default)]
    pub model: String,
    /// Chat messages.
    #[serde(default)]
    pub messages: Vec<ChatMessage>,
    /// Streaming flag.
    #[serde(default)]
    pub stream: bool,
    /// OpenAI response format object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_format: Option<Value>,
    /// Tool definitions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Value>,
    /// Tool-choice object/string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    /// Parallel tool calls flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,
    /// Max token limit.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub max_tokens: i32,
    /// Temperature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    /// Top-p.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    /// Top-k.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<f64>,
    /// llama.cpp repeat penalty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_penalty: Option<f64>,
    /// OpenAI-compatible repetition penalty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repetition_penalty: Option<f64>,
    /// Frequency penalty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f64>,
    /// Presence penalty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f64>,
    /// DRY multiplier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_multiplier: Option<f64>,
    /// DRY base.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dry_base: Option<f64>,
    /// DRY allowed length.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub dry_allowed_length: i32,
    /// Include reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_reasoning: Option<bool>,
    /// Chat template kwargs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat_template_kwargs: Option<Value>,
    /// Literal `extra_body` passthrough. OpenAI SDK clients flatten
    /// `extra_body` before sending, but closedrouter-style gateways
    /// (vram.cloud) strip unknown top-level keys and instead unwrap this key
    /// server-side — mirroring `chat_template_kwargs` here is the only way
    /// thinking control reaches their template engine. Plain llama.cpp and
    /// SGLang backends ignore the unknown key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra_body: Option<Value>,
    /// Trace metadata for low-level call observation. Never serialized to the wire.
    #[serde(skip)]
    pub trace: Option<crate::trace::LlmCallTrace>,
}

impl ChatCompletionRequest {
    /// Set `chat_template_kwargs` and mirror them into `extra_body` so the
    /// setting survives gateways that drop unknown top-level fields.
    pub fn set_chat_template_kwargs(&mut self, kwargs: Value) {
        self.extra_body = Some(json!({ "chat_template_kwargs": kwargs.clone() }));
        self.chat_template_kwargs = Some(kwargs);
    }
}

/// AIFarm completion error.
pub type CompletionError = Box<dyn Error + Send + Sync + 'static>;

/// AIFarm completion result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompletionResult {
    /// AIFarm or direct job ID.
    pub job_id: String,
    /// Upstream HTTP status code.
    pub status_code: u16,
    /// Raw response body.
    pub raw_body: String,
    /// Decoded OpenAI-compatible response body.
    pub response: Option<Value>,
}

/// AIFarm Discovery response decoding error.
#[derive(Debug, Error)]
pub enum AifarmDecodeError {
    /// Discovery response body was not valid base64.
    #[error("decode dialog response body: {0}")]
    DiscoveryBody(#[from] base64::DecodeError),
    /// Decoded chat completion payload was not JSON.
    #[error("decode chat completion payload: {0}")]
    ChatCompletionPayload(serde_json::Error),
}

/// AIFarm request construction error.
#[derive(Debug, Error)]
pub enum AifarmRequestError {
    /// Chat completion request serialization failed.
    #[error("marshal chat completion request: {0}")]
    ChatCompletionRequest(serde_json::Error),
    /// Direct chat completion request serialization failed.
    #[error("marshal direct chat completion request: {0}")]
    DirectChatCompletionRequest(serde_json::Error),
    /// Accepted direct request did not include a request ID.
    #[error("direct dialog request accepted but request ID is missing")]
    DirectRequestIdMissing,
}

/// AIFarm HTTP client error.
#[derive(Debug, Error)]
pub enum AifarmClientError {
    /// Request build failed.
    #[error(transparent)]
    Request(#[from] AifarmRequestError),
    /// Decode failed.
    #[error(transparent)]
    Decode(#[from] AifarmDecodeError),
    /// HTTP transport failed.
    #[error("{0}")]
    Transport(String),
    /// Submit failed.
    #[error("submit dialog job: {0}")]
    Submit(String),
    /// Status check failed.
    #[error("check dialog job status: {0}")]
    CheckStatus(String),
    /// Direct submit failed.
    #[error("submit direct dialog request: {0}")]
    SubmitDirect(String),
    /// Direct status check failed.
    #[error("poll direct dialog request {request_id}: {message}")]
    PollDirect {
        /// Request ID.
        request_id: String,
        /// Message.
        message: String,
    },
    /// Empty direct response body.
    #[error("{0}")]
    EmptyDirectResponse(String),
    /// Upstream non-2xx discovery result.
    #[error("{0}")]
    Upstream(String),
    /// Unknown Discovery status.
    #[error("unknown dialog job status {0:?}")]
    UnknownStatus(String),
}

/// AIFarm dialog-service error.
#[derive(Debug, Error)]
pub enum AifarmDialogError {
    /// Message formatting failed.
    #[error(transparent)]
    Message(#[from] AifarmMessageError),
    /// Upstream response did not contain a usable assistant message.
    #[error("{0}")]
    Response(String),
    /// Upstream final answer copied prompt context only.
    #[error("chat completion returned only copied context messages")]
    FinalAnswerContextLeak,
    /// Upstream final answer contained protocol artifacts only.
    #[error("chat completion returned only protocol artifacts")]
    FinalAnswerProtocolOnly,
    /// Upstream final answer looked pathological.
    #[error("chat completion returned pathological final text: {0}")]
    FinalAnswerPathological(String),
    /// Reasoning model spent the whole token budget thinking and produced no
    /// final content (`finish_reason="length"` with only `reasoning_content`).
    #[error(
        "chat completion returned reasoning without final content ({reasoning_chars} reasoning chars)"
    )]
    ReasoningBudgetExhausted { reasoning_chars: usize },
    /// Native tool definitions failed to serialize.
    #[error("encode dialog tool definition: {0}")]
    ToolDefinition(serde_json::Error),
}

/// AIFarm history-summary generation error.
#[derive(Debug, Error)]
pub enum AifarmHistorySummaryError {
    /// Prompt rendering or reading failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// Input marshaling failed.
    #[error("marshal history summary input: {0}")]
    Input(serde_json::Error),
    /// Completion failed.
    #[error("generate chat history summary: {source}")]
    Completion {
        /// Concrete completion error.
        #[source]
        source: CompletionError,
    },
    /// Response did not contain usable assistant content.
    #[error("{0}")]
    Response(String),
    /// Summary JSON decode failed.
    #[error("decode chat history summary: {0}")]
    Decode(#[from] HistorySummaryDecodeError),
}

/// AIFarm memory extraction error.
#[derive(Debug, Error)]
pub enum AifarmMemoryExtractorError {
    /// Prompt rendering or reading failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// Input marshaling failed.
    #[error("marshal memory input: {0}")]
    Input(serde_json::Error),
    /// Completion failed.
    #[error("generate memory extraction: {source}")]
    Completion {
        /// Concrete completion error.
        #[source]
        source: CompletionError,
    },
    /// Response did not contain usable assistant content.
    #[error("{0}")]
    Response(String),
    /// Memory JSON decode failed.
    #[error(transparent)]
    Decode(#[from] openplotva_memory::DecodeExtractionError),
}

#[derive(Debug, Error)]
pub enum GenkitOpenAiCompatibleMemoryExtractorError {
    /// Prompt rendering or reading failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// Input marshaling failed.
    #[error("marshal memory input: {0}")]
    Input(serde_json::Error),
    /// Completion failed.
    #[error("generate memory extraction: {source}")]
    Completion {
        /// Concrete completion error.
        #[source]
        source: CompletionError,
    },
    /// Response did not contain usable assistant content.
    #[error("{0}")]
    Response(String),
    /// Memory JSON decode failed.
    #[error(transparent)]
    Decode(#[from] openplotva_memory::DecodeExtractionError),
}

#[derive(Debug, Error)]
pub enum GenkitOpenAiCompatibleHistorySummaryError {
    /// Prompt rendering or reading failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// Input marshaling failed.
    #[error("marshal history summary input: {0}")]
    Input(serde_json::Error),
    /// Completion failed.
    #[error("generate chat history summary: {source}")]
    Completion {
        /// Concrete completion error.
        #[source]
        source: CompletionError,
    },
    /// Response did not contain usable assistant content.
    #[error("{0}")]
    Response(String),
    /// Summary JSON decode failed.
    #[error("decode chat history summary: {0}")]
    Decode(#[from] HistorySummaryDecodeError),
}

/// AIFarm structured JSON generator error.
#[derive(Debug, Error)]
pub enum AifarmStructuredJsonError {
    /// Completion failed.
    #[error("complete structured JSON request")]
    Completion {
        /// Concrete completion error.
        #[source]
        source: CompletionError,
    },
    /// Response did not contain usable assistant content.
    #[error("structured JSON response: {0}")]
    Response(String),
}

/// AIFarm media optimizer error.
#[derive(Debug, Error)]
pub enum AifarmMediaOptimizerError {
    /// Prompt rendering failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// Input text is empty.
    #[error("empty text")]
    EmptyText,
    /// Structured JSON generation failed.
    #[error(transparent)]
    Structured(#[from] AifarmStructuredJsonError),
    /// Optimizer payload failed to decode.
    #[error("decode {label} payload: {source}")]
    Decode {
        label: &'static str,
        /// JSON decode source.
        #[source]
        source: serde_json::Error,
    },
    /// Song reprompt payload failed validation.
    #[error("song reprompt: {source}")]
    SongPrompt {
        /// Concrete song-prompt validation error.
        #[source]
        source: openplotva_media::acestep::AceStepError,
    },
}

/// AIFarm status update.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StatusUpdate {
    /// Job ID.
    pub job_id: String,
    /// Normalized status.
    pub status: String,
    /// Human status message.
    pub message: String,
    /// Upstream HTTP status.
    pub http_status: u16,
    /// Backend label.
    pub backend: String,
    /// Model label.
    pub model: String,
}

/// AIFarm HTTP method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AifarmHttpMethod {
    /// GET.
    Get,
    /// POST.
    Post,
}

/// AIFarm HTTP request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AifarmHttpRequest {
    /// Method.
    pub method: AifarmHttpMethod,
    /// URL.
    pub url: String,
    /// Headers.
    pub headers: BTreeMap<String, String>,
    /// Body.
    pub body: Vec<u8>,
}

/// AIFarm HTTP response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AifarmHttpResponse {
    /// HTTP status code.
    pub status_code: u16,
    /// HTTP status text.
    pub status_text: String,
    /// Headers.
    pub headers: BTreeMap<String, String>,
    /// Body bytes.
    pub body: Vec<u8>,
}

/// Boxed AIFarm transport future.
pub type AifarmHttpFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AifarmHttpResponse, CompletionError>> + Send + 'a>>;

/// Minimal HTTP transport for AIFarm clients.
pub trait AifarmHttpTransport: Send + Sync {
    /// Send request.
    fn send<'a>(&'a self, request: AifarmHttpRequest) -> AifarmHttpFuture<'a>;
}

/// Reqwest-backed AIFarm transport.
#[derive(Clone, Debug)]
pub struct ReqwestAifarmTransport {
    client: reqwest::Client,
}

impl ReqwestAifarmTransport {
    /// Build transport.
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestAifarmTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl AifarmHttpTransport for ReqwestAifarmTransport {
    fn send<'a>(&'a self, request: AifarmHttpRequest) -> AifarmHttpFuture<'a> {
        Box::pin(async move {
            let method = match request.method {
                AifarmHttpMethod::Get => reqwest::Method::GET,
                AifarmHttpMethod::Post => reqwest::Method::POST,
            };
            let mut builder = self.client.request(method, &request.url);
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            if !request.body.is_empty() {
                builder = builder.body(request.body);
            }
            let response = builder.send().await.map_err(|err| {
                Box::new(AifarmClientError::Transport(err.to_string())) as CompletionError
            })?;
            let status = response.status();
            let mut headers = BTreeMap::new();
            for (name, value) in response.headers() {
                if let Ok(value) = value.to_str() {
                    headers.insert(name.as_str().to_owned(), value.to_owned());
                }
            }
            let body = response
                .bytes()
                .await
                .map_err(|err| {
                    Box::new(AifarmClientError::Transport(err.to_string())) as CompletionError
                })?
                .to_vec();
            Ok(AifarmHttpResponse {
                status_code: status.as_u16(),
                status_text: status.canonical_reason().unwrap_or_default().to_owned(),
                headers,
                body,
            })
        })
    }
}

/// HTTP-backed AIFarm client.
#[derive(Clone, Debug)]
pub struct AifarmHttpClient<T = ReqwestAifarmTransport> {
    cfg: AifarmClientConfig,
    transport: T,
    trace_registry: Arc<crate::trace::LlmCallTraceRegistry>,
}

impl AifarmHttpClient<ReqwestAifarmTransport> {
    #[must_use]
    pub fn new(cfg: AifarmClientConfig) -> Self {
        let cfg = cfg.with_defaults();
        Self {
            cfg,
            transport: ReqwestAifarmTransport::default(),
            trace_registry: crate::trace::global_registry(),
        }
    }
}

impl<T> AifarmHttpClient<T>
where
    T: AifarmHttpTransport,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmClientConfig, transport: T) -> Self {
        Self {
            cfg: cfg.with_defaults(),
            transport,
            trace_registry: crate::trace::global_registry(),
        }
    }

    /// Override the trace registry (production uses the global one by default; tests
    /// inject an isolated registry).
    #[must_use]
    pub fn with_trace_registry(
        mut self,
        trace_registry: Arc<crate::trace::LlmCallTraceRegistry>,
    ) -> Self {
        self.trace_registry = trace_registry;
        self
    }

    /// Cap for one fast HTTP round-trip (direct completions, submits, status
    /// polls). Without it a hung connection pinned a dialog worker for minutes
    /// while the configured `request_timeout` sat unused.
    fn request_limit(&self) -> StdDuration {
        nonzero_duration(self.cfg.request_timeout, StdDuration::from_secs(30))
    }

    /// Cap for the blocking Discovery submit and for result polling: the farm
    /// kills the job at `task_timeout` (sent as `timeout_ms`), so a connection
    /// alive past that plus a transfer margin is dead, not slow.
    fn task_limit(&self) -> StdDuration {
        let task = nonzero_duration(self.cfg.task_timeout, StdDuration::from_secs(12 * 60));
        task.saturating_add(task.min(StdDuration::from_secs(30)))
    }

    /// Total local budget for waiting out Discovery capacity, mirroring the
    /// `wait_for_capacity_ms` the farm receives; previously this loop retried
    /// forever at one-second intervals.
    fn capacity_wait_limit(&self) -> StdDuration {
        nonzero_duration(self.cfg.capacity_wait, StdDuration::from_secs(60))
    }

    async fn send_bounded(
        &self,
        request: AifarmHttpRequest,
        limit: StdDuration,
        what: &str,
    ) -> Result<AifarmHttpResponse, CompletionError> {
        match tokio::time::timeout(limit, self.transport.send(request)).await {
            Ok(result) => result,
            Err(_) => Err(Box::new(AifarmClientError::Transport(format!(
                "{what} timed out after {}s",
                limit.as_secs()
            ))) as CompletionError),
        }
    }

    /// Complete using direct endpoint when configured, otherwise Discovery.
    pub async fn complete(
        &self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let request = request_with_default_model(&request, &self.cfg.default_model);
        let started = std::time::Instant::now();
        let result = if self.uses_direct_endpoint() {
            self.complete_direct_with_job_id(request.clone(), &generated_dialog_job_id(), on_status)
                .await
        } else {
            self.complete_discovery_with_job_id(
                request.clone(),
                &generated_dialog_job_id(),
                on_status,
            )
            .await
        };
        self.emit_call_trace(&request, &result, started.elapsed());
        result
    }

    /// Emit one trace record per model round-trip when the request carries trace
    /// metadata. Fires on success and error.
    fn emit_call_trace(
        &self,
        request: &ChatCompletionRequest,
        result: &Result<CompletionResult, CompletionError>,
        elapsed: StdDuration,
    ) {
        let Some(trace) = request.trace.as_ref() else {
            return;
        };
        let tags = TraceTags {
            provider: &trace.tags.provider,
            source: &trace.tags.source,
            flow: &trace.tags.flow,
            mode: &trace.tags.mode,
            request_kind: &trace.tags.request_kind,
            iteration: trace.tags.iteration,
        };
        let mut artifact = match result {
            Ok(completion) => {
                let mut artifact =
                    aifarm_call_trace_artifacts(request, completion, trace.tags.docs_chars, tags);
                if trace.tags.flow == "dialog"
                    && let Some(error) = dialog_response_semantic_error(
                        completion.response.as_ref(),
                        &trace.tags.provider,
                        !request.tools.is_empty(),
                    )
                {
                    artifact.error = error;
                }
                artifact
            }
            Err(error) => {
                let mut artifact = aifarm_call_trace_artifacts(
                    request,
                    &CompletionResult::default(),
                    trace.tags.docs_chars,
                    tags,
                );
                artifact.error = error.to_string();
                artifact
            }
        };
        if artifact.model.trim().is_empty() {
            artifact.model = request.model.trim().to_owned();
        }
        let duration_ms = i32::try_from(elapsed.as_millis()).unwrap_or(i32::MAX);
        self.trace_registry.observe(crate::trace::LlmCallRecord {
            context: trace.context.clone(),
            artifact,
            duration_ms,
            run: None,
        });
    }

    pub async fn complete_discovery_with_job_id(
        &self,
        request: ChatCompletionRequest,
        job_id: &str,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let (job_id, initial_status) = self
            .submit_with_capacity_wait(&request, job_id, on_status)
            .await?;
        emit_status(
            on_status,
            StatusUpdate {
                job_id: job_id.clone(),
                status: STATUS_SUBMITTED.to_owned(),
                message: "dialog job submitted".to_owned(),
                ..StatusUpdate::default()
            },
        );
        if !initial_status.status.is_empty() {
            emit_status(
                on_status,
                StatusUpdate {
                    job_id: job_id.clone(),
                    status: initial_status.status.clone(),
                    message: initial_status.message.clone(),
                    ..StatusUpdate::default()
                },
            );
        }
        self.poll_discovery_result(&job_id, &initial_status, on_status)
            .await
    }

    /// Complete a raw JSON request through Discovery.
    pub async fn complete_json_discovery(
        &self,
        request: Value,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        self.complete_json_discovery_with_job_id(request, &generated_dialog_job_id(), on_status)
            .await
    }

    /// Complete a raw JSON request through Discovery with supplied job ID.
    pub async fn complete_json_discovery_with_job_id(
        &self,
        request: Value,
        job_id: &str,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let (job_id, initial_status) = self
            .submit_json_with_capacity_wait(&request, job_id, on_status)
            .await?;
        emit_status(
            on_status,
            StatusUpdate {
                job_id: job_id.clone(),
                status: STATUS_SUBMITTED.to_owned(),
                message: "dialog job submitted".to_owned(),
                ..StatusUpdate::default()
            },
        );
        if !initial_status.status.is_empty() {
            emit_status(
                on_status,
                StatusUpdate {
                    job_id: job_id.clone(),
                    status: initial_status.status.clone(),
                    message: initial_status.message.clone(),
                    ..StatusUpdate::default()
                },
            );
        }
        self.poll_discovery_result(&job_id, &initial_status, on_status)
            .await
    }

    /// Complete through a direct OpenAI-compatible endpoint with supplied job ID.
    pub async fn complete_direct_with_job_id(
        &self,
        request: ChatCompletionRequest,
        job_id: &str,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let body = direct_chat_completion_body(&request)
            .map_err(|err| Box::new(err) as CompletionError)?;
        emit_status(
            on_status,
            StatusUpdate {
                job_id: job_id.to_owned(),
                status: STATUS_SUBMITTED.to_owned(),
                message: "dialog request submitted".to_owned(),
                ..StatusUpdate::default()
            },
        );
        // For synchronous OpenAI-compatible endpoints this POST *is* the
        // generation call and legitimately runs for minutes, so it gets the
        // task budget, not the fast-round-trip budget; callers with a stricter
        // wall clock (the dialog turn) cut the whole attempt at their deadline.
        let response = self
            .send_bounded(
                AifarmHttpRequest {
                    method: AifarmHttpMethod::Post,
                    url: self.cfg.direct_url.clone(),
                    headers: direct_completion_headers(&self.cfg, &request),
                    body,
                },
                self.task_limit(),
                "direct dialog request",
            )
            .await?;
        let raw_body = response_body_text(&response);
        if response.status_code == 202 {
            let request_id = resolve_direct_request_id(&response.headers, &raw_body)
                .map_err(|err| Box::new(err) as CompletionError)?;
            emit_status(
                on_status,
                StatusUpdate {
                    job_id: request_id.clone(),
                    status: STATUS_QUEUED.to_owned(),
                    message: "dialog request accepted, polling status".to_owned(),
                    http_status: response.status_code,
                    ..StatusUpdate::default()
                },
            );
            return self.poll_direct_result(&request_id, on_status).await;
        }
        direct_http_status_error("direct dialog request", &response, &raw_body)?;
        let decoded = decode_direct_completion_payload(
            &response.body,
            &raw_body,
            "direct dialog request succeeded but response body is empty",
            "decode direct chat completion payload",
        )?;
        emit_status(
            on_status,
            StatusUpdate {
                job_id: job_id.to_owned(),
                status: STATUS_SUCCEEDED.to_owned(),
                message: "dialog request completed".to_owned(),
                http_status: response.status_code,
                ..StatusUpdate::default()
            },
        );
        Ok(CompletionResult {
            job_id: job_id.to_owned(),
            status_code: response.status_code,
            raw_body,
            response: Some(decoded),
        })
    }

    fn uses_direct_endpoint(&self) -> bool {
        !self.cfg.direct_url.trim().is_empty()
    }

    async fn submit_with_capacity_wait(
        &self,
        request: &ChatCompletionRequest,
        job_id: &str,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<(String, StatusUpdate), CompletionError> {
        let wait_deadline = tokio::time::Instant::now() + self.capacity_wait_limit();
        loop {
            match self.submit_discovery(request, job_id).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retryable_reason(err.as_ref()) != Some(FailureReason::CapacityUnavailable) {
                        return Err(err);
                    }
                    if self.cfg.fail_fast_on_capacity_unavailable
                        || tokio::time::Instant::now() >= wait_deadline
                    {
                        return Err(err);
                    }
                    emit_status(
                        on_status,
                        StatusUpdate {
                            status: STATUS_QUEUED.to_owned(),
                            message: "waiting for Discovery service capacity".to_owned(),
                            ..StatusUpdate::default()
                        },
                    );
                    tokio::time::sleep(nonzero_duration(
                        self.cfg.capacity_poll_interval,
                        StdDuration::from_secs(1),
                    ))
                    .await;
                }
            }
        }
    }

    async fn submit_json_with_capacity_wait(
        &self,
        request: &Value,
        job_id: &str,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<(String, StatusUpdate), CompletionError> {
        let wait_deadline = tokio::time::Instant::now() + self.capacity_wait_limit();
        loop {
            match self.submit_json_discovery(request, job_id).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retryable_reason(err.as_ref()) != Some(FailureReason::CapacityUnavailable) {
                        return Err(err);
                    }
                    if self.cfg.fail_fast_on_capacity_unavailable
                        || tokio::time::Instant::now() >= wait_deadline
                    {
                        return Err(err);
                    }
                    emit_status(
                        on_status,
                        StatusUpdate {
                            status: STATUS_QUEUED.to_owned(),
                            message: "waiting for Discovery service capacity".to_owned(),
                            ..StatusUpdate::default()
                        },
                    );
                    tokio::time::sleep(nonzero_duration(
                        self.cfg.capacity_poll_interval,
                        StdDuration::from_secs(1),
                    ))
                    .await;
                }
            }
        }
    }

    async fn submit_discovery(
        &self,
        request: &ChatCompletionRequest,
        job_id: &str,
    ) -> Result<(String, StatusUpdate), CompletionError> {
        let job_request = build_discovery_job_request(&self.cfg, job_id, request)
            .map_err(|err| Box::new(err) as CompletionError)?;
        let body = serde_json::to_vec(&job_request).map_err(|err| {
            Box::new(AifarmClientError::Submit(err.to_string())) as CompletionError
        })?;
        let response = self
            .send_bounded(
                AifarmHttpRequest {
                    method: AifarmHttpMethod::Post,
                    url: self.cfg.endpoint("/v1/jobs/blocking"),
                    headers: [("Content-Type".to_owned(), "application/json".to_owned())].into(),
                    body,
                },
                self.task_limit(),
                "discovery blocking submit",
            )
            .await?;
        if !(200..300).contains(&response.status_code) {
            let body = response_body_text(&response);
            if is_capacity_unavailable(response.status_code, &body) {
                return Err(Box::new(ProviderError::new(
                    "aifarm",
                    FailureReason::CapacityUnavailable,
                    format!(
                        "discovery service capacity unavailable: status {}: {}",
                        response.status_code, body
                    ),
                )));
            }
            return Err(Box::new(AifarmClientError::Submit(format!(
                "status {}: {}",
                response.status_code, body
            ))));
        }
        let envelope =
            serde_json::from_slice::<DiscoveryJobEnvelope>(&response.body).map_err(|err| {
                Box::new(AifarmClientError::Submit(format!(
                    "decode discovery submit response: {err}"
                ))) as CompletionError
            })?;
        let job = envelope.resolve_job();
        let resolved_id = fallback_string(&job.resolved_id(), job_id);
        let error_message = parse_job_error(job.error.as_ref());
        if !error_message.is_empty() {
            return Err(Box::new(AifarmClientError::Submit(error_message)));
        }
        Ok((
            resolved_id.clone(),
            StatusUpdate {
                job_id: resolved_id,
                status: normalize_status(&job.resolved_status()),
                message: parse_job_error(job.error.as_ref()),
                ..StatusUpdate::default()
            },
        ))
    }

    async fn submit_json_discovery(
        &self,
        request: &Value,
        job_id: &str,
    ) -> Result<(String, StatusUpdate), CompletionError> {
        let job_request = build_discovery_json_job_request(&self.cfg, job_id, request)
            .map_err(|err| Box::new(err) as CompletionError)?;
        let body = serde_json::to_vec(&job_request).map_err(|err| {
            Box::new(AifarmClientError::Submit(err.to_string())) as CompletionError
        })?;
        let response = self
            .send_bounded(
                AifarmHttpRequest {
                    method: AifarmHttpMethod::Post,
                    url: self.cfg.endpoint("/v1/jobs/blocking"),
                    headers: [("Content-Type".to_owned(), "application/json".to_owned())].into(),
                    body,
                },
                self.task_limit(),
                "discovery blocking submit",
            )
            .await?;
        if !(200..300).contains(&response.status_code) {
            let body = response_body_text(&response);
            if is_capacity_unavailable(response.status_code, &body) {
                return Err(Box::new(ProviderError::new(
                    "aifarm",
                    FailureReason::CapacityUnavailable,
                    format!(
                        "discovery service capacity unavailable: status {}: {}",
                        response.status_code, body
                    ),
                )));
            }
            return Err(Box::new(AifarmClientError::Submit(format!(
                "status {}: {}",
                response.status_code, body
            ))));
        }
        let envelope =
            serde_json::from_slice::<DiscoveryJobEnvelope>(&response.body).map_err(|err| {
                Box::new(AifarmClientError::Submit(format!(
                    "decode discovery submit response: {err}"
                ))) as CompletionError
            })?;
        let job = envelope.resolve_job();
        let resolved_id = fallback_string(&job.resolved_id(), job_id);
        let error_message = parse_job_error(job.error.as_ref());
        if !error_message.is_empty() {
            return Err(Box::new(AifarmClientError::Submit(error_message)));
        }
        Ok((
            resolved_id.clone(),
            StatusUpdate {
                job_id: resolved_id,
                status: normalize_status(&job.resolved_status()),
                message: parse_job_error(job.error.as_ref()),
                ..StatusUpdate::default()
            },
        ))
    }

    async fn poll_discovery_result(
        &self,
        job_id: &str,
        initial_status: &StatusUpdate,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let mut last_status = normalize_status(&initial_status.status);
        // The blocking submit returns at queue ADMISSION, so a job may still
        // wait for a farm worker before its own task_timeout window starts.
        // Allow the capacity budget for queueing, then re-anchor the task
        // budget when the job is first seen running.
        let mut running_seen = is_running_status(&last_status);
        let mut poll_deadline = if running_seen {
            tokio::time::Instant::now() + self.task_limit()
        } else {
            tokio::time::Instant::now() + self.capacity_wait_limit() + self.task_limit()
        };
        loop {
            if tokio::time::Instant::now() >= poll_deadline {
                return Err(Box::new(AifarmClientError::Transport(format!(
                    "discovery job {job_id} did not finish within the task timeout"
                ))));
            }
            tokio::time::sleep(nonzero_duration(
                self.cfg.poll_interval,
                StdDuration::from_secs(1),
            ))
            .await;
            let status = self.check_discovery_status(job_id).await?;
            let normalized = normalize_status(&status.status);
            if !running_seen && is_running_status(&normalized) {
                running_seen = true;
                poll_deadline = tokio::time::Instant::now() + self.task_limit();
            }
            if !normalized.is_empty() && normalized != last_status {
                emit_status(on_status, status.as_update_with_status(&normalized));
                last_status = normalized.clone();
            }
            match discovery_result_from_status(job_id, &status, &normalized)? {
                Some(result) => return Ok(result),
                None => continue,
            }
        }
    }

    async fn check_discovery_status(
        &self,
        job_id: &str,
    ) -> Result<DiscoveryJobStatus, CompletionError> {
        let response = self
            .send_bounded(
                AifarmHttpRequest {
                    method: AifarmHttpMethod::Get,
                    url: self.cfg.endpoint(&format!("/v1/jobs/{}", job_id.trim())),
                    headers: BTreeMap::new(),
                    body: Vec::new(),
                },
                self.request_limit(),
                "discovery job status poll",
            )
            .await?;
        if !(200..300).contains(&response.status_code) {
            return Err(Box::new(AifarmClientError::CheckStatus(format!(
                "status {}: {}",
                response.status_code,
                response_body_text(&response)
            ))));
        }
        let envelope =
            serde_json::from_slice::<DiscoveryJobEnvelope>(&response.body).map_err(|err| {
                Box::new(AifarmClientError::CheckStatus(format!(
                    "decode dialog job status response: {err}"
                ))) as CompletionError
            })?;
        discovery_status_from_envelope(job_id, &envelope)
            .map_err(|err| Box::new(err) as CompletionError)
    }

    async fn poll_direct_result(
        &self,
        request_id: &str,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let poll_deadline = tokio::time::Instant::now() + self.task_limit();
        loop {
            let response = self.check_direct_status(request_id).await?;
            if response.status_code == 202 {
                if tokio::time::Instant::now() >= poll_deadline {
                    return Err(Box::new(AifarmClientError::Transport(format!(
                        "direct dialog request {request_id} did not finish within the task timeout"
                    ))));
                }
                tokio::time::sleep(nonzero_duration(
                    self.cfg.poll_interval,
                    StdDuration::from_secs(1),
                ))
                .await;
                continue;
            }
            emit_status(
                on_status,
                StatusUpdate {
                    job_id: request_id.to_owned(),
                    status: STATUS_SUCCEEDED.to_owned(),
                    message: "dialog request completed".to_owned(),
                    http_status: response.status_code,
                    ..StatusUpdate::default()
                },
            );
            let raw_body = response_body_text(&response);
            let decoded = decode_direct_completion_payload(
                &response.body,
                &raw_body,
                &format!("direct dialog status {request_id} succeeded but response body is empty"),
                &format!("decode direct dialog status payload {request_id}"),
            )?;
            return Ok(CompletionResult {
                job_id: request_id.to_owned(),
                status_code: response.status_code,
                raw_body,
                response: Some(decoded),
            });
        }
    }

    async fn check_direct_status(
        &self,
        request_id: &str,
    ) -> Result<AifarmHttpResponse, CompletionError> {
        let response = self
            .send_bounded(
                AifarmHttpRequest {
                    method: AifarmHttpMethod::Get,
                    url: direct_status_endpoint(&self.cfg.direct_url, request_id),
                    headers: direct_status_headers(&self.cfg),
                    body: Vec::new(),
                },
                self.request_limit(),
                "direct dialog status poll",
            )
            .await?;
        if response.status_code == 202 {
            return Ok(response);
        }
        direct_http_status_error(
            &format!("poll direct dialog request {request_id}"),
            &response,
            &response_body_text(&response),
        )?;
        Ok(response)
    }
}

/// AIFarm dialog service configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmDialogConfig {
    /// Public provider name.
    pub provider_name: String,
    /// AIFarm HTTP client configuration.
    pub client: AifarmClientConfig,
    /// Default model.
    pub model: String,
    /// Maximum tool-loop iterations.
    /// Maximum selected history turns including the current turn.
    pub max_history: usize,
    /// Configured max output tokens.
    pub max_tokens: i32,
    /// Temperature.
    pub temperature: Option<f64>,
    /// Top-p.
    pub top_p: Option<f64>,
    /// llama.cpp repeat penalty.
    pub repeat_penalty: Option<f64>,
    /// Frequency penalty.
    pub frequency_penalty: Option<f64>,
    /// Presence penalty.
    pub presence_penalty: Option<f64>,
    /// DRY multiplier.
    pub dry_multiplier: Option<f64>,
    /// DRY base.
    pub dry_base: Option<f64>,
    /// DRY allowed length.
    pub dry_allowed_length: i32,
    /// Whether OpenAI-compatible native tool calls are enabled.
    /// Whether model thinking is enabled.
    pub enable_thinking: Option<bool>,
    /// Whether reasoning output is included.
    pub include_reasoning: Option<bool>,
}

/// AIFarm history-summary generator configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmHistorySummaryConfig {
    /// AIFarm HTTP client configuration.
    pub client: AifarmClientConfig,
    /// Model name.
    pub model: String,
    /// Maximum output tokens.
    pub max_output_tokens: i32,
    /// Temperature.
    pub temperature: Option<f64>,
    /// Whether model thinking is enabled.
    pub enable_thinking: Option<bool>,
    /// Whether reasoning output is included.
    pub include_reasoning: Option<bool>,
}

/// AIFarm memory extractor configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmMemoryExtractorConfig {
    /// AIFarm HTTP client configuration.
    pub client: AifarmClientConfig,
    /// Model name.
    pub model: String,
    /// Maximum output tokens.
    pub max_output_tokens: i32,
    /// Temperature.
    pub temperature: Option<f64>,
    /// Frequency penalty (breaks self-questioning repetition loops); 0 disables.
    pub frequency_penalty: Option<f64>,
    /// Presence penalty; 0 disables.
    pub presence_penalty: Option<f64>,
    /// Whether model thinking is enabled.
    pub enable_thinking: Option<bool>,
    /// Whether reasoning output is included.
    pub include_reasoning: Option<bool>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GenkitOpenAiCompatibleMemoryExtractorConfig {
    /// OpenAI-compatible `/chat/completions` URL.
    pub direct_url: String,
    /// Provider API key.
    pub api_key: String,
    /// Provider model without the GenKit provider prefix.
    pub model: String,
    /// Request timeout.
    pub request_timeout: StdDuration,
    /// Maximum output tokens.
    pub max_output_tokens: i32,
    /// Temperature.
    pub temperature: f64,
    /// Top-p.
    pub top_p: f64,
}

impl Default for GenkitOpenAiCompatibleMemoryExtractorConfig {
    fn default() -> Self {
        Self {
            direct_url: String::new(),
            api_key: String::new(),
            model: String::new(),
            request_timeout: StdDuration::ZERO,
            max_output_tokens: DEFAULT_MEMORY_MAX_OUTPUT_TOKENS,
            temperature: 0.2,
            top_p: 0.9,
        }
    }
}

impl GenkitOpenAiCompatibleMemoryExtractorConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        if self.max_output_tokens <= 0 {
            self.max_output_tokens = DEFAULT_MEMORY_MAX_OUTPUT_TOKENS;
        }
        if self.temperature <= 0.0 {
            self.temperature = 0.2;
        }
        if self.top_p <= 0.0 {
            self.top_p = 0.9;
        }
        if self.request_timeout == StdDuration::ZERO {
            self.request_timeout = StdDuration::from_secs(600);
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GenkitOpenAiCompatibleHistorySummaryConfig {
    /// OpenAI-compatible `/chat/completions` URL.
    pub direct_url: String,
    /// Provider API key.
    pub api_key: String,
    /// Provider model without the GenKit provider prefix.
    pub model: String,
    /// Request timeout.
    pub request_timeout: StdDuration,
    /// Maximum output tokens.
    pub max_output_tokens: i32,
    /// Temperature.
    pub temperature: f64,
    /// Top-p.
    pub top_p: f64,
}

impl Default for GenkitOpenAiCompatibleHistorySummaryConfig {
    fn default() -> Self {
        Self {
            direct_url: String::new(),
            api_key: String::new(),
            model: String::new(),
            request_timeout: StdDuration::ZERO,
            max_output_tokens: DEFAULT_HISTORY_SUMMARY_MAX_OUTPUT_TOKENS,
            temperature: 0.45,
            top_p: 0.9,
        }
    }
}

impl GenkitOpenAiCompatibleHistorySummaryConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        if self.max_output_tokens <= 0 {
            self.max_output_tokens = DEFAULT_HISTORY_SUMMARY_MAX_OUTPUT_TOKENS;
        }
        if self.temperature <= 0.0 {
            self.temperature = 0.45;
        }
        if self.top_p <= 0.0 {
            self.top_p = 0.9;
        }
        if self.request_timeout == StdDuration::ZERO {
            self.request_timeout = StdDuration::from_secs(600);
        }
        self
    }
}

/// AIFarm structured JSON generator configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmStructuredJsonConfig {
    /// AIFarm HTTP client configuration.
    pub client: AifarmClientConfig,
    /// Default model.
    pub model: String,
    /// Default maximum output tokens.
    pub max_tokens: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct AifarmStructuredJsonMessage {
    /// Message role.
    pub role: String,
    /// Message content.
    pub content: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmStructuredJsonRequest {
    /// JSON schema name / flow name.
    pub name: String,
    /// Per-request model override.
    pub model: String,
    /// Chat messages.
    pub messages: Vec<AifarmStructuredJsonMessage>,
    /// JSON schema object.
    pub schema: Value,
    /// Per-request maximum output tokens.
    pub max_tokens: i32,
    /// Per-request temperature.
    pub temperature: f64,
}

impl AifarmHistorySummaryConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.model = default_string(&self.model, AIFARM_DEFAULT_HISTORY_SUMMARY_MODEL);
        self.max_output_tokens = if self.max_output_tokens <= 0 {
            DEFAULT_HISTORY_SUMMARY_MAX_OUTPUT_TOKENS
        } else {
            self.max_output_tokens
        };
        if self.client.default_model.trim().is_empty() {
            self.client.default_model = self.model.clone();
        }
        if self.client.service_name.trim().is_empty() {
            self.client.service_name = DEFAULT_SERVICE_NAME.to_owned();
        }
        if self.client.endpoint_name.trim().is_empty() {
            self.client.endpoint_name = DEFAULT_ENDPOINT_NAME.to_owned();
        }
        if self.client.request_timeout == StdDuration::ZERO {
            self.client.request_timeout = StdDuration::from_secs(11 * 60);
        }
        if self.client.poll_interval == StdDuration::ZERO {
            self.client.poll_interval = StdDuration::from_secs(1);
        }
        if self.client.task_timeout == StdDuration::ZERO {
            self.client.task_timeout = StdDuration::from_secs(12 * 60);
        }
        if self.client.capacity_wait == StdDuration::ZERO {
            self.client.capacity_wait = StdDuration::from_secs(10 * 60);
        }
        if self.client.capacity_poll_interval == StdDuration::ZERO {
            self.client.capacity_poll_interval = StdDuration::from_secs(1);
        }
        self.client.workload = AIFARM_WORKLOAD_SUMMARY.to_owned();
        self.enable_thinking = Some(false);
        self.include_reasoning = Some(false);
        self
    }
}

impl AifarmMemoryExtractorConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.model = default_string(&self.model, DEFAULT_MODEL_NAME);
        self.max_output_tokens = if self.max_output_tokens <= 0 {
            DEFAULT_MEMORY_MAX_OUTPUT_TOKENS
        } else {
            self.max_output_tokens
        };
        // Apply the anti-repetition default, then clamp to the [-2.0, 2.0] range
        // the OpenAI-compatible backend enforces so an out-of-range or non-finite
        // operator override cannot 400 every extraction request.
        self.frequency_penalty = Some(clamp_penalty(self.frequency_penalty.unwrap_or(0.3), 0.3));
        self.presence_penalty = Some(clamp_penalty(self.presence_penalty.unwrap_or(0.3), 0.3));
        if self.client.default_model.trim().is_empty() {
            self.client.default_model = self.model.clone();
        }
        if self.client.service_name.trim().is_empty() {
            self.client.service_name = DEFAULT_SERVICE_NAME.to_owned();
        }
        if self.client.endpoint_name.trim().is_empty() {
            self.client.endpoint_name = DEFAULT_ENDPOINT_NAME.to_owned();
        }
        if self.client.request_timeout == StdDuration::ZERO {
            self.client.request_timeout = StdDuration::from_secs(11 * 60);
        }
        if self.client.poll_interval == StdDuration::ZERO {
            self.client.poll_interval = StdDuration::from_secs(1);
        }
        if self.client.task_timeout == StdDuration::ZERO {
            self.client.task_timeout = StdDuration::from_secs(12 * 60);
        }
        if self.client.capacity_wait == StdDuration::ZERO {
            self.client.capacity_wait = StdDuration::from_secs(10 * 60);
        }
        if self.client.capacity_poll_interval == StdDuration::ZERO {
            self.client.capacity_poll_interval = StdDuration::from_secs(1);
        }
        self.client.priority = DISCOVERY_PRIORITY_MEMORY;
        self.client.workload = AIFARM_WORKLOAD_MEMORY.to_owned();
        self.enable_thinking = Some(false);
        self.include_reasoning = Some(false);
        self
    }
}

impl AifarmStructuredJsonConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.model = default_string(&self.model, DEFAULT_MODEL_NAME);
        self.max_tokens = if self.max_tokens <= 0 {
            1024
        } else {
            self.max_tokens
        };
        if self.client.base_url.trim().is_empty() {
            self.client.base_url = DEFAULT_AIFARM_BASE_URL.to_owned();
        }
        if self.client.default_model.trim().is_empty() {
            self.client.default_model = self.model.clone();
        }
        if self.client.service_name.trim().is_empty() {
            self.client.service_name = DEFAULT_SERVICE_NAME.to_owned();
        }
        if self.client.endpoint_name.trim().is_empty() {
            self.client.endpoint_name = DEFAULT_ENDPOINT_NAME.to_owned();
        }
        if self.client.request_timeout == StdDuration::ZERO {
            self.client.request_timeout = StdDuration::from_secs(45);
        }
        self.client.task_timeout = if self.client.task_timeout == StdDuration::ZERO {
            StdDuration::from_secs(2 * 60)
        } else {
            self.client.task_timeout.min(StdDuration::from_secs(2 * 60))
        };
        if self.client.request_timeout <= self.client.task_timeout {
            self.client.request_timeout = self.client.task_timeout + StdDuration::from_secs(5);
        }
        if self.client.poll_interval == StdDuration::ZERO {
            self.client.poll_interval = StdDuration::from_secs(1);
        }
        if self.client.capacity_wait == StdDuration::ZERO {
            self.client.capacity_wait = StdDuration::from_secs(5);
        }
        if self.client.capacity_poll_interval == StdDuration::ZERO {
            self.client.capacity_poll_interval = StdDuration::from_secs(1);
        }
        self.client.priority = DISCOVERY_PRIORITY_INTERACTIVE;
        self.client.workload = AIFARM_WORKLOAD_STRUCTURED.to_owned();
        self
    }
}

impl AifarmDialogConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.provider_name = default_string(&self.provider_name, PROVIDER_AIFARM);
        self.model = default_string(&self.model, DEFAULT_MODEL_NAME);
        self.max_history = if self.max_history == 0 {
            DEFAULT_CONTEXT_HISTORY_LIMIT
        } else {
            self.max_history
        };
        if self.client.default_model.trim().is_empty() {
            self.client.default_model = self.model.clone();
        }
        if self.client.service_name.trim().is_empty() {
            self.client.service_name = DEFAULT_SERVICE_NAME.to_owned();
        }
        if self.client.endpoint_name.trim().is_empty() {
            self.client.endpoint_name = DEFAULT_ENDPOINT_NAME.to_owned();
        }
        self.client.priority = DISCOVERY_PRIORITY_INTERACTIVE;
        self.client.workload = default_string(&self.client.workload, AIFARM_WORKLOAD_DIALOG);
        if self
            .provider_name
            .trim()
            .eq_ignore_ascii_case(PROVIDER_AIFARM)
        {
            self.enable_thinking = Some(false);
            self.include_reasoning = Some(false);
        }
        self
    }

    fn provider(&self) -> String {
        canonical_provider_name(&self.provider_name)
    }

    fn model_for_input(&self, input: &DialogInput) -> String {
        default_string(&input.model, &self.model)
    }

    fn max_tokens_for_input(&self, input: &DialogInput) -> i32 {
        if input.max_output_tokens <= 0 {
            return self.max_tokens;
        }
        if self.max_tokens <= 0 {
            return input.max_output_tokens;
        }
        input.max_output_tokens.min(self.max_tokens)
    }
}

/// HTTP-backed AIFarm dialog provider.
#[derive(Clone)]
pub struct AifarmDialogProvider<T = ReqwestAifarmTransport> {
    cfg: AifarmDialogConfig,
    client: AifarmHttpClient<T>,
    provider_name: String,
}

/// HTTP-backed AIFarm history-summary generator.
#[derive(Clone)]
pub struct AifarmHistorySummaryGenerator<T = ReqwestAifarmTransport> {
    cfg: AifarmHistorySummaryConfig,
    client: AifarmHttpClient<T>,
}

/// HTTP-backed AIFarm memory extractor.
#[derive(Clone)]
pub struct AifarmMemoryExtractor<T = ReqwestAifarmTransport> {
    cfg: AifarmMemoryExtractorConfig,
    client: AifarmHttpClient<T>,
}

/// HTTP-backed GenKit OpenAI-compatible memory extractor.
#[derive(Clone)]
pub struct GenkitOpenAiCompatibleMemoryExtractor<T = ReqwestAifarmTransport> {
    cfg: GenkitOpenAiCompatibleMemoryExtractorConfig,
    client: AifarmHttpClient<T>,
}

/// HTTP-backed GenKit OpenAI-compatible history-summary generator.
#[derive(Clone)]
pub struct GenkitOpenAiCompatibleHistorySummaryGenerator<T = ReqwestAifarmTransport> {
    cfg: GenkitOpenAiCompatibleHistorySummaryConfig,
    client: AifarmHttpClient<T>,
}

/// HTTP-backed AIFarm structured JSON generator.
#[derive(Clone)]
pub struct AifarmStructuredJsonGenerator<T = ReqwestAifarmTransport> {
    cfg: AifarmStructuredJsonConfig,
    client: AifarmHttpClient<T>,
}

impl AifarmHistorySummaryGenerator<ReqwestAifarmTransport> {
    /// Build a reqwest-backed history-summary generator.
    #[must_use]
    pub fn new(cfg: AifarmHistorySummaryConfig) -> Self {
        let cfg = cfg.with_defaults();
        let client =
            AifarmHttpClient::with_transport(cfg.client.clone(), ReqwestAifarmTransport::default());
        Self { cfg, client }
    }
}

impl AifarmMemoryExtractor<ReqwestAifarmTransport> {
    /// Build a reqwest-backed memory extractor.
    #[must_use]
    pub fn new(cfg: AifarmMemoryExtractorConfig) -> Self {
        let cfg = cfg.with_defaults();
        let client =
            AifarmHttpClient::with_transport(cfg.client.clone(), ReqwestAifarmTransport::default());
        Self { cfg, client }
    }
}

/// Trace metadata for an auxiliary aifarm flow (memory/history/optimizers). Context is
/// left default — these flows are attributed by flow/source/model, which drive analytics.
fn aux_llm_call_trace(flow: &str, source: &str) -> crate::trace::LlmCallTrace {
    crate::trace::LlmCallTrace {
        context: crate::trace::LlmCallContext::default(),
        tags: crate::trace::LlmCallTags {
            provider: PROVIDER_AIFARM.to_owned(),
            source: source.to_owned(),
            flow: flow.to_owned(),
            mode: "json".to_owned(),
            request_kind: "openai.chat.completions".to_owned(),
            iteration: 1,
            docs_chars: 0,
        },
    }
}

impl AifarmStructuredJsonGenerator<ReqwestAifarmTransport> {
    /// Build a reqwest-backed structured JSON generator.
    #[must_use]
    pub fn new(cfg: AifarmStructuredJsonConfig) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::new(cfg.client.clone());
        Self { cfg, client }
    }
}

impl<T> AifarmStructuredJsonGenerator<T>
where
    T: AifarmHttpTransport,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmStructuredJsonConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::with_transport(cfg.client.clone(), transport);
        Self { cfg, client }
    }

    /// Generate one structured JSON response.
    pub async fn generate_json(
        &self,
        request: AifarmStructuredJsonRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<String, AifarmStructuredJsonError> {
        let flow = request.name.trim().to_owned();
        let mut chat_request = self.request(request);
        chat_request.trace = Some(aux_llm_call_trace(&flow, "aifarm_structured"));
        let result = self
            .client
            .complete(chat_request, on_status)
            .await
            .map_err(|source| AifarmStructuredJsonError::Completion { source })?;
        let Some(response) = result.response.as_ref() else {
            return Err(AifarmStructuredJsonError::Response(
                "chat completion returned no response".to_owned(),
            ));
        };
        first_choice_content(response)
            .map_err(|err| AifarmStructuredJsonError::Response(err.to_string()))
    }

    pub async fn optimize_image_prompt(
        &self,
        text: &str,
        options: openplotva_media::OptimizePromptOptions,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<openplotva_media::ImageOptimize, AifarmMediaOptimizerError> {
        let text = text.trim();
        if text.is_empty() {
            return Err(AifarmMediaOptimizerError::EmptyText);
        }
        let variant_count = openplotva_media::normalize_variant_count(options.variant_count);
        let prompt = openplotva_media::render_image_optimizer_prompt(variant_count)?;
        let tool = openplotva_media::optimize_prompt_terminator_definition(variant_count);
        let content = self
            .generate_json(
                AifarmStructuredJsonRequest {
                    name: "optimize_prompt".to_owned(),
                    messages: optimizer_messages(&prompt, text),
                    schema: tool.input_schema,
                    max_tokens: aifarm_optimizer_max_tokens(variant_count),
                    temperature: 0.3,
                    ..AifarmStructuredJsonRequest::default()
                },
                on_status,
            )
            .await?;
        openplotva_media::decode_image_optimize_payload(&content, text, variant_count).map_err(
            |source| AifarmMediaOptimizerError::Decode {
                label: "AI Farm image optimizer",
                source,
            },
        )
    }

    pub async fn optimize_image_edit_prompt(
        &self,
        text: &str,
        options: openplotva_media::OptimizePromptOptions,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<openplotva_media::ImageEditOptimize, AifarmMediaOptimizerError> {
        let text = text.trim();
        if text.is_empty() {
            return Err(AifarmMediaOptimizerError::EmptyText);
        }
        let variant_count = openplotva_media::normalize_variant_count(options.variant_count);
        let prompt = openplotva_media::render_image_edit_optimizer_prompt(variant_count)?;
        let tool = openplotva_media::optimize_edit_prompt_terminator_definition(variant_count);
        let content = self
            .generate_json(
                AifarmStructuredJsonRequest {
                    name: "optimize_edit_prompt".to_owned(),
                    messages: optimizer_messages(&prompt, text),
                    schema: tool.input_schema,
                    max_tokens: aifarm_optimizer_max_tokens(variant_count),
                    temperature: 0.3,
                    ..AifarmStructuredJsonRequest::default()
                },
                on_status,
            )
            .await?;
        openplotva_media::decode_image_edit_optimize_payload(&content, text, variant_count).map_err(
            |source| AifarmMediaOptimizerError::Decode {
                label: "AI Farm edit optimizer",
                source,
            },
        )
    }

    pub async fn optimize_song_prompt(
        &self,
        request: openplotva_media::acestep::SongPromptRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<openplotva_media::acestep::SongPromptResult, AifarmMediaOptimizerError> {
        let (topic, language) = openplotva_media::acestep::normalize_song_prompt_input(&request)
            .map_err(|source| AifarmMediaOptimizerError::SongPrompt { source })?;
        let messages = openplotva_media::acestep::render_song_reprompt_messages(&topic, &language)?
            .into_iter()
            .map(|message| AifarmStructuredJsonMessage {
                role: message.role,
                content: message.content,
            })
            .collect();
        let tool = openplotva_media::acestep::optimize_song_prompt_terminator_definition();
        let content = self
            .generate_json(
                AifarmStructuredJsonRequest {
                    name: "optimize_song_prompt".to_owned(),
                    messages,
                    schema: tool.input_schema,
                    max_tokens: 1024,
                    temperature: 0.5,
                    ..AifarmStructuredJsonRequest::default()
                },
                on_status,
            )
            .await?;
        openplotva_media::acestep::decode_song_prompt_payload(&content, &topic, &language)
            .map_err(|source| AifarmMediaOptimizerError::SongPrompt { source })
    }

    fn request(&self, request: AifarmStructuredJsonRequest) -> ChatCompletionRequest {
        let name = default_string(&request.name, "structured_response");
        let model = default_string(&request.model, &self.cfg.model);
        let max_tokens = if request.max_tokens <= 0 {
            self.cfg.max_tokens
        } else {
            request.max_tokens
        };
        ChatCompletionRequest {
            model,
            messages: request
                .messages
                .into_iter()
                .map(|message| ChatMessage {
                    role: message.role,
                    content: message.content,
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                })
                .collect(),
            stream: false,
            response_format: Some(json!({
                "type": "json_schema",
                "json_schema": {
                    "name": name,
                    "schema": request.schema,
                },
            })),
            max_tokens,
            temperature: Some(request.temperature),
            include_reasoning: Some(false),
            chat_template_kwargs: Some(json!({ "enable_thinking": false })),
            extra_body: Some(json!({
                "chat_template_kwargs": { "enable_thinking": false },
            })),
            ..ChatCompletionRequest::default()
        }
    }
}

#[must_use]
pub fn aifarm_optimizer_max_tokens(variant_count: usize) -> i32 {
    let normalized = openplotva_media::normalize_variant_count(variant_count);
    let budget = normalized.saturating_mul(1024);
    i32::try_from(budget.max(2048)).unwrap_or(i32::MAX)
}

fn optimizer_messages(system_prompt: &str, text: &str) -> Vec<AifarmStructuredJsonMessage> {
    vec![
        AifarmStructuredJsonMessage {
            role: "system".to_owned(),
            content: system_prompt.to_owned(),
        },
        AifarmStructuredJsonMessage {
            role: "user".to_owned(),
            content: text.to_owned(),
        },
    ]
}

impl<T> AifarmHistorySummaryGenerator<T>
where
    T: AifarmHttpTransport + Clone,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmHistorySummaryConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::with_transport(cfg.client.clone(), transport);
        Self { cfg, client }
    }

    pub async fn generate_document(
        &self,
        input: &SummaryInput,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<SummaryDocument, AifarmHistorySummaryError> {
        let system_prompt = openplotva_prompts::read("history/summary")?;
        let payload =
            serde_json::to_string_pretty(input).map_err(AifarmHistorySummaryError::Input)?;
        let mut request = self.request(&system_prompt, &payload);
        request.trace = Some(aux_llm_call_trace(
            "history_summary",
            "aifarm_history_summary",
        ));
        let result = self
            .client
            .complete(request, on_status)
            .await
            .map_err(|source| AifarmHistorySummaryError::Completion { source })?;
        let Some(response) = result.response.as_ref() else {
            return Err(AifarmHistorySummaryError::Response(
                "chat completion returned no response".to_owned(),
            ));
        };
        let content = first_choice_content(response)
            .map_err(|err| AifarmHistorySummaryError::Response(err.to_string()))?;
        let decoded = decode_history_summary_response(&content)?;
        Ok(summary_document_from_llm(
            self.cfg.model.trim(),
            input,
            &decoded,
            &system_prompt,
        ))
    }

    fn request(&self, system_prompt: &str, payload: &str) -> ChatCompletionRequest {
        let mut request = ChatCompletionRequest {
            model: self.cfg.model.trim().to_owned(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt.to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload.to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
            ],
            stream: false,
            response_format: Some(history_summary_response_format()),
            max_tokens: self.cfg.max_output_tokens,
            temperature: self.cfg.temperature,
            include_reasoning: self.cfg.include_reasoning,
            ..ChatCompletionRequest::default()
        };
        if let Some(enable_thinking) = self.cfg.enable_thinking {
            request.set_chat_template_kwargs(json!({ "enable_thinking": enable_thinking }));
        }
        request
    }
}

impl<T> AifarmMemoryExtractor<T>
where
    T: AifarmHttpTransport + Clone,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmMemoryExtractorConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::with_transport(cfg.client.clone(), transport);
        Self { cfg, client }
    }

    pub async fn extract(
        &self,
        input: &ExtractInput,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<ExtractOutput, AifarmMemoryExtractorError> {
        let system_prompt = openplotva_prompts::read("memory/extraction")?;
        let payload = input
            .to_prompt_payload()
            .map_err(AifarmMemoryExtractorError::Input)?;
        let mut request = self.request(&system_prompt, &payload);
        request.trace = Some(aux_llm_call_trace(
            "memory_extraction",
            "aifarm_memory_extractor",
        ));
        let result = self
            .client
            .complete(request, on_status)
            .await
            .map_err(|source| AifarmMemoryExtractorError::Completion { source })?;
        let Some(response) = result.response.as_ref() else {
            return Err(AifarmMemoryExtractorError::Response(
                "chat completion returned no response".to_owned(),
            ));
        };
        decode_memory_extraction_output(response, &payload)
    }

    fn request(&self, system_prompt: &str, payload: &str) -> ChatCompletionRequest {
        let mut request = ChatCompletionRequest {
            model: self.cfg.model.trim().to_owned(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt.to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload.to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
            ],
            stream: false,
            response_format: Some(memory_extraction_response_format()),
            max_tokens: self.cfg.max_output_tokens,
            temperature: self.cfg.temperature,
            // Break the self-questioning loops the extractor otherwise falls into
            // inside the JSON (repeating "but wait…" until it hits the output cap).
            frequency_penalty: self.cfg.frequency_penalty,
            presence_penalty: self.cfg.presence_penalty,
            include_reasoning: self.cfg.include_reasoning,
            ..ChatCompletionRequest::default()
        };
        if let Some(enable_thinking) = self.cfg.enable_thinking {
            request.set_chat_template_kwargs(json!({ "enable_thinking": enable_thinking }));
        }
        request
    }

    /// Run the backlog subject merge-pass over one (scope, subject) group: the
    /// model folds over-extracted near-duplicates into survivors. Same transport,
    /// config and decode/salvage as extraction, a dedicated card-on-card prompt.
    pub async fn merge_subject(
        &self,
        input: &openplotva_memory::SubjectMergeInput,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<openplotva_memory::SubjectMergePlan, AifarmMemoryExtractorError> {
        let system_prompt = openplotva_prompts::read("memory/subject_merge")?;
        let payload = serde_json::to_string(input).map_err(AifarmMemoryExtractorError::Input)?;
        let mut request = self.subject_merge_request(&system_prompt, &payload);
        request.trace = Some(aux_llm_call_trace(
            "memory_subject_merge",
            "aifarm_memory_extractor",
        ));
        let result = self
            .client
            .complete(request, on_status)
            .await
            .map_err(|source| AifarmMemoryExtractorError::Completion { source })?;
        let Some(response) = result.response.as_ref() else {
            return Err(AifarmMemoryExtractorError::Response(
                "chat completion returned no response".to_owned(),
            ));
        };
        let content = first_choice_content(response)
            .map_err(|err| AifarmMemoryExtractorError::Response(err.to_string()))?;
        Ok(openplotva_memory::decode_subject_merge_plan(&content)?)
    }

    fn subject_merge_request(&self, system_prompt: &str, payload: &str) -> ChatCompletionRequest {
        let mut request = ChatCompletionRequest {
            model: self.cfg.model.trim().to_owned(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt.to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload.to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
            ],
            stream: false,
            response_format: Some(subject_merge_response_format()),
            max_tokens: self.cfg.max_output_tokens,
            temperature: self.cfg.temperature,
            frequency_penalty: self.cfg.frequency_penalty,
            presence_penalty: self.cfg.presence_penalty,
            include_reasoning: self.cfg.include_reasoning,
            ..ChatCompletionRequest::default()
        };
        if let Some(enable_thinking) = self.cfg.enable_thinking {
            request.set_chat_template_kwargs(json!({ "enable_thinking": enable_thinking }));
        }
        request
    }
}

impl<T> MemoryExtractor for AifarmMemoryExtractor<T>
where
    T: AifarmHttpTransport + Clone,
{
    type Error = AifarmMemoryExtractorError;

    fn extract<'a>(&'a self, input: &'a ExtractInput) -> MemoryExtractorFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut on_status = |_status: StatusUpdate| {};
            AifarmMemoryExtractor::extract(self, input, &mut on_status).await
        })
    }
}

impl<T> openplotva_memory::SubjectMerger for AifarmMemoryExtractor<T>
where
    T: AifarmHttpTransport + Clone,
{
    type Error = AifarmMemoryExtractorError;

    fn merge_subject<'a>(
        &'a self,
        input: &'a openplotva_memory::SubjectMergeInput,
    ) -> openplotva_memory::SubjectMergerFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut on_status = |_status: StatusUpdate| {};
            AifarmMemoryExtractor::merge_subject(self, input, &mut on_status).await
        })
    }
}

impl GenkitOpenAiCompatibleMemoryExtractor<ReqwestAifarmTransport> {
    /// Build a reqwest-backed GenKit OpenAI-compatible memory extractor.
    #[must_use]
    pub fn new(cfg: GenkitOpenAiCompatibleMemoryExtractorConfig) -> Self {
        let cfg = cfg.with_defaults();
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self::with_transport(cfg, ReqwestAifarmTransport::new(client))
    }
}

impl<T> GenkitOpenAiCompatibleMemoryExtractor<T>
where
    T: AifarmHttpTransport,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: GenkitOpenAiCompatibleMemoryExtractorConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: cfg.direct_url.clone(),
                api_key: cfg.api_key.clone(),
                request_timeout: cfg.request_timeout,
                default_model: cfg.model.clone(),
                ..AifarmClientConfig::default()
            },
            transport,
        );
        Self { cfg, client }
    }

    pub fn request_for_input(
        &self,
        input: &ExtractInput,
    ) -> Result<ChatCompletionRequest, GenkitOpenAiCompatibleMemoryExtractorError> {
        let system_prompt = openplotva_prompts::read("memory/extraction")?;
        let payload = input
            .to_prompt_payload()
            .map_err(GenkitOpenAiCompatibleMemoryExtractorError::Input)?;
        Ok(ChatCompletionRequest {
            model: self.cfg.model.trim().to_owned(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt,
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload,
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
            ],
            stream: false,
            max_tokens: self.cfg.max_output_tokens,
            temperature: Some(self.cfg.temperature),
            top_p: Some(self.cfg.top_p),
            ..ChatCompletionRequest::default()
        })
    }

    pub async fn extract(
        &self,
        input: &ExtractInput,
    ) -> Result<ExtractOutput, GenkitOpenAiCompatibleMemoryExtractorError> {
        let request = self.request_for_input(input)?;
        let payload = request
            .messages
            .get(1)
            .map(|message| message.content.clone())
            .unwrap_or_default();
        let result = self
            .client
            .complete(request, &mut |_| {})
            .await
            .map_err(|source| GenkitOpenAiCompatibleMemoryExtractorError::Completion { source })?;
        let Some(response) = result.response.as_ref() else {
            return Err(GenkitOpenAiCompatibleMemoryExtractorError::Response(
                "chat completion returned no response".to_owned(),
            ));
        };
        let content = first_choice_content(response).map_err(|error| {
            GenkitOpenAiCompatibleMemoryExtractorError::Response(error.to_string())
        })?;
        let mut out = decode_extraction_json(&content)?;
        if out.input_tokens == 0 {
            out.input_tokens = response
                .get("usage")
                .and_then(|usage| usage.get("prompt_tokens"))
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .unwrap_or_default();
        }
        if out.output_tokens == 0 {
            out.output_tokens = response
                .get("usage")
                .and_then(|usage| usage.get("completion_tokens"))
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())
                .unwrap_or_default();
        }
        if out.input_tokens == 0 {
            out.input_tokens = estimate_memory_tokens(&payload);
        }
        if out.output_tokens == 0 {
            out.output_tokens = estimate_memory_tokens(&content);
        }
        Ok(out)
    }
}

impl<T> MemoryExtractor for GenkitOpenAiCompatibleMemoryExtractor<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    type Error = GenkitOpenAiCompatibleMemoryExtractorError;

    fn extract<'a>(&'a self, input: &'a ExtractInput) -> MemoryExtractorFuture<'a, Self::Error> {
        Box::pin(async move { GenkitOpenAiCompatibleMemoryExtractor::extract(self, input).await })
    }
}

impl GenkitOpenAiCompatibleHistorySummaryGenerator<ReqwestAifarmTransport> {
    /// Build a reqwest-backed GenKit OpenAI-compatible history-summary generator.
    #[must_use]
    pub fn new(cfg: GenkitOpenAiCompatibleHistorySummaryConfig) -> Self {
        let cfg = cfg.with_defaults();
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self::with_transport(cfg, ReqwestAifarmTransport::new(client))
    }
}

impl<T> GenkitOpenAiCompatibleHistorySummaryGenerator<T>
where
    T: AifarmHttpTransport,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: GenkitOpenAiCompatibleHistorySummaryConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: cfg.direct_url.clone(),
                api_key: cfg.api_key.clone(),
                request_timeout: cfg.request_timeout,
                default_model: cfg.model.clone(),
                ..AifarmClientConfig::default()
            },
            transport,
        );
        Self { cfg, client }
    }

    pub fn request_for_input(
        &self,
        input: &SummaryInput,
    ) -> Result<ChatCompletionRequest, GenkitOpenAiCompatibleHistorySummaryError> {
        let system_prompt = openplotva_prompts::read("history/summary")?;
        let payload = serde_json::to_string_pretty(input)
            .map_err(GenkitOpenAiCompatibleHistorySummaryError::Input)?;
        Ok(ChatCompletionRequest {
            model: self.cfg.model.trim().to_owned(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt,
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload,
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
            ],
            stream: false,
            max_tokens: self.cfg.max_output_tokens,
            temperature: Some(self.cfg.temperature),
            top_p: Some(self.cfg.top_p),
            ..ChatCompletionRequest::default()
        })
    }

    pub async fn generate_document(
        &self,
        input: &SummaryInput,
    ) -> Result<SummaryDocument, GenkitOpenAiCompatibleHistorySummaryError> {
        let system_prompt = openplotva_prompts::read("history/summary")?;
        let payload = serde_json::to_string_pretty(input)
            .map_err(GenkitOpenAiCompatibleHistorySummaryError::Input)?;
        let result = self
            .client
            .complete(
                ChatCompletionRequest {
                    model: self.cfg.model.trim().to_owned(),
                    messages: vec![
                        ChatMessage {
                            role: "system".to_owned(),
                            content: system_prompt.clone(),
                            content_parts: Vec::new(),
                            ..ChatMessage::default()
                        },
                        ChatMessage {
                            role: "user".to_owned(),
                            content: payload,
                            content_parts: Vec::new(),
                            ..ChatMessage::default()
                        },
                    ],
                    stream: false,
                    max_tokens: self.cfg.max_output_tokens,
                    temperature: Some(self.cfg.temperature),
                    top_p: Some(self.cfg.top_p),
                    ..ChatCompletionRequest::default()
                },
                &mut |_| {},
            )
            .await
            .map_err(|source| GenkitOpenAiCompatibleHistorySummaryError::Completion { source })?;
        let Some(response) = result.response.as_ref() else {
            return Err(GenkitOpenAiCompatibleHistorySummaryError::Response(
                "chat completion returned no response".to_owned(),
            ));
        };
        let content = first_choice_content(response).map_err(|error| {
            GenkitOpenAiCompatibleHistorySummaryError::Response(error.to_string())
        })?;
        let decoded = decode_history_summary_response(&content)?;
        Ok(summary_document_from_llm(
            &self.cfg.model,
            input,
            &decoded,
            &system_prompt,
        ))
    }
}

impl AifarmDialogProvider<ReqwestAifarmTransport> {
    /// Build a reqwest-backed AIFarm dialog provider.
    #[must_use]
    pub fn new(cfg: AifarmDialogConfig) -> Self {
        let cfg = cfg.with_defaults();
        let provider_name = cfg.provider();
        let client =
            AifarmHttpClient::with_transport(cfg.client.clone(), ReqwestAifarmTransport::default());
        Self {
            cfg,
            client,
            provider_name,
        }
    }
}

impl<T> AifarmDialogProvider<T>
where
    T: AifarmHttpTransport + Clone,
{
    /// Build with a custom transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmDialogConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let provider_name = cfg.provider();
        let client = AifarmHttpClient::with_transport(cfg.client.clone(), transport);
        Self {
            cfg,
            client,
            provider_name,
        }
    }

    /// Stable provider name.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider_name
    }

    /// Run one HTTP-backed dialog request and return a provider-neutral output.
    /// Run one single-shot chat step: no tool execution, no iteration — the
    /// dialog session engine owns the loop and calls this once per iteration.
    pub async fn run_chat_step_with_status(
        &self,
        request: ChatStepRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<ChatStepOutput, CompletionError> {
        let history = build_session_history_with_limit(&request.input, self.cfg.max_history);
        let iteration = request.iteration.max(1);
        let completion_request = self
            .step_request_with_history(
                &request.input,
                &history,
                &request.transcript,
                &request.tools,
                iteration,
            )
            .map_err(|error| Box::new(error) as CompletionError)?;
        let model = completion_request.model.clone();
        let trace_request = redacted_chat_completion_request(&completion_request);
        let result = match self.client.complete(completion_request, on_status).await {
            Ok(result) => result,
            Err(error) => {
                let message = error.to_string();
                let mut trace = aifarm_dialog_trace_artifacts(
                    &trace_request,
                    &CompletionResult::default(),
                    &request.input,
                    self.provider(),
                    iteration,
                );
                trace.error = message;
                return Err(aifarm_step_error_with_trace(error, trace));
            }
        };
        let trace = aifarm_dialog_trace_artifacts(
            &trace_request,
            &result,
            &request.input,
            self.provider(),
            iteration,
        );
        let Some(response) = result.response.as_ref() else {
            let error = Box::new(AifarmDialogError::Response(
                "chat completion response is nil".to_owned(),
            )) as CompletionError;
            return Err(aifarm_step_error_with_trace(error, trace));
        };

        // FinalOnly keeps the tool-aware prompt for cache stability but the
        // engine executes nothing on that pass: parse tool calls only when a
        // tools array was actually offered, so stray tool markup on the
        // forced-final step degrades to sanitized text instead of an empty
        // answer.
        let tools_offered =
            !request.input.disable_tools && matches!(request.tools, ToolsMode::Native(_));
        if !tools_offered {
            let strip_markup = !matches!(request.tools, ToolsMode::Disabled);
            let text = match extract_final_answer_for_provider(response, self.provider()) {
                Ok(text) => text,
                Err(error) => return Err(aifarm_step_error_with_trace(error, trace)),
            };
            let text = if strip_markup {
                sanitize_tool_text(&text)
            } else {
                text
            };
            return Ok(ChatStepOutput {
                provider: self.provider().to_owned(),
                model,
                text,
                tool_calls: Vec::new(),
                trace: Some(trace),
            });
        }

        match first_choice_tool_steps(response) {
            Ok(ToolStepSelection::None(decision)) => {
                self.record_step_parser_decision(&model, iteration, &decision);
                let text = match extract_final_answer_for_provider(response, self.provider()) {
                    Ok(text) => text,
                    Err(error) => return Err(aifarm_step_error_with_trace(error, trace)),
                };
                Ok(ChatStepOutput {
                    provider: self.provider().to_owned(),
                    model,
                    text,
                    tool_calls: Vec::new(),
                    trace: Some(trace),
                })
            }
            Ok(ToolStepSelection::Steps {
                steps,
                text,
                residual_protocol,
            }) => {
                if residual_protocol {
                    let error = tool_protocol_completion_error(
                        "assistant content retained ambiguous protocol markup",
                    );
                    return Err(aifarm_step_error_with_trace(error, trace));
                }
                let mut tool_calls = Vec::with_capacity(steps.len());
                let mut used_ids = steps
                    .iter()
                    .filter_map(|pending| pending.native_ref.as_deref())
                    .filter(|value| !value.trim().is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<std::collections::BTreeSet<_>>();
                for (index, pending) in steps.into_iter().enumerate() {
                    let PendingToolStep {
                        step,
                        decision,
                        native_ref,
                    } = pending;
                    self.record_step_parser_decision(&model, iteration, &decision);
                    let salvaged = native_ref.is_none();
                    let id = native_ref
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or_else(|| {
                            let mut suffix = index;
                            loop {
                                let candidate = format!("call-{iteration}-{suffix}");
                                if used_ids.insert(candidate.clone()) {
                                    break candidate;
                                }
                                suffix += 1;
                            }
                        });
                    tool_calls.push(ChatStepToolCall { id, step, salvaged });
                }
                // Native calls arrive in their own channel. Pseudo calls are
                // removed by exact protocol spans before the remaining
                // intermediate text reaches this boundary.
                Ok(ChatStepOutput {
                    provider: self.provider().to_owned(),
                    model,
                    text,
                    tool_calls,
                    trace: Some(trace),
                })
            }
            Err(error) => Err(aifarm_step_error_with_trace(error, trace)),
        }
    }

    fn step_request_with_history(
        &self,
        input: &DialogInput,
        history: &[HistoryMessage],
        transcript: &[SessionMessage],
        tools: &ToolsMode,
        iteration: usize,
    ) -> Result<ChatCompletionRequest, AifarmDialogError> {
        let model = self.cfg.model_for_input(input);
        // The tool-aware system prompt stays constant across the whole session
        // (including forced-final passes) so the prompt-cache prefix survives;
        // only the request-level tools array varies.
        let mode = if input.disable_tools || matches!(tools, ToolsMode::Disabled) {
            ToolPromptMode::None
        } else {
            ToolPromptMode::Native
        };
        // The transcript wire form follows the actual tools offer: native
        // roles only when the request carries a tools array (FinalOnly keeps
        // the tool-aware prompt but renders tool activity as text blocks).
        let native = matches!(tools, ToolsMode::Native(_));
        let mut messages = build_initial_messages_with_tool_prompt(input, history, mode)?;
        messages.extend(transcript_chat_messages(transcript, native));
        let mut request = ChatCompletionRequest {
            model,
            messages,
            stream: false,
            max_tokens: self.cfg.max_tokens_for_input(input),
            temperature: self.cfg.temperature,
            top_p: self.cfg.top_p,
            repeat_penalty: self.cfg.repeat_penalty,
            frequency_penalty: self.cfg.frequency_penalty,
            presence_penalty: self.cfg.presence_penalty,
            dry_multiplier: self.cfg.dry_multiplier,
            dry_base: self.cfg.dry_base,
            dry_allowed_length: self.cfg.dry_allowed_length,
            include_reasoning: self.cfg.include_reasoning,
            ..ChatCompletionRequest::default()
        };
        if let ToolsMode::Native(tool_values) = tools
            && !tool_values.is_empty()
        {
            request.tools = tool_values.clone();
            request.tool_choice = Some(Value::String("auto".to_owned()));
            request.parallel_tool_calls = Some(false);
        }
        if let Some(enable_thinking) = input.enable_thinking.or(self.cfg.enable_thinking) {
            request.set_chat_template_kwargs(json!({ "enable_thinking": enable_thinking }));
        }
        let docs_chars = input
            .reference_context
            .iter()
            .map(String::len)
            .sum::<usize>()
            .min(i32::MAX as usize) as i32;
        let provider = self.provider().to_owned();
        request.trace = Some(crate::trace::LlmCallTrace {
            context: crate::trace::LlmCallContext {
                chat_id: input.context.chat_id,
                thread_id: input.context.thread_id,
                chat_title: input.context.chat_title.clone(),
                user_id: input.user.id,
                full_name: input.user.full_name.clone(),
                message_id: input.message.id,
            },
            tags: crate::trace::LlmCallTags {
                provider: provider.clone(),
                source: provider,
                flow: "dialog".to_owned(),
                mode: "session".to_owned(),
                request_kind: "openai.chat.completions".to_owned(),
                iteration,
                docs_chars,
            },
        });
        Ok(request)
    }

    fn record_step_parser_decision(
        &self,
        model: &str,
        iteration: usize,
        decision: &ToolParseDecision,
    ) {
        if decision.outcome.trim().is_empty() {
            return;
        }
        tool_telemetry::record(tool_telemetry::ToolTelemetryEvent {
            provider: self.provider().to_owned(),
            model: model.trim().to_owned(),
            tool: decision.tool.trim().to_owned(),
            form: decision.form.trim().to_owned(),
            outcome: decision.outcome.trim().to_owned(),
            reason: decision.reason.trim().to_owned(),
            iteration: i32::try_from(iteration).unwrap_or(i32::MAX),
            ..tool_telemetry::ToolTelemetryEvent::default()
        });
    }
}

impl<T> crate::ChatProvider for AifarmDialogProvider<T>
where
    T: AifarmHttpTransport + Clone + Send + Sync,
{
    fn provider_name(&self) -> &str {
        self.provider()
    }

    fn as_chat_step(&self) -> Option<&dyn crate::ChatStepProvider> {
        Some(self)
    }
}

impl<T> crate::ChatStepProvider for AifarmDialogProvider<T>
where
    T: AifarmHttpTransport + Clone + Send + Sync,
{
    fn provider_name(&self) -> &str {
        self.provider()
    }

    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(&'a self, request: ChatStepRequest) -> crate::ChatStepFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_| {};
            self.run_chat_step_with_status(request, &mut ignore_status)
                .await
        })
    }
}

/// Static routing tags for a low-level aifarm call trace.
#[derive(Clone, Copy)]
pub(crate) struct TraceTags<'a> {
    pub provider: &'a str,
    pub source: &'a str,
    pub flow: &'a str,
    pub mode: &'a str,
    pub request_kind: &'a str,
    pub iteration: usize,
}

/// Build a trace artifact for any aifarm completion (dialog or auxiliary). `docs_chars`
/// is passed explicitly (0 for non-dialog flows).
pub(crate) fn aifarm_call_trace_artifacts(
    request: &ChatCompletionRequest,
    result: &CompletionResult,
    docs_chars: i32,
    tags: TraceTags<'_>,
) -> DialogTraceArtifacts {
    DialogTraceArtifacts {
        provider: tags.provider.trim().to_owned(),
        request_kind: tags.request_kind.to_owned(),
        source: tags.source.trim().to_owned(),
        mode: tags.mode.to_owned(),
        flow: tags.flow.to_owned(),
        iteration: i32::try_from(tags.iteration).unwrap_or(i32::MAX),
        model: request.model.trim().to_owned(),
        raw_request: serde_json::to_value(request).ok(),
        raw_response: aifarm_trace_raw_response(result),
        transport: aifarm_trace_transport(result),
        inference_params: aifarm_trace_inference_params(request),
        usage: result.response.as_ref().and_then(aifarm_trace_usage),
        timings: result.response.as_ref().and_then(aifarm_trace_timings),
        prompt_chars: json_size(&request.messages),
        prompt_messages: i32::try_from(request.messages.len()).unwrap_or(i32::MAX),
        docs_chars,
        ..DialogTraceArtifacts::default()
    }
}

fn aifarm_dialog_trace_artifacts(
    request: &ChatCompletionRequest,
    result: &CompletionResult,
    input: &DialogInput,
    provider: &str,
    iteration: usize,
) -> DialogTraceArtifacts {
    let docs_chars = input
        .reference_context
        .iter()
        .map(String::len)
        .sum::<usize>()
        .min(i32::MAX as usize) as i32;
    aifarm_call_trace_artifacts(
        request,
        result,
        docs_chars,
        TraceTags {
            provider,
            source: provider,
            flow: "dialog",
            mode: "tools",
            request_kind: "openai.chat.completions",
            iteration,
        },
    )
}

#[cfg(test)]
mod call_trace_artifact_tests {
    use super::{
        ChatCompletionRequest, CompletionResult, TraceTags, aifarm_call_trace_artifacts,
        aux_llm_call_trace,
    };

    #[test]
    fn aux_llm_call_trace_tags_match_go_for_memory() {
        let trace = aux_llm_call_trace("memory_extraction", "aifarm_memory_extractor");
        assert_eq!(trace.tags.flow, "memory_extraction");
        assert_eq!(trace.tags.source, "aifarm_memory_extractor");
        assert_eq!(trace.tags.provider, super::PROVIDER_AIFARM);
        assert_eq!(trace.tags.request_kind, "openai.chat.completions");
    }

    #[test]
    fn aifarm_call_trace_artifacts_tags_flow_and_model() {
        let request = ChatCompletionRequest {
            model: "vram.cloud/qwen3.6-27b".to_owned(),
            ..ChatCompletionRequest::default()
        };
        let result = CompletionResult::default();
        let artifact = aifarm_call_trace_artifacts(
            &request,
            &result,
            0,
            TraceTags {
                provider: "aifarm",
                source: "aifarm_memory_extractor",
                flow: "memory_extraction",
                mode: "json",
                request_kind: "openai.chat.completions",
                iteration: 1,
            },
        );
        assert_eq!(artifact.flow, "memory_extraction");
        assert_eq!(artifact.source, "aifarm_memory_extractor");
        assert_eq!(artifact.model, "vram.cloud/qwen3.6-27b");
        assert_eq!(artifact.request_kind, "openai.chat.completions");
        assert_eq!(artifact.provider, "aifarm");
    }
}

fn aifarm_step_error_with_trace(
    error: CompletionError,
    mut trace: DialogTraceArtifacts,
) -> CompletionError {
    if trace.error.trim().is_empty() {
        trace.error = error.to_string();
    }
    Box::new(DialogTraceError::new(error, vec![trace]))
}

/// OpenAI wire form of a tool call's `arguments`: a JSON-encoded string.
fn session_tool_call_arguments_wire(arguments: &Value) -> String {
    match arguments {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Map the in-session transcript onto wire messages. With `native` the
/// assistant tool calls and tool results use real roles and ids (OpenAI tools
/// protocol); otherwise tool activity renders as the same plain-text context
/// blocks cross-turn history uses, so tool-less echelons still see prior work.
pub(crate) fn transcript_chat_messages(
    transcript: &[SessionMessage],
    native: bool,
) -> Vec<ChatMessage> {
    let mut messages = Vec::with_capacity(transcript.len());
    for entry in transcript {
        match entry {
            SessionMessage::Assistant { text, tool_calls } => {
                if native {
                    let calls = (!tool_calls.is_empty()).then(|| {
                        Value::Array(
                            tool_calls
                                .iter()
                                .map(|call| {
                                    json!({
                                        "id": call.id,
                                        "type": "function",
                                        "function": {
                                            "name": call.name,
                                            "arguments":
                                                session_tool_call_arguments_wire(&call.arguments),
                                        },
                                    })
                                })
                                .collect(),
                        )
                    });
                    messages.push(ChatMessage {
                        role: "assistant".to_owned(),
                        content: text.clone(),
                        tool_calls: calls,
                        ..ChatMessage::default()
                    });
                } else {
                    if !text.trim().is_empty() {
                        messages.push(ChatMessage {
                            role: "assistant".to_owned(),
                            content: text.clone(),
                            ..ChatMessage::default()
                        });
                    }
                    for call in tool_calls {
                        messages.push(ChatMessage {
                            role: "assistant".to_owned(),
                            content: format!(
                                "<tool_call name=\"{}\" ref=\"{}\"/>",
                                call.name, call.id
                            ),
                            ..ChatMessage::default()
                        });
                    }
                }
            }
            SessionMessage::ToolResult {
                tool_call_id,
                name,
                content,
            } => {
                if native {
                    messages.push(ChatMessage {
                        role: "tool".to_owned(),
                        content: content.clone(),
                        tool_call_id: Some(tool_call_id.clone()),
                        name: Some(name.clone()),
                        ..ChatMessage::default()
                    });
                } else {
                    messages.push(ChatMessage {
                        role: "user".to_owned(),
                        content: format!(
                            "<tool_result name=\"{name}\" ref=\"{tool_call_id}\"><output>{content}</output></tool_result>"
                        ),
                        ..ChatMessage::default()
                    });
                }
            }
            SessionMessage::InjectedUser { rendered } => {
                messages.push(ChatMessage {
                    role: "user".to_owned(),
                    content: rendered.clone(),
                    ..ChatMessage::default()
                });
            }
        }
    }
    messages
}

fn redacted_chat_completion_request(request: &ChatCompletionRequest) -> ChatCompletionRequest {
    let mut out = request.clone();
    for message in &mut out.messages {
        for part in &mut message.content_parts {
            if let Some(image_url) = part.image_url.as_mut()
                && image_url.url.trim_start().starts_with("data:")
            {
                image_url.url = "data:<redacted-image>".to_owned();
            }
            if let Some(video_url) = part.video_url.as_mut()
                && video_url.url.trim_start().starts_with("data:")
            {
                video_url.url = "data:<redacted-video>".to_owned();
            }
        }
    }
    out
}

fn aifarm_trace_raw_response(result: &CompletionResult) -> Option<Value> {
    let raw = result.raw_body.trim();
    if !raw.is_empty() {
        return Some(
            serde_json::from_str::<Value>(raw).unwrap_or_else(|_| Value::String(raw.to_owned())),
        );
    }
    result.response.clone()
}

fn aifarm_trace_transport(result: &CompletionResult) -> Option<Value> {
    if result.job_id.trim().is_empty() {
        return None;
    }
    Some(json!({ "job_id": result.job_id.trim() }))
}

fn aifarm_trace_inference_params(request: &ChatCompletionRequest) -> Option<Value> {
    let mut map = serde_json::Map::new();
    insert_positive_i32(&mut map, "max_tokens", request.max_tokens);
    insert_opt_f64(&mut map, "temperature", request.temperature);
    insert_opt_f64(&mut map, "top_p", request.top_p);
    insert_opt_f64(&mut map, "top_k", request.top_k);
    insert_opt_f64(&mut map, "repeat_penalty", request.repeat_penalty);
    insert_opt_f64(&mut map, "repetition_penalty", request.repetition_penalty);
    insert_opt_f64(&mut map, "frequency_penalty", request.frequency_penalty);
    insert_opt_f64(&mut map, "presence_penalty", request.presence_penalty);
    insert_opt_f64(&mut map, "dry_multiplier", request.dry_multiplier);
    insert_opt_f64(&mut map, "dry_base", request.dry_base);
    insert_positive_i32(&mut map, "dry_allowed_length", request.dry_allowed_length);
    insert_opt_bool(&mut map, "include_reasoning", request.include_reasoning);
    insert_non_empty_string(&mut map, "tool_mode", aifarm_trace_tool_mode(request));
    if let Some(response_format) = aifarm_trace_response_format(request.response_format.as_ref()) {
        map.insert("response_format".to_owned(), Value::String(response_format));
    }
    if let Some(chat_template_kwargs) = request.chat_template_kwargs.clone() {
        if let Some(enable_thinking) = chat_template_kwargs
            .get("enable_thinking")
            .and_then(Value::as_bool)
        {
            insert_opt_bool(&mut map, "enable_thinking", Some(enable_thinking));
        }
        map.insert("chat_template_kwargs".to_owned(), chat_template_kwargs);
    }
    (!map.is_empty()).then_some(Value::Object(map))
}

fn aifarm_trace_tool_mode(request: &ChatCompletionRequest) -> String {
    if request.tools.is_empty() {
        return "none".to_owned();
    }
    let Some(choice) = request.tool_choice.as_ref() else {
        return "auto".to_owned();
    };
    if let Some(text) = choice.as_str() {
        let trimmed = text.trim();
        return if trimmed.is_empty() {
            "custom".to_owned()
        } else {
            trimmed.to_owned()
        };
    }
    choice
        .get("function")
        .and_then(|function| function.get("name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| "custom".to_owned())
}

fn aifarm_trace_response_format(response_format: Option<&Value>) -> Option<String> {
    let response_format = response_format?;
    if let Some(kind) = response_format.get("type").and_then(Value::as_str) {
        let kind = kind.trim();
        if !kind.is_empty() {
            return Some(kind.to_owned());
        }
    }
    response_format
        .get("json_schema")
        .and_then(|schema| schema.get("name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| format!("json_schema:{name}"))
}

fn aifarm_trace_usage(response: &Value) -> Option<DialogTraceUsage> {
    let usage = response.get("usage")?;
    let out = DialogTraceUsage {
        input_tokens: json_i32(usage, "prompt_tokens"),
        output_tokens: json_i32(usage, "completion_tokens"),
        total_tokens: json_i32(usage, "total_tokens"),
        cached_tokens: usage
            .get("prompt_tokens_details")
            .map(|details| json_i32(details, "cached_tokens"))
            .unwrap_or_default(),
        thoughts_tokens: usage
            .get("completion_tokens_details")
            .map(|details| json_i32(details, "reasoning_tokens"))
            .unwrap_or_default(),
        ..DialogTraceUsage::default()
    };
    (out.input_tokens != 0
        || out.output_tokens != 0
        || out.total_tokens != 0
        || out.cached_tokens != 0
        || out.thoughts_tokens != 0)
        .then_some(out)
}

fn aifarm_trace_timings(response: &Value) -> Option<Value> {
    let timings = response.get("timings")?;
    let mut map = serde_json::Map::new();
    insert_json_i32(&mut map, "prompt_eval_tokens", timings, "prompt_n");
    insert_json_f64(&mut map, "prompt_eval_ms", timings, "prompt_ms");
    insert_json_f64(&mut map, "prompt_tps", timings, "prompt_per_second");
    insert_json_i32(&mut map, "generation_tokens", timings, "predicted_n");
    insert_json_f64(&mut map, "generation_ms", timings, "predicted_ms");
    insert_json_f64(&mut map, "generation_tps", timings, "predicted_per_second");
    (!map.is_empty()).then_some(Value::Object(map))
}

fn insert_positive_i32(map: &mut serde_json::Map<String, Value>, key: &str, value: i32) {
    if value > 0 {
        map.insert(key.to_owned(), json!(value));
    }
}

fn insert_opt_f64(map: &mut serde_json::Map<String, Value>, key: &str, value: Option<f64>) {
    if let Some(value) = value {
        map.insert(key.to_owned(), json!(value));
    }
}

fn insert_opt_bool(map: &mut serde_json::Map<String, Value>, key: &str, value: Option<bool>) {
    if let Some(value) = value {
        map.insert(key.to_owned(), json!(value));
    }
}

fn insert_non_empty_string(map: &mut serde_json::Map<String, Value>, key: &str, value: String) {
    if !value.trim().is_empty() {
        map.insert(key.to_owned(), Value::String(value));
    }
}

fn insert_json_i32(
    map: &mut serde_json::Map<String, Value>,
    key: &str,
    value: &Value,
    field: &str,
) {
    let value = json_i32(value, field);
    if value != 0 {
        map.insert(key.to_owned(), json!(value));
    }
}

fn insert_json_f64(
    map: &mut serde_json::Map<String, Value>,
    key: &str,
    value: &Value,
    field: &str,
) {
    let value = json_f64(value, field);
    if value != 0.0 {
        map.insert(key.to_owned(), json!(value));
    }
}

fn json_i32(value: &Value, field: &str) -> i32 {
    value
        .get(field)
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

fn json_f64(value: &Value, field: &str) -> f64 {
    value.get(field).and_then(Value::as_f64).unwrap_or_default()
}

fn json_size<T: Serialize>(value: &T) -> i32 {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len().min(i32::MAX as usize) as i32)
        .unwrap_or_default()
}

pub const STATUS_SUBMITTED: &str = "SUBMITTED";
pub const STATUS_QUEUED: &str = "JOB_STATE_QUEUED";
pub const STATUS_RUNNING: &str = "JOB_STATE_RUNNING";
pub const STATUS_SUCCEEDED: &str = "JOB_STATE_SUCCEEDED";
pub const STATUS_FAILED: &str = "JOB_STATE_FAILED";
pub const STATUS_CANCELED: &str = "JOB_STATE_CANCELED";
pub const STATUS_CANCELLED: &str = "JOB_STATE_CANCELLED";

const KNOWN_STATUSES: &[&str] = &[
    STATUS_SUBMITTED,
    STATUS_QUEUED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    STATUS_FAILED,
    STATUS_CANCELED,
    STATUS_CANCELLED,
    "PENDING",
    "QUEUED",
    "RUNNING",
    "PROCESSING",
    "COMPLETED",
    "SUCCEEDED",
    "SUCCESS",
    "FAILED",
    "CANCELED",
    "CANCELLED",
];

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct DiscoveryJobEnvelope {
    /// Nested job payload.
    #[serde(default)]
    pub job: Option<DiscoveryJob>,
    /// Top-level ID.
    #[serde(default)]
    pub id: String,
    /// Top-level job ID.
    #[serde(default)]
    pub job_id: String,
    /// Top-level status.
    #[serde(default)]
    pub status: String,
    /// Top-level state.
    #[serde(default)]
    pub state: String,
    /// Top-level error.
    #[serde(default)]
    pub error: Option<Value>,
    /// Top-level result.
    #[serde(default)]
    pub result: Option<DiscoveryJobResult>,
}

impl DiscoveryJobEnvelope {
    #[must_use]
    pub fn resolve_job(&self) -> DiscoveryJob {
        if let Some(job) = &self.job {
            return DiscoveryJob {
                id: choose_string(&job.id, &self.id),
                job_id: choose_string(&job.job_id, &self.job_id),
                status: choose_string(&job.status, &self.status),
                state: choose_string(&job.state, &self.state),
                error: job.error.clone().or_else(|| self.error.clone()),
                result: job.result.clone().or_else(|| self.result.clone()),
            };
        }
        DiscoveryJob {
            id: self.id.clone(),
            job_id: self.job_id.clone(),
            status: self.status.clone(),
            state: self.state.clone(),
            error: self.error.clone(),
            result: self.result.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct DiscoveryJob {
    /// Alternate ID.
    #[serde(default)]
    pub id: String,
    /// Job ID.
    #[serde(default)]
    pub job_id: String,
    /// Status.
    #[serde(default)]
    pub status: String,
    /// State.
    #[serde(default)]
    pub state: String,
    /// Error payload.
    #[serde(default)]
    pub error: Option<Value>,
    /// Result payload.
    #[serde(default)]
    pub result: Option<DiscoveryJobResult>,
}

impl DiscoveryJob {
    #[must_use]
    pub fn resolved_id(&self) -> String {
        if !self.job_id.trim().is_empty() {
            self.job_id.trim().to_owned()
        } else {
            self.id.trim().to_owned()
        }
    }

    #[must_use]
    pub fn resolved_status(&self) -> String {
        if !self.state.trim().is_empty() {
            self.state.trim().to_owned()
        } else {
            self.status.trim().to_owned()
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct DiscoveryJobResult {
    /// HTTP response payload.
    #[serde(default)]
    pub response: Option<DiscoveryJobResponse>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct DiscoveryJobResponse {
    /// HTTP status code.
    #[serde(default)]
    pub status_code: u16,
    /// Base64-encoded body.
    #[serde(default)]
    pub body: String,
    /// Content type.
    #[serde(default)]
    pub content_type: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DiscoveryJobStatus {
    /// Job ID.
    pub job_id: String,
    /// Raw status.
    pub status: String,
    /// Message.
    pub message: String,
    /// Upstream status code.
    pub status_code: u16,
    /// Raw decoded body.
    pub raw_body: String,
    /// Decoded response.
    pub response: Option<Value>,
}

impl DiscoveryJobStatus {
    fn as_update_with_status(&self, normalized: &str) -> StatusUpdate {
        StatusUpdate {
            job_id: self.job_id.clone(),
            status: normalized.to_owned(),
            message: self.message.clone(),
            http_status: self.status_code,
            ..StatusUpdate::default()
        }
    }

    fn completion_result(&self) -> CompletionResult {
        CompletionResult {
            job_id: self.job_id.clone(),
            status_code: self.status_code,
            raw_body: self.raw_body.clone(),
            response: self.response.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AifarmClientConfig {
    /// Direct OpenAI-compatible URL.
    pub direct_url: String,
    /// API key.
    pub api_key: String,
    /// Discovery base URL.
    pub base_url: String,
    /// Discovery service name.
    pub service_name: String,
    /// Discovery endpoint name.
    pub endpoint_name: String,
    /// HTTP request timeout.
    pub request_timeout: StdDuration,
    /// Discovery poll interval.
    pub poll_interval: StdDuration,
    /// Upstream task timeout.
    pub task_timeout: StdDuration,
    /// Capacity wait.
    pub capacity_wait: StdDuration,
    /// Capacity poll interval.
    pub capacity_poll_interval: StdDuration,
    /// Default model.
    pub default_model: String,
    /// Discovery priority.
    pub priority: i32,
    /// Workload label.
    pub workload: String,
    /// Fail fast on capacity errors.
    pub fail_fast_on_capacity_unavailable: bool,
}

impl Default for AifarmClientConfig {
    fn default() -> Self {
        Self {
            direct_url: String::new(),
            api_key: String::new(),
            base_url: String::new(),
            service_name: String::new(),
            endpoint_name: String::new(),
            request_timeout: StdDuration::ZERO,
            poll_interval: StdDuration::ZERO,
            task_timeout: StdDuration::ZERO,
            capacity_wait: StdDuration::ZERO,
            capacity_poll_interval: StdDuration::ZERO,
            default_model: String::new(),
            priority: 0,
            workload: String::new(),
            fail_fast_on_capacity_unavailable: false,
        }
    }
}

impl AifarmClientConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.base_url = default_string(&self.base_url, DEFAULT_AIFARM_BASE_URL);
        self.service_name = default_string(&self.service_name, DEFAULT_SERVICE_NAME);
        self.endpoint_name = default_string(&self.endpoint_name, DEFAULT_ENDPOINT_NAME);
        self.request_timeout = default_duration(self.request_timeout, StdDuration::from_secs(30));
        self.poll_interval = default_duration(self.poll_interval, StdDuration::from_secs(1));
        self.task_timeout = default_duration(self.task_timeout, StdDuration::from_secs(12 * 60));
        self.capacity_wait = default_duration(self.capacity_wait, StdDuration::from_secs(60));
        self.capacity_poll_interval =
            default_duration(self.capacity_poll_interval, StdDuration::from_secs(1));
        if self.direct_url.trim().is_empty()
            && self.request_timeout <= self.capacity_wait.saturating_mul(2)
        {
            self.request_timeout = self.capacity_wait.saturating_mul(2);
        }
        self.default_model = default_string(&self.default_model, DEFAULT_MODEL_NAME);
        self.workload = default_string(&self.workload, AIFARM_WORKLOAD_DIALOG);
        self.priority = self.priority.max(0);
        self.api_key = self.api_key.trim().to_owned();
        self.direct_url = self.direct_url.trim().to_owned();
        self
    }

    #[must_use]
    pub fn authorization_value(&self) -> String {
        let api_key = self.api_key.trim();
        if api_key.is_empty() {
            String::new()
        } else {
            format!("Bearer {api_key}")
        }
    }

    #[must_use]
    pub fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DiscoveryJobRequest {
    /// Invocation payload.
    pub invocation: DiscoveryInvocation,
    /// Idempotency key.
    pub idempotency_key: String,
    /// Discovery priority.
    pub priority: i32,
    /// Capacity wait in ms.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub wait_for_capacity_ms: i32,
    /// Capacity poll in ms.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub capacity_poll_ms: i32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DiscoveryInvocation {
    /// Service name.
    pub service_name: String,
    /// Endpoint name.
    pub endpoint_name: String,
    /// Invocation headers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Invocation query.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub query: BTreeMap<String, String>,
    /// Base64 JSON body.
    pub body: String,
    /// Content type.
    pub content_type: String,
    /// Upstream timeout in ms.
    pub timeout_ms: i32,
}

/// AIFarm tool prompt mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ToolPromptMode {
    /// Tools unavailable.
    #[default]
    None,
    /// Native OpenAI-compatible tools available.
    Native,
}

impl ToolPromptMode {
    fn as_prompt_value(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Native => "native",
        }
    }

    fn has_tools(self) -> bool {
        matches!(self, Self::Native)
    }
}

pub fn build_initial_messages_with_options(
    input: &DialogInput,
    history: &[HistoryMessage],
    include_tools: bool,
) -> Result<Vec<ChatMessage>, AifarmMessageError> {
    let mode = if include_tools {
        ToolPromptMode::Native
    } else {
        ToolPromptMode::None
    };
    build_initial_messages_with_tool_prompt(input, history, mode)
}

pub fn build_initial_messages_with_tool_prompt(
    input: &DialogInput,
    history: &[HistoryMessage],
    mode: ToolPromptMode,
) -> Result<Vec<ChatMessage>, AifarmMessageError> {
    let mut messages = Vec::with_capacity(history.len() + 2);
    messages.push(ChatMessage {
        role: "system".to_owned(),
        content: build_system_prompt_with_tool_prompt(input, mode)?,
        content_parts: Vec::new(),
        ..ChatMessage::default()
    });
    messages.push(ChatMessage {
        role: "user".to_owned(),
        content: build_runtime_context(input),
        content_parts: Vec::new(),
        ..ChatMessage::default()
    });

    for turn in history {
        let is_current =
            turn.role == ROLE_USER && turn.message_id != 0 && turn.message_id == input.message.id;
        if let Some(message) = format_history_message(turn, is_current, &input.multimodal_images)? {
            messages.push(message);
        }
    }
    Ok(messages)
}

pub fn build_system_prompt_with_tool_prompt(
    input: &DialogInput,
    mode: ToolPromptMode,
) -> Result<String, AifarmMessageError> {
    let locale = input.context.locale.trim();
    let rendered = openplotva_prompts::render(
        "aifarm/system",
        &json!({
            "toolMode": mode.as_prompt_value(),
            "hasTools": mode.has_tools(),
            "guestMode": input.guest_mode,
            "toolCatalog": render_alternative_tool_catalog(),
            "locale": xml_text(locale),
        }),
    )?;
    Ok(rendered.trim().to_owned())
}

#[must_use]
pub fn render_alternative_tool_catalog() -> String {
    let names = alternative_dialog_tool_names();
    let tools = alternative_dialog_tools()
        .into_iter()
        .filter(|spec| names.contains(&spec.name))
        .collect::<Vec<_>>();
    render_tool_catalog(&tools)
}

#[must_use]
pub fn normalize_chat_completions_url(raw_url: &str) -> String {
    let value = raw_url.trim();
    if value.is_empty() {
        return String::new();
    }
    match Url::parse(value) {
        Ok(mut parsed) => {
            let trimmed_path = parsed.path().trim_end_matches('/').to_owned();
            if trimmed_path.ends_with("/chat/completions") {
                parsed.set_path(&trimmed_path);
                return parsed.to_string();
            }
            if trimmed_path.is_empty() {
                parsed.set_path("/v1/chat/completions");
            } else {
                parsed.set_path(&format!("{trimmed_path}/chat/completions"));
            }
            parsed.to_string()
        }
        Err(_) if !value.contains("://") => normalize_relative_chat_completions_url(value),
        Err(_) => value.to_owned(),
    }
}

#[must_use]
pub fn direct_compatible_request(
    request: &ChatCompletionRequest,
    model: &str,
) -> ChatCompletionRequest {
    let mut out = request.clone();
    out.model = model.trim().to_owned();
    out.max_tokens = DEFAULT_VRAM_CLOUD_MAX_TOKENS;
    out.temperature = Some(DEFAULT_VRAM_CLOUD_TEMPERATURE);
    out.top_p = Some(DEFAULT_VRAM_CLOUD_TOP_P);
    out.top_k = Some(DEFAULT_VRAM_CLOUD_TOP_K);
    out.repeat_penalty = None;
    out.repetition_penalty = Some(DEFAULT_VRAM_CLOUD_REPETITION_PENALTY);
    out.dry_multiplier = None;
    out.dry_base = None;
    out.dry_allowed_length = 0;
    out.frequency_penalty = None;
    out.presence_penalty = Some(DEFAULT_VRAM_CLOUD_PRESENCE_PENALTY);
    out.include_reasoning = None;
    out.chat_template_kwargs = request.chat_template_kwargs.clone();
    out
}

pub fn direct_chat_completion_body(
    request: &ChatCompletionRequest,
) -> Result<Vec<u8>, AifarmRequestError> {
    serde_json::to_vec(request).map_err(AifarmRequestError::DirectChatCompletionRequest)
}

#[must_use]
pub fn direct_completion_headers(
    cfg: &AifarmClientConfig,
    request: &ChatCompletionRequest,
) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    headers.insert("Content-Type".to_owned(), "application/json".to_owned());
    headers.insert(
        "Accept".to_owned(),
        if request.stream {
            "text/event-stream"
        } else {
            "application/json"
        }
        .to_owned(),
    );
    let auth = cfg.authorization_value();
    if !auth.is_empty() {
        headers.insert("Authorization".to_owned(), auth);
    }
    headers
}

#[must_use]
pub fn direct_status_headers(cfg: &AifarmClientConfig) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    headers.insert("Accept".to_owned(), "application/json".to_owned());
    let auth = cfg.authorization_value();
    if !auth.is_empty() {
        headers.insert("Authorization".to_owned(), auth);
    }
    headers
}

#[must_use]
pub fn is_capacity_unavailable(status_code: u16, raw_body: &str) -> bool {
    matches!(status_code, 429 | 503)
        && contains_any_ascii_fold(&parse_response_error(raw_body), &["capacity", "slot"])
}

#[must_use]
pub fn parse_response_error(raw_body: &str) -> String {
    let body = raw_body.trim();
    if body.is_empty() {
        return String::new();
    }
    match body.as_bytes()[0] {
        b'"' => serde_json::from_str::<String>(body)
            .map(|value| value.trim().to_owned())
            .unwrap_or_else(|_| body.to_owned()),
        b'{' => serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|value| parse_response_error_value(&value))
            .unwrap_or_else(|| body.to_owned()),
        _ => body.to_owned(),
    }
}

#[must_use]
pub fn direct_error_message(raw_body: &str, http_status: &str) -> String {
    let message = parse_response_error(raw_body);
    if !message.is_empty() {
        return message;
    }
    if !raw_body.is_empty() {
        return raw_body.to_owned();
    }
    http_status.to_owned()
}

#[must_use]
pub fn parse_direct_request_id_body(raw_body: &str) -> String {
    let body = raw_body.trim();
    if body.is_empty() {
        return String::new();
    }

    match body.as_bytes()[0] {
        b'"' => serde_json::from_str::<String>(body)
            .map(|value| value.trim().to_owned())
            .unwrap_or_default(),
        b'{' => serde_json::from_str::<Value>(body)
            .ok()
            .and_then(|value| direct_request_id_from_value(&value))
            .unwrap_or_default(),
        _ => String::new(),
    }
}

pub fn resolve_direct_request_id(
    headers: &BTreeMap<String, String>,
    raw_body: &str,
) -> Result<String, AifarmRequestError> {
    for header in ["NVCF-REQID", "X-Request-ID", "X-Request-Id"] {
        if let Some(value) = header_value(headers, header).map(str::trim)
            && !value.is_empty()
        {
            return Ok(value.to_owned());
        }
    }
    let request_id = parse_direct_request_id_body(raw_body);
    if request_id.is_empty() {
        Err(AifarmRequestError::DirectRequestIdMissing)
    } else {
        Ok(request_id)
    }
}

fn header_value<'a>(headers: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

#[must_use]
pub fn direct_status_endpoint(direct_url: &str, request_id: &str) -> String {
    let request_id = request_id.trim();
    match Url::parse(direct_url) {
        Ok(mut parsed) if !parsed.scheme().is_empty() && parsed.host_str().is_some() => {
            parsed.set_path(&format!("/v1/status/{request_id}"));
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        }
        _ => format!("{}/status/{}", direct_url.trim_end_matches('/'), request_id),
    }
}

#[must_use]
pub fn request_with_default_model(
    request: &ChatCompletionRequest,
    default_model: &str,
) -> ChatCompletionRequest {
    let mut out = request.clone();
    if out.model.trim().is_empty() {
        out.model = default_model.to_owned();
    }
    out
}

#[must_use]
pub fn invocation_headers(cfg: &AifarmClientConfig, job_id: &str) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    headers.insert("X-Request-Id".to_owned(), job_id.to_owned());
    let workload = cfg.workload.trim();
    if !workload.is_empty() {
        headers.insert("X-AIFarm-Workload".to_owned(), workload.to_owned());
    }
    let auth = cfg.authorization_value();
    if !auth.is_empty() {
        headers.insert("Authorization".to_owned(), auth);
    }
    headers
}

pub fn build_discovery_job_request(
    cfg: &AifarmClientConfig,
    job_id: &str,
    request: &ChatCompletionRequest,
) -> Result<DiscoveryJobRequest, AifarmRequestError> {
    let request = request_with_default_model(request, &cfg.default_model);
    let payload =
        serde_json::to_vec(&request).map_err(AifarmRequestError::ChatCompletionRequest)?;
    build_discovery_json_payload_job_request(cfg, job_id, &payload)
}

pub fn build_discovery_json_job_request(
    cfg: &AifarmClientConfig,
    job_id: &str,
    request: &Value,
) -> Result<DiscoveryJobRequest, AifarmRequestError> {
    let payload = serde_json::to_vec(request).map_err(AifarmRequestError::ChatCompletionRequest)?;
    build_discovery_json_payload_job_request(cfg, job_id, &payload)
}

fn build_discovery_json_payload_job_request(
    cfg: &AifarmClientConfig,
    job_id: &str,
    payload: &[u8],
) -> Result<DiscoveryJobRequest, AifarmRequestError> {
    Ok(DiscoveryJobRequest {
        invocation: DiscoveryInvocation {
            service_name: cfg.service_name.clone(),
            endpoint_name: cfg.endpoint_name.clone(),
            headers: invocation_headers(cfg, job_id),
            query: BTreeMap::new(),
            body: general_purpose::STANDARD.encode(payload),
            content_type: "application/json".to_owned(),
            timeout_ms: timeout_ms(cfg.task_timeout),
        },
        idempotency_key: job_id.to_owned(),
        priority: cfg.priority.max(0),
        wait_for_capacity_ms: duration_ms(cfg.capacity_wait),
        capacity_poll_ms: duration_ms(cfg.capacity_poll_interval),
    })
}

#[must_use]
pub fn normalize_status(status: &str) -> String {
    let status = status.trim();
    for known in KNOWN_STATUSES {
        if status.eq_ignore_ascii_case(known) {
            return (*known).to_owned();
        }
    }
    status.to_uppercase()
}

#[must_use]
pub fn is_queued_status(status: &str) -> bool {
    matches!(
        normalize_status(status).as_str(),
        STATUS_QUEUED | "PENDING" | "QUEUED"
    )
}

#[must_use]
pub fn is_running_status(status: &str) -> bool {
    matches!(
        normalize_status(status).as_str(),
        STATUS_RUNNING | "RUNNING" | "PROCESSING"
    )
}

#[must_use]
pub fn is_success_status(status: &str) -> bool {
    matches!(
        normalize_status(status).as_str(),
        STATUS_SUCCEEDED | "COMPLETED" | "SUCCEEDED" | "SUCCESS"
    )
}

#[must_use]
pub fn is_failure_status(status: &str) -> bool {
    matches!(
        normalize_status(status).as_str(),
        STATUS_FAILED | STATUS_CANCELED | STATUS_CANCELLED | "FAILED" | "CANCELED" | "CANCELLED"
    )
}

#[must_use]
pub fn parse_job_error(raw: Option<&Value>) -> String {
    let Some(raw) = raw else {
        return String::new();
    };
    match raw {
        Value::String(text) => text.trim().to_owned(),
        Value::Object(map) => {
            for key in ["message", "error", "detail", "code"] {
                let Some(Value::String(text)) = map.get(key) else {
                    continue;
                };
                if !text.trim().is_empty() {
                    return text.trim().to_owned();
                }
            }
            raw.to_string()
        }
        _ => raw.to_string(),
    }
    .trim()
    .to_owned()
}

pub fn decode_chat_completion_response(
    result: Option<&DiscoveryJobResult>,
) -> Result<CompletionResult, AifarmDecodeError> {
    let Some(response) = result.and_then(|result| result.response.as_ref()) else {
        return Ok(CompletionResult::default());
    };
    if response.body.is_empty() {
        return Ok(CompletionResult {
            status_code: response.status_code,
            ..CompletionResult::default()
        });
    }

    let payload = decode_discovery_body(&response.body)?;
    let body_text = String::from_utf8_lossy(&payload).trim().to_owned();
    if body_text.is_empty() {
        return Ok(CompletionResult {
            status_code: response.status_code,
            ..CompletionResult::default()
        });
    }
    if response.status_code >= 400 {
        return Ok(CompletionResult {
            status_code: response.status_code,
            raw_body: body_text,
            ..CompletionResult::default()
        });
    }

    let parsed = serde_json::from_slice::<Value>(&payload)
        .map_err(AifarmDecodeError::ChatCompletionPayload)?;
    Ok(CompletionResult {
        status_code: response.status_code,
        raw_body: body_text,
        response: Some(parsed),
        ..CompletionResult::default()
    })
}

pub fn decode_discovery_body(body: &str) -> Result<Vec<u8>, base64::DecodeError> {
    let body = body.trim();
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let encodings = [
        general_purpose::STANDARD,
        general_purpose::URL_SAFE,
        general_purpose::STANDARD_NO_PAD,
        general_purpose::URL_SAFE_NO_PAD,
    ];
    let mut first_error = None;
    for encoding in encodings {
        match encoding.decode(body) {
            Ok(payload) => return Ok(payload),
            Err(err) if first_error.is_none() => first_error = Some(err),
            Err(_) => {}
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(Vec::new()),
    }
}

pub fn job_status_from_envelope(
    fallback_job_id: &str,
    envelope: &DiscoveryJobEnvelope,
) -> Result<StatusUpdate, AifarmDecodeError> {
    let status = discovery_status_from_envelope(fallback_job_id, envelope)?;
    Ok(status.as_update_with_status(&normalize_status(&status.status)))
}

pub fn discovery_status_from_envelope(
    fallback_job_id: &str,
    envelope: &DiscoveryJobEnvelope,
) -> Result<DiscoveryJobStatus, AifarmDecodeError> {
    let job = envelope.resolve_job();
    let mut status = DiscoveryJobStatus {
        job_id: fallback_string(&job.resolved_id(), fallback_job_id),
        status: job.resolved_status(),
        message: parse_job_error(job.error.as_ref()).trim().to_owned(),
        ..DiscoveryJobStatus::default()
    };

    let result = decode_chat_completion_response(job.result.as_ref())?;
    status.status_code = result.status_code;
    status.raw_body = result.raw_body.clone();
    status.response = result.response.clone();
    if status.message.is_empty() && result.status_code >= 400 && !status.raw_body.is_empty() {
        status.message = parse_response_error(&status.raw_body);
    }
    Ok(status)
}

fn choose_string(primary: &str, fallback: &str) -> String {
    if primary.is_empty() {
        fallback.to_owned()
    } else {
        primary.to_owned()
    }
}

fn discovery_result_from_status(
    job_id: &str,
    status: &DiscoveryJobStatus,
    normalized: &str,
) -> Result<Option<CompletionResult>, CompletionError> {
    if is_queued_status(normalized) || is_running_status(normalized) {
        return Ok(None);
    }
    if is_success_status(normalized) {
        if !(200..300).contains(&status.status_code) {
            return Err(Box::new(AifarmClientError::Upstream(
                discovery_upstream_error_message(status),
            )));
        }
        if status.response.is_none() {
            return Err(Box::new(AifarmClientError::EmptyDirectResponse(format!(
                "dialog job {job_id} succeeded but response body is empty"
            ))));
        }
        return Ok(Some(status.completion_result()));
    }
    if is_failure_status(normalized) {
        return Err(Box::new(AifarmClientError::Upstream(
            failed_discovery_status_error(status),
        )));
    }
    if status.response.is_some() && (200..300).contains(&status.status_code) {
        return Ok(Some(status.completion_result()));
    }
    Err(Box::new(AifarmClientError::UnknownStatus(
        status.status.clone(),
    )))
}

fn discovery_upstream_error_message(status: &DiscoveryJobStatus) -> String {
    let message = status.message.trim();
    if message.is_empty() {
        format!("upstream returned status {}", status.status_code)
    } else {
        format!("upstream returned status {}: {message}", status.status_code)
    }
}

fn failed_discovery_status_error(status: &DiscoveryJobStatus) -> String {
    let message = status.message.trim();
    if message.is_empty() {
        "dialog job failed".to_owned()
    } else {
        message.to_owned()
    }
}

fn direct_http_status_error(
    prefix: &str,
    response: &AifarmHttpResponse,
    raw_body: &str,
) -> Result<(), CompletionError> {
    if (200..300).contains(&response.status_code) {
        return Ok(());
    }
    let hints = direct_status_error_hints(response, raw_body);
    let suffix = if hints.is_empty() {
        String::new()
    } else {
        format!(" ({})", hints.join(" "))
    };
    Err(Box::new(AifarmClientError::Upstream(format!(
        "{prefix}: status {}: {}{}",
        response.status_code,
        direct_error_message(raw_body, &response.status_text),
        suffix
    ))))
}

fn direct_status_error_hints(response: &AifarmHttpResponse, raw_body: &str) -> Vec<String> {
    let mut hints = Vec::new();
    if let Some(retry_after) = header_value(&response.headers, "retry-after")
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        hints.push(format!("retry_after_seconds={retry_after}"));
    }
    if let Ok(value) = serde_json::from_str::<Value>(raw_body)
        && let Some(error_type) = value
            .get("error")
            .and_then(|error| error.get("metadata"))
            .and_then(|metadata| metadata.get("error_type"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
    {
        hints.push(format!("openrouter_error_type={}", error_type.trim()));
    }
    hints
}

fn decode_direct_completion_payload(
    body: &[u8],
    raw_body: &str,
    empty_err: &str,
    decode_err: &str,
) -> Result<Value, CompletionError> {
    if raw_body.trim().is_empty() {
        return Err(Box::new(AifarmClientError::EmptyDirectResponse(
            empty_err.to_owned(),
        )));
    }
    serde_json::from_slice::<Value>(body).map_err(|err| {
        Box::new(AifarmClientError::EmptyDirectResponse(format!(
            "{decode_err}: {err}"
        ))) as CompletionError
    })
}

fn response_body_text(response: &AifarmHttpResponse) -> String {
    String::from_utf8_lossy(&response.body).trim().to_owned()
}

enum ToolStepSelection {
    None(ToolParseDecision),
    Steps {
        steps: Vec<PendingToolStep>,
        text: String,
        residual_protocol: bool,
    },
}

struct PendingToolStep {
    step: ToolStep,
    decision: ToolParseDecision,
    native_ref: Option<String>,
}

fn dialog_response_semantic_error(
    response: Option<&Value>,
    provider: &str,
    tools_offered: bool,
) -> Option<String> {
    let Some(response) = response else {
        return Some("chat completion response is nil".to_owned());
    };
    if !tools_offered {
        return extract_final_answer_for_provider(response, provider)
            .err()
            .map(|error| error.to_string());
    }
    match first_choice_tool_steps(response) {
        Err(error) => Some(error.to_string()),
        Ok(ToolStepSelection::None(_)) => extract_final_answer_for_provider(response, provider)
            .err()
            .map(|error| error.to_string()),
        Ok(ToolStepSelection::Steps {
            residual_protocol: true,
            ..
        }) => Some(
            tool_protocol_completion_error("assistant content retained ambiguous protocol markup")
                .to_string(),
        ),
        Ok(ToolStepSelection::Steps { .. }) => None,
    }
}

fn first_choice_tool_steps(response: &Value) -> Result<ToolStepSelection, CompletionError> {
    let message = first_choice_message_value(response)?;
    if let Some(tool_calls) = message
        .get("tool_calls")
        .filter(|value| value.as_array().is_some_and(|calls| !calls.is_empty()))
    {
        let calls =
            serde_json::from_value::<Vec<NativeToolCall>>(tool_calls.clone()).map_err(|err| {
                tool_protocol_completion_error(format!("decode native tool calls: {err}"))
            })?;
        let content = message
            .get("content")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let parsed = parse_assistant_content(content)
            .map_err(|err| tool_protocol_completion_error(err.to_string()))?;
        let mut steps = Vec::with_capacity(calls.len() + parsed.tool_steps.len());
        for step in parsed
            .tool_steps
            .into_iter()
            .filter(|step| step.step == STEP_SEND_MESSAGE)
        {
            steps.push(PendingToolStep {
                decision: ToolParseDecision {
                    form: "content_preamble".to_owned(),
                    tool: step.step.clone(),
                    outcome: "detected".to_owned(),
                    reason: String::new(),
                },
                step,
                native_ref: None,
            });
        }
        let mut seen_ids = std::collections::BTreeSet::new();
        for call in calls {
            let native_ref = if call.id.trim().is_empty() || !seen_ids.insert(call.id.clone()) {
                Some(String::new())
            } else {
                Some(call.id.clone())
            };
            let decision = native_tool_parse_decision(std::slice::from_ref(&call), None);
            let step = parse_native_tool_step(&[call])
                .map_err(|err| tool_protocol_completion_error(err.to_string()))?;
            steps.push(PendingToolStep {
                step,
                decision,
                native_ref,
            });
        }
        return Ok(ToolStepSelection::Steps {
            steps,
            text: parsed.text,
            residual_protocol: parsed.residual_protocol,
        });
    }

    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let parsed = parse_assistant_content(content)
        .map_err(|err| tool_protocol_completion_error(err.to_string()))?;
    if !parsed.tool_steps.is_empty() {
        Ok(ToolStepSelection::Steps {
            steps: parsed
                .tool_steps
                .into_iter()
                .map(|step| {
                    let mut decision = parsed.decision.clone();
                    decision.tool = step.step.clone();
                    PendingToolStep {
                        step,
                        decision,
                        native_ref: None,
                    }
                })
                .collect(),
            text: parsed.text,
            residual_protocol: parsed.residual_protocol,
        })
    } else {
        Ok(ToolStepSelection::None(parsed.decision))
    }
}

fn tool_protocol_completion_error(message: impl Into<String>) -> CompletionError {
    Box::new(AifarmDialogError::Response(format!(
        "tool protocol error: {}",
        message.into()
    ))) as CompletionError
}

fn native_tool_parse_decision(
    calls: &[NativeToolCall],
    reason: Option<String>,
) -> ToolParseDecision {
    let tool = if calls.len() == 1 {
        calls[0].function.name.trim().to_owned()
    } else {
        String::new()
    };
    if let Some(reason) = reason {
        return ToolParseDecision {
            form: "native".to_owned(),
            tool,
            outcome: "error".to_owned(),
            reason,
        };
    }
    ToolParseDecision {
        form: "native".to_owned(),
        tool,
        outcome: "detected".to_owned(),
        reason: String::new(),
    }
}

fn extract_final_answer_for_provider(
    response: &Value,
    provider: &str,
) -> Result<String, CompletionError> {
    match extract_final_answer(response) {
        Ok(answer) => Ok(answer),
        Err(err) if is_retryable_final_answer_error(&err) => Err(Box::new(ProviderError::new(
            provider,
            FailureReason::ProviderProtocolError,
            err.to_string(),
        ))),
        Err(err) => Err(Box::new(err)),
    }
}

fn extract_final_answer(response: &Value) -> Result<String, AifarmDialogError> {
    let content = first_choice_content(response)?;
    let content = legacy_final_response_answer(&content).unwrap_or(content);
    match openplotva_dialog::finalize_dialog_reply(&content) {
        openplotva_dialog::DialogReplyOutcome::Reply(answer) => Ok(answer),
        openplotva_dialog::DialogReplyOutcome::Suppressed(reason) => {
            Err(final_answer_error_from_suppression(reason))
        }
    }
}

fn final_answer_error_from_suppression(
    reason: openplotva_dialog::DialogReplySuppression,
) -> AifarmDialogError {
    use openplotva_dialog::DialogReplySuppression as Suppression;
    match reason {
        Suppression::ContextLeak => AifarmDialogError::FinalAnswerContextLeak,
        Suppression::Pathological(reason) => AifarmDialogError::FinalAnswerPathological(reason),
        Suppression::Empty | Suppression::ProtocolOnly | Suppression::ReasoningLeak => {
            AifarmDialogError::FinalAnswerProtocolOnly
        }
    }
}

fn first_choice_content(response: &Value) -> Result<String, AifarmDialogError> {
    let message = first_choice_message_value(response)?;
    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if content.is_empty() {
        // Reasoning output must never be promoted into the answer, but a
        // reasoning-only completion is a budget problem, not a protocol one:
        // classify it so retries reselect a backend with headroom.
        let reasoning_chars = first_choice_reasoning_len(message);
        if reasoning_chars > 0
            || first_choice_finish_reason(response).eq_ignore_ascii_case("length")
        {
            return Err(AifarmDialogError::ReasoningBudgetExhausted { reasoning_chars });
        }
        return Err(AifarmDialogError::Response(
            "chat completion returned empty final text".to_owned(),
        ));
    }
    Ok(content)
}

fn first_choice_reasoning_len(message: &Value) -> usize {
    ["reasoning_content", "reasoning"]
        .iter()
        .filter_map(|key| message.get(key).and_then(Value::as_str))
        .map(|text| text.trim().len())
        .max()
        .unwrap_or(0)
}

fn first_choice_finish_reason(response: &Value) -> &str {
    response
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn first_choice_message_value(response: &Value) -> Result<&Value, AifarmDialogError> {
    let Some(choices) = response.get("choices").and_then(Value::as_array) else {
        return Err(AifarmDialogError::Response(
            "chat completion returned no choices".to_owned(),
        ));
    };
    let Some(choice) = choices.first() else {
        return Err(AifarmDialogError::Response(
            "chat completion returned no choices".to_owned(),
        ));
    };
    choice.get("message").ok_or_else(|| {
        AifarmDialogError::Response("chat completion returned no message".to_owned())
    })
}

fn summary_document_from_llm(
    model: &str,
    input: &SummaryInput,
    llm: &openplotva_history::HistorySummaryLlmResponse,
    system_prompt: &str,
) -> SummaryDocument {
    SummaryDocument {
        content: llm.summary_json.clone(),
        html: llm.summary_html.clone(),
        model: model.trim().to_owned(),
        prompt_version: openplotva_history::SUMMARY_PROMPT_VERSION.to_owned(),
        prompt_hash: hash_text(system_prompt),
        input_hash: input.input_hash.clone(),
        input_token_estimate: input.input_token_estimate,
        output_token_estimate: history_output_token_estimate(llm),
        cascade_depth: input.cascade_depth,
        quality_score: llm.summary_json.quality_score,
        quality_notes: llm.summary_json.quality_notes.clone(),
    }
}

fn decode_memory_extraction_output(
    response: &Value,
    payload: &str,
) -> Result<ExtractOutput, AifarmMemoryExtractorError> {
    let content = first_choice_content(response)
        .map_err(|err| AifarmMemoryExtractorError::Response(err.to_string()))?;
    let mut out = decode_extraction_json(&content)?;
    if out.input_tokens == 0 {
        out.input_tokens = response
            .get("usage")
            .and_then(|usage| usage.get("prompt_tokens"))
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default();
    }
    if out.output_tokens == 0 {
        out.output_tokens = response
            .get("usage")
            .and_then(|usage| usage.get("completion_tokens"))
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default();
    }
    if out.input_tokens == 0 {
        out.input_tokens = estimate_memory_tokens(payload);
    }
    if out.output_tokens == 0 {
        out.output_tokens = estimate_memory_tokens(&content);
    }
    Ok(out)
}

fn history_summary_response_format() -> Value {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": "chat_history_summary",
            "schema": history_summary_response_schema(),
        },
    })
}

fn history_summary_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["summary_json"],
        "properties": {
            "summary_json": history_summary_content_schema(),
        },
    })
}

fn subject_merge_response_format() -> Value {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": "memory_subject_merge",
            "schema": subject_merge_response_schema(),
        },
    })
}

fn subject_merge_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["clusters", "demote_ids", "keep_ids"],
        "properties": {
            "clusters": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["survivor_id", "absorbed_ids", "merged_fact_text"],
                    "properties": {
                        "survivor_id": {"type": "integer"},
                        "absorbed_ids": {"type": "array", "items": {"type": "integer"}},
                        "merged_fact_text": {"type": "string"},
                    },
                },
            },
            "demote_ids": {"type": "array", "items": {"type": "integer"}},
            "keep_ids": {"type": "array", "items": {"type": "integer"}},
        },
    })
}

fn memory_extraction_response_format() -> Value {
    json!({
        "type": "json_schema",
        "json_schema": {
            "name": "memory_extraction",
            "schema": memory_extraction_response_schema(),
        },
    })
}

fn memory_extraction_response_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "episode_summary",
            "topics",
            "participants",
            "candidate_cards",
            "supersessions",
            "links",
        ],
        "properties": {
            "episode_summary": {"type": "string"},
            "topics": {
                "type": "array",
                "items": {"type": "string"},
            },
            "participants": {
                "type": "array",
                "items": {"type": "string"},
            },
            "candidate_cards": {
                "type": "array",
                "items": memory_candidate_card_schema(),
            },
            "supersessions": {
                "type": "array",
                "items": memory_supersession_schema(),
            },
            "resolutions": {
                "type": "array",
                "items": memory_resolution_schema(),
            },
            "links": {
                "type": "array",
                "items": memory_link_schema(),
            },
        },
    })
}

fn memory_candidate_card_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "scope_type",
            "card_type",
            "subject",
            "predicate",
            "object",
            "fact_text",
            "confidence",
            "salience",
            "source_entry_ids",
            "source_message_ids",
        ],
        "properties": {
            "scope_type": {"type": "string", "enum": ["chat", "thread", "user"]},
            "user_id": {"type": "integer"},
            "card_type": {"type": "string"},
            "subject": {"type": "string"},
            "predicate": {"type": "string"},
            "object": {"type": "string"},
            "fact_text": {"type": "string"},
            "confidence": {"type": "number"},
            "salience": {"type": "number"},
            "durability": {"type": "string"},
            "portable": {"type": "boolean"},
            "source_entry_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "source_message_ids": {
                "type": "array",
                "items": {"type": "integer"},
            },
        },
    })
}

fn memory_supersession_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["old_card_id", "new_fact_text", "reason"],
        "properties": {
            "old_card_id": {"type": "integer"},
            "new_fact_text": {"type": "string"},
            "reason": {"type": "string"},
        },
    })
}

fn memory_resolution_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["old_card_id", "decision"],
        "properties": {
            "old_card_id": {"type": "integer"},
            "into_card_id": {"type": "integer"},
            "new_fact_text": {"type": "string"},
            "decision": {
                "type": "string",
                "enum": ["supersede", "competing", "update", "merge", "reinforce", "demote"],
            },
            "conflict_score": {"type": "number"},
            "reason": {"type": "string"},
        },
    })
}

fn memory_link_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["from_fact_text", "to_fact_text", "relation", "confidence"],
        "properties": {
            "from_fact_text": {"type": "string"},
            "to_fact_text": {"type": "string"},
            "relation": {"type": "string"},
            "confidence": {"type": "number"},
        },
    })
}

fn history_summary_content_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": [
            "events",
            "event_details",
            "actors",
            "recap",
            "open_questions",
            "source_style",
            "quality_score",
            "quality_notes",
        ],
        "properties": {
            "events": {
                "type": "array",
                "items": {"type": "string"},
            },
            "event_details": {
                "type": "array",
                "items": history_summary_event_schema(),
            },
            "actors": {
                "type": "array",
                "items": history_summary_actor_schema(),
            },
            "recap": {"type": "string"},
            "open_questions": {
                "type": "array",
                "items": {"type": "string"},
            },
            "source_style": {"type": "string"},
            "quality_score": {"type": "number"},
            "quality_notes": {"type": "string"},
        },
    })
}

fn history_summary_event_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["title", "description", "actors", "occurred_at", "confidence"],
        "properties": {
            "title": {"type": "string"},
            "description": {"type": "string"},
            "actors": {
                "type": "array",
                "items": {"type": "string"},
            },
            "occurred_at": {"type": "string"},
            "confidence": {"type": "number"},
        },
    })
}

fn history_summary_actor_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["name", "description"],
        "properties": {
            "name": {"type": "string"},
            "description": {"type": "string"},
        },
    })
}

fn legacy_final_response_answer(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || !raw.starts_with('{') {
        return None;
    }
    let decoded = decode_plotva_final_response_with_salvage(raw).ok()?;
    decoded
        .step
        .trim()
        .eq_ignore_ascii_case("final_response")
        .then(|| decoded.answer.trim().to_owned())
}

fn is_retryable_final_answer_error(err: &AifarmDialogError) -> bool {
    matches!(
        err,
        AifarmDialogError::FinalAnswerContextLeak
            | AifarmDialogError::FinalAnswerProtocolOnly
            | AifarmDialogError::FinalAnswerPathological(_)
            | AifarmDialogError::ReasoningBudgetExhausted { .. }
    )
}

fn canonical_provider_name(provider: &str) -> String {
    let provider = provider.trim();
    if provider.eq_ignore_ascii_case(PROVIDER_AIFARM) {
        PROVIDER_AIFARM.to_owned()
    } else if provider.eq_ignore_ascii_case(PROVIDER_NVIDIA) {
        PROVIDER_NVIDIA.to_owned()
    } else if provider.eq_ignore_ascii_case(PROVIDER_VMLX) {
        PROVIDER_VMLX.to_owned()
    } else {
        provider.to_lowercase()
    }
}

fn emit_status(on_status: &mut (dyn FnMut(StatusUpdate) + Send), status: StatusUpdate) {
    if status.status.trim().is_empty() {
        return;
    }
    on_status(status);
}

fn nonzero_duration(value: StdDuration, fallback: StdDuration) -> StdDuration {
    if value.is_zero() { fallback } else { value }
}

fn generated_dialog_job_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    format!("dialog-{nanos}")
}

fn default_string(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

fn clamp_penalty(value: f64, fallback: f64) -> f64 {
    if value.is_finite() {
        value.clamp(-2.0, 2.0)
    } else {
        fallback
    }
}

fn default_duration(value: StdDuration, fallback: StdDuration) -> StdDuration {
    if value.is_zero() { fallback } else { value }
}

fn duration_ms(value: StdDuration) -> i32 {
    if value.is_zero() {
        return 0;
    }
    let ms = value.as_millis();
    if ms == 0 { 1 } else { saturating_i32(ms) }
}

fn timeout_ms(value: StdDuration) -> i32 {
    if value.is_zero() {
        return DEFAULT_TIMEOUT_MS;
    }
    let ms = value.as_millis();
    if ms == 0 {
        DEFAULT_TIMEOUT_MS
    } else {
        saturating_i32(ms)
    }
}

fn saturating_i32(value: u128) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn render_tool_catalog(tools: &[ToolSpec]) -> String {
    let mut out = String::new();
    out.push_str("    <tools>\n");
    for spec in tools {
        out.push_str("      <tool name=\"");
        out.push_str(&xml_attr(spec.name));
        out.push_str("\">\n");
        out.push_str("        <summary>");
        out.push_str(&xml_text(spec.summary));
        out.push_str("</summary>\n");
        if !spec.when_to_use.trim().is_empty() {
            out.push_str("        <use_when>");
            out.push_str(&xml_text(spec.when_to_use));
            out.push_str("</use_when>\n");
        }
        if !spec.result.trim().is_empty() {
            out.push_str("        <result>");
            out.push_str(&xml_text(spec.result));
            out.push_str("</result>\n");
        }
        if !spec.args.is_empty() {
            out.push_str("        <args>\n");
            for arg in spec.args {
                out.push_str("          <arg name=\"");
                out.push_str(&xml_attr(arg.name));
                out.push_str("\" required=\"");
                out.push_str(if arg.required { "true" } else { "false" });
                out.push_str("\">");
                if !arg.description.trim().is_empty() {
                    out.push_str(&xml_text(arg.description));
                }
                out.push_str("</arg>\n");
            }
            out.push_str("        </args>\n");
        }
        out.push_str("      </tool>\n");
    }
    out.push_str("    </tools>\n");
    out
}

#[must_use]
pub fn build_runtime_context(input: &DialogInput) -> String {
    let mut out = String::new();
    out.push_str("<chat_context>\n");
    write_text_element(
        &mut out,
        "bot_name",
        &fallback_string(&input.context.bot_name, "Plotva"),
    );
    if let Some(thread_id) = input.context.thread_id.filter(|thread_id| *thread_id != 0) {
        write_int_element(&mut out, "  ", "thread_id", i64::from(thread_id));
    }
    write_text_element(
        &mut out,
        "chat_title",
        &fallback_string(&input.context.chat_title, "private chat"),
    );
    write_text_element(
        &mut out,
        "current_user",
        &fallback_string(&input.user.full_name, "unknown"),
    );
    write_text_element(
        &mut out,
        "locale",
        &fallback_string(&input.context.locale, "ru"),
    );
    write_text_element(
        &mut out,
        "mood",
        &fallback_string(&input.persona.mood, "neutral"),
    );
    write_runtime_persona(&mut out, input);
    let shield = input.shield_context.trim();
    if !shield.is_empty() {
        out.push_str("  ");
        out.push_str(shield);
        out.push('\n');
    }
    write_reference_context(&mut out, &input.reference_context);
    out.push_str("</chat_context>\n");
    out.trim().to_owned()
}

fn write_runtime_persona(out: &mut String, input: &DialogInput) {
    let custom = input.persona.custom_persona.trim();
    if !custom.is_empty() {
        write_text_element(out, "custom_persona", custom);
        return;
    }
    let Some(persona) = input.persona.persona.as_ref() else {
        return;
    };
    write_daily_persona_accent(out, persona);
}

fn write_daily_persona_accent(out: &mut String, persona: &openplotva_dialog::DailyPersona) {
    let name = persona.name.trim();
    // Surface the speech-style `tone` as the accent, not the behavioural `boundaries`:
    // tone distinguishes the day with a light vocal tint, whereas boundaries reads as
    // "what the character does" and pushes role-play into the answer itself.
    let accent = persona.tone.trim();
    if name.is_empty() && accent.is_empty() {
        return;
    }

    out.push_str("  <daily_persona_accent>\n");
    out.push_str("    ");
    write_inline_text_element(
        out,
        "instruction",
        "Слабая дневная окраска голоса. Возьми максимум одну мелкую чёрточку манеры и часто игнорируй её; отвечай по сути обычными словами, без отыгрыша роли.",
    );
    out.push('\n');
    if !name.is_empty() {
        out.push_str("    ");
        write_inline_text_element(out, "name", name);
        out.push('\n');
    }
    if !accent.is_empty() {
        out.push_str("    ");
        write_inline_text_element(out, "accent", accent);
        out.push('\n');
    }
    out.push_str("  </daily_persona_accent>\n");
}

fn write_reference_context(out: &mut String, chunks: &[String]) {
    if chunks.is_empty() {
        return;
    }
    out.push_str("  <reference_context>\n");
    for (idx, chunk) in chunks.iter().enumerate() {
        let chunk = chunk.trim();
        if chunk.is_empty() {
            continue;
        }
        out.push_str("    <chunk index=\"");
        let _ = write!(out, "{}", idx + 1);
        out.push_str("\">");
        out.push_str(&xml_text(chunk));
        out.push_str("</chunk>\n");
    }
    out.push_str("  </reference_context>\n");
}

#[must_use]
pub fn build_session_history_with_limit(
    input: &DialogInput,
    max_history: usize,
) -> Vec<HistoryMessage> {
    let history_limit = max_history.saturating_sub(1);
    let thread_id = input.context.thread_id.unwrap_or_default();
    let selected = select_llm_history_messages_for_context(
        &input.history,
        history_limit,
        input.message.id,
        thread_id,
    );
    let mut history = Vec::with_capacity(selected.len() + 1);
    for turn in clone_history_messages(&selected) {
        if turn
            .tool_call
            .as_ref()
            .is_some_and(|tool_call| is_dialog_history_noise_tool_call_name(&tool_call.name))
        {
            continue;
        }
        history.push(turn);
    }
    history.push(HistoryMessage {
        role: ROLE_USER.to_owned(),
        kind: MESSAGE_KIND_TEXT.to_owned(),
        name: input.user.full_name.trim().to_owned(),
        text: input.message.text.clone(),
        original_text: input.message.original_text.clone(),
        timestamp: input.message.timestamp,
        message_id: input.message.id,
        thread_id,
        user_id: input.user.id,
        reply_to_id: input.message.reply_to_id,
        reply_to_name: input.message.reply_to_name.clone(),
        meta: input.message.meta.clone(),
        ..HistoryMessage::default()
    });
    history
}

pub fn build_default_initial_messages(
    input: &DialogInput,
) -> Result<Vec<ChatMessage>, AifarmMessageError> {
    let history = build_session_history_with_limit(input, DEFAULT_CONTEXT_HISTORY_LIMIT);
    build_initial_messages_with_options(input, &history, true)
}

pub fn format_history_message(
    turn: &HistoryMessage,
    is_current: bool,
    images: &[openplotva_dialog::MultimodalImage],
) -> Result<Option<ChatMessage>, AifarmMessageError> {
    let normalized = normalize_history_message(turn.clone());
    match normalized.kind.as_str() {
        MESSAGE_KIND_TOOL_REQUEST => Ok(None),
        MESSAGE_KIND_TOOL_RESPONSE => {
            let content = format_stored_tool_response_message(&normalized)?;
            if content.is_empty() {
                return Ok(None);
            }
            Ok(Some(ChatMessage {
                role: "user".to_owned(),
                content,
                content_parts: Vec::new(),
                ..ChatMessage::default()
            }))
        }
        _ => {
            if normalized.role == ROLE_MODEL {
                let Some(sanitized) = sanitize_assistant_history_turn(normalized) else {
                    return Ok(None);
                };
                let content = sanitized.text.trim().to_owned();
                if content.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(ChatMessage {
                    role: "assistant".to_owned(),
                    content,
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                }));
            }
            let content = format_turn(&normalized, is_current)?;
            let content_parts = if is_current {
                multimodal_content_parts(&content, images)
            } else {
                Vec::new()
            };
            Ok(Some(ChatMessage {
                role: "user".to_owned(),
                content,
                content_parts,
                ..ChatMessage::default()
            }))
        }
    }
}

fn multimodal_content_parts(
    text: &str,
    images: &[openplotva_dialog::MultimodalImage],
) -> Vec<ChatContentPart> {
    if images.is_empty() {
        return Vec::new();
    }
    let mut parts = Vec::with_capacity(images.len() + 1);
    for image in images {
        let data_url = image.data_url.trim();
        if data_url.is_empty() {
            continue;
        }
        parts.push(ChatContentPart {
            part_type: "image_url".to_owned(),
            text: String::new(),
            image_url: Some(ChatImageUrlPart {
                url: data_url.to_owned(),
                detail: "auto".to_owned(),
            }),
            video_url: None,
        });
    }
    if parts.is_empty() {
        return Vec::new();
    }
    parts.push(ChatContentPart {
        part_type: "text".to_owned(),
        text: text.trim().to_owned(),
        image_url: None,
        video_url: None,
    });
    parts
}

fn sanitize_assistant_history_turn(mut turn: HistoryMessage) -> Option<HistoryMessage> {
    let text = openplotva_dialog::sanitize_assistant_text(&turn.text);
    let original_text = openplotva_dialog::sanitize_assistant_text(&turn.original_text);
    if text.residual_protocol || original_text.residual_protocol {
        return None;
    }
    turn.text = text.text;
    turn.original_text = original_text.text;
    if turn.text.trim().is_empty() && turn.original_text.trim().is_empty() {
        return None;
    }
    Some(turn)
}

fn format_turn(turn: &HistoryMessage, is_current: bool) -> Result<String, AifarmMessageError> {
    let message = format_message_body(turn);
    if message.is_empty() {
        return Ok(String::new());
    }
    if !is_current {
        return Ok(message);
    }
    let rendered = openplotva_prompts::render(
        "aifarm/last_message_wrapper",
        &json!({ "message": message }),
    )?;
    Ok(rendered.trim().to_owned())
}

fn format_stored_tool_response_message(
    turn: &HistoryMessage,
) -> Result<String, AifarmMessageError> {
    let Some(tool_call) = turn.tool_call.as_ref() else {
        return Ok(String::new());
    };
    let name = tool_call.name.trim();
    if name.is_empty() {
        return Ok(String::new());
    }
    let mut payload = serde_json::Map::new();
    payload.insert("tool".to_owned(), Value::String(name.to_owned()));
    if !tool_call.r#ref.trim().is_empty() {
        payload.insert(
            "ref".to_owned(),
            Value::String(tool_call.r#ref.trim().to_owned()),
        );
    }
    if let Some(output) = tool_call.output.clone() {
        payload.insert("output".to_owned(), output);
    }
    let encoded = serde_json::to_string(&payload)?;

    let mut out = String::new();
    out.push_str("<tool_result name=\"");
    out.push_str(&xml_attr(name));
    out.push('"');
    if !tool_call.r#ref.trim().is_empty() {
        out.push_str(" ref=\"");
        out.push_str(&xml_attr(&tool_call.r#ref));
        out.push('"');
    }
    out.push_str(">\n  <output>");
    out.push_str(&xml_text(&encoded));
    out.push_str("</output>\n</tool_result>");
    Ok(out)
}

#[must_use]
pub fn format_message_body(turn: &HistoryMessage) -> String {
    let mut out = String::new();
    let (tag, speaker_tag) = message_body_tags(&turn.role);
    write_message_start(&mut out, tag, turn);
    write_reply_element(&mut out, turn);
    write_speaker_element(&mut out, speaker_tag, &turn.name, &turn.meta);
    write_message_meta_elements(&mut out, &turn.meta);
    write_message_text_elements(&mut out, &turn.original_text, &turn.text);
    write_attachment_elements(&mut out, turn);
    out.push_str("</");
    out.push_str(tag);
    out.push('>');
    out.trim().to_owned()
}

fn message_body_tags(role: &str) -> (&'static str, &'static str) {
    if role == ROLE_MODEL {
        return ("assistant_message", "assistant");
    }
    ("message", "user")
}

fn write_message_start(out: &mut String, tag: &str, turn: &HistoryMessage) {
    out.push('<');
    out.push_str(tag);
    if turn.message_id != 0 {
        write_int_attr(out, "id", i64::from(turn.message_id));
    }
    if turn.thread_id != 0 {
        write_int_attr(out, "thread_id", i64::from(turn.thread_id));
    }
    if let Some(timestamp) = turn.timestamp {
        out.push_str(" timestamp=\"");
        out.push_str(&format_timestamp(timestamp));
        out.push('"');
    }
    out.push_str(">\n");
}

fn write_reply_element(out: &mut String, turn: &HistoryMessage) {
    if turn.reply_to_id == 0 && turn.reply_to_name.trim().is_empty() {
        return;
    }
    out.push_str("  <reply");
    if turn.reply_to_id != 0 {
        write_int_attr(out, "to_id", i64::from(turn.reply_to_id));
    }
    out.push('>');
    if !turn.reply_to_name.trim().is_empty() {
        write_inline_text_element(out, "to_user", &turn.reply_to_name);
    }
    out.push_str("</reply>\n");
}

fn write_speaker_element(out: &mut String, tag: &str, sender: &str, meta: &ChatMessageMeta) {
    out.push_str("  <");
    out.push_str(tag);
    if !meta.sender_username.trim().is_empty() {
        out.push_str(" username=\"");
        out.push_str(&xml_attr(&meta.sender_username));
        out.push('"');
    }
    if !meta.sender_type.is_empty() {
        out.push_str(" type=\"");
        out.push_str(&xml_attr(&meta.sender_type));
        out.push('"');
    }
    let sender = sender.trim();
    if !sender.is_empty() {
        out.push('>');
        out.push_str(&xml_text(sender));
        out.push_str("</");
        out.push_str(tag);
        out.push_str(">\n");
        return;
    }
    out.push_str("></");
    out.push_str(tag);
    out.push_str(">\n");
}

fn write_message_meta_elements(out: &mut String, meta: &ChatMessageMeta) {
    if !meta.annotation.is_empty() {
        write_text_element(out, "annotation", &meta.annotation);
    }
    if !meta.message_type.is_empty() {
        write_text_element(out, "message_type", &meta.message_type);
    }
    if !meta.vision_description.is_empty() {
        write_text_element(out, "vision_description", &meta.vision_description);
    }
}

fn write_message_text_elements(out: &mut String, original: &str, text: &str) {
    let trimmed_original = original.trim();
    let trimmed_text = text.trim();
    if !trimmed_original.is_empty() && trimmed_original != trimmed_text {
        write_text_element(out, "original", original);
    }
    if !trimmed_text.is_empty() {
        write_text_element(out, "text", text);
        return;
    }
    out.push_str("  <no_text></no_text>\n");
}

fn write_attachment_elements(out: &mut String, turn: &HistoryMessage) {
    if turn.meta.attachments.is_empty() {
        return;
    }
    out.push_str("  <attachments>\n");
    let mut media_index = 0;
    for attachment in &turn.meta.attachments {
        media_index = write_attachment_element(out, turn.message_id, media_index, attachment);
    }
    out.push_str("  </attachments>\n");
}

fn write_attachment_element(
    out: &mut String,
    message_id: i32,
    media_index: i32,
    attachment: &ChatAttachment,
) -> i32 {
    let mut media_index = media_index;
    let kind = attachment.kind.trim();
    out.push_str("    <attachment");
    if !kind.is_empty() {
        write_string_attr(out, "kind", kind);
    }
    if !attachment.source.trim().is_empty() {
        write_string_attr(out, "source", &attachment.source);
    }
    out.push('>');
    media_index = write_attachment_file_id(
        out,
        message_id,
        media_index,
        kind,
        &attachment.file_unique_id,
    );
    if !attachment.content.trim().is_empty() {
        write_inline_text_element(out, "content", &attachment.content);
    }
    out.push_str("</attachment>\n");
    media_index
}

fn write_attachment_file_id(
    out: &mut String,
    message_id: i32,
    media_index: i32,
    kind: &str,
    file_unique_id: &str,
) -> i32 {
    let file_unique_id = file_unique_id.trim();
    if file_unique_id.is_empty() {
        return media_index;
    }
    if !matches!(
        kind.to_ascii_lowercase().as_str(),
        "image" | "video" | "animation" | "video_note"
    ) {
        write_inline_text_element(out, "file_id", file_unique_id);
        return media_index;
    }
    let media_index = media_index + 1;
    if message_id > 0 {
        out.push_str("<file_id>");
        write_vision_attachment_handle(out, message_id, kind, media_index);
        out.push_str("</file_id>");
    }
    write_inline_text_element(out, "file_unique_id", file_unique_id);
    media_index
}

fn fallback_string(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

fn xml_text(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.trim().chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

fn xml_attr(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.trim().chars() {
        match ch {
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\t' => out.push_str("&#x9;"),
            '\n' => out.push_str("&#xA;"),
            '\r' => out.push_str("&#xD;"),
            _ => out.push(ch),
        }
    }
    out
}

fn write_int_attr(out: &mut String, name: &str, value: i64) {
    out.push(' ');
    out.push_str(name);
    out.push_str("=\"");
    let _ = write!(out, "{value}");
    out.push('"');
}

fn write_string_attr(out: &mut String, name: &str, value: &str) {
    out.push(' ');
    out.push_str(name);
    out.push_str("=\"");
    out.push_str(&xml_attr(value));
    out.push('"');
}

fn write_text_element(out: &mut String, name: &str, value: &str) {
    out.push_str("  ");
    write_inline_text_element(out, name, value);
    out.push('\n');
}

fn write_int_element(out: &mut String, indent: &str, name: &str, value: i64) {
    out.push_str(indent);
    out.push('<');
    out.push_str(name);
    out.push('>');
    let _ = write!(out, "{value}");
    out.push_str("</");
    out.push_str(name);
    out.push_str(">\n");
}

fn write_inline_text_element(out: &mut String, name: &str, value: &str) {
    out.push('<');
    out.push_str(name);
    out.push('>');
    out.push_str(&xml_text(value));
    out.push_str("</");
    out.push_str(name);
    out.push('>');
}

fn write_vision_attachment_handle(out: &mut String, message_id: i32, kind: &str, index: i32) {
    out.push_str("message_");
    let _ = write!(out, "{message_id}");
    out.push('_');
    out.push_str(&kind.trim().to_ascii_lowercase());
    out.push('_');
    let _ = write!(out, "{index}");
}

fn format_timestamp(timestamp: OffsetDateTime) -> String {
    timestamp
        .to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .unwrap_or_else(|_| timestamp.unix_timestamp().to_string())
}

fn normalize_relative_chat_completions_url(value: &str) -> String {
    let trimmed_path = value.trim_end_matches('/');
    if trimmed_path.ends_with("/chat/completions") {
        return trimmed_path.to_owned();
    }
    format!("{trimmed_path}/chat/completions")
}

fn parse_response_error_value(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    for key in ["message", "error", "detail", "code"] {
        let Some(value) = object.get(key) else {
            continue;
        };
        match value {
            Value::String(text) if !text.trim().is_empty() => {
                return Some(text.trim().to_owned());
            }
            Value::Object(_) => {
                if let Some(message) = parse_response_error_value(value) {
                    return Some(message);
                }
            }
            _ => {}
        }
    }
    None
}

fn direct_request_id_from_value(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    for key in ["requestId", "request_id", "id"] {
        let Some(Value::String(value)) = object.get(key) else {
            continue;
        };
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_owned());
        }
    }
    None
}

fn contains_any_ascii_fold(haystack: &str, needles: &[&str]) -> bool {
    needles
        .iter()
        .copied()
        .any(|needle| contains_ascii_fold(haystack, needle))
}

fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
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
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex, MutexGuard},
    };

    use serde_json::json;
    use time::{Duration, Month, OffsetDateTime};

    use super::*;
    use openplotva_core::{ChatAttachment, SENDER_TYPE_USER, ToolCall};
    use openplotva_dialog::{
        DailyPersona, DialogContext, DialogMessage, DialogUser, DrawRequest, HistorySearchRequest,
        HistorySummaryRequest, Persona, ROLE_TOOL, RatesRequest, SESSION_REACT_TO_MESSAGE_SPEC,
        SESSION_SEND_MESSAGE_SPEC, STEP_CHAT_HISTORY_SUMMARY, STEP_CURRENCY_RATES, STEP_DRAW_IMAGE,
        STEP_HISTORY_SEARCH, STEP_REACT_TO_MESSAGE, STEP_SEND_MESSAGE, STEP_UNDERSTAND_MEDIA,
        STEP_WEB_SEARCH, TOOL_RESULT_STATUS_OK, ToolResult, VisionRequest,
    };

    fn at(hour: u8, minute: u8) -> OffsetDateTime {
        time::Date::from_calendar_date(2026, Month::May, 1)
            .expect("date")
            .with_hms(hour, minute, 0)
            .expect("time")
            .assume_utc()
    }

    fn base_input() -> DialogInput {
        DialogInput {
            context: DialogContext {
                bot_name: "Plotva".to_owned(),
                locale: "ru".to_owned(),
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 42,
                full_name: "Alice".to_owned(),
            },
            ..DialogInput::default()
        }
    }

    fn completion_text(result: &CompletionResult) -> Option<&str> {
        result
            .response
            .as_ref()?
            .get("choices")?
            .get(0)?
            .get("message")?
            .get("content")?
            .as_str()
    }

    #[derive(Clone, Debug)]
    struct FakeTransport {
        state: Arc<Mutex<FakeTransportState>>,
    }

    #[derive(Debug, Default)]
    struct FakeTransportState {
        requests: Vec<AifarmHttpRequest>,
        responses: VecDeque<Result<AifarmHttpResponse, CompletionError>>,
    }

    impl FakeTransport {
        fn new(responses: Vec<Result<AifarmHttpResponse, CompletionError>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeTransportState {
                    requests: Vec::new(),
                    responses: responses.into(),
                })),
            }
        }

        fn requests(&self) -> Vec<AifarmHttpRequest> {
            self.state().requests.clone()
        }

        fn state(&self) -> MutexGuard<'_, FakeTransportState> {
            match self.state.lock() {
                Ok(state) => state,
                Err(err) => panic!("fake transport state poisoned: {err}"),
            }
        }
    }

    impl AifarmHttpTransport for FakeTransport {
        fn send<'a>(&'a self, request: AifarmHttpRequest) -> AifarmHttpFuture<'a> {
            Box::pin(async move {
                let mut state = self.state();
                state.requests.push(request);
                state.responses.pop_front().unwrap_or_else(|| {
                    Ok(AifarmHttpResponse {
                        status_code: 500,
                        status_text: "Internal Server Error".to_owned(),
                        body: b"unexpected fake transport request".to_vec(),
                        ..AifarmHttpResponse::default()
                    })
                })
            })
        }
    }

    #[derive(Clone, Debug)]
    struct HangingTransport;

    impl AifarmHttpTransport for HangingTransport {
        fn send<'a>(&'a self, _request: AifarmHttpRequest) -> AifarmHttpFuture<'a> {
            Box::pin(std::future::pending())
        }
    }

    #[derive(Clone, Debug)]
    struct RepeatingTransport {
        response: AifarmHttpResponse,
    }

    impl AifarmHttpTransport for RepeatingTransport {
        fn send<'a>(&'a self, _request: AifarmHttpRequest) -> AifarmHttpFuture<'a> {
            let response = self.response.clone();
            Box::pin(async move { Ok(response) })
        }
    }

    #[tokio::test(start_paused = true)]
    async fn direct_dialog_request_times_out_on_the_task_budget_not_the_request_budget() {
        // The synchronous direct POST is the generation call: it must get the
        // task budget (60s + 30s margin here), never the fast request budget.
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                request_timeout: StdDuration::from_secs(5),
                task_timeout: StdDuration::from_secs(60),
                ..AifarmClientConfig::default()
            },
            HangingTransport,
        );
        let mut on_status = |_: StatusUpdate| {};
        let err = client
            .complete(ChatCompletionRequest::default(), &mut on_status)
            .await
            .expect_err("hung transport must time out");
        assert!(err.to_string().contains("timed out after 90s"), "{err}");
        assert_eq!(
            retryable_reason(err.as_ref()),
            Some(FailureReason::ProviderUnavailable)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_blocking_submit_times_out_on_the_task_budget_not_the_request_budget() {
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                request_timeout: StdDuration::from_secs(30),
                task_timeout: StdDuration::from_secs(600),
                ..AifarmClientConfig::default()
            },
            HangingTransport,
        );
        let mut on_status = |_: StatusUpdate| {};
        let err = client
            .complete(ChatCompletionRequest::default(), &mut on_status)
            .await
            .expect_err("hung blocking submit must time out");
        assert!(err.to_string().contains("timed out after 630s"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn discovery_poll_gives_up_on_the_task_budget_once_the_job_runs() {
        let envelope = br#"{"id": "j1", "status": "JOB_STATE_RUNNING"}"#.to_vec();
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                task_timeout: StdDuration::from_secs(5),
                poll_interval: StdDuration::from_secs(1),
                capacity_wait: StdDuration::from_secs(60),
                ..AifarmClientConfig::default()
            },
            RepeatingTransport {
                response: AifarmHttpResponse {
                    status_code: 200,
                    status_text: "OK".to_owned(),
                    body: envelope,
                    ..AifarmHttpResponse::default()
                },
            },
        );
        let started = tokio::time::Instant::now();
        let mut on_status = |_: StatusUpdate| {};
        let err = client
            .complete(ChatCompletionRequest::default(), &mut on_status)
            .await
            .expect_err("a job stuck in RUNNING must give up");
        assert!(err.to_string().contains("did not finish"), "{err}");
        // Once the job is running the task budget applies, not the extra
        // capacity/queue allowance.
        assert!(
            started.elapsed() < StdDuration::from_secs(60),
            "gave up after {:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn direct_poll_gives_up_at_the_task_timeout() {
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                task_timeout: StdDuration::from_secs(5),
                poll_interval: StdDuration::from_secs(1),
                ..AifarmClientConfig::default()
            },
            RepeatingTransport {
                response: AifarmHttpResponse {
                    status_code: 202,
                    status_text: "Accepted".to_owned(),
                    headers: [("X-Request-ID".to_owned(), "req-1".to_owned())].into(),
                    body: Vec::new(),
                },
            },
        );
        let mut on_status = |_: StatusUpdate| {};
        let err = client
            .complete(ChatCompletionRequest::default(), &mut on_status)
            .await
            .expect_err("a request stuck on 202 must give up");
        assert!(err.to_string().contains("did not finish"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_wait_gives_up_only_after_the_configured_budget() {
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                capacity_wait: StdDuration::from_secs(3),
                capacity_poll_interval: StdDuration::from_secs(1),
                ..AifarmClientConfig::default()
            },
            RepeatingTransport {
                response: AifarmHttpResponse {
                    status_code: 503,
                    status_text: "Service Unavailable".to_owned(),
                    body: br#"{"error": "no capacity available"}"#.to_vec(),
                    ..AifarmHttpResponse::default()
                },
            },
        );
        let started = tokio::time::Instant::now();
        let mut queued = 0usize;
        let mut on_status = |update: StatusUpdate| {
            if update.status == STATUS_QUEUED {
                queued += 1;
            }
        };
        let err = client
            .complete(ChatCompletionRequest::default(), &mut on_status)
            .await
            .expect_err("permanent capacity shortage must surface");
        assert_eq!(
            retryable_reason(err.as_ref()),
            Some(FailureReason::CapacityUnavailable)
        );
        assert!(
            started.elapsed() >= StdDuration::from_secs(3),
            "gave up after {:?} without waiting out the capacity budget",
            started.elapsed()
        );
        assert!(
            queued >= 2,
            "expected repeated capacity waits, got {queued}"
        );
    }

    #[derive(Default)]
    struct RecordingObserver(Arc<Mutex<Vec<crate::trace::LlmCallRecord>>>);

    impl crate::trace::LlmCallObserver for RecordingObserver {
        fn observe(&self, record: crate::trace::LlmCallRecord) {
            self.0.lock().expect("observer mutex").push(record);
        }
    }

    #[tokio::test]
    async fn aifarm_complete_emits_trace_record_with_flow_model_and_context() {
        let response = json!({
            "choices": [{"message": {"role": "assistant", "content": "ok"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5}
        });
        let transport = FakeTransport::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&response).expect("serialize response"),
            ..AifarmHttpResponse::default()
        })]);
        let sink = Arc::new(Mutex::new(Vec::new()));
        let registry = Arc::new(crate::trace::LlmCallTraceRegistry::new());
        assert!(registry.set(Arc::new(RecordingObserver(Arc::clone(&sink)))));
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                api_key: "token".to_owned(),
                ..AifarmClientConfig::default()
            },
            transport,
        )
        .with_trace_registry(registry);
        let request = ChatCompletionRequest {
            model: "vram.cloud/qwen3.6-27b".to_owned(),
            trace: Some(crate::trace::LlmCallTrace {
                context: crate::trace::LlmCallContext {
                    chat_id: -100,
                    user_id: 7,
                    message_id: 77,
                    ..crate::trace::LlmCallContext::default()
                },
                tags: crate::trace::LlmCallTags {
                    provider: "aifarm".to_owned(),
                    source: "aifarm".to_owned(),
                    flow: "dialog".to_owned(),
                    mode: "tools".to_owned(),
                    request_kind: "openai.chat.completions".to_owned(),
                    iteration: 1,
                    docs_chars: 0,
                },
            }),
            ..ChatCompletionRequest::default()
        };
        let mut noop = |_status: StatusUpdate| {};
        let result = client.complete(request, &mut noop).await;
        assert!(result.is_ok(), "complete should succeed: {result:?}");
        let records = sink.lock().expect("sink mutex");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].artifact.flow, "dialog");
        assert_eq!(records[0].artifact.model, "vram.cloud/qwen3.6-27b");
        assert_eq!(records[0].artifact.source, "aifarm");
        assert_eq!(records[0].context.chat_id, -100);
        assert_eq!(records[0].context.user_id, 7);
        assert!(records[0].artifact.error.is_empty());
    }

    #[tokio::test]
    async fn dialog_call_trace_records_semantic_protocol_failure_after_http_success() {
        let response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "<message id=\"1\"><text>copied context</text></message>"
                }
            }]
        });
        let transport = FakeTransport::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&response).expect("serialize response"),
            ..AifarmHttpResponse::default()
        })]);
        let sink = Arc::new(Mutex::new(Vec::new()));
        let registry = Arc::new(crate::trace::LlmCallTraceRegistry::new());
        assert!(registry.set(Arc::new(RecordingObserver(Arc::clone(&sink)))));
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                ..AifarmClientConfig::default()
            },
            transport,
        )
        .with_trace_registry(registry);
        let request = ChatCompletionRequest {
            trace: Some(crate::trace::LlmCallTrace {
                tags: crate::trace::LlmCallTags {
                    provider: PROVIDER_AIFARM.to_owned(),
                    flow: "dialog".to_owned(),
                    ..crate::trace::LlmCallTags::default()
                },
                ..crate::trace::LlmCallTrace::default()
            }),
            ..ChatCompletionRequest::default()
        };

        let result = client.complete(request, &mut |_status| {}).await;

        assert!(
            result.is_ok(),
            "transport result stays successful: {result:?}"
        );
        let records = sink.lock().expect("sink mutex");
        assert_eq!(records.len(), 1);
        assert!(
            records[0].artifact.error.contains("context"),
            "semantic failure was not recorded: {}",
            records[0].artifact.error
        );
    }

    #[tokio::test]
    async fn aifarm_history_summary_generator_matches_go_request_and_document_shape()
    -> Result<(), Box<dyn Error>> {
        let content = serde_json::to_string(&json!({
            "summary_json": {
                "events": [" shipped "],
                "event_details": [{
                    "title": " shipped ",
                    "description": "done",
                    "actors": ["Alice"],
                    "occurred_at": "2026-05-20T10:00:00Z",
                    "confidence": 0.8
                }],
                "actors": [{"name": " Alice ", "description": " drove "}],
                "recap": " ok ",
                "open_questions": [],
                "source_style": "log",
                "quality_score": 0.7,
                "quality_notes": "solid"
            }
        }))?;
        let response = json!({
            "choices": [{"message": {"role": "assistant", "content": content}}],
            "usage": {"prompt_tokens": 100, "completion_tokens": 20}
        });
        let transport = FakeTransport::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&response)?,
            ..AifarmHttpResponse::default()
        })]);
        let generator = AifarmHistorySummaryGenerator::with_transport(
            AifarmHistorySummaryConfig {
                client: AifarmClientConfig {
                    direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                    api_key: " token ".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "summary-model".to_owned(),
                temperature: Some(0.2),
                ..AifarmHistorySummaryConfig::default()
            },
            transport.clone(),
        );
        let input = openplotva_history::SummaryInput {
            chat_id: 100,
            thread_id: 7,
            scope: openplotva_history::SummaryScope::Thread,
            range_start_at: at(10, 0),
            range_end_at: at(11, 0),
            first_message_id: 1,
            last_message_id: 2,
            first_entry_id: "msg:1".to_owned(),
            last_entry_id: "msg:2".to_owned(),
            raw_message_count: 2,
            covered_message_count: 2,
            requested_by_user_id: 42,
            input_hash: "input-hash".to_owned(),
            input_token_estimate: 321,
            cascade_depth: 3,
            items: vec![openplotva_history::SummaryInputItem {
                kind: "message".to_owned(),
                at: at(10, 0),
                message_id: 1,
                text: "hello".to_owned(),
                ..openplotva_history::SummaryInputItem::default()
            }],
            ..openplotva_history::SummaryInput::default()
        };

        let mut statuses = Vec::new();
        let doc = generator
            .generate_document(&input, &mut |status| statuses.push(status))
            .await?;

        assert_eq!(doc.model, "summary-model");
        assert_eq!(
            doc.prompt_version,
            openplotva_history::SUMMARY_PROMPT_VERSION
        );
        assert_eq!(
            doc.prompt_hash,
            openplotva_history::hash_text(&openplotva_prompts::read("history/summary")?)
        );
        assert_eq!(doc.input_hash, "input-hash");
        assert_eq!(doc.input_token_estimate, 321);
        assert_eq!(doc.cascade_depth, 3);
        assert_eq!(doc.quality_score, 0.7);
        assert_eq!(doc.quality_notes, "solid");
        assert_eq!(doc.content.events, vec!["shipped"]);
        assert_eq!(doc.html, "• shipped\n\nok");
        assert!(doc.output_token_estimate > 0);
        assert_eq!(statuses.last().expect("status").status, STATUS_SUCCEEDED);

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://llm.example/v1/chat/completions");
        assert_eq!(
            requests[0].headers.get("Authorization").map(String::as_str),
            Some("Bearer token")
        );
        let body: Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(body["model"], "summary-model");
        assert_eq!(
            body["max_tokens"],
            DEFAULT_HISTORY_SUMMARY_MAX_OUTPUT_TOKENS
        );
        assert_eq!(body["temperature"], 0.2);
        assert_eq!(body["include_reasoning"], false);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        assert_eq!(
            body["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            false
        );
        assert_eq!(body["messages"][0]["role"], "system");
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("Ты суммаризатор живого группового чата")
        );
        assert_eq!(body["messages"][1]["role"], "user");
        assert!(
            body["messages"][1]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("\"chat_id\": 100")
        );
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(
            body["response_format"]["json_schema"]["name"],
            "chat_history_summary"
        );
        assert_eq!(
            body["response_format"]["json_schema"]["schema"]["required"],
            json!(["summary_json"])
        );
        assert_eq!(
            body["response_format"]["json_schema"]["schema"]["properties"]["summary_json"]["required"],
            json!([
                "events",
                "event_details",
                "actors",
                "recap",
                "open_questions",
                "source_style",
                "quality_score",
                "quality_notes"
            ])
        );
        Ok(())
    }

    #[tokio::test]
    async fn genkit_openai_compatible_history_summary_generator_matches_go_request()
    -> Result<(), Box<dyn Error>> {
        let content = serde_json::to_string(&json!({
            "summary_json": {
                "events": ["fallback"],
                "event_details": [],
                "actors": [],
                "recap": "fallback recap",
                "open_questions": [],
                "source_style": "log",
                "quality_score": 0.6,
                "quality_notes": "ok"
            }
        }))?;
        let response = json!({
            "choices": [{"message": {"role": "assistant", "content": content}}],
            "usage": {"prompt_tokens": 44, "completion_tokens": 11}
        });
        let transport = FakeTransport::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&response)?,
            ..AifarmHttpResponse::default()
        })]);
        let generator = GenkitOpenAiCompatibleHistorySummaryGenerator::with_transport(
            GenkitOpenAiCompatibleHistorySummaryConfig {
                direct_url: "https://openrouter.example/v1/chat/completions".to_owned(),
                api_key: " plugin-key ".to_owned(),
                model: "summary-plugin".to_owned(),
                request_timeout: StdDuration::from_secs(5),
                ..GenkitOpenAiCompatibleHistorySummaryConfig::default()
            },
            transport.clone(),
        );
        let input = openplotva_history::SummaryInput {
            chat_id: 100,
            input_hash: "input-hash".to_owned(),
            input_token_estimate: 77,
            ..openplotva_history::SummaryInput::default()
        };

        let doc = generator.generate_document(&input).await?;

        assert_eq!(doc.model, "summary-plugin");
        assert_eq!(doc.content.recap, "fallback recap");
        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].url,
            "https://openrouter.example/v1/chat/completions"
        );
        assert_eq!(
            requests[0].headers.get("Authorization").map(String::as_str),
            Some("Bearer plugin-key")
        );
        let body: Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(body["model"], "summary-plugin");
        assert_eq!(
            body["max_tokens"],
            DEFAULT_HISTORY_SUMMARY_MAX_OUTPUT_TOKENS
        );
        assert_eq!(body["temperature"], 0.45);
        assert_eq!(body["top_p"], 0.9);
        assert!(body.get("response_format").is_none());
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][1]["role"], "user");
        Ok(())
    }

    #[test]
    fn subject_merge_request_carries_schema_and_penalties() {
        let extractor = AifarmMemoryExtractor::with_transport(
            AifarmMemoryExtractorConfig {
                frequency_penalty: Some(0.3),
                presence_penalty: Some(0.3),
                ..AifarmMemoryExtractorConfig::default()
            }
            .with_defaults(),
            FakeTransport::new(vec![]),
        );
        let request = extractor.subject_merge_request("system", "payload");
        let body = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            body["response_format"]["json_schema"]["name"],
            "memory_subject_merge"
        );
        assert_eq!(body["frequency_penalty"], 0.3);
        assert_eq!(body["presence_penalty"], 0.3);
        assert_eq!(body["stream"], false);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
    }

    #[test]
    fn with_defaults_clamps_penalties_to_openai_range() {
        let cfg = AifarmMemoryExtractorConfig {
            frequency_penalty: Some(7.5),
            presence_penalty: Some(-9.0),
            ..AifarmMemoryExtractorConfig::default()
        }
        .with_defaults();
        assert_eq!(cfg.frequency_penalty, Some(2.0));
        assert_eq!(cfg.presence_penalty, Some(-2.0));

        let non_finite = AifarmMemoryExtractorConfig {
            frequency_penalty: Some(f64::NAN),
            presence_penalty: Some(f64::INFINITY),
            ..AifarmMemoryExtractorConfig::default()
        }
        .with_defaults();
        assert_eq!(non_finite.frequency_penalty, Some(0.3));
        assert_eq!(non_finite.presence_penalty, Some(0.3));
    }

    #[tokio::test]
    async fn aifarm_memory_extractor_matches_go_request_and_decode() -> Result<(), Box<dyn Error>> {
        let content = serde_json::to_string(&json!({
            "episode_summary": "summary",
            "topics": ["infra"],
            "participants": [],
            "candidate_cards": [],
            "supersessions": [],
            "resolutions": [{
                "old_card_id": 10,
                "decision": "reinforce",
                "reason": "confirmed again"
            }],
            "links": []
        }))?;
        let completion_payload = serde_json::to_string(&json!({
            "usage": {"prompt_tokens": 111, "completion_tokens": 22},
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": content
                }
            }]
        }))?;
        let transport = FakeTransport::new(vec![
            Ok(json_response(
                json!({"job_id":"memory-job-1","state":"JOB_STATE_QUEUED"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "memory-job-1",
                    "state": "JOB_STATE_SUCCEEDED",
                    "result": {
                        "response": {
                            "status_code": 200,
                            "body": general_purpose::STANDARD.encode(completion_payload),
                            "content_type": "application/json"
                        }
                    }
                }
            }))),
        ]);
        let extractor = AifarmMemoryExtractor::with_transport(
            AifarmMemoryExtractorConfig {
                client: AifarmClientConfig {
                    base_url: "https://memory.example.test".to_owned(),
                    default_model: "default".to_owned(),
                    request_timeout: StdDuration::from_secs(1),
                    poll_interval: StdDuration::from_nanos(1),
                    task_timeout: StdDuration::from_secs(1),
                    capacity_wait: StdDuration::from_secs(60),
                    capacity_poll_interval: StdDuration::from_secs(1),
                    ..AifarmClientConfig::default()
                },
                model: "default".to_owned(),
                max_output_tokens: DEFAULT_MEMORY_MAX_OUTPUT_TOKENS,
                temperature: Some(0.2),
                enable_thinking: Some(false),
                ..AifarmMemoryExtractorConfig::default()
            },
            transport.clone(),
        );

        let mut statuses = Vec::new();
        let out = extractor
            .extract(
                &ExtractInput {
                    run: openplotva_memory::Run {
                        id: 10,
                        chat_id: -100,
                        message_count: 1,
                        ..openplotva_memory::Run::default()
                    },
                    messages: vec![openplotva_memory::Message {
                        entry_id: "m1".to_owned(),
                        message_id: 1,
                        text: "remember this".to_owned(),
                        ..openplotva_memory::Message::default()
                    }],
                    ..ExtractInput::default()
                },
                &mut |status| statuses.push(status),
            )
            .await?;

        assert_eq!(out.episode_summary, "summary");
        assert_eq!(out.topics, vec!["infra"]);
        assert_eq!(out.resolutions.len(), 1);
        assert_eq!(
            out.resolutions[0].decision,
            openplotva_memory::ResolutionDecision::Reinforce
        );
        assert_eq!(out.resolutions[0].new_fact_text, "");
        assert_eq!(out.input_tokens, 111);
        assert_eq!(out.output_tokens, 22);
        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            "https://memory.example.test/v1/jobs/blocking"
        );
        let job: DiscoveryJobRequest = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(job.invocation.service_name, DEFAULT_SERVICE_NAME);
        assert_eq!(job.invocation.endpoint_name, DEFAULT_ENDPOINT_NAME);
        assert_eq!(job.invocation.headers["X-AIFarm-Workload"], "memory");
        assert_eq!(job.priority, DISCOVERY_PRIORITY_MEMORY);
        let payload = general_purpose::STANDARD.decode(&job.invocation.body)?;
        let body: Value = serde_json::from_slice(&payload)?;
        assert_eq!(body["model"], "default");
        assert_eq!(body["max_tokens"], DEFAULT_MEMORY_MAX_OUTPUT_TOKENS);
        assert_eq!(body["temperature"], 0.2);
        assert_eq!(body["frequency_penalty"], 0.3);
        assert_eq!(body["presence_penalty"], 0.3);
        assert_eq!(body["include_reasoning"], false);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        assert_eq!(
            body["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            false
        );
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(
            body["response_format"]["json_schema"]["name"],
            "memory_extraction"
        );
        assert_eq!(
            body["response_format"]["json_schema"]["schema"]["required"],
            json!([
                "episode_summary",
                "topics",
                "participants",
                "candidate_cards",
                "supersessions",
                "links"
            ])
        );
        assert_eq!(
            body["response_format"]["json_schema"]["schema"]["properties"]["resolutions"]["items"]
                ["required"],
            json!(["old_card_id", "decision"])
        );
        assert_eq!(body["messages"][0]["role"], "system");
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("memory consolidation worker")
        );
        assert_eq!(body["messages"][1]["role"], "user");
        assert!(
            body["messages"][1]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("\"id\": 10")
        );
        Ok(())
    }

    #[test]
    fn aifarm_memory_extractor_uses_provider_usage_tokens_before_estimates()
    -> Result<(), Box<dyn Error>> {
        let out = decode_memory_extraction_output(
            &json!({
                "usage": {"prompt_tokens": 13, "completion_tokens": 7},
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "{\"episode_summary\":\"summary\",\"topics\":[],\"participants\":[],\"candidate_cards\":[],\"supersessions\":[],\"links\":[]}"
                    }
                }]
            }),
            "tiny",
        )?;

        assert_eq!(out.input_tokens, 13);
        assert_eq!(out.output_tokens, 7);
        Ok(())
    }

    #[tokio::test]
    async fn aifarm_structured_json_generator_matches_go_request_shape()
    -> Result<(), Box<dyn Error>> {
        let response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "{\"input\":\"cat\",\"outputs\":[\"cat\"],\"nsfw_result\":\"safe\"}"
                }
            }]
        });
        let transport = FakeTransport::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&response)?,
            ..AifarmHttpResponse::default()
        })]);
        let generator = AifarmStructuredJsonGenerator::with_transport(
            AifarmStructuredJsonConfig {
                client: AifarmClientConfig {
                    direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                    api_key: " token ".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "structured-model".to_owned(),
                ..AifarmStructuredJsonConfig::default()
            },
            transport.clone(),
        );

        let mut statuses = Vec::new();
        let content = generator
            .generate_json(
                AifarmStructuredJsonRequest {
                    messages: vec![
                        AifarmStructuredJsonMessage {
                            role: "system".to_owned(),
                            content: "optimize".to_owned(),
                        },
                        AifarmStructuredJsonMessage {
                            role: "user".to_owned(),
                            content: "cat".to_owned(),
                        },
                    ],
                    schema: json!({
                        "type": "object",
                        "properties": {
                            "outputs": {"type": "array"}
                        },
                    }),
                    temperature: 0.5,
                    ..AifarmStructuredJsonRequest::default()
                },
                &mut |status| statuses.push(status),
            )
            .await?;

        assert_eq!(
            content,
            "{\"input\":\"cat\",\"outputs\":[\"cat\"],\"nsfw_result\":\"safe\"}"
        );
        assert_eq!(statuses.last().expect("status").status, STATUS_SUCCEEDED);

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://llm.example/v1/chat/completions");
        assert_eq!(
            requests[0].headers.get("Authorization").map(String::as_str),
            Some("Bearer token")
        );
        let body: Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(body["model"], "structured-model");
        assert_eq!(body["stream"], false);
        assert_eq!(body["max_tokens"], 1024);
        assert_eq!(body["temperature"], 0.5);
        assert_eq!(body["include_reasoning"], false);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        assert_eq!(
            body["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            false
        );
        assert_eq!(body["messages"][0]["role"], "system");
        assert_eq!(body["messages"][0]["content"], "optimize");
        assert_eq!(body["messages"][1]["role"], "user");
        assert_eq!(body["messages"][1]["content"], "cat");
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(
            body["response_format"]["json_schema"]["name"],
            "structured_response"
        );
        assert_eq!(
            body["response_format"]["json_schema"]["schema"]["properties"]["outputs"]["type"],
            "array"
        );

        let cfg = AifarmStructuredJsonConfig::default().with_defaults();
        assert_eq!(cfg.client.base_url, DEFAULT_AIFARM_BASE_URL);
        assert_eq!(cfg.client.service_name, DEFAULT_SERVICE_NAME);
        assert_eq!(cfg.client.endpoint_name, DEFAULT_ENDPOINT_NAME);
        assert_eq!(cfg.client.priority, DISCOVERY_PRIORITY_INTERACTIVE);
        assert_eq!(cfg.client.workload, AIFARM_WORKLOAD_STRUCTURED);
        assert_eq!(cfg.client.capacity_wait, StdDuration::from_secs(5));
        assert_eq!(cfg.client.task_timeout, StdDuration::from_secs(2 * 60));
        assert_eq!(cfg.client.request_timeout, StdDuration::from_secs(125));
        assert_eq!(cfg.max_tokens, 1024);
        Ok(())
    }

    #[tokio::test]
    async fn aifarm_structured_json_generator_executes_media_optimizers()
    -> Result<(), Box<dyn Error>> {
        let image_response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": r#"{"input":"cat","outputs":[" plotva "," second "],"aspect_ratio":" 16:9 ","nsfw_result":"safe"}"#
                }
            }]
        });
        let edit_response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": r#"{"input":"","outputs":[" make it day "],"nsfw_result":"weird"}"#
                }
            }]
        });
        let transport = FakeTransport::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 200,
                status_text: "OK".to_owned(),
                body: serde_json::to_vec(&image_response)?,
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 200,
                status_text: "OK".to_owned(),
                body: serde_json::to_vec(&edit_response)?,
                ..AifarmHttpResponse::default()
            }),
        ]);
        let generator = AifarmStructuredJsonGenerator::with_transport(
            AifarmStructuredJsonConfig {
                client: AifarmClientConfig {
                    direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                    ..AifarmClientConfig::default()
                },
                ..AifarmStructuredJsonConfig::default()
            },
            transport.clone(),
        );
        let mut statuses = Vec::new();

        let image = generator
            .optimize_image_prompt(
                " cat ",
                openplotva_media::OptimizePromptOptions { variant_count: 2 },
                &mut |status| statuses.push(status),
            )
            .await?;
        assert_eq!(image.input, "cat");
        assert_eq!(image.outputs, vec!["plotva", "second"]);
        assert_eq!(image.aspect_ratio, "16:9");
        assert_eq!(image.nsfw_result, openplotva_media::NsfwResult::Safe);

        let edit = generator
            .optimize_image_edit_prompt(
                " edit it ",
                openplotva_media::OptimizePromptOptions { variant_count: 2 },
                &mut |status| statuses.push(status),
            )
            .await?;
        assert_eq!(edit.input, "edit it");
        assert_eq!(edit.outputs, vec!["make it day", "make it day"]);
        assert_eq!(edit.nsfw_result, openplotva_media::NsfwResult::Adult);

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        let image_body: Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(
            image_body["response_format"]["json_schema"]["name"],
            "optimize_prompt"
        );
        assert_eq!(
            image_body["response_format"]["json_schema"]["schema"]["properties"]["outputs"]["minItems"],
            2
        );
        assert_eq!(
            image_body["response_format"]["json_schema"]["schema"]["properties"]["aspect_ratio"]["type"],
            "string"
        );
        assert!(
            image_body["messages"][0]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("optimize_prompt_terminator")
        );
        assert_eq!(image_body["messages"][1]["content"], "cat");
        assert_eq!(image_body["max_tokens"], 2048);
        assert_eq!(image_body["temperature"], 0.3);

        let edit_body: Value = serde_json::from_slice(&requests[1].body)?;
        assert_eq!(
            edit_body["response_format"]["json_schema"]["name"],
            "optimize_edit_prompt"
        );
        assert!(
            edit_body["response_format"]["json_schema"]["schema"]["properties"]["aspect_ratio"]
                .is_null()
        );
        assert!(
            edit_body["messages"][0]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("optimize_edit_prompt_terminator")
        );
        assert_eq!(edit_body["messages"][1]["content"], "edit it");
        assert_eq!(edit_body["max_tokens"], 2048);
        assert_eq!(edit_body["temperature"], 0.3);
        Ok(())
    }

    #[tokio::test]
    async fn aifarm_structured_json_generator_executes_song_reprompt() -> Result<(), Box<dyn Error>>
    {
        let lyrics = [
            "[Verse 1]",
            "line one",
            "line two",
            "line three",
            "line four",
            "[Chorus]",
            "line one",
            "line two",
            "line three",
            "line four",
            "[Verse 2]",
            "line one",
            "line two",
            "line three",
            "line four",
            "[Chorus]",
            "line one",
            "line two",
            "line three",
            "line four",
        ]
        .join("\n");
        let response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": serde_json::to_string(&json!({
                        "title": "Night City",
                        "input_topic": "night city",
                        "style": "synthwave, synth bass, neon mood, 102 bpm",
                        "vocal_language": "en",
                        "lyrics": lyrics,
                    }))?
                }
            }]
        });
        let transport = FakeTransport::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&response)?,
            ..AifarmHttpResponse::default()
        })]);
        let generator = AifarmStructuredJsonGenerator::with_transport(
            AifarmStructuredJsonConfig {
                client: AifarmClientConfig {
                    direct_url: "https://llm.example/v1/chat/completions".to_owned(),
                    ..AifarmClientConfig::default()
                },
                ..AifarmStructuredJsonConfig::default()
            },
            transport.clone(),
        );

        let result = generator
            .optimize_song_prompt(
                openplotva_media::acestep::SongPromptRequest {
                    topic: "night city".to_owned(),
                    language_hint: "en-US".to_owned(),
                    ..openplotva_media::acestep::SongPromptRequest::default()
                },
                &mut |_status| {},
            )
            .await?;

        assert_eq!(result.title, "Night City");
        assert_eq!(result.vocal_language, "en");
        assert_eq!(result.style, "synthwave, synth bass, neon mood, 102 BPM");

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(
            body["response_format"]["json_schema"]["name"],
            "optimize_song_prompt"
        );
        assert_eq!(
            body["response_format"]["json_schema"]["schema"]["required"],
            json!(["title", "input_topic", "style", "vocal_language", "lyrics"])
        );
        assert_eq!(body["messages"][0]["role"], "system");
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("optimize_song_prompt_terminator")
        );
        assert_eq!(body["messages"][1]["role"], "user");
        assert_eq!(
            body["messages"][1]["content"],
            "Topic: night city\nVocal language: en"
        );
        assert_eq!(body["max_tokens"], 1024);
        assert_eq!(body["temperature"], 0.5);
        Ok(())
    }

    #[derive(Clone, Debug)]
    struct FakeToolbox {
        state: Arc<Mutex<FakeToolboxState>>,
    }

    #[derive(Debug, Default)]
    struct FakeToolboxState {
        calls: Vec<String>,
        draw_requests: Vec<DrawRequest>,
        vision_requests: Vec<VisionRequest>,
        web_search_queries: Vec<String>,
        history_search_queries: Vec<String>,
        results: VecDeque<ToolResult>,
    }

    impl FakeToolbox {
        fn new(results: Vec<ToolResult>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeToolboxState {
                    results: results.into(),
                    ..FakeToolboxState::default()
                })),
            }
        }

        fn web_search_queries(&self) -> Vec<String> {
            self.state().web_search_queries.clone()
        }

        fn record(
            &self,
            call: impl Into<String>,
        ) -> Result<ToolResult, openplotva_dialog::ToolboxError> {
            let mut state = self.state();
            state.calls.push(call.into());
            Ok(state.results.pop_front().unwrap_or_else(|| ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message: "ok".to_owned(),
                ..ToolResult::default()
            }))
        }

        fn state(&self) -> MutexGuard<'_, FakeToolboxState> {
            match self.state.lock() {
                Ok(state) => state,
                Err(err) => panic!("fake toolbox state poisoned: {err}"),
            }
        }
    }

    impl openplotva_dialog::DialogToolbox for FakeToolbox {
        fn currency_rates<'a>(
            &'a self,
            _meta: RatesRequest,
        ) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = self.record(STEP_CURRENCY_RATES);
            Box::pin(async move { result })
        }

        fn draw_image<'a>(&'a self, req: DrawRequest) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = {
                let mut state = self.state();
                state.calls.push(STEP_DRAW_IMAGE.to_owned());
                state.draw_requests.push(req);
                Ok(state.results.pop_front().unwrap_or_else(|| ToolResult {
                    status: TOOL_RESULT_STATUS_OK.to_owned(),
                    message: "ok".to_owned(),
                    ..ToolResult::default()
                }))
            };
            Box::pin(async move { result })
        }

        fn understand_media<'a>(
            &'a self,
            req: VisionRequest,
        ) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = {
                let mut state = self.state();
                state.calls.push(STEP_UNDERSTAND_MEDIA.to_owned());
                state.vision_requests.push(req);
                Ok(state.results.pop_front().unwrap_or_else(|| ToolResult {
                    status: TOOL_RESULT_STATUS_OK.to_owned(),
                    message: "ok".to_owned(),
                    ..ToolResult::default()
                }))
            };
            Box::pin(async move { result })
        }

        fn web_search<'a>(&'a self, query: String) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = {
                let mut state = self.state();
                state.calls.push(STEP_WEB_SEARCH.to_owned());
                state.web_search_queries.push(query);
                Ok(state.results.pop_front().unwrap_or_else(|| ToolResult {
                    status: TOOL_RESULT_STATUS_OK.to_owned(),
                    message: "ok".to_owned(),
                    ..ToolResult::default()
                }))
            };
            Box::pin(async move { result })
        }

        fn history_search<'a>(
            &'a self,
            req: HistorySearchRequest,
        ) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = {
                let mut state = self.state();
                state.calls.push(STEP_HISTORY_SEARCH.to_owned());
                state.history_search_queries.push(req.query);
                Ok(state.results.pop_front().unwrap_or_else(|| ToolResult {
                    status: TOOL_RESULT_STATUS_OK.to_owned(),
                    message: "ok".to_owned(),
                    ..ToolResult::default()
                }))
            };
            Box::pin(async move { result })
        }

        fn chat_history_summary<'a>(
            &'a self,
            _req: HistorySummaryRequest,
        ) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = self.record(STEP_CHAT_HISTORY_SUMMARY);
            Box::pin(async move { result })
        }
    }

    fn json_response(value: Value) -> AifarmHttpResponse {
        AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            headers: BTreeMap::new(),
            body: serde_json::to_vec(&value).expect("json response"),
        }
    }

    #[test]
    fn direct_status_error_preserves_openrouter_retry_metadata() {
        let response = AifarmHttpResponse {
            status_code: 429,
            status_text: "Too Many Requests".to_owned(),
            headers: BTreeMap::from([("retry-after".to_owned(), "3600".to_owned())]),
            body: Vec::new(),
        };
        let raw_body = r#"{"error":{"code":429,"message":"Rate limited","metadata":{"error_type":"rate_limit_exceeded"}}}"#;

        let error = direct_http_status_error("direct dialog request", &response, raw_body)
            .expect_err("429 should be an upstream error");
        let message = error.to_string();

        assert!(message.contains("retry_after_seconds=3600"), "{message}");
        assert!(
            message.contains("openrouter_error_type=rate_limit_exceeded"),
            "{message}"
        );
    }

    fn native_tool_values(tool_names: &[&str]) -> Result<Vec<Value>, AifarmDialogError> {
        openplotva_dialog::chat_completion_tools_for_names(tool_names)
            .into_iter()
            .map(|tool| serde_json::to_value(tool).map_err(AifarmDialogError::ToolDefinition))
            .collect()
    }

    fn direct_dialog_provider(
        response: Value,
        cfg: AifarmDialogConfig,
    ) -> (
        AifarmDialogProvider<FakeTransport>,
        FakeTransport,
        FakeToolbox,
    ) {
        let transport = FakeTransport::new(vec![Ok(json_response(response))]);
        let mut cfg = cfg;
        cfg.client.direct_url = "https://direct.test/v1/chat/completions".to_owned();
        (
            AifarmDialogProvider::with_transport(cfg, transport.clone()),
            transport,
            FakeToolbox::new(Vec::new()),
        )
    }

    #[test]
    fn aifarm_trace_request_redacts_data_url_media() {
        let request = ChatCompletionRequest {
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content_parts: vec![
                    ChatContentPart {
                        part_type: "image_url".to_owned(),
                        text: String::new(),
                        image_url: Some(ChatImageUrlPart {
                            url: "data:image/png;base64,secret".to_owned(),
                            detail: "auto".to_owned(),
                        }),
                        video_url: None,
                    },
                    ChatContentPart {
                        part_type: "video_url".to_owned(),
                        text: String::new(),
                        image_url: None,
                        video_url: Some(ChatVideoUrlPart {
                            url: "data:video/mp4;base64,video-secret".to_owned(),
                        }),
                    },
                    ChatContentPart {
                        part_type: "image_url".to_owned(),
                        text: String::new(),
                        image_url: Some(ChatImageUrlPart {
                            url: "https://example.test/image.png".to_owned(),
                            detail: "auto".to_owned(),
                        }),
                        video_url: None,
                    },
                ],
                ..ChatMessage::default()
            }],
            ..ChatCompletionRequest::default()
        };

        let redacted = redacted_chat_completion_request(&request);
        let value = serde_json::to_value(redacted).expect("serialized request");

        assert_eq!(
            value["messages"][0]["content"][0]["image_url"]["url"],
            "data:<redacted-image>"
        );
        assert_eq!(
            value["messages"][0]["content"][2]["image_url"]["url"],
            "https://example.test/image.png"
        );
        assert_eq!(
            value["messages"][0]["content"][1]["video_url"]["url"],
            "data:<redacted-video>"
        );
        assert!(!value.to_string().contains("video-secret"));
    }

    #[test]
    fn final_answer_context_leak_is_retryable_provider_failure() {
        let err = extract_final_answer_for_provider(
            &json!({
                "choices": [{
                    "message": {
                        "content": "<message id=\"1\"><text>old</text></message>"
                    }
                }]
            }),
            PROVIDER_AIFARM,
        )
        .expect_err("context leak");

        assert_eq!(
            retryable_reason(err.as_ref()),
            Some(FailureReason::ProviderProtocolError)
        );
    }

    #[test]
    fn step_error_stamps_the_trace_artifact() {
        let error = aifarm_step_error_with_trace(
            Box::new(std::io::Error::other("transport failed")),
            DialogTraceArtifacts::default(),
        );
        let traced = error
            .downcast_ref::<DialogTraceError>()
            .expect("trace wrapper");

        assert_eq!(traced.trace_events()[0].error, "transport failed");
    }

    #[test]
    fn reasoning_only_completion_is_retryable_budget_exhausted() {
        let err = extract_final_answer_for_provider(
            &json!({
                "choices": [{
                    "finish_reason": "length",
                    "message": {
                        "content": "\n\n",
                        "reasoning_content": "The user wants an image. Let me think about the prompt..."
                    }
                }]
            }),
            PROVIDER_AIFARM,
        )
        .expect_err("reasoning budget exhausted");

        assert_eq!(
            retryable_reason(err.as_ref()),
            Some(FailureReason::ProviderProtocolError)
        );
        assert!(
            err.to_string().contains("reasoning without final content"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn length_truncated_empty_completion_is_retryable_budget_exhausted() {
        // Some backends drop reasoning_content from the final payload; an empty
        // content with finish_reason="length" is still a burnt budget, not a
        // protocol violation.
        let err = extract_final_answer(&json!({
            "choices": [{
                "finish_reason": "length",
                "message": { "content": "" }
            }]
        }))
        .expect_err("length truncation");

        assert!(matches!(
            err,
            AifarmDialogError::ReasoningBudgetExhausted { reasoning_chars: 0 }
        ));
    }

    #[test]
    fn length_truncated_completion_preserves_nonempty_final_content() {
        let answer = extract_final_answer(&json!({
            "choices": [{
                "finish_reason": "length",
                "message": {
                    "content": "A complete answer that reached the output limit.",
                    "reasoning_content": "internal reasoning"
                }
            }]
        }))
        .expect("nonempty final content must remain usable");

        assert_eq!(answer, "A complete answer that reached the output limit.");
    }

    #[test]
    fn aifarm_trace_usage_extracts_reasoning_tokens() {
        let usage = aifarm_trace_usage(&json!({
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 1024,
                "total_tokens": 1034,
                "completion_tokens_details": { "reasoning_tokens": 1000 }
            }
        }))
        .expect("usage");

        assert_eq!(usage.thoughts_tokens, 1000);
        assert_eq!(usage.output_tokens, 1024);
    }

    #[test]
    fn final_answer_salvages_malformed_legacy_json_envelope() -> Result<(), AifarmDialogError> {
        let answer = extract_final_answer(&json!({
            "choices": [{
                "message": {
                    "content": r#"{"step":"final_response","answer":"Он сказал "Привет" и ушел"}"#
                }
            }]
        }))?;

        assert_eq!(answer, r#"Он сказал "Привет" и ушел"#);
        Ok(())
    }

    #[test]
    fn last_message_wrapper_embeds_prerendered_block_without_re_escaping() {
        // `message` is a pre-rendered, already-single-escaped XML block, exactly
        // as format_message_body produces for the current turn. The wrapper must
        // embed it verbatim, never HTML-escape it a second time.
        let block = "<message id=\"7\">\n  <original>A &amp; B &lt; C</original>\n</message>";
        let rendered =
            openplotva_prompts::render("aifarm/last_message_wrapper", &json!({ "message": block }))
                .expect("render last_message_wrapper");
        assert!(
            !rendered.contains("&amp;amp;"),
            "ampersand was re-escaped: {rendered}"
        );
        assert!(
            !rendered.contains("&amp;lt;"),
            "&lt; was re-escaped: {rendered}"
        );
        assert!(
            !rendered.contains("&lt;message"),
            "structural <message> tag was escaped: {rendered}"
        );
        assert!(
            rendered.contains(block),
            "wrapper must embed the pre-rendered block verbatim: {rendered}"
        );
    }

    #[test]
    fn system_prompt_includes_tool_catalog() -> Result<(), AifarmMessageError> {
        let prompt = build_system_prompt_with_tool_prompt(&base_input(), ToolPromptMode::Native)?;

        assert!(prompt.contains("собеседник в живом Telegram-чате"));
        assert!(prompt.contains("Большинство реплик не требуют tool"));
        assert!(prompt.contains("Никогда не используй translate_text"));
        assert!(prompt.contains("<system_contract>"));
        assert!(prompt.contains("<tools>"));
        let names = alternative_dialog_tool_names();
        for spec in alternative_dialog_tools()
            .into_iter()
            .filter(|spec| names.contains(&spec.name))
        {
            assert!(prompt.contains(&format!("name=\"{}\"", spec.name)));
            assert!(prompt.contains(spec.summary.trim()));
        }
        assert!(!prompt.contains("name=\"memory_search\""));
        Ok(())
    }

    #[test]
    fn system_prompt_defines_base_voice_and_daily_persona_layers() -> Result<(), AifarmMessageError>
    {
        let prompt = build_system_prompt_with_tool_prompt(&base_input(), ToolPromptMode::Native)?;

        assert!(prompt.contains("<base_voice>"));
        assert!(prompt.contains("<persona_layers>"));
        assert!(prompt.contains("не услужливость"));
        assert!(prompt.contains("Не используй обращения и приветствия"));
        assert!(prompt.contains("не больше одной черты"));
        assert!(prompt.contains("custom_persona"));
        assert!(prompt.contains("daily_persona_accent"));
        Ok(())
    }

    #[test]
    fn runtime_context_uses_names_custom_persona_and_raw_shield() {
        let input = DialogInput {
            context: DialogContext {
                chat_id: -100,
                thread_id: Some(777),
                bot_name: "Plotva".to_owned(),
                chat_title: "Main <Chat>".to_owned(),
                locale: "ru".to_owned(),
            },
            user: DialogUser {
                id: 42,
                full_name: "Alice & Bob".to_owned(),
            },
            persona: Persona {
                mood: "focused".to_owned(),
                custom_persona: "Warm but precise".to_owned(),
                persona: Some(DailyPersona {
                    name: "Daily".to_owned(),
                    background: "hidden".to_owned(),
                    ..DailyPersona::default()
                }),
                ..Persona::default()
            },
            reference_context: vec!["alpha <one>".to_owned(), "beta & two".to_owned()],
            shield_context: "<shield_context><document>support</document></shield_context>"
                .to_owned(),
            ..DialogInput::default()
        };

        let context = build_runtime_context(&input);

        assert!(context.contains("<current_user>Alice &amp; Bob</current_user>"));
        assert!(context.contains("<chat_title>Main &lt;Chat&gt;</chat_title>"));
        assert!(context.contains("<thread_id>777</thread_id>"));
        assert!(context.contains("<custom_persona>Warm but precise</custom_persona>"));
        assert!(!context.contains("<persona_name>"));
        assert!(!context.contains("<persona_tone>"));
        assert!(!context.contains("<persona_background>"));
        assert!(!context.contains("<persona_boundaries>"));
        assert!(context.contains("<shield_context><document>support</document></shield_context>"));
        assert!(context.contains(r#"<chunk index="1">alpha &lt;one&gt;</chunk>"#));
        assert!(
            context.find("<custom_persona>").expect("custom persona")
                < context.find("<shield_context>").expect("shield context")
        );
        assert!(!context.contains("user_id"));
        assert!(!context.contains("chat_id"));
    }

    #[test]
    fn runtime_context_demotes_daily_persona_without_custom() {
        let mut input = base_input();
        input.persona.persona = Some(DailyPersona {
            name: "Daily Persona".to_owned(),
            tone: "Daily tone".to_owned(),
            background: "Daily background".to_owned(),
            boundaries: "Daily boundaries".to_owned(),
        });
        input.reference_context = vec!["reference".to_owned()];
        input.shield_context =
            "<shield_context><document>support</document></shield_context>".to_owned();

        let context = build_runtime_context(&input);

        assert!(context.contains("<daily_persona_accent>"));
        assert!(context.contains("<name>Daily Persona</name>"));
        // The accent is the speech-style `tone`, not the behavioural `boundaries`.
        assert!(context.contains("<accent>Daily tone</accent>"));
        assert!(context.contains("Слабая дневная окраска"));
        assert!(!context.contains("Daily boundaries"));
        assert!(!context.contains("Daily background"));
        assert!(!context.contains("<hint>"));
        assert!(!context.contains("<persona_name>"));
        assert!(!context.contains("<persona_tone>"));
        assert!(!context.contains("<persona_background>"));
        assert!(!context.contains("<persona_boundaries>"));
        assert!(!context.contains("<custom_persona>"));
        assert!(
            context
                .find("<daily_persona_accent>")
                .expect("daily persona")
                < context.find("<shield_context>").expect("shield context")
        );
        assert!(
            context
                .find("<daily_persona_accent>")
                .expect("daily persona")
                < context
                    .find("<reference_context>")
                    .expect("reference context")
        );
    }

    #[test]
    fn current_turn_serializes_as_multimodal() -> Result<(), AifarmMessageError> {
        let mut input = base_input();
        input.message = DialogMessage {
            id: 100,
            text: "что на картинке?".to_owned(),
            meta: ChatMessageMeta {
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "file-1".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..ChatMessageMeta::default()
            },
            ..DialogMessage::default()
        };
        input.multimodal_images = vec![openplotva_dialog::MultimodalImage {
            file_unique_id: "file-1".to_owned(),
            data_url: "data:image/png;base64,AAAA".to_owned(),
            ..openplotva_dialog::MultimodalImage::default()
        }];
        let history = build_session_history_with_limit(&input, DEFAULT_CONTEXT_HISTORY_LIMIT);

        let messages = build_initial_messages_with_options(&input, &history, true)?;
        let current = messages.last().expect("current");
        assert!(current.content.contains("что на картинке?"));
        assert_eq!(current.content_parts.len(), 2);

        let body = serde_json::to_string(current)?;
        assert!(body.contains(r#""type":"text""#));
        assert!(body.contains(r#""type":"image_url""#));
        assert!(body.contains("data:image/png;base64,AAAA"));
        Ok(())
    }

    #[test]
    fn normalizes_openai_base_urls() {
        assert_eq!(
            normalize_chat_completions_url("http://127.0.0.1:32777/v1"),
            "http://127.0.0.1:32777/v1/chat/completions"
        );
        assert_eq!(
            normalize_chat_completions_url("http://127.0.0.1:32779/v1/"),
            "http://127.0.0.1:32779/v1/chat/completions"
        );
        assert_eq!(
            normalize_chat_completions_url("http://127.0.0.1:32777/v1/chat/completions"),
            "http://127.0.0.1:32777/v1/chat/completions"
        );
    }

    #[test]
    fn direct_request_id_body_accepts_go_shapes() {
        assert_eq!(parse_direct_request_id_body("accepted"), "");
        assert_eq!(
            parse_direct_request_id_body(r#"" req-string ""#),
            "req-string"
        );
        assert_eq!(
            parse_direct_request_id_body(r#"{"request_id":" req-object "}"#),
            "req-object"
        );
        assert_eq!(
            parse_direct_request_id_body(r#"{"requestId":" req-camel "}"#),
            "req-camel"
        );
        assert_eq!(
            parse_direct_request_id_body(r#"{"id":" req-id "}"#),
            "req-id"
        );
    }

    #[test]
    fn direct_headers_follow_go_accept_and_authorization_rules() {
        let cfg = AifarmClientConfig {
            api_key: " direct-secret ".to_owned(),
            ..AifarmClientConfig::default()
        }
        .with_defaults();

        let json_headers = direct_completion_headers(
            &cfg,
            &ChatCompletionRequest {
                stream: false,
                ..ChatCompletionRequest::default()
            },
        );
        assert_eq!(json_headers["Content-Type"], "application/json");
        assert_eq!(json_headers["Accept"], "application/json");
        assert_eq!(json_headers["Authorization"], "Bearer direct-secret");

        let sse_headers = direct_completion_headers(
            &cfg,
            &ChatCompletionRequest {
                stream: true,
                ..ChatCompletionRequest::default()
            },
        );
        assert_eq!(sse_headers["Accept"], "text/event-stream");

        let status_headers = direct_status_headers(&cfg);
        assert_eq!(status_headers["Accept"], "application/json");
        assert_eq!(status_headers["Authorization"], "Bearer direct-secret");
    }

    #[test]
    fn direct_body_serializes_openai_request_shape() {
        let body = direct_chat_completion_body(&ChatCompletionRequest {
            model: "model".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: "ping".to_owned(),
                content_parts: Vec::new(),
                ..ChatMessage::default()
            }],
            stream: false,
            ..ChatCompletionRequest::default()
        })
        .expect("direct request body");
        let value: Value = serde_json::from_slice(&body).expect("direct request json");

        assert_eq!(value["model"], "model");
        assert_eq!(value["messages"][0]["role"], "user");
        assert_eq!(value["stream"], false);
    }

    #[test]
    fn resolves_direct_request_id_from_headers_before_body() {
        let mut headers = BTreeMap::new();
        headers.insert("X-Request-ID".to_owned(), " req-header ".to_owned());

        assert_eq!(
            resolve_direct_request_id(&headers, r#"{"request_id":"req-body"}"#)
                .expect("request id"),
            "req-header"
        );

        headers.clear();
        assert_eq!(
            resolve_direct_request_id(&headers, r#"{"requestId":" req-camel "}"#)
                .expect("request id"),
            "req-camel"
        );
        assert_eq!(
            resolve_direct_request_id(&headers, "accepted")
                .expect_err("missing request id")
                .to_string(),
            "direct dialog request accepted but request ID is missing"
        );
    }

    #[test]
    fn detects_capacity_errors_from_plain_or_json_bodies() {
        assert!(is_capacity_unavailable(503, "SERVICE CAPACITY UNAVAILABLE"));
        assert!(is_capacity_unavailable(
            429,
            r#"{"detail":"no slot available"}"#
        ));
        assert!(is_capacity_unavailable(
            503,
            r#"{"error":{"message":"service capacity unavailable"}}"#
        ));
        assert!(!is_capacity_unavailable(
            500,
            "service capacity unavailable"
        ));
        assert!(!is_capacity_unavailable(503, "upstream unavailable"));
    }

    #[test]
    fn direct_compatible_request_strips_llama_only_controls() {
        let request = ChatCompletionRequest {
            model: "primary".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: "ping".to_owned(),
                content_parts: Vec::new(),
                ..ChatMessage::default()
            }],
            max_tokens: 2048,
            temperature: Some(0.2),
            top_p: Some(0.95),
            repeat_penalty: Some(1.1),
            repetition_penalty: Some(1.9),
            frequency_penalty: Some(0.2),
            presence_penalty: Some(0.3),
            dry_multiplier: Some(1.4),
            dry_base: Some(1.5),
            dry_allowed_length: 2,
            include_reasoning: Some(false),
            chat_template_kwargs: Some(json!({"enable_thinking": false})),
            extra_body: Some(json!({
                "chat_template_kwargs": {"enable_thinking": false},
            })),
            ..ChatCompletionRequest::default()
        };

        let direct = direct_compatible_request(&request, " second-model ");

        assert_eq!(direct.model, "second-model");
        assert_eq!(direct.max_tokens, DEFAULT_VRAM_CLOUD_MAX_TOKENS);
        assert_eq!(direct.temperature, Some(DEFAULT_VRAM_CLOUD_TEMPERATURE));
        assert_eq!(direct.top_p, Some(DEFAULT_VRAM_CLOUD_TOP_P));
        assert_eq!(direct.top_k, Some(DEFAULT_VRAM_CLOUD_TOP_K));
        assert_eq!(direct.frequency_penalty, None);
        assert_eq!(
            direct.presence_penalty,
            Some(DEFAULT_VRAM_CLOUD_PRESENCE_PENALTY)
        );
        assert_eq!(direct.repeat_penalty, None);
        assert_eq!(
            direct.repetition_penalty,
            Some(DEFAULT_VRAM_CLOUD_REPETITION_PENALTY)
        );
        assert_eq!(direct.dry_multiplier, None);
        assert_eq!(direct.dry_base, None);
        assert_eq!(direct.dry_allowed_length, 0);
        assert_eq!(direct.include_reasoning, None);
        assert_eq!(
            direct.chat_template_kwargs,
            Some(json!({ "enable_thinking": false }))
        );
        assert_eq!(
            direct.extra_body,
            Some(json!({ "chat_template_kwargs": {"enable_thinking": false} }))
        );
        let body = serde_json::to_value(&direct).expect("direct request JSON");
        assert_eq!(body["top_k"], json!(20.0));
        assert_eq!(body["repetition_penalty"], json!(1.0));
        assert!(body.get("repeat_penalty").is_none());
        assert!(body.get("frequency_penalty").is_none());
    }

    #[test]
    fn direct_status_endpoint_uses_go_status_path() {
        assert_eq!(
            direct_status_endpoint("https://example.test/v1/chat/completions?x=1", " req-1 "),
            "https://example.test/v1/status/req-1"
        );
        assert_eq!(
            direct_status_endpoint("direct/base/", "req-2"),
            "direct/base/status/req-2"
        );
    }

    #[test]
    fn status_helpers_follow_go_known_statuses() {
        assert_eq!(normalize_status(" job_state_succeeded "), STATUS_SUCCEEDED);
        assert_eq!(normalize_status("queued"), "QUEUED");
        assert_eq!(normalize_status(" strange state "), "STRANGE STATE");
        assert!(is_queued_status("pending"));
        assert!(is_running_status("processing"));
        assert!(is_success_status("success"));
        assert!(is_failure_status("cancelled"));
        assert!(!is_failure_status("running"));
    }

    #[test]
    fn client_config_defaults_follow_go_values() {
        let cfg = AifarmClientConfig {
            priority: -10,
            ..AifarmClientConfig::default()
        }
        .with_defaults();

        assert_eq!(cfg.base_url, DEFAULT_AIFARM_BASE_URL);
        assert_eq!(cfg.service_name, DEFAULT_SERVICE_NAME);
        assert_eq!(cfg.endpoint_name, DEFAULT_ENDPOINT_NAME);
        assert_eq!(cfg.request_timeout, StdDuration::from_secs(120));
        assert_eq!(cfg.poll_interval, StdDuration::from_secs(1));
        assert_eq!(cfg.task_timeout, StdDuration::from_secs(12 * 60));
        assert_eq!(cfg.capacity_wait, StdDuration::from_secs(60));
        assert_eq!(cfg.capacity_poll_interval, StdDuration::from_secs(1));
        assert_eq!(cfg.default_model, DEFAULT_MODEL_NAME);
        assert_eq!(cfg.workload, AIFARM_WORKLOAD_DIALOG);
        assert_eq!(cfg.priority, 0);
        assert_eq!(
            cfg.endpoint("/v1/jobs/blocking"),
            format!("{DEFAULT_AIFARM_BASE_URL}/v1/jobs/blocking")
        );
    }

    #[test]
    fn discovery_job_request_forwards_headers_timeouts_and_default_model() {
        let cfg = AifarmClientConfig {
            api_key: " secret-token ".to_owned(),
            base_url: "https://discovery.example.test/".to_owned(),
            request_timeout: StdDuration::from_secs(1),
            task_timeout: StdDuration::from_secs(1),
            default_model: "default".to_owned(),
            workload: AIFARM_WORKLOAD_MEMORY.to_owned(),
            priority: DISCOVERY_PRIORITY_MEMORY,
            ..AifarmClientConfig::default()
        }
        .with_defaults();
        let job = build_discovery_job_request(
            &cfg,
            "dialog-1",
            &ChatCompletionRequest {
                messages: vec![ChatMessage {
                    role: "system".to_owned(),
                    content: "test".to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                }],
                response_format: Some(json!({
                    "type": "json_schema",
                    "json_schema": {"name": "plotva_step", "schema": {"type": "object"}}
                })),
                ..ChatCompletionRequest::default()
            },
        )
        .expect("discovery job request");

        assert_eq!(job.invocation.service_name, DEFAULT_SERVICE_NAME);
        assert_eq!(job.invocation.endpoint_name, DEFAULT_ENDPOINT_NAME);
        assert_eq!(job.invocation.headers["X-Request-Id"], "dialog-1");
        assert_eq!(job.invocation.headers["X-AIFarm-Workload"], "memory");
        assert_eq!(
            job.invocation.headers["Authorization"],
            "Bearer secret-token"
        );
        assert!(job.invocation.query.is_empty());
        assert_eq!(job.invocation.content_type, "application/json");
        assert_eq!(job.invocation.timeout_ms, 1000);
        assert_eq!(job.idempotency_key, "dialog-1");
        assert_eq!(job.priority, DISCOVERY_PRIORITY_MEMORY);
        assert_eq!(job.wait_for_capacity_ms, 60000);
        assert_eq!(job.capacity_poll_ms, 1000);

        let payload = general_purpose::STANDARD
            .decode(&job.invocation.body)
            .expect("base64 body");
        let chat_req: ChatCompletionRequest =
            serde_json::from_slice(&payload).expect("chat completion request");
        assert_eq!(chat_req.model, "default");
        assert!(!chat_req.stream);
        assert_eq!(chat_req.messages[0].role, "system");
        assert_eq!(
            chat_req
                .response_format
                .as_ref()
                .and_then(|value| value["json_schema"]["name"].as_str()),
            Some("plotva_step")
        );
    }

    #[test]
    fn discovery_job_request_preserves_capacity_millisecond_edges() {
        let cfg = AifarmClientConfig {
            capacity_wait: StdDuration::from_nanos(1),
            capacity_poll_interval: StdDuration::from_millis(25),
            task_timeout: StdDuration::from_nanos(1),
            ..AifarmClientConfig::default()
        }
        .with_defaults();
        let job = build_discovery_job_request(&cfg, "dialog-2", &ChatCompletionRequest::default())
            .expect("discovery job request");

        assert_eq!(job.wait_for_capacity_ms, 1);
        assert_eq!(job.capacity_poll_ms, 25);
        assert_eq!(job.invocation.timeout_ms, DEFAULT_TIMEOUT_MS);
    }

    #[tokio::test]
    async fn http_client_completes_discovery_submit_poll_flow() -> Result<(), CompletionError> {
        let completion_payload =
            r#"{"id":"cmpl-1","choices":[{"message":{"role":"assistant","content":"ok"}}]}"#;
        let transport = FakeTransport::new(vec![
            Ok(json_response(
                json!({"job_id":"job-1","state":"JOB_STATE_QUEUED"}),
            )),
            Ok(json_response(
                json!({"job":{"job_id":"job-1","state":"JOB_STATE_RUNNING"}}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "job-1",
                    "state": "JOB_STATE_SUCCEEDED",
                    "result": {
                        "response": {
                            "status_code": 200,
                            "body": general_purpose::STANDARD.encode(completion_payload),
                            "content_type": "application/json"
                        }
                    }
                }
            }))),
        ]);
        let probe = transport.clone();
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                base_url: "https://discovery.example.test".to_owned(),
                poll_interval: StdDuration::from_nanos(1),
                task_timeout: StdDuration::from_secs(1),
                ..AifarmClientConfig::default()
            },
            transport,
        );
        let mut statuses = Vec::new();

        let result = client
            .complete_discovery_with_job_id(
                ChatCompletionRequest {
                    messages: vec![ChatMessage {
                        role: "system".to_owned(),
                        content: "test".to_owned(),
                        content_parts: Vec::new(),
                        ..ChatMessage::default()
                    }],
                    ..ChatCompletionRequest::default()
                },
                "dialog-1",
                &mut |status| statuses.push(status),
            )
            .await?;

        assert_eq!(result.job_id, "job-1");
        assert_eq!(result.status_code, 200);
        assert_eq!(completion_text(&result), Some("ok"));
        assert_eq!(
            statuses
                .iter()
                .map(|status| status.status.as_str())
                .collect::<Vec<_>>(),
            vec![
                STATUS_SUBMITTED,
                STATUS_QUEUED,
                STATUS_RUNNING,
                STATUS_SUCCEEDED
            ]
        );
        let requests = probe.requests();
        assert_eq!(requests[0].method, AifarmHttpMethod::Post);
        assert_eq!(
            requests[0].url,
            "https://discovery.example.test/v1/jobs/blocking"
        );
        assert_eq!(requests[0].headers["Content-Type"], "application/json");
        assert_eq!(requests[1].method, AifarmHttpMethod::Get);
        assert_eq!(
            requests[1].url,
            "https://discovery.example.test/v1/jobs/job-1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn http_client_retries_discovery_capacity_before_submit() -> Result<(), CompletionError> {
        let completion_payload =
            r#"{"id":"cmpl-1","choices":[{"message":{"role":"assistant","content":"ok"}}]}"#;
        let transport = FakeTransport::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 429,
                status_text: "Too Many Requests".to_owned(),
                body: br#"{"detail":"service capacity unavailable"}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
            Ok(json_response(
                json!({"job_id":"job-1","state":"JOB_STATE_QUEUED"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "job-1",
                    "state": "JOB_STATE_SUCCEEDED",
                    "result": {
                        "response": {
                            "status_code": 200,
                            "body": general_purpose::STANDARD.encode(completion_payload)
                        }
                    }
                }
            }))),
        ]);
        let probe = transport.clone();
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                base_url: "https://discovery.example.test".to_owned(),
                poll_interval: StdDuration::from_nanos(1),
                capacity_poll_interval: StdDuration::from_nanos(1),
                task_timeout: StdDuration::from_secs(1),
                ..AifarmClientConfig::default()
            },
            transport,
        );
        let mut statuses = Vec::new();

        let result = client
            .complete_discovery_with_job_id(
                ChatCompletionRequest::default(),
                "dialog-1",
                &mut |status| {
                    statuses.push(status);
                },
            )
            .await?;

        assert_eq!(completion_text(&result), Some("ok"));
        assert_eq!(
            statuses.first().map(|status| status.message.as_str()),
            Some("waiting for Discovery service capacity")
        );
        assert_eq!(probe.requests().len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn http_client_completes_direct_accepted_poll_flow() -> Result<(), CompletionError> {
        let mut accepted_headers = BTreeMap::new();
        accepted_headers.insert("NVCF-REQID".to_owned(), "req-1".to_owned());
        let transport = FakeTransport::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 202,
                status_text: "Accepted".to_owned(),
                headers: accepted_headers,
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 202,
                status_text: "Accepted".to_owned(),
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 200,
                status_text: "OK".to_owned(),
                body: br#"{"id":"cmpl-1","choices":[{"message":{"role":"assistant","content":"ok"}}]}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
        ]);
        let probe = transport.clone();
        let client = AifarmHttpClient::with_transport(
            AifarmClientConfig {
                direct_url: "https://direct.example.test/v1/chat/completions".to_owned(),
                api_key: "secret".to_owned(),
                poll_interval: StdDuration::from_nanos(1),
                task_timeout: StdDuration::from_secs(1),
                ..AifarmClientConfig::default()
            },
            transport,
        );
        let mut statuses = Vec::new();

        let result = client
            .complete_direct_with_job_id(
                ChatCompletionRequest::default(),
                "dialog-1",
                &mut |status| {
                    statuses.push(status);
                },
            )
            .await?;

        assert_eq!(result.job_id, "req-1");
        assert_eq!(completion_text(&result), Some("ok"));
        assert_eq!(
            statuses
                .iter()
                .map(|status| status.status.as_str())
                .collect::<Vec<_>>(),
            vec![STATUS_SUBMITTED, STATUS_QUEUED, STATUS_SUCCEEDED]
        );
        let requests = probe.requests();
        assert_eq!(requests[0].method, AifarmHttpMethod::Post);
        assert_eq!(
            requests[0].url,
            "https://direct.example.test/v1/chat/completions"
        );
        assert_eq!(requests[0].headers["Authorization"], "Bearer secret");
        assert_eq!(requests[1].method, AifarmHttpMethod::Get);
        assert_eq!(
            requests[1].url,
            "https://direct.example.test/v1/status/req-1"
        );
        assert_eq!(requests[1].headers["Accept"], "application/json");
        Ok(())
    }

    #[test]
    fn discovery_envelope_resolves_nested_job_with_top_level_fallbacks() {
        let envelope = DiscoveryJobEnvelope {
            id: "top-id".to_owned(),
            job_id: "top-job".to_owned(),
            status: "top-status".to_owned(),
            state: "top-state".to_owned(),
            error: Some(json!(" top-error ")),
            result: Some(DiscoveryJobResult {
                response: Some(DiscoveryJobResponse {
                    status_code: 202,
                    ..DiscoveryJobResponse::default()
                }),
            }),
            job: Some(DiscoveryJob {
                id: "nested-id".to_owned(),
                ..DiscoveryJob::default()
            }),
        };

        let job = envelope.resolve_job();

        assert_eq!(job.resolved_id(), "top-job");
        assert_eq!(job.resolved_status(), "top-state");
        assert_eq!(parse_job_error(job.error.as_ref()), "top-error");
        assert_eq!(
            job.result
                .and_then(|result| result.response)
                .map(|response| response.status_code),
            Some(202)
        );
    }

    #[test]
    fn parse_job_error_accepts_go_shapes() {
        assert_eq!(parse_job_error(Some(&json!(" bad "))), "bad");
        assert_eq!(
            parse_job_error(Some(&json!({"message":" message error "}))),
            "message error"
        );
        assert_eq!(
            parse_job_error(Some(&json!({"error":" error value "}))),
            "error value"
        );
        assert_eq!(
            parse_job_error(Some(&json!({"nested":{"message":"ignored"}}))),
            r#"{"nested":{"message":"ignored"}}"#
        );
        assert_eq!(parse_job_error(None), "");
    }

    #[test]
    fn decodes_discovery_chat_completion_response_with_timings() -> Result<(), AifarmDecodeError> {
        let payload = r#"{
            "id":"cmpl-1",
            "model":"default",
            "usage":{
                "prompt_tokens":12,
                "completion_tokens":8,
                "total_tokens":20,
                "prompt_tokens_details":{"cached_tokens":4}
            },
            "timings":{
                "cache_n":4,
                "prompt_n":12,
                "prompt_ms":120,
                "prompt_per_second":100,
                "predicted_n":8,
                "predicted_ms":200,
                "predicted_per_second":40
            },
            "choices":[{"message":{"role":"assistant","content":"ok"}}]
        }"#;
        let body = general_purpose::STANDARD.encode(payload);

        let result = decode_chat_completion_response(Some(&DiscoveryJobResult {
            response: Some(DiscoveryJobResponse {
                status_code: 200,
                body,
                ..DiscoveryJobResponse::default()
            }),
        }))?;

        assert_eq!(result.status_code, 200);
        assert_eq!(result.raw_body, payload);
        let response = result.response.as_ref().expect("decoded response");
        assert_eq!(response["usage"]["prompt_tokens"], 12);
        assert_eq!(
            response["usage"]["prompt_tokens_details"]["cached_tokens"],
            4
        );
        assert_eq!(response["timings"]["predicted_per_second"], 40.0);
        assert_eq!(response["choices"][0]["message"]["content"], "ok");
        Ok(())
    }

    #[test]
    fn decodes_url_safe_discovery_body() -> Result<(), AifarmDecodeError> {
        let payload = r#"{"id":"cmpl-1","choices":[{"message":{"role":"assistant","content":"Процесс биологической экспансии завершён."}}]}"#;
        let body = general_purpose::URL_SAFE.encode(payload);

        let result = decode_chat_completion_response(Some(&DiscoveryJobResult {
            response: Some(DiscoveryJobResponse {
                status_code: 200,
                body,
                ..DiscoveryJobResponse::default()
            }),
        }))?;

        assert_eq!(result.status_code, 200);
        assert_eq!(result.raw_body, payload);
        assert!(
            result
                .response
                .as_ref()
                .and_then(|response| response["choices"][0]["message"]["content"].as_str())
                .is_some_and(|text| text.contains("Процесс"))
        );
        Ok(())
    }

    #[test]
    fn bad_discovery_status_keeps_raw_body_without_json_decode() -> Result<(), AifarmDecodeError> {
        let payload = r#"{"detail":"upstream unavailable"}"#;
        let body = general_purpose::STANDARD.encode(payload);

        let result = decode_chat_completion_response(Some(&DiscoveryJobResult {
            response: Some(DiscoveryJobResponse {
                status_code: 503,
                body,
                ..DiscoveryJobResponse::default()
            }),
        }))?;

        assert_eq!(result.status_code, 503);
        assert_eq!(result.raw_body, payload);
        assert_eq!(result.response, None);
        Ok(())
    }

    #[test]
    fn job_status_from_envelope_fills_error_message_from_bad_body() -> Result<(), AifarmDecodeError>
    {
        let body = general_purpose::STANDARD.encode(r#"{"detail":"upstream unavailable"}"#);
        let envelope = DiscoveryJobEnvelope {
            job: Some(DiscoveryJob {
                job_id: "job-1".to_owned(),
                state: "JOB_STATE_FAILED".to_owned(),
                result: Some(DiscoveryJobResult {
                    response: Some(DiscoveryJobResponse {
                        status_code: 503,
                        body,
                        ..DiscoveryJobResponse::default()
                    }),
                }),
                ..DiscoveryJob::default()
            }),
            ..DiscoveryJobEnvelope::default()
        };

        let status = job_status_from_envelope("fallback", &envelope)?;

        assert_eq!(status.job_id, "job-1");
        assert_eq!(status.status, STATUS_FAILED);
        assert_eq!(status.http_status, 503);
        assert_eq!(status.message, "upstream unavailable");
        Ok(())
    }

    #[test]
    fn message_body_uses_conversation_graph_metadata() {
        let body = format_message_body(&HistoryMessage {
            role: ROLE_USER.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            name: "Bob".to_owned(),
            text: "Я отвечал на сообщение Алисы".to_owned(),
            timestamp: Some(at(11, 59)),
            message_id: 10,
            thread_id: 7,
            user_id: 77,
            reply_to_id: 9,
            reply_to_name: "Alice".to_owned(),
            meta: ChatMessageMeta {
                sender_username: "bob".to_owned(),
                sender_type: SENDER_TYPE_USER.to_owned(),
                ..ChatMessageMeta::default()
            },
            ..HistoryMessage::default()
        });

        assert!(
            body.contains(r#"<message id="10" thread_id="7" timestamp="2026-05-01T11:59:00Z">"#)
        );
        assert!(body.contains(r#"<user username="bob" type="user">Bob</user>"#));
        assert!(body.contains(r#"<reply to_id="9"><to_user>Alice</to_user></reply>"#));
        assert!(!body.contains("user_id"));
        assert!(!body.contains("sender_id"));
    }

    #[test]
    fn message_body_uses_stable_vision_attachment_handle() {
        let body = format_message_body(&HistoryMessage {
            kind: MESSAGE_KIND_TEXT.to_owned(),
            message_id: 11951604,
            text: "Что тут?".to_owned(),
            meta: ChatMessageMeta {
                message_type: "image".to_owned(),
                attachments: vec![
                    ChatAttachment {
                        kind: "image".to_owned(),
                        source: "quoted".to_owned(),
                        file_unique_id: "AQADnRJrGyADoEt8".to_owned(),
                        ..ChatAttachment::default()
                    },
                    ChatAttachment {
                        kind: "video".to_owned(),
                        source: "message".to_owned(),
                        file_unique_id: "video-unique".to_owned(),
                        ..ChatAttachment::default()
                    },
                ],
                ..ChatMessageMeta::default()
            },
            ..HistoryMessage::default()
        });

        assert!(body.contains("<file_id>message_11951604_image_1</file_id>"));
        assert!(body.contains("<file_id>message_11951604_video_2</file_id>"));
        assert!(body.contains("<file_unique_id>AQADnRJrGyADoEt8</file_unique_id>"));
    }

    #[test]
    fn initial_messages_use_tool_results_without_synthetic_assistant_tool_requests()
    -> Result<(), AifarmMessageError> {
        let now = at(12, 0);
        let mut input = base_input();
        input.user.full_name = "User".to_owned();
        input.message = DialogMessage {
            id: 10,
            text: "а теперь напиши рассказ".to_owned(),
            timestamp: Some(now),
            ..DialogMessage::default()
        };
        input.history = vec![
            HistoryMessage {
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                name: "Plotva".to_owned(),
                text: "делаю".to_owned(),
                timestamp: Some(now - Duration::minutes(1)),
                message_id: 9,
                ..HistoryMessage::default()
            },
            HistoryMessage {
                role: ROLE_TOOL.to_owned(),
                kind: MESSAGE_KIND_TOOL_RESPONSE.to_owned(),
                name: "draw_image".to_owned(),
                timestamp: Some(now - Duration::minutes(2)),
                message_id: 9,
                tool_call: Some(ToolCall {
                    name: "draw_image".to_owned(),
                    r#ref: "req-1".to_owned(),
                    output: Some(json!({"status": "queued"})),
                    ..ToolCall::default()
                }),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                role: ROLE_TOOL.to_owned(),
                kind: MESSAGE_KIND_TOOL_RESPONSE.to_owned(),
                name: "translate_text".to_owned(),
                message_id: 8,
                tool_call: Some(ToolCall {
                    name: "translate_text".to_owned(),
                    output: Some(json!("Translation result")),
                    ..ToolCall::default()
                }),
                ..HistoryMessage::default()
            },
            HistoryMessage {
                role: ROLE_MODEL.to_owned(),
                kind: MESSAGE_KIND_TOOL_REQUEST.to_owned(),
                name: "Plotva".to_owned(),
                timestamp: Some(now - Duration::minutes(3)),
                message_id: 9,
                tool_call: Some(ToolCall {
                    name: "draw_image".to_owned(),
                    r#ref: "req-1".to_owned(),
                    input: Some(json!({"prompt":"кот"})),
                    ..ToolCall::default()
                }),
                ..HistoryMessage::default()
            },
        ];

        let history = build_session_history_with_limit(&input, DEFAULT_CONTEXT_HISTORY_LIMIT);
        let messages = build_initial_messages_with_options(&input, &history, true)?;
        assert_eq!(messages.len(), 5);
        assert!(messages[2].content.contains("<tool_result"));
        assert_eq!(messages[3].role, "assistant");
        assert_eq!(messages[3].content, "делаю");
        assert!(!messages[3].content.contains("<assistant_message"));
        assert!(messages[4].content.contains("а теперь напиши рассказ"));

        let rendered = messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        let history_and_current = messages[2..]
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!rendered.contains("TOOL_REQUEST"));
        assert!(!rendered.contains("tool_calls:"));
        assert!(!history_and_current.contains("translate_text"));
        Ok(())
    }

    #[test]
    fn initial_messages_skip_protocol_only_assistant_history() -> Result<(), AifarmMessageError> {
        let mut input = base_input();
        input.user.full_name = "User".to_owned();
        input.message = DialogMessage {
            id: 10,
            text: "привет".to_owned(),
            ..DialogMessage::default()
        };
        input.history = vec![HistoryMessage {
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TEXT.to_owned(),
            name: "Plotva".to_owned(),
            text: r#"<|channel>thought<channel|><|tool_call>call:final_response{answer:<|"|>сломанный протокол<|"|>}<tool_call|>"#.to_owned(),
            ..HistoryMessage::default()
        }];

        let history = build_session_history_with_limit(&input, DEFAULT_CONTEXT_HISTORY_LIMIT);
        let messages = build_initial_messages_with_options(&input, &history, true)?;
        assert_eq!(messages.len(), 3);
        let rendered = messages
            .iter()
            .map(|message| message.content.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!rendered.contains("<|tool_call"));
        assert!(!rendered.contains("<|channel>"));
        assert!(!rendered.contains("сломанный протокол"));
        Ok(())
    }

    #[test]
    fn session_history_uses_shared_context_limit() {
        let now = at(12, 0);
        let mut input = base_input();
        input.message = DialogMessage {
            id: 101,
            text: "current".to_owned(),
            timestamp: Some(now + Duration::minutes(1)),
            ..DialogMessage::default()
        };
        input.history = (0..100)
            .map(|i| HistoryMessage {
                role: ROLE_USER.to_owned(),
                kind: MESSAGE_KIND_TEXT.to_owned(),
                name: "User".to_owned(),
                text: "message".to_owned(),
                timestamp: Some(now - Duration::minutes(i)),
                message_id: 100 - i as i32,
                ..HistoryMessage::default()
            })
            .collect();

        let history = build_session_history_with_limit(&input, DEFAULT_CONTEXT_HISTORY_LIMIT);
        assert_eq!(history.len(), DEFAULT_CONTEXT_HISTORY_LIMIT);
        assert_eq!(history[0].message_id, 87);
        assert_eq!(history[history.len() - 2].message_id, 100);
        assert_eq!(history[history.len() - 1].message_id, 101);
    }

    #[tokio::test]
    async fn chat_step_parses_native_tool_calls_with_ids_and_intermediate_text()
    -> Result<(), CompletionError> {
        let (provider, transport, toolbox) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "щас гляну",
                        "tool_calls": [{
                            "id": "call-abc",
                            "type": "function",
                            "function": {
                                "name": "web_search",
                                "arguments": "{\"query\": \"когда солнцестояние\"}"
                            }
                        }]
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let request = openplotva_dialog::ChatStepRequest {
            input: base_input(),
            transcript: Vec::new(),
            tools: openplotva_dialog::ToolsMode::Native(
                native_tool_values(&[STEP_WEB_SEARCH]).expect("tool defs"),
            ),
            iteration: 1,
        };

        let output = crate::ChatStepProvider::run_chat_step(&provider, request).await?;

        assert_eq!(output.text, "щас гляну");
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.tool_calls[0].id, "call-abc");
        assert_eq!(output.tool_calls[0].step.step, STEP_WEB_SEARCH);
        assert_eq!(output.tool_calls[0].step.query, "когда солнцестояние");
        assert!(!output.tool_calls[0].salvaged);
        assert!(output.trace.is_some());
        // The step never executes tools — the loop belongs to the session engine.
        assert!(toolbox.web_search_queries().is_empty());
        let sent = transport.requests();
        assert_eq!(sent.len(), 1);
        let body: Value = serde_json::from_slice(&sent[0].body).expect("request json");
        assert!(
            body.get("tools")
                .and_then(Value::as_array)
                .is_some_and(|tools| !tools.is_empty()),
            "native step carries the tools array"
        );
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_keeps_native_tool_and_drops_reasoning_only_intermediate()
    -> Result<(), CompletionError> {
        let (provider, _transport, _toolbox) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "thought\n<channel|>",
                        "tool_calls": [{
                            "id": "call-react",
                            "type": "function",
                            "function": {
                                "name": "react_to_message",
                                "arguments": "{\"emoji\": \"😂\", \"message_id\": 6877}"
                            }
                        }]
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    openplotva_dialog::chat_completion_tools_for_specs(&[
                        SESSION_REACT_TO_MESSAGE_SPEC,
                    ])
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await?;

        assert_eq!(output.text, "");
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.tool_calls[0].id, "call-react");
        assert_eq!(output.tool_calls[0].step.step, STEP_REACT_TO_MESSAGE);
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_synthesizes_duplicate_native_call_ids_deterministically()
    -> Result<(), CompletionError> {
        let (provider, _transport, _toolbox) = direct_dialog_provider(
            json!({
                "choices": [{"message": {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {"id": "dup", "type": "function", "function": {
                            "name": "web_search", "arguments": "{\"query\":\"one\"}"
                        }},
                        {"id": "dup", "type": "function", "function": {
                            "name": "web_search", "arguments": "{\"query\":\"two\"}"
                        }}
                    ]
                }}]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    native_tool_values(&[STEP_WEB_SEARCH]).expect("tool defs"),
                ),
                iteration: 2,
            },
        )
        .await?;

        assert_eq!(output.tool_calls.len(), 2);
        assert_eq!(output.tool_calls[0].id, "dup");
        assert_eq!(output.tool_calls[1].id, "call-2-1");
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_native_tool_parse_errors_are_retryable_protocol_failures() {
        let (provider, _transport, _toolbox) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [{
                            "id": "call-bad",
                            "type": "function",
                            "function": {
                                "name": "history_search (retry with shorter query)",
                                "arguments": "{\"query\": \"CherryCherry123\"}"
                            }
                        }]
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );

        let error = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    native_tool_values(&[STEP_HISTORY_SEARCH]).expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await
        .expect_err("malformed native tool call should be retryable provider protocol error");

        assert_eq!(
            crate::retry::retryable_reason(error.as_ref()),
            Some(FailureReason::ProviderProtocolError)
        );
        assert!(error.to_string().contains("tool protocol error"), "{error}");
    }

    #[tokio::test]
    async fn chat_step_final_only_never_yields_tool_calls_or_empty_text()
    -> Result<(), CompletionError> {
        // The forced-final pass must not interpret tool-call markup: the
        // session engine reads only `text` there, so salvaged calls with an
        // emptied text would look like an empty provider answer.
        let response = json!({
            "choices": [{"message": {
                "role": "assistant",
                "content": "почти забыла <tool_call>{\"name\": \"web_search\", \"arguments\": {\"query\": \"солнцестояние\"}}</tool_call>"
            }}]
        });
        let (provider, _transport, _) =
            direct_dialog_provider(response, AifarmDialogConfig::default());

        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::FinalOnly,
                iteration: 3,
            },
        )
        .await?;

        assert!(output.tool_calls.is_empty(), "{:?}", output.tool_calls);
        assert_eq!(output.text, "почти забыла");
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_final_only_keeps_the_system_prompt_and_drops_the_tools_array()
    -> Result<(), CompletionError> {
        let transcript = vec![
            openplotva_dialog::SessionMessage::Assistant {
                text: "щас гляну".to_owned(),
                tool_calls: vec![openplotva_dialog::SessionToolCall {
                    id: "call-abc".to_owned(),
                    name: STEP_WEB_SEARCH.to_owned(),
                    arguments: json!({"query": "солнцестояние"}),
                }],
            },
            openplotva_dialog::SessionMessage::ToolResult {
                tool_call_id: "call-abc".to_owned(),
                name: STEP_WEB_SEARCH.to_owned(),
                content: "{\"status\":\"ok\"}".to_owned(),
            },
            openplotva_dialog::SessionMessage::InjectedUser {
                rendered: "<message id=\"7\">ну что там?</message>".to_owned(),
            },
        ];
        let text_response = json!({
            "choices": [{"message": {"role": "assistant", "content": "готово"}}]
        });

        let (native_provider, native_transport, _) =
            direct_dialog_provider(text_response.clone(), AifarmDialogConfig::default());
        crate::ChatStepProvider::run_chat_step(
            &native_provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: transcript.clone(),
                tools: openplotva_dialog::ToolsMode::Native(
                    native_tool_values(&[STEP_WEB_SEARCH]).expect("tool defs"),
                ),
                iteration: 2,
            },
        )
        .await?;

        let (final_provider, final_transport, _) =
            direct_dialog_provider(text_response, AifarmDialogConfig::default());
        let output = crate::ChatStepProvider::run_chat_step(
            &final_provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript,
                tools: openplotva_dialog::ToolsMode::FinalOnly,
                iteration: 3,
            },
        )
        .await?;
        assert_eq!(output.text, "готово");
        assert!(output.tool_calls.is_empty());

        let native_body: Value =
            serde_json::from_slice(&native_transport.requests()[0].body).expect("native json");
        let final_body: Value =
            serde_json::from_slice(&final_transport.requests()[0].body).expect("final json");
        // Forced-final keeps the tool-aware system prompt byte-identical so the
        // prompt-cache prefix survives; only the request-level tools differ.
        assert_eq!(
            native_body["messages"][0], final_body["messages"][0],
            "system prompt must not change between native and final-only steps"
        );
        assert!(
            final_body
                .get("tools")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty),
            "final-only step omits the tools array"
        );

        // Native transcript tail: assistant tool_calls with preserved ids and
        // string-encoded arguments, tool-role result, injected user message.
        let native_messages = native_body["messages"].as_array().expect("messages");
        let tail = &native_messages[native_messages.len() - 3..];
        assert_eq!(tail[0]["role"], "assistant");
        assert_eq!(tail[0]["content"], "щас гляну");
        assert_eq!(tail[0]["tool_calls"][0]["id"], "call-abc");
        assert_eq!(
            tail[0]["tool_calls"][0]["function"]["arguments"],
            "{\"query\":\"солнцестояние\"}"
        );
        assert_eq!(tail[1]["role"], "tool");
        assert_eq!(tail[1]["tool_call_id"], "call-abc");
        assert_eq!(tail[2]["role"], "user");

        // Final-only transcript renders as plain-text context blocks instead.
        let final_messages = final_body["messages"].as_array().expect("messages");
        let rendered = final_messages
            .iter()
            .map(|message| message["content"].as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("<tool_result name=\"web_search\" ref=\"call-abc\">"));
        assert!(
            final_messages
                .iter()
                .all(|message| message.get("tool_call_id").is_none()),
            "no native tool roles without a tools array"
        );
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_salvages_content_tool_calls_and_strips_them_from_text()
    -> Result<(), CompletionError> {
        let (provider, _transport, _) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<tool_call>draw_image{prompt:\"cat\"}</tool_call>"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    native_tool_values(&[STEP_DRAW_IMAGE]).expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await?;

        assert_eq!(output.tool_calls.len(), 1);
        assert!(output.tool_calls[0].salvaged);
        assert_eq!(output.tool_calls[0].step.step, STEP_DRAW_IMAGE);
        assert_eq!(output.tool_calls[0].step.prompt, "cat");
        assert!(!output.tool_calls[0].id.trim().is_empty());
        assert_eq!(
            output.text, "",
            "salvaged tool markup must not leak into the chat text"
        );
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_salvages_direct_session_tool_tags() -> Result<(), CompletionError> {
        let (provider, _transport, _) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<react_to_message chat_id=\"-1001680667629\" emoji=\"🤣\" message_id=\"316691\" />"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    openplotva_dialog::chat_completion_tools_for_specs(&[
                        SESSION_REACT_TO_MESSAGE_SPEC,
                    ])
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await?;

        assert_eq!(output.tool_calls.len(), 1);
        assert!(output.tool_calls[0].salvaged);
        assert_eq!(output.tool_calls[0].step.step, STEP_REACT_TO_MESSAGE);
        assert_eq!(output.tool_calls[0].step.emoji, "🤣");
        assert_eq!(output.tool_calls[0].step.target_chat_id, -1001680667629);
        assert_eq!(output.tool_calls[0].step.target_message_id, 316691);
        assert_eq!(
            output.text, "",
            "salvaged session tool markup must not leak into the chat text"
        );
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_salvages_multiple_direct_session_tool_tags() -> Result<(), CompletionError> {
        let (provider, _transport, _) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<react_to_message chat_id=\"-1001680667629\" emoji=\"🤣\" message_id=\"316691\" />\n<send_message text=\"Проверяю.\" />"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    openplotva_dialog::chat_completion_tools_for_specs(&[
                        SESSION_REACT_TO_MESSAGE_SPEC,
                        SESSION_SEND_MESSAGE_SPEC,
                    ])
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await?;

        assert_eq!(output.tool_calls.len(), 2);
        assert!(output.tool_calls.iter().all(|call| call.salvaged));
        assert_eq!(output.tool_calls[0].step.step, STEP_REACT_TO_MESSAGE);
        assert_eq!(output.tool_calls[0].step.emoji, "🤣");
        assert_eq!(output.tool_calls[0].step.target_message_id, 316691);
        assert_eq!(output.tool_calls[1].step.step, STEP_SEND_MESSAGE);
        assert_eq!(output.tool_calls[1].step.text, "Проверяю.");
        assert_eq!(
            output.text, "",
            "salvaged session tool markup must not leak into the chat text"
        );
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_salvages_production_named_call_sequence() -> Result<(), CompletionError> {
        let (provider, _transport, _) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<|channel>thought\n<channel|><call>\n  <tool_name>react_to_message</tool_name>\n  <arguments><emoji>🤣</emoji><message_id>999948</message_id></arguments>\n</call>\n<call>\n  <tool_name>react_to_message</tool_name>\n  <arguments><emoji>😂</emoji><message_id>999949</message_id></arguments>\n</call>\n\nНу и за что тебе такое наказание божье?"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    openplotva_dialog::chat_completion_tools_for_specs(&[
                        SESSION_REACT_TO_MESSAGE_SPEC,
                    ])
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await?;

        assert_eq!(output.tool_calls.len(), 2);
        assert!(output.tool_calls.iter().all(|call| call.salvaged));
        assert_eq!(output.tool_calls[0].step.emoji, "🤣");
        assert_eq!(output.tool_calls[0].step.target_message_id, 999948);
        assert_eq!(output.tool_calls[1].step.emoji, "😂");
        assert_eq!(output.tool_calls[1].step.target_message_id, 999949);
        assert_eq!(output.text, "Ну и за что тебе такое наказание божье?");
        Ok(())
    }

    #[tokio::test]
    async fn chat_step_malformed_named_call_is_retryable_protocol_failure() {
        let (provider, _transport, _) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<call><tool_name>react_to_message</tool_name><arguments><emoji>😂</emoji>"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );

        let error = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    openplotva_dialog::chat_completion_tools_for_specs(&[
                        SESSION_REACT_TO_MESSAGE_SPEC,
                    ])
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await
        .expect_err("malformed named call must be a retryable provider protocol error");

        assert_eq!(
            crate::retry::retryable_reason(error.as_ref()),
            Some(FailureReason::ProviderProtocolError)
        );
        assert!(error.to_string().contains("tool protocol error"), "{error}");
    }

    #[tokio::test]
    async fn chat_step_salvages_production_nested_video_preamble_sequence()
    -> Result<(), CompletionError> {
        let (provider, _transport, _) = direct_dialog_provider(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<send_message><text>Так, сейчас гляну, что там за видео</text></send_message><understand_media><file_id>message_1110901_video_1</file_id></understand_media>"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
        );
        let output = crate::ChatStepProvider::run_chat_step(
            &provider,
            openplotva_dialog::ChatStepRequest {
                input: base_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Native(
                    openplotva_dialog::chat_completion_tools_for_specs(&[
                        SESSION_SEND_MESSAGE_SPEC,
                        alternative_dialog_tools()
                            .into_iter()
                            .find(|tool| tool.name == STEP_UNDERSTAND_MEDIA)
                            .expect("vision tool"),
                    ])
                    .into_iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<_>, _>>()
                    .expect("tool defs"),
                ),
                iteration: 1,
            },
        )
        .await?;

        assert_eq!(output.text, "");
        assert_eq!(output.tool_calls.len(), 2);
        assert_eq!(output.tool_calls[0].step.step, STEP_SEND_MESSAGE);
        assert_eq!(output.tool_calls[1].step.step, STEP_UNDERSTAND_MEDIA);
        assert!(output.tool_calls.iter().all(|call| call.salvaged));
        Ok(())
    }

    #[test]
    fn dialog_request_prefers_input_enable_thinking_over_config() -> Result<(), AifarmDialogError> {
        let provider = AifarmDialogProvider::with_transport(
            AifarmDialogConfig::default(),
            FakeTransport::new(Vec::new()),
        );
        let mut input = base_input();
        input.enable_thinking = Some(true);

        let history = build_session_history_with_limit(&input, provider.cfg.max_history);
        let request = provider.step_request_with_history(
            &input,
            &history,
            &[],
            &openplotva_dialog::ToolsMode::FinalOnly,
            1,
        )?;

        assert_eq!(
            request.chat_template_kwargs,
            Some(json!({ "enable_thinking": true }))
        );
        Ok(())
    }
}
