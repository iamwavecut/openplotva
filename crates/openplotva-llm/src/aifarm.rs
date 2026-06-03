//! AIFarm/OpenAI-compatible dialog message formatting.

use std::error::Error;
use std::fmt::Write as _;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, Ordering},
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

use openplotva_core::{ChatAttachment, ChatMessageMeta, ToolCall};
use openplotva_dialog::{
    DEFAULT_CONTEXT_HISTORY_LIMIT, DialogInput, DialogToolbox, DialogTraceArtifacts,
    DialogTraceError, DialogTraceUsage, DrawRequest, HistoryMessage, HistorySummaryRequest,
    MESSAGE_KIND_TEXT, MESSAGE_KIND_TOOL_REQUEST, MESSAGE_KIND_TOOL_RESPONSE, NativeToolCall,
    PROVIDER_AIFARM, PROVIDER_NVIDIA, PROVIDER_VMLX, ROLE_MODEL, ROLE_TOOL, ROLE_USER,
    STEP_CANCEL_DRAWING, STEP_CHAT_HISTORY_SUMMARY, STEP_CRAWL_URL, STEP_CURRENCY_RATES,
    STEP_DRAW_IMAGE, STEP_GENERATE_SONG, STEP_QUEUE_STATUS, STEP_TRANSLATE_TEXT, STEP_VISION_IMAGE,
    STEP_WEB_SEARCH, STEP_YOUTUBE_SUMMARY, SongRequest, TOOL_RESULT_STATUS_EXECUTED,
    TOOL_RESULT_STATUS_FAILED, TOOL_RESULT_STATUS_NOOP, TOOL_RESULT_STATUS_OK,
    TOOL_RESULT_STATUS_QUEUED, ToolContext, ToolError, ToolParseDecision, ToolResult, ToolSpec,
    ToolStep, VisionRequest, alternative_dialog_tool_names, alternative_dialog_tools,
    chat_completion_tools_for_names, clone_history_messages,
    decode_plotva_final_response_with_salvage, extract_content_tool_step,
    has_leading_context_message, is_dialog_history_noise_tool_call_name,
    is_internal_not_scheduled_instruction, normalize_history_message, parse_native_tool_step,
    sanitize_final_text, select_llm_history_messages_for_context, tool_telemetry,
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
const DEFAULT_POOL_PRIMARY_CAPACITY_WAIT: StdDuration = StdDuration::from_millis(500);
pub const DEFAULT_POOL_REASONING_MAX_TOKENS: i32 = 8192;
const DEFAULT_VRAM_CLOUD_TEMPERATURE: f64 = 0.7;
const DEFAULT_VRAM_CLOUD_TOP_P: f64 = 0.8;
const DEFAULT_VRAM_CLOUD_TOP_K: f64 = 20.0;
const DEFAULT_VRAM_CLOUD_PRESENCE_PENALTY: f64 = 1.5;
const DEFAULT_VRAM_CLOUD_REPETITION_PENALTY: f64 = 1.0;
const DEFAULT_VRAM_CLOUD_MAX_TOKENS: i32 = 768;

static POOL_ENABLED: AtomicBool = AtomicBool::new(true);
static POOL_REASONING_ENABLED: AtomicBool = AtomicBool::new(false);
static POOL_REASONING_MAX_TOKENS: AtomicI64 =
    AtomicI64::new(DEFAULT_POOL_REASONING_MAX_TOKENS as i64);

pub fn set_pool_enabled(enabled: bool) {
    POOL_ENABLED.store(enabled, Ordering::SeqCst);
}

#[must_use]
pub fn pool_enabled() -> bool {
    POOL_ENABLED.load(Ordering::SeqCst)
}

pub fn set_pool_reasoning_enabled(enabled: bool) {
    POOL_REASONING_ENABLED.store(enabled, Ordering::SeqCst);
}

#[must_use]
pub fn pool_reasoning_enabled() -> bool {
    POOL_REASONING_ENABLED.load(Ordering::SeqCst)
}

pub fn set_pool_reasoning_max_tokens(max_tokens: i32) {
    POOL_REASONING_MAX_TOKENS.store(i64::from(max_tokens.max(0)), Ordering::SeqCst);
}

#[must_use]
pub fn pool_reasoning_max_tokens() -> i32 {
    i32::try_from(POOL_REASONING_MAX_TOKENS.load(Ordering::SeqCst)).unwrap_or(i32::MAX)
}

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
}

impl Serialize for ChatMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("ChatMessage", 2)?;
        state.serialize_field("role", &self.role)?;
        if self.content_parts.is_empty() {
            state.serialize_field("content", &self.content)?;
        } else {
            state.serialize_field("content", &self.content_parts)?;
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
}

impl AifarmHttpClient<ReqwestAifarmTransport> {
    #[must_use]
    pub fn new(cfg: AifarmClientConfig) -> Self {
        let cfg = cfg.with_defaults();
        Self {
            cfg,
            transport: ReqwestAifarmTransport::default(),
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
        }
    }

    /// Complete using direct endpoint when configured, otherwise Discovery.
    pub async fn complete(
        &self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        let request = request_with_default_model(&request, &self.cfg.default_model);
        if self.uses_direct_endpoint() {
            self.complete_direct_with_job_id(request, &generated_dialog_job_id(), on_status)
                .await
        } else {
            self.complete_discovery_with_job_id(request, &generated_dialog_job_id(), on_status)
                .await
        }
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
        let response = self
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Post,
                url: self.cfg.direct_url.clone(),
                headers: direct_completion_headers(&self.cfg, &request),
                body,
            })
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
        loop {
            match self.submit_discovery(request, job_id).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retryable_reason(err.as_ref()) != Some(FailureReason::CapacityUnavailable) {
                        return Err(err);
                    }
                    if self.cfg.fail_fast_on_capacity_unavailable {
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
        loop {
            match self.submit_json_discovery(request, job_id).await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retryable_reason(err.as_ref()) != Some(FailureReason::CapacityUnavailable) {
                        return Err(err);
                    }
                    if self.cfg.fail_fast_on_capacity_unavailable {
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
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Post,
                url: self.cfg.endpoint("/v1/jobs/blocking"),
                headers: [("Content-Type".to_owned(), "application/json".to_owned())].into(),
                body,
            })
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
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Post,
                url: self.cfg.endpoint("/v1/jobs/blocking"),
                headers: [("Content-Type".to_owned(), "application/json".to_owned())].into(),
                body,
            })
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
        loop {
            tokio::time::sleep(nonzero_duration(
                self.cfg.poll_interval,
                StdDuration::from_secs(1),
            ))
            .await;
            let status = self.check_discovery_status(job_id).await?;
            let normalized = normalize_status(&status.status);
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
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Get,
                url: self.cfg.endpoint(&format!("/v1/jobs/{}", job_id.trim())),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
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
        loop {
            let response = self.check_direct_status(request_id).await?;
            if response.status_code == 202 {
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
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Get,
                url: direct_status_endpoint(&self.cfg.direct_url, request_id),
                headers: direct_status_headers(&self.cfg),
                body: Vec::new(),
            })
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
    pub max_iterations: usize,
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
    pub use_tool_calls: Option<bool>,
    /// Whether model thinking is enabled.
    pub enable_thinking: Option<bool>,
    /// Whether reasoning output is included.
    pub include_reasoning: Option<bool>,
    pub pool: AifarmPoolConfig,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmPoolConfig {
    /// Secondary backends.
    pub secondary_backends: Vec<AifarmPoolBackendConfig>,
    /// Fallback API key for secondary backends.
    pub secondary_api_key: String,
    /// Primary queued-capacity wait before falling through to secondaries.
    pub primary_capacity_wait: StdDuration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AifarmPoolBackendConfig {
    /// Backend status/trace name.
    pub name: String,
    /// Base URL normalized to `/chat/completions` when `url` is empty.
    pub base_url: String,
    /// Explicit direct chat-completions URL.
    pub url: String,
    /// Per-backend API key.
    pub api_key: String,
    /// Secondary model.
    pub model: String,
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
    pub pool: AifarmPoolConfig,
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
    /// Whether model thinking is enabled.
    pub enable_thinking: Option<bool>,
    /// Whether reasoning output is included.
    pub include_reasoning: Option<bool>,
    pub pool: AifarmPoolConfig,
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

impl AifarmPoolConfig {
    #[must_use]
    pub fn from_go_lists(
        models: &[String],
        base_urls: &[String],
        api_key: &str,
        primary_capacity_wait: StdDuration,
    ) -> Self {
        let count = models.len().min(base_urls.len());
        let mut secondary_backends = Vec::with_capacity(count);
        for index in 0..count {
            let model = models[index].trim();
            let base_url = base_urls[index].trim();
            if model.is_empty() || base_url.is_empty() {
                continue;
            }
            secondary_backends.push(AifarmPoolBackendConfig {
                name: model.to_owned(),
                base_url: base_url.to_owned(),
                model: model.to_owned(),
                ..AifarmPoolBackendConfig::default()
            });
        }
        Self {
            secondary_backends,
            secondary_api_key: api_key.trim().to_owned(),
            primary_capacity_wait: default_duration(
                primary_capacity_wait,
                DEFAULT_POOL_PRIMARY_CAPACITY_WAIT,
            ),
        }
    }

    fn enabled(&self) -> bool {
        !self.secondary_backends.is_empty()
    }
}

impl AifarmDialogConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.provider_name = default_string(&self.provider_name, PROVIDER_AIFARM);
        self.model = default_string(&self.model, DEFAULT_MODEL_NAME);
        self.max_iterations = if self.max_iterations == 0 {
            8
        } else {
            self.max_iterations
        };
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
        if self.use_tool_calls.is_none()
            && self
                .provider_name
                .trim()
                .eq_ignore_ascii_case(PROVIDER_AIFARM)
        {
            self.use_tool_calls = Some(false);
        }
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

    fn use_tool_calls(&self) -> bool {
        self.use_tool_calls.unwrap_or(false)
    }
}

/// HTTP-backed AIFarm dialog provider.
#[derive(Clone)]
pub struct AifarmDialogProvider<T = ReqwestAifarmTransport> {
    cfg: AifarmDialogConfig,
    client: AifarmCompletionClient<T>,
    provider_name: String,
    toolbox: Option<Arc<dyn DialogToolbox>>,
}

#[derive(Clone, Debug)]
enum AifarmCompletionClient<T = ReqwestAifarmTransport> {
    Single(Box<AifarmHttpClient<T>>),
    Pooled(Box<AifarmHttpPoolClient<T>>),
}

#[derive(Clone, Debug)]
struct AifarmHttpPoolClient<T = ReqwestAifarmTransport> {
    primary: AifarmHttpClient<T>,
    primary_wait: AifarmHttpClient<T>,
    primary_name: String,
    secondaries: Vec<AifarmHttpPoolBackend<T>>,
}

#[derive(Clone, Debug)]
struct AifarmHttpPoolBackend<T = ReqwestAifarmTransport> {
    name: String,
    model: String,
    client: AifarmHttpClient<T>,
}

/// HTTP-backed AIFarm history-summary generator.
#[derive(Clone)]
pub struct AifarmHistorySummaryGenerator<T = ReqwestAifarmTransport> {
    cfg: AifarmHistorySummaryConfig,
    client: AifarmCompletionClient<T>,
}

/// HTTP-backed AIFarm memory extractor.
#[derive(Clone)]
pub struct AifarmMemoryExtractor<T = ReqwestAifarmTransport> {
    cfg: AifarmMemoryExtractorConfig,
    client: AifarmCompletionClient<T>,
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
        let client = AifarmCompletionClient::with_transport(
            cfg.client.clone(),
            cfg.pool.clone(),
            ReqwestAifarmTransport::default(),
        );
        Self { cfg, client }
    }
}

impl AifarmMemoryExtractor<ReqwestAifarmTransport> {
    /// Build a reqwest-backed memory extractor.
    #[must_use]
    pub fn new(cfg: AifarmMemoryExtractorConfig) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmCompletionClient::with_transport(
            cfg.client.clone(),
            cfg.pool.clone(),
            ReqwestAifarmTransport::default(),
        );
        Self { cfg, client }
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
        let result = self
            .client
            .complete(self.request(request), on_status)
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

impl<T> AifarmCompletionClient<T>
where
    T: AifarmHttpTransport + Clone,
{
    fn with_transport(cfg: AifarmClientConfig, pool: AifarmPoolConfig, transport: T) -> Self {
        if !pool.enabled() {
            return Self::Single(Box::new(AifarmHttpClient::with_transport(cfg, transport)));
        }
        let pooled = AifarmHttpPoolClient::with_transport(cfg.clone(), pool, transport.clone());
        if pooled.secondaries.is_empty() {
            Self::Single(Box::new(AifarmHttpClient::with_transport(cfg, transport)))
        } else {
            Self::Pooled(Box::new(pooled))
        }
    }

    async fn complete(
        &self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        match self {
            Self::Single(client) => client.complete(request, on_status).await,
            Self::Pooled(client) => client.complete(request, on_status).await,
        }
    }
}

impl<T> AifarmHttpPoolClient<T>
where
    T: AifarmHttpTransport + Clone,
{
    fn with_transport(cfg: AifarmClientConfig, pool: AifarmPoolConfig, transport: T) -> Self {
        let mut primary_cfg = cfg.clone();
        primary_cfg.fail_fast_on_capacity_unavailable = true;
        primary_cfg.capacity_wait = default_duration(
            pool.primary_capacity_wait,
            DEFAULT_POOL_PRIMARY_CAPACITY_WAIT,
        );
        let primary = AifarmHttpClient::with_transport(primary_cfg, transport.clone());
        let primary_wait = AifarmHttpClient::with_transport(cfg.clone(), transport.clone());
        let secondaries = pool
            .secondary_backends
            .into_iter()
            .filter_map(|backend| {
                let model = backend.model.trim().to_owned();
                if model.is_empty() {
                    return None;
                }
                let direct_url = if backend.url.trim().is_empty() {
                    normalize_chat_completions_url(&backend.base_url)
                } else {
                    backend.url.trim().to_owned()
                };
                if direct_url.trim().is_empty() {
                    return None;
                }
                let api_key = default_string(&backend.api_key, &pool.secondary_api_key);
                if api_key.trim().is_empty() {
                    return None;
                }
                let name = default_string(&backend.name, &model);
                let mut secondary_cfg = cfg.clone();
                secondary_cfg.direct_url = direct_url;
                secondary_cfg.api_key = api_key;
                secondary_cfg.default_model = model.clone();
                secondary_cfg.fail_fast_on_capacity_unavailable = false;
                Some(AifarmHttpPoolBackend {
                    name,
                    model,
                    client: AifarmHttpClient::with_transport(secondary_cfg, transport.clone()),
                })
            })
            .collect();
        Self {
            primary,
            primary_wait,
            primary_name: PROVIDER_AIFARM.to_owned(),
            secondaries,
        }
    }

    async fn complete(
        &self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        if !pool_enabled() {
            return self.primary_wait.complete(request, on_status).await;
        }

        let primary_name = self.resolved_primary_name().to_owned();
        let primary_result = {
            let mut status = |status| {
                emit_backend_status(on_status, &primary_name, &request.model, status);
            };
            self.primary.complete(request.clone(), &mut status).await
        };
        let primary_err = match primary_result {
            Ok(result) => return Ok(result),
            Err(err) if self.secondaries.is_empty() => return Err(err),
            Err(err) => err,
        };
        let Some(primary_reason) = retryable_reason(primary_err.as_ref()) else {
            return Err(primary_err);
        };
        let mut errors = vec![format!("{primary_name}: {primary_err}")];
        emit_pool_fallback(on_status, &primary_name, &request.model);

        if let Some(result) = self
            .try_secondary_backends(&request, on_status, primary_reason, &mut errors)
            .await?
        {
            return Ok(result);
        }

        if primary_reason == FailureReason::CapacityUnavailable {
            emit_primary_wait(on_status, &primary_name, &request.model);
            let mut status =
                |status| emit_backend_status(on_status, &primary_name, &request.model, status);
            match self
                .primary_wait
                .complete(request.clone(), &mut status)
                .await
            {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retryable_reason(err.as_ref()).is_none() {
                        return Err(err);
                    }
                    errors.push(format!("{primary_name} queued wait: {err}"));
                }
            }
        }

        Err(Box::new(ProviderError::new(
            "aifarm_pool",
            FailureReason::ProviderUnavailable,
            errors.join("\n"),
        )))
    }

    async fn try_secondary_backends(
        &self,
        request: &ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
        primary_reason: FailureReason,
        errors: &mut Vec<String>,
    ) -> Result<Option<CompletionResult>, CompletionError> {
        for index in self.shuffled_secondary_indices() {
            let Some(backend) = self.secondaries.get(index) else {
                continue;
            };
            let direct_request = direct_compatible_request(request, &backend.model);
            let mut status = |status| {
                emit_backend_status(on_status, &backend.name, &backend.model, status);
            };
            match backend.client.complete(direct_request, &mut status).await {
                Ok(result) => return Ok(Some(result)),
                Err(err) => {
                    let retryable = retryable_reason(err.as_ref()).is_some();
                    if !retryable && primary_reason != FailureReason::CapacityUnavailable {
                        return Err(err);
                    }
                    errors.push(format!("{}: {err}", backend.name));
                    emit_pool_fallback(on_status, &backend.name, &backend.model);
                }
            }
        }
        Ok(None)
    }

    fn shuffled_secondary_indices(&self) -> Vec<usize> {
        let mut indices = (0..self.secondaries.len()).collect::<Vec<_>>();
        indices.shuffle(&mut rand::rng());
        indices
    }

    fn resolved_primary_name(&self) -> &str {
        if self.primary_name.trim().is_empty() {
            PROVIDER_AIFARM
        } else {
            self.primary_name.trim()
        }
    }
}

impl<T> AifarmHistorySummaryGenerator<T>
where
    T: AifarmHttpTransport + Clone,
{
    /// Build with custom transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmHistorySummaryConfig, transport: T) -> Self {
        let cfg = cfg.with_defaults();
        let client =
            AifarmCompletionClient::with_transport(cfg.client.clone(), cfg.pool.clone(), transport);
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
        let request = self.request(&system_prompt, &payload);
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
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload.to_owned(),
                    content_parts: Vec::new(),
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
            request.chat_template_kwargs = Some(json!({ "enable_thinking": enable_thinking }));
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
        let client =
            AifarmCompletionClient::with_transport(cfg.client.clone(), cfg.pool.clone(), transport);
        Self { cfg, client }
    }

    pub async fn extract(
        &self,
        input: &ExtractInput,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<ExtractOutput, AifarmMemoryExtractorError> {
        let system_prompt = openplotva_prompts::read("memory/extraction")?;
        let payload =
            serde_json::to_string_pretty(input).map_err(AifarmMemoryExtractorError::Input)?;
        let result = self
            .client
            .complete(self.request(&system_prompt, &payload), on_status)
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
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload.to_owned(),
                    content_parts: Vec::new(),
                },
            ],
            stream: false,
            response_format: Some(memory_extraction_response_format()),
            max_tokens: self.cfg.max_output_tokens,
            temperature: self.cfg.temperature,
            include_reasoning: self.cfg.include_reasoning,
            ..ChatCompletionRequest::default()
        };
        if let Some(enable_thinking) = self.cfg.enable_thinking {
            request.chat_template_kwargs = Some(json!({ "enable_thinking": enable_thinking }));
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
        let payload = serde_json::to_string_pretty(input)
            .map_err(GenkitOpenAiCompatibleMemoryExtractorError::Input)?;
        Ok(ChatCompletionRequest {
            model: self.cfg.model.trim().to_owned(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt,
                    content_parts: Vec::new(),
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload,
                    content_parts: Vec::new(),
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
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: payload,
                    content_parts: Vec::new(),
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
                        },
                        ChatMessage {
                            role: "user".to_owned(),
                            content: payload,
                            content_parts: Vec::new(),
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
        let client = AifarmCompletionClient::with_transport(
            cfg.client.clone(),
            cfg.pool.clone(),
            ReqwestAifarmTransport::default(),
        );
        Self {
            cfg,
            client,
            provider_name,
            toolbox: None,
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
        let client =
            AifarmCompletionClient::with_transport(cfg.client.clone(), cfg.pool.clone(), transport);
        Self {
            cfg,
            client,
            provider_name,
            toolbox: None,
        }
    }

    /// Attach the provider-neutral local dialog toolbox.
    #[must_use]
    pub fn with_toolbox(mut self, toolbox: Arc<dyn DialogToolbox>) -> Self {
        self.toolbox = Some(toolbox);
        self
    }

    /// Stable provider name.
    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider_name
    }

    /// Run one HTTP-backed dialog request and return a provider-neutral output.
    pub async fn run_with_status(
        &self,
        input: DialogInput,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<openplotva_dialog::DialogOutput, CompletionError> {
        self.validate_run()?;
        let mut state = AifarmDialogRunState::new(&self.cfg, input);
        for iteration in 1..=self.cfg.max_iterations {
            let request = match self.iteration_request_with_history(
                &state.input,
                &state.session_history,
                iteration,
            ) {
                Ok(request) => request,
                Err(error) => {
                    return Err(aifarm_dialog_error_with_traces(Box::new(error), &state));
                }
            };
            let trace_request = redacted_chat_completion_request(&request);
            let result = match self.client.complete(request, on_status).await {
                Ok(result) => result,
                Err(error) => {
                    let message = error.to_string();
                    let mut trace = aifarm_dialog_trace_artifacts(
                        &trace_request,
                        &CompletionResult::default(),
                        &state.input,
                        self.provider(),
                        iteration,
                    );
                    trace.error = message;
                    state.trace_events.push(trace);
                    return Err(aifarm_dialog_error_with_traces(error, &state));
                }
            };
            match self
                .handle_completion_result(&mut state, iteration, trace_request, result)
                .await
            {
                Ok(Some(output)) => return Ok(output),
                Ok(None) => {}
                Err(error) => {
                    let message = error.to_string();
                    if let Some(trace) = state.trace_events.last_mut()
                        && trace.error.trim().is_empty()
                    {
                        trace.error = message;
                    }
                    return Err(aifarm_dialog_error_with_traces(error, &state));
                }
            }
        }
        let error = Box::new(AifarmDialogError::Response(format!(
            "tool protocol error: final text was not produced within {} iterations",
            self.cfg.max_iterations
        ))) as CompletionError;
        Err(aifarm_dialog_error_with_traces(error, &state))
    }

    #[cfg(test)]
    fn iteration_request(
        &self,
        input: &DialogInput,
        iteration: usize,
    ) -> Result<ChatCompletionRequest, AifarmDialogError> {
        let history = build_session_history_with_limit(input, self.cfg.max_history);
        self.iteration_request_with_history(input, &history, iteration)
    }

    fn iteration_request_with_history(
        &self,
        input: &DialogInput,
        history: &[HistoryMessage],
        iteration: usize,
    ) -> Result<ChatCompletionRequest, AifarmDialogError> {
        let model = self.cfg.model_for_input(input);
        let tool_names = tool_names_for_iteration(input, iteration, self.cfg.max_iterations);
        let mode = tool_prompt_mode_for_request(self.cfg.use_tool_calls(), &tool_names);
        let messages = build_initial_messages_with_tool_prompt(input, history, mode)?;
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
        if self.cfg.use_tool_calls() && !tool_names.is_empty() {
            request.tools = native_tool_values(&tool_names)?;
            request.tool_choice = Some(Value::String("auto".to_owned()));
            request.parallel_tool_calls = Some(false);
        }
        if let Some(enable_thinking) = self.cfg.enable_thinking {
            request.chat_template_kwargs = Some(json!({ "enable_thinking": enable_thinking }));
        }
        Ok(request)
    }

    fn validate_run(&self) -> Result<(), CompletionError> {
        if self.toolbox.is_none() {
            return Err(Box::new(AifarmDialogError::Response(format!(
                "{} dialog toolbox is not configured",
                PROVIDER_AIFARM
            ))));
        }
        Ok(())
    }

    async fn handle_completion_result(
        &self,
        state: &mut AifarmDialogRunState,
        iteration: usize,
        trace_request: ChatCompletionRequest,
        result: CompletionResult,
    ) -> Result<Option<openplotva_dialog::DialogOutput>, CompletionError> {
        let trace = aifarm_dialog_trace_artifacts(
            &trace_request,
            &result,
            &state.input,
            self.provider(),
            iteration,
        );
        state.trace_events.push(trace);
        let Some(response) = result.response.as_ref() else {
            return Err(Box::new(AifarmDialogError::Response(
                "chat completion response is nil".to_owned(),
            )));
        };
        if state.input.disable_tools {
            return self.final_text_output(state, response).map(Some);
        }
        match first_choice_tool_steps(response)? {
            ToolStepSelection::None(decision) => {
                self.record_tool_parser_decision(state, iteration, &decision);
                self.final_text_output(state, response).map(Some)
            }
            ToolStepSelection::Steps(steps) => {
                for pending in steps {
                    let PendingToolStep {
                        step,
                        decision,
                        native_ref,
                    } = pending;
                    self.record_tool_parser_decision(state, iteration, &decision);
                    if let Some(output) = self
                        .execute_and_record_tool(
                            state,
                            iteration,
                            step,
                            decision.form.as_str(),
                            native_ref.as_deref(),
                        )
                        .await?
                    {
                        return Ok(Some(output));
                    }
                }
                Ok(None)
            }
        }
    }

    fn final_text_output(
        &self,
        state: &AifarmDialogRunState,
        response: &Value,
    ) -> Result<openplotva_dialog::DialogOutput, CompletionError> {
        let answer = extract_final_answer_for_provider(response, self.provider())?;
        Ok(openplotva_dialog::DialogOutput {
            provider: self.provider().to_owned(),
            response: answer.clone(),
            answer,
            tool_calls: state.tool_calls.clone(),
            trace: state.trace_events.last().cloned(),
            trace_events: state.trace_events.clone(),
            ..openplotva_dialog::DialogOutput::default()
        })
    }

    async fn execute_and_record_tool(
        &self,
        state: &mut AifarmDialogRunState,
        iteration: usize,
        step: ToolStep,
        form: &str,
        ref_hint: Option<&str>,
    ) -> Result<Option<openplotva_dialog::DialogOutput>, CompletionError> {
        let duplicate_first_ref = state.seen_single_effect_tool_request(&step)?;
        let result = if let Some(first_ref) = duplicate_first_ref.as_deref() {
            duplicate_tool_result(&step.step, first_ref)
        } else {
            match self.execute_tool(&state.meta, &step).await {
                Ok(result) => result,
                Err(error) => {
                    self.record_tool_parser_decision(
                        state,
                        iteration,
                        &ToolParseDecision {
                            form: form.to_owned(),
                            tool: step.step.clone(),
                            outcome: "error".to_owned(),
                            reason: error.to_string(),
                        },
                    );
                    return Err(error);
                }
            }
        };
        self.record_tool_parser_decision(
            state,
            iteration,
            &ToolParseDecision {
                form: form.to_owned(),
                tool: step.step.clone(),
                outcome: "executed".to_owned(),
                reason: String::new(),
            },
        );
        let tool_call = recorded_tool_call_with_ref(&step, &result, ref_hint, iteration)?;
        if duplicate_first_ref.is_none() {
            let remembered_ref = ref_hint
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&tool_call.r#ref);
            state.remember_single_effect_tool_request(&step, remembered_ref)?;
        }
        state.tool_calls.push(tool_call.clone());
        if let Some(answer) = immediate_tool_answer(&step, &result) {
            return Ok(Some(openplotva_dialog::DialogOutput {
                provider: self.provider().to_owned(),
                response: answer.clone(),
                answer,
                tool_calls: state.tool_calls.clone(),
                trace: state.trace_events.last().cloned(),
                trace_events: state.trace_events.clone(),
                ..openplotva_dialog::DialogOutput::default()
            }));
        }
        state.append_tool_history(tool_call);
        Ok(None)
    }

    fn record_tool_parser_decision(
        &self,
        state: &AifarmDialogRunState,
        iteration: usize,
        decision: &ToolParseDecision,
    ) {
        if decision.outcome.trim().is_empty() {
            return;
        }
        tool_telemetry::record(tool_telemetry::ToolTelemetryEvent {
            provider: self.provider().to_owned(),
            model: self.cfg.model_for_input(&state.input).trim().to_owned(),
            tool: decision.tool.trim().to_owned(),
            form: decision.form.trim().to_owned(),
            outcome: decision.outcome.trim().to_owned(),
            reason: decision.reason.trim().to_owned(),
            iteration: i32::try_from(iteration).unwrap_or(i32::MAX),
            ..tool_telemetry::ToolTelemetryEvent::default()
        });
    }

    async fn execute_tool(
        &self,
        meta: &ToolContext,
        step: &ToolStep,
    ) -> Result<ToolResult, CompletionError> {
        execute_dialog_tool(self.provider(), self.toolbox.as_ref(), meta, step).await
    }
}

pub(crate) async fn execute_dialog_tool(
    provider_name: &str,
    toolbox: Option<&Arc<dyn DialogToolbox>>,
    meta: &ToolContext,
    step: &ToolStep,
) -> Result<ToolResult, CompletionError> {
    let toolbox = toolbox.ok_or_else(|| {
        Box::new(AifarmDialogError::Response(format!(
            "{} dialog toolbox is not configured",
            provider_name.trim()
        ))) as CompletionError
    })?;
    match step.step.as_str() {
        STEP_DRAW_IMAGE => {
            if step.prompt.trim().is_empty() {
                return Err(Box::new(AifarmDialogError::Response(
                    "tool protocol error: draw_image prompt is empty".to_owned(),
                )));
            }
            toolbox
                .draw_image(DrawRequest {
                    context: meta.clone(),
                    prompt: step.prompt.clone(),
                    negative_prompt: step.negative_prompt.clone(),
                    aspect_ratio: step.aspect_ratio.clone(),
                    seed: step.seed.clone(),
                })
                .await
        }
        STEP_GENERATE_SONG => {
            if step.topic.trim().is_empty() {
                return Err(Box::new(AifarmDialogError::Response(
                    "tool protocol error: generate_song topic is empty".to_owned(),
                )));
            }
            toolbox
                .generate_song(SongRequest {
                    context: meta.clone(),
                    topic: step.topic.clone(),
                })
                .await
        }
        STEP_VISION_IMAGE => {
            let file_id = resolve_vision_tool_file_id(&step.file_id, meta)?;
            toolbox
                .vision_image(VisionRequest {
                    context: meta.clone(),
                    file_id,
                })
                .await
        }
        STEP_CURRENCY_RATES => toolbox.currency_rates(meta.clone()).await,
        STEP_WEB_SEARCH => {
            if step.query.trim().is_empty() {
                return Err(Box::new(AifarmDialogError::Response(
                    "tool protocol error: web_search query is empty".to_owned(),
                )));
            }
            toolbox.web_search(step.query.clone()).await
        }
        STEP_CRAWL_URL => {
            if step.url.trim().is_empty() {
                return Err(Box::new(AifarmDialogError::Response(
                    "tool protocol error: crawl_url url is empty".to_owned(),
                )));
            }
            toolbox.crawl_url(step.url.clone()).await
        }
        STEP_YOUTUBE_SUMMARY => {
            if step.video.trim().is_empty() {
                return Err(Box::new(AifarmDialogError::Response(
                    "tool protocol error: youtube_summary video is empty".to_owned(),
                )));
            }
            toolbox.youtube_summary(step.video.clone()).await
        }
        STEP_QUEUE_STATUS => toolbox.queue_status(meta.user_id).await,
        STEP_CANCEL_DRAWING => toolbox.cancel_drawing(meta.user_id, meta.chat_id).await,
        STEP_TRANSLATE_TEXT => {
            if step.text.trim().is_empty() {
                return Err(Box::new(AifarmDialogError::Response(
                    "tool protocol error: translate_text text is empty".to_owned(),
                )));
            }
            let target_lang = default_string(&step.target_lang, "ru");
            toolbox.translate_text(step.text.clone(), target_lang).await
        }
        STEP_CHAT_HISTORY_SUMMARY => {
            toolbox
                .chat_history_summary(HistorySummaryRequest {
                    context: meta.clone(),
                    window: step.window.clone(),
                    hours: step.hours,
                    message_count: step.message_count,
                    since: step.since.clone(),
                    scope: step.scope.clone(),
                })
                .await
        }
        other => Err(Box::new(AifarmDialogError::Response(format!(
            "tool protocol error: unsupported step {other:?}"
        )))),
    }
}

impl<T> crate::ChatProvider for AifarmDialogProvider<T>
where
    T: AifarmHttpTransport + Clone + Send + Sync,
{
    fn provider_name(&self) -> &str {
        self.provider()
    }

    fn run_dialog<'a>(&'a self, input: DialogInput) -> crate::ChatProviderFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_| {};
            self.run_with_status(input, &mut ignore_status).await
        })
    }
}

struct AifarmDialogRunState {
    input: DialogInput,
    session_history: Vec<HistoryMessage>,
    tool_calls: Vec<ToolCall>,
    trace_events: Vec<DialogTraceArtifacts>,
    meta: ToolContext,
    seen_tool_requests: BTreeMap<String, String>,
}

impl AifarmDialogRunState {
    fn new(cfg: &AifarmDialogConfig, input: DialogInput) -> Self {
        let session_history = build_session_history_with_limit(&input, cfg.max_history);
        let meta = tool_context_from_input(&input);
        Self {
            input,
            session_history,
            tool_calls: Vec::with_capacity(cfg.max_iterations.saturating_add(1)),
            trace_events: Vec::with_capacity(cfg.max_iterations),
            meta,
            seen_tool_requests: BTreeMap::new(),
        }
    }

    fn seen_single_effect_tool_request(
        &self,
        step: &ToolStep,
    ) -> Result<Option<String>, CompletionError> {
        let Some(key) = single_effect_tool_request_key(step)? else {
            return Ok(None);
        };
        Ok(self.seen_tool_requests.get(&key).cloned())
    }

    fn remember_single_effect_tool_request(
        &mut self,
        step: &ToolStep,
        r#ref: &str,
    ) -> Result<(), CompletionError> {
        let Some(key) = single_effect_tool_request_key(step)? else {
            return Ok(());
        };
        self.seen_tool_requests.insert(key, r#ref.trim().to_owned());
        Ok(())
    }

    fn append_tool_history(&mut self, tool_call: ToolCall) {
        let now = OffsetDateTime::now_utc();
        let thread_id = self.input.context.thread_id.unwrap_or_default();
        self.session_history.push(HistoryMessage {
            role: ROLE_MODEL.to_owned(),
            kind: MESSAGE_KIND_TOOL_REQUEST.to_owned(),
            name: self.input.context.bot_name.trim().to_owned(),
            timestamp: Some(now),
            message_id: self.input.message.id,
            thread_id,
            user_id: self.input.user.id,
            tool_call: Some(ToolCall {
                name: tool_call.name.clone(),
                r#ref: tool_call.r#ref.clone(),
                input: tool_call.input.clone(),
                at: tool_call.at.clone(),
                ..ToolCall::default()
            }),
            ..HistoryMessage::default()
        });
        self.session_history.push(HistoryMessage {
            role: ROLE_TOOL.to_owned(),
            kind: MESSAGE_KIND_TOOL_RESPONSE.to_owned(),
            name: tool_call.name.clone(),
            timestamp: Some(now),
            message_id: self.input.message.id,
            thread_id,
            tool_call: Some(tool_call),
            ..HistoryMessage::default()
        });
    }
}

fn aifarm_dialog_trace_artifacts(
    request: &ChatCompletionRequest,
    result: &CompletionResult,
    input: &DialogInput,
    provider: &str,
    iteration: usize,
) -> DialogTraceArtifacts {
    DialogTraceArtifacts {
        provider: provider.trim().to_owned(),
        request_kind: "openai.chat.completions".to_owned(),
        source: provider.trim().to_owned(),
        mode: "tools".to_owned(),
        flow: "dialog".to_owned(),
        iteration: i32::try_from(iteration).unwrap_or(i32::MAX),
        model: request.model.trim().to_owned(),
        raw_request: serde_json::to_value(request).ok(),
        raw_response: aifarm_trace_raw_response(result),
        transport: aifarm_trace_transport(result),
        inference_params: aifarm_trace_inference_params(request),
        usage: result.response.as_ref().and_then(aifarm_trace_usage),
        timings: result.response.as_ref().and_then(aifarm_trace_timings),
        prompt_chars: json_size(&request.messages),
        prompt_messages: i32::try_from(request.messages.len()).unwrap_or(i32::MAX),
        docs_chars: input
            .reference_context
            .iter()
            .map(String::len)
            .sum::<usize>()
            .min(i32::MAX as usize) as i32,
        ..DialogTraceArtifacts::default()
    }
}

fn aifarm_dialog_error_with_traces(
    error: CompletionError,
    state: &AifarmDialogRunState,
) -> CompletionError {
    if state.trace_events.is_empty() {
        return error;
    }
    Box::new(DialogTraceError::new(error, state.trace_events.clone()))
}

fn redacted_chat_completion_request(request: &ChatCompletionRequest) -> ChatCompletionRequest {
    let mut out = request.clone();
    for message in &mut out.messages {
        for part in &mut message.content_parts {
            let Some(image_url) = part.image_url.as_mut() else {
                continue;
            };
            if image_url.url.trim_start().starts_with("data:") {
                image_url.url = "data:<redacted-image>".to_owned();
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
        ..DialogTraceUsage::default()
    };
    (out.input_tokens != 0
        || out.output_tokens != 0
        || out.total_tokens != 0
        || out.cached_tokens != 0)
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

/// Completion client boundary used by pure pool routing.
pub trait CompletionClient {
    /// Complete a request.
    fn complete(
        &mut self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError>;
}

/// Secondary backend in the text pool.
pub struct PooledBackend {
    name: String,
    model: String,
    client: Box<dyn CompletionClient>,
}

impl PooledBackend {
    /// Build a pooled backend.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        model: impl Into<String>,
        client: Box<dyn CompletionClient>,
    ) -> Self {
        Self {
            name: name.into().trim().to_owned(),
            model: model.into().trim().to_owned(),
            client,
        }
    }
}

pub struct PooledClient {
    primary: Box<dyn CompletionClient>,
    primary_wait: Option<Box<dyn CompletionClient>>,
    primary_name: String,
    secondaries: Vec<PooledBackend>,
    secondary_order: Option<Vec<usize>>,
}

impl PooledClient {
    /// Build a pooled client.
    #[must_use]
    pub fn new(primary: Box<dyn CompletionClient>, secondaries: Vec<PooledBackend>) -> Self {
        Self {
            primary,
            primary_wait: None,
            primary_name: "aifarm".to_owned(),
            secondaries,
            secondary_order: None,
        }
    }

    /// Override queued primary-wait client.
    #[must_use]
    pub fn with_primary_wait(mut self, primary_wait: Box<dyn CompletionClient>) -> Self {
        self.primary_wait = Some(primary_wait);
        self
    }

    /// Override primary backend label.
    #[must_use]
    pub fn with_primary_name(mut self, primary_name: impl Into<String>) -> Self {
        self.primary_name = primary_name.into().trim().to_owned();
        self
    }

    #[must_use]
    pub fn with_secondary_order(mut self, secondary_order: Vec<usize>) -> Self {
        self.secondary_order = Some(secondary_order);
        self
    }

    pub fn complete(
        &mut self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    ) -> Result<CompletionResult, CompletionError> {
        if !pool_enabled() {
            return match self.primary_wait.as_deref_mut() {
                Some(primary_wait) => primary_wait.complete(request, on_status),
                None => self.primary.complete(request, on_status),
            };
        }

        let primary_name = self.resolved_primary_name().to_owned();
        let primary_result = {
            let mut status = |status| {
                emit_backend_status(on_status, &primary_name, &request.model, status);
            };
            self.primary.complete(request.clone(), &mut status)
        };
        let primary_err = match primary_result {
            Ok(result) => return Ok(result),
            Err(err) if self.secondaries.is_empty() => return Err(err),
            Err(err) => err,
        };

        let Some(primary_reason) = retryable_reason(primary_err.as_ref()) else {
            return Err(primary_err);
        };

        let mut errors = vec![format!("{primary_name}: {primary_err}")];
        emit_pool_fallback(on_status, &primary_name, &request.model);

        if let Some(result) =
            self.try_secondary_backends(&request, on_status, primary_reason, &mut errors)?
        {
            return Ok(result);
        }

        if primary_reason == FailureReason::CapacityUnavailable {
            emit_primary_wait(on_status, &primary_name, &request.model);
            match self.complete_primary_wait(request.clone(), on_status, &primary_name) {
                Ok(result) => return Ok(result),
                Err(err) => {
                    if retryable_reason(err.as_ref()).is_none() {
                        return Err(err);
                    }
                    errors.push(format!("{primary_name} queued wait: {err}"));
                }
            }
        }

        Err(Box::new(ProviderError::new(
            "aifarm_pool",
            FailureReason::ProviderUnavailable,
            errors.join("\n"),
        )))
    }

    fn resolved_primary_name(&self) -> &str {
        if self.primary_name.trim().is_empty() {
            "aifarm"
        } else {
            self.primary_name.trim()
        }
    }

    fn try_secondary_backends(
        &mut self,
        request: &ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
        primary_reason: FailureReason,
        errors: &mut Vec<String>,
    ) -> Result<Option<CompletionResult>, CompletionError> {
        for index in self.ordered_secondary_indices() {
            let Some(backend) = self.secondaries.get_mut(index) else {
                continue;
            };
            let direct_request = direct_compatible_request(request, &backend.model);
            let mut status = |status| {
                emit_backend_status(on_status, &backend.name, &backend.model, status);
            };
            let result = backend.client.complete(direct_request, &mut status);
            match result {
                Ok(result) => return Ok(Some(result)),
                Err(err) => {
                    let retryable = retryable_reason(err.as_ref()).is_some();
                    if !retryable && primary_reason != FailureReason::CapacityUnavailable {
                        return Err(err);
                    }
                    errors.push(format!("{}: {err}", backend.name));
                    emit_pool_fallback(on_status, &backend.name, &backend.model);
                }
            }
        }
        Ok(None)
    }

    fn complete_primary_wait(
        &mut self,
        request: ChatCompletionRequest,
        on_status: &mut (dyn FnMut(StatusUpdate) + Send),
        primary_name: &str,
    ) -> Result<CompletionResult, CompletionError> {
        let model = request.model.clone();
        let mut status = |status| emit_backend_status(on_status, primary_name, &model, status);
        match self.primary_wait.as_deref_mut() {
            Some(primary_wait) => primary_wait.complete(request, &mut status),
            None => self.primary.complete(request, &mut status),
        }
    }

    fn ordered_secondary_indices(&self) -> Vec<usize> {
        self.secondary_order
            .clone()
            .unwrap_or_else(|| (0..self.secondaries.len()).collect())
    }
}

/// AIFarm tool prompt mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ToolPromptMode {
    /// Tools unavailable.
    #[default]
    None,
    /// Native OpenAI-compatible tools available.
    Native,
    /// Plain text tool calls available.
    Text,
}

impl ToolPromptMode {
    fn as_prompt_value(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Native => "native",
            Self::Text => "text",
        }
    }

    fn has_tools(self) -> bool {
        matches!(self, Self::Native | Self::Text)
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
    });
    messages.push(ChatMessage {
        role: "user".to_owned(),
        content: build_runtime_context(input),
        content_parts: Vec::new(),
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
    render_tool_catalog(&alternative_dialog_tools())
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
    let caller_max_tokens = request.max_tokens;
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
    let mut kwargs = request
        .chat_template_kwargs
        .as_ref()
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let reasoning_on = pool_reasoning_enabled();
    kwargs.insert("enable_thinking".to_owned(), json!(reasoning_on));
    out.chat_template_kwargs = Some(Value::Object(kwargs));
    if reasoning_on {
        out.max_tokens = caller_max_tokens;
        let floor = pool_reasoning_max_tokens();
        if floor > 0 && out.max_tokens < floor {
            out.max_tokens = floor;
        }
    }
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
    Err(Box::new(AifarmClientError::Upstream(format!(
        "{prefix}: status {}: {}",
        response.status_code,
        direct_error_message(raw_body, &response.status_text)
    ))))
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

pub(crate) fn tool_context_from_input(input: &DialogInput) -> ToolContext {
    ToolContext {
        chat_id: input.context.chat_id,
        thread_id: input.context.thread_id,
        message_id: input.message.id,
        user_id: input.user.id,
        user_full_name: input.user.full_name.clone(),
        message_text: input.message.text.clone(),
        message_meta: input.message.meta.clone(),
    }
}

pub(crate) fn recorded_tool_call_with_ref(
    step: &ToolStep,
    result: &ToolResult,
    ref_hint: Option<&str>,
    iteration: usize,
) -> Result<ToolCall, CompletionError> {
    let at = OffsetDateTime::now_utc();
    let r#ref = ref_hint
        .filter(|value| !value.trim().is_empty())
        .map(str::trim)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("{}-{iteration}", step.step));
    Ok(ToolCall {
        name: step.step.clone(),
        r#ref,
        input: Some(serde_json::to_value(step).map_err(|err| {
            Box::new(AifarmDialogError::Response(format!(
                "encode tool input: {err}"
            ))) as CompletionError
        })?),
        output: Some(serde_json::to_value(result).map_err(|err| {
            Box::new(AifarmDialogError::Response(format!(
                "encode tool result: {err}"
            ))) as CompletionError
        })?),
        at: Some(format_timestamp(at)),
    })
}

pub(crate) fn immediate_tool_answer(step: &ToolStep, result: &ToolResult) -> Option<String> {
    if result
        .error
        .as_ref()
        .is_some_and(|error| error.code == "duplicate_tool_request")
    {
        return None;
    }
    if result.no_reply || is_internal_not_scheduled_instruction(&result.message) {
        return Some(String::new());
    }
    match step.step.as_str() {
        STEP_DRAW_IMAGE => queued_tool_answer(result, "Готово, поставила изображение в очередь."),
        STEP_GENERATE_SONG => queued_tool_answer(result, "Готово, поставила песню в очередь."),
        STEP_CHAT_HISTORY_SUMMARY => completed_tool_answer(result),
        _ => None,
    }
}

pub(crate) fn single_effect_tool_request_key(
    step: &ToolStep,
) -> Result<Option<String>, CompletionError> {
    if !single_effect_tool_name(&step.step) {
        return Ok(None);
    }
    let input = serde_json::to_value(step).map_err(|err| {
        Box::new(AifarmDialogError::Response(format!(
            "encode tool input: {err}"
        ))) as CompletionError
    })?;
    let encoded = serde_json::to_string(&input).map_err(|err| {
        Box::new(AifarmDialogError::Response(format!(
            "encode tool input: {err}"
        ))) as CompletionError
    })?;
    Ok(Some(format!("{}:{encoded}", step.step.trim())))
}

fn single_effect_tool_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(STEP_GENERATE_SONG)
        || name.eq_ignore_ascii_case(STEP_DRAW_IMAGE)
        || name.eq_ignore_ascii_case(STEP_CANCEL_DRAWING)
        || name.eq_ignore_ascii_case(STEP_CURRENCY_RATES)
        || name.eq_ignore_ascii_case(STEP_CHAT_HISTORY_SUMMARY)
}

pub(crate) fn duplicate_tool_result(tool_name: &str, first_ref: &str) -> ToolResult {
    let message = "Duplicate tool request suppressed in the same turn. Use the previous result and continue with the final text answer.";
    let tool = tool_name.trim().to_owned();
    let mut data = serde_json::Map::new();
    data.insert("duplicate".to_owned(), Value::Bool(true));
    data.insert("tool".to_owned(), Value::String(tool));
    if !first_ref.trim().is_empty() {
        data.insert(
            "first_ref".to_owned(),
            Value::String(first_ref.trim().to_owned()),
        );
    }
    ToolResult {
        status: TOOL_RESULT_STATUS_NOOP.to_owned(),
        message: message.to_owned(),
        data: Some(Value::Object(data)),
        error: Some(ToolError {
            code: "duplicate_tool_request".to_owned(),
            reason: "identical request already executed in this turn".to_owned(),
            retryable: false,
        }),
        ..ToolResult::default()
    }
}

fn completed_tool_answer(result: &ToolResult) -> Option<String> {
    let message = result.message.trim();
    if message.is_empty() {
        return None;
    }
    let status = result.status.trim();
    if status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_OK)
        || status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_EXECUTED)
        || status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_FAILED)
        || status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_NOOP)
    {
        return Some(message.to_owned());
    }
    None
}

fn queued_tool_answer(result: &ToolResult, fallback: &str) -> Option<String> {
    let status = result.status.trim();
    if status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_QUEUED) {
        let message = result.message.trim();
        if message.is_empty() {
            return Some(fallback.to_owned());
        }
        return Some(message.to_owned());
    }
    if status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_FAILED)
        || status.eq_ignore_ascii_case(TOOL_RESULT_STATUS_NOOP)
    {
        let message = result.message.trim();
        if !message.is_empty() {
            return Some(message.to_owned());
        }
    }
    None
}

fn resolve_vision_tool_file_id(
    file_id: &str,
    meta: &ToolContext,
) -> Result<String, CompletionError> {
    let file_id = file_id.trim();
    if !file_id.is_empty() {
        return Ok(file_id.to_owned());
    }
    single_current_image_file_id(&meta.message_meta).ok_or_else(|| {
        Box::new(AifarmDialogError::Response(
            "tool protocol error: vision_image file_id is empty".to_owned(),
        )) as CompletionError
    })
}

fn single_current_image_file_id(meta: &ChatMessageMeta) -> Option<String> {
    let mut found = None;
    for attachment in &meta.attachments {
        if !attachment.kind.trim().eq_ignore_ascii_case("image") {
            continue;
        }
        let file_id = attachment.file_unique_id.trim();
        if file_id.is_empty() {
            continue;
        }
        if found.is_some() {
            return None;
        }
        found = Some(file_id.to_owned());
    }
    found
}

pub(crate) fn tool_names_for_iteration(
    input: &DialogInput,
    iteration: usize,
    max_iterations: usize,
) -> Vec<&'static str> {
    if input.disable_tools || (max_iterations > 0 && iteration >= max_iterations) {
        return Vec::new();
    }
    alternative_dialog_tool_names()
}

pub(crate) fn tool_prompt_mode_for_request(
    use_tool_calls: bool,
    tool_names: &[&str],
) -> ToolPromptMode {
    if tool_names.is_empty() {
        return ToolPromptMode::None;
    }
    if use_tool_calls {
        ToolPromptMode::Native
    } else {
        ToolPromptMode::Text
    }
}

fn native_tool_values(tool_names: &[&str]) -> Result<Vec<Value>, AifarmDialogError> {
    chat_completion_tools_for_names(tool_names)
        .into_iter()
        .map(|tool| serde_json::to_value(tool).map_err(AifarmDialogError::ToolDefinition))
        .collect()
}

enum ToolStepSelection {
    None(ToolParseDecision),
    Steps(Vec<PendingToolStep>),
}

struct PendingToolStep {
    step: ToolStep,
    decision: ToolParseDecision,
    native_ref: Option<String>,
}

fn first_choice_tool_steps(response: &Value) -> Result<ToolStepSelection, CompletionError> {
    let message = first_choice_message_value(response)?;
    if let Some(tool_calls) = message
        .get("tool_calls")
        .filter(|value| value.as_array().is_some_and(|calls| !calls.is_empty()))
    {
        let calls =
            serde_json::from_value::<Vec<NativeToolCall>>(tool_calls.clone()).map_err(|err| {
                Box::new(AifarmDialogError::Response(format!(
                    "decode native tool calls: {err}"
                ))) as CompletionError
            })?;
        let mut steps = Vec::with_capacity(calls.len());
        for call in calls {
            let native_ref = Some(call.id.clone());
            let decision = native_tool_parse_decision(std::slice::from_ref(&call), None);
            let step =
                parse_native_tool_step(&[call]).map_err(|err| Box::new(err) as CompletionError)?;
            steps.push(PendingToolStep {
                step,
                decision,
                native_ref,
            });
        }
        return Ok(ToolStepSelection::Steps(steps));
    }

    let content = message
        .get("content")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let (step, decision) =
        extract_content_tool_step(content).map_err(|err| Box::new(err) as CompletionError)?;
    if let Some(step) = step {
        Ok(ToolStepSelection::Steps(vec![PendingToolStep {
            step,
            decision,
            native_ref: None,
        }]))
    } else {
        Ok(ToolStepSelection::None(decision))
    }
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
            FailureReason::ProviderUnavailable,
            err.to_string(),
        ))),
        Err(err) => Err(Box::new(err)),
    }
}

fn extract_final_answer(response: &Value) -> Result<String, AifarmDialogError> {
    let content = first_choice_content(response)?;
    let content = legacy_final_response_answer(&content).unwrap_or(content);
    if content.trim().is_empty() {
        return Err(AifarmDialogError::Response(
            "chat completion returned empty final text".to_owned(),
        ));
    }

    let answer = sanitize_final_text(&content);
    if answer.trim().is_empty() {
        if has_leading_context_message(&content) {
            return Err(AifarmDialogError::FinalAnswerContextLeak);
        }
        return Err(AifarmDialogError::FinalAnswerProtocolOnly);
    }
    if let Some(reason) = pathological_final_answer_reason(&answer) {
        return Err(AifarmDialogError::FinalAnswerPathological(reason));
    }
    Ok(answer)
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
        return Err(AifarmDialogError::Response(
            "chat completion returned empty final text".to_owned(),
        ));
    }
    Ok(content)
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
    )
}

fn pathological_final_answer_reason(value: &str) -> Option<String> {
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
    None
}

fn is_cyrillic(ch: char) -> bool {
    ('\u{0400}'..='\u{052f}').contains(&ch)
        || ('\u{2de0}'..='\u{2dff}').contains(&ch)
        || ('\u{a640}'..='\u{a69f}').contains(&ch)
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

fn emit_backend_status(
    on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    backend: &str,
    model: &str,
    mut status: StatusUpdate,
) {
    if status.backend.is_empty() {
        status.backend = backend.trim().to_owned();
    }
    if status.model.is_empty() {
        status.model = model.trim().to_owned();
    }
    on_status(status);
}

fn emit_pool_fallback(
    on_status: &mut (dyn FnMut(StatusUpdate) + Send),
    backend: &str,
    model: &str,
) {
    on_status(StatusUpdate {
        status: STATUS_QUEUED.to_owned(),
        message: "text llm pool backend failed, trying next backend".to_owned(),
        backend: backend.trim().to_owned(),
        model: model.trim().to_owned(),
        ..StatusUpdate::default()
    });
}

fn emit_primary_wait(on_status: &mut (dyn FnMut(StatusUpdate) + Send), backend: &str, model: &str) {
    on_status(StatusUpdate {
        status: STATUS_QUEUED.to_owned(),
        message: "text llm pool secondaries unavailable, waiting for primary backend capacity"
            .to_owned(),
        backend: backend.trim().to_owned(),
        model: model.trim().to_owned(),
        ..StatusUpdate::default()
    });
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
    write_optional_text_element(out, "persona_name", &persona.name);
    write_optional_text_element(out, "persona_tone", &persona.tone);
    write_optional_text_element(out, "persona_background", &persona.background);
    write_optional_text_element(out, "persona_boundaries", &persona.boundaries);
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
    let mut normalized = normalize_history_message(turn.clone());
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
            }))
        }
        _ => {
            if normalized.role == ROLE_MODEL {
                let Some(sanitized) = sanitize_assistant_history_turn(normalized) else {
                    return Ok(None);
                };
                normalized = sanitized;
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
    let mut parts = vec![ChatContentPart {
        part_type: "text".to_owned(),
        text: text.trim().to_owned(),
        image_url: None,
    }];
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
        });
    }
    if parts.len() <= 1 {
        return Vec::new();
    }
    parts
}

fn sanitize_assistant_history_turn(mut turn: HistoryMessage) -> Option<HistoryMessage> {
    turn.text = openplotva_dialog::sanitize_tool_text(&turn.text);
    turn.original_text = openplotva_dialog::sanitize_tool_text(&turn.original_text);
    if turn.text.trim().is_empty()
        && turn.original_text.trim().is_empty()
        && !has_renderable_message_meta(&turn.meta)
    {
        return None;
    }
    Some(turn)
}

fn has_renderable_message_meta(meta: &ChatMessageMeta) -> bool {
    !meta.sender_type.trim().is_empty()
        || !meta.annotation.trim().is_empty()
        || !meta.message_type.trim().is_empty()
        || !meta.vision_description.trim().is_empty()
        || !meta.attachments.is_empty()
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
    let mut image_index = 0;
    for attachment in &turn.meta.attachments {
        image_index = write_attachment_element(out, turn.message_id, image_index, attachment);
    }
    out.push_str("  </attachments>\n");
}

fn write_attachment_element(
    out: &mut String,
    message_id: i32,
    image_index: i32,
    attachment: &ChatAttachment,
) -> i32 {
    let mut image_index = image_index;
    let kind = attachment.kind.trim();
    out.push_str("    <attachment");
    if !kind.is_empty() {
        write_string_attr(out, "kind", kind);
    }
    if !attachment.source.trim().is_empty() {
        write_string_attr(out, "source", &attachment.source);
    }
    out.push('>');
    image_index = write_attachment_file_id(
        out,
        message_id,
        image_index,
        kind,
        &attachment.file_unique_id,
    );
    if !attachment.content.trim().is_empty() {
        write_inline_text_element(out, "content", &attachment.content);
    }
    out.push_str("</attachment>\n");
    image_index
}

fn write_attachment_file_id(
    out: &mut String,
    message_id: i32,
    image_index: i32,
    kind: &str,
    file_unique_id: &str,
) -> i32 {
    let file_unique_id = file_unique_id.trim();
    if file_unique_id.is_empty() {
        return image_index;
    }
    if !kind.eq_ignore_ascii_case("image") {
        write_inline_text_element(out, "file_id", file_unique_id);
        return image_index;
    }
    let image_index = image_index + 1;
    if message_id > 0 {
        out.push_str("<file_id>");
        write_vision_attachment_handle(out, message_id, image_index);
        out.push_str("</file_id>");
    }
    write_inline_text_element(out, "file_unique_id", file_unique_id);
    image_index
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

fn write_optional_text_element(out: &mut String, name: &str, value: &str) {
    if value.trim().is_empty() {
        return;
    }
    write_text_element(out, name, value);
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

fn write_vision_attachment_handle(out: &mut String, message_id: i32, image_index: i32) {
    out.push_str("message_");
    let _ = write!(out, "{message_id}");
    out.push_str("_image_");
    let _ = write!(out, "{image_index}");
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
        fmt,
        sync::{Arc, Mutex, MutexGuard},
    };

    use serde_json::json;
    use time::{Duration, Month, OffsetDateTime};

    use super::*;
    use openplotva_core::{ChatAttachment, SENDER_TYPE_USER, ToolCall};
    use openplotva_dialog::{
        DailyPersona, DialogContext, DialogMessage, DialogUser, Persona, ROLE_TOOL,
    };

    static POOL_REASONING_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn lock_pool_reasoning_test() -> MutexGuard<'static, ()> {
        POOL_REASONING_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

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

    #[derive(Clone, Debug)]
    struct FakeCompletionClient {
        state: Arc<Mutex<FakeCompletionClientState>>,
    }

    #[derive(Debug, Default)]
    struct FakeCompletionClientState {
        requests: Vec<ChatCompletionRequest>,
        results: VecDeque<Result<CompletionResult, CompletionError>>,
    }

    impl FakeCompletionClient {
        fn new(results: Vec<Result<CompletionResult, CompletionError>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeCompletionClientState {
                    requests: Vec::new(),
                    results: results.into(),
                })),
            }
        }

        fn request_count(&self) -> usize {
            self.state().requests.len()
        }

        fn request_at(&self, index: usize) -> ChatCompletionRequest {
            self.state().requests[index].clone()
        }

        fn state(&self) -> MutexGuard<'_, FakeCompletionClientState> {
            match self.state.lock() {
                Ok(state) => state,
                Err(err) => panic!("fake completion client state poisoned: {err}"),
            }
        }
    }

    impl CompletionClient for FakeCompletionClient {
        fn complete(
            &mut self,
            request: ChatCompletionRequest,
            _on_status: &mut (dyn FnMut(StatusUpdate) + Send),
        ) -> Result<CompletionResult, CompletionError> {
            let mut state = self.state();
            state.requests.push(request);
            state
                .results
                .pop_front()
                .unwrap_or_else(|| Ok(CompletionResult::default()))
        }
    }

    #[derive(Debug)]
    struct SimpleError(String);

    impl fmt::Display for SimpleError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl Error for SimpleError {}

    fn ok_completion(text: &str) -> Result<CompletionResult, CompletionError> {
        Ok(CompletionResult {
            response: Some(json!({
                "choices": [{
                    "message": {"role": "assistant", "content": text}
                }]
            })),
            ..CompletionResult::default()
        })
    }

    fn provider_failure(
        provider: &str,
        reason: FailureReason,
        message: &str,
    ) -> Result<CompletionResult, CompletionError> {
        Err(Box::new(ProviderError::new(provider, reason, message)))
    }

    fn simple_failure(message: &str) -> Result<CompletionResult, CompletionError> {
        Err(Box::new(SimpleError(message.to_owned())))
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

    #[tokio::test]
    async fn aifarm_history_summary_generator_uses_pool_client() -> Result<(), Box<dyn Error>> {
        let content = serde_json::to_string(&json!({
            "summary_json": {
                "events": ["pooled summary"],
                "event_details": [],
                "actors": [],
                "recap": "secondary",
                "open_questions": [],
                "source_style": "log",
                "quality_score": 0.5
            }
        }))?;
        let response = json!({
            "choices": [{"message": {"role": "assistant", "content": content}}],
        });
        let transport = FakeTransport::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 503,
                status_text: "Service Unavailable".to_owned(),
                body: br#"{"error":"capacity slot unavailable"}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 200,
                status_text: "OK".to_owned(),
                body: serde_json::to_vec(&response)?,
                ..AifarmHttpResponse::default()
            }),
        ]);
        let generator = AifarmHistorySummaryGenerator::with_transport(
            AifarmHistorySummaryConfig {
                client: AifarmClientConfig {
                    base_url: "https://primary-summary.example.test".to_owned(),
                    default_model: "summary-primary".to_owned(),
                    poll_interval: StdDuration::from_nanos(1),
                    task_timeout: StdDuration::from_secs(1),
                    ..AifarmClientConfig::default()
                },
                model: "summary-primary".to_owned(),
                pool: AifarmPoolConfig {
                    secondary_backends: vec![AifarmPoolBackendConfig {
                        base_url: "https://secondary-summary.example.test/v1".to_owned(),
                        model: "summary-secondary".to_owned(),
                        ..AifarmPoolBackendConfig::default()
                    }],
                    secondary_api_key: "summary-pool-key".to_owned(),
                    primary_capacity_wait: StdDuration::from_millis(500),
                },
                ..AifarmHistorySummaryConfig::default()
            },
            transport.clone(),
        );
        let mut statuses = Vec::new();

        let doc = generator
            .generate_document(
                &openplotva_history::SummaryInput {
                    input_hash: "pool-input".to_owned(),
                    ..openplotva_history::SummaryInput::default()
                },
                &mut |status| statuses.push(status),
            )
            .await?;

        assert_eq!(doc.content.events, vec!["pooled summary"]);
        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            "https://primary-summary.example.test/v1/jobs/blocking"
        );
        assert_eq!(
            requests[1].url,
            "https://secondary-summary.example.test/v1/chat/completions"
        );
        assert_eq!(
            requests[1].headers["Authorization"],
            "Bearer summary-pool-key"
        );
        let body: Value = serde_json::from_slice(&requests[1].body)?;
        assert_eq!(body["model"], "summary-secondary");
        assert_eq!(
            statuses.first().map(|status| status.message.as_str()),
            Some("text llm pool backend failed, trying next backend")
        );
        Ok(())
    }

    #[tokio::test]
    async fn aifarm_memory_extractor_matches_go_request_and_decode() -> Result<(), Box<dyn Error>> {
        let completion_payload = serde_json::to_string(&json!({
            "usage": {"prompt_tokens": 111, "completion_tokens": 22},
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "{\"episode_summary\":\"summary\",\"topics\":[\"infra\"],\"participants\":[],\"candidate_cards\":[],\"supersessions\":[],\"links\":[]}"
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
        assert_eq!(body["include_reasoning"], false);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
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
    async fn aifarm_memory_extractor_uses_pool_client() -> Result<(), Box<dyn Error>> {
        let response = json!({
            "choices": [{
                "message": {
                    "role": "assistant",
                    "content": "{\"episode_summary\":\"pooled\",\"topics\":[],\"participants\":[],\"candidate_cards\":[],\"supersessions\":[],\"links\":[]}"
                }
            }]
        });
        let transport = FakeTransport::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 503,
                status_text: "Service Unavailable".to_owned(),
                body: br#"{"error":"capacity slot unavailable"}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 200,
                status_text: "OK".to_owned(),
                body: serde_json::to_vec(&response)?,
                ..AifarmHttpResponse::default()
            }),
        ]);
        let extractor = AifarmMemoryExtractor::with_transport(
            AifarmMemoryExtractorConfig {
                client: AifarmClientConfig {
                    base_url: "https://primary-memory.example.test".to_owned(),
                    default_model: "memory-primary".to_owned(),
                    poll_interval: StdDuration::from_nanos(1),
                    task_timeout: StdDuration::from_secs(1),
                    ..AifarmClientConfig::default()
                },
                model: "memory-primary".to_owned(),
                pool: AifarmPoolConfig {
                    secondary_backends: vec![AifarmPoolBackendConfig {
                        base_url: "https://secondary-memory.example.test/v1".to_owned(),
                        model: "memory-secondary".to_owned(),
                        ..AifarmPoolBackendConfig::default()
                    }],
                    secondary_api_key: "memory-pool-key".to_owned(),
                    primary_capacity_wait: StdDuration::from_millis(500),
                },
                ..AifarmMemoryExtractorConfig::default()
            },
            transport.clone(),
        );
        let mut statuses = Vec::new();

        let out = extractor
            .extract(&ExtractInput::default(), &mut |status| {
                statuses.push(status)
            })
            .await?;

        assert_eq!(out.episode_summary, "pooled");
        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[1].url,
            "https://secondary-memory.example.test/v1/chat/completions"
        );
        assert_eq!(
            requests[1].headers["Authorization"],
            "Bearer memory-pool-key"
        );
        let body: Value = serde_json::from_slice(&requests[1].body)?;
        assert_eq!(body["model"], "memory-secondary");
        assert_eq!(
            statuses.first().map(|status| status.message.as_str()),
            Some("text llm pool backend failed, trying next backend")
        );
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

        fn calls(&self) -> Vec<String> {
            self.state().calls.clone()
        }

        fn draw_requests(&self) -> Vec<DrawRequest> {
            self.state().draw_requests.clone()
        }

        fn vision_requests(&self) -> Vec<VisionRequest> {
            self.state().vision_requests.clone()
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

    impl DialogToolbox for FakeToolbox {
        fn currency_rates<'a>(
            &'a self,
            _meta: ToolContext,
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

        fn vision_image<'a>(&'a self, req: VisionRequest) -> openplotva_dialog::ToolboxFuture<'a> {
            let result = {
                let mut state = self.state();
                state.calls.push(STEP_VISION_IMAGE.to_owned());
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

    fn direct_dialog_provider(
        response: Value,
        cfg: AifarmDialogConfig,
    ) -> (
        AifarmDialogProvider<FakeTransport>,
        FakeTransport,
        FakeToolbox,
    ) {
        direct_dialog_provider_with_toolbox(response, cfg, FakeToolbox::new(Vec::new()))
    }

    fn direct_dialog_provider_with_toolbox(
        response: Value,
        cfg: AifarmDialogConfig,
        toolbox: FakeToolbox,
    ) -> (
        AifarmDialogProvider<FakeTransport>,
        FakeTransport,
        FakeToolbox,
    ) {
        direct_dialog_provider_with_responses(vec![response], cfg, toolbox)
    }

    fn direct_dialog_provider_with_responses(
        responses: Vec<Value>,
        cfg: AifarmDialogConfig,
        toolbox: FakeToolbox,
    ) -> (
        AifarmDialogProvider<FakeTransport>,
        FakeTransport,
        FakeToolbox,
    ) {
        let transport = FakeTransport::new(
            responses
                .into_iter()
                .map(|response| Ok(json_response(response)))
                .collect(),
        );
        let mut cfg = cfg;
        cfg.client.direct_url = "https://direct.test/v1/chat/completions".to_owned();
        (
            AifarmDialogProvider::with_transport(cfg, transport.clone())
                .with_toolbox(Arc::new(toolbox.clone())),
            transport,
            toolbox,
        )
    }

    #[tokio::test]
    async fn dialog_provider_runs_plain_http_completion() -> Result<(), CompletionError> {
        let (provider, transport, _) = direct_dialog_provider(
            json!({
                "id": "cmpl-plain",
                "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 8,
                    "total_tokens": 20,
                    "prompt_tokens_details": {"cached_tokens": 4}
                },
                "timings": {
                    "prompt_n": 12,
                    "prompt_ms": 120.0,
                    "prompt_per_second": 100.0,
                    "predicted_n": 8,
                    "predicted_ms": 200.0,
                    "predicted_per_second": 40.0
                },
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<message id=\"1\"><text>old</text></message>\nНовый ответ."
                    }
                }]
            }),
            AifarmDialogConfig {
                max_tokens: 256,
                ..AifarmDialogConfig::default()
            },
        );
        let mut input = base_input();
        input.disable_tools = true;
        input.max_output_tokens = 512;
        input.message = DialogMessage {
            id: 101,
            text: "ответь коротко".to_owned(),
            ..DialogMessage::default()
        };

        let output = crate::ChatProvider::run_dialog(&provider, input).await?;

        assert_eq!(output.provider, PROVIDER_AIFARM);
        assert_eq!(output.answer, "Новый ответ.");
        assert_eq!(output.response, "Новый ответ.");
        let trace = output.trace.as_ref().expect("trace artifacts");
        assert_eq!(trace.request_kind, "openai.chat.completions");
        assert_eq!(
            trace.raw_request.as_ref().expect("raw request")["max_tokens"],
            256
        );
        assert_eq!(
            trace.inference_params.as_ref().expect("inference params")["tool_mode"],
            "none"
        );
        assert_eq!(
            trace.raw_response.as_ref().expect("raw response")["id"],
            "cmpl-plain"
        );
        assert_eq!(trace.usage.as_ref().expect("usage").cached_tokens, 4);
        assert_eq!(
            trace.timings.as_ref().expect("timings")["generation_tps"],
            40.0
        );

        let requests = transport.requests();
        assert_eq!(requests.len(), 1);
        let body: Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(body["model"], DEFAULT_MODEL_NAME);
        assert_eq!(body["max_tokens"], 256);
        assert_eq!(body["include_reasoning"], false);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        assert!(body.get("tools").is_none());
        Ok(())
    }

    #[test]
    fn aifarm_trace_request_redacts_data_url_images() {
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
                    },
                    ChatContentPart {
                        part_type: "image_url".to_owned(),
                        text: String::new(),
                        image_url: Some(ChatImageUrlPart {
                            url: "https://example.test/image.png".to_owned(),
                            detail: "auto".to_owned(),
                        }),
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
            value["messages"][0]["content"][1]["image_url"]["url"],
            "https://example.test/image.png"
        );
    }

    #[tokio::test]
    async fn dialog_provider_executes_tool_then_continues_to_final_text()
    -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![ToolResult {
            status: TOOL_RESULT_STATUS_OK.to_owned(),
            message: "tool ok".to_owned(),
            ..ToolResult::default()
        }]);
        let (provider, transport, toolbox) = direct_dialog_provider_with_responses(
            vec![
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "<tool_call>draw_image{prompt:\"cat\"}</tool_call>"
                        }
                    }]
                }),
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "готово после инструмента"
                        }
                    }]
                }),
            ],
            AifarmDialogConfig::default(),
            toolbox,
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 102,
            text: "нарисуй кота".to_owned(),
            ..DialogMessage::default()
        };

        let output = crate::ChatProvider::run_dialog(&provider, input).await?;

        assert_eq!(output.answer, "готово после инструмента");
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.tool_calls[0].name, STEP_DRAW_IMAGE);
        assert_eq!(output.trace_events.len(), 2);
        assert_eq!(output.trace_events[0].iteration, 1);
        assert_eq!(output.trace_events[0].source, PROVIDER_AIFARM);
        assert_eq!(output.trace_events[0].mode, "tools");
        assert_eq!(output.trace_events[0].flow, "dialog");
        assert_eq!(output.trace_events[0].model, DEFAULT_MODEL_NAME);
        assert_eq!(output.trace_events[1].iteration, 2);
        assert!(output.trace_events[1].prompt_messages > output.trace_events[0].prompt_messages);
        assert!(
            output.trace_events[1]
                .raw_request
                .as_ref()
                .expect("second raw request")["messages"]
                .as_array()
                .expect("messages")
                .iter()
                .any(|message| message["content"]
                    .as_str()
                    .is_some_and(|content| content.contains("<tool_result")))
        );
        assert_eq!(toolbox.calls(), vec![STEP_DRAW_IMAGE.to_owned()]);
        assert_eq!(toolbox.draw_requests()[0].prompt, "cat");

        let requests = transport.requests();
        assert_eq!(requests.len(), 2);
        let second_request: Value = serde_json::from_slice(&requests[1].body)?;
        assert!(
            second_request["messages"]
                .as_array()
                .expect("messages")
                .iter()
                .any(|message| message["content"]
                    .as_str()
                    .is_some_and(|content| content.contains("<tool_result")))
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_executes_xml_wrapper_tool_call_then_continues_to_final_text()
    -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![ToolResult {
            status: TOOL_RESULT_STATUS_OK.to_owned(),
            message: "weather ready".to_owned(),
            ..ToolResult::default()
        }]);
        let (provider, transport, toolbox) = direct_dialog_provider_with_responses(
            vec![
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "<tool_calls>\n  <tool_call>web_search{query: \"weather St. Petersburg June 2026 forecast\"}</tool_call>\n</tool_calls>"
                        }
                    }]
                }),
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "На ПМЭФ захвати зонт: прогноз уже в руках"
                        }
                    }]
                }),
            ],
            AifarmDialogConfig::default(),
            toolbox,
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 104,
            text: "Плотва, дай погоду на ПМЭФ на 4 дня".to_owned(),
            ..DialogMessage::default()
        };

        let output = crate::ChatProvider::run_dialog(&provider, input).await?;

        assert_eq!(output.answer, "На ПМЭФ захвати зонт: прогноз уже в руках");
        assert!(!output.answer.contains("<tool_calls>"));
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.tool_calls[0].name, STEP_WEB_SEARCH);
        assert_eq!(
            output.tool_calls[0].input.as_ref().expect("tool input")["query"],
            "weather St. Petersburg June 2026 forecast"
        );
        assert_eq!(toolbox.calls(), vec![STEP_WEB_SEARCH.to_owned()]);
        assert_eq!(
            toolbox.web_search_queries(),
            vec!["weather St. Petersburg June 2026 forecast".to_owned()]
        );
        assert_eq!(transport.requests().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_suppresses_duplicate_single_effect_tool_requests()
    -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![
            ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message: "rates ready".to_owned(),
                ..ToolResult::default()
            },
            ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message: "should not execute".to_owned(),
                ..ToolResult::default()
            },
        ]);
        let (provider, _transport, toolbox) = direct_dialog_provider_with_responses(
            vec![
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "<tool_call>currency_rates{}</tool_call>"
                        }
                    }]
                }),
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "<tool_call>currency_rates{}</tool_call>"
                        }
                    }]
                }),
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "готово"
                        }
                    }]
                }),
            ],
            AifarmDialogConfig::default(),
            toolbox,
        );

        let output = crate::ChatProvider::run_dialog(&provider, base_input()).await?;

        assert_eq!(output.answer, "готово");
        assert_eq!(toolbox.calls(), vec![STEP_CURRENCY_RATES.to_owned()]);
        assert_eq!(output.tool_calls.len(), 2);
        assert_eq!(
            output.tool_calls[1]
                .output
                .as_ref()
                .expect("duplicate output")["error"]["code"],
            "duplicate_tool_request"
        );
        assert_eq!(
            output.tool_calls[1]
                .output
                .as_ref()
                .expect("duplicate output")["data"]["duplicate"],
            true
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_executes_native_tool_call_batch_with_duplicate_guard()
    -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![
            ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message: "draw accepted".to_owned(),
                ..ToolResult::default()
            },
            ToolResult {
                status: TOOL_RESULT_STATUS_OK.to_owned(),
                message: "should not execute".to_owned(),
                ..ToolResult::default()
            },
        ]);
        let (provider, transport, toolbox) = direct_dialog_provider_with_responses(
            vec![
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "draw_image",
                                        "arguments": "{\"prompt\":\"cat\"}"
                                    }
                                },
                                {
                                    "id": "call-2",
                                    "type": "function",
                                    "function": {
                                        "name": "draw_image",
                                        "arguments": "{\"prompt\":\"cat\"}"
                                    }
                                }
                            ]
                        }
                    }]
                }),
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "готово после native batch"
                        }
                    }]
                }),
            ],
            AifarmDialogConfig {
                use_tool_calls: Some(true),
                ..AifarmDialogConfig::default()
            },
            toolbox,
        );

        let output = crate::ChatProvider::run_dialog(&provider, base_input()).await?;

        assert_eq!(output.answer, "готово после native batch");
        assert_eq!(toolbox.calls(), vec![STEP_DRAW_IMAGE.to_owned()]);
        assert_eq!(toolbox.draw_requests()[0].prompt, "cat");
        assert_eq!(output.tool_calls.len(), 2);
        assert_eq!(output.tool_calls[0].r#ref, "call-1");
        assert_eq!(output.tool_calls[1].r#ref, "call-2");
        assert_eq!(
            output.tool_calls[1]
                .output
                .as_ref()
                .expect("duplicate output")["error"]["code"],
            "duplicate_tool_request"
        );
        assert_eq!(
            output.tool_calls[1]
                .output
                .as_ref()
                .expect("duplicate output")["data"]["first_ref"],
            "call-1"
        );
        assert_eq!(transport.requests().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_records_tool_parser_telemetry_like_go() -> Result<(), CompletionError>
    {
        openplotva_dialog::tool_telemetry::clear_for_tests();
        let since = OffsetDateTime::now_utc();
        let toolbox = FakeToolbox::new(vec![ToolResult {
            status: TOOL_RESULT_STATUS_QUEUED.to_owned(),
            message: "queued".to_owned(),
            ..ToolResult::default()
        }]);
        let (provider, _transport, _) = direct_dialog_provider_with_toolbox(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "```tool\ndraw_image{prompt:\"cat in space\"}\n```"
                    }
                }]
            }),
            AifarmDialogConfig {
                model: "telemetry-model".to_owned(),
                max_iterations: 2,
                use_tool_calls: Some(false),
                ..AifarmDialogConfig::default()
            },
            toolbox,
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 103,
            text: "нарисуй кота".to_owned(),
            ..DialogMessage::default()
        };

        crate::ChatProvider::run_dialog(&provider, input).await?;

        let snapshot = openplotva_dialog::tool_telemetry::snapshot_since(since, 1_000);
        let events = snapshot
            .recent
            .iter()
            .filter(|event| event.model == "telemetry-model")
            .collect::<Vec<_>>();
        assert_eq!(events.len(), 2);
        assert!(events.iter().any(|event| event.outcome == "detected"
            && event.form == "fenced"
            && event.tool == STEP_DRAW_IMAGE));
        assert!(events.iter().any(|event| event.outcome == "executed"
            && event.form == "fenced"
            && event.tool == STEP_DRAW_IMAGE));
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_returns_immediate_queued_draw_answer() -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![ToolResult {
            status: TOOL_RESULT_STATUS_QUEUED.to_owned(),
            ..ToolResult::default()
        }]);
        let (provider, transport, _) = direct_dialog_provider_with_toolbox(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<tool_call>draw_image{prompt:\"cat\"}</tool_call>"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
            toolbox,
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 103,
            text: "нарисуй кота".to_owned(),
            ..DialogMessage::default()
        };

        let output = crate::ChatProvider::run_dialog(&provider, input).await?;

        assert_eq!(output.answer, "Готово, поставила изображение в очередь.");
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(output.trace_events.len(), 1);
        assert_eq!(output.trace_events[0].iteration, 1);
        assert_eq!(transport.requests().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_wraps_provider_error_with_trace_events() -> Result<(), CompletionError>
    {
        let transport = FakeTransport::new(vec![Err(Box::new(ProviderError::new(
            PROVIDER_AIFARM,
            FailureReason::ProviderUnavailable,
            "capacity unavailable",
        )) as CompletionError)]);
        let mut cfg = AifarmDialogConfig::default();
        cfg.client.direct_url = "https://direct.test/v1/chat/completions".to_owned();
        let provider = AifarmDialogProvider::with_transport(cfg, transport.clone())
            .with_toolbox(Arc::new(FakeToolbox::new(Vec::new())));
        let mut input = base_input();
        input.disable_tools = true;

        let error = crate::ChatProvider::run_dialog(&provider, input)
            .await
            .expect_err("provider error");

        assert_eq!(
            retryable_reason(error.as_ref()),
            Some(FailureReason::ProviderUnavailable)
        );
        let trace_error = error
            .downcast_ref::<DialogTraceError>()
            .expect("trace wrapper");
        assert_eq!(trace_error.trace_events().len(), 1);
        let trace = &trace_error.trace_events()[0];
        assert_eq!(trace.provider, PROVIDER_AIFARM);
        assert_eq!(trace.request_kind, "openai.chat.completions");
        assert_eq!(trace.source, PROVIDER_AIFARM);
        assert_eq!(trace.mode, "tools");
        assert_eq!(trace.flow, "dialog");
        assert_eq!(trace.iteration, 1);
        assert!(trace.raw_request.is_some());
        assert!(trace.raw_response.is_none());
        assert!(trace.error.contains("capacity unavailable"));
        assert_eq!(transport.requests().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_suppresses_answer_for_no_reply_tool_result()
    -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![ToolResult {
            status: TOOL_RESULT_STATUS_NOOP.to_owned(),
            no_reply: true,
            ..ToolResult::default()
        }]);
        let (provider, transport, _) = direct_dialog_provider_with_toolbox(
            json!({
                "choices": [{
                    "message": {
                        "role": "assistant",
                        "content": "<tool_call>draw_image{prompt:\"cat\"}</tool_call>"
                    }
                }]
            }),
            AifarmDialogConfig::default(),
            toolbox,
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 104,
            text: "нарисуй кота".to_owned(),
            ..DialogMessage::default()
        };

        let output = crate::ChatProvider::run_dialog(&provider, input).await?;

        assert_eq!(output.answer, "");
        assert_eq!(output.tool_calls.len(), 1);
        assert_eq!(transport.requests().len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_provider_uses_current_single_image_for_vision_tool()
    -> Result<(), CompletionError> {
        let toolbox = FakeToolbox::new(vec![ToolResult {
            status: TOOL_RESULT_STATUS_OK.to_owned(),
            message: "на фото кот".to_owned(),
            ..ToolResult::default()
        }]);
        let (provider, _transport, toolbox) = direct_dialog_provider_with_responses(
            vec![
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "<tool_call>vision_image{}</tool_call>"
                        }
                    }]
                }),
                json!({
                    "choices": [{
                        "message": {
                            "role": "assistant",
                            "content": "вижу кота"
                        }
                    }]
                }),
            ],
            AifarmDialogConfig::default(),
            toolbox,
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 105,
            text: "что на фото?".to_owned(),
            meta: ChatMessageMeta {
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    file_unique_id: "unique-image-1".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..ChatMessageMeta::default()
            },
            ..DialogMessage::default()
        };

        let output = crate::ChatProvider::run_dialog(&provider, input).await?;

        assert_eq!(output.answer, "вижу кота");
        assert_eq!(toolbox.calls(), vec![STEP_VISION_IMAGE.to_owned()]);
        assert_eq!(toolbox.vision_requests()[0].file_id, "unique-image-1");
        Ok(())
    }

    #[test]
    fn dialog_provider_builds_native_tool_request_when_enabled() -> Result<(), AifarmDialogError> {
        let provider = AifarmDialogProvider::with_transport(
            AifarmDialogConfig {
                use_tool_calls: Some(true),
                ..AifarmDialogConfig::default()
            },
            FakeTransport::new(Vec::new()),
        );
        let mut input = base_input();
        input.message = DialogMessage {
            id: 103,
            text: "что на картинке?".to_owned(),
            ..DialogMessage::default()
        };

        let request = provider.iteration_request(&input, 1)?;

        assert_eq!(request.tool_choice, Some(Value::String("auto".to_owned())));
        assert_eq!(request.parallel_tool_calls, Some(false));
        assert!(request.tools.iter().any(|tool| {
            tool["function"]["name"]
                .as_str()
                .is_some_and(|name| name == "vision_image")
        }));
        assert_eq!(request.include_reasoning, Some(false));
        assert_eq!(
            request.chat_template_kwargs,
            Some(json!({ "enable_thinking": false }))
        );
        Ok(())
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
            Some(FailureReason::ProviderUnavailable)
        );
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
    fn system_prompt_includes_tool_catalog() -> Result<(), AifarmMessageError> {
        let prompt = build_system_prompt_with_tool_prompt(&base_input(), ToolPromptMode::Native)?;

        assert!(prompt.contains("ведёшь персонажа в живом Telegram-чате"));
        assert!(prompt.contains("Большинство реплик не требуют tool"));
        assert!(prompt.contains("Никогда не используй translate_text"));
        assert!(prompt.contains("<system_contract>"));
        assert!(prompt.contains("<tools>"));
        for spec in alternative_dialog_tools() {
            assert!(prompt.contains(&format!("name=\"{}\"", spec.name)));
            assert!(prompt.contains(spec.summary.trim()));
        }
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
        assert!(!context.contains("Daily"));
        assert!(context.contains("<shield_context><document>support</document></shield_context>"));
        assert!(context.contains(r#"<chunk index="1">alpha &lt;one&gt;</chunk>"#));
        assert!(!context.contains("user_id"));
        assert!(!context.contains("chat_id"));
    }

    #[test]
    fn runtime_context_uses_daily_persona_without_custom() {
        let mut input = base_input();
        input.persona.persona = Some(DailyPersona {
            name: "Daily Persona".to_owned(),
            tone: "Daily tone".to_owned(),
            background: "Daily background".to_owned(),
            boundaries: "Daily boundaries".to_owned(),
        });

        let context = build_runtime_context(&input);

        assert!(context.contains("<persona_name>Daily Persona</persona_name>"));
        assert!(context.contains("<persona_tone>Daily tone</persona_tone>"));
        assert!(context.contains("<persona_background>Daily background</persona_background>"));
        assert!(context.contains("<persona_boundaries>Daily boundaries</persona_boundaries>"));
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
        let _guard = lock_pool_reasoning_test();
        let previous_reasoning = pool_reasoning_enabled();
        let previous_floor = pool_reasoning_max_tokens();
        set_pool_reasoning_enabled(false);
        let request = ChatCompletionRequest {
            model: "primary".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: "ping".to_owned(),
                content_parts: Vec::new(),
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
            ..ChatCompletionRequest::default()
        };

        let direct = direct_compatible_request(&request, " second-model ");

        set_pool_reasoning_enabled(previous_reasoning);
        set_pool_reasoning_max_tokens(previous_floor);
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
        let body = serde_json::to_value(&direct).expect("direct request JSON");
        assert_eq!(body["top_k"], json!(20.0));
        assert_eq!(body["repetition_penalty"], json!(1.0));
        assert!(body.get("repeat_penalty").is_none());
        assert!(body.get("frequency_penalty").is_none());
    }

    #[test]
    fn direct_compatible_request_reasoning_restores_caller_tokens_with_floor() {
        let _guard = lock_pool_reasoning_test();
        let previous_reasoning = pool_reasoning_enabled();
        let previous_floor = pool_reasoning_max_tokens();
        set_pool_reasoning_enabled(true);
        set_pool_reasoning_max_tokens(4096);
        let request = ChatCompletionRequest {
            model: "primary".to_owned(),
            messages: vec![ChatMessage {
                role: "user".to_owned(),
                content: "ping".to_owned(),
                content_parts: Vec::new(),
            }],
            max_tokens: 2048,
            temperature: Some(0.2),
            top_p: Some(0.95),
            repeat_penalty: Some(1.1),
            frequency_penalty: Some(0.2),
            presence_penalty: Some(0.3),
            chat_template_kwargs: Some(json!({"enable_thinking": false})),
            ..ChatCompletionRequest::default()
        };

        let direct = direct_compatible_request(&request, "second-model");

        set_pool_reasoning_enabled(previous_reasoning);
        set_pool_reasoning_max_tokens(previous_floor);
        assert_eq!(direct.max_tokens, 4096);
        assert_eq!(direct.temperature, Some(DEFAULT_VRAM_CLOUD_TEMPERATURE));
        assert_eq!(direct.top_p, Some(DEFAULT_VRAM_CLOUD_TOP_P));
        assert_eq!(direct.top_k, Some(DEFAULT_VRAM_CLOUD_TOP_K));
        assert_eq!(direct.repeat_penalty, None);
        assert_eq!(direct.frequency_penalty, None);
        assert_eq!(
            direct.repetition_penalty,
            Some(DEFAULT_VRAM_CLOUD_REPETITION_PENALTY)
        );
        assert_eq!(
            direct.presence_penalty,
            Some(DEFAULT_VRAM_CLOUD_PRESENCE_PENALTY)
        );
        assert_eq!(
            direct.chat_template_kwargs,
            Some(json!({ "enable_thinking": true }))
        );
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

    #[test]
    fn aifarm_pool_config_from_go_lists_trims_skips_and_defaults() {
        let models = vec![
            " first-model ".to_owned(),
            " ".to_owned(),
            "third-model".to_owned(),
        ];
        let base_urls = vec![
            " http://first.test/v1 ".to_owned(),
            "http://skip.test/v1".to_owned(),
            " ".to_owned(),
        ];

        let pool =
            AifarmPoolConfig::from_go_lists(&models, &base_urls, " pool-key ", StdDuration::ZERO);

        assert_eq!(
            pool.secondary_backends,
            vec![AifarmPoolBackendConfig {
                name: "first-model".to_owned(),
                base_url: "http://first.test/v1".to_owned(),
                model: "first-model".to_owned(),
                ..AifarmPoolBackendConfig::default()
            }]
        );
        assert_eq!(pool.secondary_api_key, "pool-key");
        assert_eq!(
            pool.primary_capacity_wait,
            DEFAULT_POOL_PRIMARY_CAPACITY_WAIT
        );
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
    async fn http_dialog_pool_falls_through_capacity_to_secondary_direct_endpoint()
    -> Result<(), CompletionError> {
        let transport = FakeTransport::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 503,
                status_text: "Service Unavailable".to_owned(),
                body: br#"{"error":"capacity slot unavailable"}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 200,
                status_text: "OK".to_owned(),
                body: br#"{"id":"cmpl-1","choices":[{"message":{"role":"assistant","content":"secondary-ok"}}]}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
        ]);
        let probe = transport.clone();
        let client = AifarmCompletionClient::with_transport(
            AifarmClientConfig {
                base_url: "https://primary.example.test".to_owned(),
                default_model: "primary-model".to_owned(),
                poll_interval: StdDuration::from_nanos(1),
                task_timeout: StdDuration::from_secs(1),
                ..AifarmClientConfig::default()
            },
            AifarmPoolConfig {
                secondary_backends: vec![AifarmPoolBackendConfig {
                    base_url: "https://secondary.example.test/v1".to_owned(),
                    model: "secondary-model".to_owned(),
                    ..AifarmPoolBackendConfig::default()
                }],
                secondary_api_key: "pool-key".to_owned(),
                primary_capacity_wait: StdDuration::from_millis(750),
            },
            transport,
        );
        let mut statuses = Vec::new();

        let result = client
            .complete(
                ChatCompletionRequest {
                    model: "primary-model".to_owned(),
                    messages: vec![ChatMessage {
                        role: "user".to_owned(),
                        content: "ping".to_owned(),
                        content_parts: Vec::new(),
                    }],
                    repeat_penalty: Some(1.1),
                    include_reasoning: Some(false),
                    chat_template_kwargs: Some(json!({"enable_thinking": false})),
                    ..ChatCompletionRequest::default()
                },
                &mut |status| statuses.push(status),
            )
            .await?;

        assert_eq!(completion_text(&result), Some("secondary-ok"));
        let requests = probe.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            "https://primary.example.test/v1/jobs/blocking"
        );
        let job: DiscoveryJobRequest =
            serde_json::from_slice(&requests[0].body).expect("discovery job request");
        assert_eq!(job.wait_for_capacity_ms, 750);
        assert_eq!(
            requests[1].url,
            "https://secondary.example.test/v1/chat/completions"
        );
        assert_eq!(requests[1].headers["Authorization"], "Bearer pool-key");
        let direct_request: ChatCompletionRequest =
            serde_json::from_slice(&requests[1].body).expect("direct chat request");
        assert_eq!(direct_request.model, "secondary-model");
        assert_eq!(direct_request.repeat_penalty, None);
        assert_eq!(direct_request.include_reasoning, None);
        assert_eq!(
            direct_request.chat_template_kwargs,
            Some(json!({ "enable_thinking": false }))
        );
        assert_eq!(
            statuses.first(),
            Some(&StatusUpdate {
                status: STATUS_QUEUED.to_owned(),
                message: "text llm pool backend failed, trying next backend".to_owned(),
                backend: "aifarm".to_owned(),
                model: "primary-model".to_owned(),
                ..StatusUpdate::default()
            })
        );
        assert_eq!(
            statuses.last().map(|status| status.backend.as_str()),
            Some("secondary-model")
        );
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
    fn priority_pool_uses_primary_when_accepted() -> Result<(), CompletionError> {
        let primary = FakeCompletionClient::new(vec![ok_completion("primary")]);
        let primary_probe = primary.clone();
        let secondary = FakeCompletionClient::new(vec![ok_completion("secondary")]);
        let secondary_probe = secondary.clone();
        let mut client = PooledClient::new(
            Box::new(primary),
            vec![PooledBackend::new(
                "pooled-model-a",
                "pooled-model-a",
                Box::new(secondary),
            )],
        );
        let mut statuses = Vec::new();

        let result = client.complete(
            ChatCompletionRequest {
                model: "primary-model".to_owned(),
                messages: vec![ChatMessage {
                    role: "user".to_owned(),
                    content: "ping".to_owned(),
                    content_parts: Vec::new(),
                }],
                ..ChatCompletionRequest::default()
            },
            &mut |status| statuses.push(status),
        )?;

        assert_eq!(completion_text(&result), Some("primary"));
        assert_eq!(primary_probe.request_count(), 1);
        assert_eq!(secondary_probe.request_count(), 0);
        assert!(statuses.is_empty());
        Ok(())
    }

    #[test]
    fn priority_pool_falls_through_capacity_to_ordered_secondary() -> Result<(), CompletionError> {
        let primary = FakeCompletionClient::new(vec![provider_failure(
            "aifarm",
            FailureReason::CapacityUnavailable,
            "no slots",
        )]);
        let primary_probe = primary.clone();
        let first = FakeCompletionClient::new(vec![ok_completion("first")]);
        let first_probe = first.clone();
        let second = FakeCompletionClient::new(vec![ok_completion("second")]);
        let second_probe = second.clone();
        let mut client = PooledClient::new(
            Box::new(primary),
            vec![
                PooledBackend::new("first", "first-model", Box::new(first)),
                PooledBackend::new("second", "second-model", Box::new(second)),
            ],
        )
        .with_secondary_order(vec![1, 0]);
        let mut statuses = Vec::new();

        let result = client.complete(
            ChatCompletionRequest {
                model: "primary-model".to_owned(),
                messages: vec![ChatMessage {
                    role: "user".to_owned(),
                    content: "ping".to_owned(),
                    content_parts: Vec::new(),
                }],
                repeat_penalty: Some(1.1),
                include_reasoning: Some(false),
                chat_template_kwargs: Some(json!({"enable_thinking": false})),
                ..ChatCompletionRequest::default()
            },
            &mut |status| statuses.push(status),
        )?;

        assert_eq!(completion_text(&result), Some("second"));
        assert_eq!(primary_probe.request_count(), 1);
        assert_eq!(first_probe.request_count(), 0);
        assert_eq!(second_probe.request_count(), 1);
        let second_request = second_probe.request_at(0);
        assert_eq!(second_request.model, "second-model");
        assert_eq!(second_request.repeat_penalty, None);
        assert_eq!(second_request.include_reasoning, None);
        assert_eq!(
            second_request.chat_template_kwargs,
            Some(json!({ "enable_thinking": false }))
        );
        assert_eq!(
            statuses,
            vec![StatusUpdate {
                status: STATUS_QUEUED.to_owned(),
                message: "text llm pool backend failed, trying next backend".to_owned(),
                backend: "aifarm".to_owned(),
                model: "primary-model".to_owned(),
                ..StatusUpdate::default()
            }]
        );
        Ok(())
    }

    #[test]
    fn priority_pool_waits_for_primary_capacity_when_secondaries_unavailable()
    -> Result<(), CompletionError> {
        let primary = FakeCompletionClient::new(vec![provider_failure(
            "aifarm",
            FailureReason::CapacityUnavailable,
            "no slots",
        )]);
        let primary_probe = primary.clone();
        let primary_wait = FakeCompletionClient::new(vec![ok_completion("primary-after-queue")]);
        let primary_wait_probe = primary_wait.clone();
        let first = FakeCompletionClient::new(vec![provider_failure(
            "first",
            FailureReason::ProviderUnavailable,
            "connection refused",
        )]);
        let first_probe = first.clone();
        let second = FakeCompletionClient::new(vec![simple_failure("context deadline exceeded")]);
        let second_probe = second.clone();
        let mut client = PooledClient::new(
            Box::new(primary),
            vec![
                PooledBackend::new("first", "first-model", Box::new(first)),
                PooledBackend::new("second", "second-model", Box::new(second)),
            ],
        )
        .with_primary_wait(Box::new(primary_wait));
        let mut statuses = Vec::new();

        let result = client.complete(
            ChatCompletionRequest {
                model: "primary-model".to_owned(),
                messages: vec![ChatMessage {
                    role: "user".to_owned(),
                    content: "ping".to_owned(),
                    content_parts: Vec::new(),
                }],
                ..ChatCompletionRequest::default()
            },
            &mut |status| statuses.push(status),
        )?;

        assert_eq!(completion_text(&result), Some("primary-after-queue"));
        assert_eq!(primary_probe.request_count(), 1);
        assert_eq!(primary_wait_probe.request_count(), 1);
        assert_eq!(first_probe.request_count(), 1);
        assert_eq!(second_probe.request_count(), 1);
        assert_eq!(
            statuses.last(),
            Some(&StatusUpdate {
                status: STATUS_QUEUED.to_owned(),
                message:
                    "text llm pool secondaries unavailable, waiting for primary backend capacity"
                        .to_owned(),
                backend: "aifarm".to_owned(),
                model: "primary-model".to_owned(),
                ..StatusUpdate::default()
            })
        );
        Ok(())
    }

    #[test]
    fn priority_pool_skips_secondaries_on_nonretryable_primary_error() {
        let primary =
            FakeCompletionClient::new(vec![simple_failure("validation failed: empty prompt")]);
        let secondary = FakeCompletionClient::new(vec![ok_completion("secondary")]);
        let secondary_probe = secondary.clone();
        let mut client = PooledClient::new(
            Box::new(primary),
            vec![PooledBackend::new(
                "pooled-model-a",
                "pooled-model-a",
                Box::new(secondary),
            )],
        );
        let mut statuses = Vec::new();

        let err = client
            .complete(
                ChatCompletionRequest {
                    model: "primary-model".to_owned(),
                    messages: vec![ChatMessage {
                        role: "user".to_owned(),
                        content: "ping".to_owned(),
                        content_parts: Vec::new(),
                    }],
                    ..ChatCompletionRequest::default()
                },
                &mut |status| statuses.push(status),
            )
            .err()
            .map(|err| err.to_string());

        assert_eq!(err.as_deref(), Some("validation failed: empty prompt"));
        assert_eq!(secondary_probe.request_count(), 0);
        assert!(statuses.is_empty());
    }

    #[test]
    fn priority_pool_returns_retryable_error_after_all_backends_and_primary_wait_fail() {
        let primary = FakeCompletionClient::new(vec![provider_failure(
            "aifarm",
            FailureReason::CapacityUnavailable,
            "no slots",
        )]);
        let primary_wait =
            FakeCompletionClient::new(vec![simple_failure("context deadline exceeded")]);
        let first = FakeCompletionClient::new(vec![provider_failure(
            "first",
            FailureReason::ProviderUnavailable,
            "connection refused",
        )]);
        let second = FakeCompletionClient::new(vec![simple_failure("context deadline exceeded")]);
        let mut client = PooledClient::new(
            Box::new(primary),
            vec![
                PooledBackend::new("first", "first-model", Box::new(first)),
                PooledBackend::new("second", "second-model", Box::new(second)),
            ],
        )
        .with_primary_wait(Box::new(primary_wait));
        let mut statuses = Vec::new();

        let err = client
            .complete(
                ChatCompletionRequest {
                    model: "primary-model".to_owned(),
                    messages: vec![ChatMessage {
                        role: "user".to_owned(),
                        content: "ping".to_owned(),
                        content_parts: Vec::new(),
                    }],
                    ..ChatCompletionRequest::default()
                },
                &mut |status| statuses.push(status),
            )
            .expect_err("pool should fail");

        assert_eq!(
            retryable_reason(err.as_ref()),
            Some(FailureReason::ProviderUnavailable)
        );
        assert!(err.to_string().contains("aifarm_pool provider"));
        assert_eq!(statuses.len(), 4);
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
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    source: "quoted".to_owned(),
                    file_unique_id: "AQADnRJrGyADoEt8".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..ChatMessageMeta::default()
            },
            ..HistoryMessage::default()
        });

        assert!(body.contains("<file_id>message_11951604_image_1</file_id>"));
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
        assert!(messages[3].content.contains("<assistant_message"));
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
}
