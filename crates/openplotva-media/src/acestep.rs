//! ACE-Step music API client and song material helpers.

use std::{collections::BTreeSet, path::Path, time::Duration};

use base64::{Engine as _, engine::general_purpose};
use reqwest::multipart;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::time::Instant;
use url::Url;

pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8001";
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(90);
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);
pub const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(360);
pub const DEFAULT_AUDIO_FORMAT: &str = "mp3";
pub const DEFAULT_MODEL: &str = "acemusic/acestep-v1.5-turbo";
pub const SONG_DIRECTOR_TERMINATOR_TOOL_NAME: &str = "song_director_terminator";
/// Hard cap of the music model (9000 semantic tokens at 25 tokens per second).
pub const SONG_MAX_DURATION_SECONDS: u32 = 360;
pub const SONG_MIN_TAGS: usize = 10;
pub const SONG_MAX_TAGS: usize = 40;
const SONG_MAX_TAG_CHARS: usize = 80;
const SONG_MIN_LYRIC_LINES: usize = 8;
const SONG_MAX_LYRIC_LINES: usize = 80;
const SONG_MIN_INSTRUMENTAL_SECONDS: u32 = 45;
const SONG_DEFAULT_INSTRUMENTAL_SECONDS: u32 = 180;
const SONG_MIN_VOCAL_SECONDS: u32 = 60;
const SONG_DEFAULT_VOCAL_SECONDS: u32 = 200;
/// Vocal songs end with their lyrics; the farm cap only guards against runaway takes.
const SONG_VOCAL_CAP_MARGIN_SECONDS: u32 = 45;
pub const SONG_VOCALS: [&str; 5] = ["male", "female", "duet", "choir", "instrumental"];
pub const DEFAULT_SONG_STYLE_TAGS: &str =
    "indie pop, clear vocal, acoustic guitar, emotional, 96 BPM";

const MAX_LOGGED_ERROR_BODY_BYTES: usize = 4096;
const LOGGED_ERROR_BODY_SUFFIX: &str = "...[truncated]";
const FILE_CANDIDATE_KEYS: [&str; 5] = ["file", "url", "audio", "audio_url", "path"];
const ERROR_CANDIDATE_KEYS: [&str; 4] = ["error", "message", "detail", "status_message"];
pub const SUPPORTED_SONG_LANGUAGES: [&str; 14] = [
    "ru", "en", "es", "de", "fr", "it", "pt", "pl", "tr", "uk", "be", "ja", "ko", "zh",
];

/// ACE-Step API mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AceStepApiMode {
    /// Native `/release_task` + `/query_result` API.
    Native,
    /// OpenAI-compatible `/v1/chat/completions` API.
    #[default]
    Completion,
}

impl AceStepApiMode {
    #[must_use]
    pub fn from_go(value: &str) -> Self {
        let value = value.trim();
        if value.eq_ignore_ascii_case("native") {
            Self::Native
        } else {
            Self::Completion
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Completion => "completion",
        }
    }
}

/// ACE-Step client config.
#[derive(Clone, Debug, PartialEq)]
pub struct AceStepConfig {
    /// Base URL.
    pub base_url: String,
    /// API key and native `ai_token`.
    pub api_key: String,
    /// API mode.
    pub api_mode: AceStepApiMode,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Poll interval.
    pub poll_interval: Duration,
    /// Task timeout.
    pub task_timeout: Duration,
    /// Audio format.
    pub audio_format: String,
    /// Model name.
    pub model: String,
}

impl Default for AceStepConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            api_key: String::new(),
            api_mode: AceStepApiMode::Completion,
            request_timeout: Duration::ZERO,
            poll_interval: Duration::ZERO,
            task_timeout: Duration::ZERO,
            audio_format: String::new(),
            model: String::new(),
        }
    }
}

impl AceStepConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.base_url = if self.base_url.trim().is_empty() {
            DEFAULT_BASE_URL.to_owned()
        } else {
            self.base_url.trim().trim_end_matches('/').to_owned()
        };
        self.api_key = self.api_key.trim().to_owned();
        if self.request_timeout == Duration::ZERO {
            self.request_timeout = DEFAULT_REQUEST_TIMEOUT;
        }
        if self.poll_interval == Duration::ZERO {
            self.poll_interval = DEFAULT_POLL_INTERVAL;
        }
        if self.task_timeout == Duration::ZERO {
            self.task_timeout = DEFAULT_TASK_TIMEOUT;
        }
        self.audio_format = if self.audio_format.trim().is_empty() {
            DEFAULT_AUDIO_FORMAT.to_owned()
        } else {
            self.audio_format.trim().to_owned()
        };
        self.model = if self.model.trim().is_empty() {
            DEFAULT_MODEL.to_owned()
        } else {
            self.model.trim().to_owned()
        };
        self
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

/// ACE-Step HTTP client.
#[derive(Clone, Debug)]
pub struct AceStepClient {
    cfg: AceStepConfig,
    http: reqwest::Client,
}

impl AceStepClient {
    /// Build a reqwest-backed client.
    pub fn new(cfg: AceStepConfig) -> Result<Self, AceStepError> {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(cfg.request_timeout)
            .build()
            .map_err(AceStepError::BuildHttpClient)?;
        Ok(Self { cfg, http })
    }

    /// Active API mode.
    #[must_use]
    pub const fn mode(&self) -> AceStepApiMode {
        self.cfg.api_mode
    }

    /// Health probe.
    pub async fn health(&self) -> Result<(), AceStepError> {
        let response = self
            .auth(self.http.get(self.cfg.endpoint("/health")))
            .send()
            .await
            .map_err(AceStepError::Http)?;
        self.success_bytes("GET", "/health", response)
            .await
            .map(|_| ())
    }

    /// List OpenAI-compatible models.
    pub async fn list_models(&self) -> Result<Vec<String>, AceStepError> {
        let response = self
            .auth(self.http.get(self.cfg.endpoint("/v1/models")))
            .send()
            .await
            .map_err(AceStepError::Http)?;
        let body = self.success_bytes("GET", "/v1/models", response).await?;
        let value: Value = serde_json::from_slice(&body).map_err(AceStepError::Json)?;
        Ok(value
            .get("data")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .collect())
    }

    /// Generate song audio through OpenAI-compatible completions.
    pub async fn generate_completion(
        &self,
        req: CompletionRequest,
    ) -> Result<CompletionResult, AceStepError> {
        let model = first_non_empty([req.model.as_str(), self.cfg.model.as_str()])
            .unwrap_or("")
            .to_owned();
        if model.is_empty() {
            return Err(AceStepError::InvalidResponse(
                "no models available".to_owned(),
            ));
        }
        let audio_format =
            first_non_empty([req.audio_format.as_str(), self.cfg.audio_format.as_str()])
                .unwrap_or(DEFAULT_AUDIO_FORMAT)
                .to_owned();
        let vocal_language = first_non_empty([req.vocal_language.as_str(), "en"])
            .unwrap_or("en")
            .to_owned();
        let content = completion_content(&req)?;
        let mut audio_config = json!({
            "format": audio_format,
            "vocal_language": vocal_language,
        });
        if let Some(max_seconds) = req.max_seconds.filter(|seconds| *seconds > 0)
            && let Some(config) = audio_config.as_object_mut()
        {
            config.insert("max_seconds".to_owned(), json!(max_seconds));
        }
        let body = json!({
            "model": model,
            "messages": [{"role": "user", "content": content}],
            "stream": false,
            "thinking": req.thinking,
            "audio_config": audio_config,
        });
        let response = self
            .auth(self.http.post(self.cfg.endpoint("/v1/chat/completions")))
            .timeout(self.cfg.task_timeout)
            .json(&body)
            .send()
            .await
            .map_err(AceStepError::Http)?;
        let body = self
            .success_bytes("POST", "/v1/chat/completions", response)
            .await?;
        parse_completion_response(&body, &audio_format)
    }

    /// Submit a native ACE-Step task.
    pub async fn release_task(&self, req: ReleaseTaskRequest) -> Result<String, AceStepError> {
        let mut form = multipart::Form::new();
        form = add_text_field(form, "prompt", &req.prompt);
        form = add_text_field(form, "lyrics", &req.lyrics);
        form = add_text_field(form, "vocal_language", &req.vocal_language);
        form = add_text_field(
            form,
            "audio_format",
            first_non_empty([req.audio_format.as_str(), self.cfg.audio_format.as_str()])
                .unwrap_or(DEFAULT_AUDIO_FORMAT),
        );
        form = add_text_field(
            form,
            "model",
            first_non_empty([req.model.as_str(), self.cfg.model.as_str()]).unwrap_or(DEFAULT_MODEL),
        );
        form = add_text_field(form, "ai_token", &self.cfg.api_key);
        if !req.reference_audio.is_empty() {
            let filename =
                first_non_empty([req.reference_file_name.as_str(), "reference_audio.wav"])
                    .unwrap_or("reference_audio.wav")
                    .to_owned();
            form = form.part(
                "reference_audio",
                multipart::Part::bytes(req.reference_audio).file_name(filename),
            );
        }
        let response = self
            .auth(self.http.post(self.cfg.endpoint("/release_task")))
            .multipart(form)
            .send()
            .await
            .map_err(AceStepError::Http)?;
        let body = self
            .success_bytes("POST", "/release_task", response)
            .await?;
        release_task_id(&body)
    }

    /// Query a native ACE-Step task.
    pub async fn query_result(&self, task_id: &str) -> Result<TaskResult, AceStepError> {
        let task_id = task_id.trim();
        if task_id.is_empty() {
            return Err(AceStepError::InvalidRequest("task id is empty".to_owned()));
        }
        let mut payload = json!({ "task_id_list": [task_id] });
        if !self.cfg.api_key.is_empty() {
            payload["ai_token"] = json!(self.cfg.api_key);
        }
        let response = self
            .auth(self.http.post(self.cfg.endpoint("/query_result")))
            .json(&payload)
            .send()
            .await
            .map_err(AceStepError::Http)?;
        let body = self
            .success_bytes("POST", "/query_result", response)
            .await?;
        let items = query_result_items(&body)?;
        Ok(choose_task_result(items, task_id))
    }

    /// Poll a native task until terminal state.
    pub async fn wait_result(&self, task_id: &str) -> Result<TaskResult, AceStepError> {
        let deadline = Instant::now() + self.cfg.task_timeout;
        loop {
            let result = self.query_result(task_id).await?;
            match final_task_result(result) {
                TaskWaitDecision::Done(result) => return Ok(result),
                TaskWaitDecision::Failed(error) => {
                    return Err(AceStepError::InvalidResponse(error));
                }
                TaskWaitDecision::Continue => {}
            }
            if Instant::now() >= deadline {
                return Err(AceStepError::Timeout(format!(
                    "timeout waiting for task {task_id}"
                )));
            }
            tokio::time::sleep(self.cfg.poll_interval).await;
        }
    }

    /// Download generated audio.
    pub async fn download_audio(&self, audio_url: &str) -> Result<DownloadedAudio, AceStepError> {
        let resolved = self.build_audio_url(audio_url);
        if resolved.is_empty() {
            return Err(AceStepError::InvalidRequest(
                "audio url is empty".to_owned(),
            ));
        }
        let response = self
            .auth(self.http.get(&resolved))
            .send()
            .await
            .map_err(AceStepError::Http)?;
        let headers = response.headers().clone();
        let body = self.success_bytes("GET", &resolved, response).await?;
        if body.is_empty() {
            return Err(AceStepError::InvalidResponse(
                "downloaded audio is empty".to_owned(),
            ));
        }
        let file_name = filename_from_headers(&headers)
            .or_else(|| filename_from_url(&resolved))
            .unwrap_or_else(|| fallback_song_filename(&self.cfg.audio_format));
        Ok(DownloadedAudio {
            data: body,
            file_name,
        })
    }

    #[must_use]
    pub fn build_audio_url(&self, file: &str) -> String {
        build_audio_url(&self.cfg.base_url, file)
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.cfg.api_key.is_empty() {
            req
        } else {
            req.bearer_auth(&self.cfg.api_key)
        }
    }

    async fn success_bytes(
        &self,
        method: &str,
        url: &str,
        response: reqwest::Response,
    ) -> Result<Vec<u8>, AceStepError> {
        let status = response.status();
        let body = response.bytes().await.map_err(AceStepError::Http)?.to_vec();
        if !status.is_success() {
            if status.is_server_error() || status.as_u16() == 429 {
                tracing::warn!(
                    target: "openplotva::media::acestep",
                    method,
                    url,
                    status = status.as_u16(),
                    response_body = %bounded_http_error_body(&body),
                    "ACE-Step request returned a non-success status"
                );
            }
            return Err(AceStepError::HttpStatus {
                method: method.to_owned(),
                url: url.to_owned(),
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).trim().to_owned(),
            });
        }
        Ok(body)
    }
}

fn bounded_http_error_body(body: &[u8]) -> String {
    let body = String::from_utf8_lossy(body).trim().to_owned();
    if body.len() <= MAX_LOGGED_ERROR_BODY_BYTES {
        return body;
    }
    let content_limit = MAX_LOGGED_ERROR_BODY_BYTES.saturating_sub(LOGGED_ERROR_BODY_SUFFIX.len());
    format!(
        "{}{}",
        truncate_utf8(&body, content_limit),
        LOGGED_ERROR_BODY_SUFFIX
    )
}

/// Completion-mode request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompletionRequest {
    pub prompt: String,
    pub lyrics: String,
    pub vocal_language: String,
    pub audio_format: String,
    pub model: String,
    pub thinking: bool,
    /// Farm-side cap on the generated length; instrumentals run up to it.
    pub max_seconds: Option<u32>,
}

/// Completion-mode result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompletionResult {
    pub audio_data: Vec<u8>,
    pub file_name: String,
    pub content: String,
    /// Sampling seed reported by the farm, when present.
    pub seed: Option<i64>,
    /// Audio length reported by the farm, in seconds.
    pub duration_seconds: Option<f64>,
}

/// Native release-task request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReleaseTaskRequest {
    pub prompt: String,
    pub lyrics: String,
    pub vocal_language: String,
    pub audio_format: String,
    pub model: String,
    pub reference_audio: Vec<u8>,
    pub reference_file_name: String,
}

/// Native task status.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[repr(i32)]
pub enum TaskStatus {
    #[default]
    Pending = 0,
    Success = 1,
    Failed = 2,
}

/// Native query result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TaskResult {
    pub task_id: String,
    pub status: TaskStatus,
    pub files: Vec<String>,
    pub error: String,
    pub raw_data: Value,
}

impl TaskResult {
    /// First non-empty file.
    #[must_use]
    pub fn first_file(&self) -> String {
        self.files
            .first()
            .map_or_else(String::new, |value| value.trim().to_owned())
    }
}

/// Downloaded audio file.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DownloadedAudio {
    pub data: Vec<u8>,
    pub file_name: String,
}

/// Song Director input: the listener's request plus optional gathered context.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SongPromptRequest {
    /// Topic as extracted by the command or the dialog tool.
    pub topic: String,
    /// The listener's message as written; carries instructions the topic may lose.
    pub request_text: String,
    pub user_full_name: String,
    /// Interface language hint; the director may override it from the request.
    pub language_hint: String,
    /// Pre-gathered chat/memory context, already trimmed to a budget.
    pub context: String,
    pub user_id: i64,
    pub message_id: i32,
}

/// Validated song material ready for the music model.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SongPromptResult {
    pub title: String,
    pub topic: String,
    /// Compact human-readable style line (genre · BPM · key · vocals).
    pub raw_style: String,
    /// Full compiled tag list sent to the music model.
    pub style: String,
    pub vocal_language: String,
    /// One of [`SONG_VOCALS`], or empty when the model did not say.
    pub vocals: String,
    /// Section-marked lyrics; empty for instrumentals.
    pub lyrics: String,
    /// Target length in seconds.
    pub duration_seconds: u32,
    /// The director's brief as returned by the model, kept for tracing.
    pub brief: Value,
}

impl SongPromptResult {
    #[must_use]
    pub fn is_instrumental(&self) -> bool {
        self.lyrics.trim().is_empty()
    }

    /// Farm-side length cap: instrumentals run to the target, vocal songs end with
    /// their lyrics and only get a guard margin.
    #[must_use]
    pub fn max_audio_seconds(&self) -> u32 {
        song_max_audio_seconds(self.duration_seconds, self.is_instrumental())
    }
}

#[must_use]
pub fn song_max_audio_seconds(duration_seconds: u32, instrumental: bool) -> u32 {
    if instrumental {
        duration_seconds.clamp(SONG_MIN_INSTRUMENTAL_SECONDS, SONG_MAX_DURATION_SECONDS)
    } else {
        duration_seconds
            .saturating_add(SONG_VOCAL_CAP_MARGIN_SECONDS)
            .clamp(SONG_MIN_VOCAL_SECONDS, SONG_MAX_DURATION_SECONDS)
    }
}

/// Song Director tool payload (mirrors the terminator schema).
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct SongPromptPayload {
    pub analysis: String,
    pub title: String,
    pub vocal_language: String,
    pub vocals: String,
    pub genre: String,
    #[serde(deserialize_with = "lenient_u32")]
    pub bpm: u32,
    pub key: String,
    pub sound: Vec<String>,
    pub character: Vec<String>,
    pub structure: Vec<String>,
    pub vocal_style: String,
    pub references: Vec<String>,
    #[serde(deserialize_with = "lenient_u32")]
    pub duration_seconds: u32,
    pub lyrics: String,
}

fn lenient_u32<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(lenient_number(&value))
}

fn lenient_number(value: &Value) -> u32 {
    match value {
        Value::Number(number) => number
            .as_u64()
            .or_else(|| number.as_f64().map(float_to_seconds))
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0),
        Value::String(text) => lenient_number_text(text),
        _ => 0,
    }
}

fn float_to_seconds(value: f64) -> u64 {
    if value.is_finite() && value > 0.0 {
        // The value is bounded by the caller's clamp; the cast cannot overflow.
        value.round() as u64
    } else {
        0
    }
}

fn lenient_number_text(text: &str) -> u32 {
    let text = text.trim();
    if let Some((minutes, seconds)) = text.split_once(':')
        && let (Ok(minutes), Ok(seconds)) =
            (minutes.trim().parse::<u32>(), seconds.trim().parse::<u32>())
    {
        return minutes.saturating_mul(60).saturating_add(seconds);
    }
    let digits: String = text.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().unwrap_or(0)
}

/// Tool schema for the song director terminator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SongPromptTerminatorDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub input_schema: Value,
}

/// ACE-Step errors.
#[derive(Debug, Error)]
pub enum AceStepError {
    /// Request is malformed before reaching ACE-Step.
    #[error("{0}")]
    InvalidRequest(String),
    /// HTTP client setup failed.
    #[error("build HTTP client: {0}")]
    BuildHttpClient(#[source] reqwest::Error),
    /// Transport failed.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// Non-success status.
    #[error("request {method} {url} returned status={status} body={body}")]
    HttpStatus {
        method: String,
        url: String,
        status: u16,
        body: String,
    },
    /// JSON decoding failed.
    #[error("decode response: {0}")]
    Json(#[source] serde_json::Error),
    #[error("{0}")]
    InvalidResponse(String),
    /// Polling timed out.
    #[error("{0}")]
    Timeout(String),
    /// Prompt rendering failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
}

fn song_director_template_data(
    request: &SongPromptRequest,
    topic: &str,
    language_hint: &str,
) -> Value {
    let request_text = request.request_text.trim();
    let user_name = request.user_full_name.trim();
    let language_hint = language_hint.trim();
    json!({
        "request": if request_text.is_empty() { topic } else { request_text },
        "topic": topic,
        "userName": if user_name.is_empty() { "listener" } else { user_name },
        "languageHint": if language_hint.is_empty() { "unknown" } else { language_hint },
        "context": request.context.trim(),
        "maxDuration": SONG_MAX_DURATION_SECONDS,
    })
}

pub fn render_song_director_messages(
    request: &SongPromptRequest,
    topic: &str,
    language_hint: &str,
) -> Result<Vec<openplotva_prompts::PromptMessage>, openplotva_prompts::PromptError> {
    openplotva_prompts::render_messages(
        "music/song_director",
        &song_director_template_data(request, topic, language_hint),
    )
}

pub fn render_song_director_messages_with(
    prompts: &openplotva_prompts::PromptStore,
    request: &SongPromptRequest,
    topic: &str,
    language_hint: &str,
) -> Result<Vec<openplotva_prompts::PromptMessage>, openplotva_prompts::PromptError> {
    prompts.render_messages(
        "music/song_director",
        &song_director_template_data(request, topic, language_hint),
    )
}

#[must_use]
pub fn song_director_terminator_definition() -> SongPromptTerminatorDefinition {
    SongPromptTerminatorDefinition {
        name: SONG_DIRECTOR_TERMINATOR_TOOL_NAME,
        description: "Deliver the complete song package: the analysis, the three-layer production brief for the music model and the lyrics.",
        input_schema: json!({
            "type": "object",
            "properties": {
                "analysis": {
                    "type": "string",
                    "description": "2-4 sentences: the genre family chosen and why, vocals or instrumental, the lyrics language, the target duration and the story angle"
                },
                "title": { "type": "string", "description": "2-5 words in the lyrics language" },
                "vocal_language": {
                    "type": "string",
                    "description": "ISO 639-1 code of the lyrics language: ru, en, uk, be, es, de, fr, it, pt, pl, tr, ja, ko, zh"
                },
                "vocals": { "type": "string", "enum": SONG_VOCALS },
                "genre": { "type": "string", "description": "main genre and subgenre tags, comma-separated" },
                "bpm": { "type": "integer" },
                "key": { "type": "string", "description": "musical key such as F minor, or an empty string" },
                "sound": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "5-10 instrument and sound-design tags"
                },
                "character": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "3-6 energy, mood, era and attitude tags"
                },
                "structure": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "4-8 arrangement tags in playback order"
                },
                "vocal_style": {
                    "type": "string",
                    "description": "one phrase describing the singer; empty for instrumentals"
                },
                "references": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "0-3 tags of the form 'in the style of ...'"
                },
                "duration_seconds": { "type": "integer" },
                "lyrics": {
                    "type": "string",
                    "description": "section-marked lyrics, or an empty string for instrumentals"
                }
            },
            "required": [
                "analysis", "title", "vocal_language", "vocals", "genre", "bpm", "key", "sound",
                "character", "structure", "vocal_style", "references", "duration_seconds", "lyrics"
            ]
        }),
    }
}

pub fn normalize_song_prompt_input(
    req: &SongPromptRequest,
) -> Result<(String, String), AceStepError> {
    let topic = req.topic.trim();
    if topic.is_empty() {
        return Err(AceStepError::InvalidRequest(
            "song topic is empty".to_owned(),
        ));
    }
    let mut language = normalize_song_language(&req.language_hint);
    if language.is_empty() {
        let request_text = req.request_text.trim();
        language = detect_song_language(if request_text.is_empty() {
            topic
        } else {
            request_text
        });
    }
    Ok((topic.to_owned(), language))
}

pub fn normalize_song_prompt_payload(
    payload: SongPromptPayload,
    requested_topic: &str,
    requested_language: &str,
) -> Result<SongPromptResult, AceStepError> {
    let topic = requested_topic.trim();
    if topic.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "song topic is empty".to_owned(),
        ));
    }
    let mut language = normalize_song_language(&payload.vocal_language);
    if language.is_empty() {
        language = normalize_song_language(requested_language);
    }
    if language.is_empty() {
        return Err(AceStepError::InvalidResponse(
            SONG_LANGUAGE_INVALID_REJECTION.to_owned(),
        ));
    }
    let lyrics = canonicalize_song_lyrics(&payload.lyrics);
    let vocals_field = payload.vocals.trim().to_ascii_lowercase();
    let instrumental = vocals_field == "instrumental" || lyrics.line_count == 0;
    let vocals = if instrumental {
        "instrumental".to_owned()
    } else if SONG_VOCALS.contains(&vocals_field.as_str()) {
        vocals_field
    } else {
        String::new()
    };
    if !instrumental {
        if lyrics.line_count < SONG_MIN_LYRIC_LINES
            || lyrics.line_count > SONG_MAX_LYRIC_LINES
            || lyrics.sections < 2
            || !lyrics.has_chorus
        {
            return Err(AceStepError::InvalidResponse(
                SONG_LYRICS_STRUCTURE_REJECTION.to_owned(),
            ));
        }
        if !lyrics_script_matches_language(&lyrics.text, &language) {
            return Err(AceStepError::InvalidResponse(
                SONG_LYRICS_LANGUAGE_REJECTION.to_owned(),
            ));
        }
    }
    let tags = compile_song_tags(&payload, &vocals, instrumental);
    if tags.len() < SONG_MIN_TAGS {
        return Err(AceStepError::InvalidResponse(
            SONG_STYLE_INVALID_REJECTION.to_owned(),
        ));
    }
    let duration_seconds = if instrumental {
        non_zero_or(payload.duration_seconds, SONG_DEFAULT_INSTRUMENTAL_SECONDS)
            .clamp(SONG_MIN_INSTRUMENTAL_SECONDS, SONG_MAX_DURATION_SECONDS)
    } else {
        non_zero_or(payload.duration_seconds, SONG_DEFAULT_VOCAL_SECONDS)
            .clamp(SONG_MIN_VOCAL_SECONDS, SONG_MAX_DURATION_SECONDS)
    };
    let title = payload.title.trim();
    let title = if title.is_empty() {
        topic
            .split_whitespace()
            .take(5)
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        title.to_owned()
    };
    let raw_style = song_style_summary(&payload, &vocals, instrumental);
    let brief = serde_json::to_value(&payload).unwrap_or(Value::Null);
    Ok(SongPromptResult {
        title,
        topic: topic.to_owned(),
        raw_style,
        style: tags.join(", "),
        vocal_language: language,
        vocals,
        lyrics: if instrumental {
            String::new()
        } else {
            lyrics.text
        },
        duration_seconds,
        brief,
    })
}

const fn non_zero_or(value: u32, fallback: u32) -> u32 {
    if value == 0 { fallback } else { value }
}

pub const SONG_STYLE_INVALID_REJECTION: &str = "song style is invalid";
pub const SONG_LYRICS_STRUCTURE_REJECTION: &str = "song lyrics do not satisfy minimum structure";
pub const SONG_LYRICS_LANGUAGE_REJECTION: &str = "song lyrics script does not match the language";
pub const SONG_LANGUAGE_INVALID_REJECTION: &str = "song vocal language is invalid";

/// The model answered but produced unusable song material. Retry classifiers
/// key off these markers to fall through to another routed model; keeping them
/// as the same constants the validator throws makes that contract compile-time.
pub const INVALID_SONG_MATERIAL_MARKERS: &[&str] = &[
    SONG_STYLE_INVALID_REJECTION,
    SONG_LYRICS_STRUCTURE_REJECTION,
    SONG_LYRICS_LANGUAGE_REJECTION,
    SONG_LANGUAGE_INVALID_REJECTION,
];

#[must_use]
pub fn is_invalid_song_material_message(message: &str) -> bool {
    INVALID_SONG_MATERIAL_MARKERS
        .iter()
        .any(|marker| message.contains(marker))
}

pub fn decode_song_prompt_payload(
    content: &str,
    requested_topic: &str,
    requested_language: &str,
) -> Result<SongPromptResult, AceStepError> {
    let payload: SongPromptPayload = serde_json::from_str(content).map_err(AceStepError::Json)?;
    normalize_song_prompt_payload(payload, requested_topic, requested_language)
}

#[must_use]
pub fn detect_song_language(text: &str) -> String {
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }
    let lowered = text.to_lowercase();
    let count = |letters: &[char]| lowered.chars().filter(|ch| letters.contains(ch)).count();
    // ў is Belarusian only, and і/ї/є/ґ never occur in Russian. Between the
    // other two, Belarusian kept ы/э/ё that Ukrainian dropped, while Ukrainian
    // kept и/щ that Belarusian dropped.
    if lowered.contains('ў') {
        return "be".to_owned();
    }
    if count(&['і', 'ї', 'є', 'ґ']) > 0 {
        let belarusian = count(&['ы', 'э', 'ё']);
        let ukrainian = count(&['ї', 'є', 'ґ', 'и', 'щ']);
        return if belarusian > ukrainian { "be" } else { "uk" }.to_owned();
    }
    if lowered
        .chars()
        .any(|ch| ('\u{0400}'..='\u{04ff}').contains(&ch))
    {
        "ru".to_owned()
    } else {
        "en".to_owned()
    }
}

#[must_use]
pub fn normalize_song_language(language: &str) -> String {
    let mut lang = language.trim();
    if let Some((prefix, _)) = lang.split_once('-') {
        lang = prefix;
    }
    SUPPORTED_SONG_LANGUAGES
        .iter()
        .copied()
        .find(|supported| supported.eq_ignore_ascii_case(lang))
        .unwrap_or("")
        .to_owned()
}

/// Compile the director's brief into the tag list the music model reads, in a
/// fixed order: genre, tempo, key, vocals, sound, character, structure, references.
#[must_use]
pub fn compile_song_tags(
    payload: &SongPromptPayload,
    vocals: &str,
    instrumental: bool,
) -> Vec<String> {
    let mut tags = SongTagList::default();
    for tag in split_song_tag_field(&payload.genre) {
        tags.push(&tag);
    }
    if (40..=300).contains(&payload.bpm) {
        tags.push(&format!("{} BPM", payload.bpm));
    }
    tags.push(&payload.key);
    if instrumental {
        tags.push("instrumental");
    } else {
        tags.push(&vocal_descriptor(&payload.vocal_style, vocals));
    }
    for field in [
        &payload.sound,
        &payload.character,
        &payload.structure,
        &payload.references,
    ] {
        for raw in field {
            for tag in split_song_tag_field(raw) {
                tags.push(&tag);
            }
        }
    }
    tags.into_tags()
}

fn vocal_descriptor(vocal_style: &str, vocals: &str) -> String {
    let style = vocal_style.trim();
    if style.is_empty() {
        return if vocals.is_empty() {
            String::new()
        } else {
            format!("{vocals} vocals")
        };
    }
    let lowered = style.to_ascii_lowercase();
    let names_voice = [
        "male", "female", "duet", "choir", "man", "woman", "girl", "boy",
    ]
    .iter()
    .any(|word| {
        lowered
            .split(|ch: char| !ch.is_ascii_alphabetic())
            .any(|w| w == *word)
    });
    if names_voice || vocals.is_empty() {
        style.to_owned()
    } else {
        format!("{vocals} {style}")
    }
}

fn split_song_tag_field(raw: &str) -> Vec<String> {
    raw.split([',', ';', '\n', '|'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

#[derive(Default)]
struct SongTagList {
    tags: Vec<String>,
    seen: BTreeSet<String>,
}

impl SongTagList {
    fn push(&mut self, raw: &str) {
        if self.tags.len() >= SONG_MAX_TAGS {
            return;
        }
        let Some(tag) = normalize_song_tag(raw) else {
            return;
        };
        if self.seen.insert(tag.to_ascii_lowercase()) {
            self.tags.push(tag);
        }
    }

    fn into_tags(self) -> Vec<String> {
        self.tags
    }
}

/// Clean one tag for the music model: ASCII words only, collapsed whitespace,
/// numbering and quotes stripped. Returns `None` for anything that is not an
/// English descriptor (empty, too long, no letters, non-Latin script).
#[must_use]
pub fn normalize_song_tag(raw: &str) -> Option<String> {
    let raw = strip_list_numbering(raw.trim()).trim_matches(|ch: char| {
        ch == '"' || ch == '\'' || ch == '`' || ch == '*' || ch == '-' || ch == '.'
    });
    let mut out = String::with_capacity(raw.len());
    let mut pending_space = false;
    for ch in raw.chars() {
        if ch.is_whitespace() {
            pending_space = !out.is_empty();
            continue;
        }
        if !(ch.is_ascii_alphanumeric() || "#+&/'()-.".contains(ch)) {
            return None;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        out.push(ch);
    }
    let count = out.chars().count();
    if !(2..=SONG_MAX_TAG_CHARS).contains(&count) || !out.chars().any(|ch| ch.is_ascii_alphabetic())
    {
        return None;
    }
    Some(out)
}

fn strip_list_numbering(text: &str) -> &str {
    let digits = text.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 || digits > 3 {
        return text;
    }
    let rest = &text[digits..];
    rest.strip_prefix(". ")
        .or_else(|| rest.strip_prefix(") "))
        .unwrap_or(text)
}

fn song_style_summary(payload: &SongPromptPayload, vocals: &str, instrumental: bool) -> String {
    let mut parts = Vec::new();
    let genre: Vec<String> = split_song_tag_field(&payload.genre)
        .iter()
        .filter_map(|tag| normalize_song_tag(tag))
        .collect();
    if !genre.is_empty() {
        parts.push(genre.join(", "));
    }
    if (40..=300).contains(&payload.bpm) {
        parts.push(format!("{} BPM", payload.bpm));
    }
    if let Some(key) = normalize_song_tag(&payload.key) {
        parts.push(key);
    }
    if instrumental {
        parts.push("instrumental".to_owned());
    } else if let Some(voice) = normalize_song_tag(&vocal_descriptor(&payload.vocal_style, vocals))
    {
        parts.push(voice);
    }
    parts.join(" · ")
}

/// Lyrics after section canonicalization and placeholder removal.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CanonicalLyrics {
    pub text: String,
    pub sections: usize,
    pub line_count: usize,
    pub has_chorus: bool,
}

/// Canonicalize section markers, drop stage directions and placeholder lines,
/// and lay the lyrics out one section per block.
#[must_use]
pub fn canonicalize_song_lyrics(raw: &str) -> CanonicalLyrics {
    let mut sections: Vec<(String, Vec<String>)> = Vec::new();
    let mut current: Option<(String, Vec<String>)> = None;
    for raw_line in raw.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(marker) = section_marker(line) {
            if let Some(name) = canonical_section_name(marker) {
                if let Some(section) = current.take().filter(|(_, lines)| !lines.is_empty()) {
                    sections.push(section);
                }
                current = Some((name, Vec::new()));
            }
            continue;
        }
        if is_placeholder_lyric_line(line) {
            continue;
        }
        let line = clean_lyric_line(line);
        if line.is_empty() {
            continue;
        }
        match current.as_mut() {
            Some((_, lines)) => lines.push(line),
            None => current = Some(("Verse".to_owned(), vec![line])),
        }
    }
    if let Some(section) = current.filter(|(_, lines)| !lines.is_empty()) {
        sections.push(section);
    }
    let line_count = sections.iter().map(|(_, lines)| lines.len()).sum();
    let has_chorus = sections.iter().any(|(name, _)| name == "Chorus");
    let text = sections
        .iter()
        .map(|(name, lines)| format!("[{name}]\n{}", lines.join("\n")))
        .collect::<Vec<_>>()
        .join("\n\n");
    CanonicalLyrics {
        text,
        sections: sections.len(),
        line_count,
        has_chorus,
    }
}

fn section_marker(line: &str) -> Option<&str> {
    let inner = line.strip_prefix('[')?.strip_suffix(']')?;
    (!inner.contains('[') && !inner.contains(']')).then_some(inner)
}

fn canonical_section_name(marker: &str) -> Option<String> {
    let lowered = marker.trim().to_lowercase();
    let head: String = lowered
        .chars()
        .take_while(|ch| ch.is_alphabetic() || *ch == '-' || *ch == ' ')
        .collect();
    let head = head.trim().replace(' ', "-");
    let number: String = lowered.chars().filter(char::is_ascii_digit).collect();
    let numbered = |name: &str| {
        if number.is_empty() {
            name.to_owned()
        } else {
            format!("{name} {number}")
        }
    };
    match head.as_str() {
        "verse" | "куплет" => Some(numbered("Verse")),
        "pre-chorus" | "prechorus" | "предприпев" => Some("Pre-Chorus".to_owned()),
        "chorus" | "hook" | "refrain" | "drop" | "припев" => Some("Chorus".to_owned()),
        "bridge" | "breakdown" | "бридж" => Some("Bridge".to_owned()),
        "intro" | "интро" | "вступление" => Some("Intro".to_owned()),
        "outro" | "ending" | "аутро" | "финал" => Some("Outro".to_owned()),
        _ => None,
    }
}

/// Stage directions and placeholders are never sung: a line wrapped entirely in
/// parentheses, asterisks or angle brackets, or a line without any letters.
fn is_placeholder_lyric_line(line: &str) -> bool {
    let trimmed = line.trim();
    if !trimmed.chars().any(char::is_alphanumeric) {
        return true;
    }
    (trimmed.starts_with('(') && trimmed.ends_with(')'))
        || (trimmed.starts_with('*') && trimmed.ends_with('*'))
        || (trimmed.starts_with('<') && trimmed.ends_with('>'))
}

fn clean_lyric_line(line: &str) -> String {
    let line = strip_list_numbering(line.trim());
    line.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Cheap sanity check that the lyrics are written in the declared language's script.
#[must_use]
pub fn lyrics_script_matches_language(text: &str, language: &str) -> bool {
    let cyrillic = text
        .chars()
        .filter(|ch| ('\u{0400}'..='\u{04ff}').contains(ch))
        .count();
    let latin = text.chars().filter(char::is_ascii_alphabetic).count();
    match language {
        "ru" | "uk" | "be" => cyrillic >= latin,
        "ja" | "ko" | "zh" => true,
        _ => latin >= cyrillic,
    }
}

/// The prompt sent to the music model is the compiled tag list itself.
#[must_use]
pub fn build_song_release_prompt(style: &str) -> String {
    let style = style.trim();
    if style.is_empty() {
        DEFAULT_SONG_STYLE_TAGS.to_owned()
    } else {
        style.to_owned()
    }
}

#[must_use]
pub fn build_song_file_name(author: &str, topic: &str, ext: &str) -> String {
    let author = author.trim();
    let topic = topic.trim();
    let ext = ext.trim().trim_start_matches('.');
    let ext = if ext.is_empty() {
        DEFAULT_AUDIO_FORMAT
    } else {
        ext
    };
    if author.is_empty() && topic.is_empty() {
        return format!("song.{ext}");
    }
    let base = match (author.is_empty(), topic.is_empty()) {
        (true, false) => topic.to_owned(),
        (false, true) => author.to_owned(),
        (false, false) => format!("{author} - {topic}"),
        (true, true) => String::new(),
    };
    let mut base = sanitize_song_file_name(&base);
    if base.len() > 60 {
        base = truncate_utf8(&base, 57);
        base.push_str("...");
    }
    format!("{base}.{ext}")
}

#[must_use]
pub fn song_file_extension(file_name: &str) -> String {
    file_name.rsplit_once('.').map_or_else(
        || DEFAULT_AUDIO_FORMAT.to_owned(),
        |(_, ext)| ext.to_owned(),
    )
}

#[must_use]
pub fn song_file_title(title: &str, fallback: &str) -> String {
    if title.trim().is_empty() {
        fallback.to_owned()
    } else {
        title.to_owned()
    }
}

fn add_text_field(form: multipart::Form, key: &'static str, value: &str) -> multipart::Form {
    if value.trim().is_empty() {
        form
    } else {
        form.text(key, value.to_owned())
    }
}

fn completion_content(req: &CompletionRequest) -> Result<String, AceStepError> {
    let mut out = String::new();
    write_tagged_text(&mut out, "prompt", &req.prompt);
    write_tagged_text(&mut out, "lyrics", &req.lyrics);
    if out.is_empty() {
        Err(AceStepError::InvalidRequest(
            "prompt and lyrics are both empty".to_owned(),
        ))
    } else {
        Ok(out)
    }
}

fn write_tagged_text(out: &mut String, tag: &str, value: &str) {
    let value = value.trim();
    if !value.is_empty() {
        out.push('<');
        out.push_str(tag);
        out.push('>');
        out.push_str(value);
        out.push_str("</");
        out.push_str(tag);
        out.push('>');
    }
}

fn parse_completion_response(
    body: &[u8],
    fallback_format: &str,
) -> Result<CompletionResult, AceStepError> {
    let value: Value = serde_json::from_slice(body).map_err(AceStepError::Json)?;
    if let Some(message) = value
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|message| !message.is_empty())
    {
        return Err(AceStepError::InvalidResponse(format!(
            "completion error: {message}"
        )));
    }
    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .ok_or_else(|| {
            AceStepError::InvalidResponse("completion response has no choices".to_owned())
        })?;
    let content = choice
        .pointer("/message/content")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let data_url = choice
        .pointer("/message/audio/0/audio_url/url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| {
            AceStepError::InvalidResponse("completion response has no audio".to_owned())
        })?;
    let (audio_data, ext) = decode_data_url(data_url)?;
    if audio_data.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "decoded audio is empty".to_owned(),
        ));
    }
    let ext = if ext.is_empty() {
        fallback_format.to_owned()
    } else {
        ext
    };
    Ok(CompletionResult {
        audio_data,
        file_name: format!("song.{ext}"),
        content,
        seed: value.pointer("/usage/seed").and_then(Value::as_i64),
        duration_seconds: value
            .pointer("/usage/duration_seconds")
            .and_then(Value::as_f64),
    })
}

fn decode_data_url(data_url: &str) -> Result<(Vec<u8>, String), AceStepError> {
    let Some(rest) = data_url.strip_prefix("data:") else {
        return Err(AceStepError::InvalidResponse("not a data URL".to_owned()));
    };
    let Some((header, b64_data)) = rest.split_once(',') else {
        return Err(AceStepError::InvalidResponse(
            "malformed data URL: no comma".to_owned(),
        ));
    };
    let decoded = general_purpose::STANDARD
        .decode(b64_data)
        .or_else(|_| general_purpose::STANDARD_NO_PAD.decode(b64_data))
        .map_err(|error| AceStepError::InvalidResponse(format!("base64 decode: {error}")))?;
    Ok((decoded, audio_extension_by_mime(data_url_mime(header))))
}

fn data_url_mime(header: &str) -> &str {
    header
        .split_once(';')
        .map_or(header, |(mime, _)| mime)
        .trim()
}

fn audio_extension_by_mime(mime: &str) -> String {
    match mime {
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/wav" | "audio/wave" | "audio/x-wav" => "wav",
        "audio/flac" => "flac",
        "audio/ogg" => "ogg",
        "audio/aac" | "audio/mp4" => "aac",
        _ => "",
    }
    .to_owned()
}

fn release_task_id(body: &[u8]) -> Result<String, AceStepError> {
    let value: Value = serde_json::from_slice(body).map_err(AceStepError::Json)?;
    let mut ids = extract_task_id_list(value.get("data").unwrap_or(&Value::Null));
    if ids.is_empty() {
        ids = extract_task_id_list(&value);
    }
    ids.into_iter().next().ok_or_else(|| {
        AceStepError::InvalidResponse("release_task response does not contain task id".to_owned())
    })
}

fn query_result_items(body: &[u8]) -> Result<Vec<TaskResult>, AceStepError> {
    let value: Value = serde_json::from_slice(body).map_err(AceStepError::Json)?;
    let mut items = parse_query_items(value.get("data").unwrap_or(&Value::Null));
    if items.is_empty() {
        items = parse_query_items(&value);
    }
    if items.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "query_result response does not contain task result".to_owned(),
        ));
    }
    Ok(items)
}

fn choose_task_result(items: Vec<TaskResult>, task_id: &str) -> TaskResult {
    items
        .iter()
        .find(|item| item.task_id.trim() == task_id)
        .cloned()
        .or_else(|| items.into_iter().next())
        .unwrap_or_default()
}

enum TaskWaitDecision {
    Done(TaskResult),
    Failed(String),
    Continue,
}

fn final_task_result(result: TaskResult) -> TaskWaitDecision {
    match result.status {
        TaskStatus::Success if result.files.is_empty() => {
            TaskWaitDecision::Failed("task completed without audio file".to_owned())
        }
        TaskStatus::Success => TaskWaitDecision::Done(result),
        TaskStatus::Failed => {
            let reason = if result.error.trim().is_empty() {
                "unknown error"
            } else {
                result.error.trim()
            };
            TaskWaitDecision::Failed(format!("task failed: {reason}"))
        }
        TaskStatus::Pending => TaskWaitDecision::Continue,
    }
}

fn extract_task_id_list(data: &Value) -> Vec<String> {
    match data {
        Value::String(value) => {
            let value = value.trim();
            if value.is_empty() {
                Vec::new()
            } else if let Some(decoded) = decode_json_container(value) {
                extract_task_id_list(&decoded)
            } else {
                vec![value.to_owned()]
            }
        }
        Value::Array(values) => unique_strings(
            values
                .iter()
                .flat_map(extract_task_id_list)
                .collect::<Vec<_>>(),
        ),
        Value::Object(map) => {
            let ids = map
                .get("task_id_list")
                .map(extract_task_id_list)
                .unwrap_or_default();
            if ids.is_empty() {
                map.get("task_id")
                    .map(string_from_value)
                    .map(|id| id.trim().to_owned())
                    .filter(|id| !id.is_empty())
                    .into_iter()
                    .collect()
            } else {
                ids
            }
        }
        _ => Vec::new(),
    }
}

fn parse_query_items(data: &Value) -> Vec<TaskResult> {
    match data {
        Value::String(value) => decode_json_text(value)
            .as_ref()
            .map(parse_query_items)
            .unwrap_or_default(),
        Value::Array(values) => values
            .iter()
            .filter_map(|item| item.as_object())
            .map(parse_task_result_map)
            .collect(),
        Value::Object(map) if map.contains_key("status") => vec![parse_task_result_map(map)],
        Value::Object(map) => {
            let ids = map
                .get("task_id_list")
                .map(extract_task_id_list)
                .unwrap_or_default();
            task_results_from_ids(data, ids)
        }
        _ => Vec::new(),
    }
}

fn task_results_from_ids(data: &Value, ids: Vec<String>) -> Vec<TaskResult> {
    if ids.is_empty() {
        return Vec::new();
    }
    ids.into_iter()
        .map(|task_id| TaskResult {
            task_id,
            status: parse_status(data.get("status").unwrap_or(&Value::Null)),
            files: extract_files(data.get("result").unwrap_or(&Value::Null)),
            error: task_result_error(data),
            raw_data: data.clone(),
        })
        .collect()
}

fn parse_task_result_map(map: &serde_json::Map<String, Value>) -> TaskResult {
    let value = Value::Object(map.clone());
    let mut error = map
        .get("error")
        .map(string_from_value)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if error.is_empty() {
        error = map
            .get("message")
            .map(string_from_value)
            .unwrap_or_default()
            .trim()
            .to_owned();
    }
    if error.is_empty() {
        error = map
            .get("result")
            .map(extract_error_from_result)
            .unwrap_or_default();
    }
    let mut files = map.get("result").map(extract_files).unwrap_or_default();
    files.extend(map.get("file").map(extract_files).unwrap_or_default());
    TaskResult {
        task_id: map
            .get("task_id")
            .map(string_from_value)
            .unwrap_or_default()
            .trim()
            .to_owned(),
        status: parse_status(map.get("status").unwrap_or(&Value::Null)),
        files: unique_strings(files),
        error,
        raw_data: value,
    }
}

fn parse_status(value: &Value) -> TaskStatus {
    match value {
        Value::Number(number) => number
            .as_i64()
            .map_or(TaskStatus::Pending, task_status_from_i64),
        Value::String(raw) => {
            let raw = raw.trim();
            if raw.parse::<i64>().is_ok() {
                return raw
                    .parse::<i64>()
                    .map_or(TaskStatus::Pending, task_status_from_i64);
            }
            if ["success", "completed", "done"]
                .iter()
                .any(|status| status.eq_ignore_ascii_case(raw))
            {
                TaskStatus::Success
            } else if ["failed", "error"]
                .iter()
                .any(|status| status.eq_ignore_ascii_case(raw))
            {
                TaskStatus::Failed
            } else {
                TaskStatus::Pending
            }
        }
        _ => TaskStatus::Pending,
    }
}

fn task_status_from_i64(value: i64) -> TaskStatus {
    match value {
        1 => TaskStatus::Success,
        2 => TaskStatus::Failed,
        _ => TaskStatus::Pending,
    }
}

fn extract_files(value: &Value) -> Vec<String> {
    unique_strings(collect_files(value))
}

fn collect_files(value: &Value) -> Vec<String> {
    match value {
        Value::String(raw) => {
            let raw = raw.trim();
            if raw.is_empty() {
                Vec::new()
            } else if let Some(decoded) = decode_json_container(raw) {
                collect_files(&decoded)
            } else {
                vec![raw.to_owned()]
            }
        }
        Value::Array(values) => values.iter().flat_map(collect_files).collect(),
        Value::Object(map) => FILE_CANDIDATE_KEYS
            .iter()
            .flat_map(|key| collect_files(map.get(*key).unwrap_or(&Value::Null)))
            .collect(),
        _ => Vec::new(),
    }
}

fn task_result_error(data: &Value) -> String {
    let error = data
        .get("error")
        .map(string_from_value)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if error.is_empty() {
        data.get("result")
            .map(extract_error_from_result)
            .unwrap_or_default()
    } else {
        error
    }
}

fn extract_error_from_result(value: &Value) -> String {
    match value {
        Value::String(raw) => decode_json_container(raw)
            .as_ref()
            .map(extract_error_from_result)
            .unwrap_or_default(),
        Value::Array(values) => values
            .iter()
            .find_map(|item| {
                let error = extract_error_from_result(item);
                (!error.is_empty()).then_some(error)
            })
            .unwrap_or_default(),
        Value::Object(map) => {
            for key in ERROR_CANDIDATE_KEYS {
                let message = map
                    .get(key)
                    .map(string_from_value)
                    .unwrap_or_default()
                    .trim()
                    .to_owned();
                if !message.is_empty() && message != "null" && message != "None" {
                    return message;
                }
            }
            map.get("result")
                .map(extract_error_from_result)
                .unwrap_or_default()
        }
        _ => String::new(),
    }
}

fn decode_json_container(raw: &str) -> Option<Value> {
    let raw = raw.trim();
    if raw.is_empty() || (!raw.starts_with('{') && !raw.starts_with('[')) {
        None
    } else {
        decode_json_text(raw)
    }
}

fn decode_json_text(raw: &str) -> Option<Value> {
    serde_json::from_str(raw).ok()
}

fn unique_strings(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_owned()) {
            continue;
        }
        out.push(trimmed.to_owned());
    }
    out
}

fn string_from_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn build_audio_url(base_url: &str, file: &str) -> String {
    let trimmed = file.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let base_url = base_url.trim_end_matches('/');
    let candidate = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_owned()
    } else if trimmed.starts_with('/') {
        format!("{base_url}{trimmed}")
    } else if trimmed.starts_with("v1/") || trimmed.contains('?') || trimmed.contains('/') {
        format!("{base_url}/{}", trimmed.trim_start_matches('/'))
    } else {
        let encoded: String = url::form_urlencoded::byte_serialize(trimmed.as_bytes()).collect();
        format!("{base_url}/v1/audio?path={encoded}")
    };
    let Ok(base) = url::Url::parse(base_url) else {
        return String::new();
    };
    let Ok(candidate) = url::Url::parse(&candidate) else {
        return String::new();
    };
    if !matches!(candidate.scheme(), "http" | "https")
        || !candidate.username().is_empty()
        || candidate.password().is_some()
        || candidate.scheme() != base.scheme()
        || candidate.host_str() != base.host_str()
        || candidate.port_or_known_default() != base.port_or_known_default()
    {
        return String::new();
    }
    candidate.to_string()
}

fn filename_from_headers(headers: &reqwest::header::HeaderMap) -> Option<String> {
    let raw = headers
        .get(reqwest::header::CONTENT_DISPOSITION)?
        .to_str()
        .ok()?
        .trim();
    for part in raw.split(';') {
        let part = part.trim();
        let Some(name) = part.strip_prefix("filename=") else {
            continue;
        };
        let name = name.trim().trim_matches('"').trim();
        if !name.is_empty() {
            return Some(name.to_owned());
        }
    }
    None
}

fn filename_from_url(raw_url: &str) -> Option<String> {
    let url = Url::parse(raw_url).ok()?;
    let base = Path::new(url.path())
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "." && *value != "/");
    if let Some(base) = base {
        return Some(base.to_owned());
    }
    url.query_pairs()
        .find(|(key, _)| key == "path")
        .and_then(|(_, path)| {
            Path::new(path.as_ref())
                .file_name()
                .and_then(|value| value.to_str())
                .map(str::trim)
                .filter(|value| !value.is_empty() && *value != "." && *value != "/")
                .map(str::to_owned)
        })
}

fn fallback_song_filename(fallback_ext: &str) -> String {
    let ext = fallback_ext.trim().trim_start_matches('.');
    if ext.is_empty() {
        format!("song.{DEFAULT_AUDIO_FORMAT}")
    } else {
        format!("song.{ext}")
    }
}

fn sanitize_song_file_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            '\n' | '\r' | '\t' => ' ',
            other => other,
        })
        .collect::<String>()
        .trim()
        .to_owned()
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    let mut end = 0;
    for (idx, ch) in value.char_indices() {
        let next = idx + ch.len_utf8();
        if next > max_bytes {
            break;
        }
        end = next;
    }
    value[..end].to_owned()
}

fn first_non_empty<const N: usize>(values: [&str; N]) -> Option<&str> {
    values
        .into_iter()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
        time::{Duration, SystemTime},
    };

    use serde_json::json;

    use super::{
        AceStepApiMode, AceStepClient, AceStepConfig, CompletionRequest,
        MAX_LOGGED_ERROR_BODY_BYTES, ReleaseTaskRequest, SONG_LANGUAGE_INVALID_REJECTION,
        SONG_LYRICS_LANGUAGE_REJECTION, SONG_LYRICS_STRUCTURE_REJECTION, SONG_MAX_TAGS,
        SONG_STYLE_INVALID_REJECTION, SongPromptPayload, SongPromptRequest, TaskStatus,
        bounded_http_error_body, build_audio_url, build_song_file_name, build_song_release_prompt,
        canonicalize_song_lyrics, compile_song_tags, detect_song_language, extract_files,
        extract_task_id_list, lenient_number, normalize_song_language, normalize_song_prompt_input,
        normalize_song_prompt_payload, normalize_song_tag, parse_completion_response,
        parse_query_items, query_result_items, release_task_id, render_song_director_messages_with,
        song_max_audio_seconds,
    };

    fn prompt_store_with(files: &[(&str, &str)]) -> openplotva_prompts::PromptStore {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "openplotva-acestep-prompts-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create prompt root");
        for (name, source) in files {
            let path = root.join(name);
            fs::create_dir_all(path.parent().expect("prompt parent"))
                .expect("create prompt directory");
            fs::write(path, source).expect("write prompt");
        }
        let store =
            openplotva_prompts::PromptStore::from_root(&root).expect("compile prompt store");
        fs::remove_dir_all(root).expect("remove prompt root");
        store
    }

    #[derive(Debug)]
    struct CapturedHttpRequest {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    struct FixtureHttpResponse {
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl FixtureHttpResponse {
        fn json(body: &str) -> Self {
            Self {
                status: 200,
                headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
                body: body.as_bytes().to_vec(),
            }
        }

        fn bytes(body: &[u8], headers: Vec<(String, String)>) -> Self {
            Self {
                status: 200,
                headers,
                body: body.to_vec(),
            }
        }
    }

    fn spawn_http_sequence(
        responses: Vec<FixtureHttpResponse>,
    ) -> (
        String,
        thread::JoinHandle<Result<Vec<CapturedHttpRequest>, String>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture server");
        let base_url = format!("http://{}", listener.local_addr().expect("local addr"));
        let handle = thread::spawn(move || {
            let mut captured = Vec::with_capacity(responses.len());
            for response in responses {
                let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
                let request = read_http_request(&mut stream)?;
                write_http_response(&mut stream, &response)?;
                captured.push(request);
            }
            Ok(captured)
        });
        (base_url, handle)
    }

    fn collect_requests(
        handle: thread::JoinHandle<Result<Vec<CapturedHttpRequest>, String>>,
    ) -> Result<Vec<CapturedHttpRequest>, Box<dyn std::error::Error>> {
        let requests = handle
            .join()
            .map_err(|_| std::io::Error::other("fixture server panicked"))?
            .map_err(std::io::Error::other)?;
        Ok(requests)
    }

    fn read_http_request(stream: &mut TcpStream) -> Result<CapturedHttpRequest, String> {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut chunk).map_err(|error| error.to_string())?;
            if read == 0 {
                return Err("connection closed before headers".to_owned());
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(end) = header_end(&buffer) {
                break end;
            }
        };
        let header_text = String::from_utf8_lossy(&buffer[..header_end - 4]);
        let mut lines = header_text.split("\r\n");
        let first_line = lines.next().ok_or("missing request line")?;
        let mut first_parts = first_line.split_whitespace();
        let method = first_parts.next().unwrap_or("").to_owned();
        let path = first_parts.next().unwrap_or("").to_owned();
        let headers: Vec<(String, String)> = lines
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_owned(), value.trim().to_owned()))
            })
            .collect();
        let content_length = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = buffer[header_end..].to_vec();
        while body.len() < content_length {
            let read = stream.read(&mut chunk).map_err(|error| error.to_string())?;
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
        }
        body.truncate(content_length);
        Ok(CapturedHttpRequest {
            method,
            path,
            headers,
            body,
        })
    }

    fn header_end(buffer: &[u8]) -> Option<usize> {
        buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
    }

    fn write_http_response(
        stream: &mut TcpStream,
        response: &FixtureHttpResponse,
    ) -> Result<(), String> {
        let reason = if response.status == 200 {
            "OK"
        } else {
            "ERROR"
        };
        write!(
            stream,
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            response.status,
            reason,
            response.body.len()
        )
        .map_err(|error| error.to_string())?;
        for (name, value) in &response.headers {
            write!(stream, "{name}: {value}\r\n").map_err(|error| error.to_string())?;
        }
        stream
            .write_all(b"\r\n")
            .map_err(|error| error.to_string())?;
        stream
            .write_all(&response.body)
            .map_err(|error| error.to_string())
    }

    fn request_header<'a>(request: &'a CapturedHttpRequest, name: &str) -> Option<&'a str> {
        request
            .headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn test_client(base_url: String, api_mode: AceStepApiMode) -> AceStepClient {
        AceStepClient::new(AceStepConfig {
            base_url,
            api_key: "secret".to_owned(),
            api_mode,
            request_timeout: Duration::from_secs(3),
            poll_interval: Duration::from_millis(1),
            task_timeout: Duration::from_secs(3),
            audio_format: "mp3".to_owned(),
            model: "model-a".to_owned(),
        })
        .expect("ACE-Step client")
    }

    #[test]
    fn api_mode_defaults_to_completion() {
        assert_eq!(AceStepApiMode::from_go(" native "), AceStepApiMode::Native);
        assert_eq!(
            AceStepApiMode::from_go("unknown"),
            AceStepApiMode::Completion
        );
    }

    #[test]
    fn completion_response_decodes_audio_data_url() -> Result<(), Box<dyn std::error::Error>> {
        let result = parse_completion_response(
            br#"{"choices":[{"message":{"content":"ok","audio":[{"audio_url":{"url":"data:audio/mpeg;base64,QUJD"}}]}}]}"#,
            "mp3",
        )?;

        assert_eq!(result.audio_data, b"ABC");
        assert_eq!(result.file_name, "song.mp3");
        assert_eq!(result.content, "ok");
        Ok(())
    }

    #[test]
    fn http_error_body_for_log_trims_and_caps_backend_response() {
        assert_eq!(
            bounded_http_error_body(b"\n error code: 504 \n"),
            "error code: 504"
        );

        let body = "x".repeat(MAX_LOGGED_ERROR_BODY_BYTES + 32);
        let logged = bounded_http_error_body(body.as_bytes());
        assert!(logged.len() <= MAX_LOGGED_ERROR_BODY_BYTES);
        assert!(logged.ends_with("...[truncated]"));
    }

    #[tokio::test]
    async fn completion_client_sends_go_shaped_request_and_decodes_audio()
    -> Result<(), Box<dyn std::error::Error>> {
        let (base_url, handle) = spawn_http_sequence(vec![FixtureHttpResponse::json(
            r#"{"choices":[{"message":{"content":"ok","audio":[{"audio_url":{"url":"data:audio/mpeg;base64,TVAz"}}]}}],"usage":{"seed":42,"duration_seconds":180.5}}"#,
        )]);
        let client = test_client(base_url, AceStepApiMode::Completion);

        let result = client
            .generate_completion(CompletionRequest {
                prompt: "neon rain".to_owned(),
                lyrics: "lyrics".to_owned(),
                vocal_language: "ru".to_owned(),
                audio_format: "mp3".to_owned(),
                model: "model-a".to_owned(),
                thinking: false,
                max_seconds: Some(200),
            })
            .await?;
        let requests = collect_requests(handle)?;

        assert_eq!(result.audio_data, b"MP3");
        assert_eq!(result.file_name, "song.mp3");
        assert_eq!(result.seed, Some(42));
        assert_eq!(result.duration_seconds, Some(180.5));
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/chat/completions");
        assert_eq!(
            request_header(request, "authorization"),
            Some("Bearer secret")
        );
        let body: serde_json::Value = serde_json::from_slice(&request.body)?;
        assert_eq!(body["model"], "model-a");
        assert_eq!(body["stream"], false);
        assert_eq!(body["thinking"], false);
        assert_eq!(body["audio_config"]["format"], "mp3");
        assert_eq!(body["audio_config"]["vocal_language"], "ru");
        assert_eq!(body["audio_config"]["max_seconds"], 200);
        assert_eq!(body["messages"][0]["role"], "user");
        let content = body["messages"][0]["content"].as_str().unwrap_or_default();
        assert!(content.contains("<prompt>neon rain</prompt>"));
        assert!(content.contains("<lyrics>lyrics</lyrics>"));
        Ok(())
    }

    #[test]
    fn native_release_and_query_parsers_handle_go_shapes() -> Result<(), Box<dyn std::error::Error>>
    {
        assert_eq!(
            release_task_id(br#"{"data":{"task_id_list":"[\"task-123\"]"}}"#)?,
            "task-123"
        );

        let result = query_result_items(
            br#"{"data":[{"task_id":"task-fail","status":2,"result":"[{\"file\":\"\",\"status\":2,\"error\":\"GPU out of memory\"}]"}]}"#,
        )?;
        assert_eq!(result[0].status, TaskStatus::Failed);
        assert_eq!(result[0].error, "GPU out of memory");

        let items = parse_query_items(
            &json!({"task_id_list":["task-1","task-2"],"result":{"file":"/done.mp3"}}),
        );
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].files, vec!["/done.mp3"]);
        assert_eq!(items[0].status, TaskStatus::Pending);
        Ok(())
    }

    #[tokio::test]
    async fn native_client_posts_release_query_and_downloads_audio()
    -> Result<(), Box<dyn std::error::Error>> {
        let (base_url, handle) = spawn_http_sequence(vec![
            FixtureHttpResponse::json(r#"{"data":{"task_id_list":"[\"task-1\"]"}}"#),
            FixtureHttpResponse::json(
                r#"{"data":[{"task_id":"task-1","status":1,"result":{"file":"/v1/audio?path=done.mp3"}}]}"#,
            ),
            FixtureHttpResponse::bytes(
                b"WAV",
                vec![(
                    "Content-Disposition".to_owned(),
                    r#"attachment; filename="done.wav""#.to_owned(),
                )],
            ),
        ]);
        let client = test_client(base_url, AceStepApiMode::Native);

        let task_id = client
            .release_task(ReleaseTaskRequest {
                prompt: "bright city".to_owned(),
                lyrics: "[Verse 1]\nline".to_owned(),
                vocal_language: "en".to_owned(),
                audio_format: "wav".to_owned(),
                model: "model-a".to_owned(),
                reference_audio: b"REF".to_vec(),
                reference_file_name: "ref.wav".to_owned(),
            })
            .await?;
        let result = client.wait_result(&task_id).await?;
        let audio = client.download_audio(&result.first_file()).await?;
        let requests = collect_requests(handle)?;

        assert_eq!(task_id, "task-1");
        assert_eq!(result.first_file(), "/v1/audio?path=done.mp3");
        assert_eq!(audio.data, b"WAV");
        assert_eq!(audio.file_name, "done.wav");
        assert_eq!(requests.len(), 3);

        let release = &requests[0];
        assert_eq!(release.method, "POST");
        assert_eq!(release.path, "/release_task");
        assert_eq!(
            request_header(release, "authorization"),
            Some("Bearer secret")
        );
        assert!(
            request_header(release, "content-type")
                .unwrap_or_default()
                .starts_with("multipart/form-data; boundary=")
        );
        let release_body = String::from_utf8_lossy(&release.body);
        for value in [
            "name=\"prompt\"",
            "bright city",
            "name=\"lyrics\"",
            "[Verse 1]\nline",
            "name=\"vocal_language\"",
            "en",
            "name=\"audio_format\"",
            "wav",
            "name=\"model\"",
            "model-a",
            "name=\"ai_token\"",
            "secret",
            "name=\"reference_audio\"",
            "filename=\"ref.wav\"",
            "REF",
        ] {
            assert!(release_body.contains(value), "missing {value}");
        }

        let query = &requests[1];
        assert_eq!(query.method, "POST");
        assert_eq!(query.path, "/query_result");
        assert_eq!(
            request_header(query, "authorization"),
            Some("Bearer secret")
        );
        let query_body: serde_json::Value = serde_json::from_slice(&query.body)?;
        assert_eq!(query_body["task_id_list"][0], "task-1");
        assert_eq!(query_body["ai_token"], "secret");

        let download = &requests[2];
        assert_eq!(download.method, "GET");
        assert_eq!(download.path, "/v1/audio?path=done.mp3");
        assert_eq!(
            request_header(download, "authorization"),
            Some("Bearer secret")
        );
        Ok(())
    }

    #[test]
    fn file_and_task_extractors_flatten_nested_containers_in_order() {
        assert_eq!(
            extract_task_id_list(&json!({"task_id_list": "[\"task-1\",\"task-2\"]"})),
            vec!["task-1", "task-2"]
        );
        assert_eq!(
            extract_files(&json!({
                "file": [" /one.mp3 ", "{\"file\":[\"/two.mp3\",\"/one.mp3\"],\"url\":\"/three.mp3\"}"],
                "url": " /four.mp3 "
            })),
            vec!["/one.mp3", "/two.mp3", "/three.mp3", "/four.mp3"]
        );
    }

    #[test]
    fn audio_url_builder_matches_go_branches() {
        let base = "http://127.0.0.1:8001";
        assert_eq!(build_audio_url(base, ""), "");
        assert_eq!(build_audio_url(base, "https://x/a.mp3"), "");
        assert_eq!(
            build_audio_url(base, "http://127.0.0.1:8001/a.mp3"),
            "http://127.0.0.1:8001/a.mp3"
        );
        assert_eq!(build_audio_url(base, "http://127.0.0.1:8002/a.mp3"), "");
        assert_eq!(
            build_audio_url(base, "/v1/a.mp3"),
            format!("{base}/v1/a.mp3")
        );
        assert_eq!(
            build_audio_url(base, "v1/a.mp3"),
            format!("{base}/v1/a.mp3")
        );
        assert_eq!(
            build_audio_url(base, "abc def.mp3"),
            format!("{base}/v1/audio?path=abc+def.mp3")
        );
    }

    fn rap_payload() -> SongPromptPayload {
        SongPromptPayload {
            analysis: "Brostep with rap.".to_owned(),
            title: "Bass To The Face".to_owned(),
            vocal_language: "en".to_owned(),
            vocals: "male".to_owned(),
            genre: "brostep, dubstep".to_owned(),
            bpm: 140,
            key: "F minor".to_owned(),
            sound: vec![
                "distorted mid-range growl bass".to_owned(),
                "wobble bass in call and response".to_owned(),
                "half-time drums with a big clap".to_owned(),
                "laser synths".to_owned(),
                "clean sub".to_owned(),
            ],
            character: vec![
                "aggressive".to_owned(),
                "energetic".to_owned(),
                "Aggressive".to_owned(),
            ],
            structure: vec![
                "serene melodic synth intro".to_owned(),
                "rap verse over a sparse beat".to_owned(),
                "snare-roll build with a shouted vocal sample".to_owned(),
                "second drop a whole step higher".to_owned(),
            ],
            vocal_style: "rap vocals with an aggressive chanted flow".to_owned(),
            references: vec!["in the style of Skrillex".to_owned()],
            duration_seconds: 190,
            lyrics: [
                "[Verse 1]",
                "Sirens on the skyline, engine in the red,",
                "Bass in the basement shaking every thread,",
                "Call the doctor, call the cops,",
                "When the sub drops low the whole block haunts.",
                "[Hook]",
                "Drop it! Drop it! Let the speakers bleed!",
                "Wub wub, that's the only thing we need!",
                "[verse 2]",
                "Concrete jungle, lasers on the glass,",
                "Every step a snare, every breath a bass,",
                "(instrumental break)",
                "Growl in the left, growl in the right,",
                "[Chorus]",
                "Drop it! Drop it! Let the speakers bleed!",
                "Wub wub, that's the only thing we need!",
            ]
            .join("\n"),
        }
    }

    #[test]
    fn song_prompt_input_prefers_hint_then_detects_from_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let (topic, lang) = normalize_song_prompt_input(&SongPromptRequest {
            topic: "ночной город".to_owned(),
            language_hint: "PL-pl".to_owned(),
            ..SongPromptRequest::default()
        })?;
        assert_eq!(topic, "ночной город");
        assert_eq!(lang, "pl");
        let (_, lang) = normalize_song_prompt_input(&SongPromptRequest {
            topic: "sad song".to_owned(),
            request_text: "!song сумна пісня про Київ уночі".to_owned(),
            ..SongPromptRequest::default()
        })?;
        assert_eq!(lang, "uk");
        assert_eq!(detect_song_language("city lights"), "en");
        assert_eq!(detect_song_language("ночной город"), "ru");
        assert_eq!(
            detect_song_language("рэйв у закінутым заводзе да світання"),
            "be"
        );
        assert_eq!(normalize_song_language("be-BY"), "be");
        assert_eq!(normalize_song_language("klingon"), "");
        assert!(
            normalize_song_prompt_input(&SongPromptRequest::default()).is_err(),
            "empty topic is rejected"
        );
        Ok(())
    }

    #[test]
    fn song_director_payload_compiles_tags_and_canonical_lyrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let result = normalize_song_prompt_payload(rap_payload(), "bass to the face", "ru")?;

        assert_eq!(
            result.style,
            "brostep, dubstep, 140 BPM, F minor, male rap vocals with an aggressive chanted flow, \
             distorted mid-range growl bass, wobble bass in call and response, \
             half-time drums with a big clap, laser synths, clean sub, aggressive, energetic, \
             serene melodic synth intro, rap verse over a sparse beat, \
             snare-roll build with a shouted vocal sample, second drop a whole step higher, \
             in the style of Skrillex"
        );
        assert_eq!(
            result.raw_style,
            "brostep, dubstep · 140 BPM · F minor · male rap vocals with an aggressive chanted flow"
        );
        assert_eq!(
            result.vocal_language, "en",
            "the director's language wins over the hint"
        );
        assert_eq!(result.vocals, "male");
        assert_eq!(result.title, "Bass To The Face");
        assert_eq!(result.duration_seconds, 190);
        assert_eq!(result.max_audio_seconds(), 235);
        assert_eq!(
            result.lyrics,
            "[Verse 1]\nSirens on the skyline, engine in the red,\nBass in the basement shaking every thread,\n\
             Call the doctor, call the cops,\nWhen the sub drops low the whole block haunts.\n\n\
             [Chorus]\nDrop it! Drop it! Let the speakers bleed!\nWub wub, that's the only thing we need!\n\n\
             [Verse 2]\nConcrete jungle, lasers on the glass,\nEvery step a snare, every breath a bass,\n\
             Growl in the left, growl in the right,\n\n\
             [Chorus]\nDrop it! Drop it! Let the speakers bleed!\nWub wub, that's the only thing we need!"
        );
        assert_eq!(result.brief["bpm"], 140);
        assert_eq!(result.brief["analysis"], "Brostep with rap.");
        Ok(())
    }

    #[test]
    fn song_director_payload_handles_instrumentals() -> Result<(), Box<dyn std::error::Error>> {
        let mut payload = rap_payload();
        payload.vocals = "instrumental".to_owned();
        payload.vocal_style = String::new();
        payload.duration_seconds = 0;
        payload.lyrics =
            "[Verse 1]\n[Instrumental - fast tremolo picking]\n(instrumental)".to_owned();

        let result = normalize_song_prompt_payload(payload, "dark neurofunk", "en")?;

        assert!(result.is_instrumental());
        assert_eq!(result.lyrics, "");
        assert_eq!(result.vocals, "instrumental");
        assert!(
            result.style.contains(", instrumental, "),
            "{}",
            result.style
        );
        assert!(!result.style.contains("rap vocals"));
        assert_eq!(result.duration_seconds, 180);
        assert_eq!(result.max_audio_seconds(), 180);
        assert_eq!(
            result.raw_style,
            "brostep, dubstep · 140 BPM · F minor · instrumental"
        );

        let mut placeholder_only = rap_payload();
        placeholder_only.lyrics = "(instrumental)\n---".to_owned();
        let result = normalize_song_prompt_payload(placeholder_only, "dark neurofunk", "en")?;
        assert!(
            result.is_instrumental(),
            "placeholder-only lyrics mean instrumental"
        );

        assert_eq!(song_max_audio_seconds(400, true), 360);
        assert_eq!(song_max_audio_seconds(10, true), 45);
        assert_eq!(song_max_audio_seconds(350, false), 360);
        assert_eq!(song_max_audio_seconds(0, false), 60);
        Ok(())
    }

    #[test]
    fn song_director_payload_rejects_bad_material() {
        let rejection = |payload: SongPromptPayload, language: &str| {
            normalize_song_prompt_payload(payload, "topic", language)
                .expect_err("rejected")
                .to_string()
        };

        let mut short = rap_payload();
        short.sound.clear();
        short.structure.clear();
        short.references.clear();
        assert_eq!(rejection(short, "en"), SONG_STYLE_INVALID_REJECTION);

        let mut no_chorus = rap_payload();
        no_chorus.lyrics = no_chorus
            .lyrics
            .replace("[Hook]", "[Verse 3]")
            .replace("[Chorus]", "[Bridge]");
        assert_eq!(rejection(no_chorus, "en"), SONG_LYRICS_STRUCTURE_REJECTION);

        let mut wrong_script = rap_payload();
        wrong_script.lyrics = [
            "[Verse 1]",
            "Ночь, улица, фонарь, аптека,",
            "Бессмысленный и тусклый свет,",
            "Живи ещё хоть четверть века,",
            "Всё будет так, исхода нет,",
            "[Chorus]",
            "Умрёшь, начнёшь опять сначала,",
            "И повторится всё, как встарь,",
            "Ночь, ледяная рябь канала,",
            "Аптека, улица, фонарь.",
        ]
        .join("\n");
        assert_eq!(
            rejection(wrong_script, "en"),
            SONG_LYRICS_LANGUAGE_REJECTION
        );

        let mut unsupported = rap_payload();
        unsupported.vocal_language = "klingon".to_owned();
        assert_eq!(
            rejection(unsupported, "tlh"),
            SONG_LANGUAGE_INVALID_REJECTION
        );

        let mut too_long = rap_payload();
        too_long.lyrics = std::iter::once("[Chorus]".to_owned())
            .chain((0..90).map(|index| format!("line {index}")))
            .collect::<Vec<_>>()
            .join("\n");
        assert_eq!(rejection(too_long, "en"), SONG_LYRICS_STRUCTURE_REJECTION);
    }

    #[test]
    fn song_tags_are_normalized_deduplicated_and_capped() {
        assert_eq!(
            normalize_song_tag("  1. Dark   sci-fi  atmosphere "),
            Some("Dark sci-fi atmosphere".to_owned())
        );
        assert_eq!(
            normalize_song_tag("\"flanged reese\""),
            Some("flanged reese".to_owned())
        );
        assert_eq!(
            normalize_song_tag("тёмный бас"),
            None,
            "non-Latin tags are dropped"
        );
        assert_eq!(
            normalize_song_tag("808"),
            None,
            "digits alone are not a tag"
        );
        assert_eq!(normalize_song_tag("x"), None);
        assert_eq!(
            normalize_song_tag("A flat major"),
            Some("A flat major".to_owned())
        );

        let mut payload = rap_payload();
        payload.sound = (0..60)
            .map(|index| format!("layer number {index}"))
            .collect();
        let tags = compile_song_tags(&payload, "male", false);
        assert_eq!(tags.len(), SONG_MAX_TAGS);
        assert_eq!(tags[0], "brostep");
        assert_eq!(tags[1], "dubstep");
        assert_eq!(tags[2], "140 BPM");

        let mut no_voice_word = rap_payload();
        no_voice_word.vocal_style = "clean pop vocals".to_owned();
        let tags = compile_song_tags(&no_voice_word, "female", false);
        assert!(tags.contains(&"female clean pop vocals".to_owned()));
        let mut no_style = rap_payload();
        no_style.vocal_style = String::new();
        let tags = compile_song_tags(&no_style, "duet", false);
        assert!(tags.contains(&"duet vocals".to_owned()));
    }

    #[test]
    fn lyrics_canonicalization_maps_markers_and_drops_placeholders() {
        let canonical = canonicalize_song_lyrics(
            "[Intro]\n(soft piano)\n[Куплет 1]\n1. первая строка\nвторая строка\n[Припев]\nхук раз\nхук два\n[Guitar solo]\n[Instrumental - tremolo]\n[Outro]\n*fade out*\nпоследняя строка",
        );
        assert_eq!(
            canonical.text,
            "[Verse 1]\nпервая строка\nвторая строка\n\n[Chorus]\nхук раз\nхук два\n\n[Outro]\nпоследняя строка"
        );
        assert_eq!(canonical.sections, 3);
        assert_eq!(canonical.line_count, 5);
        assert!(canonical.has_chorus);

        let orphan = canonicalize_song_lyrics("no markers here\nsecond line");
        assert_eq!(orphan.text, "[Verse]\nno markers here\nsecond line");
        assert!(!orphan.has_chorus);
        assert_eq!(canonicalize_song_lyrics("").line_count, 0);
    }

    #[test]
    fn lenient_numbers_parse_strings_and_timecodes() {
        assert_eq!(lenient_number(&json!(140)), 140);
        assert_eq!(lenient_number(&json!(174.4)), 174);
        assert_eq!(lenient_number(&json!("140 BPM")), 140);
        assert_eq!(lenient_number(&json!("3:00")), 180);
        assert_eq!(lenient_number(&json!("about ninety")), 0);
        assert_eq!(lenient_number(&json!(null)), 0);
        let payload: SongPromptPayload = serde_json::from_str(
            r#"{"title":"t","bpm":"128","duration_seconds":"2:30","sound":["a"]}"#,
        )
        .expect("lenient payload");
        assert_eq!(payload.bpm, 128);
        assert_eq!(payload.duration_seconds, 150);
        assert_eq!(payload.sound, vec!["a".to_owned()]);
    }

    #[test]
    fn song_director_messages_render_with_injected_store() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = prompt_store_with(&[(
            "music/song_director.prompt",
            "{{role \"system\"}}director {{maxDuration}}{{role \"user\"}}{{request}}|{{topic}}|{{userName}}|{{languageHint}}|{{#if context}}ctx:{{context}}{{/if}}",
        )]);
        let request = SongPromptRequest {
            topic: "night city".to_owned(),
            request_text: "!song night city, female vocals".to_owned(),
            user_full_name: "Alice".to_owned(),
            context: "Alice likes synthwave".to_owned(),
            ..SongPromptRequest::default()
        };

        let messages = render_song_director_messages_with(&store, &request, "night city", "en")?;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "system");
        assert_eq!(messages[0].content, "director 360");
        assert_eq!(messages[1].role, "user");
        assert_eq!(
            messages[1].content,
            "!song night city, female vocals|night city|Alice|en|ctx:Alice likes synthwave"
        );

        let bare = render_song_director_messages_with(
            &store,
            &SongPromptRequest {
                topic: "night city".to_owned(),
                ..SongPromptRequest::default()
            },
            "night city",
            "",
        )?;
        assert_eq!(bare[1].content, "night city|night city|listener|unknown|");
        Ok(())
    }

    #[test]
    fn song_file_and_release_prompt_helpers_match_go_shapes() {
        assert_eq!(
            build_song_release_prompt("  indie pop, 96 BPM "),
            "indie pop, 96 BPM"
        );
        assert_eq!(
            build_song_release_prompt(""),
            "indie pop, clear vocal, acoustic guitar, emotional, 96 BPM"
        );
        assert_eq!(
            build_song_file_name("Alice/Bob", "Night:City", "mp3"),
            "Alice_Bob - Night_City.mp3"
        );
    }
}
