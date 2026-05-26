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
pub const OPTIMIZE_SONG_PROMPT_TERMINATOR_TOOL_NAME: &str = "optimize_song_prompt_terminator";

const FILE_CANDIDATE_KEYS: [&str; 5] = ["file", "url", "audio", "audio_url", "path"];
const ERROR_CANDIDATE_KEYS: [&str; 4] = ["error", "message", "detail", "status_message"];
const SUPPORTED_SONG_LANGUAGES: [&str; 13] = [
    "ru", "en", "es", "de", "fr", "it", "pt", "pl", "tr", "uk", "ja", "ko", "zh",
];
const CONTRADICTORY_STYLE_PAIRS: [(&str, &str); 4] = [
    ("upbeat", "melancholic"),
    ("happy", "sad"),
    ("energetic", "ambient"),
    ("aggressive", "tender"),
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
        let body = json!({
            "model": model,
            "messages": [{"role": "user", "content": content}],
            "stream": false,
            "thinking": req.thinking,
            "audio_config": {
                "format": audio_format,
                "vocal_language": vocal_language,
            }
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

/// Completion-mode request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompletionRequest {
    pub prompt: String,
    pub lyrics: String,
    pub vocal_language: String,
    pub audio_format: String,
    pub model: String,
    pub thinking: bool,
}

/// Completion-mode result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompletionResult {
    pub audio_data: Vec<u8>,
    pub file_name: String,
    pub content: String,
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

/// Song-prompt input.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SongPromptRequest {
    pub topic: String,
    pub user_id: i64,
    pub message_id: i32,
    pub language_hint: String,
}

/// Normalized song material.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SongPromptResult {
    pub title: String,
    pub topic: String,
    pub raw_style: String,
    pub style: String,
    pub vocal_language: String,
    pub lyrics: String,
}

/// Provider payload for song reprompt.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SongPromptPayload {
    pub title: String,
    pub input_topic: String,
    pub style: String,
    pub vocal_language: String,
    pub lyrics: String,
}

/// Tool schema for the song reprompt terminator.
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

pub fn render_song_reprompt_prompt(
    topic: &str,
    vocal_language: &str,
) -> Result<String, openplotva_prompts::PromptError> {
    openplotva_prompts::render(
        "music/song_reprompt",
        &json!({
            "topic": topic,
            "vocalLanguage": vocal_language,
        }),
    )
}

pub fn render_song_reprompt_messages(
    topic: &str,
    vocal_language: &str,
) -> Result<Vec<openplotva_prompts::PromptMessage>, openplotva_prompts::PromptError> {
    openplotva_prompts::render_messages(
        "music/song_reprompt",
        &json!({
            "topic": topic,
            "vocalLanguage": vocal_language,
        }),
    )
}

#[must_use]
pub fn optimize_song_prompt_terminator_definition() -> SongPromptTerminatorDefinition {
    SongPromptTerminatorDefinition {
        name: OPTIMIZE_SONG_PROMPT_TERMINATOR_TOOL_NAME,
        description: "Finalize ACE-Step song prompt optimization with style tags and section-marked lyrics.",
        input_schema: json!({
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Short catchy song title (2-5 words), matching vocal_language"
                },
                "input_topic": { "type": "string" },
                "style": { "type": "string" },
                "vocal_language": { "type": "string" },
                "lyrics": { "type": "string" }
            },
            "required": ["title", "input_topic", "style", "vocal_language", "lyrics"]
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
        language = detect_song_language(topic);
    }
    if language.is_empty() {
        return Err(AceStepError::InvalidRequest(
            "song language is empty".to_owned(),
        ));
    }
    Ok((topic.to_owned(), language))
}

pub fn normalize_song_prompt_payload(
    payload: SongPromptPayload,
    requested_topic: &str,
    requested_language: &str,
) -> Result<SongPromptResult, AceStepError> {
    let mut result = SongPromptResult {
        title: payload.title.trim().to_owned(),
        topic: requested_topic.trim().to_owned(),
        vocal_language: requested_language.trim().to_owned(),
        ..SongPromptResult::default()
    };
    if !payload.input_topic.trim().is_empty() {
        result.topic = payload.input_topic.trim().to_owned();
    }
    if result.topic.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "song topic is empty".to_owned(),
        ));
    }
    if !payload.vocal_language.trim().is_empty() {
        let language = normalize_song_language(&payload.vocal_language);
        if language.is_empty() {
            return Err(AceStepError::InvalidResponse(
                "song vocal language is invalid".to_owned(),
            ));
        }
        result.vocal_language = language;
    }
    if result.vocal_language.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "song vocal language is empty".to_owned(),
        ));
    }
    let raw_style = payload.style.trim().to_owned();
    let style = normalize_song_style(&raw_style);
    if style.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "song style is invalid".to_owned(),
        ));
    }
    result.raw_style = raw_style;
    result.style = style;
    let lyrics = normalize_song_lyrics(&payload.lyrics);
    if lyrics.is_empty() {
        return Err(AceStepError::InvalidResponse(
            "song lyrics are empty".to_owned(),
        ));
    }
    if !has_song_minimum_structure(&lyrics) {
        return Err(AceStepError::InvalidResponse(
            "song lyrics do not satisfy minimum structure".to_owned(),
        ));
    }
    result.lyrics = lyrics;
    Ok(result)
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
    if text
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

/// Normalize ACE-Step style tags.
#[must_use]
pub fn normalize_song_style(style: &str) -> String {
    let cleaned = style.replace(['|', ';', '\n'], ",");
    let mut acc = SongStyleAccumulator::default();
    for raw_tag in cleaned.split(',') {
        if !acc.add(raw_tag) {
            return String::new();
        }
    }
    if acc.valid() {
        acc.tags.join(", ")
    } else {
        String::new()
    }
}

/// Normalize lyrics lines.
#[must_use]
pub fn normalize_song_lyrics(lyrics: &str) -> String {
    lyrics
        .trim()
        .split('\n')
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("\n")
}

#[must_use]
pub fn has_song_minimum_structure(lyrics: &str) -> bool {
    let sections = parse_song_sections(lyrics);
    if sections.is_empty() {
        return false;
    }
    let mut has_verse_1 = false;
    let mut has_verse_2 = false;
    let mut chorus_count = 0;
    for section in sections {
        match section.name.trim().to_ascii_lowercase().as_str() {
            "verse 1" => has_verse_1 = true,
            "verse 2" => has_verse_2 = true,
            "chorus" => chorus_count += 1,
            _ => {}
        }
        if section.line_count < 4 || section.line_count > 8 {
            return false;
        }
    }
    has_verse_1 && has_verse_2 && chorus_count >= 2
}

/// Build the prompt sent to ACE-Step.
#[must_use]
pub fn build_song_release_prompt(style: &str, topic: &str, vocal_language: &str) -> String {
    let mut style = style.trim().to_owned();
    let topic = topic.trim();
    if style.is_empty() && topic.is_empty() {
        return String::new();
    }
    if style.is_empty() {
        style = "indie pop, clear vocal, acoustic guitar, emotional, 96 BPM".to_owned();
    }
    if topic.is_empty() {
        return style;
    }
    if vocal_language.trim().eq_ignore_ascii_case("ru") {
        format!("{style}, песня о {topic}").trim().to_owned()
    } else {
        format!("{style}, song about {topic}").trim().to_owned()
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
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_owned();
    }
    let base_url = base_url.trim_end_matches('/');
    if trimmed.starts_with('/') {
        return format!("{base_url}{trimmed}");
    }
    if trimmed.starts_with("v1/") {
        return format!("{base_url}/{trimmed}");
    }
    if trimmed.contains('?') || trimmed.contains('/') {
        return format!("{base_url}/{}", trimmed.trim_start_matches('/'));
    }
    let encoded: String = url::form_urlencoded::byte_serialize(trimmed.as_bytes()).collect();
    format!("{base_url}/v1/audio?path={encoded}")
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

#[derive(Default)]
struct SongStyleAccumulator {
    tags: Vec<String>,
    has_bpm: bool,
}

impl SongStyleAccumulator {
    fn add(&mut self, raw_tag: &str) -> bool {
        let (tag, is_bpm) = normalize_song_style_tag(raw_tag);
        if tag.is_empty() {
            return true;
        }
        if !is_valid_song_style_tag(&tag, is_bpm) {
            return false;
        }
        if self.tags.iter().any(|seen| seen == &tag) {
            return true;
        }
        if is_bpm {
            if self.has_bpm {
                return false;
            }
            self.has_bpm = true;
        }
        self.tags.push(tag);
        true
    }

    fn valid(&self) -> bool {
        if self.tags.len() < 3 || self.tags.len() > 7 {
            return false;
        }
        if self.has_bpm
            && !self
                .tags
                .last()
                .is_some_and(|tag| is_song_style_bpm_tag(tag))
        {
            return false;
        }
        !has_contradictory_song_style_tags(&self.tags)
    }
}

fn normalize_song_style_tag(raw_tag: &str) -> (String, bool) {
    if let Some(bpm) = normalize_raw_song_bpm_tag(raw_tag) {
        return (bpm, true);
    }
    let tag = normalize_song_style_tag_text(raw_tag);
    if tag.is_empty() {
        return (String::new(), false);
    }
    if let Some(bpm) = normalize_song_bpm_tag(&tag) {
        return (bpm, true);
    }
    (tag, false)
}

fn normalize_raw_song_bpm_tag(raw_tag: &str) -> Option<String> {
    let mut fields = raw_tag.split_whitespace();
    let value = fields.next()?;
    let suffix = fields.next()?;
    if fields.next().is_some()
        || !suffix.eq_ignore_ascii_case("bpm")
        || !valid_song_bpm_value(value)
    {
        None
    } else {
        Some(format!("{value} BPM"))
    }
}

fn normalize_song_style_tag_text(raw_tag: &str) -> String {
    let mut out = String::with_capacity(raw_tag.len());
    let mut pending_space = false;
    for ch in raw_tag.trim().chars() {
        if ch.is_whitespace() {
            if !out.is_empty() {
                pending_space = true;
            }
            continue;
        }
        if pending_space {
            out.push(' ');
            pending_space = false;
        }
        out.extend(ch.to_lowercase());
    }
    out
}

fn normalize_song_bpm_tag(tag: &str) -> Option<String> {
    let value = tag.strip_suffix(" bpm")?.trim();
    valid_song_bpm_value(value).then(|| format!("{value} BPM"))
}

fn valid_song_bpm_value(value: &str) -> bool {
    (2..=3).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_valid_song_style_tag(tag: &str, is_bpm: bool) -> bool {
    if is_bpm {
        return true;
    }
    let mut has_letter = false;
    for ch in tag.chars() {
        let is_letter = ch.is_ascii_lowercase();
        let ok = is_letter || ch.is_ascii_digit() || " +&/-'".contains(ch);
        if !ok {
            return false;
        }
        has_letter |= is_letter;
    }
    has_letter
}

fn is_song_style_bpm_tag(tag: &str) -> bool {
    tag.ends_with(" BPM")
}

fn has_contradictory_song_style_tags(tags: &[String]) -> bool {
    CONTRADICTORY_STYLE_PAIRS.iter().any(|(left, right)| {
        tags.iter().any(|tag| tag == left) && tags.iter().any(|tag| tag == right)
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SongSection {
    name: String,
    line_count: usize,
}

fn parse_song_sections(lyrics: &str) -> Vec<SongSection> {
    let mut sections = Vec::new();
    let mut current: Option<usize> = None;
    for raw in lyrics.split('\n') {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if is_song_section_tag(line) {
            sections.push(SongSection {
                name: line[1..line.len() - 1].trim().to_owned(),
                line_count: 0,
            });
            current = sections.len().checked_sub(1);
            continue;
        }
        let Some(index) = current else {
            return Vec::new();
        };
        sections[index].line_count += 1;
    }
    sections
}

fn is_song_section_tag(line: &str) -> bool {
    line.starts_with('[') && line.ends_with(']') && line.len() > 2
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
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    use serde_json::json;

    use super::{
        AceStepApiMode, AceStepClient, AceStepConfig, CompletionRequest, ReleaseTaskRequest,
        SongPromptPayload, SongPromptRequest, TaskStatus, build_audio_url, build_song_file_name,
        build_song_release_prompt, detect_song_language, extract_files, extract_task_id_list,
        has_song_minimum_structure, normalize_song_language, normalize_song_lyrics,
        normalize_song_prompt_input, normalize_song_prompt_payload, normalize_song_style,
        parse_completion_response, parse_query_items, query_result_items, release_task_id,
        render_song_reprompt_messages,
    };

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

    #[tokio::test]
    async fn completion_client_sends_go_shaped_request_and_decodes_audio()
    -> Result<(), Box<dyn std::error::Error>> {
        let (base_url, handle) = spawn_http_sequence(vec![FixtureHttpResponse::json(
            r#"{"choices":[{"message":{"content":"ok","audio":[{"audio_url":{"url":"data:audio/mpeg;base64,TVAz"}}]}}]}"#,
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
            })
            .await?;
        let requests = collect_requests(handle)?;

        assert_eq!(result.audio_data, b"MP3");
        assert_eq!(result.file_name, "song.mp3");
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
        assert_eq!(build_audio_url(base, "https://x/a.mp3"), "https://x/a.mp3");
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

    #[test]
    fn song_prompt_normalization_matches_go_contract() -> Result<(), Box<dyn std::error::Error>> {
        let (topic, lang) = normalize_song_prompt_input(&SongPromptRequest {
            topic: "ночной город".to_owned(),
            ..SongPromptRequest::default()
        })?;
        assert_eq!(topic, "ночной город");
        assert_eq!(lang, "ru");
        assert_eq!(detect_song_language("city lights"), "en");
        assert_eq!(normalize_song_language("PL-pl"), "pl");
        assert_eq!(normalize_song_language("klingon"), "");

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
        let result = normalize_song_prompt_payload(
            SongPromptPayload {
                title: "City Lights".to_owned(),
                input_topic: "city lights".to_owned(),
                style: "synthwave|male vocal; synth bass\ndrum machine, atmospheric, 102 BPM"
                    .to_owned(),
                vocal_language: "en".to_owned(),
                lyrics,
            },
            "fallback",
            "en",
        )?;
        assert_eq!(
            result.style,
            "synthwave, male vocal, synth bass, drum machine, atmospheric, 102 BPM"
        );
        assert_eq!(result.vocal_language, "en");
        Ok(())
    }

    #[test]
    fn song_reprompt_messages_preserve_go_roles_and_variables()
    -> Result<(), Box<dyn std::error::Error>> {
        let messages = render_song_reprompt_messages("ночной город", "ru")?;

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].role, "system");
        assert!(
            messages[0]
                .content
                .contains("optimize_song_prompt_terminator")
        );
        assert_eq!(messages[1].role, "user");
        assert_eq!(
            messages[1].content,
            "Topic: ночной город\nVocal language: ru"
        );
        Ok(())
    }

    #[test]
    fn song_prompt_rejects_invalid_style_and_structure() {
        assert_eq!(
            normalize_song_style("upbeat, melancholic, piano"),
            String::new()
        );
        assert!(!has_song_minimum_structure(
            "[Verse 1]\none\ntwo\n[Chorus]\none\ntwo"
        ));
        assert_eq!(
            normalize_song_lyrics("  first line  \n\n second line\t\nthird line  "),
            "first line\n\nsecond line\nthird line"
        );
    }

    #[test]
    fn song_file_and_release_prompt_helpers_match_go_shapes() {
        assert_eq!(
            build_song_release_prompt("indie pop", "ночной город", "ru"),
            "indie pop, песня о ночной город"
        );
        assert_eq!(
            build_song_release_prompt("indie pop", "city lights", "en"),
            "indie pop, song about city lights"
        );
        assert_eq!(
            build_song_file_name("Alice/Bob", "Night:City", "mp3"),
            "Alice_Bob - Night_City.mp3"
        );
    }
}
