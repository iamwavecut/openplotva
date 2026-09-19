//! App-level YouTube summary runtime for the dialog toolbox.

use std::{
    collections::HashMap,
    error::Error,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use openplotva_config::AppConfig;
use openplotva_dialog::{DialogTraceArtifacts, DialogTraceUsage};
use openplotva_llm::gemini::{MODEL_GEMINI_FLASH_LITE, cache_contour_model};
use openplotva_llm::retry::{FailureReason, retryable_reason_from_message};
use quick_xml::de::from_str as xml_from_str;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use url::Url;

use crate::{
    dialog_tools::{YouTubeSummarizer, YouTubeSummaryFuture, YouTubeSummaryResult},
    routed_attempts::{
        RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
    },
    runtime_gemini_cache::resolve_google_ai_key,
};

const YOUTUBE_VIDEO_URL: &str = "https://www.youtube.com/watch?v=";
const INNERTUBE_PLAYER_URL: &str = "https://www.youtube.com/youtubei/v1/player";
const INNERTUBE_CLIENT_NAME: &str = "ANDROID";
const INNERTUBE_CLIENT_VERSION: &str = "20.10.38";
const GEMINI_API_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
const YOUTUBE_SUMMARY_TEMPERATURE: f64 = 0.3;
const YOUTUBE_TRANSCRIPT_TIMEOUT: Duration = Duration::from_secs(45);

type BoxedError = Box<dyn Error + Send + Sync>;

/// Error returned by the YouTube summary runtime.
#[derive(Debug, Error)]
pub enum YouTubeSummaryError {
    /// Input is not a YouTube URL or 11-character video ID.
    #[error("invalid video identifier")]
    InvalidVideoIdentifier,
    #[error("no transcript found for languages [ru en]")]
    NoTranscriptForLanguages,
    /// YouTube did not expose caption data.
    #[error("{0}")]
    Transcript(String),
    /// Google AI key is missing.
    #[error("google ai key is required")]
    MissingGoogleAiKey,
    /// Prompt loading failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// HTTP failed.
    #[error("{0}")]
    Http(String),
    /// JSON/XML decode failed.
    #[error("{0}")]
    Decode(String),
}

/// Error returned while building the runtime YouTube summarizer.
#[derive(Debug, Error)]
pub enum YouTubeSummaryBuildError {
    #[error("{provider} API key is required")]
    ProviderApiKeyRequired {
        /// Provider that needs credentials.
        provider: &'static str,
    },
}

/// Runtime config for YouTube summary.
#[derive(Clone, Debug, PartialEq)]
pub struct YouTubeSummaryConfig {
    /// Google AI API key.
    pub api_key: String,
    /// Direct Gemini model.
    pub model: String,
    /// Gemini API base URL.
    pub base_url: String,
    /// Generation request timeout.
    pub request_timeout: Duration,
    /// Transcript fetch timeout.
    pub transcript_timeout: Duration,
}

impl Default for YouTubeSummaryConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            model: MODEL_GEMINI_FLASH_LITE.to_owned(),
            base_url: GEMINI_API_BASE_URL.to_owned(),
            request_timeout: Duration::from_secs(600),
            transcript_timeout: YOUTUBE_TRANSCRIPT_TIMEOUT,
        }
    }
}

impl YouTubeSummaryConfig {
    fn with_defaults(mut self) -> Self {
        if self.model.trim().is_empty() {
            self.model = MODEL_GEMINI_FLASH_LITE.to_owned();
        }
        self.model = cache_contour_model(&self.model);
        if self.base_url.trim().is_empty() {
            self.base_url = GEMINI_API_BASE_URL.to_owned();
        }
        if self.request_timeout.is_zero() {
            self.request_timeout = Duration::from_secs(600);
        }
        if self.transcript_timeout.is_zero() {
            self.transcript_timeout = YOUTUBE_TRANSCRIPT_TIMEOUT;
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenAiCompatibleYouTubeSummaryConfig {
    /// Provider API key.
    pub api_key: String,
    /// Provider model after the GenKit provider prefix.
    pub model: String,
    /// Chat completions endpoint.
    pub direct_url: String,
    /// Request timeout.
    pub request_timeout: Duration,
}

impl OpenAiCompatibleYouTubeSummaryConfig {
    fn with_defaults(mut self) -> Self {
        if self.request_timeout.is_zero() {
            self.request_timeout = Duration::from_secs(600);
        }
        self
    }
}

#[derive(Clone)]
pub enum RuntimeYouTubeSummarizer {
    /// DB-routed summary generation with the existing transcript fetcher.
    Routed(RoutedYouTubeSummarizer),
    /// Direct Gemini/GoogleAI route.
    Gemini(GeminiYouTubeSummarizer),
    /// GenKit OpenAI-compatible plugin route.
    OpenAiCompatible(OpenAiCompatibleYouTubeSummarizer),
}

impl RuntimeYouTubeSummarizer {
    pub fn from_app_config(config: &AppConfig) -> Result<Option<Self>, YouTubeSummaryBuildError> {
        let api_key = resolve_google_ai_key(&config.google_ai);
        if api_key.trim().is_empty() {
            return Ok(None);
        }
        let model = crate::memory_runtime::genkit_runtime_default_model(config);
        let transcript = GeminiYouTubeSummarizer::new(YouTubeSummaryConfig {
            api_key: api_key.clone(),
            model: model.clone(),
            request_timeout: Duration::from_secs(
                config.llm.dialog.request_timeout_seconds.max(1) as u64
            ),
            ..YouTubeSummaryConfig::default()
        });
        if let Some(cfg) = openai_compatible_youtube_config_from_app_config(config, &model)? {
            return Ok(Some(Self::OpenAiCompatible(
                OpenAiCompatibleYouTubeSummarizer::new(transcript, cfg),
            )));
        }
        Ok(Some(Self::Gemini(transcript)))
    }

    pub fn routed_from_app_config(
        config: &AppConfig,
        walker: RoutedAttemptWalker,
    ) -> Result<Option<Self>, YouTubeSummaryBuildError> {
        let api_key = resolve_google_ai_key(&config.google_ai);
        if api_key.trim().is_empty() {
            return Ok(None);
        }
        let transcript = GeminiYouTubeSummarizer::new(YouTubeSummaryConfig {
            api_key,
            model: crate::memory_runtime::genkit_runtime_default_model(config),
            request_timeout: Duration::from_secs(
                config.llm.dialog.request_timeout_seconds.max(1) as u64
            ),
            ..YouTubeSummaryConfig::default()
        });
        Ok(Some(Self::Routed(RoutedYouTubeSummarizer::new(
            transcript, walker, config,
        ))))
    }

    /// Human-readable provider label for readiness diagnostics.
    #[must_use]
    pub fn provider_label(&self) -> &'static str {
        match self {
            Self::Routed(_) => "routed provider",
            Self::Gemini(_) => "direct Gemini",
            Self::OpenAiCompatible(summarizer) => summarizer.provider,
        }
    }
}

impl YouTubeSummarizer for RuntimeYouTubeSummarizer {
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
        match self {
            Self::Routed(summarizer) => summarizer.summarize(video),
            Self::Gemini(summarizer) => summarizer.summarize(video),
            Self::OpenAiCompatible(summarizer) => summarizer.summarize(video),
        }
    }
}

#[derive(Clone)]
pub struct RoutedYouTubeSummarizer {
    transcript: GeminiYouTubeSummarizer,
    walker: RoutedAttemptWalker,
    config: Arc<AppConfig>,
}

impl RoutedYouTubeSummarizer {
    #[must_use]
    pub fn new(
        transcript: GeminiYouTubeSummarizer,
        walker: RoutedAttemptWalker,
        config: &AppConfig,
    ) -> Self {
        Self {
            transcript,
            walker,
            config: Arc::new(config.clone()),
        }
    }

    async fn run(&self, video: &str) -> Result<YouTubeSummaryResult, YouTubeSummaryError> {
        let fetched = self.transcript.transcript_for_video(video).await?;
        summary_result(self, fetched).await
    }
}

impl YouTubeSummaryModel for RoutedYouTubeSummarizer {
    fn complete<'a>(&'a self, stage: YouTubeStage, payload: &'a str) -> YouTubeModelFuture<'a> {
        Box::pin(async move {
            let config = Arc::clone(&self.config);
            let payload = payload.to_owned();
            self.walker
                .run(
                    RoutedRequestContext {
                        workflow_key: "youtube_summary".to_owned(),
                        vip: false,
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let config = Arc::clone(&config);
                        let payload = payload.clone();
                        async move {
                            generate_youtube_summary_with_attempt(&config, attempt, stage, &payload)
                                .await
                        }
                    },
                    youtube_summary_retryable,
                )
                .await
                .map_err(|error| match error {
                    RoutedAttemptRunError::Attempt(error) => error,
                    RoutedAttemptRunError::Routing(error) => {
                        YouTubeSummaryError::Http(error.to_string())
                    }
                })
        })
    }
}

impl YouTubeSummarizer for RoutedYouTubeSummarizer {
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
        Box::pin(async move {
            self.run(video)
                .await
                .map_err(|error| Box::new(error) as BoxedError)
        })
    }
}

#[derive(Clone)]
pub struct GeminiYouTubeSummarizer {
    cfg: YouTubeSummaryConfig,
    http: reqwest::Client,
}

impl GeminiYouTubeSummarizer {
    /// Build from app config when a Google AI key resolves.
    pub fn from_app_config(config: &AppConfig) -> Option<Self> {
        let api_key = resolve_google_ai_key(&config.google_ai);
        if api_key.trim().is_empty() {
            return None;
        }
        Some(Self::new(YouTubeSummaryConfig {
            api_key,
            model: crate::memory_runtime::genkit_runtime_default_model(config),
            request_timeout: Duration::from_secs(
                config.llm.dialog.request_timeout_seconds.max(1) as u64
            ),
            ..YouTubeSummaryConfig::default()
        }))
    }

    /// Build with explicit config.
    #[must_use]
    pub fn new(cfg: YouTubeSummaryConfig) -> Self {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(cfg.request_timeout)
            .build()
            .expect("YouTube reqwest client configuration is valid");
        Self { cfg, http }
    }

    async fn run(&self, video: &str) -> Result<YouTubeSummaryResult, YouTubeSummaryError> {
        let fetched = self.transcript_for_video(video).await?;
        summary_result(self, fetched).await
    }

    async fn transcript_for_video(
        &self,
        video: &str,
    ) -> Result<FetchedTranscript, YouTubeSummaryError> {
        if self.cfg.api_key.trim().is_empty() {
            return Err(YouTubeSummaryError::MissingGoogleAiKey);
        }
        let video_id =
            parse_youtube_video_id(video).ok_or(YouTubeSummaryError::InvalidVideoIdentifier)?;
        let transcript = tokio::time::timeout(
            self.cfg.transcript_timeout,
            self.fetch_youtube_transcript(&video_id),
        )
        .await
        .map_err(|_| YouTubeSummaryError::Http("context deadline exceeded".to_owned()))??;
        Ok(FetchedTranscript::new(&transcript))
    }

    async fn fetch_youtube_transcript(
        &self,
        video_id: &str,
    ) -> Result<Vec<Transcript>, YouTubeSummaryError> {
        let (watch_html, consent_cookie) = self.fetch_video_page(video_id).await?;
        let api_key = extract_innertube_api_key(&watch_html).ok_or_else(|| {
            YouTubeSummaryError::Transcript("innerTube API key not found".to_owned())
        })?;
        let player = self
            .fetch_innertube_player(video_id, &api_key, consent_cookie.as_deref())
            .await?;
        let tracks = select_caption_tracks(&player, &["ru", "en"])?;
        let mut transcripts = Vec::with_capacity(tracks.len());
        for track in tracks {
            let caption_url = validated_caption_url(&track.base_url)?;
            let xml = self
                .get_text(&caption_url, consent_cookie.as_deref())
                .await?;
            let lines = parse_transcript_xml(&xml)?;
            transcripts.push(Transcript {
                language: track.display_language(),
                language_code: track.language_code,
                lines,
            });
        }
        Ok(transcripts)
    }

    async fn fetch_video_page(
        &self,
        video_id: &str,
    ) -> Result<(String, Option<String>), YouTubeSummaryError> {
        let url = format!("{YOUTUBE_VIDEO_URL}{video_id}");
        let body = self.get_text(&url, None).await?;
        if !consent_required(&body) {
            return Ok((body, None));
        }
        let cookie = consent_cookie_from_html(&body).ok_or_else(|| {
            YouTubeSummaryError::Transcript("failed to find consent value in HTML".to_owned())
        })?;
        let body = self.get_text(&url, Some(&cookie)).await?;
        Ok((body, Some(cookie)))
    }

    async fn fetch_innertube_player(
        &self,
        video_id: &str,
        api_key: &str,
        cookie: Option<&str>,
    ) -> Result<InnertubePlayerResponse, YouTubeSummaryError> {
        let url = format!("{INNERTUBE_PLAYER_URL}?key={api_key}");
        let payload = json!({
            "context": {
                "client": {
                    "clientName": INNERTUBE_CLIENT_NAME,
                    "clientVersion": INNERTUBE_CLIENT_VERSION,
                }
            },
            "videoId": video_id,
        });
        let mut request = self.http.post(url).json(&payload);
        if let Some(cookie) = cookie {
            request = request.header(reqwest::header::COOKIE, cookie);
        }
        let response = request.send().await.map_err(http_error_text)?;
        let status = response.status();
        let body = response.text().await.map_err(http_error_text)?;
        if !status.is_success() {
            return Err(YouTubeSummaryError::Http(format!(
                "received non-OK status code: {}",
                status.as_u16()
            )));
        }
        serde_json::from_str(&body).map_err(|error| {
            YouTubeSummaryError::Decode(format!("failed to decode response JSON: {error}"))
        })
    }

    async fn get_text(
        &self,
        url: &str,
        cookie: Option<&str>,
    ) -> Result<String, YouTubeSummaryError> {
        let mut last_error = None;
        for attempt in 1..=3 {
            let mut request = self
                .http
                .get(url)
                .header(reqwest::header::ACCEPT_LANGUAGE, "en-US");
            if let Some(cookie) = cookie {
                request = request.header(reqwest::header::COOKIE, cookie);
            }
            match request.send().await {
                Ok(response) if response.status().is_success() => {
                    return response.text().await.map_err(http_error_text);
                }
                Ok(response) => {
                    last_error = Some(format!(
                        "Retry {attempt}: received non-OK status code: {}",
                        response.status().as_u16()
                    ));
                }
                Err(error) => {
                    last_error = Some(format!("Retry {attempt}: failed to fetch: {error}"));
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Err(YouTubeSummaryError::Http(format!(
            "failed to fetch after retries: {}",
            last_error.unwrap_or_else(|| "unknown error".to_owned())
        )))
    }

    async fn generate(
        &self,
        stage: YouTubeStage,
        payload: &str,
    ) -> Result<String, YouTubeSummaryError> {
        let system = openplotva_prompts::read(stage.prompt_name())?;
        let request = youtube_summary_gemini_request(&system, payload, stage);
        let model = cache_contour_model(&self.cfg.model);
        let url = gemini_generate_url(&self.cfg.base_url, &model)?;
        let trace = YouTubeCallTrace::begin(
            YouTubeTraceTags {
                provider: "genkit",
                source: "youtube_gemini",
                request_kind: "gemini.generateContent",
            },
            &model,
            &request,
        );
        let sent = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-goog-api-key", self.cfg.api_key.trim())
            .json(&request)
            .send()
            .await;
        let (status, body) = trace.read_response(sent).await?;
        if !status.is_success() {
            return Err(YouTubeSummaryError::Http(format!(
                "HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body).trim()
            )));
        }
        decode_gemini_text(&body)
    }
}

impl YouTubeSummaryModel for GeminiYouTubeSummarizer {
    fn complete<'a>(&'a self, stage: YouTubeStage, payload: &'a str) -> YouTubeModelFuture<'a> {
        Box::pin(self.generate(stage, payload))
    }
}

impl YouTubeSummarizer for GeminiYouTubeSummarizer {
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
        Box::pin(async move {
            self.run(video)
                .await
                .map_err(|error| Box::new(error) as BoxedError)
        })
    }
}

#[derive(Clone)]
pub struct OpenAiCompatibleYouTubeSummarizer {
    transcript: GeminiYouTubeSummarizer,
    cfg: OpenAiCompatibleYouTubeSummaryConfig,
    provider: &'static str,
    http: reqwest::Client,
}

impl OpenAiCompatibleYouTubeSummarizer {
    /// Build with explicit transcript fetcher and OpenAI-compatible config.
    #[must_use]
    pub fn new(
        transcript: GeminiYouTubeSummarizer,
        cfg: OpenAiCompatibleYouTubeSummaryConfig,
    ) -> Self {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(cfg.request_timeout)
            .build()
            .expect("OpenAI-compatible YouTube reqwest client configuration is valid");
        let provider = if cfg.direct_url.contains("openrouter.ai") {
            "openrouter"
        } else {
            "openai-compatible"
        };
        Self {
            transcript,
            cfg,
            provider,
            http,
        }
    }

    async fn run(&self, video: &str) -> Result<YouTubeSummaryResult, YouTubeSummaryError> {
        let fetched = self.transcript.transcript_for_video(video).await?;
        summary_result(self, fetched).await
    }

    async fn generate(
        &self,
        stage: YouTubeStage,
        payload: &str,
    ) -> Result<String, YouTubeSummaryError> {
        let system = openplotva_prompts::read(stage.prompt_name())?;
        let request = youtube_summary_openai_request(&self.cfg.model, &system, payload, stage);
        let trace = YouTubeCallTrace::begin(
            YouTubeTraceTags {
                provider: self.provider,
                source: "youtube_openai_compatible",
                request_kind: "openai.chat.completions",
            },
            &self.cfg.model,
            &request,
        );
        let sent = self
            .http
            .post(self.cfg.direct_url.trim())
            .bearer_auth(self.cfg.api_key.trim())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&request)
            .send()
            .await;
        let (status, body) = trace.read_response(sent).await?;
        if !status.is_success() {
            return Err(YouTubeSummaryError::Http(format!(
                "HTTP {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body).trim()
            )));
        }
        decode_openai_text(&body)
    }
}

impl YouTubeSummaryModel for OpenAiCompatibleYouTubeSummarizer {
    fn complete<'a>(&'a self, stage: YouTubeStage, payload: &'a str) -> YouTubeModelFuture<'a> {
        Box::pin(self.generate(stage, payload))
    }
}

impl YouTubeSummarizer for OpenAiCompatibleYouTubeSummarizer {
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
        Box::pin(async move {
            self.run(video)
                .await
                .map_err(|error| Box::new(error) as BoxedError)
        })
    }
}

async fn generate_youtube_summary_with_attempt(
    config: &AppConfig,
    attempt: RoutedAttempt,
    stage: YouTubeStage,
    payload: &str,
) -> Result<String, YouTubeSummaryError> {
    let system = openplotva_prompts::read(stage.prompt_name())?;
    let model = youtube_model_for_attempt(&attempt);
    let request = youtube_summary_openai_request(&model, &system, payload, stage);
    let endpoint = routed_attempt_endpoint(&attempt).ok_or_else(|| {
        YouTubeSummaryError::Http("routed provider has no chat completions endpoint".to_owned())
    })?;
    let api_key = routed_attempt_api_key(config, &attempt).ok_or_else(|| {
        YouTubeSummaryError::Http(format!(
            "provider {} API key is not configured",
            attempt.provider_name
        ))
    })?;
    let timeout = routed_attempt_timeout(config, &attempt);
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(timeout)
        .build()
        .expect("routed YouTube reqwest client configuration is valid");
    let mut builder = http
        .post(endpoint)
        .bearer_auth(api_key.trim())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .json(&request);
    if let Some(site_url) = attempt
        .provider_config
        .get("site_url")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        builder = builder.header("HTTP-Referer", site_url.trim());
    }
    if let Some(app_title) = attempt
        .provider_config
        .get("app_title")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        builder = builder.header("X-Title", app_title.trim());
    }
    let trace = YouTubeCallTrace::begin(
        YouTubeTraceTags {
            provider: &attempt.provider_name,
            source: "youtube_routed",
            request_kind: "openai.chat.completions",
        },
        &model,
        &request,
    );
    let sent = builder.send().await;
    let retry_after = sent
        .as_ref()
        .ok()
        .and_then(|response| response.headers().get(reqwest::header::RETRY_AFTER))
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let (status, body) = trace.read_response(sent).await?;
    if !status.is_success() {
        let mut message = format!(
            "HTTP {}: {}",
            status.as_u16(),
            String::from_utf8_lossy(&body).trim()
        );
        if let Some(retry_after) = retry_after
            && !retry_after.trim().is_empty()
        {
            message.push_str(&format!(" retry_after_seconds={}", retry_after.trim()));
        }
        return Err(YouTubeSummaryError::Http(message));
    }
    decode_openai_text(&body)
}

struct YouTubeTraceTags<'a> {
    provider: &'a str,
    source: &'a str,
    request_kind: &'a str,
}

struct YouTubeCallTrace {
    artifact: DialogTraceArtifacts,
    started: Instant,
}

impl YouTubeCallTrace {
    fn begin<T: Serialize>(tags: YouTubeTraceTags<'_>, model: &str, request: &T) -> Self {
        let raw_request = serde_json::to_value(request).ok();
        let prompt_chars = raw_request.as_ref().map_or(0, |value| {
            i32::try_from(value.to_string().len()).unwrap_or(i32::MAX)
        });
        let request_param = |openai: &str, gemini: &str| {
            raw_request
                .as_ref()
                .and_then(|value| value.get(openai).or_else(|| value.pointer(gemini)))
                .cloned()
        };
        let max_tokens = request_param("max_tokens", "/generationConfig/maxOutputTokens");
        let temperature = request_param("temperature", "/generationConfig/temperature");
        Self {
            artifact: DialogTraceArtifacts {
                provider: tags.provider.trim().to_owned(),
                request_kind: tags.request_kind.to_owned(),
                source: tags.source.to_owned(),
                mode: "text".to_owned(),
                flow: "youtube_summary".to_owned(),
                iteration: 1,
                model: model.trim().to_owned(),
                raw_request,
                inference_params: Some(json!({
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                })),
                prompt_chars,
                prompt_messages: 2,
                ..DialogTraceArtifacts::default()
            },
            started: Instant::now(),
        }
    }

    async fn read_response(
        self,
        sent: Result<reqwest::Response, reqwest::Error>,
    ) -> Result<(reqwest::StatusCode, Vec<u8>), YouTubeSummaryError> {
        let response = match sent {
            Ok(response) => response,
            Err(error) => {
                let error = http_error_text(error);
                self.finish(None, Some(error.to_string()));
                return Err(error);
            }
        };
        let status = response.status();
        match response.bytes().await {
            Ok(body) => {
                let error = (!status.is_success()).then(|| format!("HTTP {}", status.as_u16()));
                self.finish(Some(&body), error);
                Ok((status, body.to_vec()))
            }
            Err(error) => {
                let error = http_error_text(error);
                self.finish(None, Some(error.to_string()));
                Err(error)
            }
        }
    }

    fn finish(self, body: Option<&[u8]>, error: Option<String>) {
        let duration_ms = i32::try_from(self.started.elapsed().as_millis()).unwrap_or(i32::MAX);
        openplotva_llm::trace::observe(openplotva_llm::LlmCallRecord {
            artifact: youtube_trace_artifact(self.artifact, body, error),
            duration_ms,
            ..openplotva_llm::LlmCallRecord::default()
        });
    }
}

fn youtube_trace_artifact(
    mut artifact: DialogTraceArtifacts,
    body: Option<&[u8]>,
    error: Option<String>,
) -> DialogTraceArtifacts {
    let response = body.and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok());
    artifact.usage = response.as_ref().and_then(youtube_trace_usage);
    artifact.raw_response = response;
    artifact.error = error.unwrap_or_default();
    artifact
}

fn youtube_trace_usage(response: &serde_json::Value) -> Option<DialogTraceUsage> {
    let count = |value: &serde_json::Value, key: &str| {
        value
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .and_then(|count| i32::try_from(count).ok())
            .unwrap_or_default()
    };
    if let Some(usage) = response.get("usage") {
        return Some(DialogTraceUsage {
            input_tokens: count(usage, "prompt_tokens"),
            output_tokens: count(usage, "completion_tokens"),
            total_tokens: count(usage, "total_tokens"),
            cached_tokens: usage
                .get("prompt_tokens_details")
                .map_or(0, |details| count(details, "cached_tokens")),
            ..DialogTraceUsage::default()
        });
    }
    let usage = response.get("usageMetadata")?;
    Some(DialogTraceUsage {
        input_tokens: count(usage, "promptTokenCount"),
        output_tokens: count(usage, "candidatesTokenCount"),
        total_tokens: count(usage, "totalTokenCount"),
        cached_tokens: count(usage, "cachedContentTokenCount"),
        thoughts_tokens: count(usage, "thoughtsTokenCount"),
        ..DialogTraceUsage::default()
    })
}

fn youtube_model_for_attempt(attempt: &RoutedAttempt) -> String {
    if routed_attempt_is_openrouter(attempt)
        && !attempt
            .model_name
            .get(..OPENROUTER_MODEL_PREFIX.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(OPENROUTER_MODEL_PREFIX))
    {
        format!("{OPENROUTER_MODEL_PREFIX}{}", attempt.model_name.trim())
    } else {
        attempt.model_name.clone()
    }
}

fn routed_attempt_is_openrouter(attempt: &RoutedAttempt) -> bool {
    attempt.provider_name.eq_ignore_ascii_case("openrouter")
        || attempt
            .provider_name
            .eq_ignore_ascii_case("openrouter-free")
        || attempt
            .provider_endpoint
            .as_deref()
            .is_some_and(|endpoint| endpoint.contains("openrouter.ai"))
        || attempt
            .model_base_url
            .as_deref()
            .is_some_and(|endpoint| endpoint.contains("openrouter.ai"))
}

fn routed_attempt_endpoint(attempt: &RoutedAttempt) -> Option<String> {
    attempt
        .model_base_url
        .as_deref()
        .or(attempt.provider_endpoint.as_deref())
        .filter(|endpoint| !endpoint.trim().is_empty())
        .map(openplotva_llm::aifarm::normalize_chat_completions_url)
}

fn routed_attempt_api_key(config: &AppConfig, attempt: &RoutedAttempt) -> Option<String> {
    if let Some(reference) = attempt
        .provider_api_key_ref
        .as_deref()
        .filter(|reference| !reference.trim().is_empty())
        && let Ok(value) = std::env::var(reference.trim())
        && !value.trim().is_empty()
    {
        return Some(value);
    }
    if let Some(sealed) = attempt.provider_api_key_encrypted.as_deref() {
        let master = std::env::var("MASTER_KEY").unwrap_or_default();
        if let Ok(value) = openplotva_storage::llm_routing::open_key(&master, sealed)
            && !value.trim().is_empty()
        {
            return Some(value);
        }
    }
    if routed_attempt_is_openrouter(attempt) && !config.open_router.key.trim().is_empty() {
        return Some(config.open_router.key.trim().to_owned());
    }
    None
}

fn routed_attempt_timeout(config: &AppConfig, attempt: &RoutedAttempt) -> Duration {
    attempt
        .provider_config
        .get("timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .map(Duration::from_millis)
        .filter(|duration| !duration.is_zero())
        .unwrap_or_else(|| {
            Duration::from_secs(config.open_router.request_timeout_seconds.max(1) as u64)
        })
}

fn youtube_summary_retryable(error: &YouTubeSummaryError) -> Option<FailureReason> {
    match error {
        YouTubeSummaryError::Http(message) => retryable_reason_from_message(message),
        _ => None,
    }
}

fn openai_compatible_youtube_config_from_app_config(
    config: &AppConfig,
    model: &str,
) -> Result<Option<OpenAiCompatibleYouTubeSummaryConfig>, YouTubeSummaryBuildError> {
    let model = model.trim();
    let (direct_url, api_key, model, request_timeout_seconds, provider) =
        if let Some(model) = strip_prefix_fold(model, OPENROUTER_MODEL_PREFIX) {
            (
                OPENROUTER_CHAT_COMPLETIONS_URL,
                config.open_router.key.trim().to_owned(),
                model.trim().to_owned(),
                config.open_router.request_timeout_seconds,
                "openrouter",
            )
        } else {
            return Ok(None);
        };
    if model.is_empty() {
        return Ok(None);
    }
    if api_key.trim().is_empty() {
        return Err(YouTubeSummaryBuildError::ProviderApiKeyRequired { provider });
    }
    Ok(Some(OpenAiCompatibleYouTubeSummaryConfig {
        api_key,
        model,
        direct_url: direct_url.to_owned(),
        request_timeout: Duration::from_secs(request_timeout_seconds.max(1) as u64),
    }))
}

#[must_use]
pub fn parse_youtube_video_id(input: &str) -> Option<String> {
    let value = input.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(id) = extract_youtube_video_id_from_text(value) {
        return Some(id);
    }
    if value.len() == 11 && is_valid_video_id(value) {
        return Some(value.to_owned());
    }
    None
}

fn extract_youtube_video_id_from_text(value: &str) -> Option<String> {
    find_after_any(value, &["youtu.be/"])
        .or_else(|| {
            find_after_any(
                value,
                &[
                    "youtube.com/shorts/",
                    "youtube.com/live/",
                    "youtube.com/embed/",
                    "youtube.com/v/",
                ],
            )
        })
        .or_else(|| find_watch_video_id(value))
}

fn find_after_any(value: &str, prefixes: &[&str]) -> Option<String> {
    prefixes
        .iter()
        .find_map(|prefix| find_video_id_after_prefix(value, prefix))
}

fn find_video_id_after_prefix(value: &str, prefix: &str) -> Option<String> {
    let start = value.find(prefix)? + prefix.len();
    take_video_id(&value[start..])
}

fn find_watch_video_id(value: &str) -> Option<String> {
    let mut rest = value;
    while let Some(pos) = rest.find("youtube.com/watch?") {
        let query = &rest[pos + "youtube.com/watch?".len()..];
        for part in query.split('&') {
            if let Some(candidate) = part.strip_prefix("v=") {
                return take_video_id(candidate);
            }
        }
        rest = &query[query.len().min(1)..];
    }
    None
}

fn take_video_id(value: &str) -> Option<String> {
    let candidate: String = value.chars().take(11).collect();
    if candidate.len() == 11 && is_valid_video_id(&candidate) {
        Some(candidate)
    } else {
        None
    }
}

fn is_valid_video_id(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn extract_innertube_api_key(html: &str) -> Option<String> {
    let marker = "\"INNERTUBE_API_KEY\"";
    let start = html.find(marker)? + marker.len();
    let after_marker = &html[start..];
    let colon = after_marker.find(':')?;
    let after_colon = after_marker[colon + 1..].trim_start();
    let after_quote = after_colon.strip_prefix('"')?;
    let end = after_quote.find('"')?;
    Some(after_quote[..end].to_owned())
}

fn consent_required(body: &str) -> bool {
    body.contains("https://consent.youtube.com/s")
}

fn consent_cookie_from_html(body: &str) -> Option<String> {
    let marker = "name=\"v\" value=\"";
    let start = body.find(marker)? + marker.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    Some(format!("CONSENT=YES+{}", &rest[..end]))
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InnertubePlayerResponse {
    #[serde(default)]
    captions: InnertubeCaptions,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InnertubeCaptions {
    #[serde(default)]
    player_captions_tracklist_renderer: CaptionTrackList,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaptionTrackList {
    #[serde(default)]
    caption_tracks: Vec<CaptionTrack>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaptionTrack {
    #[serde(default)]
    language_code: String,
    #[serde(default)]
    base_url: String,
    #[serde(default)]
    name: CaptionTrackName,
}

impl CaptionTrack {
    fn display_language(&self) -> String {
        if !self.name.simple_text.trim().is_empty() {
            return self.name.simple_text.trim().to_owned();
        }
        self.language_code.trim().to_owned()
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CaptionTrackName {
    #[serde(default)]
    simple_text: String,
}

fn select_caption_tracks(
    player: &InnertubePlayerResponse,
    languages: &[&str],
) -> Result<Vec<CaptionTrack>, YouTubeSummaryError> {
    let all = &player
        .captions
        .player_captions_tracklist_renderer
        .caption_tracks;
    if all.is_empty() {
        return Err(YouTubeSummaryError::Transcript(
            "playerCaptionsTracklistRenderer not found".to_owned(),
        ));
    }
    let mut selected = Vec::new();
    for language in languages {
        selected.extend(
            all.iter()
                .filter(|track| track.language_code == *language)
                .cloned(),
        );
    }
    if selected.is_empty() {
        return Err(YouTubeSummaryError::NoTranscriptForLanguages);
    }
    Ok(selected)
}

fn validated_caption_url(base_url: &str) -> Result<String, YouTubeSummaryError> {
    let normalized = base_url.replace("&fmt=srv3", "");
    let url = Url::parse(&normalized)
        .map_err(|_| YouTubeSummaryError::Transcript("invalid caption URL".to_owned()))?;
    let approved_host = matches!(
        url.host_str(),
        Some("www.youtube.com" | "youtube.com" | "m.youtube.com")
    );
    if url.scheme() != "https"
        || !approved_host
        || url.port_or_known_default() != Some(443)
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(YouTubeSummaryError::Transcript(
            "caption URL is outside approved YouTube origins".to_owned(),
        ));
    }
    Ok(url.to_string())
}

#[derive(Debug, Deserialize)]
struct XmlTranscript {
    #[serde(rename = "text", default)]
    texts: Vec<XmlTranscriptText>,
}

#[derive(Debug, Deserialize)]
struct XmlTranscriptText {
    #[serde(rename = "@start", default)]
    start: String,
    #[serde(rename = "@dur", default)]
    duration: String,
    #[serde(rename = "$text", default)]
    text: String,
}

#[derive(Clone, Debug, PartialEq)]
struct Transcript {
    language: String,
    language_code: String,
    lines: Vec<TranscriptLine>,
}

#[derive(Clone, Debug, PartialEq)]
struct TranscriptLine {
    text: String,
    start: f64,
    duration: f64,
}

fn parse_transcript_xml(xml: &str) -> Result<Vec<TranscriptLine>, YouTubeSummaryError> {
    let parsed: XmlTranscript = xml_from_str(xml).map_err(|error| {
        YouTubeSummaryError::Decode(format!("failed to parse transcript: {error}"))
    })?;
    Ok(parsed
        .texts
        .into_iter()
        .map(|line| TranscriptLine {
            text: strip_html_tags(&line.text),
            start: line.start.parse::<f64>().unwrap_or(0.0),
            duration: line.duration.parse::<f64>().unwrap_or(0.0),
        })
        .collect())
}

fn strip_html_tags(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_tag = false;
    for ch in text.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

fn format_text_transcripts(transcripts: &[Transcript]) -> String {
    let mut out = String::new();
    for (index, transcript) in transcripts.iter().enumerate() {
        let language = if transcript.language.trim().is_empty() {
            transcript.language_code.trim()
        } else {
            transcript.language.trim()
        };
        if !language.is_empty() {
            out.push_str("Language: ");
            out.push_str(language);
            out.push('\n');
        }
        for line in &transcript.lines {
            out.push_str(&format!("{:.6}: {}\n", line.start, line.text));
        }
        if transcripts.len() > 1 && index + 1 < transcripts.len() {
            out.push('\n');
        }
    }
    out
}

fn trim_youtube_transcript_like_go(transcript: &str) -> String {
    let trimmed = transcript.trim();
    if trimmed.len() <= 12_000 {
        return trimmed.to_owned();
    }
    let mut end = 12_000;
    while !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    trimmed[..end].to_owned()
}

fn youtube_summary_gemini_request(
    system: &str,
    payload: &str,
    stage: YouTubeStage,
) -> GeminiTextRequest {
    GeminiTextRequest {
        system_instruction: Some(GeminiTextContent {
            role: String::new(),
            parts: vec![GeminiTextPart {
                text: system.to_owned(),
            }],
        }),
        contents: vec![GeminiTextContent {
            role: "user".to_owned(),
            parts: vec![GeminiTextPart {
                text: payload.to_owned(),
            }],
        }],
        generation_config: GeminiTextGenerationConfig {
            max_output_tokens: stage.max_output_tokens(),
            temperature: YOUTUBE_SUMMARY_TEMPERATURE,
            response_mime_type: "application/json".to_owned(),
        },
    }
}

/// Qwen 3.6/3.8 publish temperature 0.7, top_p 0.8 and top_k 20 for
/// non-thinking use; other models keep the previous 0.3.
fn youtube_sampling(model: &str) -> (f64, Option<f64>, Option<i32>) {
    if model.to_ascii_lowercase().contains("qwen") {
        (0.7, Some(0.8), Some(20))
    } else {
        (YOUTUBE_SUMMARY_TEMPERATURE, None, None)
    }
}

fn youtube_summary_openai_request(
    model: &str,
    system: &str,
    payload: &str,
    stage: YouTubeStage,
) -> OpenAiChatCompletionRequest {
    let (temperature, top_p, top_k) = youtube_sampling(model);
    OpenAiChatCompletionRequest {
        model: model.to_owned(),
        messages: vec![
            OpenAiChatMessage {
                role: "system".to_owned(),
                content: system.to_owned(),
            },
            OpenAiChatMessage {
                role: "user".to_owned(),
                content: payload.to_owned(),
            },
        ],
        max_tokens: stage.max_output_tokens(),
        temperature,
        top_p,
        top_k,
        response_format: json!({"type": "json_object"}),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiTextRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiTextContent>,
    contents: Vec<GeminiTextContent>,
    generation_config: GeminiTextGenerationConfig,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct GeminiTextContent {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    role: String,
    parts: Vec<GeminiTextPart>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct GeminiTextPart {
    text: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
struct GeminiTextGenerationConfig {
    max_output_tokens: i32,
    temperature: f64,
    response_mime_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct OpenAiChatCompletionRequest {
    model: String,
    messages: Vec<OpenAiChatMessage>,
    max_tokens: i32,
    temperature: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    top_k: Option<i32>,
    response_format: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct OpenAiChatMessage {
    role: String,
    content: String,
}

fn gemini_generate_url(base_url: &str, model: &str) -> Result<String, YouTubeSummaryError> {
    let model = gemini_api_model_name(model);
    let mut url = Url::parse(base_url.trim())
        .map_err(|error| YouTubeSummaryError::Decode(format!("gemini base url: {error}")))?;
    {
        let mut path = url.path_segments_mut().map_err(|()| {
            YouTubeSummaryError::Decode("gemini base url cannot be a base".to_owned())
        })?;
        path.pop_if_empty();
        path.push("models");
        path.push(&format!("{model}:generateContent"));
    }
    Ok(url.to_string())
}

fn gemini_api_model_name(model: &str) -> String {
    let trimmed = cache_contour_model(model);
    strip_prefix_fold(&trimmed, "googleai/")
        .or_else(|| strip_prefix_fold(&trimmed, "vertexai/"))
        .unwrap_or(trimmed.as_str())
        .to_owned()
}

fn strip_prefix_fold<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .filter(|head| head.eq_ignore_ascii_case(prefix))?;
    value.get(prefix.len()..)
}

fn decode_gemini_text(body: &[u8]) -> Result<String, YouTubeSummaryError> {
    let decoded: GeminiGenerateContentResponse = serde_json::from_slice(body).map_err(|error| {
        YouTubeSummaryError::Decode(format!("decode Gemini response JSON: {error}"))
    })?;
    if let Some(reason) = decoded
        .prompt_feedback
        .block_reason
        .filter(|value| !value.trim().is_empty())
    {
        return Err(YouTubeSummaryError::Http(reason));
    }
    let Some(candidate) = decoded.candidates.first() else {
        return Err(YouTubeSummaryError::Http("empty model response".to_owned()));
    };
    if !candidate.finish_reason.trim().is_empty()
        && candidate.finish_reason != "STOP"
        && candidate.finish_reason != "MAX_TOKENS"
    {
        return Err(YouTubeSummaryError::Http(candidate.finish_reason.clone()));
    }
    let text = candidate
        .content
        .parts
        .iter()
        .map(|part| part.text.as_str())
        .collect::<String>();
    if text.trim().is_empty() {
        return Err(YouTubeSummaryError::Http("empty model response".to_owned()));
    }
    Ok(text)
}

fn decode_openai_text(body: &[u8]) -> Result<String, YouTubeSummaryError> {
    let decoded: OpenAiChatCompletionResponse = serde_json::from_slice(body).map_err(|error| {
        YouTubeSummaryError::Decode(format!("decode OpenAI-compatible response JSON: {error}"))
    })?;
    if let Some(error) = decoded.error {
        return Err(YouTubeSummaryError::Http(error.message));
    }
    let Some(choice) = decoded.choices.first() else {
        return Err(YouTubeSummaryError::Http("empty model response".to_owned()));
    };
    if !choice.finish_reason.trim().is_empty()
        && choice.finish_reason != "stop"
        && choice.finish_reason != "length"
    {
        return Err(YouTubeSummaryError::Http(choice.finish_reason.clone()));
    }
    let text = choice.message.content.clone();
    if text.trim().is_empty() {
        return Err(YouTubeSummaryError::Http("empty model response".to_owned()));
    }
    Ok(text)
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiGenerateContentResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(default)]
    prompt_feedback: GeminiPromptFeedback,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiPromptFeedback {
    block_reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCandidate {
    #[serde(default)]
    content: GeminiResponseContent,
    #[serde(default)]
    finish_reason: String,
}

#[derive(Debug, Default, Deserialize)]
struct GeminiResponseContent {
    #[serde(default)]
    parts: Vec<GeminiResponsePart>,
}

#[derive(Debug, Default, Deserialize)]
struct GeminiResponsePart {
    #[serde(default)]
    text: String,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAiChatCompletionResponse {
    #[serde(default)]
    choices: Vec<OpenAiChoice>,
    error: Option<OpenAiError>,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAiChoice {
    #[serde(default)]
    message: OpenAiChoiceMessage,
    #[serde(default)]
    finish_reason: String,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAiChoiceMessage {
    #[serde(default)]
    content: String,
}

#[derive(Debug, Deserialize)]
struct OpenAiError {
    message: String,
}

fn http_error_text(error: reqwest::Error) -> YouTubeSummaryError {
    YouTubeSummaryError::Http(error.without_url().to_string())
}

/// Longest segment built from merged caption fragments.
const SEGMENT_MAX_CHARS: usize = 240;
/// Transcripts longer than this are summarized in parts and merged.
const SINGLE_CALL_MAX_CHARS: usize = 40_000;
/// Characters per part of a long transcript.
const PART_MAX_CHARS: usize = 30_000;
/// Most sections a summary shows.
const MAX_SUMMARY_SECTIONS: usize = 12;
const SUMMARY_TASK_LINE: &str =
    "По транскрипту выше верни JSON с саммари, как описано в инструкции.";
const MERGE_TASK_LINE: &str =
    "По частям выше верни один JSON с саммари всего видео, как описано в инструкции.";

/// The model calls of a summary: one per transcript (or per part of a long
/// one), and a merge call over the parts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum YouTubeStage {
    Summary,
    Merge,
}

impl YouTubeStage {
    const fn prompt_name(self) -> &'static str {
        match self {
            Self::Summary => "youtube/summary_system",
            Self::Merge => "youtube/merge",
        }
    }

    const fn max_output_tokens(self) -> i32 {
        match self {
            Self::Summary => 3072,
            Self::Merge => 8192,
        }
    }
}

type YouTubeModelFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, YouTubeSummaryError>> + Send + 'a>>;

/// One summary model call on a summarizer's transport; returns the JSON text.
trait YouTubeSummaryModel: Send + Sync {
    fn complete<'a>(&'a self, stage: YouTubeStage, payload: &'a str) -> YouTubeModelFuture<'a>;
}

/// A merged caption sentence with its id and start time.
#[derive(Clone, Debug, PartialEq)]
struct TranscriptSegment {
    id: String,
    start: f64,
    text: String,
}

/// A fetched transcript: the text handed to the dialog (trimmed as before) and
/// the numbered segments the summary is built from.
#[derive(Clone, Debug, Default, PartialEq)]
struct FetchedTranscript {
    text: String,
    segments: Vec<TranscriptSegment>,
}

impl FetchedTranscript {
    fn new(transcripts: &[Transcript]) -> Self {
        Self {
            text: trim_youtube_transcript_like_go(&format_text_transcripts(transcripts))
                .trim()
                .to_owned(),
            segments: transcript_segments(transcripts),
        }
    }
}

/// Merge the preferred track's caption fragments into sentences: a segment ends
/// at sentence punctuation or at `SEGMENT_MAX_CHARS`, keeps the start of its
/// first fragment, and gets the id `s1`, `s2`, …
fn transcript_segments(transcripts: &[Transcript]) -> Vec<TranscriptSegment> {
    let Some(track) = transcripts
        .iter()
        .find(|track| track.lines.iter().any(|line| !line.text.trim().is_empty()))
    else {
        return Vec::new();
    };
    let mut segments = Vec::new();
    let mut text = String::new();
    let mut start = 0.0;
    for line in &track.lines {
        let piece = line.text.split_whitespace().collect::<Vec<_>>().join(" ");
        if piece.is_empty() {
            continue;
        }
        if text.is_empty() {
            start = line.start;
        } else {
            text.push(' ');
        }
        text.push_str(&piece);
        if text.ends_with(['.', '!', '?', '…']) || text.chars().count() >= SEGMENT_MAX_CHARS {
            segments.push(TranscriptSegment {
                id: format!("s{}", segments.len() + 1),
                start,
                text: std::mem::take(&mut text),
            });
        }
    }
    if !text.is_empty() {
        segments.push(TranscriptSegment {
            id: format!("s{}", segments.len() + 1),
            start,
            text,
        });
    }
    segments
}

fn timecode(seconds: f64) -> String {
    let total = if seconds.is_finite() && seconds > 0.0 {
        seconds as u64
    } else {
        0
    };
    format!(
        "{:02}:{:02}:{:02}",
        total / 3600,
        total / 60 % 60,
        total % 60
    )
}

fn escape_telegram_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn segment_line(segment: &TranscriptSegment) -> String {
    format!(
        "[{} {}] {}",
        segment.id,
        timecode(segment.start),
        segment.text.replace('<', "&lt;")
    )
}

/// User message of a summary call: the numbered transcript (or one part of it),
/// then the task.
fn summary_payload(segments: &[TranscriptSegment], part: Option<(usize, usize)>) -> String {
    let mut out = String::from("<transcript");
    if let Some((index, total)) = part {
        out.push_str(&format!(" part=\"{index}\" of=\"{total}\""));
    }
    out.push_str(">\n");
    for segment in segments {
        out.push_str(&segment_line(segment));
        out.push('\n');
    }
    out.push_str("</transcript>\n");
    out.push_str(SUMMARY_TASK_LINE);
    out
}

/// The whole transcript when it is short, otherwise parts of at most
/// `PART_MAX_CHARS` characters on segment boundaries.
fn transcript_parts(segments: &[TranscriptSegment]) -> Vec<&[TranscriptSegment]> {
    let sizes: Vec<usize> = segments
        .iter()
        .map(|segment| segment_line(segment).chars().count() + 1)
        .collect();
    if sizes.iter().sum::<usize>() <= SINGLE_CALL_MAX_CHARS {
        return vec![segments];
    }
    let mut parts = Vec::new();
    let mut start = 0;
    let mut used = 0;
    for (index, size) in sizes.iter().enumerate() {
        if used + size > PART_MAX_CHARS && index > start {
            parts.push(&segments[start..index]);
            start = index;
            used = 0;
        }
        used += size;
    }
    parts.push(&segments[start..]);
    parts
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct SummaryJson {
    #[serde(default)]
    overview: String,
    #[serde(default)]
    sections: Vec<SummarySection>,
    #[serde(default)]
    conclusion: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct SummarySection {
    #[serde(default)]
    segment_id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    points: Vec<String>,
}

fn decode_summary_json(text: &str) -> Result<SummaryJson, YouTubeSummaryError> {
    let trimmed = text.trim();
    let object = match (trimmed.find('{'), trimmed.rfind('}')) {
        (Some(start), Some(end)) if end > start => &trimmed[start..=end],
        _ => {
            return Err(YouTubeSummaryError::Decode(
                "summary response holds no JSON object".to_owned(),
            ));
        }
    };
    serde_json::from_str(object)
        .map_err(|error| YouTubeSummaryError::Decode(format!("summary JSON: {error}")))
}

/// User message of the merge call: every part's overview, sections with their
/// segment ids, and conclusion, then the task.
fn merge_payload(parts: &[SummaryJson], segments: &[TranscriptSegment]) -> String {
    let starts: HashMap<&str, f64> = segments
        .iter()
        .map(|segment| (segment.id.as_str(), segment.start))
        .collect();
    let mut out = String::from("<parts>\n");
    for (index, part) in parts.iter().enumerate() {
        out.push_str(&format!("<part index=\"{}\">\n", index + 1));
        out.push_str(&format!("overview: {}\n", part.overview.trim()));
        for section in &part.sections {
            let id = section.segment_id.trim();
            let Some(start) = starts.get(id) else {
                continue;
            };
            out.push_str(&format!(
                "[{id} {}] {}: {}\n",
                timecode(*start),
                section.title.trim().replace('<', "&lt;"),
                section.points.join("; ").replace('<', "&lt;")
            ));
        }
        out.push_str(&format!(
            "conclusion: {}\n</part>\n",
            part.conclusion.trim()
        ));
    }
    out.push_str("</parts>\n");
    out.push_str(MERGE_TASK_LINE);
    out
}

/// Telegram HTML for a summary. Timecodes come from the segment ids, a section
/// naming an unknown id is dropped, every text is escaped, and only `<b>` and
/// `<i>` tags are written.
fn render_summary_html(summary: &SummaryJson, segments: &[TranscriptSegment]) -> String {
    let starts: HashMap<&str, f64> = segments
        .iter()
        .map(|segment| (segment.id.as_str(), segment.start))
        .collect();
    let mut sections: Vec<(f64, &SummarySection)> = summary
        .sections
        .iter()
        .filter_map(|section| {
            let start = starts.get(section.segment_id.trim()).copied();
            if start.is_none() {
                tracing::warn!(
                    segment_id = %section.segment_id,
                    "youtube summary section names an unknown segment; dropped"
                );
            }
            start.map(|start| (start, section))
        })
        .collect();
    sections.sort_by(|left, right| left.0.total_cmp(&right.0));
    sections.truncate(MAX_SUMMARY_SECTIONS);
    let mut blocks = Vec::new();
    let overview = summary.overview.trim();
    if !overview.is_empty() {
        blocks.push(escape_telegram_html(overview));
    }
    for (start, section) in sections {
        let mut block = format!(
            "[{}] <b>{}</b>",
            timecode(start),
            escape_telegram_html(section.title.trim())
        );
        for point in section.points.iter().map(|point| point.trim()) {
            if !point.is_empty() {
                block.push_str("\n• ");
                block.push_str(&escape_telegram_html(point));
            }
        }
        blocks.push(block);
    }
    let conclusion = summary.conclusion.trim();
    if !conclusion.is_empty() {
        blocks.push(format!("<i>{}</i>", escape_telegram_html(conclusion)));
    }
    blocks.join("\n\n")
}

/// Summarize the segments: one call for a short transcript; for a long one a
/// call per part and a merge call that keeps at most `MAX_SUMMARY_SECTIONS`.
async fn summarize_segments(
    model: &dyn YouTubeSummaryModel,
    segments: &[TranscriptSegment],
) -> Result<String, YouTubeSummaryError> {
    let parts = transcript_parts(segments);
    if parts.len() == 1 {
        let text = model
            .complete(YouTubeStage::Summary, &summary_payload(segments, None))
            .await?;
        return Ok(render_summary_html(&decode_summary_json(&text)?, segments));
    }
    let total = parts.len();
    let mut summaries = Vec::with_capacity(total);
    for (index, part) in parts.iter().enumerate() {
        let text = model
            .complete(
                YouTubeStage::Summary,
                &summary_payload(part, Some((index + 1, total))),
            )
            .await?;
        summaries.push(decode_summary_json(&text)?);
    }
    let text = model
        .complete(YouTubeStage::Merge, &merge_payload(&summaries, segments))
        .await?;
    Ok(render_summary_html(&decode_summary_json(&text)?, segments))
}

async fn summary_result(
    model: &dyn YouTubeSummaryModel,
    fetched: FetchedTranscript,
) -> Result<YouTubeSummaryResult, YouTubeSummaryError> {
    let summary = if fetched.segments.is_empty() {
        String::new()
    } else {
        summarize_segments(model, &fetched.segments).await?
    };
    Ok(YouTubeSummaryResult {
        summary,
        transcript: fetched.text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn youtube_video_id_parser_matches_go_inputs() {
        for input in [
            "dQw4w9WgXcQ",
            " https://youtu.be/dQw4w9WgXcQ ",
            "https://www.youtube.com/watch?foo=1&v=dQw4w9WgXcQ&t=4",
            "https://youtube.com/shorts/dQw4w9WgXcQ",
            "https://youtube.com/live/dQw4w9WgXcQ?si=x",
            "https://youtube.com/embed/dQw4w9WgXcQ",
            "https://youtube.com/v/dQw4w9WgXcQ",
            "summarize youtube.com/watch?v=dQw4w9WgXcQ please",
            "please review https://youtu.be/dQw4w9WgXcQ?t=42",
        ] {
            assert_eq!(
                parse_youtube_video_id(input).as_deref(),
                Some("dQw4w9WgXcQ"),
                "{input}"
            );
        }
        assert_eq!(parse_youtube_video_id("bad id"), None);
        assert_eq!(
            parse_youtube_video_id("https://example.test/dQw4w9WgXcQ"),
            None
        );
    }

    #[test]
    fn transcript_xml_formats_like_go_text_formatter() {
        let lines = parse_transcript_xml(
            r#"<transcript><text start="0" dur="1.5">Hello &amp; hi</text><text start="2.25" dur="3">world</text></transcript>"#,
        )
        .expect("xml");
        let transcript = Transcript {
            language: "Russian".to_owned(),
            language_code: "ru".to_owned(),
            lines,
        };

        let out = format_text_transcripts(&[transcript]);

        assert_eq!(
            out,
            "Language: Russian\n0.000000: Hello & hi\n2.250000: world\n"
        );
    }

    #[test]
    fn caption_urls_are_restricted_to_youtube_https_origins() {
        assert_eq!(
            validated_caption_url("https://www.youtube.com/api/timedtext?v=abc&fmt=srv3")
                .expect("approved caption URL"),
            "https://www.youtube.com/api/timedtext?v=abc"
        );
        for url in [
            "http://www.youtube.com/api/timedtext?v=abc",
            "https://127.0.0.1/private",
            "https://www.youtube.com.evil.test/caption",
            "https://user@www.youtube.com/api/timedtext",
        ] {
            assert!(validated_caption_url(url).is_err(), "{url}");
        }
    }

    #[test]
    fn youtube_transcript_trim_caps_gemini_input_like_go() {
        let transcript = format!("  {}\n{}tail  ", "a".repeat(12_000), "b".repeat(128));

        let out = trim_youtube_transcript_like_go(&transcript);

        assert_eq!(out.len(), 12_000);
        assert!(out.starts_with('a'));
        assert!(!out.contains("tail"));
    }

    #[test]
    fn youtube_summary_gemini_request_asks_for_json_within_the_stage_budget() {
        let request = youtube_summary_gemini_request("sys", "payload", YouTubeStage::Summary);
        let value = serde_json::to_value(&request).expect("json");

        assert_eq!(value["systemInstruction"]["parts"][0]["text"], "sys");
        assert_eq!(value["contents"][0]["role"], "user");
        assert_eq!(value["contents"][0]["parts"][0]["text"], "payload");
        assert_eq!(value["generationConfig"]["maxOutputTokens"], 3072);
        assert_eq!(value["generationConfig"]["temperature"], 0.3);
        assert_eq!(
            value["generationConfig"]["responseMimeType"],
            "application/json"
        );
        let merge = youtube_summary_gemini_request("sys", "payload", YouTubeStage::Merge);
        let merge = serde_json::to_value(&merge).expect("json");
        assert_eq!(merge["generationConfig"]["maxOutputTokens"], 8192);
    }

    #[test]
    fn youtube_summary_openai_request_uses_family_sampling_and_json_mode() {
        let request =
            youtube_summary_openai_request("gpt-5-mini", "sys", "payload", YouTubeStage::Summary);
        let value = serde_json::to_value(&request).expect("json");

        assert_eq!(value["model"], "gpt-5-mini");
        assert_eq!(value["messages"][0]["content"], "sys");
        assert_eq!(value["messages"][1]["content"], "payload");
        assert_eq!(value["max_tokens"], 3072);
        assert_eq!(value["temperature"], 0.3);
        assert!(value.get("top_p").is_none());
        assert_eq!(value["response_format"]["type"], "json_object");

        let qwen =
            youtube_summary_openai_request("qwen3.6-27b", "sys", "payload", YouTubeStage::Merge);
        let qwen = serde_json::to_value(&qwen).expect("json");
        assert_eq!(qwen["max_tokens"], 8192);
        assert_eq!(qwen["temperature"], 0.7);
        assert_eq!(qwen["top_p"], 0.8);
        assert_eq!(qwen["top_k"], 20);
    }

    fn caption(start: f64, text: &str) -> TranscriptLine {
        TranscriptLine {
            text: text.to_owned(),
            start,
            duration: 2.0,
        }
    }

    fn sample_segments() -> Vec<TranscriptSegment> {
        transcript_segments(&[Transcript {
            language: "Russian".to_owned(),
            language_code: "ru".to_owned(),
            lines: vec![
                caption(5.0, "всем привет, сегодня"),
                caption(7.5, "разбираем сборку ПК."),
                caption(754.2, "теперь про блок питания"),
                caption(757.0, "и его мощность!"),
                caption(3725.0, "итоги"),
            ],
        }])
    }

    #[test]
    fn caption_fragments_merge_into_numbered_sentences() {
        let segments = sample_segments();
        let lines: Vec<String> = segments.iter().map(segment_line).collect();
        assert_eq!(
            lines,
            vec![
                "[s1 00:00:05] всем привет, сегодня разбираем сборку ПК.",
                "[s2 00:12:34] теперь про блок питания и его мощность!",
                "[s3 01:02:05] итоги",
            ]
        );
        let payload = summary_payload(&segments, None);
        assert!(payload.starts_with("<transcript>\n[s1 00:00:05]"));
        assert!(payload.ends_with(SUMMARY_TASK_LINE));
    }

    #[test]
    fn summary_timestamps_come_from_segment_ids() {
        let summary = decode_summary_json(
            r#"```json
{"overview":"Сборка ПК.","sections":[{"segment_id":"s2","title":"Блок питания","points":["Выбор мощности"]},{"segment_id":"s1","title":"Вступление","points":[]}],"conclusion":"Готово."}
```"#,
        )
        .expect("decode");
        let html = render_summary_html(&summary, &sample_segments());
        assert_eq!(
            html,
            "Сборка ПК.\n\n[00:00:05] <b>Вступление</b>\n\n[00:12:34] <b>Блок питания</b>\n• Выбор мощности\n\n<i>Готово.</i>"
        );
    }

    #[test]
    fn unknown_segment_id_is_dropped() {
        let summary = SummaryJson {
            sections: vec![
                SummarySection {
                    segment_id: "s99".to_owned(),
                    title: "Выдумка".to_owned(),
                    points: vec!["нет такого места".to_owned()],
                },
                SummarySection {
                    segment_id: "s3".to_owned(),
                    title: "Итоги".to_owned(),
                    points: Vec::new(),
                },
            ],
            ..SummaryJson::default()
        };
        let html = render_summary_html(&summary, &sample_segments());
        assert_eq!(html, "[01:02:05] <b>Итоги</b>");
    }

    #[test]
    fn rendered_html_uses_only_allowed_tags() {
        let summary = SummaryJson {
            overview: "Сравнение <script> & «кавычки»".to_owned(),
            sections: vec![SummarySection {
                segment_id: "s1".to_owned(),
                title: "Цена < 100 & > 50".to_owned(),
                points: vec!["<b>жирно</b>".to_owned()],
            }],
            conclusion: "a & b".to_owned(),
        };
        let html = render_summary_html(&summary, &sample_segments());
        assert!(
            html.contains("Сравнение &lt;script&gt; &amp; «кавычки»"),
            "{html}"
        );
        assert!(
            html.contains("<b>Цена &lt; 100 &amp; &gt; 50</b>"),
            "{html}"
        );
        assert!(html.contains("• &lt;b&gt;жирно&lt;/b&gt;"), "{html}");
        let tags: Vec<&str> = html
            .match_indices('<')
            .map(|(index, _)| {
                &html[index
                    ..html[index..]
                        .find('>')
                        .map_or(html.len(), |end| index + end + 1)]
            })
            .collect();
        assert!(
            tags.iter()
                .all(|tag| matches!(*tag, "<b>" | "</b>" | "<i>" | "</i>")),
            "{tags:?}"
        );
    }

    struct ScriptedModel {
        calls: std::sync::Mutex<Vec<(YouTubeStage, String)>>,
    }

    impl YouTubeSummaryModel for ScriptedModel {
        fn complete<'a>(&'a self, stage: YouTubeStage, payload: &'a str) -> YouTubeModelFuture<'a> {
            Box::pin(async move {
                let mut calls = self.calls.lock().expect("calls");
                calls.push((stage, payload.to_owned()));
                let first_id = payload
                    .split('[')
                    .nth(1)
                    .and_then(|rest| rest.split(' ').next())
                    .unwrap_or("s1")
                    .to_owned();
                Ok(json!({
                    "overview": format!("часть {}", calls.len()),
                    "sections": [{"segment_id": first_id, "title": "Раздел", "points": ["пункт"]}],
                    "conclusion": "итог"
                })
                .to_string())
            })
        }
    }

    #[tokio::test]
    async fn long_transcript_is_chunked_and_merged() {
        let lines: Vec<TranscriptLine> = (0..3000)
            .map(|index| {
                caption(
                    f64::from(index) * 3.6,
                    "длинная фраза о сборке компьютера и выборе деталей для него.",
                )
            })
            .collect();
        let segments = transcript_segments(&[Transcript {
            language: String::new(),
            language_code: "ru".to_owned(),
            lines,
        }]);
        assert!(transcript_parts(&segments).len() > 1);
        let model = ScriptedModel {
            calls: std::sync::Mutex::new(Vec::new()),
        };

        let html = summarize_segments(&model, &segments)
            .await
            .expect("summary");

        let calls = model.calls.lock().expect("calls");
        let parts = calls
            .iter()
            .filter(|(stage, _)| *stage == YouTubeStage::Summary)
            .count();
        assert_eq!(parts, transcript_parts(&segments).len());
        assert_eq!(
            calls.last().map(|(stage, _)| *stage),
            Some(YouTubeStage::Merge)
        );
        assert!(
            calls
                .last()
                .expect("merge")
                .1
                .contains("<part index=\"1\">")
        );
        assert!(html.starts_with("часть"), "{html}");
        assert!(html.contains("<b>Раздел</b>"), "{html}");
    }

    #[test]
    fn gemini_url_uses_pinned_flash_lite_model() {
        assert_eq!(
            gemini_generate_url(GEMINI_API_BASE_URL, MODEL_GEMINI_FLASH_LITE).expect("url"),
            format!(
                "{GEMINI_API_BASE_URL}/models/{}:generateContent",
                openplotva_llm::gemini::MODEL_GEMINI_FLASH_LITE_PINNED
            )
        );
    }

    #[test]
    fn gemini_youtube_config_uses_genkit_default_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("google-key".to_owned()),
            genkit_default_model: Some(" googleai/gemini-2.5-flash ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let summarizer = GeminiYouTubeSummarizer::from_app_config(&config).expect("summarizer");

        assert_eq!(summarizer.cfg.model, "googleai/gemini-2.5-flash");
    }

    #[test]
    fn provider_youtube_config_routes_openrouter_default_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("google-key".to_owned()),
            genkit_default_model: Some(" openrouter/openai/gpt-4.1-mini ".to_owned()),
            openrouter_key: Some(" openrouter-key ".to_owned()),
            openrouter_request_timeout_seconds: Some("333".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let summarizer = RuntimeYouTubeSummarizer::from_app_config(&config)
            .expect("build")
            .expect("summary");

        let RuntimeYouTubeSummarizer::OpenAiCompatible(summarizer) = summarizer else {
            panic!("expected OpenAI-compatible route");
        };
        assert_eq!(summarizer.provider, "openrouter");
        assert_eq!(summarizer.cfg.model, "openai/gpt-4.1-mini");
        assert_eq!(summarizer.cfg.api_key, "openrouter-key");
        assert_eq!(summarizer.cfg.direct_url, OPENROUTER_CHAT_COMPLETIONS_URL);
        assert_eq!(summarizer.cfg.request_timeout, Duration::from_secs(333));
    }

    #[test]
    fn youtube_trace_artifact_reads_openai_usage_and_http_errors() {
        let trace = YouTubeCallTrace::begin(
            YouTubeTraceTags {
                provider: "openrouter",
                source: "youtube_routed",
                request_kind: "openai.chat.completions",
            },
            "summary-model",
            &youtube_summary_openai_request(
                "summary-model",
                "system",
                "0.5: hello",
                YouTubeStage::Summary,
            ),
        );
        let artifact = youtube_trace_artifact(
            trace.artifact,
            Some(br#"{"usage":{"prompt_tokens":120,"completion_tokens":30,"total_tokens":150,"prompt_tokens_details":{"cached_tokens":64}}}"#),
            Some("HTTP 429".to_owned()),
        );

        assert_eq!(artifact.flow, "youtube_summary");
        assert_eq!(artifact.source, "youtube_routed");
        assert_eq!(artifact.model, "summary-model");
        assert_eq!(artifact.error, "HTTP 429");
        assert!(artifact.prompt_chars > 0);
        assert_eq!(
            artifact.inference_params,
            Some(json!({"max_tokens": 3072, "temperature": 0.3}))
        );
        let usage = artifact.usage.unwrap_or_default();
        assert_eq!(usage.input_tokens, 120);
        assert_eq!(usage.output_tokens, 30);
        assert_eq!(usage.cached_tokens, 64);
    }

    #[test]
    fn youtube_trace_artifact_reads_gemini_usage_metadata() {
        let trace = YouTubeCallTrace::begin(
            YouTubeTraceTags {
                provider: "genkit",
                source: "youtube_gemini",
                request_kind: "gemini.generateContent",
            },
            "gemini-model",
            &youtube_summary_gemini_request("system", "0.5: hello", YouTubeStage::Summary),
        );
        let artifact = youtube_trace_artifact(
            trace.artifact,
            Some(br#"{"usageMetadata":{"promptTokenCount":90,"candidatesTokenCount":10,"totalTokenCount":100,"thoughtsTokenCount":5}}"#),
            None,
        );

        assert!(artifact.error.is_empty());
        assert_eq!(artifact.request_kind, "gemini.generateContent");
        let usage = artifact.usage.unwrap_or_default();
        assert_eq!(usage.input_tokens, 90);
        assert_eq!(usage.output_tokens, 10);
        assert_eq!(usage.thoughts_tokens, 5);
    }

    #[test]
    fn decodes_openai_compatible_text_response() {
        let text = decode_openai_text(
            br#"{"choices":[{"message":{"content":"<b>summary</b>"},"finish_reason":"stop"}]}"#,
        )
        .expect("text");

        assert_eq!(text, "<b>summary</b>");
    }

    #[test]
    fn decodes_gemini_text_response() {
        let text = decode_gemini_text(
            br#"{"candidates":[{"content":{"parts":[{"text":"<b>summary</b>"}]},"finishReason":"STOP"}]}"#,
        )
        .expect("text");

        assert_eq!(text, "<b>summary</b>");
    }
}
