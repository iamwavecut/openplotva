//! App-level YouTube summary runtime for the dialog toolbox.

use std::{error::Error, time::Duration};

use openplotva_config::AppConfig;
use openplotva_llm::gemini::{MODEL_GEMINI_FLASH_LITE, cache_contour_model};
use quick_xml::de::from_str as xml_from_str;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use url::Url;

use crate::{
    dialog_tools::{YouTubeSummarizer, YouTubeSummaryFuture, YouTubeSummaryResult},
    runtime_gemini_cache::resolve_google_ai_key,
};

const YOUTUBE_VIDEO_URL: &str = "https://www.youtube.com/watch?v=";
const INNERTUBE_PLAYER_URL: &str = "https://www.youtube.com/youtubei/v1/player";
const INNERTUBE_CLIENT_NAME: &str = "ANDROID";
const INNERTUBE_CLIENT_VERSION: &str = "20.10.38";
const GEMINI_API_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const TOGETHER_MODEL_PREFIX: &str = "together/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
const TOGETHER_CHAT_COMPLETIONS_URL: &str = "https://api.together.xyz/v1/chat/completions";
const YOUTUBE_SUMMARY_MAX_OUTPUT_TOKENS: i32 = 8192;
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

    /// Human-readable provider label for readiness diagnostics.
    #[must_use]
    pub fn provider_label(&self) -> &'static str {
        match self {
            Self::Gemini(_) => "direct Gemini",
            Self::OpenAiCompatible(summarizer) => summarizer.provider,
        }
    }
}

impl YouTubeSummarizer for RuntimeYouTubeSummarizer {
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
        match self {
            Self::Gemini(summarizer) => summarizer.summarize(video),
            Self::OpenAiCompatible(summarizer) => summarizer.summarize(video),
        }
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
            .timeout(cfg.request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self { cfg, http }
    }

    async fn run(&self, video: &str) -> Result<YouTubeSummaryResult, YouTubeSummaryError> {
        let transcript = self.transcript_for_video(video).await?;
        if transcript.is_empty() {
            return Ok(YouTubeSummaryResult {
                summary: String::new(),
                transcript,
            });
        }
        let summary = self.generate_summary(&transcript).await?;
        Ok(YouTubeSummaryResult {
            summary: summary.trim().to_owned(),
            transcript,
        })
    }

    async fn transcript_for_video(&self, video: &str) -> Result<String, YouTubeSummaryError> {
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
        Ok(transcript.trim().to_owned())
    }

    async fn fetch_youtube_transcript(
        &self,
        video_id: &str,
    ) -> Result<String, YouTubeSummaryError> {
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
            let xml = self
                .get_text(&transcript_url(&track.base_url), consent_cookie.as_deref())
                .await?;
            let lines = parse_transcript_xml(&xml)?;
            transcripts.push(Transcript {
                language: track.display_language(),
                language_code: track.language_code,
                lines,
            });
        }
        Ok(trim_youtube_transcript_like_go(&format_text_transcripts(
            &transcripts,
        )))
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

    async fn generate_summary(&self, transcript: &str) -> Result<String, YouTubeSummaryError> {
        let system = openplotva_prompts::read("youtube/summary_system")?;
        let request = youtube_summary_gemini_request(&system, transcript);
        let model = cache_contour_model(&self.cfg.model);
        let url = gemini_generate_url(&self.cfg.base_url, &model)?;
        let response = self
            .http
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-goog-api-key", self.cfg.api_key.trim())
            .json(&request)
            .send()
            .await
            .map_err(http_error_text)?;
        let status = response.status();
        let body = response.bytes().await.map_err(http_error_text)?;
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
            .timeout(cfg.request_timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let provider = if cfg.direct_url.contains("openrouter.ai") {
            "openrouter"
        } else if cfg.direct_url.contains("together.xyz") {
            "together"
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
        let transcript = self.transcript.transcript_for_video(video).await?;
        if transcript.is_empty() {
            return Ok(YouTubeSummaryResult {
                summary: String::new(),
                transcript,
            });
        }
        let summary = self.generate_summary(&transcript).await?;
        Ok(YouTubeSummaryResult {
            summary: summary.trim().to_owned(),
            transcript,
        })
    }

    async fn generate_summary(&self, transcript: &str) -> Result<String, YouTubeSummaryError> {
        let system = openplotva_prompts::read("youtube/summary_system")?;
        let request = youtube_summary_openai_request(&self.cfg.model, &system, transcript);
        let response = self
            .http
            .post(self.cfg.direct_url.trim())
            .bearer_auth(self.cfg.api_key.trim())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&request)
            .send()
            .await
            .map_err(http_error_text)?;
        let status = response.status();
        let body = response.bytes().await.map_err(http_error_text)?;
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

impl YouTubeSummarizer for OpenAiCompatibleYouTubeSummarizer {
    fn summarize<'a>(&'a self, video: &'a str) -> YouTubeSummaryFuture<'a> {
        Box::pin(async move {
            self.run(video)
                .await
                .map_err(|error| Box::new(error) as BoxedError)
        })
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
        } else if let Some(model) = strip_prefix_fold(model, TOGETHER_MODEL_PREFIX) {
            (
                TOGETHER_CHAT_COMPLETIONS_URL,
                together_api_key(config),
                model.trim().to_owned(),
                config.llm.dialog.request_timeout_seconds,
                "together",
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

fn together_api_key(config: &AppConfig) -> String {
    let key = config.together.key.trim();
    if !key.is_empty() {
        return key.to_owned();
    }
    config
        .together
        .keys
        .iter()
        .map(|key| key.trim())
        .find(|key| !key.is_empty())
        .unwrap_or_default()
        .to_owned()
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

fn transcript_url(base_url: &str) -> String {
    base_url.replace("&fmt=srv3", "")
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

fn youtube_summary_gemini_request(system: &str, transcript: &str) -> GeminiTextRequest {
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
                text: transcript.to_owned(),
            }],
        }],
        generation_config: GeminiTextGenerationConfig {
            max_output_tokens: YOUTUBE_SUMMARY_MAX_OUTPUT_TOKENS,
            temperature: YOUTUBE_SUMMARY_TEMPERATURE,
        },
    }
}

fn youtube_summary_openai_request(
    model: &str,
    system: &str,
    transcript: &str,
) -> OpenAiChatCompletionRequest {
    OpenAiChatCompletionRequest {
        model: model.to_owned(),
        messages: vec![
            OpenAiChatMessage {
                role: "system".to_owned(),
                content: system.to_owned(),
            },
            OpenAiChatMessage {
                role: "user".to_owned(),
                content: transcript.to_owned(),
            },
        ],
        max_tokens: YOUTUBE_SUMMARY_MAX_OUTPUT_TOKENS,
        temperature: YOUTUBE_SUMMARY_TEMPERATURE,
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
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct OpenAiChatCompletionRequest {
    model: String,
    messages: Vec<OpenAiChatMessage>,
    max_tokens: i32,
    temperature: f64,
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
    YouTubeSummaryError::Http(error.to_string())
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
    fn youtube_transcript_trim_caps_gemini_input_like_go() {
        let transcript = format!("  {}\n{}tail  ", "a".repeat(12_000), "b".repeat(128));

        let out = trim_youtube_transcript_like_go(&transcript);

        assert_eq!(out.len(), 12_000);
        assert!(out.starts_with('a'));
        assert!(!out.contains("tail"));
    }

    #[test]
    fn youtube_summary_gemini_request_matches_go_generation_config() {
        let request = youtube_summary_gemini_request("sys", "0.000000: transcript");
        let value = serde_json::to_value(&request).expect("json");

        assert_eq!(value["systemInstruction"]["parts"][0]["text"], "sys");
        assert_eq!(value["contents"][0]["role"], "user");
        assert_eq!(
            value["contents"][0]["parts"][0]["text"],
            "0.000000: transcript"
        );
        assert_eq!(value["generationConfig"]["maxOutputTokens"], 8192);
        assert_eq!(value["generationConfig"]["temperature"], 0.3);
        assert!(value["generationConfig"].get("topP").is_none());
    }

    #[test]
    fn youtube_summary_openai_request_matches_go_genkit_config() {
        let request = youtube_summary_openai_request("gpt-5-mini", "sys", "0.000000: transcript");
        let value = serde_json::to_value(&request).expect("json");

        assert_eq!(value["model"], "gpt-5-mini");
        assert_eq!(value["messages"][0]["role"], "system");
        assert_eq!(value["messages"][0]["content"], "sys");
        assert_eq!(value["messages"][1]["role"], "user");
        assert_eq!(value["messages"][1]["content"], "0.000000: transcript");
        assert_eq!(value["max_tokens"], 8192);
        assert_eq!(value["temperature"], 0.3);
        assert!(value.get("top_p").is_none());
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
    fn provider_youtube_config_routes_together_default_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("google-key".to_owned()),
            genkit_default_model: Some(" together/meta-llama/Llama-4 ".to_owned()),
            together_keys: Some(" , together-key ".to_owned()),
            dialog_request_timeout_seconds: Some("222".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let summarizer = RuntimeYouTubeSummarizer::from_app_config(&config)
            .expect("build")
            .expect("summary");

        let RuntimeYouTubeSummarizer::OpenAiCompatible(summarizer) = summarizer else {
            panic!("expected OpenAI-compatible route");
        };
        assert_eq!(summarizer.provider, "together");
        assert_eq!(summarizer.cfg.model, "meta-llama/Llama-4");
        assert_eq!(summarizer.cfg.api_key, "together-key");
        assert_eq!(summarizer.cfg.direct_url, TOGETHER_CHAT_COMPLETIONS_URL);
        assert_eq!(summarizer.cfg.request_timeout, Duration::from_secs(222));
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
