//! App-level Telegram vision caption service.

use std::{fmt, future::Future, io::Cursor, pin::Pin, sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures_util::StreamExt;
use image::{GenericImageView, ImageFormat, ImageReader, Limits, imageops::FilterType};
use openplotva_config::AppConfig;
use openplotva_core::ChatAttachment;
use openplotva_dialog::{DialogInput, MultimodalImage, ToolboxError};
use openplotva_llm::aifarm::{
    AifarmClientConfig, AifarmHttpClient, AifarmHttpTransport, ChatCompletionRequest,
    ChatContentPart, ChatImageUrlPart, ChatMessage, ChatVideoUrlPart, ReqwestAifarmTransport,
};
use openplotva_llm::retry::{FailureReason, retryable_reason_from_message};
use openplotva_storage::{
    PostgresHistoryStore, PostgresTelegramFileStore, TELEGRAM_FILE_ASR_STATUS_COMPLETED,
    TELEGRAM_FILE_ASR_STATUS_FAILED, TELEGRAM_FILE_ASR_STATUS_UNAVAILABLE,
    TELEGRAM_FILE_VISION_REQUEST_TIMEOUT, TELEGRAM_FILE_VISION_STATUS_COMPLETED,
    TELEGRAM_FILE_VISION_STATUS_FAILED, TELEGRAM_FILE_VISION_STATUS_PROCESSING,
    TelegramFileMetadataUpsert, TelegramFileRecord, TelegramFileVisionUpdate,
    VisionDescriptionUpdate,
};
use openplotva_telegram::TelegramClient;
use thiserror::Error;
use time::OffsetDateTime;

use crate::{
    asr::DialogAsrInputMaterializer,
    dialog_context::{
        apply_materialized_dialog_vision_context, select_dialog_vision_candidates,
        update_dialog_vision_attachment_caption, vision_attachment_file_id_candidates,
    },
    dialog_tools::{
        VisionDescribeRequest, VisionDescribeResult, VisionDescriber, VisionImageFuture,
    },
    routed_attempts::{
        RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
    },
};

pub const DEFAULT_VISION_MODEL_NAME: &str = "Gemma 4 26B Heretic";
pub const DEFAULT_VISION_MAX_TOKENS: i32 = 1536;
pub const DEFAULT_VISION_TEMPERATURE: f64 = 0.1;
pub const AIFARM_VISION_WORKLOAD: &str = "vision";
pub const LEGACY_VISION_SERVICE_NAME: &str = "vision-api";
pub const LEGACY_VISION_ENDPOINT_NAME: &str = "generate";
pub const VISION_MAX_SIDE: u32 = 512;
/// Longest side of a photo, screenshot or document sent for a caption: small
/// text is unreadable at `VISION_MAX_SIDE`. Stickers keep `VISION_MAX_SIDE`.
pub const DEFAULT_VISION_CAPTION_MAX_SIDE: u32 = 1024;
const VISION_CAPTION_MAX_SIDE_LIMIT: u32 = 2048;
pub const VISION_MAX_VIDEO_BYTES: usize = 20 * 1024 * 1024;
/// Completion floor for a video caption, which walks the clip segment by segment.
const VISION_VIDEO_MIN_MAX_TOKENS: i32 = 2048;
/// Caption stored when the model declines to describe the media.
pub const VISION_UNAVAILABLE_CAPTION: &str = "Описание недоступно.";
/// A caption that repeats one line this many times is a decoding loop.
const VISION_RUNAWAY_LINE_REPEATS: usize = 5;
const VISION_REFUSAL_MAX_CHARS: usize = 120;
const VISION_REFUSAL_MARKERS: [&str; 5] =
    ["не могу", "не буду", "i can't", "i cannot", "i'm unable"];
const VISION_MAX_IMAGE_BYTES: usize = 20 * 1024 * 1024;
const VISION_MAX_SOURCE_SIDE: u32 = 16_384;
const VISION_MAX_SOURCE_PIXELS: u64 = 64 * 1024 * 1024;
const VISION_MAX_DECODER_ALLOC_BYTES: u64 = 256 * 1024 * 1024;
const TELEGRAM_DOWNLOAD_UNAVAILABLE_VISION_NOTE: &str =
    "Визуальное описание недоступно: файл превышает лимит скачивания Telegram Bot API.";

/// Boxed future returned by Telegram file metadata lookups.
pub type TelegramFileLookupFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<Option<TelegramFileRecord>, E>> + Send + 'a>>;

/// Boxed future returned by Telegram file vision status writes.
pub type TelegramFileVisionUpdateFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TelegramFileRecord, E>> + Send + 'a>>;

/// Boxed future returned by concrete vision captioners.
pub type TelegramVisionCaptionFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<TelegramVisionCaptionResult, E>> + Send + 'a>>;

/// Boxed future returned by history vision-description writes.
pub type VisionHistoryUpdateFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<bool, E>> + Send + 'a>>;

/// Boxed future returned by direct Telegram image payload preparation.
pub type TelegramVisionDataUrlFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<String, E>> + Send + 'a>>;

/// Boxed future returned by dialog vision context materializers.
pub type DialogVisionInputFuture<'a> = Pin<Box<dyn Future<Output = DialogInput> + Send + 'a>>;

pub trait TelegramFileVisionStore {
    /// Error returned by the concrete store.
    type Error: fmt::Display;

    /// Load a Telegram file by stable unique file ID.
    fn get_file<'a>(&'a self, file_unique_id: &'a str)
    -> TelegramFileLookupFuture<'a, Self::Error>;

    /// Load a Telegram file by the latest downloadable file ID.
    fn get_file_by_latest_file_id<'a>(
        &'a self,
        latest_file_id: &'a str,
    ) -> TelegramFileLookupFuture<'a, Self::Error>;

    /// Recreate metadata from canonical history when retention removed the cache row.
    fn upsert_metadata<'a>(
        &'a self,
        params: &'a TelegramFileMetadataUpsert,
    ) -> TelegramFileLookupFuture<'a, Self::Error>;

    /// Update Telegram file vision status/caption metadata.
    fn update_vision<'a>(
        &'a self,
        params: &'a TelegramFileVisionUpdate,
    ) -> TelegramFileVisionUpdateFuture<'a, Self::Error>;
}

impl TelegramFileVisionStore for PostgresTelegramFileStore {
    type Error = openplotva_storage::StorageError;

    fn get_file<'a>(
        &'a self,
        file_unique_id: &'a str,
    ) -> TelegramFileLookupFuture<'a, Self::Error> {
        Box::pin(PostgresTelegramFileStore::get_file(self, file_unique_id))
    }

    fn get_file_by_latest_file_id<'a>(
        &'a self,
        latest_file_id: &'a str,
    ) -> TelegramFileLookupFuture<'a, Self::Error> {
        Box::pin(PostgresTelegramFileStore::get_file_by_latest_file_id(
            self,
            latest_file_id,
        ))
    }

    fn upsert_metadata<'a>(
        &'a self,
        params: &'a TelegramFileMetadataUpsert,
    ) -> TelegramFileLookupFuture<'a, Self::Error> {
        Box::pin(async move {
            PostgresTelegramFileStore::upsert_metadata(self, params)
                .await
                .map(Some)
        })
    }

    fn update_vision<'a>(
        &'a self,
        params: &'a TelegramFileVisionUpdate,
    ) -> TelegramFileVisionUpdateFuture<'a, Self::Error> {
        Box::pin(PostgresTelegramFileStore::update_vision(self, params))
    }
}

/// Caption request after Telegram file metadata has been resolved.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramVisionCaptionRequest {
    /// Telegram stable unique file ID.
    pub file_unique_id: String,
    /// Latest downloadable Telegram file ID.
    pub latest_file_id: String,
    pub media_kind: String,
    /// MIME type when known.
    pub mime_type: Option<String>,
}

/// Caption result returned by the concrete VLM/download adapter.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TelegramVisionCaptionResult {
    /// Generated caption.
    pub caption: String,
    /// Provider processing time in seconds.
    pub processing_time_seconds: f64,
}

/// OpenAI-compatible AIFarm vision caption provider configuration.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AifarmVisionCaptionerConfig {
    /// AIFarm HTTP client configuration.
    pub client: AifarmClientConfig,
    /// Vision model.
    pub model: String,
    /// Maximum output tokens.
    pub max_tokens: i32,
    /// Temperature.
    pub temperature: f64,
    /// Optional chat-template thinking control for models that expose it.
    pub enable_thinking: Option<bool>,
}

impl AifarmVisionCaptionerConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        if self.model.trim().is_empty() {
            self.model = DEFAULT_VISION_MODEL_NAME.to_owned();
        }
        if self.max_tokens <= 0 {
            self.max_tokens = DEFAULT_VISION_MAX_TOKENS;
        }
        if !(0.0..=1.0).contains(&self.temperature) {
            self.temperature = DEFAULT_VISION_TEMPERATURE;
        }
        if self.client.default_model.trim().is_empty() {
            self.client.default_model = self.model.clone();
        }
        if self.client.workload.trim().is_empty() {
            self.client.workload = AIFARM_VISION_WORKLOAD.to_owned();
        }
        self.client.priority = self.client.priority.max(0);
        self
    }

    #[must_use]
    pub fn uses_legacy_vision_api(&self) -> bool {
        self.client
            .service_name
            .trim()
            .eq_ignore_ascii_case(LEGACY_VISION_SERVICE_NAME)
            || self
                .client
                .endpoint_name
                .trim()
                .eq_ignore_ascii_case(LEGACY_VISION_ENDPOINT_NAME)
    }
}

#[must_use]
pub fn aifarm_vision_captioner_config_from_app_config(
    config: &AppConfig,
) -> AifarmVisionCaptionerConfig {
    let timeout = positive_seconds(config.vision.request_timeout_seconds);
    let model = config.vision.model.clone();
    AifarmVisionCaptionerConfig {
        client: AifarmClientConfig {
            base_url: config.llm.discovery.base_url.clone(),
            service_name: config.vision.discovery_service_name.clone(),
            endpoint_name: config.vision.discovery_endpoint_name.clone(),
            request_timeout: timeout,
            poll_interval: Duration::from_secs(1),
            task_timeout: timeout,
            capacity_wait: vision_capacity_wait(timeout),
            capacity_poll_interval: Duration::from_secs(1),
            default_model: model.clone(),
            workload: AIFARM_VISION_WORKLOAD.to_owned(),
            ..AifarmClientConfig::default()
        },
        model,
        max_tokens: config.vision.max_tokens,
        temperature: config.vision.temperature,
        enable_thinking: None,
    }
    .with_defaults()
}

/// Concrete VLM/download boundary for Telegram file captioning.
pub trait TelegramVisionCaptioner {
    /// Error returned by the concrete captioner.
    type Error: fmt::Display;

    /// Generate a caption for one Telegram file.
    fn caption_telegram_file<'a>(
        &'a self,
        request: TelegramVisionCaptionRequest,
    ) -> TelegramVisionCaptionFuture<'a, Self::Error>;
}

/// AIFarm/OpenAI-compatible VLM captioner for Telegram files.
#[derive(Clone)]
pub struct AifarmVisionCaptioner<DataUrl, Transport = ReqwestAifarmTransport> {
    cfg: AifarmVisionCaptionerConfig,
    data_url: DataUrl,
    client: AifarmHttpClient<Transport>,
}

impl<DataUrl, Transport> fmt::Debug for AifarmVisionCaptioner<DataUrl, Transport> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AifarmVisionCaptioner")
            .field("model", &self.cfg.model)
            .field("max_tokens", &self.cfg.max_tokens)
            .field("temperature", &self.cfg.temperature)
            .finish_non_exhaustive()
    }
}

impl<DataUrl> AifarmVisionCaptioner<DataUrl, ReqwestAifarmTransport> {
    /// Build a reqwest-backed AIFarm vision captioner.
    #[must_use]
    pub fn new(cfg: AifarmVisionCaptionerConfig, data_url: DataUrl) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::new(cfg.client.clone());
        Self {
            cfg,
            data_url,
            client,
        }
    }
}

impl<DataUrl, Transport> AifarmVisionCaptioner<DataUrl, Transport>
where
    Transport: AifarmHttpTransport,
{
    /// Build with a custom transport for tests.
    #[must_use]
    pub fn with_transport(
        cfg: AifarmVisionCaptionerConfig,
        data_url: DataUrl,
        transport: Transport,
    ) -> Self {
        let cfg = cfg.with_defaults();
        let client = AifarmHttpClient::with_transport(cfg.client.clone(), transport);
        Self {
            cfg,
            data_url,
            client,
        }
    }
}

impl<DataUrl, Transport> TelegramVisionCaptioner for AifarmVisionCaptioner<DataUrl, Transport>
where
    DataUrl: TelegramVisionDataUrlProvider + Send + Sync,
    Transport: AifarmHttpTransport + Send + Sync,
{
    type Error = AifarmVisionCaptionerError;

    fn caption_telegram_file<'a>(
        &'a self,
        request: TelegramVisionCaptionRequest,
    ) -> TelegramVisionCaptionFuture<'a, Self::Error> {
        Box::pin(async move {
            let started = std::time::Instant::now();
            let data_url = self
                .data_url
                .telegram_file_data_url(
                    &request.latest_file_id,
                    &request.media_kind,
                    request.mime_type.as_deref(),
                )
                .await
                .map_err(|error| AifarmVisionCaptionerError::DataUrl(error.to_string()))?;
            let completion = if self.cfg.uses_legacy_vision_api() {
                if vision_request_is_video(&request) {
                    return Err(AifarmVisionCaptionerError::LegacyVideoUnsupported);
                }
                self.client
                    .complete_json_discovery(self.legacy_request(&data_url), &mut |_status| {})
                    .await
            } else {
                self.client
                    .complete(self.request(&data_url, &request)?, &mut |_status| {})
                    .await
            }
            .map_err(|error| AifarmVisionCaptionerError::Provider(error.to_string()))?;
            let caption = extract_aifarm_vision_caption(completion.response.as_ref())
                .ok_or(AifarmVisionCaptionerError::EmptyCaption)?;
            let caption = checked_caption(caption)?;
            Ok(TelegramVisionCaptionResult {
                caption,
                processing_time_seconds: started.elapsed().as_secs_f64(),
            })
        })
    }
}

#[derive(Clone)]
pub struct RoutedVisionCaptioner<DataUrl> {
    walker: RoutedAttemptWalker,
    base_config: AifarmVisionCaptionerConfig,
    data_url: DataUrl,
}

impl<DataUrl> fmt::Debug for RoutedVisionCaptioner<DataUrl> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutedVisionCaptioner")
            .field("base_model", &self.base_config.model)
            .finish_non_exhaustive()
    }
}

impl<DataUrl> RoutedVisionCaptioner<DataUrl> {
    #[must_use]
    pub fn new(
        walker: RoutedAttemptWalker,
        base_config: AifarmVisionCaptionerConfig,
        data_url: DataUrl,
    ) -> Self {
        Self {
            walker,
            base_config: base_config.with_defaults(),
            data_url,
        }
    }
}

impl<DataUrl> TelegramVisionCaptioner for RoutedVisionCaptioner<DataUrl>
where
    DataUrl: TelegramVisionDataUrlProvider + Clone + Send + Sync + 'static,
{
    type Error = AifarmVisionCaptionerError;

    fn caption_telegram_file<'a>(
        &'a self,
        request: TelegramVisionCaptionRequest,
    ) -> TelegramVisionCaptionFuture<'a, Self::Error> {
        Box::pin(async move {
            let request_for_attempts = request.clone();
            let base_config = self.base_config.clone();
            let data_url = self.data_url.clone();
            let result = self
                .walker
                .run(
                    RoutedRequestContext {
                        workflow_key: "vision".to_owned(),
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let request = request_for_attempts.clone();
                        let base_config = base_config.clone();
                        let data_url = data_url.clone();
                        async move {
                            let captioner = AifarmVisionCaptioner::new(
                                vision_config_for_attempt(base_config, &attempt),
                                data_url,
                            );
                            captioner.caption_telegram_file(request).await
                        }
                    },
                    vision_retryable_reason,
                )
                .await;
            match result {
                Ok(result) => Ok(result),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(AifarmVisionCaptionerError::Provider(error.to_string()))
                }
            }
        })
    }
}

fn vision_config_for_attempt(
    mut config: AifarmVisionCaptionerConfig,
    attempt: &RoutedAttempt,
) -> AifarmVisionCaptionerConfig {
    config.model = attempt.model_name.clone();
    config.client.default_model = attempt.model_name.clone();
    config.client.api_key = crate::dialog_runtime::resolve_provider_api_key(
        attempt.provider_api_key_ref.as_deref(),
        attempt.provider_api_key_encrypted.as_deref(),
    )
    .unwrap_or_default();
    if let Some(endpoint) = attempt
        .model_base_url
        .as_deref()
        .or(attempt.provider_endpoint.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if attempt.discovery_service_name.is_some() || attempt.discovery_endpoint_name.is_some() {
            config.client.base_url = endpoint.to_owned();
        } else {
            config.client.direct_url =
                openplotva_llm::aifarm::normalize_chat_completions_url(endpoint);
        }
    }
    if let Some(service) = attempt.discovery_service_name.as_deref() {
        config.client.service_name = service.to_owned();
    }
    if let Some(endpoint) = attempt.discovery_endpoint_name.as_deref() {
        config.client.endpoint_name = endpoint.to_owned();
    }
    if let Some(max_tokens) = attempt.overrides.max_tokens
        && max_tokens > 0
    {
        config.max_tokens = max_tokens;
    }
    if let Some(temperature) = attempt.overrides.temperature {
        config.temperature = temperature;
    }
    config.enable_thinking = attempt
        .overrides
        .extra
        .get("enable_thinking")
        .and_then(serde_json::Value::as_bool);
    config.with_defaults()
}

fn vision_call_trace(is_video: bool) -> openplotva_llm::LlmCallTrace {
    openplotva_llm::LlmCallTrace {
        context: openplotva_llm::LlmCallContext::default(),
        tags: openplotva_llm::LlmCallTags {
            provider: "aifarm".to_owned(),
            source: "aifarm_vision".to_owned(),
            flow: "vision".to_owned(),
            mode: if is_video { "video" } else { "image" }.to_owned(),
            request_kind: "openai.chat.completions".to_owned(),
            iteration: 1,
            docs_chars: 0,
        },
    }
}

fn vision_retryable_reason(error: &AifarmVisionCaptionerError) -> Option<FailureReason> {
    match error {
        AifarmVisionCaptionerError::Provider(message) => retryable_reason_from_message(message),
        AifarmVisionCaptionerError::Runaway => Some(FailureReason::ModelOutputRejected),
        _ => None,
    }
}

impl<DataUrl, Transport> AifarmVisionCaptioner<DataUrl, Transport> {
    fn request(
        &self,
        data_url: &str,
        request: &TelegramVisionCaptionRequest,
    ) -> Result<ChatCompletionRequest, AifarmVisionCaptionerError> {
        let is_video = vision_request_is_video(request);
        let video = if is_video {
            data_url_payload(data_url)
        } else {
            None
        };
        if video.as_deref().is_some_and(mp4_has_av1_track) {
            return Err(AifarmVisionCaptionerError::Av1VideoUnsupported);
        }
        let (system_prompt, user_prompt) = if is_video {
            (
                openplotva_prompts::read("vision/video_system")?,
                openplotva_prompts::render(
                    "vision/video_user",
                    &video_prompt_data(video.as_deref().and_then(mp4_duration_seconds)),
                )?,
            )
        } else {
            (
                openplotva_prompts::read("vision/caption_system")?,
                openplotva_prompts::read("vision/caption_user")?,
            )
        };
        let media_part = if is_video {
            ChatContentPart {
                part_type: "video_url".to_owned(),
                text: String::new(),
                image_url: None,
                video_url: Some(ChatVideoUrlPart {
                    url: data_url.trim().to_owned(),
                }),
            }
        } else {
            ChatContentPart {
                part_type: "image_url".to_owned(),
                text: String::new(),
                image_url: Some(ChatImageUrlPart {
                    url: data_url.trim().to_owned(),
                    detail: "auto".to_owned(),
                }),
                video_url: None,
            }
        };
        let mut completion_request = ChatCompletionRequest {
            model: self.cfg.model.clone(),
            messages: vec![
                ChatMessage {
                    role: "system".to_owned(),
                    content: system_prompt.trim().to_owned(),
                    content_parts: Vec::new(),
                    ..ChatMessage::default()
                },
                ChatMessage {
                    role: "user".to_owned(),
                    content: String::new(),
                    content_parts: vec![
                        media_part,
                        ChatContentPart {
                            part_type: "text".to_owned(),
                            text: user_prompt.trim().to_owned(),
                            image_url: None,
                            video_url: None,
                        },
                    ],
                    ..ChatMessage::default()
                },
            ],
            stream: false,
            max_tokens: if is_video {
                self.cfg.max_tokens.max(VISION_VIDEO_MIN_MAX_TOKENS)
            } else {
                self.cfg.max_tokens
            },
            temperature: Some(self.cfg.temperature),
            ..ChatCompletionRequest::default()
        };
        if let Some(enable_thinking) = self.cfg.enable_thinking {
            completion_request.set_chat_template_kwargs(
                serde_json::json!({ "enable_thinking": enable_thinking }),
            );
        }
        completion_request.trace = Some(vision_call_trace(is_video));
        Ok(completion_request)
    }

    fn legacy_request(&self, data_url: &str) -> serde_json::Value {
        if let Some((mime, b64)) = split_vision_data_url(data_url) {
            return serde_json::json!({
                "image_b64": b64,
                "image_mime": if mime.trim().is_empty() { "image/jpeg" } else { mime.trim() },
            });
        }
        serde_json::json!({
            "image_url": data_url.trim(),
        })
    }
}

/// Error returned by the AIFarm vision captioner.
#[derive(Debug, Error)]
pub enum AifarmVisionCaptionerError {
    /// Prompt loading/rendering failed.
    #[error(transparent)]
    Prompt(#[from] openplotva_prompts::PromptError),
    /// Telegram data URL preparation failed.
    #[error("prepare telegram image data url: {0}")]
    DataUrl(String),
    /// AIFarm provider request failed.
    #[error("vision provider request failed: {0}")]
    Provider(String),
    /// Upstream did not return a caption.
    #[error("empty caption")]
    EmptyCaption,
    #[error("legacy vision API does not support video input")]
    LegacyVideoUnsupported,
    /// AV1 video, from which the vision model's frame decoder reads no frames.
    #[error("AV1 video is not supported for captions")]
    Av1VideoUnsupported,
    /// The caption kept repeating one line.
    #[error("vision caption repeats a line {VISION_RUNAWAY_LINE_REPEATS} times")]
    Runaway,
}

pub trait TelegramVisionDataUrlProvider {
    /// Error returned by the concrete downloader/encoder.
    type Error: fmt::Display;

    /// Return a `data:image/...;base64,...` payload for a Telegram `file_id`.
    fn telegram_file_data_url<'a>(
        &'a self,
        latest_file_id: &'a str,
        media_kind: &'a str,
        mime_type: Option<&'a str>,
    ) -> TelegramVisionDataUrlFuture<'a, Self::Error>;
}

#[derive(Clone)]
pub struct TelegramClientVisionDataUrlProvider {
    client: TelegramClient,
    max_side: u32,
}

impl TelegramClientVisionDataUrlProvider {
    /// Build a direct image provider around the runtime Telegram client.
    #[must_use]
    pub fn new(client: TelegramClient) -> Self {
        Self {
            client,
            max_side: VISION_MAX_SIDE,
        }
    }

    /// Send photos, screenshots and documents at up to `max_side` pixels on the
    /// longest side; stickers stay at `VISION_MAX_SIDE`.
    #[must_use]
    pub fn with_max_side(mut self, max_side: u32) -> Self {
        self.max_side = max_side.clamp(VISION_MAX_SIDE, VISION_CAPTION_MAX_SIDE_LIMIT);
        self
    }
}

impl fmt::Debug for TelegramClientVisionDataUrlProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramClientVisionDataUrlProvider")
            .finish_non_exhaustive()
    }
}

impl TelegramVisionDataUrlProvider for TelegramClientVisionDataUrlProvider {
    type Error = TelegramVisionDataUrlError;

    fn telegram_file_data_url<'a>(
        &'a self,
        latest_file_id: &'a str,
        media_kind: &'a str,
        mime_type: Option<&'a str>,
    ) -> TelegramVisionDataUrlFuture<'a, Self::Error> {
        Box::pin(async move {
            let video = vision_media_is_video(media_kind, mime_type);
            let file: carapax::types::File = self
                .client
                .execute(carapax::types::GetFile::new(latest_file_id))
                .await
                .map_err(TelegramVisionDataUrlError::GetFile)?;
            let file_path = file
                .file_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .ok_or(TelegramVisionDataUrlError::MissingFilePath)?;
            let mut stream = self
                .client
                .download_file(file_path)
                .await
                .map_err(TelegramVisionDataUrlError::Download)?;
            let mut data = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(TelegramVisionDataUrlError::DownloadChunk)?;
                if video && data.len().saturating_add(chunk.len()) > VISION_MAX_VIDEO_BYTES {
                    return Err(TelegramVisionDataUrlError::VideoTooLarge);
                }
                if !video && data.len().saturating_add(chunk.len()) > VISION_MAX_IMAGE_BYTES {
                    return Err(TelegramVisionDataUrlError::ImageTooLarge);
                }
                data.extend_from_slice(&chunk);
            }
            if video {
                telegram_vision_video_data_url_from_bytes(&data, mime_type)
                    .map_err(TelegramVisionDataUrlError::Build)
            } else {
                telegram_vision_data_url_from_bytes_with_max_side(
                    &data,
                    still_max_side(media_kind, self.max_side),
                )
                .map_err(TelegramVisionDataUrlError::Build)
            }
        })
    }
}

/// Error returned by concrete direct image payload preparation.
#[derive(Debug, Error)]
pub enum TelegramVisionDataUrlError {
    /// `getFile` failed.
    #[error("get telegram file: {0}")]
    GetFile(#[source] carapax::api::ExecuteError),
    /// Telegram returned no downloadable path.
    #[error("telegram file_path is empty")]
    MissingFilePath,
    /// Download setup failed.
    #[error("download telegram file: {0}")]
    Download(#[source] carapax::api::DownloadFileError),
    /// Download stream failed.
    #[error("download telegram file chunk: {0}")]
    DownloadChunk(#[source] reqwest::Error),
    /// Video exceeded the Bot API download contract and local memory bound.
    #[error("telegram video exceeds 20 MiB")]
    VideoTooLarge,
    /// Image exceeded the encoded input bound.
    #[error("telegram image exceeds 20 MiB")]
    ImageTooLarge,
    /// Downloaded bytes were not usable as a vision image payload.
    #[error("build vision data url: {0}")]
    Build(#[source] TelegramVisionDataUrlBuildError),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum TelegramVisionDataUrlBuildError {
    /// Empty file body.
    #[error("empty image data")]
    EmptyImageData,
    #[error("unsupported image data")]
    UnsupportedImageData,
    #[error("video exceeds 20 MiB")]
    VideoTooLarge,
    /// Image re-encoding failed after resize.
    #[error("encode image data: {0}")]
    EncodeImage(String),
}

pub trait DialogVisionInputMaterializer: fmt::Debug + Send + Sync {
    /// Materialize prompt captions/direct VLM image payloads on a dialog input.
    fn materialize_dialog_vision_input<'a>(
        &'a self,
        input: DialogInput,
        now: OffsetDateTime,
    ) -> DialogVisionInputFuture<'a>;
}

pub trait VisionHistoryStore {
    /// Error returned by the concrete history store.
    type Error: fmt::Display;

    /// Upsert a generated image caption into a text history entry.
    fn upsert_vision_description<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        file_unique_id: &'a str,
        caption: &'a str,
    ) -> VisionHistoryUpdateFuture<'a, Self::Error>;
}

/// No-op history store used until runtime wiring supplies the real store.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopVisionHistoryStore;

impl VisionHistoryStore for NoopVisionHistoryStore {
    type Error = std::convert::Infallible;

    fn upsert_vision_description<'a>(
        &'a self,
        _chat_id: i64,
        _message_id: i32,
        _file_unique_id: &'a str,
        _caption: &'a str,
    ) -> VisionHistoryUpdateFuture<'a, Self::Error> {
        Box::pin(async { Ok(false) })
    }
}

impl VisionHistoryStore for PostgresHistoryStore {
    type Error = openplotva_storage::StorageError;

    fn upsert_vision_description<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        file_unique_id: &'a str,
        caption: &'a str,
    ) -> VisionHistoryUpdateFuture<'a, Self::Error> {
        Box::pin(async move {
            self.upsert_vision_descriptions(
                chat_id,
                message_id,
                &[VisionDescriptionUpdate {
                    file_unique_id: file_unique_id.to_owned(),
                    caption: caption.to_owned(),
                }],
            )
            .await
            .map(|_| true)
        })
    }
}

/// App-level `understand_media` implementation over Telegram file metadata.
#[derive(Clone, Debug)]
pub struct TelegramVisionDescriber<Store, Captioner, History = NoopVisionHistoryStore> {
    store: Store,
    captioner: Captioner,
    history: History,
    model_name: String,
}

#[derive(Clone)]
pub struct TelegramDialogVisionInputMaterializer<Store, Captioner, History, DataUrl> {
    describer: TelegramVisionDescriber<Store, Captioner, History>,
    data_url: DataUrl,
    direct_image_limit: usize,
}

impl<Store, Captioner, History, DataUrl> fmt::Debug
    for TelegramDialogVisionInputMaterializer<Store, Captioner, History, DataUrl>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramDialogVisionInputMaterializer")
            .field("direct_image_limit", &self.direct_image_limit)
            .finish_non_exhaustive()
    }
}

impl<Store, Captioner, History, DataUrl>
    TelegramDialogVisionInputMaterializer<Store, Captioner, History, DataUrl>
{
    #[must_use]
    pub const fn new(
        describer: TelegramVisionDescriber<Store, Captioner, History>,
        data_url: DataUrl,
        direct_image_limit: usize,
    ) -> Self {
        Self {
            describer,
            data_url,
            direct_image_limit,
        }
    }
}

impl<Store, Captioner> TelegramVisionDescriber<Store, Captioner, NoopVisionHistoryStore> {
    /// Build a Telegram vision describer.
    #[must_use]
    pub fn new(store: Store, captioner: Captioner) -> Self {
        Self {
            store,
            captioner,
            history: NoopVisionHistoryStore,
            model_name: DEFAULT_VISION_MODEL_NAME.to_owned(),
        }
    }
}

impl<Store, Captioner, History> TelegramVisionDescriber<Store, Captioner, History> {
    #[must_use]
    pub fn with_model_name(mut self, model_name: impl Into<String>) -> Self {
        let model_name = model_name.into().trim().to_owned();
        if !model_name.is_empty() {
            self.model_name = model_name;
        }
        self
    }

    #[must_use]
    pub fn with_history_store<NewHistory>(
        self,
        history: NewHistory,
    ) -> TelegramVisionDescriber<Store, Captioner, NewHistory> {
        TelegramVisionDescriber {
            store: self.store,
            captioner: self.captioner,
            history,
            model_name: self.model_name,
        }
    }
}

impl<Store, Captioner, History> TelegramVisionDescriber<Store, Captioner, History>
where
    Store: TelegramFileVisionStore + Sync,
    Captioner: TelegramVisionCaptioner + Sync,
    History: VisionHistoryStore + Sync,
{
    /// Describe one Telegram image using an explicit clock instant for tests.
    pub async fn describe_image_at(
        &self,
        request: VisionDescribeRequest,
        now: OffsetDateTime,
    ) -> Result<VisionDescribeResult, VisionDescribeError> {
        self.describe_image_with_record_at(request, now)
            .await
            .map(|(_, result)| result)
    }

    async fn describe_image_with_record_at(
        &self,
        request: VisionDescribeRequest,
        now: OffsetDateTime,
    ) -> Result<(TelegramFileRecord, VisionDescribeResult), VisionDescribeError> {
        let record = self.resolve_telegram_file_for_vision(&request).await?;
        if let Some(caption) = caption_from_record(&record) {
            let history_updated = self
                .update_history_best_effort(&request, &record, &caption)
                .await;
            let result = vision_describe_result(&record, caption, "cache", history_updated);
            return Ok((record, result));
        }
        if telegram_file_exceeds_download_limit(&record) {
            return Err(VisionDescribeError::DownloadUnavailable);
        }
        if vision_caption_pending_at(&record, now) {
            return Err(VisionDescribeError::CaptionPending);
        }

        self.mark_processing(&record, now).await?;
        let caption_result = self
            .captioner
            .caption_telegram_file(caption_request_from_record(&record))
            .await
            .map_err(|error| VisionDescribeError::Caption {
                message: error.to_string(),
            });
        let caption_result = match caption_result {
            Ok(result) => result,
            Err(error) => {
                self.mark_failed_best_effort(&record).await;
                return Err(error);
            }
        };

        let caption = caption_result.caption.trim().to_owned();
        if caption.is_empty() {
            self.mark_failed_best_effort(&record).await;
            return Err(VisionDescribeError::EmptyCaption);
        }

        self.store_completed_caption(&record, &caption, &caption_result, now)
            .await?;
        let history_updated = self
            .update_history_best_effort(&request, &record, &caption)
            .await;
        let result = vision_describe_result(&record, caption, "generated", history_updated);
        Ok((record, result))
    }

    async fn resolve_telegram_file_for_vision(
        &self,
        request: &VisionDescribeRequest,
    ) -> Result<TelegramFileRecord, VisionDescribeError> {
        let input = request.file_id.trim();
        if input.is_empty() {
            return Err(VisionDescribeError::EmptyFileId);
        }

        if let Some(record) = self.lookup_file(input, request.chat_id).await? {
            return resolved_record(record);
        }

        for candidate in
            vision_attachment_file_id_candidates(input, request.message_id, &request.message_meta)
        {
            let candidate = candidate.trim().to_owned();
            if candidate.is_empty() || candidate == input {
                continue;
            }
            if let Some(record) = self.lookup_file(&candidate, request.chat_id).await? {
                return resolved_record(record);
            }
        }

        if let Some(record) = self.restore_file_from_history(request, input).await? {
            return resolved_record(record);
        }

        Err(VisionDescribeError::NotFound)
    }

    async fn restore_file_from_history(
        &self,
        request: &VisionDescribeRequest,
        input: &str,
    ) -> Result<Option<TelegramFileRecord>, VisionDescribeError> {
        let aliases =
            vision_attachment_file_id_candidates(input, request.message_id, &request.message_meta);
        let attachment = request.message_meta.attachments.iter().find(|attachment| {
            let unique_id = attachment.file_unique_id.trim();
            !attachment.file_id.trim().is_empty()
                && (unique_id == input || aliases.iter().any(|alias| alias.trim() == unique_id))
        });
        let Some(attachment) = attachment else {
            return Ok(None);
        };
        let chat_id = (request.chat_id != 0).then_some(request.chat_id);
        let message_id = (request.message_id != 0).then_some(i64::from(request.message_id));
        self.store
            .upsert_metadata(&TelegramFileMetadataUpsert {
                file_unique_id: attachment.file_unique_id.trim().to_owned(),
                latest_file_id: attachment.file_id.trim().to_owned(),
                media_kind: attachment.kind.trim().to_owned(),
                mime_type: (!attachment.mime_type.trim().is_empty())
                    .then(|| attachment.mime_type.trim().to_owned()),
                width: i32::try_from(attachment.width)
                    .ok()
                    .filter(|value| *value > 0),
                height: i32::try_from(attachment.height)
                    .ok()
                    .filter(|value| *value > 0),
                file_size: None,
                first_seen_chat_id: chat_id,
                first_seen_message_id: message_id,
                last_seen_chat_id: chat_id,
                last_seen_message_id: message_id,
            })
            .await
            .map_err(|error| VisionDescribeError::Storage {
                message: error.to_string(),
            })
    }

    async fn lookup_file(
        &self,
        candidate: &str,
        chat_id: i64,
    ) -> Result<Option<TelegramFileRecord>, VisionDescribeError> {
        if chat_id == 0 {
            return Ok(None);
        }
        if let Some(record) =
            self.store
                .get_file(candidate)
                .await
                .map_err(|error| VisionDescribeError::Storage {
                    message: error.to_string(),
                })?
        {
            return Ok(telegram_file_seen_in_chat(&record, chat_id).then_some(record));
        }
        let record = self
            .store
            .get_file_by_latest_file_id(candidate)
            .await
            .map_err(|error| VisionDescribeError::Storage {
                message: error.to_string(),
            })?;
        Ok(record.filter(|record| telegram_file_seen_in_chat(record, chat_id)))
    }

    async fn mark_processing(
        &self,
        record: &TelegramFileRecord,
        now: OffsetDateTime,
    ) -> Result<(), VisionDescribeError> {
        self.store
            .update_vision(&TelegramFileVisionUpdate {
                file_unique_id: record.file_unique_id.clone(),
                vision_status: TELEGRAM_FILE_VISION_STATUS_PROCESSING.to_owned(),
                recognition_requested_at: Some(now),
                ..TelegramFileVisionUpdate::default()
            })
            .await
            .map(|_| ())
            .map_err(|error| VisionDescribeError::Storage {
                message: error.to_string(),
            })
    }

    async fn mark_failed_best_effort(&self, record: &TelegramFileRecord) {
        if let Err(error) = self
            .store
            .update_vision(&TelegramFileVisionUpdate {
                file_unique_id: record.file_unique_id.clone(),
                vision_status: TELEGRAM_FILE_VISION_STATUS_FAILED.to_owned(),
                ..TelegramFileVisionUpdate::default()
            })
            .await
        {
            tracing::warn!(
                error = error.to_string(),
                file_unique_id = record.file_unique_id,
                "failed to mark Telegram vision caption as failed"
            );
        }
    }

    async fn store_completed_caption(
        &self,
        record: &TelegramFileRecord,
        caption: &str,
        caption_result: &TelegramVisionCaptionResult,
        now: OffsetDateTime,
    ) -> Result<(), VisionDescribeError> {
        self.store
            .update_vision(&TelegramFileVisionUpdate {
                file_unique_id: record.file_unique_id.clone(),
                vision_status: TELEGRAM_FILE_VISION_STATUS_COMPLETED.to_owned(),
                vision_caption: Some(caption.to_owned()),
                vision_model: Some(self.model_name.clone()),
                vision_latency_ms: Some((caption_result.processing_time_seconds * 1000.0) as i32),
                recognition_completed_at: Some(now),
                ..TelegramFileVisionUpdate::default()
            })
            .await
            .map(|_| ())
            .map_err(|error| VisionDescribeError::Storage {
                message: error.to_string(),
            })
    }

    async fn update_history_best_effort(
        &self,
        request: &VisionDescribeRequest,
        record: &TelegramFileRecord,
        caption: &str,
    ) -> bool {
        let Some((chat_id, message_id, file_unique_id)) =
            vision_history_binding(record, request.chat_id)
        else {
            return false;
        };

        match self
            .history
            .upsert_vision_description(chat_id, message_id, &file_unique_id, caption)
            .await
        {
            Ok(updated) => updated,
            Err(error) => {
                tracing::warn!(
                    error = error.to_string(),
                    chat_id,
                    message_id,
                    file_unique_id,
                    "failed to upsert vision description into history"
                );
                true
            }
        }
    }
}

fn telegram_file_seen_in_chat(record: &TelegramFileRecord, chat_id: i64) -> bool {
    chat_id != 0
        && (record.first_seen_chat_id == Some(chat_id) || record.last_seen_chat_id == Some(chat_id))
}

impl<Store, Captioner, History, DataUrl> DialogVisionInputMaterializer
    for TelegramDialogVisionInputMaterializer<Store, Captioner, History, DataUrl>
where
    Store: TelegramFileVisionStore + Send + Sync,
    Captioner: TelegramVisionCaptioner + Send + Sync,
    History: VisionHistoryStore + Send + Sync,
    DataUrl: TelegramVisionDataUrlProvider + Send + Sync,
{
    fn materialize_dialog_vision_input<'a>(
        &'a self,
        mut input: DialogInput,
        now: OffsetDateTime,
    ) -> DialogVisionInputFuture<'a> {
        Box::pin(async move {
            let candidates = select_dialog_vision_candidates(input.message.id, &input.message.meta);
            if candidates.is_empty() {
                return input;
            }

            let mut meta = input.message.meta.clone();
            let mut captions = Vec::with_capacity(candidates.len());
            let direct_limit = self.direct_image_limit;
            let mut direct_images = Vec::with_capacity(direct_limit.min(candidates.len()));

            for candidate in candidates {
                let request = dialog_vision_candidate_request(&input, &candidate.file_unique_id);
                let (record, result) = match self
                    .describer
                    .describe_image_with_record_at(request, now)
                    .await
                {
                    Ok(result) => result,
                    Err(VisionDescribeError::DownloadUnavailable) => {
                        update_dialog_vision_attachment_caption(
                            &mut meta,
                            candidate.attachment_index,
                            &candidate.file_unique_id,
                            TELEGRAM_DOWNLOAD_UNAVAILABLE_VISION_NOTE,
                        );
                        captions.push(format!(
                            "{}: {}",
                            candidate.label, TELEGRAM_DOWNLOAD_UNAVAILABLE_VISION_NOTE
                        ));
                        tracing::debug!(
                            file_unique_id = candidate.file_unique_id,
                            label = candidate.label,
                            "skipped VLM caption for Telegram file above download limit"
                        );
                        continue;
                    }
                    Err(error) => {
                        if error == VisionDescribeError::NotFound {
                            tracing::debug!(
                                file_unique_id = candidate.file_unique_id,
                                label = candidate.label,
                                "VLM vision metadata is unavailable"
                            );
                        } else {
                            tracing::warn!(
                                error = error.to_string(),
                                file_unique_id = candidate.file_unique_id,
                                label = candidate.label,
                                "failed to materialize VLM vision caption"
                            );
                        }
                        continue;
                    }
                };

                let caption = result.caption.trim();
                if caption.is_empty() {
                    continue;
                }
                update_dialog_vision_attachment_caption(
                    &mut meta,
                    candidate.attachment_index,
                    &result.file_unique_id,
                    caption,
                );
                captions.push(format!("{}: {}", candidate.label, caption));

                if candidate.media_kind != "image" {
                    continue;
                }
                if direct_images.len() >= direct_limit {
                    continue;
                }
                let latest_file_id = record.latest_file_id.trim();
                if latest_file_id.is_empty() {
                    continue;
                }
                match self
                    .data_url
                    .telegram_file_data_url(
                        latest_file_id,
                        &record.media_kind,
                        record.mime_type.as_deref(),
                    )
                    .await
                {
                    Ok(data_url) if !data_url.trim().is_empty() => {
                        direct_images.push(MultimodalImage {
                            file_unique_id: result.file_unique_id.clone(),
                            source: candidate.source,
                            label: candidate.label,
                            caption: caption.to_owned(),
                            data_url: data_url.trim().to_owned(),
                        });
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(
                            error = error.to_string(),
                            file_unique_id = result.file_unique_id,
                            label = candidate.label,
                            "failed to prepare direct VLM image payload"
                        );
                    }
                }
            }

            apply_materialized_dialog_vision_context(&mut input, meta, &captions, direct_images);
            input
        })
    }
}

impl<Store, Captioner, History> VisionDescriber
    for TelegramVisionDescriber<Store, Captioner, History>
where
    Store: TelegramFileVisionStore + Send + Sync,
    Captioner: TelegramVisionCaptioner + Send + Sync,
    History: VisionHistoryStore + Send + Sync,
{
    fn describe_image<'a>(&'a self, request: VisionDescribeRequest) -> VisionImageFuture<'a> {
        Box::pin(async move {
            self.describe_image_at(request, OffsetDateTime::now_utc())
                .await
                .map_err(|error| Box::new(error) as ToolboxError)
        })
    }
}

#[derive(Clone)]
pub struct TelegramMediaDescriber {
    vision: Arc<dyn VisionDescriber>,
    asr: Arc<dyn DialogAsrInputMaterializer>,
    files: PostgresTelegramFileStore,
}

impl fmt::Debug for TelegramMediaDescriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramMediaDescriber")
            .finish_non_exhaustive()
    }
}

impl TelegramMediaDescriber {
    #[must_use]
    pub fn new(
        vision: Arc<dyn VisionDescriber>,
        asr: Arc<dyn DialogAsrInputMaterializer>,
        files: PostgresTelegramFileStore,
    ) -> Self {
        Self { vision, asr, files }
    }

    async fn resolve_attachment(
        &self,
        request: &VisionDescribeRequest,
    ) -> Result<ChatAttachment, VisionDescribeError> {
        if let Some(attachment) = request_media_attachment(request) {
            return Ok(attachment);
        }
        let input = request.file_id.trim();
        let record = if let Some(record) =
            self.files
                .get_file(input)
                .await
                .map_err(|error| VisionDescribeError::Storage {
                    message: error.to_string(),
                })? {
            Some(record)
        } else {
            self.files
                .get_file_by_latest_file_id(input)
                .await
                .map_err(|error| VisionDescribeError::Storage {
                    message: error.to_string(),
                })?
        };
        let Some(record) = record else {
            return Err(VisionDescribeError::NotFound);
        };
        Ok(ChatAttachment {
            kind: record.media_kind,
            source: "message".to_owned(),
            file_unique_id: record.file_unique_id,
            file_id: record.latest_file_id,
            mime_type: record.mime_type.unwrap_or_default(),
            ..ChatAttachment::default()
        })
    }

    async fn attach_transcript(
        &self,
        request: &VisionDescribeRequest,
        mut attachment: ChatAttachment,
        result: &mut VisionDescribeResult,
    ) {
        attachment.source = "message".to_owned();
        let mut input = DialogInput::default();
        input.context.chat_id = request.chat_id;
        input.context.thread_id = request.thread_id;
        input.message.id = request.message_id;
        input.message.meta.attachments = vec![attachment];
        let _ = self
            .asr
            .materialize_dialog_asr_input(input, OffsetDateTime::now_utc())
            .await;

        match self.files.get_file(&result.file_unique_id).await {
            Ok(Some(record)) if record.asr_status == TELEGRAM_FILE_ASR_STATUS_COMPLETED => {
                result.transcript = record.asr_text.unwrap_or_default().trim().to_owned();
                if result.transcript.is_empty() {
                    result.transcript_error = "ASR completed without transcript text".to_owned();
                }
            }
            Ok(Some(record)) if record.asr_status == TELEGRAM_FILE_ASR_STATUS_FAILED => {
                result.transcript_error = record
                    .asr_error
                    .unwrap_or_else(|| "ASR failed without diagnostic".to_owned());
            }
            Ok(Some(record)) if record.asr_status == TELEGRAM_FILE_ASR_STATUS_UNAVAILABLE => {
                result.transcript_note = asr_unavailable_note(&record);
                result.download_unavailable |=
                    telegram_download_unavailable_error(&result.transcript_note);
            }
            Ok(Some(record)) => {
                result.transcript_error = format!(
                    "ASR did not complete during media analysis (status={})",
                    record.asr_status
                );
            }
            Ok(None) => {
                result.transcript_error =
                    "Telegram media metadata disappeared during ASR".to_owned();
            }
            Err(error) => {
                result.transcript_error = format!("load ASR result: {error}");
            }
        }
        result.download_unavailable |= telegram_download_unavailable_error(&format!(
            "{} {}",
            result.transcript_note, result.transcript_error
        ));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MediaAnalysisRoute {
    Visual,
    Audio,
    VisualAndAudio,
    Unsupported,
}

fn media_analysis_route(attachment: &ChatAttachment) -> MediaAnalysisRoute {
    let kind = attachment.kind.trim().to_ascii_lowercase();
    let mime = attachment.mime_type.trim().to_ascii_lowercase();
    if mime.starts_with("video/") || matches!(kind.as_str(), "video" | "video_note") {
        return MediaAnalysisRoute::VisualAndAudio;
    }
    if mime.starts_with("audio/") || matches!(kind.as_str(), "voice" | "audio") {
        return MediaAnalysisRoute::Audio;
    }
    if mime.starts_with("image/") || matches!(kind.as_str(), "image" | "photo" | "animation") {
        return MediaAnalysisRoute::Visual;
    }
    MediaAnalysisRoute::Unsupported
}

impl VisionDescriber for TelegramMediaDescriber {
    fn describe_image<'a>(&'a self, request: VisionDescribeRequest) -> VisionImageFuture<'a> {
        Box::pin(async move {
            let attachment = self
                .resolve_attachment(&request)
                .await
                .map_err(|error| Box::new(error) as ToolboxError)?;
            if attachment.file_unique_id.trim().is_empty() {
                return Err(Box::new(VisionDescribeError::EmptyFileUniqueId) as ToolboxError);
            }
            let route = media_analysis_route(&attachment);
            if route == MediaAnalysisRoute::Unsupported {
                return Err(Box::new(VisionDescribeError::UnsupportedMedia {
                    kind: attachment.kind.clone(),
                    mime_type: attachment.mime_type.clone(),
                }) as ToolboxError);
            }
            if route == MediaAnalysisRoute::Audio {
                let mut result = VisionDescribeResult {
                    media_kind: attachment.kind.trim().to_ascii_lowercase(),
                    source: "asr".to_owned(),
                    file_unique_id: attachment.file_unique_id.clone(),
                    ..VisionDescribeResult::default()
                };
                self.attach_transcript(&request, attachment, &mut result)
                    .await;
                return Ok(result);
            }

            let described = self.vision.describe_image(request.clone()).await;
            let (mut result, attachment) = match described {
                Ok(result) => (result, attachment),
                Err(error) if telegram_download_unavailable_error(&error.to_string()) => {
                    let result = VisionDescribeResult {
                        visual_error: "файл превышает лимит скачивания Telegram Bot API".to_owned(),
                        download_unavailable: true,
                        media_kind: attachment.kind.trim().to_lowercase(),
                        source: "unavailable".to_owned(),
                        file_unique_id: attachment.file_unique_id.clone(),
                        ..VisionDescribeResult::default()
                    };
                    (result, attachment)
                }
                Err(error) => return Err(error),
            };
            result.media_kind = attachment.kind.trim().to_lowercase();
            if route != MediaAnalysisRoute::VisualAndAudio {
                return Ok(result);
            }
            self.attach_transcript(&request, attachment, &mut result)
                .await;
            Ok(result)
        })
    }
}

fn request_media_attachment(request: &VisionDescribeRequest) -> Option<ChatAttachment> {
    let candidates = vision_attachment_file_id_candidates(
        request.file_id.trim(),
        request.message_id,
        &request.message_meta,
    );
    request
        .message_meta
        .attachments
        .iter()
        .find(|attachment| {
            let file_id = attachment.file_id.trim();
            let unique_id = attachment.file_unique_id.trim();
            file_id == request.file_id.trim()
                || unique_id == request.file_id.trim()
                || candidates
                    .iter()
                    .any(|candidate| candidate == file_id || candidate == unique_id)
        })
        .cloned()
}

fn telegram_download_unavailable_error(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("file is too big")
        || message.contains("file too big")
        || message.contains("exceeds 20 mib")
        || message.contains("exceeds the telegram bot api")
        || message.contains("превышает лимит telegram bot api")
        || message.contains("превышает лимит скачивания telegram bot api")
}

/// Error returned by the app-level Telegram vision describer.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum VisionDescribeError {
    /// Empty tool/file argument.
    #[error("file_id is empty")]
    EmptyFileId,
    /// Metadata row was not found.
    #[error("telegram file not found")]
    NotFound,
    /// The resolved Telegram attachment cannot be analyzed by vision or ASR.
    #[error("unsupported media kind {kind:?} with MIME type {mime_type:?}")]
    UnsupportedMedia { kind: String, mime_type: String },
    /// Metadata row has no stable unique ID.
    #[error("file_unique_id is empty")]
    EmptyFileUniqueId,
    /// Metadata row has no latest downloadable file ID.
    #[error("latest file_id is empty")]
    EmptyLatestFileId,
    /// Another worker is already processing a fresh caption.
    #[error("vision caption pending")]
    CaptionPending,
    /// Telegram Bot API cannot download the known file size.
    #[error("telegram file exceeds 20 MiB Telegram Bot API limit")]
    DownloadUnavailable,
    /// Provider returned an empty caption.
    #[error("empty caption")]
    EmptyCaption,
    /// Storage operation failed.
    #[error("{message}")]
    Storage {
        /// Display form of the storage error.
        message: String,
    },
    /// Caption provider failed.
    #[error("{message}")]
    Caption {
        /// Display form of the caption error.
        message: String,
    },
}

fn resolved_record(record: TelegramFileRecord) -> Result<TelegramFileRecord, VisionDescribeError> {
    if record.file_unique_id.trim().is_empty() {
        return Err(VisionDescribeError::EmptyFileUniqueId);
    }
    if record.latest_file_id.trim().is_empty() {
        return Err(VisionDescribeError::EmptyLatestFileId);
    }
    Ok(record)
}

fn caption_from_record(record: &TelegramFileRecord) -> Option<String> {
    if record.vision_status != TELEGRAM_FILE_VISION_STATUS_COMPLETED {
        return None;
    }
    let caption = record.vision_caption.as_deref()?.trim();
    (!caption.is_empty()).then(|| caption.to_owned())
}

fn telegram_file_exceeds_download_limit(record: &TelegramFileRecord) -> bool {
    record
        .file_size
        .is_some_and(|size| size > i64::try_from(VISION_MAX_VIDEO_BYTES).unwrap_or(i64::MAX))
}

fn vision_caption_pending_at(record: &TelegramFileRecord, now: OffsetDateTime) -> bool {
    record.vision_status == TELEGRAM_FILE_VISION_STATUS_PROCESSING
        && record.recognition_requested_at.is_some_and(|requested_at| {
            requested_at + time_duration(TELEGRAM_FILE_VISION_REQUEST_TIMEOUT) > now
        })
}

fn time_duration(duration: Duration) -> time::Duration {
    time::Duration::seconds(i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
}

fn caption_request_from_record(record: &TelegramFileRecord) -> TelegramVisionCaptionRequest {
    TelegramVisionCaptionRequest {
        file_unique_id: record.file_unique_id.trim().to_owned(),
        latest_file_id: record.latest_file_id.trim().to_owned(),
        media_kind: record.media_kind.trim().to_owned(),
        mime_type: record.mime_type.clone(),
    }
}

fn vision_history_binding(
    record: &TelegramFileRecord,
    current_chat_id: i64,
) -> Option<(i64, i32, String)> {
    if current_chat_id == 0 {
        return None;
    }
    let chat_id = record.last_seen_chat_id?;
    if chat_id != current_chat_id {
        return None;
    }
    let message_id = i32::try_from(record.last_seen_message_id?).ok()?;
    let file_unique_id = record.file_unique_id.trim();
    if file_unique_id.is_empty() {
        return None;
    }
    Some((chat_id, message_id, file_unique_id.to_owned()))
}

fn vision_describe_result(
    record: &TelegramFileRecord,
    caption: String,
    source: &str,
    history_updated: bool,
) -> VisionDescribeResult {
    VisionDescribeResult {
        caption,
        visual_error: String::new(),
        transcript: record
            .asr_text
            .as_deref()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .unwrap_or_default()
            .to_owned(),
        transcript_note: String::new(),
        transcript_error: String::new(),
        download_unavailable: false,
        media_kind: match record.media_kind.trim().to_lowercase().as_str() {
            "photo" => "image".to_owned(),
            kind => kind.to_owned(),
        },
        source: source.to_owned(),
        file_unique_id: record.file_unique_id.clone(),
        history_updated,
    }
}

fn asr_unavailable_note(record: &TelegramFileRecord) -> String {
    let error = record
        .asr_error
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if error.contains("empty transcript") {
        return "Речь в аудиодорожке не обнаружена.".to_owned();
    }
    if error.contains("file is too big") || error.contains("exceeds 20 mib") {
        return "Аудиодорожку нельзя распознать: файл превышает лимит Telegram Bot API.".to_owned();
    }
    "В медиа нет доступной для распознавания аудиодорожки.".to_owned()
}

pub fn telegram_vision_data_url_from_bytes(
    data: &[u8],
) -> Result<String, TelegramVisionDataUrlBuildError> {
    telegram_vision_data_url_from_bytes_with_max_side(data, VISION_MAX_SIDE)
}

pub fn telegram_vision_data_url_from_bytes_with_max_side(
    data: &[u8],
    max_side: u32,
) -> Result<String, TelegramVisionDataUrlBuildError> {
    if data.is_empty() {
        return Err(TelegramVisionDataUrlBuildError::EmptyImageData);
    }
    let data = normalize_understand_media(data, max_side)?;
    let mime = telegram_understand_media_mime(&data).unwrap_or("image/jpeg");
    Ok(format!(
        "data:{mime};base64,{}",
        BASE64_STANDARD.encode(data)
    ))
}

pub fn telegram_vision_video_data_url_from_bytes(
    data: &[u8],
    mime_type: Option<&str>,
) -> Result<String, TelegramVisionDataUrlBuildError> {
    if data.is_empty() {
        return Err(TelegramVisionDataUrlBuildError::EmptyImageData);
    }
    if data.len() > VISION_MAX_VIDEO_BYTES {
        return Err(TelegramVisionDataUrlBuildError::VideoTooLarge);
    }
    let mime = mime_type
        .map(str::trim)
        .filter(|mime| mime.starts_with("video/"))
        .unwrap_or("video/mp4");
    Ok(format!(
        "data:{mime};base64,{}",
        BASE64_STANDARD.encode(data)
    ))
}

fn vision_request_is_video(request: &TelegramVisionCaptionRequest) -> bool {
    vision_media_is_video(&request.media_kind, request.mime_type.as_deref())
}

fn vision_media_is_video(media_kind: &str, mime_type: Option<&str>) -> bool {
    matches!(
        media_kind.trim().to_lowercase().as_str(),
        "video" | "animation" | "video_note"
    ) || mime_type
        .map(str::trim)
        .is_some_and(|mime| mime.starts_with("video/"))
}

fn telegram_understand_media_mime(data: &[u8]) -> Option<&'static str> {
    if data.starts_with(&[0xff, 0xd8, 0xff]) {
        return Some("image/jpeg");
    }
    if data.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some("image/png");
    }
    if data.starts_with(b"GIF87a") || data.starts_with(b"GIF89a") {
        return Some("image/gif");
    }
    if data.len() >= 12 && data.starts_with(b"RIFF") && &data[8..12] == b"WEBP" {
        return Some("image/webp");
    }
    None
}

fn normalize_understand_media(
    data: &[u8],
    max_side: u32,
) -> Result<Vec<u8>, TelegramVisionDataUrlBuildError> {
    if data.len() > VISION_MAX_IMAGE_BYTES {
        return Err(TelegramVisionDataUrlBuildError::UnsupportedImageData);
    }
    let reader = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|_| TelegramVisionDataUrlBuildError::UnsupportedImageData)?;
    let Some(format) = reader.format() else {
        return Err(TelegramVisionDataUrlBuildError::UnsupportedImageData);
    };
    if !matches!(
        format,
        ImageFormat::Jpeg | ImageFormat::Png | ImageFormat::Gif | ImageFormat::WebP
    ) {
        return Err(TelegramVisionDataUrlBuildError::UnsupportedImageData);
    }
    let mut limits = Limits::default();
    limits.max_image_width = Some(VISION_MAX_SOURCE_SIDE);
    limits.max_image_height = Some(VISION_MAX_SOURCE_SIDE);
    limits.max_alloc = Some(VISION_MAX_DECODER_ALLOC_BYTES);
    let mut dimension_reader = reader;
    dimension_reader.limits(limits.clone());
    let (source_width, source_height) = dimension_reader
        .into_dimensions()
        .map_err(|_| TelegramVisionDataUrlBuildError::UnsupportedImageData)?;
    if !vision_source_dimensions_allowed(source_width, source_height) {
        return Err(TelegramVisionDataUrlBuildError::UnsupportedImageData);
    }
    let mut reader = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .map_err(|_| TelegramVisionDataUrlBuildError::UnsupportedImageData)?;
    reader.limits(limits);
    let img = reader
        .decode()
        .map_err(|_| TelegramVisionDataUrlBuildError::UnsupportedImageData)?;
    let (width, height) = img.dimensions();
    if width == 0 || height == 0 {
        return Err(TelegramVisionDataUrlBuildError::UnsupportedImageData);
    }
    let (new_width, new_height) = normalized_vision_dimensions(width, height, max_side);
    if new_width == width && new_height == height {
        return Ok(data.to_vec());
    }
    let resized = img.resize_exact(new_width, new_height, FilterType::CatmullRom);
    encode_normalized_understand_media(&resized, format)
}

fn vision_source_dimensions_allowed(width: u32, height: u32) -> bool {
    width > 0
        && height > 0
        && width <= VISION_MAX_SOURCE_SIDE
        && height <= VISION_MAX_SOURCE_SIDE
        && u64::from(width) * u64::from(height) <= VISION_MAX_SOURCE_PIXELS
}

fn normalized_vision_dimensions(width: u32, height: u32, max_side: u32) -> (u32, u32) {
    let width = width.max(1);
    let height = height.max(1);
    if width <= max_side && height <= max_side {
        return (width, height);
    }
    let scale = (f64::from(max_side) / f64::from(width))
        .min(f64::from(max_side) / f64::from(height))
        .min(1.0);
    (
        clamp_vision_dimension(f64::from(width) * scale, max_side),
        clamp_vision_dimension(f64::from(height) * scale, max_side),
    )
}

fn clamp_vision_dimension(value: f64, max_side: u32) -> u32 {
    let dimension = value.round();
    if dimension < 1.0 {
        1
    } else if dimension > f64::from(max_side) {
        max_side
    } else {
        dimension as u32
    }
}

fn encode_normalized_understand_media(
    img: &image::DynamicImage,
    format: ImageFormat,
) -> Result<Vec<u8>, TelegramVisionDataUrlBuildError> {
    let mut out = Vec::new();
    match format {
        ImageFormat::Png | ImageFormat::Gif | ImageFormat::WebP => {
            img.write_to(&mut Cursor::new(&mut out), ImageFormat::Png)
                .map_err(|error| TelegramVisionDataUrlBuildError::EncodeImage(error.to_string()))?;
        }
        _ => {
            let rgb = img.to_rgb8();
            let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 90);
            encoder
                .encode_image(&rgb)
                .map_err(|error| TelegramVisionDataUrlBuildError::EncodeImage(error.to_string()))?;
        }
    }
    Ok(out)
}

fn split_vision_data_url(data_url: &str) -> Option<(&str, &str)> {
    let data_url = data_url.trim();
    let rest = data_url.strip_prefix("data:")?;
    let (mime, body) = rest.split_once(";base64,")?;
    let b64 = body.trim();
    (!b64.is_empty()).then_some((mime.trim(), b64))
}

fn still_max_side(media_kind: &str, max_side: u32) -> u32 {
    if media_kind.trim().eq_ignore_ascii_case("sticker") {
        VISION_MAX_SIDE
    } else {
        max_side
    }
}

fn data_url_payload(data_url: &str) -> Option<Vec<u8>> {
    let (_, payload) = split_vision_data_url(data_url)?;
    BASE64_STANDARD.decode(payload).ok()
}

/// Whole seconds from the `mvhd` box of an MP4 or MOV file.
fn mp4_duration_seconds(data: &[u8]) -> Option<u64> {
    let moov = mp4_child(data, b"moov")?;
    let mvhd = mp4_child(moov, b"mvhd")?;
    let (&version, fields) = mvhd.split_first()?;
    let be_u32 = |at: usize| -> Option<u64> {
        Some(u64::from(u32::from_be_bytes(
            fields.get(at..at + 4)?.try_into().ok()?,
        )))
    };
    // After the version byte: 3 bytes of flags, then creation and modification
    // times (4 bytes each in version 0, 8 in version 1), the timescale and the
    // duration (as wide as the times).
    let (timescale, duration) = if version == 1 {
        let duration = u64::from_be_bytes(fields.get(23..31)?.try_into().ok()?);
        (be_u32(19)?, duration)
    } else {
        (be_u32(11)?, be_u32(15)?)
    };
    (timescale > 0).then(|| duration / timescale)
}

/// Whether a track of an MP4 or MOV file stores AV1 video. The first sample
/// entry of `stsd` follows its version, flags and entry count.
fn mp4_has_av1_track(data: &[u8]) -> bool {
    let Some(moov) = mp4_child(data, b"moov") else {
        return false;
    };
    mp4_boxes(moov)
        .filter(|(box_type, _)| *box_type == b"trak")
        .filter_map(|(_, trak)| {
            let mdia = mp4_child(trak, b"mdia")?;
            let minf = mp4_child(mdia, b"minf")?;
            let stbl = mp4_child(minf, b"stbl")?;
            mp4_child(stbl, b"stsd")
        })
        .any(|stsd| {
            stsd.get(12..16)
                .is_some_and(|sample_entry| sample_entry == b"av01")
        })
}

/// Payload of the first box called `name` among the boxes in `data`.
fn mp4_child<'a>(data: &'a [u8], name: &[u8; 4]) -> Option<&'a [u8]> {
    mp4_boxes(data).find_map(|(box_type, payload)| (box_type == name).then_some(payload))
}

/// Type and payload of each box laid out one after another in `data`.
fn mp4_boxes(mut data: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    std::iter::from_fn(move || {
        if data.len() < 8 {
            return None;
        }
        let declared = u32::from_be_bytes(data.get(0..4)?.try_into().ok()?);
        let (header, size) = match declared {
            0 => (8, data.len()),
            1 => (
                16,
                usize::try_from(u64::from_be_bytes(data.get(8..16)?.try_into().ok()?)).ok()?,
            ),
            size => (8, usize::try_from(size).ok()?),
        };
        if size < header || size > data.len() {
            return None;
        }
        let (current, rest) = data.split_at(size);
        data = rest;
        Some((current.get(4..8)?, current.get(header..)?))
    })
}

fn video_prompt_data(duration_seconds: Option<u64>) -> serde_json::Value {
    match duration_seconds {
        Some(seconds) => serde_json::json!({
            "duration": format!("{:02}:{:02}", seconds / 60, seconds % 60),
            "long_clip": seconds > 60,
        }),
        None => serde_json::json!({}),
    }
}

/// The caption to store: a decoding loop is rejected so the route samples
/// again or falls back, and a refusal becomes `VISION_UNAVAILABLE_CAPTION`.
fn checked_caption(caption: String) -> Result<String, AifarmVisionCaptionerError> {
    let mut seen: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    let runaway = caption
        .lines()
        .map(str::trim)
        .filter(|line| line.chars().count() > 8)
        .any(|line| {
            let count = seen.entry(line).or_default();
            *count += 1;
            *count >= VISION_RUNAWAY_LINE_REPEATS
        });
    if runaway {
        return Err(AifarmVisionCaptionerError::Runaway);
    }
    let bare = caption
        .trim()
        .trim_matches(|ch| matches!(ch, '«' | '»' | '"'));
    let lowered = bare.to_lowercase();
    let refused = bare == VISION_UNAVAILABLE_CAPTION
        || (bare.chars().count() <= VISION_REFUSAL_MAX_CHARS
            && !bare.contains("Тип:")
            && VISION_REFUSAL_MARKERS
                .iter()
                .any(|marker| lowered.contains(marker)));
    if refused {
        if bare != VISION_UNAVAILABLE_CAPTION {
            tracing::warn!(caption = %bare, "vision model declined to describe the media");
        }
        return Ok(VISION_UNAVAILABLE_CAPTION.to_owned());
    }
    Ok(caption)
}

fn extract_aifarm_vision_caption(response: Option<&serde_json::Value>) -> Option<String> {
    let response = response?;
    first_nested_caption_text(
        response,
        &["caption", "text", "description", "result", "output"],
    )
    .or_else(|| first_nested_caption_text(response, &["choices", "message", "content"]))
}

fn first_nested_caption_text(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    match value {
        serde_json::Value::String(text) => non_empty_trimmed(text),
        serde_json::Value::Array(values) => values
            .iter()
            .find_map(|value| first_nested_caption_text(value, keys)),
        serde_json::Value::Object(obj) => keys
            .iter()
            .filter_map(|key| obj.get(*key))
            .find_map(|value| first_nested_caption_text(value, keys)),
        _ => None,
    }
}

fn non_empty_trimmed(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

fn positive_seconds(seconds: i32) -> Duration {
    Duration::from_secs(u64::try_from(seconds.max(1)).unwrap_or(1))
}

fn vision_capacity_wait(timeout: Duration) -> Duration {
    let wait = timeout / 2;
    if wait.is_zero() || wait > Duration::from_secs(60) {
        Duration::from_secs(60)
    } else {
        wait
    }
}

fn dialog_vision_candidate_request(
    input: &DialogInput,
    file_unique_id: &str,
) -> VisionDescribeRequest {
    VisionDescribeRequest {
        file_id: file_unique_id.to_owned(),
        chat_id: input.context.chat_id,
        message_id: input.message.id,
        thread_id: input.context.thread_id,
        message_meta: input.message.meta.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    use image::{DynamicImage, RgbImage};
    use openplotva_core::{ChatAttachment, ChatMessageMeta};
    use openplotva_dialog::{DialogContext, DialogMessage};
    use openplotva_llm::aifarm::{
        AifarmHttpFuture, AifarmHttpMethod, AifarmHttpRequest, AifarmHttpResponse,
        DiscoveryJobRequest,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn routed_vision_attempt_resolves_direct_endpoint_key_and_thinking_override() {
        let path = std::env::var("PATH").expect("test process PATH");
        let attempt = RoutedAttempt {
            provider_id: 7,
            model_id: 11,
            provider_name: "vram-cloud".to_owned(),
            model_name: "vram.cloud/qwen3.6-35b-a3b".to_owned(),
            provider_runtime_hint: None,
            provider_endpoint: Some("https://vision.example.test/v1".to_owned()),
            discovery_service_name: None,
            discovery_endpoint_name: None,
            provider_api_key_ref: Some("PATH".to_owned()),
            provider_api_key_encrypted: None,
            model_base_url: None,
            embedding_dim: None,
            provider_config: json!({}),
            model_config: json!({}),
            overrides: openplotva_llm::router::InferenceOverrides {
                extra: json!({ "enable_thinking": false }),
                ..openplotva_llm::router::InferenceOverrides::default()
            },
            variant: None,
        };

        let config = vision_config_for_attempt(AifarmVisionCaptionerConfig::default(), &attempt);

        assert_eq!(config.model, "vram.cloud/qwen3.6-35b-a3b");
        assert_eq!(
            config.client.direct_url,
            "https://vision.example.test/v1/chat/completions"
        );
        assert_eq!(config.client.api_key, path);
        assert_eq!(config.enable_thinking, Some(false));
    }

    #[test]
    fn telegram_download_limit_is_terminal_but_provider_failures_are_not() {
        assert!(telegram_download_unavailable_error(
            "Bad Request: file is too big"
        ));
        assert!(telegram_download_unavailable_error(
            "file exceeds 20 MiB Telegram Bot API limit"
        ));
        assert!(!telegram_download_unavailable_error(
            "vision provider temporarily unavailable"
        ));
    }

    #[test]
    fn media_router_selects_modalities_before_decoding() {
        let route = |kind: &str, mime: &str| {
            media_analysis_route(&ChatAttachment {
                kind: kind.to_owned(),
                mime_type: mime.to_owned(),
                ..ChatAttachment::default()
            })
        };
        assert_eq!(route("image", "image/jpeg"), MediaAnalysisRoute::Visual);
        assert_eq!(
            route("video", "video/mp4"),
            MediaAnalysisRoute::VisualAndAudio
        );
        assert_eq!(route("voice", "audio/ogg"), MediaAnalysisRoute::Audio);
        assert_eq!(route("audio", "audio/mpeg"), MediaAnalysisRoute::Audio);
        assert_eq!(
            route("document", "video/webm"),
            MediaAnalysisRoute::VisualAndAudio
        );
        assert_eq!(
            route("sticker", "application/x-tgsticker"),
            MediaAnalysisRoute::Unsupported
        );
        assert_eq!(
            route("sticker", "application/octet-stream"),
            MediaAnalysisRoute::Unsupported
        );
    }

    #[tokio::test]
    async fn vision_describer_returns_cached_caption_by_alias_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let record = file_record("photo-u", "photo-file", now)
            .with_status(TELEGRAM_FILE_VISION_STATUS_COMPLETED)
            .with_caption(" cached cat ");
        let store = FileStoreStub::with_records(vec![record]);
        let captioner = CaptionerStub::successful("should not run", 1.0);
        let service = TelegramVisionDescriber::new(store.clone(), captioner.clone());

        let result = service
            .describe_image_at(vision_request("message_77_image_1"), now)
            .await?;

        assert_eq!(
            result,
            VisionDescribeResult {
                caption: "cached cat".to_owned(),
                visual_error: String::new(),
                transcript: String::new(),
                transcript_note: String::new(),
                transcript_error: String::new(),
                download_unavailable: false,
                media_kind: "image".to_owned(),
                source: "cache".to_owned(),
                file_unique_id: "photo-u".to_owned(),
                history_updated: false,
            }
        );
        assert!(captioner.calls().is_empty());
        assert!(store.updates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn vision_describer_restores_expired_metadata_from_canonical_history()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let store = FileStoreStub::default();
        let captioner = CaptionerStub::successful("restored video", 0.5);
        let service = TelegramVisionDescriber::new(store.clone(), captioner.clone());
        let mut request = vision_request("message_77_image_1");
        request.message_meta.attachments[0].kind = "video".to_owned();
        request.message_meta.attachments[0].mime_type = "video/mp4".to_owned();
        request.message_meta.attachments[0].file_id = "fresh-download-id".to_owned();

        let result = service.describe_image_at(request, now).await?;

        assert_eq!(result.caption, "restored video");
        assert_eq!(result.file_unique_id, "photo-u");
        assert_eq!(result.media_kind, "video");
        assert_eq!(
            captioner.calls(),
            vec![TelegramVisionCaptionRequest {
                file_unique_id: "photo-u".to_owned(),
                latest_file_id: "fresh-download-id".to_owned(),
                media_kind: "video".to_owned(),
                mime_type: Some("video/mp4".to_owned()),
            }]
        );
        assert_eq!(store.updates().len(), 2);
        Ok(())
    }

    #[test]
    fn unavailable_asr_note_preserves_terminal_reason() -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let mut record = file_record("video-u", "video-file", now);

        record.asr_error = Some("empty transcript".to_owned());
        assert_eq!(
            asr_unavailable_note(&record),
            "Речь в аудиодорожке не обнаружена."
        );

        record.asr_error = Some("Telegram getFile failed: file is too big".to_owned());
        assert_eq!(
            asr_unavailable_note(&record),
            "Аудиодорожку нельзя распознать: файл превышает лимит Telegram Bot API."
        );

        record.asr_error = Some("no audio stream".to_owned());
        assert_eq!(
            asr_unavailable_note(&record),
            "В медиа нет доступной для распознавания аудиодорожки."
        );
        Ok(())
    }

    #[tokio::test]
    async fn vision_describer_marks_processing_generates_and_stores_caption()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let record = file_record("photo-u", "photo-file", now);
        let store = FileStoreStub::with_records(vec![record]);
        let captioner = CaptionerStub::successful(" generated dog ", 1.25);
        let service = TelegramVisionDescriber::new(store.clone(), captioner.clone())
            .with_model_name(" vision-model ");

        let result = service
            .describe_image_at(vision_request("photo-u"), now)
            .await?;

        assert_eq!(result.caption, "generated dog");
        assert_eq!(result.source, "generated");
        assert_eq!(
            captioner.calls(),
            vec![TelegramVisionCaptionRequest {
                file_unique_id: "photo-u".to_owned(),
                latest_file_id: "photo-file".to_owned(),
                media_kind: "photo".to_owned(),
                mime_type: None,
            }]
        );
        let updates = store.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[0].vision_status,
            TELEGRAM_FILE_VISION_STATUS_PROCESSING
        );
        assert_eq!(updates[0].recognition_requested_at, Some(now));
        assert_eq!(
            updates[1].vision_status,
            TELEGRAM_FILE_VISION_STATUS_COMPLETED
        );
        assert_eq!(updates[1].vision_caption.as_deref(), Some("generated dog"));
        assert_eq!(updates[1].vision_model.as_deref(), Some("vision-model"));
        assert_eq!(updates[1].vision_latency_ms, Some(1250));
        assert_eq!(updates[1].recognition_completed_at, Some(now));
        Ok(())
    }

    #[tokio::test]
    async fn vision_describer_updates_history_when_binding_matches_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let record = file_record("photo-u", "photo-file", now)
            .with_status(TELEGRAM_FILE_VISION_STATUS_COMPLETED)
            .with_caption(" cached cat ");
        let store = FileStoreStub::with_records(vec![record]);
        let captioner = CaptionerStub::successful("unused", 1.0);
        let history = HistoryStoreStub::default();
        let service =
            TelegramVisionDescriber::new(store, captioner).with_history_store(history.clone());

        let result = service
            .describe_image_at(vision_request("photo-u"), now)
            .await?;

        assert!(result.history_updated);
        assert_eq!(
            history.calls(),
            vec![HistoryCall {
                chat_id: 42,
                message_id: 77,
                file_unique_id: "photo-u".to_owned(),
                caption: "cached cat".to_owned(),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn vision_describer_reports_pending_without_captioner_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let record = file_record("photo-u", "photo-file", now)
            .with_status(TELEGRAM_FILE_VISION_STATUS_PROCESSING)
            .with_requested_at(now - time::Duration::seconds(10));
        let store = FileStoreStub::with_records(vec![record]);
        let captioner = CaptionerStub::successful("unused", 1.0);
        let service = TelegramVisionDescriber::new(store.clone(), captioner.clone());

        let error = service
            .describe_image_at(vision_request("photo-u"), now)
            .await
            .expect_err("fresh processing caption should be pending");

        assert_eq!(error, VisionDescribeError::CaptionPending);
        assert!(captioner.calls().is_empty());
        assert!(store.updates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn vision_describer_marks_failure_after_caption_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let record = file_record("photo-u", "photo-file", now);
        let store = FileStoreStub::with_records(vec![record]);
        let captioner = CaptionerStub::failed("vision unavailable");
        let service = TelegramVisionDescriber::new(store.clone(), captioner);

        let error = service
            .describe_image_at(vision_request("photo-u"), now)
            .await
            .expect_err("caption failure should propagate");

        assert_eq!(
            error,
            VisionDescribeError::Caption {
                message: "vision unavailable".to_owned(),
            }
        );
        let updates = store.updates();
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[0].vision_status,
            TELEGRAM_FILE_VISION_STATUS_PROCESSING
        );
        assert_eq!(updates[1].vision_status, TELEGRAM_FILE_VISION_STATUS_FAILED);
        Ok(())
    }

    #[tokio::test]
    async fn vision_describer_skips_known_oversized_file_without_download_or_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let mut record = file_record("video-u", "video-file", now);
        record.media_kind = "video".to_owned();
        record.mime_type = Some("video/mp4".to_owned());
        record.file_size = Some(i64::try_from(VISION_MAX_VIDEO_BYTES)? + 1);
        let store = FileStoreStub::with_records(vec![record]);
        let captioner = CaptionerStub::successful("must not run", 1.0);
        let service = TelegramVisionDescriber::new(store.clone(), captioner.clone());

        let error = service
            .describe_image_at(vision_request("video-u"), now)
            .await
            .expect_err("known oversized Telegram file must be terminal");

        assert_eq!(error, VisionDescribeError::DownloadUnavailable);
        assert!(captioner.calls().is_empty());
        assert!(store.updates().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn dialog_vision_materializer_adds_captions_direct_images_and_history_like_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let message = file_record("photo-u", "photo-file", now)
            .with_status(TELEGRAM_FILE_VISION_STATUS_COMPLETED)
            .with_caption(" cached cat ");
        let mut quoted = file_record("quoted-u", "quoted-file", now)
            .with_status(TELEGRAM_FILE_VISION_STATUS_COMPLETED)
            .with_caption(" quoted dog ");
        quoted.last_seen_message_id = Some(66);
        let store = FileStoreStub::with_records(vec![message, quoted]);
        let captioner = CaptionerStub::successful("unused", 1.0);
        let history = HistoryStoreStub::default();
        let describer =
            TelegramVisionDescriber::new(store, captioner).with_history_store(history.clone());
        let data_urls = DataUrlStub::default();
        let materializer =
            TelegramDialogVisionInputMaterializer::new(describer, data_urls.clone(), 1);

        let input = materializer
            .materialize_dialog_vision_input(dialog_input_with_images(), now)
            .await;

        assert_eq!(
            input.message.meta.vision_description,
            "message_77_image_1: cached cat\nquoted_image_1: quoted dog"
        );
        assert_eq!(input.message.meta.attachments[0].caption, "cached cat");
        assert_eq!(input.message.meta.attachments[1].caption, "quoted dog");
        assert_eq!(
            input.multimodal_images,
            vec![MultimodalImage {
                file_unique_id: "photo-u".to_owned(),
                source: "message".to_owned(),
                label: "message_77_image_1".to_owned(),
                caption: "cached cat".to_owned(),
                data_url: "data:image/jpeg;base64,photo-file".to_owned(),
            }]
        );
        assert_eq!(data_urls.calls(), vec!["photo-file"]);
        assert_eq!(
            history.calls(),
            vec![
                HistoryCall {
                    chat_id: 42,
                    message_id: 77,
                    file_unique_id: "photo-u".to_owned(),
                    caption: "cached cat".to_owned(),
                },
                HistoryCall {
                    chat_id: 42,
                    message_id: 66,
                    file_unique_id: "quoted-u".to_owned(),
                    caption: "quoted dog".to_owned(),
                },
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn dialog_vision_materializer_adds_video_caption_without_reembedding_video()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let mut video = file_record("video-u", "video-file", now)
            .with_status(TELEGRAM_FILE_VISION_STATUS_COMPLETED)
            .with_caption("00:00 человек входит в комнату");
        video.media_kind = "video".to_owned();
        video.mime_type = Some("video/mp4".to_owned());
        let store = FileStoreStub::with_records(vec![video]);
        let describer =
            TelegramVisionDescriber::new(store, CaptionerStub::successful("unused", 1.0));
        let data_urls = DataUrlStub::default();
        let materializer =
            TelegramDialogVisionInputMaterializer::new(describer, data_urls.clone(), 1);
        let input = DialogInput {
            context: DialogContext {
                chat_id: 42,
                ..DialogContext::default()
            },
            message: DialogMessage {
                id: 77,
                text: "Аудиодорожка видео (ASR): добрый вечер".to_owned(),
                normalized: "Аудиодорожка видео (ASR): добрый вечер".to_owned(),
                meta: ChatMessageMeta {
                    attachments: vec![ChatAttachment {
                        kind: "video".to_owned(),
                        source: "message".to_owned(),
                        file_unique_id: "video-u".to_owned(),
                        mime_type: "video/mp4".to_owned(),
                        content: "Аудиодорожка видео (ASR): добрый вечер".to_owned(),
                        ..ChatAttachment::default()
                    }],
                    ..ChatMessageMeta::default()
                },
                ..DialogMessage::default()
            },
            ..DialogInput::default()
        };

        let input = materializer
            .materialize_dialog_vision_input(input, now)
            .await;

        assert_eq!(
            input.message.meta.vision_description,
            "message_77_video_1: 00:00 человек входит в комнату"
        );
        assert_eq!(input.message.text, "Аудиодорожка видео (ASR): добрый вечер");
        assert_eq!(
            input.message.meta.attachments[0].content,
            "Аудиодорожка видео (ASR): добрый вечер"
        );
        assert_eq!(
            input.message.meta.attachments[0].caption,
            "00:00 человек входит в комнату"
        );
        assert!(input.multimodal_images.is_empty());
        assert!(data_urls.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn dialog_vision_materializer_exposes_oversized_file_note_without_download()
    -> Result<(), Box<dyn std::error::Error>> {
        let now = unix_time(1_710_000_000)?;
        let mut video = file_record("video-u", "video-file", now);
        video.media_kind = "video".to_owned();
        video.mime_type = Some("video/mp4".to_owned());
        video.file_size = Some(i64::try_from(VISION_MAX_VIDEO_BYTES)? + 1);
        let store = FileStoreStub::with_records(vec![video]);
        let captioner = CaptionerStub::successful("must not run", 1.0);
        let describer = TelegramVisionDescriber::new(store.clone(), captioner.clone());
        let data_urls = DataUrlStub::default();
        let materializer =
            TelegramDialogVisionInputMaterializer::new(describer, data_urls.clone(), 1);
        let input = DialogInput {
            context: DialogContext {
                chat_id: 42,
                ..DialogContext::default()
            },
            message: DialogMessage {
                id: 77,
                meta: ChatMessageMeta {
                    attachments: vec![ChatAttachment {
                        kind: "video".to_owned(),
                        source: "message".to_owned(),
                        file_unique_id: "video-u".to_owned(),
                        mime_type: "video/mp4".to_owned(),
                        ..ChatAttachment::default()
                    }],
                    ..ChatMessageMeta::default()
                },
                ..DialogMessage::default()
            },
            ..DialogInput::default()
        };

        let input = materializer
            .materialize_dialog_vision_input(input, now)
            .await;

        assert_eq!(
            input.message.meta.vision_description,
            format!("message_77_video_1: {TELEGRAM_DOWNLOAD_UNAVAILABLE_VISION_NOTE}")
        );
        assert_eq!(
            input.message.meta.attachments[0].caption,
            TELEGRAM_DOWNLOAD_UNAVAILABLE_VISION_NOTE
        );
        assert!(input.multimodal_images.is_empty());
        assert!(captioner.calls().is_empty());
        assert!(store.updates().is_empty());
        assert!(data_urls.calls().is_empty());
        Ok(())
    }

    #[test]
    fn telegram_vision_data_url_from_bytes_accepts_and_resizes_go_image_formats()
    -> Result<(), Box<dyn std::error::Error>> {
        let jpeg = encode_rgb_test_image(ImageFormat::Jpeg, 1024, 768)?;
        let jpeg_data_url = telegram_vision_data_url_from_bytes(&jpeg)?;
        assert!(jpeg_data_url.starts_with("data:image/jpeg;base64,"));
        let jpeg_payload = decode_data_url_payload(&jpeg_data_url)?;
        let decoded_jpeg = image::load_from_memory(&jpeg_payload)?;
        assert_eq!(decoded_jpeg.dimensions(), (512, 384));

        let png = encode_rgb_test_image(ImageFormat::Png, 64, 32)?;
        let png_data_url = telegram_vision_data_url_from_bytes(&png)?;
        assert!(png_data_url.starts_with("data:image/png;base64,"));
        let png_payload = decode_data_url_payload(&png_data_url)?;
        let decoded_png = image::load_from_memory(&png_payload)?;
        assert_eq!(decoded_png.dimensions(), (64, 32));

        Ok(())
    }

    #[test]
    fn normalized_vision_dimensions_match_go_boundaries() {
        assert_eq!(normalized_vision_dimensions(1, 1, VISION_MAX_SIDE), (1, 1));
        assert_eq!(
            normalized_vision_dimensions(512, 512, VISION_MAX_SIDE),
            (512, 512)
        );
        assert_eq!(
            normalized_vision_dimensions(1024, 768, VISION_MAX_SIDE),
            (512, 384)
        );
        assert_eq!(
            normalized_vision_dimensions(2000, 100, VISION_MAX_SIDE),
            (512, 26)
        );
    }

    #[test]
    fn screenshots_are_sent_at_1024px() -> Result<(), Box<dyn std::error::Error>> {
        let screenshot = encode_rgb_test_image(ImageFormat::Png, 1080, 1920)?;
        let data_url = telegram_vision_data_url_from_bytes_with_max_side(
            &screenshot,
            DEFAULT_VISION_CAPTION_MAX_SIDE,
        )?;
        let decoded = image::load_from_memory(&decode_data_url_payload(&data_url)?)?;
        assert_eq!(decoded.dimensions(), (576, 1024));
        Ok(())
    }

    #[test]
    fn stickers_stay_at_512px() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            still_max_side("photo", DEFAULT_VISION_CAPTION_MAX_SIDE),
            DEFAULT_VISION_CAPTION_MAX_SIDE
        );
        let side = still_max_side("sticker", DEFAULT_VISION_CAPTION_MAX_SIDE);
        let sticker = encode_rgb_test_image(ImageFormat::Png, 1024, 1024)?;
        let data_url = telegram_vision_data_url_from_bytes_with_max_side(&sticker, side)?;
        let decoded = image::load_from_memory(&decode_data_url_payload(&data_url)?)?;
        assert_eq!(decoded.dimensions(), (512, 512));
        Ok(())
    }

    #[test]
    fn video_prompt_contains_duration_and_mmss_format() -> Result<(), Box<dyn std::error::Error>> {
        let mut mvhd = vec![0_u8; 100];
        mvhd[12..16].copy_from_slice(&1_000_u32.to_be_bytes());
        mvhd[16..20].copy_from_slice(&95_000_u32.to_be_bytes());
        let mut mvhd_box = (8 + mvhd.len() as u32).to_be_bytes().to_vec();
        mvhd_box.extend_from_slice(b"mvhd");
        mvhd_box.extend_from_slice(&mvhd);
        let mut clip = 16_u32.to_be_bytes().to_vec();
        clip.extend_from_slice(b"ftypisom\0\0\0\0");
        clip.extend_from_slice(&(8 + mvhd_box.len() as u32).to_be_bytes());
        clip.extend_from_slice(b"moov");
        clip.extend_from_slice(&mvhd_box);
        assert_eq!(mp4_duration_seconds(&clip), Some(95));

        let prompt = openplotva_prompts::render("vision/video_user", &video_prompt_data(Some(95)))?;
        assert!(prompt.contains("Длительность ролика: 01:35."), "{prompt}");
        assert!(prompt.contains("первая минута"), "{prompt}");
        assert!(prompt.contains("ММ:СС–ММ:СС"), "{prompt}");
        let short = openplotva_prompts::render("vision/video_user", &video_prompt_data(Some(24)))?;
        assert!(
            short.starts_with("Длительность ролика: 00:24.\nОпиши видео"),
            "{short}"
        );
        let unknown = openplotva_prompts::render("vision/video_user", &video_prompt_data(None))?;
        assert!(unknown.starts_with("Опиши видео"), "{unknown}");
        assert_eq!(mp4_duration_seconds(&[0, 0, 0]), None);
        Ok(())
    }

    #[test]
    fn h264_video_is_sent_with_its_duration() -> Result<(), Box<dyn std::error::Error>> {
        let data_url = format!(
            "data:video/mp4;base64,{}",
            BASE64_STANDARD.encode(mp4_clip(&[b"avc1", b"mp4a"], 7))
        );
        let captioner = AifarmVisionCaptioner::with_transport(
            AifarmVisionCaptionerConfig {
                client: AifarmClientConfig {
                    direct_url: "https://vision.example.test/v1/chat/completions".to_owned(),
                    default_model: "vision-model".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "vision-model".to_owned(),
                ..AifarmVisionCaptionerConfig::default()
            },
            DataUrlStub::default(),
            AifarmTransportStub::new(Vec::new()),
        );

        let request = captioner.request(
            &data_url,
            &TelegramVisionCaptionRequest {
                file_unique_id: "video-u".to_owned(),
                latest_file_id: "video-file".to_owned(),
                media_kind: "video".to_owned(),
                mime_type: Some("video/mp4".to_owned()),
            },
        )?;

        let parts = &request.messages[1].content_parts;
        assert_eq!(
            parts[0].video_url.as_ref().map(|video| video.url.as_str()),
            Some(data_url.as_str())
        );
        assert!(
            parts[1].text.starts_with("Длительность ролика: 00:07."),
            "{}",
            parts[1].text
        );
        Ok(())
    }

    #[test]
    fn refusal_is_replaced_by_placeholder() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            checked_caption("I'm sorry, but I can't help with that.".to_owned())?,
            VISION_UNAVAILABLE_CAPTION
        );
        assert_eq!(
            checked_caption("Извините, я не могу описать это изображение.".to_owned())?,
            VISION_UNAVAILABLE_CAPTION
        );
        assert_eq!(
            checked_caption("«Описание недоступно.»".to_owned())?,
            VISION_UNAVAILABLE_CAPTION
        );
        let quoted = "Тип: стикер — котик.\nТекст: «не могу»";
        assert_eq!(checked_caption(quoted.to_owned())?, quoted);
        Ok(())
    }

    #[test]
    fn caption_that_repeats_a_line_is_rejected_for_another_sample() {
        let looping = format!(
            "Тип: скриншот.\n{}",
            "Текст: | 1 | 2 | 3 | 4 |\n".repeat(VISION_RUNAWAY_LINE_REPEATS)
        );
        let error = checked_caption(looping).expect_err("a loop is rejected");
        assert!(matches!(error, AifarmVisionCaptionerError::Runaway));
        assert_eq!(
            vision_retryable_reason(&error),
            Some(FailureReason::ModelOutputRejected)
        );
        assert!(checked_caption("Тип: фото.\nТекст: нет".to_owned()).is_ok());
    }

    #[test]
    fn vision_source_dimensions_are_bounded_before_decode() {
        assert!(vision_source_dimensions_allowed(8_192, 8_192));
        assert!(!vision_source_dimensions_allowed(8_193, 8_192));
        assert!(!vision_source_dimensions_allowed(16_385, 1));
        assert!(!vision_source_dimensions_allowed(0, 512));
    }

    #[test]
    fn telegram_file_lookup_is_scoped_to_observed_chat() -> Result<(), Box<dyn std::error::Error>> {
        let record = file_record("unique", "latest", unix_time(1_700_000_000)?);

        assert!(telegram_file_seen_in_chat(&record, 42));
        assert!(!telegram_file_seen_in_chat(&record, 99));
        assert!(!telegram_file_seen_in_chat(&record, 0));
        Ok(())
    }

    #[test]
    fn telegram_understand_media_mime_accepts_go_registered_signatures() {
        let jpeg = telegram_understand_media_mime(&[0xff, 0xd8, 0xff, 0x00]).expect("jpeg mime");
        assert_eq!(jpeg, "image/jpeg");

        let png = telegram_understand_media_mime(b"\x89PNG\r\n\x1a\nbody").expect("png mime");
        assert_eq!(png, "image/png");

        let gif = telegram_understand_media_mime(b"GIF89abody").expect("gif mime");
        assert_eq!(gif, "image/gif");

        let webp = telegram_understand_media_mime(b"RIFF----WEBPbody").expect("webp mime");
        assert_eq!(webp, "image/webp");
    }

    #[test]
    fn telegram_vision_data_url_from_bytes_rejects_empty_or_unknown_payloads() {
        assert_eq!(
            telegram_vision_data_url_from_bytes(&[]),
            Err(TelegramVisionDataUrlBuildError::EmptyImageData)
        );
        assert_eq!(
            telegram_vision_data_url_from_bytes(b"not an image"),
            Err(TelegramVisionDataUrlBuildError::UnsupportedImageData)
        );
    }

    #[test]
    fn telegram_vision_video_data_url_preserves_container_and_enforces_limit() {
        let data_url =
            telegram_vision_video_data_url_from_bytes(b"small mp4 payload", Some("video/mp4"))
                .expect("video data url");
        assert!(data_url.starts_with("data:video/mp4;base64,"));
        assert_eq!(
            telegram_vision_video_data_url_from_bytes(&[], Some("video/mp4")),
            Err(TelegramVisionDataUrlBuildError::EmptyImageData)
        );
        assert_eq!(
            telegram_vision_video_data_url_from_bytes(
                &vec![0_u8; VISION_MAX_VIDEO_BYTES + 1],
                Some("video/mp4"),
            ),
            Err(TelegramVisionDataUrlBuildError::VideoTooLarge)
        );
    }

    #[tokio::test]
    async fn aifarm_vision_captioner_sends_go_openai_compatible_vlm_request()
    -> Result<(), Box<dyn std::error::Error>> {
        let data_urls = DataUrlStub::default();
        let transport = AifarmTransportStub::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            body:
                br#"{"choices":[{"message":{"role":"assistant","content":" caption from vlm "}}]}"#
                    .to_vec(),
            ..AifarmHttpResponse::default()
        })]);
        let probe = transport.clone();
        let captioner = AifarmVisionCaptioner::with_transport(
            AifarmVisionCaptionerConfig {
                client: AifarmClientConfig {
                    direct_url: "https://vision.example.test/v1/chat/completions".to_owned(),
                    default_model: "vision-model".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "vision-model".to_owned(),
                max_tokens: 123,
                temperature: 0.2,
                enable_thinking: Some(false),
            },
            data_urls.clone(),
            transport,
        );

        let result = captioner
            .caption_telegram_file(TelegramVisionCaptionRequest {
                file_unique_id: "photo-u".to_owned(),
                latest_file_id: "photo-file".to_owned(),
                media_kind: "photo".to_owned(),
                mime_type: None,
            })
            .await?;

        assert_eq!(result.caption, "caption from vlm");
        assert_eq!(data_urls.calls(), vec!["photo-file"]);
        let requests = probe.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, AifarmHttpMethod::Post);
        assert_eq!(
            requests[0].url,
            "https://vision.example.test/v1/chat/completions"
        );
        let body: serde_json::Value = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(body["model"], "vision-model");
        assert_eq!(body["stream"], false);
        assert_eq!(body["max_tokens"], 123);
        assert_eq!(body["temperature"], 0.2);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        assert_eq!(
            body["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            false
        );
        assert!(
            body["messages"][0]["content"]
                .as_str()
                .is_some_and(|text| text.contains("vision-модуль"))
        );
        assert!(
            body["messages"][1]["content"][1]["text"]
                .as_str()
                .is_some_and(|text| text.contains("Опиши изображение"))
        );
        assert_eq!(
            body["messages"][1]["content"][0]["image_url"]["url"],
            "data:image/jpeg;base64,photo-file"
        );
        assert_eq!(
            body["messages"][1]["content"][0]["image_url"]["detail"],
            "auto"
        );
        Ok(())
    }

    #[test]
    fn vision_requests_are_traced_as_their_own_flow() -> Result<(), Box<dyn std::error::Error>> {
        let captioner = AifarmVisionCaptioner::with_transport(
            AifarmVisionCaptionerConfig {
                client: AifarmClientConfig {
                    direct_url: "https://vision.example.test/v1/chat/completions".to_owned(),
                    default_model: "vision-model".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "vision-model".to_owned(),
                ..AifarmVisionCaptionerConfig::default()
            },
            DataUrlStub::default(),
            AifarmTransportStub::new(Vec::new()),
        );
        let photo = captioner.request(
            "data:image/jpeg;base64,photo",
            &TelegramVisionCaptionRequest {
                file_unique_id: "photo-u".to_owned(),
                latest_file_id: "photo-file".to_owned(),
                media_kind: "photo".to_owned(),
                mime_type: None,
            },
        )?;
        let video = captioner.request(
            "data:video/mp4;base64,video",
            &TelegramVisionCaptionRequest {
                file_unique_id: "video-u".to_owned(),
                latest_file_id: "video-file".to_owned(),
                media_kind: "video".to_owned(),
                mime_type: Some("video/mp4".to_owned()),
            },
        )?;

        let photo_tags = photo.trace.map(|trace| trace.tags).unwrap_or_default();
        assert_eq!(photo_tags.flow, "vision");
        assert_eq!(photo_tags.source, "aifarm_vision");
        assert_eq!(photo_tags.mode, "image");
        assert_eq!(photo_tags.request_kind, "openai.chat.completions");
        let video_tags = video.trace.map(|trace| trace.tags).unwrap_or_default();
        assert_eq!(video_tags.flow, "vision");
        assert_eq!(video_tags.mode, "video");
        Ok(())
    }

    #[tokio::test]
    async fn aifarm_vision_captioner_sends_video_before_instruction()
    -> Result<(), Box<dyn std::error::Error>> {
        let data_urls = DataUrlStub::default();
        let transport = AifarmTransportStub::new(vec![Ok(AifarmHttpResponse {
            status_code: 200,
            body:
                br#"{"choices":[{"message":{"role":"assistant","content":" visual timeline "}}]}"#
                    .to_vec(),
            ..AifarmHttpResponse::default()
        })]);
        let probe = transport.clone();
        let captioner = AifarmVisionCaptioner::with_transport(
            AifarmVisionCaptionerConfig {
                client: AifarmClientConfig {
                    direct_url: "https://vision.example.test/v1/chat/completions".to_owned(),
                    default_model: "vision-model".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "vision-model".to_owned(),
                max_tokens: 768,
                temperature: 0.1,
                enable_thinking: Some(false),
            },
            data_urls,
            transport,
        );

        let result = captioner
            .caption_telegram_file(TelegramVisionCaptionRequest {
                file_unique_id: "video-u".to_owned(),
                latest_file_id: "video-file".to_owned(),
                media_kind: "video".to_owned(),
                mime_type: Some("video/mp4".to_owned()),
            })
            .await?;

        assert_eq!(result.caption, "visual timeline");
        let body: serde_json::Value = serde_json::from_slice(&probe.requests()[0].body)?;
        assert_eq!(
            body["messages"][1]["content"][0]["video_url"]["url"],
            "data:video/mp4;base64,video-file"
        );
        assert!(
            body["messages"][1]["content"][1]["text"]
                .as_str()
                .is_some_and(|text| text.contains("ММ:СС–ММ:СС"))
        );
        assert!(body.to_string().contains("звук расшифровывают отдельно"));
        assert_eq!(body["max_tokens"], VISION_VIDEO_MIN_MAX_TOKENS);
        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], false);
        assert_eq!(
            body["extra_body"]["chat_template_kwargs"]["enable_thinking"],
            false
        );
        Ok(())
    }

    #[tokio::test]
    async fn av1_video_is_skipped_before_the_provider_call()
    -> Result<(), Box<dyn std::error::Error>> {
        let clip = mp4_clip(&[b"mp4a", b"av01"], 6);
        let transport = AifarmTransportStub::new(Vec::new());
        let probe = transport.clone();
        let captioner = AifarmVisionCaptioner::with_transport(
            AifarmVisionCaptionerConfig {
                client: AifarmClientConfig {
                    direct_url: "https://vision.example.test/v1/chat/completions".to_owned(),
                    default_model: "vision-model".to_owned(),
                    ..AifarmClientConfig::default()
                },
                model: "vision-model".to_owned(),
                ..AifarmVisionCaptionerConfig::default()
            },
            FixedDataUrl(format!(
                "data:video/mp4;base64,{}",
                BASE64_STANDARD.encode(&clip)
            )),
            transport,
        );

        let error = captioner
            .caption_telegram_file(TelegramVisionCaptionRequest {
                file_unique_id: "video-u".to_owned(),
                latest_file_id: "video-file".to_owned(),
                media_kind: "video".to_owned(),
                mime_type: Some("video/mp4".to_owned()),
            })
            .await
            .expect_err("an AV1 clip is not sent for a caption");

        assert!(
            matches!(error, AifarmVisionCaptionerError::Av1VideoUnsupported),
            "{error}"
        );
        assert!(probe.requests().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn aifarm_vision_captioner_sends_legacy_generate_payload_when_configured()
    -> Result<(), Box<dyn std::error::Error>> {
        let data_urls = DataUrlStub::default();
        let body = BASE64_STANDARD.encode(br#"{"caption":" legacy caption "}"#);
        let transport = AifarmTransportStub::new(vec![
            Ok(AifarmHttpResponse {
                status_code: 200,
                body: br#"{"job_id":"vision-job","state":"JOB_STATE_QUEUED"}"#.to_vec(),
                ..AifarmHttpResponse::default()
            }),
            Ok(AifarmHttpResponse {
                status_code: 200,
                body: format!(
                    r#"{{"job":{{"job_id":"vision-job","state":"JOB_STATE_SUCCEEDED","result":{{"response":{{"status_code":200,"body":"{body}","content_type":"application/json"}}}}}}}}"#
                )
                .into_bytes(),
                ..AifarmHttpResponse::default()
            }),
        ]);
        let probe = transport.clone();
        let captioner = AifarmVisionCaptioner::with_transport(
            AifarmVisionCaptionerConfig {
                client: AifarmClientConfig {
                    base_url: "https://discovery.example.test".to_owned(),
                    service_name: LEGACY_VISION_SERVICE_NAME.to_owned(),
                    endpoint_name: LEGACY_VISION_ENDPOINT_NAME.to_owned(),
                    poll_interval: Duration::from_millis(1),
                    task_timeout: Duration::from_secs(120),
                    capacity_wait: Duration::from_secs(60),
                    capacity_poll_interval: Duration::from_secs(1),
                    ..AifarmClientConfig::default()
                },
                model: "legacy-ignored".to_owned(),
                max_tokens: 123,
                temperature: 0.2,
                enable_thinking: None,
            },
            data_urls.clone(),
            transport,
        );

        let result = captioner
            .caption_telegram_file(TelegramVisionCaptionRequest {
                file_unique_id: "photo-u".to_owned(),
                latest_file_id: "photo-file".to_owned(),
                media_kind: "photo".to_owned(),
                mime_type: None,
            })
            .await?;

        assert_eq!(result.caption, "legacy caption");
        let requests = probe.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0].url,
            "https://discovery.example.test/v1/jobs/blocking"
        );
        let job: DiscoveryJobRequest = serde_json::from_slice(&requests[0].body)?;
        assert_eq!(job.invocation.service_name, LEGACY_VISION_SERVICE_NAME);
        assert_eq!(job.invocation.endpoint_name, LEGACY_VISION_ENDPOINT_NAME);
        assert_eq!(job.invocation.headers["X-AIFarm-Workload"], "vision");
        assert_eq!(job.invocation.timeout_ms, 120_000);
        assert_eq!(job.wait_for_capacity_ms, 60_000);
        let payload = BASE64_STANDARD.decode(job.invocation.body)?;
        let payload: serde_json::Value = serde_json::from_slice(&payload)?;
        assert_eq!(payload["image_mime"], "image/jpeg");
        assert_eq!(payload["image_b64"], "photo-file");
        assert!(payload.get("prompt").is_none());
        assert!(payload.get("model").is_none());
        assert!(payload.get("messages").is_none());
        Ok(())
    }

    #[test]
    fn aifarm_vision_captioner_config_maps_go_vision_env() -> Result<(), Box<dyn std::error::Error>>
    {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            discovery_base_url: Some("https://discovery.example.test".to_owned()),
            vision_discovery_service_name: Some("vision-service".to_owned()),
            vision_discovery_endpoint_name: Some("vision-endpoint".to_owned()),
            vision_model: Some("vision-model".to_owned()),
            vision_max_tokens: Some("345".to_owned()),
            vision_temperature: Some("0.25".to_owned()),
            vision_request_timeout_seconds: Some("42".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let cfg = aifarm_vision_captioner_config_from_app_config(&config);

        assert_eq!(cfg.client.base_url, "https://discovery.example.test");
        assert_eq!(cfg.client.service_name, "vision-service");
        assert_eq!(cfg.client.endpoint_name, "vision-endpoint");
        assert_eq!(cfg.client.default_model, "vision-model");
        assert_eq!(cfg.client.workload, AIFARM_VISION_WORKLOAD);
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(42));
        assert_eq!(cfg.client.task_timeout, Duration::from_secs(42));
        assert_eq!(cfg.client.capacity_wait, Duration::from_secs(21));
        assert_eq!(cfg.client.capacity_poll_interval, Duration::from_secs(1));
        assert_eq!(cfg.model, "vision-model");
        assert_eq!(cfg.max_tokens, 345);
        assert_eq!(cfg.temperature, 0.25);
        Ok(())
    }

    #[derive(Clone, Default)]
    struct FileStoreStub {
        records: Arc<Mutex<Vec<TelegramFileRecord>>>,
        updates: Arc<Mutex<Vec<TelegramFileVisionUpdate>>>,
    }

    impl FileStoreStub {
        fn with_records(records: Vec<TelegramFileRecord>) -> Self {
            Self {
                records: Arc::new(Mutex::new(records)),
                updates: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn updates(&self) -> Vec<TelegramFileVisionUpdate> {
            self.updates.lock().expect("updates").clone()
        }
    }

    impl TelegramFileVisionStore for FileStoreStub {
        type Error = StubError;

        fn get_file<'a>(
            &'a self,
            file_unique_id: &'a str,
        ) -> TelegramFileLookupFuture<'a, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .records
                    .lock()
                    .map_err(|_| StubError("records poisoned".to_owned()))?
                    .iter()
                    .find(|record| record.file_unique_id == file_unique_id)
                    .cloned())
            })
        }

        fn get_file_by_latest_file_id<'a>(
            &'a self,
            latest_file_id: &'a str,
        ) -> TelegramFileLookupFuture<'a, Self::Error> {
            Box::pin(async move {
                Ok(self
                    .records
                    .lock()
                    .map_err(|_| StubError("records poisoned".to_owned()))?
                    .iter()
                    .find(|record| record.latest_file_id == latest_file_id)
                    .cloned())
            })
        }

        fn upsert_metadata<'a>(
            &'a self,
            params: &'a TelegramFileMetadataUpsert,
        ) -> TelegramFileLookupFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut records = self
                    .records
                    .lock()
                    .map_err(|_| StubError("records poisoned".to_owned()))?;
                if let Some(record) = records
                    .iter_mut()
                    .find(|record| record.file_unique_id == params.file_unique_id)
                {
                    record.latest_file_id.clone_from(&params.latest_file_id);
                    return Ok(Some(record.clone()));
                }
                let now = OffsetDateTime::now_utc();
                let mut record = file_record(
                    params.file_unique_id.clone(),
                    params.latest_file_id.clone(),
                    now,
                );
                record.media_kind.clone_from(&params.media_kind);
                record.mime_type.clone_from(&params.mime_type);
                record.width = params.width;
                record.height = params.height;
                record.file_size = params.file_size;
                record.first_seen_chat_id = params.first_seen_chat_id;
                record.first_seen_message_id = params.first_seen_message_id;
                record.last_seen_chat_id = params.last_seen_chat_id;
                record.last_seen_message_id = params.last_seen_message_id;
                records.push(record.clone());
                Ok(Some(record))
            })
        }

        fn update_vision<'a>(
            &'a self,
            params: &'a TelegramFileVisionUpdate,
        ) -> TelegramFileVisionUpdateFuture<'a, Self::Error> {
            Box::pin(async move {
                self.updates
                    .lock()
                    .map_err(|_| StubError("updates poisoned".to_owned()))?
                    .push(params.clone());
                let mut records = self
                    .records
                    .lock()
                    .map_err(|_| StubError("records poisoned".to_owned()))?;
                let Some(record) = records
                    .iter_mut()
                    .find(|record| record.file_unique_id == params.file_unique_id)
                else {
                    return Err(StubError("missing record".to_owned()));
                };
                record.vision_status = params.vision_status.clone();
                if params.vision_caption.is_some() {
                    record.vision_caption = params.vision_caption.clone();
                }
                if params.vision_model.is_some() {
                    record.vision_model = params.vision_model.clone();
                }
                if params.vision_latency_ms.is_some() {
                    record.vision_latency_ms = params.vision_latency_ms;
                }
                if params.recognition_requested_at.is_some() {
                    record.recognition_requested_at = params.recognition_requested_at;
                }
                if params.recognition_completed_at.is_some() {
                    record.recognition_completed_at = params.recognition_completed_at;
                }
                Ok(record.clone())
            })
        }
    }

    #[derive(Clone)]
    struct CaptionerStub {
        result: Result<TelegramVisionCaptionResult, StubError>,
        calls: Arc<Mutex<Vec<TelegramVisionCaptionRequest>>>,
    }

    impl CaptionerStub {
        fn successful(caption: impl Into<String>, processing_time_seconds: f64) -> Self {
            Self {
                result: Ok(TelegramVisionCaptionResult {
                    caption: caption.into(),
                    processing_time_seconds,
                }),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn failed(message: impl Into<String>) -> Self {
            Self {
                result: Err(StubError(message.into())),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> Vec<TelegramVisionCaptionRequest> {
            self.calls.lock().expect("calls").clone()
        }
    }

    impl TelegramVisionCaptioner for CaptionerStub {
        type Error = StubError;

        fn caption_telegram_file<'a>(
            &'a self,
            request: TelegramVisionCaptionRequest,
        ) -> TelegramVisionCaptionFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls.lock().expect("calls").push(request);
                self.result.clone()
            })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct HistoryCall {
        chat_id: i64,
        message_id: i32,
        file_unique_id: String,
        caption: String,
    }

    #[derive(Clone, Default)]
    struct HistoryStoreStub {
        calls: Arc<Mutex<Vec<HistoryCall>>>,
    }

    impl HistoryStoreStub {
        fn calls(&self) -> Vec<HistoryCall> {
            self.calls.lock().expect("history calls").clone()
        }
    }

    impl VisionHistoryStore for HistoryStoreStub {
        type Error = StubError;

        fn upsert_vision_description<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            file_unique_id: &'a str,
            caption: &'a str,
        ) -> VisionHistoryUpdateFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls.lock().expect("history calls").push(HistoryCall {
                    chat_id,
                    message_id,
                    file_unique_id: file_unique_id.to_owned(),
                    caption: caption.to_owned(),
                });
                Ok(true)
            })
        }
    }

    #[derive(Clone, Default)]
    struct DataUrlStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl DataUrlStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("data url calls").clone()
        }
    }

    impl TelegramVisionDataUrlProvider for DataUrlStub {
        type Error = StubError;

        fn telegram_file_data_url<'a>(
            &'a self,
            latest_file_id: &'a str,
            media_kind: &'a str,
            mime_type: Option<&'a str>,
        ) -> TelegramVisionDataUrlFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("data url calls")
                    .push(latest_file_id.to_owned());
                if vision_media_is_video(media_kind, mime_type) {
                    Ok(format!("data:video/mp4;base64,{latest_file_id}"))
                } else {
                    Ok(format!("data:image/jpeg;base64,{latest_file_id}"))
                }
            })
        }
    }

    #[derive(Clone)]
    struct FixedDataUrl(String);

    impl TelegramVisionDataUrlProvider for FixedDataUrl {
        type Error = StubError;

        fn telegram_file_data_url<'a>(
            &'a self,
            _latest_file_id: &'a str,
            _media_kind: &'a str,
            _mime_type: Option<&'a str>,
        ) -> TelegramVisionDataUrlFuture<'a, Self::Error> {
            Box::pin(async move { Ok(self.0.clone()) })
        }
    }

    #[derive(Clone, Default)]
    struct AifarmTransportStub {
        state: Arc<Mutex<AifarmTransportState>>,
    }

    #[derive(Default)]
    struct AifarmTransportState {
        requests: Vec<AifarmHttpRequest>,
        responses: VecDeque<Result<AifarmHttpResponse, openplotva_llm::aifarm::CompletionError>>,
    }

    impl AifarmTransportStub {
        fn new(
            responses: Vec<Result<AifarmHttpResponse, openplotva_llm::aifarm::CompletionError>>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(AifarmTransportState {
                    requests: Vec::new(),
                    responses: VecDeque::from(responses),
                })),
            }
        }

        fn requests(&self) -> Vec<AifarmHttpRequest> {
            self.state.lock().expect("transport state").requests.clone()
        }
    }

    impl AifarmHttpTransport for AifarmTransportStub {
        fn send<'a>(&'a self, request: AifarmHttpRequest) -> AifarmHttpFuture<'a> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("transport state");
                state.requests.push(request);
                state
                    .responses
                    .pop_front()
                    .unwrap_or_else(|| Ok(AifarmHttpResponse::default()))
            })
        }
    }

    #[derive(Clone, Debug, Error, Eq, PartialEq)]
    #[error("{0}")]
    struct StubError(String);

    fn vision_request(file_id: &str) -> VisionDescribeRequest {
        VisionDescribeRequest {
            file_id: file_id.to_owned(),
            chat_id: 42,
            message_id: 77,
            thread_id: None,
            message_meta: ChatMessageMeta {
                attachments: vec![ChatAttachment {
                    kind: "image".to_owned(),
                    source: "message".to_owned(),
                    file_unique_id: "photo-u".to_owned(),
                    ..ChatAttachment::default()
                }],
                ..ChatMessageMeta::default()
            },
        }
    }

    fn dialog_input_with_images() -> DialogInput {
        DialogInput {
            context: DialogContext {
                chat_id: 42,
                ..DialogContext::default()
            },
            message: DialogMessage {
                id: 77,
                meta: ChatMessageMeta {
                    attachments: vec![
                        ChatAttachment {
                            kind: "image".to_owned(),
                            source: "message".to_owned(),
                            file_unique_id: "photo-u".to_owned(),
                            ..ChatAttachment::default()
                        },
                        ChatAttachment {
                            kind: "image".to_owned(),
                            source: "quoted".to_owned(),
                            file_unique_id: "quoted-u".to_owned(),
                            ..ChatAttachment::default()
                        },
                    ],
                    ..ChatMessageMeta::default()
                },
                ..DialogMessage::default()
            },
            ..DialogInput::default()
        }
    }

    fn file_record(
        file_unique_id: impl Into<String>,
        latest_file_id: impl Into<String>,
        now: OffsetDateTime,
    ) -> TelegramFileRecord {
        TelegramFileRecord {
            file_unique_id: file_unique_id.into(),
            latest_file_id: latest_file_id.into(),
            media_kind: "photo".to_owned(),
            mime_type: None,
            width: Some(1024),
            height: Some(768),
            file_size: Some(1000),
            first_seen_chat_id: Some(42),
            first_seen_message_id: Some(77),
            last_seen_chat_id: Some(42),
            last_seen_message_id: Some(77),
            last_seen_at: now,
            vision_status: TELEGRAM_FILE_VISION_STATUS_FAILED.to_owned(),
            vision_caption: None,
            vision_model: None,
            vision_latency_ms: None,
            recognition_requested_at: None,
            recognition_completed_at: None,
            asr_status: openplotva_storage::TELEGRAM_FILE_ASR_STATUS_PENDING.to_owned(),
            asr_text: None,
            asr_provider: None,
            asr_model: None,
            asr_latency_ms: None,
            asr_fallback_used: None,
            asr_chunks: None,
            asr_warnings: None,
            asr_error: None,
            asr_requested_at: None,
            asr_completed_at: None,
            extra: json!({}),
            created_at: now,
            updated_at: now,
        }
    }

    trait FileRecordTestExt {
        fn with_status(self, status: &str) -> Self;
        fn with_caption(self, caption: &str) -> Self;
        fn with_requested_at(self, requested_at: OffsetDateTime) -> Self;
    }

    impl FileRecordTestExt for TelegramFileRecord {
        fn with_status(mut self, status: &str) -> Self {
            self.vision_status = status.to_owned();
            self
        }

        fn with_caption(mut self, caption: &str) -> Self {
            self.vision_caption = Some(caption.to_owned());
            self
        }

        fn with_requested_at(mut self, requested_at: OffsetDateTime) -> Self {
            self.recognition_requested_at = Some(requested_at);
            self
        }
    }

    fn unix_time(seconds: i64) -> Result<OffsetDateTime, time::error::ComponentRange> {
        OffsetDateTime::from_unix_timestamp(seconds)
    }

    fn encode_rgb_test_image(
        format: ImageFormat,
        width: u32,
        height: u32,
    ) -> Result<Vec<u8>, image::ImageError> {
        let image = RgbImage::from_fn(width, height, |x, y| {
            image::Rgb([(x % 255) as u8, (y % 255) as u8, 128])
        });
        let dynamic = DynamicImage::ImageRgb8(image);
        let mut out = Vec::new();
        match format {
            ImageFormat::Jpeg => {
                let mut encoder = image::codecs::jpeg::JpegEncoder::new_with_quality(&mut out, 90);
                encoder.encode_image(&dynamic)?;
            }
            other => {
                dynamic.write_to(&mut Cursor::new(&mut out), other)?;
            }
        }
        Ok(out)
    }

    fn mp4_box(box_type: &[u8; 4], payload: &[u8]) -> Vec<u8> {
        let size = u32::try_from(8 + payload.len()).expect("test box size fits u32");
        let mut out = size.to_be_bytes().to_vec();
        out.extend_from_slice(box_type);
        out.extend_from_slice(payload);
        out
    }

    /// `ftyp`, `moov` and `mdat` of a clip lasting `seconds`, with one track
    /// per sample entry type laid out as an encoder writes it.
    fn mp4_clip(sample_entries: &[&[u8; 4]], seconds: u32) -> Vec<u8> {
        let mut mvhd = vec![0_u8; 100];
        mvhd[12..16].copy_from_slice(&1_000_u32.to_be_bytes());
        mvhd[16..20].copy_from_slice(&(seconds * 1_000).to_be_bytes());
        let mut moov = mp4_box(b"mvhd", &mvhd);
        for sample_entry in sample_entries {
            let mut stsd = vec![0, 0, 0, 0, 0, 0, 0, 1];
            stsd.extend_from_slice(&mp4_box(sample_entry, &[0_u8; 78]));
            let mut stbl = mp4_box(b"stsd", &stsd);
            stbl.extend_from_slice(&mp4_box(b"stts", &[0_u8; 8]));
            let mut minf = mp4_box(b"dinf", &[0_u8; 28]);
            minf.extend_from_slice(&mp4_box(b"stbl", &stbl));
            let mut mdia = mp4_box(b"mdhd", &[0_u8; 24]);
            mdia.extend_from_slice(&mp4_box(b"hdlr", &[0_u8; 25]));
            mdia.extend_from_slice(&mp4_box(b"minf", &minf));
            let mut trak = mp4_box(b"tkhd", &[0_u8; 84]);
            trak.extend_from_slice(&mp4_box(b"mdia", &mdia));
            moov.extend_from_slice(&mp4_box(b"trak", &trak));
        }
        let mut clip = mp4_box(b"ftyp", b"isom\0\0\0\0");
        clip.extend_from_slice(&mp4_box(b"moov", &moov));
        clip.extend_from_slice(&mp4_box(b"mdat", &[0_u8; 32]));
        clip
    }

    fn decode_data_url_payload(data_url: &str) -> Result<Vec<u8>, base64::DecodeError> {
        let (_, payload) = data_url
            .split_once(";base64,")
            .expect("test data URL should contain base64 marker");
        BASE64_STANDARD.decode(payload)
    }
}
