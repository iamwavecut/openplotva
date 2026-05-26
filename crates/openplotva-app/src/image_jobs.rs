//! App-level image generation task execution.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose};
use openplotva_config::{AppConfig, DEFAULT_AIHORDE_MODEL};
use openplotva_core::ChatMessageMeta;
use openplotva_dialog::sanitize_tool_text;
use openplotva_llm::aifarm::{
    AifarmHttpMethod, AifarmHttpRequest, AifarmHttpTransport, DiscoveryInvocation,
    DiscoveryJobEnvelope, DiscoveryJobRequest, DiscoveryJobResult, ReqwestAifarmTransport,
    decode_discovery_body, is_failure_status, is_queued_status, is_running_status,
    is_success_status, parse_job_error,
};
use openplotva_media::aihorde::{AIHordeClient, AIHordeConfig, AIHordeRequest};
use openplotva_media::modelscope::{ModelScopeClient, ModelScopeConfig, ModelScopeGenerateRequest};
use openplotva_media::pruna::{PrunaClient, PrunaConfig, PrunaRequest};
use openplotva_media::together::{
    TogetherClient, TogetherConfig, TogetherError, TogetherImageRequest,
};
use openplotva_taskman::{
    DEFAULT_LLM_JOB_MAX_ATTEMPTS, IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME,
    ImageEditJobParams, ImageGenJobParams, InMemoryTaskQueue, JobType,
    LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE, TaskQueueError, TaskQueueJobEvent,
    TaskQueueWorkItem, image_edit_job_params_from_stateless_job,
    image_gen_job_params_from_stateless_job,
};
use openplotva_telegram::{
    ChatRef, DeleteMessageRequest, ReplyMessageRef, StickerMessageRequest, TelegramOutboundMethod,
    TelegramOutboundResponse, TextMessageRequest, build_delete_message_method,
    build_sticker_message_method, build_text_message_methods, execute_telegram_method,
};
use serde::Serialize;
use serde_json::Value;
use time::OffsetDateTime;
use tokio::time::Instant;

use crate::media::{MediaPromptOptimizer, MediaPromptOptimizerService};
use crate::vision::TelegramVisionDataUrlProvider;

/// Boxed future returned by image generation providers.
pub type ImageGenerationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ImageGenerationResult, ImageGenerationError>> + Send + 'a>>;
/// Boxed future returned by image edit providers.
pub type ImageEditFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ImageEditResult, ImageEditError>> + Send + 'a>>;

/// Boxed future returned by image job side effects.
pub type ImageJobEffectFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub const AIFARM_DRAW_API_SERVICE_NAME: &str = "draw-api";
pub const AIFARM_DRAW_API_ENDPOINT_NAME: &str = "generate";
pub const AIFARM_DRAW_API_DEFAULT_BASE_URL: &str = "http://127.0.0.1:50051";
pub const AIFARM_DRAW_API_DEFAULT_TIMEOUT: StdDuration = StdDuration::from_secs(600);
pub const AIFARM_DRAW_API_DEFAULT_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub const STICKER_DRAW_FILE_ID: &str =
    "CAACAgIAAxkBAAEeRZ5kDllSPdVQ_-kGLny406MDN5dDvAACCisAAksYcEhs6T-nxUJBVy8E";
pub const STICKER_DOWN_FILE_ID: &str =
    "CAACAgIAAxkBAAEeROBkDjnz1i3WxxyNLBgWA_IKyjxbnQACuioAAqPicEh1C96_WINTHS8E";
pub const DRAWING_STICKER_DELETE_AFTER: StdDuration = StdDuration::from_secs(30);
pub const NSFW_BLOCKED_MESSAGE_TEXT: &str = "Ваш запрос заблокирован, так как содержит неприемлемый контент. Попробуйте переформулировать запрос.";
pub const IMAGE_JOB_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub const IMAGE_JOB_WORKER_QUEUES: [&str; 2] = [IMAGE_VIP_QUEUE_NAME, IMAGE_REGULAR_QUEUE_NAME];
pub const IMAGE_VIP_JOB_WORKER_QUEUES: [&str; 1] = [IMAGE_VIP_QUEUE_NAME];
pub const IMAGE_REGULAR_JOB_WORKER_QUEUES: [&str; 1] = [IMAGE_REGULAR_QUEUE_NAME];

const IMAGE_REGULAR_WORKER_ID: &str = "image-regular-worker";
const IMAGE_VIP_WORKER_ID: &str = "image-vip-worker";

/// Provider-neutral image generation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageGenerationRequest {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Source message ID.
    pub message_id: i32,
    /// Caller user ID.
    pub user_id: i64,
    /// Caller display name.
    pub user_full_name: String,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
    /// Effective image prompt.
    pub prompt: String,
    pub caption_text: String,
    /// Optimized prompt variants.
    pub prompt_variants: Vec<String>,
    pub is_nsfw: bool,
    /// Raw negative prompt.
    pub negative_prompt: String,
    /// Raw aspect ratio.
    pub aspect_ratio: String,
    /// Raw seed.
    pub seed: String,
}

/// Provider-neutral image-edit request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageEditRequest {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Source control message ID.
    pub message_id: i32,
    /// Caller user ID.
    pub user_id: i64,
    /// Caller display name.
    pub user_full_name: String,
    /// Forum topic ID.
    pub thread_id: Option<i32>,
    /// Sanitized edit instruction.
    pub prompt: String,
    pub photo_file_id: String,
    pub photo_urls: Vec<String>,
}

/// Successful provider result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageGenerationResult {
    pub image_url: String,
    /// Generated image URLs returned by the draw provider.
    pub image_urls: Vec<String>,
    /// Generated image bytes returned by the draw provider.
    pub image_bytes: Vec<Vec<u8>>,
}

impl ImageGenerationResult {
    /// First non-empty image URL, preserving the legacy single-URL field first.
    #[must_use]
    pub fn first_image_url(&self) -> Option<String> {
        non_empty(self.image_url.clone()).or_else(|| {
            self.image_urls
                .iter()
                .find_map(|value| non_empty(value.clone()))
        })
    }
}

/// Successful image-edit provider result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageEditResult {
    /// URLs produced by a concrete edit provider, if it reports them.
    pub image_urls: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageGenerationError {
    Forbidden,
    /// Any provider/runtime error that should fail the taskman job.
    Provider(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImageEditError {
    Forbidden,
    /// Any provider/runtime error that should fail the taskman job.
    Provider(String),
}

impl From<ImageGenerationError> for ImageEditError {
    fn from(error: ImageGenerationError) -> Self {
        match error {
            ImageGenerationError::Forbidden => Self::Forbidden,
            ImageGenerationError::Provider(message) => Self::Provider(message),
        }
    }
}

impl ImageEditError {
    fn message(&self) -> String {
        match self {
            Self::Forbidden => "image edit blocked by safety policy".to_owned(),
            Self::Provider(message) => message.trim().to_owned(),
        }
    }
}

impl ImageGenerationError {
    fn message(&self) -> String {
        match self {
            Self::Forbidden => "forbidden".to_owned(),
            Self::Provider(message) => message.clone(),
        }
    }
}

/// Concrete AIFarm draw-api client config.
#[derive(Clone, Debug, PartialEq)]
pub struct AifarmDrawApiConfig {
    /// Discovery base URL.
    pub base_url: String,
    /// Upstream draw task timeout.
    pub timeout: StdDuration,
    /// Poll interval for `/v1/jobs/{id}`.
    pub poll_interval: StdDuration,
}

impl Default for AifarmDrawApiConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            timeout: StdDuration::ZERO,
            poll_interval: StdDuration::ZERO,
        }
    }
}

impl AifarmDrawApiConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.base_url = if self.base_url.trim().is_empty() {
            AIFARM_DRAW_API_DEFAULT_BASE_URL.to_owned()
        } else {
            self.base_url.trim().to_owned()
        };
        if self.timeout == StdDuration::ZERO {
            self.timeout = AIFARM_DRAW_API_DEFAULT_TIMEOUT;
        }
        if self.poll_interval == StdDuration::ZERO {
            self.poll_interval = AIFARM_DRAW_API_DEFAULT_POLL_INTERVAL;
        }
        self
    }

    /// Discovery endpoint URL.
    #[must_use]
    pub fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.base_url.trim_end_matches('/'), path)
    }
}

/// Build draw-api config from app config.
#[must_use]
pub fn aifarm_draw_api_config_from_app_config(config: &AppConfig) -> AifarmDrawApiConfig {
    AifarmDrawApiConfig {
        base_url: config.llm.discovery.base_url.clone(),
        ..AifarmDrawApiConfig::default()
    }
    .with_defaults()
}

/// Build Pruna config from app config.
#[must_use]
pub fn pruna_config_from_app_config(config: &AppConfig) -> PrunaConfig {
    PrunaConfig {
        endpoint: config.pruna.endpoint.clone(),
        model: config.pruna.model.clone(),
        api_key: config.pruna.api_key.clone(),
        bearer: config.pruna.bearer.clone(),
        timeout: duration_seconds_or_zero(config.pruna.timeout_seconds),
    }
    .with_defaults()
}

/// Build ModelScope config from app config.
#[must_use]
pub fn modelscope_config_from_app_config(config: &AppConfig) -> ModelScopeConfig {
    ModelScopeConfig {
        api_key: config.model_scope.key.clone(),
        base_url: config.model_scope.base_url.clone(),
        poll_interval: duration_seconds_or_zero(config.model_scope.poll_interval_seconds),
        request_timeout: StdDuration::ZERO,
    }
    .with_defaults()
}

/// Build Together image-provider config from app config.
#[must_use]
pub fn together_config_from_app_config(config: &AppConfig) -> TogetherConfig {
    let api_key = non_empty(config.together.key.clone()).unwrap_or_else(|| {
        config
            .together
            .keys
            .iter()
            .find_map(|key| non_empty(key.clone()))
            .unwrap_or_default()
    });
    TogetherConfig {
        api_key,
        rate_limit_duration: duration_seconds_or_zero(config.together.rate_limit_seconds),
        ..TogetherConfig::default()
    }
    .with_defaults()
}

/// Build AIHorde image-provider config from app config.
#[must_use]
pub fn aihorde_config_from_app_config(config: &AppConfig) -> AIHordeConfig {
    AIHordeConfig {
        api_key: config.ai_horde.api_key.clone(),
        base_url: config.ai_horde.base_url.clone(),
        client_agent: config.ai_horde.client_agent.clone(),
        request_timeout: duration_seconds_or_zero(config.ai_horde.timeout_seconds),
        poll_interval: duration_seconds_or_zero(config.ai_horde.poll_interval_seconds),
    }
    .with_defaults()
}

/// Generated draw-api result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrawApiGenerateResult {
    /// Decoded image bytes from `image_b64`.
    pub images: Vec<Vec<u8>>,
    /// Image URLs from `image_url`.
    pub urls: Vec<String>,
}

/// AIFarm draw-api image generator.
#[derive(Clone, Debug)]
pub struct AifarmDrawApiImageGenerator<T = ReqwestAifarmTransport> {
    cfg: AifarmDrawApiConfig,
    transport: T,
}

impl AifarmDrawApiImageGenerator<ReqwestAifarmTransport> {
    /// Build a reqwest-backed draw-api generator.
    #[must_use]
    pub fn new(cfg: AifarmDrawApiConfig) -> Self {
        Self {
            cfg: cfg.with_defaults(),
            transport: ReqwestAifarmTransport::default(),
        }
    }
}

impl<T> AifarmDrawApiImageGenerator<T>
where
    T: AifarmHttpTransport,
{
    /// Build with a custom HTTP transport.
    #[must_use]
    pub fn with_transport(cfg: AifarmDrawApiConfig, transport: T) -> Self {
        Self {
            cfg: cfg.with_defaults(),
            transport,
        }
    }

    pub async fn generate_image_with_job_id(
        &self,
        request: ImageGenerationRequest,
        job_id: &str,
    ) -> Result<ImageGenerationResult, ImageGenerationError> {
        if request.caption_text.trim().is_empty() {
            return Ok(ImageGenerationResult::default());
        }
        let prompt = draw_api_prompt_text(&request);
        if prompt.trim().is_empty() {
            return Ok(ImageGenerationResult::default());
        }
        let payload = draw_api_payload(&prompt, &[])
            .map_err(|err| ImageGenerationError::Provider(err.to_string()))?;
        let job_id = self.submit_draw_api_job(job_id, payload).await?;
        let result = self.wait_draw_api_job(&job_id).await?;
        if result.images.is_empty() && result.urls.is_empty() {
            return Err(ImageGenerationError::Provider(
                "job completed but produced no images".to_owned(),
            ));
        }
        Ok(ImageGenerationResult {
            image_url: result.urls.first().cloned().unwrap_or_default(),
            image_urls: result.urls,
            image_bytes: result.images,
        })
    }

    pub async fn edit_image_with_job_id(
        &self,
        request: ImageEditRequest,
        job_id: &str,
    ) -> Result<ImageEditResult, ImageEditError> {
        let prompt = request.prompt.trim().to_owned();
        if prompt.is_empty() {
            return Err(ImageEditError::Provider(
                "image edit prompt is empty".to_owned(),
            ));
        }
        let image_inputs = normalized_image_edit_inputs(&request);
        if image_inputs.is_empty() {
            return Err(ImageEditError::Provider(
                "image edit requires image input".to_owned(),
            ));
        }
        let payload = draw_api_payload(&prompt, &image_inputs)
            .map_err(|err| ImageEditError::Provider(err.to_string()))?;
        let job_id = self
            .submit_draw_api_job(job_id, payload)
            .await
            .map_err(ImageEditError::from)?;
        let result = self
            .wait_draw_api_job(&job_id)
            .await
            .map_err(ImageEditError::from)?;
        if result.images.is_empty() && result.urls.is_empty() {
            return Err(ImageEditError::Provider(
                "job completed but produced no images".to_owned(),
            ));
        }
        Ok(ImageEditResult {
            image_urls: result.urls,
        })
    }

    async fn submit_draw_api_job(
        &self,
        job_id: &str,
        payload: Vec<u8>,
    ) -> Result<String, ImageGenerationError> {
        let job_id = if job_id.trim().is_empty() {
            generated_draw_job_id()
        } else {
            job_id.trim().to_owned()
        };
        let request = DiscoveryJobRequest {
            invocation: DiscoveryInvocation {
                service_name: AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
                endpoint_name: AIFARM_DRAW_API_ENDPOINT_NAME.to_owned(),
                headers: [("X-Request-Id".to_owned(), job_id.clone())].into(),
                query: BTreeMap::new(),
                body: general_purpose::STANDARD.encode(payload),
                content_type: "application/json".to_owned(),
                timeout_ms: duration_ms(self.cfg.timeout),
            },
            idempotency_key: job_id.clone(),
            priority: 0,
            wait_for_capacity_ms: 0,
            capacity_poll_ms: 0,
        };
        let body = serde_json::to_vec(&request)
            .map_err(|err| ImageGenerationError::Provider(err.to_string()))?;
        let response = self
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Post,
                url: self.cfg.endpoint("/v1/jobs"),
                headers: [("Content-Type".to_owned(), "application/json".to_owned())].into(),
                body,
            })
            .await
            .map_err(|err| ImageGenerationError::Provider(err.to_string()))?;
        if !(200..300).contains(&response.status_code) {
            return Err(ImageGenerationError::Provider(format!(
                "generation request failed: status {}: {}",
                response.status_code,
                String::from_utf8_lossy(&response.body).trim()
            )));
        }
        let envelope =
            serde_json::from_slice::<DiscoveryJobEnvelope>(&response.body).map_err(|err| {
                ImageGenerationError::Provider(format!("generation request failed: {err}"))
            })?;
        let job = envelope.resolve_job();
        let error = parse_job_error(job.error.as_ref());
        if !error.is_empty() {
            return Err(classify_draw_api_error(format!(
                "generation request failed: job submission failed: {error}"
            )));
        }
        Ok(non_empty(job.resolved_id()).unwrap_or(job_id))
    }

    async fn wait_draw_api_job(
        &self,
        job_id: &str,
    ) -> Result<DrawApiGenerateResult, ImageGenerationError> {
        let deadline = Instant::now() + self.cfg.timeout;
        loop {
            let status = self.check_draw_api_job(job_id).await?;
            match evaluate_draw_api_status(&status) {
                DrawApiWaitDecision::Done(result) => return Ok(result),
                DrawApiWaitDecision::Failed(message) => {
                    return Err(classify_draw_api_error(format!("job failed: {message}")));
                }
                DrawApiWaitDecision::Continue => {}
            }
            if Instant::now() >= deadline {
                return Err(ImageGenerationError::Provider(format!(
                    "timeout waiting for job {job_id}"
                )));
            }
            tokio::time::sleep(self.cfg.poll_interval).await;
        }
    }

    async fn check_draw_api_job(
        &self,
        job_id: &str,
    ) -> Result<DrawApiJobStatus, ImageGenerationError> {
        let job_id = job_id.trim();
        if job_id.is_empty() {
            return Err(ImageGenerationError::Provider("job ID is empty".to_owned()));
        }
        let response = self
            .transport
            .send(AifarmHttpRequest {
                method: AifarmHttpMethod::Get,
                url: self.cfg.endpoint(&format!("/v1/jobs/{job_id}")),
                headers: BTreeMap::new(),
                body: Vec::new(),
            })
            .await
            .map_err(|err| ImageGenerationError::Provider(err.to_string()))?;
        if !(200..300).contains(&response.status_code) {
            return Err(ImageGenerationError::Provider(format!(
                "status {}: {}",
                response.status_code,
                String::from_utf8_lossy(&response.body).trim()
            )));
        }
        let envelope =
            serde_json::from_slice::<DiscoveryJobEnvelope>(&response.body).map_err(|err| {
                ImageGenerationError::Provider(format!("decode draw job status response: {err}"))
            })?;
        draw_api_status_from_envelope(job_id, &envelope)
    }
}

impl<T> ImageGenerator for AifarmDrawApiImageGenerator<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            self.generate_image_with_job_id(request, &generated_draw_job_id())
                .await
        })
    }
}

impl<T> ImageEditor for AifarmDrawApiImageGenerator<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            self.edit_image_with_job_id(request, &generated_draw_job_id())
                .await
        })
    }
}

#[derive(Clone, Debug)]
pub struct PrunaImageGenerator {
    client: PrunaClient,
}

impl PrunaImageGenerator {
    /// Build a Pruna image generator.
    pub fn new(config: PrunaConfig) -> Result<Self, ImageGenerationError> {
        Ok(Self {
            client: PrunaClient::new(config)
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?,
        })
    }
}

impl ImageGenerator for PrunaImageGenerator {
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let prompt = draw_api_prompt_text(&request);
            if prompt.trim().is_empty() {
                return Ok(ImageGenerationResult::default());
            }
            let result = self
                .client
                .generate(PrunaRequest {
                    prompt,
                    aspect_ratio: request.aspect_ratio,
                    visitor_id: String::new(),
                })
                .await
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?;
            Ok(ImageGenerationResult {
                image_url: result.url.clone(),
                image_urls: (!result.url.is_empty())
                    .then_some(result.url)
                    .into_iter()
                    .collect(),
                image_bytes: (!result.image.is_empty())
                    .then_some(result.image)
                    .into_iter()
                    .collect(),
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct TogetherImageGenerator {
    client: TogetherClient,
}

impl TogetherImageGenerator {
    /// Build a Together image generator.
    pub fn new(config: TogetherConfig) -> Result<Self, ImageGenerationError> {
        Ok(Self {
            client: TogetherClient::new(config)
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?,
        })
    }
}

impl ImageGenerator for TogetherImageGenerator {
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let prompt = draw_api_prompt_text(&request);
            if prompt.trim().is_empty() {
                return Ok(ImageGenerationResult::default());
            }
            let (width, height) = image_dimensions_from_aspect_ratio(&request.aspect_ratio, 1024);
            let result = self
                .client
                .images_generate(TogetherImageRequest {
                    prompt,
                    seed: parse_image_seed(&request.seed),
                    width: Some(width),
                    height: Some(height),
                    negative_prompt: request.negative_prompt,
                    ..TogetherImageRequest::default()
                })
                .await
                .map_err(together_generation_error)?;
            Ok(ImageGenerationResult {
                image_url: result.url.clone(),
                image_urls: (!result.url.is_empty())
                    .then_some(result.url)
                    .into_iter()
                    .collect(),
                image_bytes: Vec::new(),
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct ModelScopeImageGenerator {
    client: ModelScopeClient,
}

impl ModelScopeImageGenerator {
    /// Build a ModelScope image generator.
    pub fn new(config: ModelScopeConfig) -> Result<Self, ImageGenerationError> {
        Ok(Self {
            client: ModelScopeClient::new(config)
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?,
        })
    }
}

impl ImageGenerator for ModelScopeImageGenerator {
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let prompt = draw_api_prompt_text(&request);
            if prompt.trim().is_empty() {
                return Ok(ImageGenerationResult::default());
            }
            let result = self
                .client
                .generate(ModelScopeGenerateRequest {
                    model: String::new(),
                    prompt,
                    poll_interval: StdDuration::ZERO,
                })
                .await
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?;
            Ok(ImageGenerationResult {
                image_bytes: result.images,
                ..ImageGenerationResult::default()
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct AIHordeImageGenerator {
    client: AIHordeClient,
    model: String,
}

impl AIHordeImageGenerator {
    /// Build an AIHorde image generator.
    pub fn new(config: AIHordeConfig, model: String) -> Result<Self, ImageGenerationError> {
        Ok(Self {
            client: AIHordeClient::new(config)
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?,
            model: non_empty(model).unwrap_or_else(|| DEFAULT_AIHORDE_MODEL.to_owned()),
        })
    }
}

impl ImageGenerator for AIHordeImageGenerator {
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let prompt = draw_api_prompt_text(&request);
            if prompt.trim().is_empty() {
                return Ok(ImageGenerationResult::default());
            }
            let (width, height) = image_dimensions_from_aspect_ratio(&request.aspect_ratio, 576);
            let result = self
                .client
                .generate(AIHordeRequest {
                    prompt,
                    negative_prompt: request.negative_prompt,
                    width,
                    height,
                    seed: parse_image_seed(&request.seed).unwrap_or_default(),
                    models: vec![self.model.clone()],
                    nsfw: request.is_nsfw,
                    censor_nsfw: !request.is_nsfw,
                    ..AIHordeRequest::default()
                })
                .await
                .map_err(|error| ImageGenerationError::Provider(error.to_string()))?;
            Ok(ImageGenerationResult {
                image_bytes: result.images,
                ..ImageGenerationResult::default()
            })
        })
    }
}

#[derive(Clone, Debug)]
pub struct FallbackImageGenerator<Primary, Fallback> {
    primary: Primary,
    fallback: Fallback,
}

impl<Primary, Fallback> FallbackImageGenerator<Primary, Fallback> {
    /// Build a fallback generator.
    #[must_use]
    pub const fn new(primary: Primary, fallback: Fallback) -> Self {
        Self { primary, fallback }
    }
}

impl<Primary, Fallback> ImageGenerator for FallbackImageGenerator<Primary, Fallback>
where
    Primary: ImageGenerator + Sync,
    Fallback: ImageGenerator + Sync,
{
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            match self.primary.generate_image(request.clone()).await {
                Ok(result) if image_generation_result_has_images(&result) => Ok(result),
                Ok(_) | Err(ImageGenerationError::Provider(_)) => {
                    self.fallback.generate_image(request).await
                }
                Err(ImageGenerationError::Forbidden) => Err(ImageGenerationError::Forbidden),
            }
        })
    }
}

/// Image editor wrapper that resolves Telegram `file_id` inputs before draw-api submission.
#[derive(Clone, Debug)]
pub struct ResolvingImageEditor<DataUrl, Editor> {
    data_urls: DataUrl,
    editor: Editor,
}

impl<DataUrl, Editor> ResolvingImageEditor<DataUrl, Editor> {
    /// Build a resolving image editor.
    #[must_use]
    pub fn new(data_urls: DataUrl, editor: Editor) -> Self {
        Self { data_urls, editor }
    }
}

impl<DataUrl, Editor> ImageEditor for ResolvingImageEditor<DataUrl, Editor>
where
    DataUrl: TelegramVisionDataUrlProvider + Send + Sync,
    DataUrl::Error: fmt::Display,
    Editor: ImageEditor + Sync,
{
    fn edit_image<'a>(&'a self, mut request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            if normalized_image_edit_inputs(&request).is_empty()
                && !request.photo_file_id.trim().is_empty()
            {
                let data_url = self
                    .data_urls
                    .telegram_file_data_url(&request.photo_file_id)
                    .await
                    .map_err(|error| {
                        ImageEditError::Provider(format!("resolve image edit file: {error}"))
                    })?;
                let base64 = image_b64_from_data_url(&data_url).ok_or_else(|| {
                    ImageEditError::Provider("resolved image edit data URL is empty".to_owned())
                })?;
                request.photo_urls.push(base64);
            }
            self.editor.edit_image(request).await
        })
    }
}

pub trait ImageGenerator {
    /// Generate and send an image.
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a>;
}

pub trait ImageEditor {
    /// Edit and send an image.
    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a>;
}

#[derive(Clone, Debug)]
pub struct OptimizingImageGenerator<Generator, Optimizer> {
    generator: Generator,
    optimizer: MediaPromptOptimizerService<Optimizer>,
}

impl<Generator, Optimizer> OptimizingImageGenerator<Generator, Optimizer> {
    /// Build an optimizing generator around an existing provider.
    #[must_use]
    pub const fn new(
        generator: Generator,
        optimizer: MediaPromptOptimizerService<Optimizer>,
    ) -> Self {
        Self {
            generator,
            optimizer,
        }
    }
}

impl<Generator, Optimizer> ImageGenerator for OptimizingImageGenerator<Generator, Optimizer>
where
    Generator: ImageGenerator + Sync,
    Optimizer: MediaPromptOptimizer,
{
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let request = optimized_image_generation_request(&self.optimizer, request).await?;
            self.generator.generate_image(request).await
        })
    }
}

#[derive(Clone, Debug)]
pub struct OptimizingImageEditor<Editor, Optimizer> {
    editor: Editor,
    optimizer: MediaPromptOptimizerService<Optimizer>,
}

impl<Editor, Optimizer> OptimizingImageEditor<Editor, Optimizer> {
    /// Build an optimizing editor around an existing provider.
    #[must_use]
    pub const fn new(editor: Editor, optimizer: MediaPromptOptimizerService<Optimizer>) -> Self {
        Self { editor, optimizer }
    }
}

impl<Editor, Optimizer> ImageEditor for OptimizingImageEditor<Editor, Optimizer>
where
    Editor: ImageEditor + Sync,
    Optimizer: MediaPromptOptimizer,
{
    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            let request = optimized_image_edit_request(&self.optimizer, request).await?;
            self.editor.edit_image(request).await
        })
    }
}

async fn optimized_image_generation_request<Optimizer>(
    optimizer: &MediaPromptOptimizerService<Optimizer>,
    mut request: ImageGenerationRequest,
) -> Result<ImageGenerationRequest, ImageGenerationError>
where
    Optimizer: MediaPromptOptimizer,
{
    if request
        .prompt_variants
        .iter()
        .any(|value| !value.trim().is_empty())
    {
        return Ok(request);
    }
    let original_prompt = build_draw_prompt_text(
        &request.prompt,
        &request.negative_prompt,
        &request.aspect_ratio,
        &request.seed,
    );
    if original_prompt.trim().is_empty() {
        return Ok(request);
    }
    let optimized = optimizer
        .enhance_image_prompt(&original_prompt, &request.aspect_ratio, 1)
        .await;
    if let Some(error) = optimized.provider_error.as_deref() {
        tracing::debug!(%error, "image prompt optimization failed; using original prompt");
    }
    let optimized = openplotva_media::apply_word_replacements(optimized.value);
    apply_image_nsfw_result(optimized.nsfw_result, &mut request)?;
    request.prompt_variants = optimized
        .outputs
        .into_iter()
        .filter_map(non_empty)
        .collect();
    if !optimized.aspect_ratio.trim().is_empty() {
        request.aspect_ratio = optimized.aspect_ratio.trim().to_owned();
    }
    Ok(request)
}

async fn optimized_image_edit_request<Optimizer>(
    optimizer: &MediaPromptOptimizerService<Optimizer>,
    mut request: ImageEditRequest,
) -> Result<ImageEditRequest, ImageEditError>
where
    Optimizer: MediaPromptOptimizer,
{
    let original_prompt = request.prompt.trim().to_owned();
    if original_prompt.is_empty() {
        return Ok(request);
    }
    let optimized = optimizer
        .enhance_image_edit_prompt(&original_prompt, 1)
        .await;
    if let Some(error) = optimized.provider_error.as_deref() {
        tracing::debug!(%error, "image edit prompt optimization failed; using original prompt");
    }
    match optimized.value.nsfw_result {
        openplotva_media::NsfwResult::Forbidden => return Err(ImageEditError::Forbidden),
        openplotva_media::NsfwResult::Safe | openplotva_media::NsfwResult::Adult => {}
    }
    request.prompt = first_prompt_variant(optimized.value.outputs, &original_prompt);
    Ok(request)
}

fn apply_image_nsfw_result(
    nsfw: openplotva_media::NsfwResult,
    request: &mut ImageGenerationRequest,
) -> Result<(), ImageGenerationError> {
    match nsfw {
        openplotva_media::NsfwResult::Safe => {
            request.is_nsfw = false;
            Ok(())
        }
        openplotva_media::NsfwResult::Adult => {
            request.is_nsfw = true;
            Ok(())
        }
        openplotva_media::NsfwResult::Forbidden => Err(ImageGenerationError::Forbidden),
    }
}

fn first_prompt_variant(variants: Vec<String>, fallback: &str) -> String {
    variants
        .into_iter()
        .find_map(non_empty)
        .unwrap_or_else(|| fallback.trim().to_owned())
}

fn image_generation_result_has_images(result: &ImageGenerationResult) -> bool {
    result.first_image_url().is_some() || result.image_bytes.iter().any(|image| !image.is_empty())
}

pub trait QueuedStickerStore {
    /// Store error type.
    type Error: std::fmt::Display + Send + Sync + 'static;

    /// Load the queued sticker message ID for a source message.
    fn queued_sticker_message_id<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> ImageJobEffectFuture<'a, Result<Option<i64>, Self::Error>>;

    /// Delete the queued sticker record for a source message.
    fn delete_queued_sticker<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> ImageJobEffectFuture<'a, Result<(), Self::Error>>;
}

impl QueuedStickerStore for openplotva_storage::RedisQueuedStickerStore {
    type Error = openplotva_storage::StorageError;

    fn queued_sticker_message_id<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> ImageJobEffectFuture<'a, Result<Option<i64>, Self::Error>> {
        Box::pin(async move {
            self.queued_sticker_message_id(chat_id, i64::from(message_id))
                .await
        })
    }

    fn delete_queued_sticker<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> ImageJobEffectFuture<'a, Result<(), Self::Error>> {
        Box::pin(async move {
            self.delete_queued_sticker(chat_id, i64::from(message_id))
                .await
        })
    }
}

/// Telegram sender boundary for image-job stickers.
pub trait ImageJobTelegramSender {
    /// Send one outbound Telegram method.
    fn send_image_job_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> ImageJobEffectFuture<'a, Result<TelegramOutboundResponse, String>>;
}

impl ImageJobTelegramSender for openplotva_telegram::TelegramClient {
    fn send_image_job_method<'a>(
        &'a self,
        method: TelegramOutboundMethod,
    ) -> ImageJobEffectFuture<'a, Result<TelegramOutboundResponse, String>> {
        Box::pin(async move {
            execute_telegram_method(self, method)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

pub trait ImageJobEffects {
    /// Remove the queued sticker tied to the source message.
    fn remove_queued_sticker<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()>;

    /// Send the drawing sticker and return its Telegram message ID when available.
    fn send_drawing_sticker<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        user_id: i64,
        thread_id: Option<i32>,
    ) -> ImageJobEffectFuture<'a, Option<i32>>;

    /// Remove the drawing sticker.
    fn remove_drawing_sticker<'a>(
        &'a self,
        chat_id: i64,
        sticker_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()>;

    fn send_nsfw_blocked_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        user_id: i64,
        thread_id: Option<i32>,
    ) -> ImageJobEffectFuture<'a, ()>;
}

/// Concrete image-job sticker effects over Redis and Telegram.
#[derive(Clone, Debug)]
pub struct TelegramImageJobEffects<Queued, Ephemeral, Sender> {
    queued_stickers: Queued,
    ephemeral_messages: Ephemeral,
    telegram: Sender,
}

impl<Queued, Ephemeral, Sender> TelegramImageJobEffects<Queued, Ephemeral, Sender> {
    /// Build concrete image-job effects.
    #[must_use]
    pub fn new(queued_stickers: Queued, ephemeral_messages: Ephemeral, telegram: Sender) -> Self {
        Self {
            queued_stickers,
            ephemeral_messages,
            telegram,
        }
    }
}

impl<Queued, Ephemeral, Sender> ImageJobEffects
    for TelegramImageJobEffects<Queued, Ephemeral, Sender>
where
    Queued: QueuedStickerStore + Send + Sync,
    Ephemeral: crate::virtual_messages::EphemeralMessageTracker + Send + Sync,
    Sender: ImageJobTelegramSender + Send + Sync,
{
    fn remove_queued_sticker<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            let Ok(Some(sticker_message_id)) = self
                .queued_stickers
                .queued_sticker_message_id(chat_id, message_id)
                .await
            else {
                return;
            };
            if sticker_message_id > 0 {
                self.delete_telegram_message(chat_id, sticker_message_id)
                    .await;
            }
            let _ = self
                .queued_stickers
                .delete_queued_sticker(chat_id, message_id)
                .await;
        })
    }

    fn send_drawing_sticker<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        user_id: i64,
        thread_id: Option<i32>,
    ) -> ImageJobEffectFuture<'a, Option<i32>> {
        Box::pin(async move {
            let (request, reply_to) =
                drawing_sticker_message_request(chat_id, message_id, user_id, thread_id);
            let Ok(method) = build_sticker_message_method(&request, Some(&reply_to)) else {
                return None;
            };
            let Ok(response) = self
                .telegram
                .send_image_job_method(TelegramOutboundMethod::from(method))
                .await
            else {
                return None;
            };
            let sticker_message_id = image_job_response_message_id(&response)?;
            let _ = self
                .ephemeral_messages
                .track_ephemeral_message(
                    chat_id,
                    sticker_message_id,
                    DRAWING_STICKER_DELETE_AFTER,
                    OffsetDateTime::now_utc(),
                )
                .await;
            Some(sticker_message_id)
        })
    }

    fn remove_drawing_sticker<'a>(
        &'a self,
        chat_id: i64,
        sticker_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            if sticker_message_id > 0 {
                self.delete_telegram_message(chat_id, i64::from(sticker_message_id))
                    .await;
            }
        })
    }

    fn send_nsfw_blocked_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        _user_id: i64,
        thread_id: Option<i32>,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            let (request, reply_to) = nsfw_blocked_message_request(chat_id, message_id, thread_id);
            let Ok(methods) = build_text_message_methods(&request, Some(&reply_to)) else {
                return;
            };
            for method in methods {
                let _ = self
                    .telegram
                    .send_image_job_method(TelegramOutboundMethod::from(method))
                    .await;
            }
        })
    }
}

impl<Queued, Ephemeral, Sender> TelegramImageJobEffects<Queued, Ephemeral, Sender>
where
    Sender: ImageJobTelegramSender + Send + Sync,
{
    async fn delete_telegram_message(&self, chat_id: i64, message_id: i64) {
        let Ok(method) = build_delete_message_method(&DeleteMessageRequest {
            chat_id,
            message_id,
        }) else {
            return;
        };
        let _ = self
            .telegram
            .send_image_job_method(TelegramOutboundMethod::from(method))
            .await;
    }
}

fn nsfw_blocked_message_request(
    chat_id: i64,
    message_id: i32,
    thread_id: Option<i32>,
) -> (TextMessageRequest, ReplyMessageRef) {
    let message_thread_id = i64::from(thread_id.unwrap_or_default());
    let is_topic_message = message_thread_id != 0;
    (
        TextMessageRequest {
            chat: None,
            message_thread_id,
            disable_notification: false,
            allow_sending_without_reply: Some(true),
            text: NSFW_BLOCKED_MESSAGE_TEXT.to_owned(),
            render_as: String::new(),
            reply_markup: None,
        },
        ReplyMessageRef {
            message_id: i64::from(message_id),
            chat: ChatRef {
                id: chat_id,
                is_forum: is_topic_message,
            },
            is_topic_message,
            message_thread_id,
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageGenJobExecutionOutcome {
    Completed,
    SafetyBlocked,
    Failed,
}

/// Report from one image job execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageGenJobExecutionReport {
    /// Terminal outcome.
    pub outcome: ImageGenJobExecutionOutcome,
    /// Effective prompt.
    pub prompt: String,
    /// Caption sent to the provider.
    pub caption_text: String,
    /// Generated image URL when present.
    pub image_url: Option<String>,
    /// Failure text when the job should fail.
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageEditJobExecutionOutcome {
    Completed,
    SafetyBlocked,
    Failed,
}

/// Report from one image-edit job execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageEditJobExecutionReport {
    /// Terminal outcome.
    pub outcome: ImageEditJobExecutionOutcome,
    /// Sanitized edit instruction.
    pub prompt: String,
    /// Image URLs produced by the edit provider, if present.
    pub image_urls: Vec<String>,
    /// Failure text when the job should fail.
    pub error: Option<String>,
}

/// Result from one queue poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageGenQueuePollOutcome {
    /// No matching image generation job was pending.
    Idle,
    /// Job completed successfully.
    Completed,
    /// Job was safety-blocked and completed without retry.
    SafetyBlocked,
    /// Retryable provider failure was recorded and the job was requeued.
    RetryRequeued,
    /// Retryable provider failure exhausted attempts and failed the job.
    RetryExhausted,
    /// Job failed.
    Failed,
    /// Job payload could not be decoded and was failed.
    DecodeFailed,
}

/// Report from one image-generation queue poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageGenQueuePollReport {
    /// Queue that was polled.
    pub queue_name: String,
    /// Taskman job ID if a job was dequeued.
    pub job_id: Option<i64>,
    /// Poll outcome.
    pub outcome: ImageGenQueuePollOutcome,
    /// Error text stored on failed jobs.
    pub error: Option<String>,
}

/// Result from one image-edit queue poll.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageEditQueuePollOutcome {
    /// No matching image edit job was pending.
    Idle,
    /// Job completed successfully.
    Completed,
    /// Job was safety-blocked and completed without retry.
    SafetyBlocked,
    /// Retryable provider failure was recorded and the job was requeued.
    RetryRequeued,
    /// Retryable provider failure exhausted attempts and failed the job.
    RetryExhausted,
    /// Job failed.
    Failed,
    /// Job payload could not be decoded and was failed.
    DecodeFailed,
}

/// Report from one image-edit queue poll.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImageEditQueuePollReport {
    /// Queue that was polled.
    pub queue_name: String,
    /// Taskman job ID if a job was dequeued.
    pub job_id: Option<i64>,
    /// Poll outcome.
    pub outcome: ImageEditQueuePollOutcome,
    /// Error text stored on failed jobs.
    pub error: Option<String>,
}

/// Aggregate report from a long-running image taskman worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageGenWorkerRunReport {
    /// Number of queue poll ticks.
    pub ticks: u64,
    /// Number of dequeued jobs.
    pub dequeued: u64,
    /// Number of completed jobs.
    pub completed: u64,
    /// Number of safety-blocked jobs completed without retry.
    pub safety_blocked: u64,
    /// Number of retryable provider failures requeued.
    pub retry_requeued: u64,
    /// Number of retryable provider failures exhausted.
    pub retry_exhausted: u64,
    /// Number of failed jobs.
    pub failed: u64,
    /// Number of payload decode failures.
    pub decode_failed: u64,
    /// Number of idle ticks.
    pub idle: u64,
    /// Number of poll reports carrying a status/failure error.
    pub errors: u64,
}

impl ImageGenWorkerRunReport {
    fn record_poll(&mut self, report: &ImageGenQueuePollReport) {
        self.ticks += 1;
        if report.job_id.is_some() {
            self.dequeued += 1;
        }
        match report.outcome {
            ImageGenQueuePollOutcome::Idle => self.idle += 1,
            ImageGenQueuePollOutcome::Completed => self.completed += 1,
            ImageGenQueuePollOutcome::SafetyBlocked => self.safety_blocked += 1,
            ImageGenQueuePollOutcome::RetryRequeued => self.retry_requeued += 1,
            ImageGenQueuePollOutcome::RetryExhausted => self.retry_exhausted += 1,
            ImageGenQueuePollOutcome::Failed => self.failed += 1,
            ImageGenQueuePollOutcome::DecodeFailed => self.decode_failed += 1,
        }
        if report.error.is_some() {
            self.errors += 1;
        }
    }
}

/// Aggregate report from a long-running image-edit taskman worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImageEditWorkerRunReport {
    /// Number of queue poll ticks.
    pub ticks: u64,
    /// Number of dequeued jobs.
    pub dequeued: u64,
    /// Number of completed jobs.
    pub completed: u64,
    /// Number of safety-blocked jobs completed without retry.
    pub safety_blocked: u64,
    /// Number of retryable provider failures requeued.
    pub retry_requeued: u64,
    /// Number of retryable provider failures exhausted.
    pub retry_exhausted: u64,
    /// Number of failed jobs.
    pub failed: u64,
    /// Number of payload decode failures.
    pub decode_failed: u64,
    /// Number of idle ticks.
    pub idle: u64,
    /// Number of poll reports carrying a status/failure error.
    pub errors: u64,
}

impl ImageEditWorkerRunReport {
    fn record_poll(&mut self, report: &ImageEditQueuePollReport) {
        self.ticks += 1;
        if report.job_id.is_some() {
            self.dequeued += 1;
        }
        match report.outcome {
            ImageEditQueuePollOutcome::Idle => self.idle += 1,
            ImageEditQueuePollOutcome::Completed => self.completed += 1,
            ImageEditQueuePollOutcome::SafetyBlocked => self.safety_blocked += 1,
            ImageEditQueuePollOutcome::RetryRequeued => self.retry_requeued += 1,
            ImageEditQueuePollOutcome::RetryExhausted => self.retry_exhausted += 1,
            ImageEditQueuePollOutcome::Failed => self.failed += 1,
            ImageEditQueuePollOutcome::DecodeFailed => self.decode_failed += 1,
        }
        if report.error.is_some() {
            self.errors += 1;
        }
    }
}

#[must_use]
pub fn sanitize_image_gen_job_params(mut params: ImageGenJobParams) -> ImageGenJobParams {
    params.prompt = sanitize_tool_text(&params.prompt);
    params.original_text = sanitize_tool_text(&params.original_text);
    params.negative_prompt = sanitize_tool_text(&params.negative_prompt);
    params.aspect_ratio = sanitize_tool_text(&params.aspect_ratio);
    params.seed = sanitize_tool_text(&params.seed);
    for prompt in &mut params.prompt_variants {
        *prompt = sanitize_tool_text(prompt);
    }
    params
}

#[must_use]
pub fn image_gen_prompt(params: &ImageGenJobParams) -> String {
    let prompt = params.prompt.trim();
    if !prompt.is_empty() {
        return prompt.to_owned();
    }
    let meta = serde_json::from_value::<ChatMessageMeta>(params.meta.clone()).unwrap_or_default();
    openplotva_updates::compose_image_prompt("", &meta)
}

#[must_use]
pub fn image_gen_caption_text(params: &ImageGenJobParams, prompt: &str) -> String {
    let original = params.original_text.trim();
    if !original.is_empty() {
        return original.to_owned();
    }
    prompt.to_owned()
}

pub async fn execute_image_gen_job<Generator, Effects>(
    generator: &Generator,
    effects: &Effects,
    params: ImageGenJobParams,
) -> ImageGenJobExecutionReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
{
    let params = sanitize_image_gen_job_params(params);
    let prompt = image_gen_prompt(&params);
    let caption_text = image_gen_caption_text(&params, &prompt);
    effects
        .remove_queued_sticker(params.chat_id, params.message_id)
        .await;
    let drawing_sticker = effects
        .send_drawing_sticker(
            params.chat_id,
            params.message_id,
            params.user_id,
            params.thread_id,
        )
        .await;

    let request = ImageGenerationRequest {
        chat_id: params.chat_id,
        message_id: params.message_id,
        user_id: params.user_id,
        user_full_name: params.user_full_name,
        thread_id: params.thread_id,
        prompt: prompt.clone(),
        caption_text: caption_text.clone(),
        prompt_variants: params.prompt_variants,
        is_nsfw: params.is_nsfw,
        negative_prompt: params.negative_prompt,
        aspect_ratio: params.aspect_ratio,
        seed: params.seed,
    };

    let result = generator.generate_image(request).await;
    if let Some(sticker_id) = drawing_sticker.filter(|sticker_id| *sticker_id > 0) {
        effects
            .remove_drawing_sticker(params.chat_id, sticker_id)
            .await;
    }

    match result {
        Ok(result) => ImageGenJobExecutionReport {
            outcome: ImageGenJobExecutionOutcome::Completed,
            prompt,
            caption_text,
            image_url: result.first_image_url(),
            error: None,
        },
        Err(ImageGenerationError::Forbidden) => {
            effects
                .send_nsfw_blocked_message(
                    params.chat_id,
                    params.message_id,
                    params.user_id,
                    params.thread_id,
                )
                .await;
            ImageGenJobExecutionReport {
                outcome: ImageGenJobExecutionOutcome::SafetyBlocked,
                prompt,
                caption_text,
                image_url: None,
                error: None,
            }
        }
        Err(error) => ImageGenJobExecutionReport {
            outcome: ImageGenJobExecutionOutcome::Failed,
            prompt,
            caption_text,
            image_url: None,
            error: Some(error.message()),
        },
    }
}

#[must_use]
pub fn sanitize_image_edit_job_params(mut params: ImageEditJobParams) -> ImageEditJobParams {
    params.prompt = sanitize_tool_text(&params.prompt);
    params
}

pub async fn execute_image_edit_job<Editor, Effects>(
    editor: &Editor,
    effects: &Effects,
    params: ImageEditJobParams,
) -> ImageEditJobExecutionReport
where
    Editor: ImageEditor + Sync,
    Effects: ImageJobEffects + Sync,
{
    let params = sanitize_image_edit_job_params(params);
    effects
        .remove_queued_sticker(params.chat_id, params.message_id)
        .await;
    let drawing_sticker = effects
        .send_drawing_sticker(
            params.chat_id,
            params.message_id,
            params.user_id,
            params.thread_id,
        )
        .await;

    let request = ImageEditRequest {
        chat_id: params.chat_id,
        message_id: params.message_id,
        user_id: params.user_id,
        user_full_name: params.user_full_name,
        thread_id: params.thread_id,
        prompt: params.prompt.clone(),
        photo_file_id: params.photo_file_id,
        photo_urls: params.photo_urls,
    };

    let result = editor.edit_image(request).await;
    if let Some(sticker_id) = drawing_sticker.filter(|sticker_id| *sticker_id > 0) {
        effects
            .remove_drawing_sticker(params.chat_id, sticker_id)
            .await;
    }

    match result {
        Ok(result) => ImageEditJobExecutionReport {
            outcome: ImageEditJobExecutionOutcome::Completed,
            prompt: params.prompt,
            image_urls: result
                .image_urls
                .into_iter()
                .filter_map(non_empty)
                .collect(),
            error: None,
        },
        Err(ImageEditError::Forbidden) => {
            effects
                .send_nsfw_blocked_message(
                    params.chat_id,
                    params.message_id,
                    params.user_id,
                    params.thread_id,
                )
                .await;
            ImageEditJobExecutionReport {
                outcome: ImageEditJobExecutionOutcome::SafetyBlocked,
                prompt: params.prompt,
                image_urls: Vec::new(),
                error: None,
            }
        }
        Err(error) => ImageEditJobExecutionReport {
            outcome: ImageEditJobExecutionOutcome::Failed,
            prompt: params.prompt,
            image_urls: Vec::new(),
            error: Some(error.message()),
        },
    }
}

/// Dequeue and process one image-generation job from a queue.
pub async fn run_image_gen_queue_once<Generator, Effects>(
    queue: &InMemoryTaskQueue,
    queue_name: &str,
    generator: &Generator,
    effects: &Effects,
    worker_id: &str,
    now: OffsetDateTime,
) -> ImageGenQueuePollReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
{
    run_image_gen_queue_once_with_max_attempts(
        queue,
        queue_name,
        generator,
        effects,
        worker_id,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        now,
    )
    .await
}

/// Dequeue and process one image-generation job with a configured retry limit.
pub async fn run_image_gen_queue_once_with_max_attempts<Generator, Effects>(
    queue: &InMemoryTaskQueue,
    queue_name: &str,
    generator: &Generator,
    effects: &Effects,
    worker_id: &str,
    max_llm_job_attempts: i32,
    now: OffsetDateTime,
) -> ImageGenQueuePollReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
{
    let Some(work) = queue.dequeue_matching(queue_name, worker_id, now, |job| {
        job.data.job_type == JobType::ImageGen
    }) else {
        return ImageGenQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: None,
            outcome: ImageGenQueuePollOutcome::Idle,
            error: None,
        };
    };

    let params = match image_gen_job_params_from_stateless_job(&work.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), now);
            return ImageGenQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageGenQueuePollOutcome::DecodeFailed,
                error: Some(error),
            };
        }
    };

    let _ = queue.set_execution_started(work.id, now);
    let execution = execute_image_gen_job(generator, effects, params).await;
    match execution.outcome {
        ImageGenJobExecutionOutcome::Completed => {
            if let Some(image_url) = execution.image_url {
                let _ = queue.set_job_image_urls(work.id, vec![image_url]);
            }
            finalize_completed(
                queue,
                work.id,
                queue_name,
                ImageGenQueuePollOutcome::Completed,
                now,
            )
        }
        ImageGenJobExecutionOutcome::SafetyBlocked => finalize_completed(
            queue,
            work.id,
            queue_name,
            ImageGenQueuePollOutcome::SafetyBlocked,
            now,
        ),
        ImageGenJobExecutionOutcome::Failed => {
            let error = execution
                .error
                .unwrap_or_else(|| "image generation failed".to_owned());
            if let Some(report) = retry_or_exhaust_image_gen_job(
                queue,
                &work,
                queue_name,
                &error,
                max_llm_job_attempts,
                now,
            ) {
                return report;
            }
            let _ = queue.fail(work.id, error.clone(), now);
            ImageGenQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageGenQueuePollOutcome::Failed,
                error: Some(error),
            }
        }
    }
}

fn retry_or_exhaust_image_gen_job(
    queue: &InMemoryTaskQueue,
    work: &TaskQueueWorkItem,
    queue_name: &str,
    error: &str,
    max_llm_job_attempts: i32,
    now: OffsetDateTime,
) -> Option<ImageGenQueuePollReport> {
    let reason = openplotva_llm::retry::retryable_reason_from_message(error)?;
    let attempt = next_image_llm_job_attempt(&work.events);
    let max_attempts = max_llm_job_attempts.max(1);
    let target_queue = retryable_image_job_target_queue(queue_name);
    let exhausted = attempt >= max_attempts;
    let stage = if exhausted {
        LLM_JOB_RETRY_EXHAUSTED_STAGE
    } else {
        LLM_JOB_RETRY_STAGE
    };
    let mut event = image_retry_job_event(
        stage,
        attempt,
        max_attempts,
        &infer_image_retry_provider(error),
        reason,
        &target_queue,
        error,
    );
    if exhausted {
        event.message = "retryable LLM provider error exhausted job attempts".to_owned();
    }
    queue.append_job_event(work.id, event, now).ok()?;
    if exhausted {
        let _ = queue.fail(work.id, error.to_owned(), now);
        return Some(ImageGenQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: Some(work.id),
            outcome: ImageGenQueuePollOutcome::RetryExhausted,
            error: Some(error.to_owned()),
        });
    }
    queue.requeue_job_to_queue(work.id, &target_queue).ok()?;
    Some(ImageGenQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(work.id),
        outcome: ImageGenQueuePollOutcome::RetryRequeued,
        error: None,
    })
}

fn next_image_llm_job_attempt(events: &[TaskQueueJobEvent]) -> i32 {
    events
        .iter()
        .filter(|event| {
            event.stage == LLM_JOB_RETRY_STAGE || event.stage == LLM_JOB_RETRY_EXHAUSTED_STAGE
        })
        .map(|event| event.attempt)
        .max()
        .unwrap_or(0)
        + 1
}

fn retryable_image_job_target_queue(queue_name: &str) -> String {
    if queue_name.trim().is_empty() {
        IMAGE_REGULAR_QUEUE_NAME.to_owned()
    } else {
        queue_name.to_owned()
    }
}

fn infer_image_retry_provider(error: &str) -> String {
    if let Some((provider, _rest)) = error.trim().split_once(" provider ") {
        let provider = provider.trim();
        if !provider.is_empty()
            && provider
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
        {
            return provider.to_owned();
        }
    }
    "llm".to_owned()
}

fn image_retry_job_event(
    stage: &str,
    attempt: i32,
    max_attempts: i32,
    provider: &str,
    reason: openplotva_llm::retry::FailureReason,
    target_queue: &str,
    error: &str,
) -> TaskQueueJobEvent {
    let mut data = BTreeMap::new();
    data.insert("fallback_reason".to_owned(), reason.to_string());
    data.insert("max_attempts".to_owned(), max_attempts.to_string());
    data.insert("target_queue".to_owned(), target_queue.to_owned());

    TaskQueueJobEvent {
        level: "warn".to_owned(),
        stage: stage.to_owned(),
        attempt,
        provider: provider.to_owned(),
        message: "retryable LLM provider error, requeueing job".to_owned(),
        error: error.to_owned(),
        data,
        ..TaskQueueJobEvent::default()
    }
}

/// Dequeue and process one image-edit job from a queue.
pub async fn run_image_edit_queue_once<Editor, Effects>(
    queue: &InMemoryTaskQueue,
    queue_name: &str,
    editor: &Editor,
    effects: &Effects,
    worker_id: &str,
    now: OffsetDateTime,
) -> ImageEditQueuePollReport
where
    Editor: ImageEditor + Sync,
    Effects: ImageJobEffects + Sync,
{
    run_image_edit_queue_once_with_max_attempts(
        queue,
        queue_name,
        editor,
        effects,
        worker_id,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        now,
    )
    .await
}

/// Dequeue and process one image-edit job with a configured retry limit.
pub async fn run_image_edit_queue_once_with_max_attempts<Editor, Effects>(
    queue: &InMemoryTaskQueue,
    queue_name: &str,
    editor: &Editor,
    effects: &Effects,
    worker_id: &str,
    max_llm_job_attempts: i32,
    now: OffsetDateTime,
) -> ImageEditQueuePollReport
where
    Editor: ImageEditor + Sync,
    Effects: ImageJobEffects + Sync,
{
    let Some(work) = queue.dequeue_matching(queue_name, worker_id, now, |job| {
        job.data.job_type == JobType::ImageEdit
    }) else {
        return ImageEditQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: None,
            outcome: ImageEditQueuePollOutcome::Idle,
            error: None,
        };
    };

    let params = match image_edit_job_params_from_stateless_job(&work.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), now);
            return ImageEditQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageEditQueuePollOutcome::DecodeFailed,
                error: Some(error),
            };
        }
    };

    let _ = queue.set_execution_started(work.id, now);
    let execution = execute_image_edit_job(editor, effects, params).await;
    match execution.outcome {
        ImageEditJobExecutionOutcome::Completed => {
            if !execution.image_urls.is_empty() {
                let _ = queue.set_job_image_urls(work.id, execution.image_urls);
            }
            finalize_image_edit_completed(
                queue,
                work.id,
                queue_name,
                ImageEditQueuePollOutcome::Completed,
                now,
            )
        }
        ImageEditJobExecutionOutcome::SafetyBlocked => finalize_image_edit_completed(
            queue,
            work.id,
            queue_name,
            ImageEditQueuePollOutcome::SafetyBlocked,
            now,
        ),
        ImageEditJobExecutionOutcome::Failed => {
            let error = execution
                .error
                .unwrap_or_else(|| "image edit failed".to_owned());
            if let Some(report) = retry_or_exhaust_image_edit_job(
                queue,
                &work,
                queue_name,
                &error,
                max_llm_job_attempts,
                now,
            ) {
                return report;
            }
            let _ = queue.fail(work.id, error.clone(), now);
            ImageEditQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageEditQueuePollOutcome::Failed,
                error: Some(error),
            }
        }
    }
}

fn retry_or_exhaust_image_edit_job(
    queue: &InMemoryTaskQueue,
    work: &TaskQueueWorkItem,
    queue_name: &str,
    error: &str,
    max_llm_job_attempts: i32,
    now: OffsetDateTime,
) -> Option<ImageEditQueuePollReport> {
    let reason = openplotva_llm::retry::retryable_reason_from_message(error)?;
    let attempt = next_image_llm_job_attempt(&work.events);
    let max_attempts = max_llm_job_attempts.max(1);
    let target_queue = retryable_image_job_target_queue(queue_name);
    let exhausted = attempt >= max_attempts;
    let stage = if exhausted {
        LLM_JOB_RETRY_EXHAUSTED_STAGE
    } else {
        LLM_JOB_RETRY_STAGE
    };
    let mut event = image_retry_job_event(
        stage,
        attempt,
        max_attempts,
        &infer_image_retry_provider(error),
        reason,
        &target_queue,
        error,
    );
    if exhausted {
        event.message = "retryable LLM provider error exhausted job attempts".to_owned();
    }
    queue.append_job_event(work.id, event, now).ok()?;
    if exhausted {
        let _ = queue.fail(work.id, error.to_owned(), now);
        return Some(ImageEditQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: Some(work.id),
            outcome: ImageEditQueuePollOutcome::RetryExhausted,
            error: Some(error.to_owned()),
        });
    }
    queue.requeue_job_to_queue(work.id, &target_queue).ok()?;
    Some(ImageEditQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(work.id),
        outcome: ImageEditQueuePollOutcome::RetryRequeued,
        error: None,
    })
}

pub async fn run_regular_image_gen_queue_once<Generator, Effects>(
    queue: &InMemoryTaskQueue,
    generator: &Generator,
    effects: &Effects,
    worker_id: &str,
    now: OffsetDateTime,
) -> ImageGenQueuePollReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
{
    run_image_gen_queue_once(
        queue,
        IMAGE_REGULAR_QUEUE_NAME,
        generator,
        effects,
        worker_id,
        now,
    )
    .await
}

/// Run image-generation taskman workers for selected queues until stop resolves.
pub async fn run_image_gen_worker_every_until<Generator, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    generator: &Generator,
    effects: &Effects,
    queue_names: &'static [&'static str],
    interval: StdDuration,
    stop: Stop,
) -> ImageGenWorkerRunReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    run_image_gen_worker_every_until_with_max_attempts(
        queue,
        generator,
        effects,
        queue_names,
        interval,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        stop,
    )
    .await
}

/// Run image-generation taskman workers with a configured retry limit.
pub async fn run_image_gen_worker_every_until_with_max_attempts<Generator, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    generator: &Generator,
    effects: &Effects,
    queue_names: &'static [&'static str],
    interval: StdDuration,
    max_llm_job_attempts: i32,
    stop: Stop,
) -> ImageGenWorkerRunReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = ImageGenWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                for queue_name in queue_names {
                    let tick = run_image_gen_queue_once_with_max_attempts(
                        queue,
                        queue_name,
                        generator,
                        effects,
                        image_worker_id(queue_name),
                        max_llm_job_attempts,
                        OffsetDateTime::now_utc(),
                    ).await;
                    trace_image_gen_queue_tick(&tick);
                    report.record_poll(&tick);
                }
            }
        }
    }

    report
}

pub async fn run_image_gen_worker_until<Generator, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    generator: &Generator,
    effects: &Effects,
    stop: Stop,
) -> ImageGenWorkerRunReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    run_image_gen_worker_every_until(
        queue,
        generator,
        effects,
        &IMAGE_JOB_WORKER_QUEUES,
        IMAGE_JOB_POLL_INTERVAL,
        stop,
    )
    .await
}

/// Run image-edit taskman workers for selected queues until stop resolves.
pub async fn run_image_edit_worker_every_until<Editor, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    editor: &Editor,
    effects: &Effects,
    queue_names: &'static [&'static str],
    interval: StdDuration,
    stop: Stop,
) -> ImageEditWorkerRunReport
where
    Editor: ImageEditor + Sync,
    Effects: ImageJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    run_image_edit_worker_every_until_with_max_attempts(
        queue,
        editor,
        effects,
        queue_names,
        interval,
        DEFAULT_LLM_JOB_MAX_ATTEMPTS,
        stop,
    )
    .await
}

/// Run image-edit taskman workers with a configured retry limit.
pub async fn run_image_edit_worker_every_until_with_max_attempts<Editor, Effects, Stop>(
    queue: &InMemoryTaskQueue,
    editor: &Editor,
    effects: &Effects,
    queue_names: &'static [&'static str],
    interval: StdDuration,
    max_llm_job_attempts: i32,
    stop: Stop,
) -> ImageEditWorkerRunReport
where
    Editor: ImageEditor + Sync,
    Effects: ImageJobEffects + Sync,
    Stop: Future<Output = ()>,
{
    let mut report = ImageEditWorkerRunReport::default();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(interval) => {
                for queue_name in queue_names {
                    let tick = run_image_edit_queue_once_with_max_attempts(
                        queue,
                        queue_name,
                        editor,
                        effects,
                        image_worker_id(queue_name),
                        max_llm_job_attempts,
                        OffsetDateTime::now_utc(),
                    ).await;
                    trace_image_edit_queue_tick(&tick);
                    report.record_poll(&tick);
                }
            }
        }
    }

    report
}

fn finalize_completed(
    queue: &InMemoryTaskQueue,
    job_id: i64,
    queue_name: &str,
    outcome: ImageGenQueuePollOutcome,
    now: OffsetDateTime,
) -> ImageGenQueuePollReport {
    let error = queue
        .complete(job_id, now)
        .err()
        .map(task_queue_error_message);
    ImageGenQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(job_id),
        outcome,
        error,
    }
}

fn finalize_image_edit_completed(
    queue: &InMemoryTaskQueue,
    job_id: i64,
    queue_name: &str,
    outcome: ImageEditQueuePollOutcome,
    now: OffsetDateTime,
) -> ImageEditQueuePollReport {
    let error = queue
        .complete(job_id, now)
        .err()
        .map(task_queue_error_message);
    ImageEditQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(job_id),
        outcome,
        error,
    }
}

fn task_queue_error_message(error: TaskQueueError) -> String {
    error.to_string()
}

fn image_worker_id(queue_name: &str) -> &'static str {
    match queue_name {
        IMAGE_VIP_QUEUE_NAME => IMAGE_VIP_WORKER_ID,
        IMAGE_REGULAR_QUEUE_NAME => IMAGE_REGULAR_WORKER_ID,
        _ => "image-worker",
    }
}

fn trace_image_gen_queue_tick(tick: &ImageGenQueuePollReport) {
    if tick.outcome == ImageGenQueuePollOutcome::Idle && tick.error.is_none() {
        return;
    }

    tracing::debug!(
        queue_name = tick.queue_name,
        job_id = tick.job_id,
        outcome = ?tick.outcome,
        error = tick.error.as_deref(),
        "processed image generation taskman worker tick"
    );
}

fn non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn trace_image_edit_queue_tick(tick: &ImageEditQueuePollReport) {
    match tick.outcome {
        ImageEditQueuePollOutcome::Idle => {}
        ImageEditQueuePollOutcome::Completed
        | ImageEditQueuePollOutcome::SafetyBlocked
        | ImageEditQueuePollOutcome::RetryRequeued => {
            tracing::debug!(
                queue_name = tick.queue_name,
                job_id = tick.job_id,
                outcome = ?tick.outcome,
                "processed image edit taskman worker tick"
            );
        }
        ImageEditQueuePollOutcome::RetryExhausted
        | ImageEditQueuePollOutcome::Failed
        | ImageEditQueuePollOutcome::DecodeFailed => {
            tracing::warn!(
                queue_name = tick.queue_name,
                job_id = tick.job_id,
                outcome = ?tick.outcome,
                error = tick.error.as_deref().unwrap_or_default(),
                "image edit taskman job failed"
            );
        }
    }
}

#[must_use]
pub fn drawing_sticker_message_request(
    chat_id: i64,
    message_id: i32,
    _user_id: i64,
    thread_id: Option<i32>,
) -> (StickerMessageRequest, ReplyMessageRef) {
    (
        StickerMessageRequest {
            chat: None,
            message_thread_id: 0,
            disable_notification: true,
            file_id: STICKER_DRAW_FILE_ID.to_owned(),
        },
        ReplyMessageRef {
            message_id: i64::from(message_id),
            chat: ChatRef {
                id: chat_id,
                is_forum: false,
            },
            is_topic_message: false,
            message_thread_id: i64::from(thread_id.unwrap_or_default()),
        },
    )
}

fn image_job_response_message_id(response: &TelegramOutboundResponse) -> Option<i32> {
    match response {
        TelegramOutboundResponse::Message(message) => i32::try_from(message.id).ok(),
        TelegramOutboundResponse::Messages(messages) => messages
            .first()
            .and_then(|message| i32::try_from(message.id).ok()),
        TelegramOutboundResponse::EditMessage(_)
        | TelegramOutboundResponse::Boolean(_)
        | TelegramOutboundResponse::SentGuestMessage(_)
        | TelegramOutboundResponse::String(_) => None,
    }
}

#[must_use]
pub fn draw_api_prompt_text(request: &ImageGenerationRequest) -> String {
    if let Some(variant) = request.prompt_variants.iter().find_map(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    }) {
        return variant;
    }
    build_draw_prompt_text(
        &request.prompt,
        &request.negative_prompt,
        &request.aspect_ratio,
        &request.seed,
    )
}

pub fn decode_draw_api_result_payload(payload: &[u8]) -> Result<DrawApiGenerateResult, String> {
    let raw = serde_json::from_slice::<Value>(payload)
        .map_err(|err| format!("failed to decode response: {err}"))?;
    match raw {
        Value::Object(map) => {
            if let Some(value) = map.get("image_b64")
                && let Ok(images) = decode_image_b64_value(value)
                && !images.is_empty()
            {
                return Ok(DrawApiGenerateResult {
                    images,
                    urls: Vec::new(),
                });
            }
            let urls = extract_string_list(map.get("image_url").unwrap_or(&Value::Null));
            if !urls.is_empty() {
                return Ok(DrawApiGenerateResult {
                    images: Vec::new(),
                    urls,
                });
            }
            Err("response missing image_b64".to_owned())
        }
        Value::String(value) => Ok(DrawApiGenerateResult {
            images: decode_image_b64_list(&[value])?,
            urls: Vec::new(),
        }),
        Value::Array(values) => {
            let values = extract_string_list(&Value::Array(values));
            if values.is_empty() {
                return Err("response missing image_b64".to_owned());
            }
            Ok(DrawApiGenerateResult {
                images: decode_image_b64_list(&values)?,
                urls: Vec::new(),
            })
        }
        _ => Err("response missing image_b64".to_owned()),
    }
}

#[derive(Serialize)]
struct DrawApiGenerateRequest<'a> {
    #[serde(skip_serializing_if = "str::is_empty")]
    prompt: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_url: Option<OneOrManyStrings<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    image_b64: Option<OneOrManyStrings<'a>>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum OneOrManyStrings<'a> {
    One(&'a str),
    Many(Vec<&'a str>),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DrawApiJobStatus {
    status: String,
    error: Option<String>,
    result: Option<DrawApiGenerateResult>,
}

enum DrawApiWaitDecision {
    Continue,
    Done(DrawApiGenerateResult),
    Failed(String),
}

fn build_draw_prompt_text(
    prompt: &str,
    negative_prompt: &str,
    aspect_ratio: &str,
    seed: &str,
) -> String {
    let prompt = prompt.trim();
    let negative_prompt = negative_prompt.trim();
    let aspect_ratio = aspect_ratio.trim();
    let seed = seed.trim();
    let mut out = String::with_capacity(
        prompt.len() + negative_prompt.len() + aspect_ratio.len() + seed.len() + 6,
    );
    out.push_str(prompt);
    if !negative_prompt.is_empty() {
        out.push_str(" | ");
        out.push_str(negative_prompt);
    }
    if !aspect_ratio.is_empty() {
        out.push(' ');
        out.push_str(aspect_ratio);
    }
    if !seed.is_empty() {
        out.push(' ');
        out.push_str(seed);
    }
    out
}

fn together_generation_error(error: TogetherError) -> ImageGenerationError {
    match error {
        TogetherError::ProviderStatus { status: 420, .. } => ImageGenerationError::Forbidden,
        other => ImageGenerationError::Provider(other.to_string()),
    }
}

fn image_dimensions_from_aspect_ratio(aspect_ratio: &str, max_side: u32) -> (u32, u32) {
    let max_side = max_side.max(1);
    let Some((width_ratio, height_ratio)) = parse_aspect_ratio(aspect_ratio) else {
        return (max_side, max_side);
    };
    if width_ratio >= height_ratio {
        let height = scale_side(max_side, height_ratio / width_ratio);
        (max_side, height)
    } else {
        let width = scale_side(max_side, width_ratio / height_ratio);
        (width, max_side)
    }
}

fn parse_aspect_ratio(aspect_ratio: &str) -> Option<(f64, f64)> {
    let candidate = aspect_ratio
        .split_whitespace()
        .find(|part| part.contains(':') || part.contains('x') || part.contains('/'))
        .unwrap_or(aspect_ratio)
        .trim();
    let separator = [':', 'x', '/']
        .into_iter()
        .find(|separator| candidate.contains(*separator))?;
    let (width, height) = candidate.split_once(separator)?;
    let width = width.trim().parse::<f64>().ok()?;
    let height = height.trim().parse::<f64>().ok()?;
    (width.is_finite() && height.is_finite() && width > 0.0 && height > 0.0)
        .then_some((width, height))
}

fn scale_side(max_side: u32, ratio: f64) -> u32 {
    ((max_side as f64 * ratio).round() as u32).clamp(1, max_side)
}

fn parse_image_seed(seed: &str) -> Option<i64> {
    let seed = seed.trim();
    if seed.is_empty() {
        return None;
    }
    seed.split_whitespace()
        .last()
        .unwrap_or(seed)
        .trim_matches(|ch: char| !ch.is_ascii_digit() && ch != '-')
        .parse::<i64>()
        .ok()
}

fn draw_api_payload(prompt: &str, image_inputs: &[String]) -> serde_json::Result<Vec<u8>> {
    let (image_urls, image_b64) = split_draw_api_image_inputs(image_inputs);
    let image_url = one_or_many_strings(&image_urls);
    let image_b64 = if image_url.is_some() {
        None
    } else {
        one_or_many_strings(&image_b64)
    };
    serde_json::to_vec(&DrawApiGenerateRequest {
        prompt,
        image_url,
        image_b64,
    })
}

fn normalized_image_edit_inputs(request: &ImageEditRequest) -> Vec<String> {
    request
        .photo_urls
        .iter()
        .filter_map(|value| non_empty(value.clone()))
        .collect()
}

fn split_draw_api_image_inputs(images: &[String]) -> (Vec<&str>, Vec<&str>) {
    let mut urls = Vec::new();
    let mut b64 = Vec::new();
    for image in images {
        let trimmed = image.trim();
        if trimmed.is_empty() {
            continue;
        }
        if starts_http_url(trimmed) {
            urls.push(trimmed);
        } else {
            b64.push(trimmed);
        }
    }
    (urls, b64)
}

fn starts_http_url(value: &str) -> bool {
    value
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http://"))
        || value
            .get(..8)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
}

fn one_or_many_strings<'a>(values: &[&'a str]) -> Option<OneOrManyStrings<'a>> {
    match values {
        [] => None,
        [value] => Some(OneOrManyStrings::One(value)),
        values => Some(OneOrManyStrings::Many(values.to_vec())),
    }
}

fn image_b64_from_data_url(data_url: &str) -> Option<String> {
    let trimmed = data_url.trim();
    let (_, encoded) = trimmed.split_once(',')?;
    non_empty(encoded.to_owned())
}

fn draw_api_status_from_envelope(
    _fallback_job_id: &str,
    envelope: &DiscoveryJobEnvelope,
) -> Result<DrawApiJobStatus, ImageGenerationError> {
    let job = envelope.resolve_job();
    let (result, status_code, body_text) = decode_draw_job_result(job.result.as_ref())?;
    let error = draw_job_error(job.error.as_ref(), status_code, &body_text);
    let mut status = job.resolved_status();
    if error.is_some() && status.trim().is_empty() {
        status = "failed".to_owned();
    }
    if job.resolved_id().is_empty()
        && status.trim().is_empty()
        && result.is_none()
        && error.is_none()
    {
        status = "processing".to_owned();
    }
    Ok(DrawApiJobStatus {
        status,
        error,
        result,
    })
}

fn decode_draw_job_result(
    result: Option<&DiscoveryJobResult>,
) -> Result<(Option<DrawApiGenerateResult>, u16, String), ImageGenerationError> {
    let Some(response) = result.and_then(|result| result.response.as_ref()) else {
        return Ok((None, 0, String::new()));
    };
    let status_code = response.status_code;
    if response.body.trim().is_empty() {
        return Ok((None, status_code, String::new()));
    }
    let payload = decode_discovery_body(&response.body).map_err(|err| {
        ImageGenerationError::Provider(format!("failed to decode response body: {err}"))
    })?;
    let body_text = String::from_utf8_lossy(&payload).trim().to_owned();
    if payload.is_empty() {
        return Ok((None, status_code, body_text));
    }
    let result =
        decode_draw_api_result_payload(&payload).map_err(ImageGenerationError::Provider)?;
    Ok((Some(result), status_code, body_text))
}

fn draw_job_error(error: Option<&Value>, status_code: u16, body_text: &str) -> Option<String> {
    let parsed = non_empty(parse_job_error(error));
    if status_code < 400 {
        return parsed;
    }
    parsed
        .or_else(|| non_empty(body_text.to_owned()))
        .or_else(|| Some(format!("API error (status {status_code})")))
}

fn evaluate_draw_api_status(status: &DrawApiJobStatus) -> DrawApiWaitDecision {
    if let Some(result) = &status.result
        && status.status.trim().is_empty()
    {
        return DrawApiWaitDecision::Done(result.clone());
    }
    if is_success_status(&status.status) {
        return match &status.result {
            Some(result) => DrawApiWaitDecision::Done(result.clone()),
            None => DrawApiWaitDecision::Failed("job completed but no result available".to_owned()),
        };
    }
    if is_failure_status(&status.status) {
        return DrawApiWaitDecision::Failed(
            status
                .error
                .clone()
                .unwrap_or_else(|| "job failed".to_owned()),
        );
    }
    if is_queued_status(&status.status) || is_running_status(&status.status) {
        return DrawApiWaitDecision::Continue;
    }
    if let Some(result) = &status.result {
        return DrawApiWaitDecision::Done(result.clone());
    }
    DrawApiWaitDecision::Failed(format!("unknown job status: {}", status.status))
}

fn decode_image_b64_value(value: &Value) -> Result<Vec<Vec<u8>>, String> {
    if let Value::String(value) = value {
        return decode_image_b64_list(std::slice::from_ref(value));
    }
    let values = extract_string_list(value);
    if values.is_empty() {
        return Err("image_b64 is empty".to_owned());
    }
    decode_image_b64_list(&values)
}

fn decode_image_b64_list(values: &[String]) -> Result<Vec<Vec<u8>>, String> {
    let mut images = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        let data = general_purpose::STANDARD
            .decode(trimmed)
            .map_err(|err| format!("failed to decode image: {err}"))?;
        if !data.is_empty() {
            images.push(data);
        }
    }
    if images.is_empty() {
        Err("decoded images are empty".to_owned())
    } else {
        Ok(images)
    }
}

fn extract_string_list(value: &Value) -> Vec<String> {
    match value {
        Value::String(value) => append_trimmed_string(Vec::new(), value),
        Value::Array(values) => {
            values
                .iter()
                .fold(Vec::with_capacity(values.len()), |out, item| {
                    if let Value::String(value) = item {
                        append_trimmed_string(out, value)
                    } else {
                        out
                    }
                })
        }
        _ => Vec::new(),
    }
}

fn append_trimmed_string(mut out: Vec<String>, value: &str) -> Vec<String> {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        out.push(trimmed.to_owned());
    }
    out
}

fn classify_draw_api_error(message: String) -> ImageGenerationError {
    let lower = message.to_ascii_lowercase();
    if lower.contains("forbidden") || lower.contains("safety") || lower.contains("nsfw") {
        ImageGenerationError::Forbidden
    } else {
        ImageGenerationError::Provider(message)
    }
}

fn generated_draw_job_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("draw-{nanos}")
}

fn duration_ms(duration: StdDuration) -> i32 {
    duration.as_millis().min(i32::MAX as u128) as i32
}

fn duration_seconds_or_zero(seconds: i32) -> StdDuration {
    u64::try_from(seconds)
        .ok()
        .filter(|seconds| *seconds > 0)
        .map_or(StdDuration::ZERO, StdDuration::from_secs)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex, MutexGuard},
    };

    use openplotva_llm::aifarm::{AifarmHttpFuture, AifarmHttpResponse, CompletionError};
    use openplotva_taskman::{
        DEFAULT_LLM_JOB_MAX_ATTEMPTS, DEFAULT_PRIORITY, HIGHEST_PRIORITY, IMAGE_REGULAR_QUEUE_NAME,
        IMAGE_VIP_QUEUE_NAME, ImageEditJobParams, ImageGenJobParams, ImageJobData, JobPayload,
        JobStatus, LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE, StatelessJobItem,
        TaskQueueJobEvent, TelegramData, new_image_edit_job_at, new_image_gen_job_at,
    };
    use openplotva_telegram::TelegramOutboundMethodKind;
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn execute_image_gen_job_matches_go_sanitize_prompt_caption_and_effect_order() {
        let generator = GeneratorStub::success(" https://img.test/1.png ");
        let effects = EffectsStub::new(Some(777));
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "  neon castle\"\"\"}<tool_call|>ignored ".to_owned(),
                original_text: " original caption<|tool_call>ignored ".to_owned(),
                prompt_variants: vec![
                    "  v1'''<|channel>ignored ".to_owned(),
                    " v2</tool_call>ignored ".to_owned(),
                ],
                is_nsfw: true,
                negative_prompt: " blur ".to_owned(),
                aspect_ratio: " 16:9 ".to_owned(),
                seed: " 42 ".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Completed);
        assert_eq!(report.prompt, "neon castle");
        assert_eq!(report.caption_text, "original caption");
        assert_eq!(report.image_url.as_deref(), Some("https://img.test/1.png"));
        assert_eq!(
            generator.requests(),
            vec![ImageGenerationRequest {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                thread_id: Some(9),
                prompt: "neon castle".to_owned(),
                caption_text: "original caption".to_owned(),
                prompt_variants: vec!["v1".to_owned(), "v2".to_owned()],
                is_nsfw: true,
                negative_prompt: "blur".to_owned(),
                aspect_ratio: "16:9".to_owned(),
                seed: "42".to_owned(),
            }]
        );
        assert_eq!(
            effects.calls(),
            vec![
                "remove_queued:-100:20",
                "send_drawing:-100:20:30:9",
                "remove_drawing:-100:777",
            ]
        );
    }

    #[tokio::test]
    async fn execute_image_gen_job_uses_meta_prompt_and_completes_safety_blocks() {
        let generator = GeneratorStub::forbidden();
        let effects = EffectsStub::new(None);
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                meta: json!({
                    "vision_description": " рыжий ",
                    "attachments": [{"content": "кот"}],
                }),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::SafetyBlocked);
        assert_eq!(report.prompt, "рыжий\n\nкот");
        assert_eq!(report.caption_text, "рыжий\n\nкот");
        assert_eq!(report.error, None);
        assert_eq!(
            effects.calls(),
            vec![
                "remove_queued:-100:20",
                "send_drawing:-100:20:30:0",
                "send_nsfw_blocked:-100:20:30:0:Ваш запрос заблокирован, так как содержит неприемлемый контент. Попробуйте переформулировать запрос.",
            ]
        );
    }

    #[tokio::test]
    async fn execute_image_edit_job_matches_go_sanitize_prompt_and_effect_order() {
        let editor = EditorStub::success(vec![
            " https://img.test/edit-1.png ".to_owned(),
            String::new(),
            "https://img.test/edit-2.png".to_owned(),
        ]);
        let effects = EffectsStub::new(Some(777));
        let report = execute_image_edit_job(
            &editor,
            &effects,
            ImageEditJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: " make it night\"\"\"}<tool_call|>ignored ".to_owned(),
                photo_file_id: "photo-file".to_owned(),
                photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                thread_id: Some(9),
            },
        )
        .await;

        assert_eq!(report.outcome, ImageEditJobExecutionOutcome::Completed);
        assert_eq!(report.prompt, "make it night");
        assert_eq!(
            report.image_urls,
            vec![
                "https://img.test/edit-1.png".to_owned(),
                "https://img.test/edit-2.png".to_owned()
            ]
        );
        assert_eq!(
            editor.requests(),
            vec![ImageEditRequest {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                thread_id: Some(9),
                prompt: "make it night".to_owned(),
                photo_file_id: "photo-file".to_owned(),
                photo_urls: vec!["https://telegram.test/original.png".to_owned()],
            }]
        );
        assert_eq!(
            effects.calls(),
            vec![
                "remove_queued:-100:20",
                "send_drawing:-100:20:30:9",
                "remove_drawing:-100:777",
            ]
        );
    }

    #[tokio::test]
    async fn execute_image_edit_job_completes_safety_blocks() {
        let editor = EditorStub::forbidden();
        let effects = EffectsStub::new(None);
        let report = execute_image_edit_job(
            &editor,
            &effects,
            ImageEditJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                prompt: " edit ".to_owned(),
                ..ImageEditJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageEditJobExecutionOutcome::SafetyBlocked);
        assert_eq!(report.prompt, "edit");
        assert!(report.image_urls.is_empty());
        assert_eq!(report.error, None);
        assert_eq!(
            effects.calls(),
            vec![
                "remove_queued:-100:20",
                "send_drawing:-100:20:30:0",
                "send_nsfw_blocked:-100:20:30:0:Ваш запрос заблокирован, так как содержит неприемлемый контент. Попробуйте переформулировать запрос.",
            ]
        );
    }

    #[tokio::test]
    async fn optimizing_image_generator_applies_go_prompt_optimizer_before_provider() {
        let generator = GeneratorStub::success("https://img.test/1.png");
        let optimizer =
            OptimizerStub::default().with_image_result(openplotva_media::ImageOptimize {
                input: "cat | blur 1:1 seed 42".to_owned(),
                outputs: vec!["plotva swims near a roach".to_owned()],
                aspect_ratio: "16:9".to_owned(),
                nsfw_result: openplotva_media::NsfwResult::Safe,
            });
        let optimizing = OptimizingImageGenerator::new(
            generator.clone(),
            crate::media::MediaPromptOptimizerService::new(Some(optimizer.clone())),
        );

        let result = optimizing
            .generate_image(ImageGenerationRequest {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "cat".to_owned(),
                negative_prompt: "blur".to_owned(),
                aspect_ratio: "1:1".to_owned(),
                seed: "seed 42".to_owned(),
                is_nsfw: true,
                ..ImageGenerationRequest::default()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(
            optimizer.calls(),
            vec!["image:cat | blur 1:1 seed 42:1".to_owned()]
        );
        assert_eq!(
            generator.requests(),
            vec![ImageGenerationRequest {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "cat".to_owned(),
                prompt_variants: vec!["roach-fish swims near a roach-fish".to_owned()],
                is_nsfw: false,
                negative_prompt: "blur".to_owned(),
                aspect_ratio: "16:9".to_owned(),
                seed: "seed 42".to_owned(),
                ..ImageGenerationRequest::default()
            }]
        );
    }

    #[tokio::test]
    async fn optimizing_image_editor_applies_go_edit_optimizer_before_provider() {
        let editor = EditorStub::success(vec!["https://img.test/edit.png".to_owned()]);
        let optimizer =
            OptimizerStub::default().with_edit_result(openplotva_media::ImageEditOptimize {
                input: "make it day".to_owned(),
                outputs: vec!["make the scene bright daylight".to_owned()],
                nsfw_result: openplotva_media::NsfwResult::Safe,
            });
        let optimizing = OptimizingImageEditor::new(
            editor.clone(),
            crate::media::MediaPromptOptimizerService::new(Some(optimizer.clone())),
        );

        let result = optimizing
            .edit_image(ImageEditRequest {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "make it day".to_owned(),
                photo_file_id: "photo-file".to_owned(),
                photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                ..ImageEditRequest::default()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(optimizer.calls(), vec!["edit:make it day:1".to_owned()]);
        assert_eq!(
            editor.requests(),
            vec![ImageEditRequest {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "make the scene bright daylight".to_owned(),
                photo_file_id: "photo-file".to_owned(),
                photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                ..ImageEditRequest::default()
            }]
        );
    }

    #[tokio::test]
    async fn image_gen_queue_worker_completes_and_stores_generated_url() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: -100,
                    message_id: 20,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "castle".to_owned(),
                    ..ImageGenJobParams::default()
                },
                now,
            ),
        );
        let report = run_regular_image_gen_queue_once(
            &queue,
            &GeneratorStub::success("https://img.test/1.png"),
            &EffectsStub::new(Some(777)),
            "image-worker-1",
            now,
        )
        .await;

        assert_eq!(
            report,
            ImageGenQueuePollReport {
                queue_name: IMAGE_REGULAR_QUEUE_NAME.to_owned(),
                job_id: Some(job_id),
                outcome: ImageGenQueuePollOutcome::Completed,
                error: None,
            }
        );
        let record = &queue.records()[0];
        assert_eq!(record.status, JobStatus::Completed);
        assert_eq!(
            record
                .job
                .data
                .image_data
                .as_ref()
                .expect("image data")
                .image_urls,
            vec!["https://img.test/1.png"]
        );
    }

    #[tokio::test]
    async fn image_gen_queue_worker_requeues_retryable_provider_error_like_go() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: -100,
                    message_id: 20,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "castle".to_owned(),
                    ..ImageGenJobParams::default()
                },
                now,
            ),
        );

        let report = run_regular_image_gen_queue_once(
            &queue,
            &GeneratorStub::error("aifarm provider provider_unavailable: status 503"),
            &EffectsStub::new(Some(777)),
            "image-worker-1",
            now,
        )
        .await;

        assert_eq!(report.outcome, ImageGenQueuePollOutcome::RetryRequeued);
        assert_eq!(report.error, None);
        let record = queue.record(job_id).expect("job");
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.queue_name, IMAGE_REGULAR_QUEUE_NAME);
        assert_eq!(record.error, None);
        assert_eq!(record.worker_id, None);
        assert_eq!(record.started_at, None);
        assert_eq!(record.completed_at, None);
        assert_eq!(record.events.len(), 1);
        let event = &record.events[0];
        assert_eq!(event.stage, LLM_JOB_RETRY_STAGE);
        assert_eq!(event.attempt, 1);
        assert_eq!(event.provider, "aifarm");
        assert_eq!(
            event.message,
            "retryable LLM provider error, requeueing job"
        );
        assert_eq!(
            event.error,
            "aifarm provider provider_unavailable: status 503"
        );
        assert_eq!(event.data["fallback_reason"], "provider_unavailable");
        assert_eq!(
            event.data["max_attempts"],
            DEFAULT_LLM_JOB_MAX_ATTEMPTS.to_string()
        );
        assert_eq!(event.data["target_queue"], IMAGE_REGULAR_QUEUE_NAME);
    }

    #[tokio::test]
    async fn image_edit_queue_worker_completes_and_stores_generated_urls() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_edit_job_at(
                ImageEditJobParams {
                    chat_id: -100,
                    message_id: 20,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "make it night".to_owned(),
                    photo_file_id: "photo-file".to_owned(),
                    photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                    thread_id: Some(9),
                },
                now,
            )
            .with_name("image_edit")
            .with_priority(HIGHEST_PRIORITY),
        );

        let report = run_image_edit_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &EditorStub::success(vec!["https://img.test/edit.png".to_owned()]),
            &EffectsStub::new(Some(777)),
            "image-edit-worker-1",
            now,
        )
        .await;

        assert_eq!(
            report,
            ImageEditQueuePollReport {
                queue_name: IMAGE_VIP_QUEUE_NAME.to_owned(),
                job_id: Some(job_id),
                outcome: ImageEditQueuePollOutcome::Completed,
                error: None,
            }
        );
        let record = &queue.records()[0];
        assert_eq!(record.status, JobStatus::Completed);
        assert_eq!(
            record
                .job
                .data
                .image_data
                .as_ref()
                .expect("image data")
                .image_urls,
            vec!["https://img.test/edit.png"]
        );
    }

    #[tokio::test]
    async fn image_edit_queue_worker_requeues_retryable_provider_error_like_go() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        let job_id = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_edit_job_at(
                ImageEditJobParams {
                    chat_id: -100,
                    message_id: 20,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "make it night".to_owned(),
                    photo_file_id: "photo-file".to_owned(),
                    photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                    thread_id: Some(9),
                },
                now,
            )
            .with_name("image_edit")
            .with_priority(HIGHEST_PRIORITY),
        );

        let report = run_image_edit_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &EditorStub::error("aifarm provider provider_unavailable: status 503"),
            &EffectsStub::new(Some(777)),
            "image-edit-worker-1",
            now,
        )
        .await;

        assert_eq!(report.outcome, ImageEditQueuePollOutcome::RetryRequeued);
        assert_eq!(report.error, None);
        let record = queue.record(job_id).expect("job");
        assert_eq!(record.status, JobStatus::Pending);
        assert_eq!(record.queue_name, IMAGE_VIP_QUEUE_NAME);
        assert_eq!(record.error, None);
        assert_eq!(record.worker_id, None);
        assert_eq!(record.started_at, None);
        assert_eq!(record.completed_at, None);
        assert_eq!(record.events.len(), 1);
        let event = &record.events[0];
        assert_eq!(event.stage, LLM_JOB_RETRY_STAGE);
        assert_eq!(event.attempt, 1);
        assert_eq!(event.provider, "aifarm");
        assert_eq!(
            event.message,
            "retryable LLM provider error, requeueing job"
        );
        assert_eq!(
            event.error,
            "aifarm provider provider_unavailable: status 503"
        );
        assert_eq!(event.data["fallback_reason"], "provider_unavailable");
        assert_eq!(
            event.data["max_attempts"],
            DEFAULT_LLM_JOB_MAX_ATTEMPTS.to_string()
        );
        assert_eq!(event.data["target_queue"], IMAGE_VIP_QUEUE_NAME);
    }

    #[tokio::test]
    async fn image_workers_use_configured_retry_attempt_limit_like_go() {
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");

        let gen_queue = InMemoryTaskQueue::new();
        let gen_job_id = gen_queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            new_image_gen_job_at(
                ImageGenJobParams {
                    chat_id: -100,
                    message_id: 20,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "castle".to_owned(),
                    ..ImageGenJobParams::default()
                },
                now,
            ),
        );
        gen_queue
            .append_job_event(
                gen_job_id,
                TaskQueueJobEvent {
                    stage: LLM_JOB_RETRY_STAGE.to_owned(),
                    attempt: 1,
                    ..TaskQueueJobEvent::default()
                },
                now,
            )
            .expect("seed image retry event");

        let gen_report = run_image_gen_queue_once_with_max_attempts(
            &gen_queue,
            IMAGE_REGULAR_QUEUE_NAME,
            &GeneratorStub::error("aifarm provider provider_unavailable: status 503"),
            &EffectsStub::new(Some(777)),
            "image-worker-1",
            2,
            now,
        )
        .await;

        assert_eq!(gen_report.outcome, ImageGenQueuePollOutcome::RetryExhausted);
        let gen_record = gen_queue.record(gen_job_id).expect("image gen job");
        assert_eq!(gen_record.status, JobStatus::Failed);
        let gen_event = gen_record.events.last().expect("exhaustion event");
        assert_eq!(gen_event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
        assert_eq!(gen_event.attempt, 2);
        assert_eq!(gen_event.data["max_attempts"], "2");

        let edit_queue = InMemoryTaskQueue::new();
        let edit_job_id = edit_queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_edit_job_at(
                ImageEditJobParams {
                    chat_id: -100,
                    message_id: 21,
                    user_id: 30,
                    user_full_name: "Alice".to_owned(),
                    prompt: "make it night".to_owned(),
                    photo_file_id: "photo-file".to_owned(),
                    photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                    thread_id: Some(9),
                },
                now,
            )
            .with_name("image_edit")
            .with_priority(HIGHEST_PRIORITY),
        );
        edit_queue
            .append_job_event(
                edit_job_id,
                TaskQueueJobEvent {
                    stage: LLM_JOB_RETRY_STAGE.to_owned(),
                    attempt: 1,
                    ..TaskQueueJobEvent::default()
                },
                now,
            )
            .expect("seed image edit retry event");

        let edit_report = run_image_edit_queue_once_with_max_attempts(
            &edit_queue,
            IMAGE_VIP_QUEUE_NAME,
            &EditorStub::error("aifarm provider provider_unavailable: status 503"),
            &EffectsStub::new(Some(777)),
            "image-edit-worker-1",
            2,
            now,
        )
        .await;

        assert_eq!(
            edit_report.outcome,
            ImageEditQueuePollOutcome::RetryExhausted
        );
        let edit_record = edit_queue.record(edit_job_id).expect("image edit job");
        assert_eq!(edit_record.status, JobStatus::Failed);
        let edit_event = edit_record.events.last().expect("exhaustion event");
        assert_eq!(edit_event.stage, LLM_JOB_RETRY_EXHAUSTED_STAGE);
        assert_eq!(edit_event.attempt, 2);
        assert_eq!(edit_event.data["max_attempts"], "2");
    }

    #[tokio::test]
    async fn image_edit_queue_worker_fails_decode_and_provider_errors() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::UNIX_EPOCH;
        let missing_image = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            StatelessJobItem {
                title: "broken".to_owned(),
                created: now,
                priority: DEFAULT_PRIORITY,
                processing_timeout_seconds: 0,
                data: JobPayload {
                    job_type: JobType::ImageEdit,
                    telegram_data: Some(TelegramData {
                        chat_id: -100,
                        user_id: 30,
                        message_id: 20,
                        thread_message_id: None,
                        user_full_name: "Alice".to_owned(),
                        chat_title: String::new(),
                    }),
                    image_data: None,
                    music_data: None,
                    dialog_data: None,
                    control_data: None,
                },
            },
        );
        let failed = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            new_image_edit_job_at(
                ImageEditJobParams {
                    chat_id: -100,
                    message_id: 21,
                    user_id: 30,
                    prompt: "edit".to_owned(),
                    ..ImageEditJobParams::default()
                },
                now,
            ),
        );

        let decode_report = run_image_edit_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &EditorStub::success(vec!["ignored".to_owned()]),
            &EffectsStub::new(None),
            "image-edit-worker-1",
            now,
        )
        .await;
        let provider_report = run_image_edit_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &EditorStub::error("provider down"),
            &EffectsStub::new(None),
            "image-edit-worker-1",
            now,
        )
        .await;

        assert_eq!(decode_report.job_id, Some(missing_image));
        assert_eq!(
            decode_report.outcome,
            ImageEditQueuePollOutcome::DecodeFailed
        );
        assert_eq!(provider_report.job_id, Some(failed));
        assert_eq!(provider_report.outcome, ImageEditQueuePollOutcome::Failed);
        let records = queue.records();
        assert_eq!(records[0].status, JobStatus::Failed);
        assert_eq!(records[1].status, JobStatus::Failed);
        assert_eq!(records[1].error.as_deref(), Some("provider down"));
    }

    #[tokio::test]
    async fn image_gen_queue_worker_fails_decode_and_provider_errors() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::UNIX_EPOCH;
        let missing_image = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            StatelessJobItem {
                title: "broken".to_owned(),
                created: now,
                priority: DEFAULT_PRIORITY,
                processing_timeout_seconds: 0,
                data: JobPayload {
                    job_type: JobType::ImageGen,
                    telegram_data: Some(TelegramData {
                        chat_id: -100,
                        user_id: 30,
                        message_id: 20,
                        thread_message_id: None,
                        user_full_name: "Alice".to_owned(),
                        chat_title: String::new(),
                    }),
                    image_data: None,
                    music_data: None,
                    dialog_data: None,
                    control_data: None,
                },
            },
        );
        let failed = queue.assign(
            IMAGE_VIP_QUEUE_NAME,
            StatelessJobItem {
                title: "image".to_owned(),
                created: now,
                priority: DEFAULT_PRIORITY,
                processing_timeout_seconds: 0,
                data: JobPayload {
                    job_type: JobType::ImageGen,
                    telegram_data: Some(TelegramData {
                        chat_id: -100,
                        user_id: 30,
                        message_id: 21,
                        thread_message_id: None,
                        user_full_name: "Alice".to_owned(),
                        chat_title: String::new(),
                    }),
                    image_data: Some(ImageJobData {
                        prompt: "castle".to_owned(),
                        ..ImageJobData::default()
                    }),
                    music_data: None,
                    dialog_data: None,
                    control_data: None,
                },
            },
        );

        let decode_report = run_image_gen_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &GeneratorStub::success("ignored"),
            &EffectsStub::new(None),
            "image-worker-1",
            now,
        )
        .await;
        let provider_report = run_image_gen_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &GeneratorStub::error("provider down"),
            &EffectsStub::new(None),
            "image-worker-1",
            now,
        )
        .await;

        assert_eq!(decode_report.job_id, Some(missing_image));
        assert_eq!(
            decode_report.outcome,
            ImageGenQueuePollOutcome::DecodeFailed
        );
        assert_eq!(provider_report.job_id, Some(failed));
        assert_eq!(provider_report.outcome, ImageGenQueuePollOutcome::Failed);
        let records = queue.records();
        assert_eq!(records[0].status, JobStatus::Failed);
        assert_eq!(records[1].status, JobStatus::Failed);
        assert_eq!(records[1].error.as_deref(), Some("provider down"));
    }

    #[tokio::test]
    async fn image_gen_queue_worker_leaves_other_jobs_pending_and_reports_idle() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::UNIX_EPOCH;
        queue.assign(
            IMAGE_REGULAR_QUEUE_NAME,
            StatelessJobItem {
                title: "not-image".to_owned(),
                created: now,
                priority: DEFAULT_PRIORITY,
                processing_timeout_seconds: 0,
                data: JobPayload {
                    job_type: JobType::Dialog,
                    telegram_data: None,
                    image_data: None,
                    music_data: None,
                    dialog_data: None,
                    control_data: None,
                },
            },
        );

        let report = run_regular_image_gen_queue_once(
            &queue,
            &GeneratorStub::success("ignored"),
            &EffectsStub::new(None),
            "image-worker-1",
            now,
        )
        .await;

        assert_eq!(report.outcome, ImageGenQueuePollOutcome::Idle);
        assert_eq!(queue.records()[0].status, JobStatus::Pending);
    }

    #[test]
    fn image_gen_worker_run_report_counts_queue_ticks() {
        let mut report = ImageGenWorkerRunReport::default();
        report.record_poll(&ImageGenQueuePollReport {
            queue_name: IMAGE_VIP_QUEUE_NAME.to_owned(),
            job_id: Some(1),
            outcome: ImageGenQueuePollOutcome::SafetyBlocked,
            error: None,
        });
        report.record_poll(&ImageGenQueuePollReport {
            queue_name: IMAGE_REGULAR_QUEUE_NAME.to_owned(),
            job_id: Some(2),
            outcome: ImageGenQueuePollOutcome::Failed,
            error: Some("provider down".to_owned()),
        });
        report.record_poll(&ImageGenQueuePollReport {
            queue_name: IMAGE_REGULAR_QUEUE_NAME.to_owned(),
            job_id: None,
            outcome: ImageGenQueuePollOutcome::Idle,
            error: None,
        });

        assert_eq!(
            report,
            ImageGenWorkerRunReport {
                ticks: 3,
                dequeued: 2,
                completed: 0,
                safety_blocked: 1,
                retry_requeued: 0,
                retry_exhausted: 0,
                failed: 1,
                decode_failed: 0,
                idle: 1,
                errors: 1,
            }
        );
    }

    #[test]
    fn draw_api_prompt_text_matches_go_variant_and_option_fallback() {
        let with_variant = ImageGenerationRequest {
            prompt: "castle".to_owned(),
            prompt_variants: vec!["  ".to_owned(), " neon castle ".to_owned()],
            negative_prompt: "blur".to_owned(),
            aspect_ratio: "16:9".to_owned(),
            seed: "42".to_owned(),
            ..ImageGenerationRequest::default()
        };
        let without_variant = ImageGenerationRequest {
            prompt: "castle".to_owned(),
            negative_prompt: "blur".to_owned(),
            aspect_ratio: "16:9".to_owned(),
            seed: "42".to_owned(),
            ..ImageGenerationRequest::default()
        };

        assert_eq!(draw_api_prompt_text(&with_variant), "neon castle");
        assert_eq!(
            draw_api_prompt_text(&without_variant),
            "castle | blur 16:9 42"
        );
    }

    #[test]
    fn pruna_config_from_app_config_maps_go_pruna_env() -> Result<(), openplotva_config::ConfigError>
    {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            pruna_endpoint: Some(" https://pruna.test/replicate ".to_owned()),
            pruna_model: Some(" test/pruna ".to_owned()),
            pruna_api_key: Some(" api-key ".to_owned()),
            pruna_bearer: Some(" bearer-token ".to_owned()),
            pruna_timeout_seconds: Some("45".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let pruna = pruna_config_from_app_config(&config);

        assert_eq!(pruna.endpoint, "https://pruna.test/replicate");
        assert_eq!(pruna.model, "test/pruna");
        assert_eq!(pruna.api_key, "api-key");
        assert_eq!(pruna.bearer, "bearer-token");
        assert_eq!(pruna.timeout, StdDuration::from_secs(45));
        assert!(pruna.configured());
        Ok(())
    }

    #[test]
    fn modelscope_config_from_app_config_maps_go_env() -> Result<(), openplotva_config::ConfigError>
    {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            modelscope_key: Some(" modelscope-key ".to_owned()),
            modelscope_base_url: Some(" https://modelscope.test/api/ ".to_owned()),
            modelscope_poll_interval_seconds: Some("7".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let modelscope = modelscope_config_from_app_config(&config);

        assert_eq!(modelscope.api_key, "modelscope-key");
        assert_eq!(modelscope.base_url, "https://modelscope.test/api");
        assert_eq!(modelscope.poll_interval, StdDuration::from_secs(7));
        assert_eq!(
            modelscope.request_timeout,
            openplotva_media::modelscope::DEFAULT_REQUEST_TIMEOUT
        );
        assert!(modelscope.configured());
        Ok(())
    }

    #[test]
    fn together_config_from_app_config_maps_go_env() -> Result<(), openplotva_config::ConfigError> {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            together_key: Some(" together-key ".to_owned()),
            together_rate_limit_seconds: Some("13".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let together = together_config_from_app_config(&config);

        assert_eq!(together.api_key, "together-key");
        assert_eq!(
            together.base_url,
            openplotva_media::together::DEFAULT_BASE_URL
        );
        assert_eq!(together.rate_limit_duration, StdDuration::from_secs(13));
        assert_eq!(
            together.request_timeout,
            openplotva_media::together::DEFAULT_REQUEST_TIMEOUT
        );
        assert!(together.configured());
        Ok(())
    }

    #[test]
    fn together_config_from_app_config_falls_back_to_key_pool()
    -> Result<(), openplotva_config::ConfigError> {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            together_key: Some(" ".to_owned()),
            together_keys: Some(" pool-key-1,pool-key-2 ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let together = together_config_from_app_config(&config);

        assert_eq!(together.api_key, "pool-key-1");
        assert!(together.configured());
        Ok(())
    }

    #[test]
    fn aihorde_config_from_app_config_maps_go_env() -> Result<(), openplotva_config::ConfigError> {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            aihorde_api_key: Some(" horde-key ".to_owned()),
            aihorde_base_url: Some(" https://aihorde.test/api/ ".to_owned()),
            aihorde_client_agent: Some(" openplotva:test ".to_owned()),
            aihorde_timeout_seconds: Some("66".to_owned()),
            aihorde_poll_interval_seconds: Some("4".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;

        let horde = aihorde_config_from_app_config(&config);

        assert_eq!(horde.api_key, "horde-key");
        assert_eq!(horde.base_url, "https://aihorde.test/api");
        assert_eq!(horde.client_agent, "openplotva:test");
        assert_eq!(horde.request_timeout, StdDuration::from_secs(66));
        assert_eq!(horde.poll_interval, StdDuration::from_secs(4));
        assert!(horde.configured());
        Ok(())
    }

    #[test]
    fn image_provider_request_helpers_preserve_go_dimensions_and_seed() {
        assert_eq!(
            image_dimensions_from_aspect_ratio("16:9", 1024),
            (1024, 576)
        );
        assert_eq!(image_dimensions_from_aspect_ratio("9:16", 576), (324, 576));
        assert_eq!(image_dimensions_from_aspect_ratio("", 576), (576, 576));
        assert_eq!(parse_image_seed("seed 42"), Some(42));
        assert_eq!(parse_image_seed("  -7  "), Some(-7));
        assert_eq!(parse_image_seed("random"), None);
    }

    #[test]
    fn decode_draw_api_result_payload_accepts_go_shapes() {
        let image = general_purpose::STANDARD.encode([1_u8, 2, 3]);
        let images = decode_draw_api_result_payload(
            &serde_json::to_vec(&json!({"image_b64": [" ", image]})).expect("payload"),
        )
        .expect("image result");
        let urls = decode_draw_api_result_payload(
            &serde_json::to_vec(&json!({
                "image_b64": "bad-base64",
                "image_url": [" https://img.test/1.png ", ""]
            }))
            .expect("payload"),
        )
        .expect("url result");
        let string_image = decode_draw_api_result_payload(
            &serde_json::to_vec(&json!(general_purpose::STANDARD.encode([4_u8, 5])))
                .expect("payload"),
        )
        .expect("string image result");

        assert_eq!(images.images, vec![vec![1, 2, 3]]);
        assert_eq!(urls.urls, vec!["https://img.test/1.png"]);
        assert_eq!(string_image.images, vec![vec![4, 5]]);
    }

    #[tokio::test]
    async fn aifarm_draw_api_generator_submits_go_shaped_job_and_polls_result() {
        let draw_payload = serde_json::to_vec(&json!({"image_url": [" https://img.test/1.png "]}))
            .expect("draw payload");
        let response_body = general_purpose::STANDARD.encode(draw_payload);
        let transport = AifarmTransportStub::new(vec![
            Ok(json_response(
                json!({"job_id": "job-1", "state": "processing"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "job-1",
                    "state": "completed",
                    "result": {
                        "response": {
                            "status_code": 200,
                            "body": response_body
                        }
                    }
                }
            }))),
        ]);
        let probe = transport.clone();
        let generator = AifarmDrawApiImageGenerator::with_transport(
            AifarmDrawApiConfig {
                base_url: "https://draw.example.test".to_owned(),
                timeout: StdDuration::from_secs(5),
                poll_interval: StdDuration::from_nanos(1),
            },
            transport,
        );

        let result = generator
            .generate_image_with_job_id(
                ImageGenerationRequest {
                    prompt: "castle".to_owned(),
                    caption_text: "caption".to_owned(),
                    prompt_variants: vec![" variant one ".to_owned()],
                    negative_prompt: "blur".to_owned(),
                    aspect_ratio: "16:9".to_owned(),
                    seed: "42".to_owned(),
                    ..ImageGenerationRequest::default()
                },
                "draw-123",
            )
            .await
            .expect("draw result");

        assert_eq!(result.image_url, "https://img.test/1.png");
        assert_eq!(
            result.first_image_url().as_deref(),
            Some("https://img.test/1.png")
        );
        let requests = probe.requests();
        assert_eq!(requests[0].method, AifarmHttpMethod::Post);
        assert_eq!(requests[0].url, "https://draw.example.test/v1/jobs");
        assert_eq!(requests[0].headers["Content-Type"], "application/json");
        let job: DiscoveryJobRequest =
            serde_json::from_slice(&requests[0].body).expect("job request");
        assert_eq!(job.idempotency_key, "draw-123");
        assert_eq!(job.priority, 0);
        assert_eq!(job.invocation.service_name, AIFARM_DRAW_API_SERVICE_NAME);
        assert_eq!(job.invocation.endpoint_name, AIFARM_DRAW_API_ENDPOINT_NAME);
        assert_eq!(job.invocation.headers["X-Request-Id"], "draw-123");
        assert_eq!(job.invocation.timeout_ms, 5000);
        let draw_body = decode_discovery_body(&job.invocation.body).expect("draw body");
        let draw_request: Value = serde_json::from_slice(&draw_body).expect("draw request");
        assert_eq!(draw_request, json!({"prompt": "variant one"}));
        assert_eq!(requests[1].method, AifarmHttpMethod::Get);
        assert_eq!(requests[1].url, "https://draw.example.test/v1/jobs/job-1");
    }

    #[tokio::test]
    async fn aifarm_draw_api_editor_submits_go_shaped_image_url_payload() {
        let draw_payload = serde_json::to_vec(&json!({"image_url": "https://img.test/edit.png"}))
            .expect("draw payload");
        let response_body = general_purpose::STANDARD.encode(draw_payload);
        let transport = AifarmTransportStub::new(vec![
            Ok(json_response(
                json!({"job_id": "edit-1", "state": "queued"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "edit-1",
                    "state": "completed",
                    "result": {
                        "response": {
                            "status_code": 200,
                            "body": response_body
                        }
                    }
                }
            }))),
        ]);
        let probe = transport.clone();
        let editor = AifarmDrawApiImageGenerator::with_transport(
            AifarmDrawApiConfig {
                base_url: "https://draw.example.test".to_owned(),
                timeout: StdDuration::from_secs(5),
                poll_interval: StdDuration::from_nanos(1),
            },
            transport,
        );

        let result = editor
            .edit_image_with_job_id(
                ImageEditRequest {
                    prompt: " make it night ".to_owned(),
                    photo_urls: vec![
                        " https://files.test/input.png ".to_owned(),
                        "raw-b64-ignored-because-url-wins".to_owned(),
                    ],
                    ..ImageEditRequest::default()
                },
                "edit-123",
            )
            .await
            .expect("edit result");

        assert_eq!(result.image_urls, vec!["https://img.test/edit.png"]);
        let requests = probe.requests();
        let job: DiscoveryJobRequest =
            serde_json::from_slice(&requests[0].body).expect("job request");
        assert_eq!(job.idempotency_key, "edit-123");
        let draw_body = decode_discovery_body(&job.invocation.body).expect("draw body");
        let draw_request: Value = serde_json::from_slice(&draw_body).expect("draw request");
        assert_eq!(
            draw_request,
            json!({
                "prompt": "make it night",
                "image_url": "https://files.test/input.png"
            })
        );
    }

    #[tokio::test]
    async fn resolving_image_editor_turns_telegram_file_data_url_into_draw_api_b64() {
        let draw_payload = serde_json::to_vec(&json!({"image_url": ["https://img.test/edit.png"]}))
            .expect("draw payload");
        let response_body = general_purpose::STANDARD.encode(draw_payload);
        let transport = AifarmTransportStub::new(vec![
            Ok(json_response(
                json!({"job_id": "edit-2", "state": "queued"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "edit-2",
                    "state": "completed",
                    "result": {
                        "response": {
                            "status_code": 200,
                            "body": response_body
                        }
                    }
                }
            }))),
        ]);
        let probe = transport.clone();
        let editor = ResolvingImageEditor::new(
            DataUrlStub::success("data:image/png;base64,ZmFrZS1wbmc="),
            AifarmDrawApiImageGenerator::with_transport(
                AifarmDrawApiConfig {
                    base_url: "https://draw.example.test".to_owned(),
                    timeout: StdDuration::from_secs(5),
                    poll_interval: StdDuration::from_nanos(1),
                },
                transport,
            ),
        );

        let result = editor
            .edit_image(ImageEditRequest {
                prompt: "change style".to_owned(),
                photo_file_id: "telegram-file".to_owned(),
                ..ImageEditRequest::default()
            })
            .await
            .expect("edit result");

        assert_eq!(result.image_urls, vec!["https://img.test/edit.png"]);
        let requests = probe.requests();
        let job: DiscoveryJobRequest =
            serde_json::from_slice(&requests[0].body).expect("job request");
        let draw_body = decode_discovery_body(&job.invocation.body).expect("draw body");
        let draw_request: Value = serde_json::from_slice(&draw_body).expect("draw request");
        assert_eq!(
            draw_request,
            json!({
                "prompt": "change style",
                "image_b64": "ZmFrZS1wbmc="
            })
        );
    }

    #[test]
    fn drawing_sticker_request_matches_external_artifact_contract() {
        let (request, reply) = drawing_sticker_message_request(-10042, 77, 30, Some(9));
        let plan =
            openplotva_telegram::build_sticker_message_plan(&request, Some(&reply)).expect("plan");

        assert_eq!(plan.chat_id, -10042);
        assert_eq!(plan.file_id, STICKER_DRAW_FILE_ID);
        assert!(plan.disable_notification);
        assert_eq!(
            plan.message_thread_id, None,
            "Image jobs construct a minimal reply message, so topic id is not copied here"
        );
        assert_eq!(
            plan.reply_parameters,
            Some(openplotva_telegram::ReplyParametersPlan {
                message_id: 77,
                chat_id: -10042,
                allow_sending_without_reply: true,
            })
        );
    }

    #[tokio::test]
    async fn telegram_image_job_effects_send_drawing_sticker_tracks_ephemeral_message() {
        let queued = QueuedStickerStoreStub::default();
        let ephemeral = EphemeralTrackerStub::default();
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Message(
            Box::new(telegram_message(-10042, 555)),
        ))]);
        let effects =
            TelegramImageJobEffects::new(queued.clone(), ephemeral.clone(), telegram.clone());

        let sticker_id = effects.send_drawing_sticker(-10042, 77, 30, Some(9)).await;

        assert_eq!(sticker_id, Some(555));
        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::SendSticker]
        );
        assert_eq!(
            ephemeral.tracked(),
            vec![(-10042, 555, DRAWING_STICKER_DELETE_AFTER)]
        );
        assert!(queued.snapshot().loads.is_empty());
    }

    #[tokio::test]
    async fn telegram_image_job_effects_remove_queued_sticker_deletes_message_and_key() {
        let queued = QueuedStickerStoreStub::with_queued_id(Some(444));
        let ephemeral = EphemeralTrackerStub::default();
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Boolean(true))]);
        let effects =
            TelegramImageJobEffects::new(queued.clone(), ephemeral.clone(), telegram.clone());

        effects.remove_queued_sticker(-10042, 77).await;

        let snapshot = queued.snapshot();
        assert_eq!(snapshot.loads, vec![(-10042, 77)]);
        assert_eq!(snapshot.deletes, vec![(-10042, 77)]);
        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::DeleteMessage]
        );
        assert!(ephemeral.tracked().is_empty());
    }

    #[tokio::test]
    async fn telegram_image_job_effects_remove_queued_sticker_keeps_go_missing_record_behavior() {
        let queued = QueuedStickerStoreStub::with_queued_id(None);
        let telegram = TelegramSenderStub::new(Vec::new());
        let effects = TelegramImageJobEffects::new(
            queued.clone(),
            EphemeralTrackerStub::default(),
            telegram.clone(),
        );

        effects.remove_queued_sticker(-10042, 77).await;

        let snapshot = queued.snapshot();
        assert_eq!(snapshot.loads, vec![(-10042, 77)]);
        assert!(snapshot.deletes.is_empty());
        assert!(telegram.kinds().is_empty());
    }

    #[tokio::test]
    async fn telegram_image_job_effects_remove_queued_sticker_deletes_zero_id_key_only() {
        let queued = QueuedStickerStoreStub::with_queued_id(Some(0));
        let telegram = TelegramSenderStub::new(Vec::new());
        let effects = TelegramImageJobEffects::new(
            queued.clone(),
            EphemeralTrackerStub::default(),
            telegram.clone(),
        );

        effects.remove_queued_sticker(-10042, 77).await;

        let snapshot = queued.snapshot();
        assert_eq!(snapshot.loads, vec![(-10042, 77)]);
        assert_eq!(snapshot.deletes, vec![(-10042, 77)]);
        assert!(telegram.kinds().is_empty());
    }

    #[tokio::test]
    async fn telegram_image_job_effects_send_nsfw_blocked_message_sends_reply() {
        let queued = QueuedStickerStoreStub::default();
        let ephemeral = EphemeralTrackerStub::default();
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Message(
            Box::new(telegram_message(-10042, 556)),
        ))]);
        let effects =
            TelegramImageJobEffects::new(queued.clone(), ephemeral.clone(), telegram.clone());

        effects
            .send_nsfw_blocked_message(-10042, 77, 30, Some(9))
            .await;

        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::SendMessage]
        );
        assert!(queued.snapshot().loads.is_empty());
        assert!(ephemeral.tracked().is_empty());
    }

    #[derive(Clone, Debug)]
    struct AifarmTransportStub {
        state: Arc<Mutex<AifarmTransportState>>,
    }

    #[derive(Debug)]
    struct AifarmTransportState {
        requests: Vec<AifarmHttpRequest>,
        responses: VecDeque<Result<AifarmHttpResponse, CompletionError>>,
    }

    impl AifarmTransportStub {
        fn new(responses: Vec<Result<AifarmHttpResponse, CompletionError>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(AifarmTransportState {
                    requests: Vec::new(),
                    responses: responses.into(),
                })),
            }
        }

        fn requests(&self) -> Vec<AifarmHttpRequest> {
            self.state.lock().expect("aifarm state").requests.clone()
        }
    }

    impl AifarmHttpTransport for AifarmTransportStub {
        fn send<'a>(&'a self, request: AifarmHttpRequest) -> AifarmHttpFuture<'a> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("aifarm state");
                state.requests.push(request);
                state
                    .responses
                    .pop_front()
                    .unwrap_or_else(|| Ok(AifarmHttpResponse::default()))
            })
        }
    }

    fn json_response(value: Value) -> AifarmHttpResponse {
        AifarmHttpResponse {
            status_code: 200,
            status_text: "OK".to_owned(),
            body: serde_json::to_vec(&value).expect("json response"),
            ..AifarmHttpResponse::default()
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct StubError(&'static str);

    impl std::fmt::Display for StubError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    #[derive(Clone, Debug)]
    struct DataUrlStub {
        result: Result<String, StubError>,
        requested: Arc<Mutex<Vec<String>>>,
    }

    impl DataUrlStub {
        fn success(data_url: impl Into<String>) -> Self {
            Self {
                result: Ok(data_url.into()),
                requested: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl TelegramVisionDataUrlProvider for DataUrlStub {
        type Error = StubError;

        fn telegram_file_data_url<'a>(
            &'a self,
            latest_file_id: &'a str,
        ) -> crate::vision::TelegramVisionDataUrlFuture<'a, Self::Error> {
            self.requested
                .lock()
                .expect("requested files")
                .push(latest_file_id.to_owned());
            let result = self.result.clone();
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct QueuedStickerStoreStub {
        state: Arc<Mutex<QueuedStickerState>>,
    }

    #[derive(Clone, Debug, Default)]
    struct QueuedStickerState {
        queued_id: Option<i64>,
        loads: Vec<(i64, i32)>,
        deletes: Vec<(i64, i32)>,
    }

    impl QueuedStickerStoreStub {
        fn with_queued_id(queued_id: Option<i64>) -> Self {
            Self {
                state: Arc::new(Mutex::new(QueuedStickerState {
                    queued_id,
                    ..QueuedStickerState::default()
                })),
            }
        }

        fn snapshot(&self) -> QueuedStickerState {
            self.state.lock().expect("queued sticker state").clone()
        }
    }

    impl QueuedStickerStore for QueuedStickerStoreStub {
        type Error = StubError;

        fn queued_sticker_message_id<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
        ) -> ImageJobEffectFuture<'a, Result<Option<i64>, Self::Error>> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("queued sticker state");
                state.loads.push((chat_id, message_id));
                Ok(state.queued_id)
            })
        }

        fn delete_queued_sticker<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
        ) -> ImageJobEffectFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("queued sticker state")
                    .deletes
                    .push((chat_id, message_id));
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct EphemeralTrackerStub {
        tracked: Arc<Mutex<Vec<(i64, i32, StdDuration)>>>,
    }

    impl EphemeralTrackerStub {
        fn tracked(&self) -> Vec<(i64, i32, StdDuration)> {
            self.tracked.lock().expect("ephemeral state").clone()
        }
    }

    impl crate::virtual_messages::EphemeralMessageTracker for EphemeralTrackerStub {
        type Error = StubError;

        fn track_ephemeral_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            delete_after: StdDuration,
            _now: OffsetDateTime,
        ) -> ImageJobEffectFuture<'a, Result<(), Self::Error>> {
            Box::pin(async move {
                self.tracked.lock().expect("ephemeral state").push((
                    chat_id,
                    message_id,
                    delete_after,
                ));
                Ok(())
            })
        }
    }

    #[derive(Clone, Debug)]
    struct TelegramSenderStub {
        state: Arc<Mutex<TelegramSenderState>>,
    }

    #[derive(Debug)]
    struct TelegramSenderState {
        kinds: Vec<TelegramOutboundMethodKind>,
        responses: VecDeque<Result<TelegramOutboundResponse, String>>,
    }

    impl TelegramSenderStub {
        fn new(responses: Vec<Result<TelegramOutboundResponse, String>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(TelegramSenderState {
                    kinds: Vec::new(),
                    responses: responses.into(),
                })),
            }
        }

        fn kinds(&self) -> Vec<TelegramOutboundMethodKind> {
            self.state.lock().expect("telegram state").kinds.clone()
        }
    }

    impl ImageJobTelegramSender for TelegramSenderStub {
        fn send_image_job_method<'a>(
            &'a self,
            method: TelegramOutboundMethod,
        ) -> ImageJobEffectFuture<'a, Result<TelegramOutboundResponse, String>> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("telegram state");
                state.kinds.push(method.kind());
                state
                    .responses
                    .pop_front()
                    .unwrap_or(Ok(TelegramOutboundResponse::Boolean(true)))
            })
        }
    }

    fn telegram_message(chat_id: i64, message_id: i64) -> carapax::types::Message {
        serde_json::from_value(json!({
            "message_id": message_id,
            "date": 0,
            "chat": {
                "type": "private",
                "id": chat_id,
                "first_name": "Plotva",
            },
            "from": {
                "id": 1,
                "is_bot": true,
                "first_name": "Plotva",
            },
        }))
        .expect("telegram message")
    }

    #[tokio::test]
    async fn fallback_image_generator_uses_primary_success_without_fallback() {
        let primary = GeneratorStub::success("https://pruna.test/1.png");
        let fallback = GeneratorStub::success("https://drawapi.test/1.png");
        let generator = FallbackImageGenerator::new(primary.clone(), fallback.clone());

        let result = generator
            .generate_image(ImageGenerationRequest {
                prompt: "quiet lake".to_owned(),
                caption_text: "quiet lake".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await
            .expect("primary result");

        assert_eq!(result.image_url, "https://pruna.test/1.png");
        assert_eq!(primary.requests().len(), 1);
        assert!(fallback.requests().is_empty());
    }

    #[tokio::test]
    async fn fallback_image_generator_falls_back_after_primary_provider_failure() {
        let primary = GeneratorStub::error("pruna unavailable");
        let fallback = GeneratorStub::success("https://drawapi.test/1.png");
        let generator = FallbackImageGenerator::new(primary.clone(), fallback.clone());

        let result = generator
            .generate_image(ImageGenerationRequest {
                prompt: "quiet lake".to_owned(),
                caption_text: "quiet lake".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await
            .expect("fallback result");

        assert_eq!(result.image_url, "https://drawapi.test/1.png");
        assert_eq!(primary.requests().len(), 1);
        assert_eq!(fallback.requests().len(), 1);
    }

    #[derive(Clone, Debug)]
    struct GeneratorStub {
        result: Result<ImageGenerationResult, ImageGenerationError>,
        requests: Arc<Mutex<Vec<ImageGenerationRequest>>>,
    }

    impl GeneratorStub {
        fn success(image_url: impl Into<String>) -> Self {
            Self {
                result: Ok(ImageGenerationResult {
                    image_url: image_url.into(),
                    ..ImageGenerationResult::default()
                }),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn forbidden() -> Self {
            Self {
                result: Err(ImageGenerationError::Forbidden),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn error(message: impl Into<String>) -> Self {
            Self {
                result: Err(ImageGenerationError::Provider(message.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<ImageGenerationRequest> {
            self.requests.lock().expect("requests").clone()
        }
    }

    impl ImageGenerator for GeneratorStub {
        fn generate_image<'a>(
            &'a self,
            request: ImageGenerationRequest,
        ) -> ImageGenerationFuture<'a> {
            let result = self.result.clone();
            self.requests.lock().expect("requests").push(request);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Debug)]
    struct EditorStub {
        result: Result<ImageEditResult, ImageEditError>,
        requests: Arc<Mutex<Vec<ImageEditRequest>>>,
    }

    impl EditorStub {
        fn success(image_urls: Vec<String>) -> Self {
            Self {
                result: Ok(ImageEditResult { image_urls }),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn forbidden() -> Self {
            Self {
                result: Err(ImageEditError::Forbidden),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn error(message: impl Into<String>) -> Self {
            Self {
                result: Err(ImageEditError::Provider(message.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<ImageEditRequest> {
            self.requests.lock().expect("image edit requests").clone()
        }
    }

    impl ImageEditor for EditorStub {
        fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
            let result = self.result.clone();
            self.requests
                .lock()
                .expect("image edit requests")
                .push(request);
            Box::pin(async move { result })
        }
    }

    #[derive(Clone, Debug, Default)]
    struct OptimizerStub {
        image_result: Arc<
            Mutex<
                Option<
                    Result<
                        openplotva_media::ImageOptimize,
                        crate::media::MediaPromptOptimizerError,
                    >,
                >,
            >,
        >,
        edit_result: Arc<
            Mutex<
                Option<
                    Result<
                        openplotva_media::ImageEditOptimize,
                        crate::media::MediaPromptOptimizerError,
                    >,
                >,
            >,
        >,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl OptimizerStub {
        fn with_image_result(self, value: openplotva_media::ImageOptimize) -> Self {
            *self.image_result.lock().expect("image optimizer result") = Some(Ok(value));
            self
        }

        fn with_edit_result(self, value: openplotva_media::ImageEditOptimize) -> Self {
            *self.edit_result.lock().expect("edit optimizer result") = Some(Ok(value));
            self
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("optimizer calls").clone()
        }
    }

    impl crate::media::MediaPromptOptimizer for OptimizerStub {
        fn optimize_image_prompt<'a>(
            &'a self,
            text: &'a str,
            options: openplotva_media::OptimizePromptOptions,
        ) -> crate::media::ImagePromptOptimizeFuture<'a> {
            self.calls
                .lock()
                .expect("optimizer calls")
                .push(format!("image:{text}:{}", options.variant_count));
            let result = self
                .image_result
                .lock()
                .expect("image optimizer result")
                .take()
                .unwrap_or_else(|| {
                    Err(crate::media::MediaPromptOptimizerError::Provider(
                        "missing image optimizer result".to_owned(),
                    ))
                });
            Box::pin(async move { result })
        }

        fn optimize_image_edit_prompt<'a>(
            &'a self,
            text: &'a str,
            options: openplotva_media::OptimizePromptOptions,
        ) -> crate::media::ImageEditPromptOptimizeFuture<'a> {
            self.calls
                .lock()
                .expect("optimizer calls")
                .push(format!("edit:{text}:{}", options.variant_count));
            let result = self
                .edit_result
                .lock()
                .expect("edit optimizer result")
                .take()
                .unwrap_or_else(|| {
                    Err(crate::media::MediaPromptOptimizerError::Provider(
                        "missing edit optimizer result".to_owned(),
                    ))
                });
            Box::pin(async move { result })
        }
    }

    #[derive(Debug)]
    struct EffectsStub {
        drawing_sticker: Option<i32>,
        calls: Mutex<Vec<String>>,
    }

    impl EffectsStub {
        fn new(drawing_sticker: Option<i32>) -> Self {
            Self {
                drawing_sticker,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.call_log().clone()
        }

        fn call_log(&self) -> MutexGuard<'_, Vec<String>> {
            self.calls.lock().expect("calls")
        }
    }

    impl ImageJobEffects for EffectsStub {
        fn remove_queued_sticker<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log()
                .push(format!("remove_queued:{chat_id}:{message_id}"));
            Box::pin(async {})
        }

        fn send_drawing_sticker<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            user_id: i64,
            thread_id: Option<i32>,
        ) -> ImageJobEffectFuture<'a, Option<i32>> {
            self.call_log().push(format!(
                "send_drawing:{chat_id}:{message_id}:{user_id}:{}",
                thread_id.unwrap_or_default()
            ));
            Box::pin(async move { self.drawing_sticker })
        }

        fn remove_drawing_sticker<'a>(
            &'a self,
            chat_id: i64,
            sticker_message_id: i32,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log()
                .push(format!("remove_drawing:{chat_id}:{sticker_message_id}"));
            Box::pin(async {})
        }

        fn send_nsfw_blocked_message<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            user_id: i64,
            thread_id: Option<i32>,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log().push(format!(
                "send_nsfw_blocked:{chat_id}:{message_id}:{user_id}:{}:{NSFW_BLOCKED_MESSAGE_TEXT}",
                thread_id.unwrap_or_default()
            ));
            Box::pin(async {})
        }
    }
}
