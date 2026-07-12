//! App-level image generation task execution.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose};
use openplotva_config::AppConfig;
use openplotva_core::ChatMessageMeta;
use openplotva_dialog::sanitize_tool_text;
use openplotva_llm::aifarm::{
    AifarmHttpMethod, AifarmHttpRequest, AifarmHttpTransport, DiscoveryInvocation,
    DiscoveryJobEnvelope, DiscoveryJobRequest, DiscoveryJobResult, ReqwestAifarmTransport,
    decode_discovery_body, is_failure_status, is_queued_status, is_running_status,
    is_success_status, parse_job_error,
};
use openplotva_llm::retry::{FailureReason, retryable_reason_from_message};
use openplotva_taskman::{
    DEFAULT_LLM_JOB_MAX_ATTEMPTS, IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME,
    ImageEditJobParams, ImageGenJobParams, InMemoryTaskQueue, JobType,
    LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE, TaskQueueError, TaskQueueJobEvent,
    TaskQueueWorkItem, image_edit_job_params_from_stateless_job,
    image_gen_job_params_from_stateless_job,
};
use openplotva_telegram::{
    ChatRef, DeleteMessageRequest, EditMediaMessageRequest, MediaGroupMessageRequest,
    MediaGroupPhotoItem, PhotoMessageRequest, PhotoSource, ReplyMessageRef, ReplyParametersPlan,
    TELEGRAM_PARSE_MODE_HTML, TelegramOutboundMethod, TelegramOutboundResponse, TextMessageRequest,
    build_delete_message_method, build_edit_media_message_method, build_media_group_message_method,
    build_photo_message_method, build_text_message_methods, ensure_telegram_safe_text,
    escape_telegram_html_text, execute_telegram_method, strip_telegram_html,
};
use rand::{Rng, RngExt};
use serde::Serialize;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::time::Instant;

use crate::media::{MediaPromptOptimizer, MediaPromptOptimizerService};
use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
};
use crate::telegram_activity::{
    TelegramActivityAction, TelegramActivityPulse, TelegramActivitySnapshot,
};
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
pub const AIFARM_DRAW_API_BOOGU_TURBO_ENDPOINT_NAME: &str = "boogu_turbo_generate";
pub const AIFARM_DRAW_API_BOOGU_EDIT_ENDPOINT_NAME: &str = "boogu_edit_generate";
pub const AIFARM_DRAW_API_DEFAULT_BASE_URL: &str = "http://127.0.0.1:50051";
pub const AIFARM_DRAW_API_DEFAULT_TIMEOUT: StdDuration = StdDuration::from_secs(600);
pub const AIFARM_DRAW_API_DEFAULT_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub const STICKER_DOWN_FILE_ID: &str =
    "CAACAgIAAxkBAAEeROBkDjnz1i3WxxyNLBgWA_IKyjxbnQACuioAAqPicEh1C96_WINTHS8E";
pub const NSFW_BLOCKED_MESSAGE_TEXT: &str = "Ваш запрос заблокирован, так как содержит неприемлемый контент. Попробуйте переформулировать запрос.";
pub const IMAGE_PLACEHOLDER_FILE_ID: &str =
    "AgACAgIAAxkBAAFhmg5oDV5-lLcooLSE8nKFLlF768nEygAC6O8xG2uvaUjfFg40SWg2rgEAAwIAA3kAAzYE";
pub const IMAGE_JOB_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub const IMAGE_JOB_WORKER_QUEUES: [&str; 2] = [IMAGE_VIP_QUEUE_NAME, IMAGE_REGULAR_QUEUE_NAME];
pub const IMAGE_VIP_JOB_WORKER_QUEUES: [&str; 1] = [IMAGE_VIP_QUEUE_NAME];
pub const IMAGE_REGULAR_JOB_WORKER_QUEUES: [&str; 1] = [IMAGE_REGULAR_QUEUE_NAME];
pub const IMAGE_EDIT_WORKFLOW_KEY: &str = "image_edit";
pub const IMAGE_GENERATION_WORKFLOW_KEY: &str = "image_generation";
pub const IMAGE_GENERATION_FLUX_WORKFLOW_KEY: &str = "image_generation_flux";
pub const IMAGE_GENERATION_BOOGU_TURBO_WORKFLOW_KEY: &str = "image_generation_boogu_turbo";
pub const IMAGE_EDIT_FLUX_WORKFLOW_KEY: &str = "image_edit_flux";
pub const IMAGE_EDIT_BOOGU_TURBO_WORKFLOW_KEY: &str = "image_edit_boogu_turbo";
pub const DRAW_SUPPORT_ME_URL: &str = "https://t.me/PlotvoBot?start=donate";
pub const DRAW_VIP_URL: &str = "https://t.me/PlotvoBot?start=vip";
pub const BOOGU_GRADIO_RESOLUTION_MODE: &str = "Recommended resolutions";

const IMAGE_REGULAR_WORKER_ID: &str = "image-regular-worker";
const IMAGE_VIP_WORKER_ID: &str = "image-vip-worker";
const TELEGRAM_MEDIA_GROUP_MAX_ITEMS: usize = 10;
const DRAW_CAPTION_WORD_THRESHOLD: usize = 25;
const TELEGRAM_CAPTION_MAX_VISIBLE: usize = 1024;
const DRAW_CAPTION_ELLIPSIS: &str = "…";
const BOOGU_GRADIO_FN_INDEX: i32 = 2;
const BOOGU_GRADIO_DEFAULT_SEED: i64 = 42;
const BOOGU_GRADIO_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) OpenPlotva/1.0";
const IMAGE_SUPPORT_PHRASES: [&str; 15] = [
    "автору на вдохновение ✨",
    "Плотва в долгу ❤️",
    "за каждый мем 🐸",
    "на улучшения бота 🔧",
    "спасибо от всей стаи 🐟",
    "донат = волшебство ✨",
    "помоги расти 📈",
    "на новые эксперименты 🧪",
    "автору на счастье 😊",
    "Плотва любит донаты 💙",
    "за креатив без границ 🌈",
    "на развитие магии 🪄",
    "спасибо, ты лучший 🏆",
    "поддержи Плотвин дом 🏠",
    "поддержать ❤️",
];

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
    /// Generated image bytes returned by the edit provider.
    pub image_bytes: Vec<Vec<u8>>,
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
    /// Discovery service name.
    pub service_name: String,
    /// Discovery endpoint name.
    pub endpoint_name: String,
    /// Upstream draw task timeout.
    pub timeout: StdDuration,
    /// Poll interval for `/v1/jobs/{id}`.
    pub poll_interval: StdDuration,
}

impl Default for AifarmDrawApiConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            service_name: String::new(),
            endpoint_name: String::new(),
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
        self.service_name = if self.service_name.trim().is_empty() {
            AIFARM_DRAW_API_SERVICE_NAME.to_owned()
        } else {
            self.service_name.trim().to_owned()
        };
        self.endpoint_name = if self.endpoint_name.trim().is_empty() {
            AIFARM_DRAW_API_ENDPOINT_NAME.to_owned()
        } else {
            self.endpoint_name.trim().to_owned()
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BooguGradioImageConfig {
    pub enabled: bool,
    pub base_url: String,
    pub timeout: StdDuration,
    pub steps: i32,
    pub resolution: String,
    pub width: i32,
    pub height: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BooguGradioEditConfig {
    pub enabled: bool,
    pub base_url: String,
    pub timeout: StdDuration,
    pub steps: i32,
    pub resolution_category: String,
    pub resolution: String,
    pub width: i32,
    pub height: i32,
}

#[must_use]
pub fn boogu_gradio_image_config_from_app_config(config: &AppConfig) -> BooguGradioImageConfig {
    let provider = &config.image_providers.boogu_turbo;
    BooguGradioImageConfig {
        enabled: provider.enabled,
        base_url: provider.base_url.clone(),
        timeout: StdDuration::from_secs(provider.timeout_seconds.max(1) as u64),
        steps: provider.steps.max(1),
        resolution: provider.resolution.clone(),
        width: provider.width.max(1),
        height: provider.height.max(1),
    }
}

#[must_use]
pub fn boogu_gradio_edit_config_from_app_config(config: &AppConfig) -> BooguGradioEditConfig {
    let provider = &config.image_providers.boogu_edit_turbo;
    BooguGradioEditConfig {
        enabled: provider.enabled,
        base_url: provider.base_url.clone(),
        timeout: StdDuration::from_secs(provider.timeout_seconds.max(1) as u64),
        steps: provider.steps.max(1),
        resolution_category: provider.resolution_category.clone(),
        resolution: provider.resolution.clone(),
        width: provider.width.max(1),
        height: provider.height.max(1),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BooguHttpMethod {
    Get,
    Post,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BooguHttpRequest {
    pub method: BooguHttpMethod,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub timeout: StdDuration,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BooguHttpResponse {
    pub status_code: u16,
    pub body: Vec<u8>,
}

pub type BooguHttpFuture<'a> =
    Pin<Box<dyn Future<Output = Result<BooguHttpResponse, String>> + Send + 'a>>;

pub trait BooguHttpTransport: Send + Sync {
    fn send<'a>(&'a self, request: BooguHttpRequest) -> BooguHttpFuture<'a>;
}

#[derive(Clone, Debug)]
pub struct ReqwestBooguTransport {
    client: reqwest::Client,
}

impl Default for ReqwestBooguTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl BooguHttpTransport for ReqwestBooguTransport {
    fn send<'a>(&'a self, request: BooguHttpRequest) -> BooguHttpFuture<'a> {
        Box::pin(async move {
            let method = match request.method {
                BooguHttpMethod::Get => reqwest::Method::GET,
                BooguHttpMethod::Post => reqwest::Method::POST,
            };
            let mut builder = self
                .client
                .request(method, &request.url)
                .timeout(request.timeout);
            for (name, value) in &request.headers {
                builder = builder.header(name, value);
            }
            if !request.body.is_empty() {
                builder = builder.body(request.body);
            }
            let response = builder
                .send()
                .await
                .map_err(|error| format!("request failed: {error}"))?;
            let status_code = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .map_err(|error| format!("read response body: {error}"))?
                .to_vec();
            Ok(BooguHttpResponse { status_code, body })
        })
    }
}

#[derive(Clone)]
pub struct BooguGradioImageClient<DataUrl, Transport = ReqwestBooguTransport> {
    image_config: BooguGradioImageConfig,
    edit_config: BooguGradioEditConfig,
    data_urls: DataUrl,
    transport: Transport,
}

impl<DataUrl, Transport> fmt::Debug for BooguGradioImageClient<DataUrl, Transport>
where
    DataUrl: fmt::Debug,
    Transport: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BooguGradioImageClient")
            .field("image_config", &self.image_config)
            .field("edit_config", &self.edit_config)
            .field("data_urls", &self.data_urls)
            .field("transport", &self.transport)
            .finish()
    }
}

impl<DataUrl> BooguGradioImageClient<DataUrl, ReqwestBooguTransport> {
    #[must_use]
    pub fn new(
        image_config: BooguGradioImageConfig,
        edit_config: BooguGradioEditConfig,
        data_urls: DataUrl,
    ) -> Self {
        Self::with_transport(
            image_config,
            edit_config,
            data_urls,
            ReqwestBooguTransport::default(),
        )
    }
}

impl<DataUrl, Transport> BooguGradioImageClient<DataUrl, Transport>
where
    Transport: BooguHttpTransport,
{
    #[must_use]
    pub fn with_transport(
        image_config: BooguGradioImageConfig,
        edit_config: BooguGradioEditConfig,
        data_urls: DataUrl,
        transport: Transport,
    ) -> Self {
        Self {
            image_config,
            edit_config,
            data_urls,
            transport,
        }
    }

    pub async fn generate_image_with_session_hash(
        &self,
        request: ImageGenerationRequest,
        session_hash: &str,
    ) -> Result<ImageGenerationResult, ImageGenerationError> {
        let config = &self.image_config;
        if !config.enabled {
            return Ok(ImageGenerationResult::default());
        }
        let prompt = boogu_generation_prompt(&request);
        if prompt.trim().is_empty() {
            return Ok(ImageGenerationResult::default());
        }
        let (seed, randomize_seed) = boogu_generation_seed(&request.seed);
        let session_hash =
            non_empty(session_hash.to_owned()).unwrap_or_else(generated_boogu_session_hash);
        let payload = json!({
            "fn_index": BOOGU_GRADIO_FN_INDEX,
            "data": [
                prompt,
                seed,
                randomize_seed,
                false,
                BOOGU_GRADIO_RESOLUTION_MODE,
                config.resolution,
                config.width,
                config.height,
                config.steps,
                null
            ],
            "session_hash": session_hash
        });
        let image_url = self
            .run_gradio_queue(&config.base_url, config.timeout, &session_hash, payload)
            .await
            .map_err(|error| ImageGenerationError::Provider(format!("boogu provider {error}")))?;
        Ok(ImageGenerationResult {
            image_url: image_url.clone(),
            image_urls: vec![image_url],
            image_bytes: Vec::new(),
        })
    }

    async fn run_gradio_queue(
        &self,
        base_url: &str,
        timeout: StdDuration,
        session_hash: &str,
        payload: Value,
    ) -> Result<String, String> {
        let join_url = boogu_endpoint(base_url, "/gradio_api/queue/join")?;
        let body = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
        let join_response = self
            .transport
            .send(BooguHttpRequest {
                method: BooguHttpMethod::Post,
                url: join_url,
                headers: boogu_headers(base_url, Some("application/json"), None)?,
                body,
                timeout,
            })
            .await?;
        if !(200..300).contains(&join_response.status_code) {
            return Err(boogu_http_status_error(
                "join",
                join_response.status_code,
                &join_response.body,
            ));
        }
        let join_payload = serde_json::from_slice::<Value>(&join_response.body)
            .map_err(|error| format!("decode join response: {error}"))?;
        let _event_id = join_payload
            .get("event_id")
            .and_then(Value::as_str)
            .and_then(|value| non_empty(value.to_owned()))
            .ok_or_else(|| "join response did not include event_id".to_owned())?;

        let data_url = boogu_queue_data_url(base_url, session_hash)?;
        let data_response = self
            .transport
            .send(BooguHttpRequest {
                method: BooguHttpMethod::Get,
                url: data_url,
                headers: boogu_headers(base_url, None, Some("text/event-stream"))?,
                body: Vec::new(),
                timeout,
            })
            .await?;
        if !(200..300).contains(&data_response.status_code) {
            return Err(boogu_http_status_error(
                "queue data",
                data_response.status_code,
                &data_response.body,
            ));
        }
        let data_text = String::from_utf8_lossy(&data_response.body);
        parse_boogu_completed_url(&data_text)
    }
}

impl<DataUrl, Transport> ImageGenerator for BooguGradioImageClient<DataUrl, Transport>
where
    DataUrl: Send + Sync,
    Transport: BooguHttpTransport,
{
    fn expected_image_count(&self) -> usize {
        usize::from(self.image_config.enabled)
    }

    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            self.generate_image_with_session_hash(request, &generated_boogu_session_hash())
                .await
        })
    }
}

impl<DataUrl, Transport> BooguGradioImageClient<DataUrl, Transport>
where
    DataUrl: TelegramVisionDataUrlProvider + Send + Sync,
    DataUrl::Error: fmt::Display,
    Transport: BooguHttpTransport,
{
    pub async fn edit_image_with_session_hash(
        &self,
        request: ImageEditRequest,
        session_hash: &str,
    ) -> Result<ImageEditResult, ImageEditError> {
        let config = &self.edit_config;
        if !config.enabled {
            return Ok(ImageEditResult::default());
        }
        let prompt = request.prompt.trim().to_owned();
        if prompt.is_empty() {
            return Err(ImageEditError::Provider(
                "boogu provider image edit prompt is empty".to_owned(),
            ));
        }
        let photo_file_id = request.photo_file_id.trim();
        if photo_file_id.is_empty() {
            return Err(ImageEditError::Provider(
                "boogu provider image edit requires Telegram file_id".to_owned(),
            ));
        }
        let data_url = self
            .data_urls
            .telegram_file_data_url(photo_file_id, "photo", None)
            .await
            .map_err(|error| ImageEditError::Provider(format!("boogu provider {error}")))?;
        let session_hash =
            non_empty(session_hash.to_owned()).unwrap_or_else(generated_boogu_session_hash);
        let payload = json!({
            "fn_index": BOOGU_GRADIO_FN_INDEX,
            "data": [
                prompt,
                {
                    "url": data_url,
                    "orig_name": "image.png",
                    "meta": {"_type": "gradio.FileData"}
                },
                "",
                BOOGU_GRADIO_DEFAULT_SEED,
                false,
                false,
                config.resolution_category,
                BOOGU_GRADIO_RESOLUTION_MODE,
                config.resolution,
                config.width,
                config.height,
                config.steps,
                null
            ],
            "session_hash": session_hash
        });
        let image_url = self
            .run_gradio_queue(&config.base_url, config.timeout, &session_hash, payload)
            .await
            .map_err(|error| ImageEditError::Provider(format!("boogu provider {error}")))?;
        Ok(ImageEditResult {
            image_urls: vec![image_url],
            image_bytes: Vec::new(),
        })
    }
}

impl<DataUrl, Transport> ImageEditor for BooguGradioImageClient<DataUrl, Transport>
where
    DataUrl: TelegramVisionDataUrlProvider + Send + Sync,
    DataUrl::Error: fmt::Display,
    Transport: BooguHttpTransport,
{
    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            self.edit_image_with_session_hash(request, &generated_boogu_session_hash())
                .await
        })
    }
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

    /// Generate with an explicit job ID, useful for deterministic contract tests.
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
        let payload = draw_api_payload(&prompt, &[], draw_api_dimensions(&request.aspect_ratio))
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

    /// Edit an image with an explicit job ID, useful for deterministic contract tests.
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
        let payload = draw_api_payload(&prompt, &image_inputs, None)
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
            image_bytes: result.images,
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
                service_name: self.cfg.service_name.clone(),
                endpoint_name: self.cfg.endpoint_name.clone(),
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
    fn expected_image_count(&self) -> usize {
        self.primary.expected_image_count()
    }

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

#[derive(Clone, Debug)]
pub struct SequentialImageGenerator<First, Second> {
    first: First,
    second: Second,
}

impl<First, Second> SequentialImageGenerator<First, Second> {
    /// Build a sequential generator where both slots should be attempted.
    #[must_use]
    pub const fn new(first: First, second: Second) -> Self {
        Self { first, second }
    }
}

impl<First, Second> ImageGenerator for SequentialImageGenerator<First, Second>
where
    First: ImageGenerator + Sync,
    Second: ImageGenerator + Sync,
{
    fn expected_image_count(&self) -> usize {
        self.first.expected_image_count() + self.second.expected_image_count()
    }

    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let mut combined = ImageGenerationResult::default();
            let mut last_provider_error = None;

            match self
                .first
                .generate_image(image_generation_request_for_prompt_slot(&request, 0))
                .await
            {
                Ok(result) if image_generation_result_has_images(&result) => {
                    append_image_generation_result(&mut combined, result);
                }
                Ok(_) => {
                    last_provider_error = Some(ImageGenerationError::Provider(
                        "first image slot was empty".to_owned(),
                    ));
                }
                Err(ImageGenerationError::Forbidden) => {
                    return Err(ImageGenerationError::Forbidden);
                }
                Err(error) => last_provider_error = Some(error),
            }

            let second_slot = self.first.expected_image_count().max(1);
            match self
                .second
                .generate_image(image_generation_request_for_prompt_slot(
                    &request,
                    second_slot,
                ))
                .await
            {
                Ok(result) if image_generation_result_has_images(&result) => {
                    append_image_generation_result(&mut combined, result);
                }
                Ok(_) => {
                    last_provider_error = Some(ImageGenerationError::Provider(
                        "second image slot was empty".to_owned(),
                    ));
                }
                Err(ImageGenerationError::Forbidden) => {
                    return Err(ImageGenerationError::Forbidden);
                }
                Err(error) => last_provider_error = Some(error),
            }

            if image_generation_result_has_images(&combined) {
                return Ok(combined);
            }
            Err(last_provider_error.unwrap_or_else(|| {
                ImageGenerationError::Provider(
                    "sequential image workflow produced no image".to_owned(),
                )
            }))
        })
    }

    fn generate_image_streaming<'a>(
        &'a self,
        request: ImageGenerationRequest,
        progress: ImageGenerationProgressSink,
    ) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let mut combined = ImageGenerationResult::default();
            let mut last_provider_error = None;

            match self
                .first
                .generate_image(image_generation_request_for_prompt_slot(&request, 0))
                .await
            {
                Ok(result) if image_generation_result_has_images(&result) => {
                    let _ = progress.send(result.clone());
                    append_image_generation_result(&mut combined, result);
                }
                Ok(_) => {
                    last_provider_error = Some(ImageGenerationError::Provider(
                        "first image slot was empty".to_owned(),
                    ));
                }
                Err(ImageGenerationError::Forbidden) => {
                    return Err(ImageGenerationError::Forbidden);
                }
                Err(error) => last_provider_error = Some(error),
            }

            let second_slot = self.first.expected_image_count().max(1);
            match self
                .second
                .generate_image(image_generation_request_for_prompt_slot(
                    &request,
                    second_slot,
                ))
                .await
            {
                Ok(result) if image_generation_result_has_images(&result) => {
                    let _ = progress.send(result.clone());
                    append_image_generation_result(&mut combined, result);
                }
                Ok(_) => {
                    last_provider_error = Some(ImageGenerationError::Provider(
                        "second image slot was empty".to_owned(),
                    ));
                }
                Err(ImageGenerationError::Forbidden) => {
                    return Err(ImageGenerationError::Forbidden);
                }
                Err(error) => last_provider_error = Some(error),
            }

            if image_generation_result_has_images(&combined) {
                return Ok(combined);
            }
            Err(last_provider_error.unwrap_or_else(|| {
                ImageGenerationError::Provider(
                    "sequential image workflow produced no image".to_owned(),
                )
            }))
        })
    }
}

#[derive(Clone, Debug)]
pub struct ParallelImageGenerator<First, Second> {
    first: First,
    second: Second,
}

impl<First, Second> ParallelImageGenerator<First, Second> {
    #[must_use]
    pub const fn new(first: First, second: Second) -> Self {
        Self { first, second }
    }
}

impl<First, Second> ImageGenerator for ParallelImageGenerator<First, Second>
where
    First: ImageGenerator + Sync,
    Second: ImageGenerator + Sync,
{
    fn expected_image_count(&self) -> usize {
        self.first.expected_image_count() + self.second.expected_image_count()
    }

    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let first_slot = 0;
            let second_slot = self.first.expected_image_count();
            let first_request = image_generation_request_for_prompt_slot(&request, first_slot);
            let second_request = image_generation_request_for_prompt_slot(&request, second_slot);
            let (first, second) = tokio::join!(
                self.first.generate_image(first_request),
                self.second.generate_image(second_request)
            );
            combine_parallel_image_generation_results(first, second)
        })
    }

    fn generate_image_streaming<'a>(
        &'a self,
        request: ImageGenerationRequest,
        progress: ImageGenerationProgressSink,
    ) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let first_slot = 0;
            let second_slot = self.first.expected_image_count();
            let first_request = image_generation_request_for_prompt_slot(&request, first_slot);
            let second_request = image_generation_request_for_prompt_slot(&request, second_slot);
            let first_progress = progress.clone();
            let second_progress = progress;
            let (first, second) = tokio::join!(
                self.first
                    .generate_image_streaming(first_request, first_progress),
                self.second
                    .generate_image_streaming(second_request, second_progress)
            );
            combine_parallel_image_generation_results(first, second)
        })
    }
}

#[derive(Clone, Debug)]
pub struct ParallelImageEditor<First, Second> {
    first: First,
    second: Second,
}

impl<First, Second> ParallelImageEditor<First, Second> {
    #[must_use]
    pub const fn new(first: First, second: Second) -> Self {
        Self { first, second }
    }
}

impl<First, Second> ImageEditor for ParallelImageEditor<First, Second>
where
    First: ImageEditor + Sync,
    Second: ImageEditor + Sync,
{
    fn expected_image_count(&self) -> usize {
        self.first.expected_image_count() + self.second.expected_image_count()
    }

    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            let (first, second) = tokio::join!(
                self.first.edit_image(request.clone()),
                self.second.edit_image(request)
            );
            combine_parallel_image_edit_results(first, second)
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
    fn expected_image_count(&self) -> usize {
        self.editor.expected_image_count()
    }

    fn edit_image<'a>(&'a self, mut request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            if normalized_image_edit_inputs(&request).is_empty()
                && !request.photo_file_id.trim().is_empty()
            {
                let data_url = self
                    .data_urls
                    .telegram_file_data_url(&request.photo_file_id, "photo", None)
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

/// Sink for incremental image results: one send per provider slot as it completes, so the
/// worker can publish and redraw the post progressively instead of waiting for all images.
pub type ImageGenerationProgressSink = tokio::sync::mpsc::UnboundedSender<ImageGenerationResult>;

pub trait ImageGenerator {
    /// Number of placeholder/result slots this workflow should reserve.
    fn expected_image_count(&self) -> usize {
        1
    }

    /// Generate and send an image.
    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a>;

    /// Generate, emitting each slot's result on `progress` as soon as it is ready. The
    /// default emits the whole result once — correct for providers that return every image
    /// at once; multi-slot combinators override this to emit per slot.
    fn generate_image_streaming<'a>(
        &'a self,
        request: ImageGenerationRequest,
        progress: ImageGenerationProgressSink,
    ) -> ImageGenerationFuture<'a>
    where
        Self: Sync,
    {
        Box::pin(async move {
            let result = self.generate_image(request).await?;
            if image_generation_result_has_images(&result) {
                let _ = progress.send(result.clone());
            }
            Ok(result)
        })
    }
}

pub trait ImageEditor {
    /// Number of placeholder/result slots this workflow should reserve.
    fn expected_image_count(&self) -> usize {
        1
    }

    /// Edit and send an image.
    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a>;
}

#[derive(Clone)]
pub struct RoutedImageGenerator {
    walker: RoutedAttemptWalker,
    draw_api_config: AifarmDrawApiConfig,
    workflow_key: String,
    expected_image_count: usize,
}

impl RoutedImageGenerator {
    #[must_use]
    pub fn new(walker: RoutedAttemptWalker, draw_api_config: AifarmDrawApiConfig) -> Self {
        Self {
            walker,
            draw_api_config: draw_api_config.with_defaults(),
            workflow_key: IMAGE_GENERATION_WORKFLOW_KEY.to_owned(),
            expected_image_count: 1,
        }
    }

    #[must_use]
    pub fn with_expected_image_count(mut self, expected_image_count: usize) -> Self {
        self.expected_image_count = expected_image_count.max(1);
        self
    }

    #[must_use]
    pub fn with_workflow_key(mut self, workflow_key: impl Into<String>) -> Self {
        self.workflow_key = workflow_key.into();
        self
    }
}

impl ImageGenerator for RoutedImageGenerator {
    fn expected_image_count(&self) -> usize {
        self.expected_image_count
    }

    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let request_for_attempts = request.clone();
            let draw_api_config = self.draw_api_config.clone();
            let result = self
                .walker
                .run(
                    image_generation_context(&self.workflow_key, &request),
                    move |attempt| {
                        let request = request_for_attempts.clone();
                        let draw_api_config = draw_api_config.clone();
                        async move {
                            generate_image_with_routed_attempt(attempt, request, draw_api_config)
                                .await
                        }
                    },
                    image_generation_retryable_reason,
                )
                .await;
            match result {
                Ok(result) => Ok(result),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(ImageGenerationError::Provider(error.to_string()))
                }
            }
        })
    }
}

#[derive(Clone)]
pub struct RoutedImageEditor<DataUrl> {
    walker: RoutedAttemptWalker,
    data_urls: DataUrl,
    draw_api_config: AifarmDrawApiConfig,
    workflow_key: String,
}

impl<DataUrl> RoutedImageEditor<DataUrl> {
    #[must_use]
    pub fn new(
        walker: RoutedAttemptWalker,
        data_urls: DataUrl,
        draw_api_config: AifarmDrawApiConfig,
    ) -> Self {
        Self {
            walker,
            data_urls,
            draw_api_config: draw_api_config.with_defaults(),
            workflow_key: IMAGE_EDIT_WORKFLOW_KEY.to_owned(),
        }
    }

    #[must_use]
    pub fn with_workflow_key(mut self, workflow_key: impl Into<String>) -> Self {
        self.workflow_key = workflow_key.into();
        self
    }
}

impl<DataUrl> ImageEditor for RoutedImageEditor<DataUrl>
where
    DataUrl: TelegramVisionDataUrlProvider + Clone + Send + Sync + 'static,
    DataUrl::Error: fmt::Display,
{
    fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
        Box::pin(async move {
            let request_for_attempts = request.clone();
            let data_urls = self.data_urls.clone();
            let draw_api_config = self.draw_api_config.clone();
            let result = self
                .walker
                .run(
                    image_edit_context(&self.workflow_key, &request),
                    move |attempt| {
                        let request = request_for_attempts.clone();
                        let data_urls = data_urls.clone();
                        let draw_api_config = draw_api_config.clone();
                        async move {
                            edit_image_with_routed_attempt(
                                attempt,
                                request,
                                data_urls,
                                draw_api_config,
                            )
                            .await
                        }
                    },
                    image_edit_retryable_reason,
                )
                .await;
            match result {
                Ok(result) => Ok(result),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(ImageEditError::Provider(error.to_string()))
                }
            }
        })
    }
}

fn image_generation_context(
    workflow_key: &str,
    request: &ImageGenerationRequest,
) -> RoutedRequestContext {
    RoutedRequestContext {
        workflow_key: workflow_key.to_owned(),
        chat_id: (request.chat_id != 0).then_some(request.chat_id),
        thread_id: request.thread_id,
        message_id: (request.message_id != 0).then_some(request.message_id),
        ..RoutedRequestContext::default()
    }
}

fn image_edit_context(workflow_key: &str, request: &ImageEditRequest) -> RoutedRequestContext {
    RoutedRequestContext {
        workflow_key: workflow_key.to_owned(),
        chat_id: (request.chat_id != 0).then_some(request.chat_id),
        thread_id: request.thread_id,
        message_id: (request.message_id != 0).then_some(request.message_id),
        ..RoutedRequestContext::default()
    }
}

async fn generate_image_with_routed_attempt(
    attempt: RoutedAttempt,
    request: ImageGenerationRequest,
    draw_api_config: AifarmDrawApiConfig,
) -> Result<ImageGenerationResult, ImageGenerationError> {
    if routed_attempt_is_draw_api(&attempt) {
        let generator = AifarmDrawApiImageGenerator::new(draw_api_config_for_attempt(
            draw_api_config,
            &attempt,
        ));
        return generator.generate_image(request).await;
    }
    Err(ImageGenerationError::Provider(format!(
        "unsupported image provider {}",
        attempt.provider_name
    )))
}

async fn edit_image_with_routed_attempt<DataUrl>(
    attempt: RoutedAttempt,
    request: ImageEditRequest,
    data_urls: DataUrl,
    draw_api_config: AifarmDrawApiConfig,
) -> Result<ImageEditResult, ImageEditError>
where
    DataUrl: TelegramVisionDataUrlProvider + Send + Sync,
    DataUrl::Error: fmt::Display,
{
    if !routed_attempt_is_draw_api(&attempt) {
        return Err(ImageEditError::Provider(format!(
            "unsupported image edit provider {}",
            attempt.provider_name
        )));
    }
    let editor = ResolvingImageEditor::new(
        data_urls,
        AifarmDrawApiImageGenerator::new(draw_api_config_for_attempt(draw_api_config, &attempt)),
    );
    editor.edit_image(request).await
}

fn routed_attempt_is_draw_api(attempt: &RoutedAttempt) -> bool {
    attempt.provider_name.eq_ignore_ascii_case("aifarm-draw")
        || attempt.provider_name.eq_ignore_ascii_case("draw-api")
        || attempt
            .discovery_service_name
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case(AIFARM_DRAW_API_SERVICE_NAME))
}

fn draw_api_config_for_attempt(
    mut config: AifarmDrawApiConfig,
    attempt: &RoutedAttempt,
) -> AifarmDrawApiConfig {
    if let Some(endpoint) = routed_endpoint(attempt) {
        config.base_url = endpoint;
    }
    if let Some(service_name) = routed_config_value(attempt, "service_name")
        .or_else(|| routed_discovery_value(attempt.discovery_service_name.as_deref()))
    {
        config.service_name = service_name;
    }
    if let Some(endpoint_name) = routed_config_value(attempt, "endpoint_name")
        .or_else(|| routed_discovery_value(attempt.discovery_endpoint_name.as_deref()))
    {
        config.endpoint_name = endpoint_name;
    }
    config.with_defaults()
}

fn routed_endpoint(attempt: &RoutedAttempt) -> Option<String> {
    attempt
        .model_base_url
        .as_deref()
        .or(attempt.provider_endpoint.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn routed_config_value(attempt: &RoutedAttempt, key: &str) -> Option<String> {
    attempt
        .model_config
        .get(key)
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            attempt
                .provider_config
                .get(key)
                .and_then(serde_json::Value::as_str)
        })
        .and_then(|value| routed_discovery_value(Some(value)))
}

fn routed_discovery_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn image_generation_retryable_reason(error: &ImageGenerationError) -> Option<FailureReason> {
    match error {
        ImageGenerationError::Forbidden => None,
        ImageGenerationError::Provider(message) => retryable_reason_from_message(message),
    }
}

fn image_edit_retryable_reason(error: &ImageEditError) -> Option<FailureReason> {
    match error {
        ImageEditError::Forbidden => None,
        ImageEditError::Provider(message) => retryable_reason_from_message(message),
    }
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
    fn expected_image_count(&self) -> usize {
        self.generator.expected_image_count()
    }

    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let request = optimized_image_generation_request(
                &self.optimizer,
                request,
                self.generator.expected_image_count().max(1),
            )
            .await?;
            self.generator.generate_image(request).await
        })
    }

    fn generate_image_streaming<'a>(
        &'a self,
        request: ImageGenerationRequest,
        progress: ImageGenerationProgressSink,
    ) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let request = optimized_image_generation_request(
                &self.optimizer,
                request,
                self.generator.expected_image_count().max(1),
            )
            .await?;
            self.generator
                .generate_image_streaming(request, progress)
                .await
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
    fn expected_image_count(&self) -> usize {
        self.editor.expected_image_count()
    }

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
    variant_count: usize,
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
    extract_prompt_modifiers(&mut request);
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
        .enhance_image_prompt(&original_prompt, &request.aspect_ratio, variant_count)
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
    if request.aspect_ratio.trim().is_empty() {
        request.aspect_ratio = optimized.aspect_ratio.trim().to_owned();
    }
    Ok(request)
}

/// Split user-typed modifiers out of the prompt before optimization so an
/// explicit aspect ratio, seed, or negative prompt survives optimizer misses.
fn extract_prompt_modifiers(request: &mut ImageGenerationRequest) {
    let parts = openplotva_media::part_image_prompt(&request.prompt);
    request.prompt = parts.prompt;
    if request.negative_prompt.trim().is_empty() {
        request.negative_prompt = parts.negative_prompt;
    }
    if request.aspect_ratio.trim().is_empty() {
        request.aspect_ratio = parts.aspect_ratio;
    }
    if request.seed.trim().is_empty() {
        request.seed = parts.seed;
    }
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

fn image_generation_request_for_prompt_slot(
    request: &ImageGenerationRequest,
    slot: usize,
) -> ImageGenerationRequest {
    let mut request = request.clone();
    request.prompt_variants = request
        .prompt_variants
        .get(slot)
        .and_then(|variant| non_empty(variant.clone()))
        .into_iter()
        .collect();
    request
}

fn append_image_generation_result(
    combined: &mut ImageGenerationResult,
    result: ImageGenerationResult,
) {
    let urls = image_generation_urls(&result);
    if combined.image_url.trim().is_empty()
        && let Some(first_url) = urls.first()
    {
        combined.image_url = first_url.clone();
    }
    combined.image_urls.extend(urls);
    combined.image_bytes.extend(
        result
            .image_bytes
            .into_iter()
            .filter(|image| !image.is_empty()),
    );
}

fn combine_parallel_image_generation_results(
    first: Result<ImageGenerationResult, ImageGenerationError>,
    second: Result<ImageGenerationResult, ImageGenerationError>,
) -> Result<ImageGenerationResult, ImageGenerationError> {
    let mut combined = ImageGenerationResult::default();
    let mut last_provider_error = None;

    for result in [first, second] {
        match result {
            Ok(result) if image_generation_result_has_images(&result) => {
                append_image_generation_result(&mut combined, result);
            }
            Ok(_) => {
                last_provider_error = Some(ImageGenerationError::Provider(
                    "parallel image provider returned no image".to_owned(),
                ));
            }
            Err(ImageGenerationError::Forbidden) => return Err(ImageGenerationError::Forbidden),
            Err(error) => last_provider_error = Some(error),
        }
    }

    if image_generation_result_has_images(&combined) {
        return Ok(combined);
    }
    Err(last_provider_error.unwrap_or_else(|| {
        ImageGenerationError::Provider("parallel image workflow produced no image".to_owned())
    }))
}

fn image_edit_result_has_images(result: &ImageEditResult) -> bool {
    result.image_urls.iter().any(|url| !url.trim().is_empty())
        || result.image_bytes.iter().any(|image| !image.is_empty())
}

fn append_image_edit_result(combined: &mut ImageEditResult, result: ImageEditResult) {
    combined
        .image_urls
        .extend(result.image_urls.into_iter().filter_map(non_empty));
    combined.image_bytes.extend(
        result
            .image_bytes
            .into_iter()
            .filter(|image| !image.is_empty()),
    );
}

fn combine_parallel_image_edit_results(
    first: Result<ImageEditResult, ImageEditError>,
    second: Result<ImageEditResult, ImageEditError>,
) -> Result<ImageEditResult, ImageEditError> {
    let mut combined = ImageEditResult::default();
    let mut last_provider_error = None;

    for result in [first, second] {
        match result {
            Ok(result) if image_edit_result_has_images(&result) => {
                append_image_edit_result(&mut combined, result);
            }
            Ok(_) => {
                last_provider_error = Some(ImageEditError::Provider(
                    "parallel image edit provider returned no image".to_owned(),
                ));
            }
            Err(ImageEditError::Forbidden) => return Err(ImageEditError::Forbidden),
            Err(error) => last_provider_error = Some(error),
        }
    }

    if image_edit_result_has_images(&combined) {
        return Ok(combined);
    }
    Err(last_provider_error.unwrap_or_else(|| {
        ImageEditError::Provider("parallel image edit workflow produced no image".to_owned())
    }))
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
    /// Best-effort: mark the trigger message with the "drawing" reaction.
    fn signal_draw_progress<'a>(
        &'a self,
        _chat_id: i64,
        _trigger_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()>
    where
        Self: Sync,
    {
        Box::pin(async {})
    }

    /// Best-effort: remove the lifecycle reaction from the trigger message.
    /// Called only on the success path so a late clear can never erase the
    /// obligations watcher's failure signal.
    fn clear_draw_signal<'a>(
        &'a self,
        _chat_id: i64,
        _trigger_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()>
    where
        Self: Sync,
    {
        Box::pin(async {})
    }

    /// Tell the requester their prompt was blocked by the safety verdict.
    fn send_nsfw_blocked_message<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        user_id: i64,
        thread_id: Option<i32>,
    ) -> ImageJobEffectFuture<'a, ()>;

    /// Send the placeholder album (a single photo when `count` is 1); returns
    /// the frame message ids in album order.
    fn send_initial_placeholders<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        thread_id: Option<i32>,
        caption_text: String,
        is_nsfw: bool,
        count: usize,
    ) -> ImageJobEffectFuture<'a, Result<Vec<i32>, String>>;

    /// Replace one placeholder frame with a generated photo (editMessageMedia).
    fn replace_placeholder_image<'a>(
        &'a self,
        chat_id: i64,
        placeholder_message_id: i32,
        thread_id: Option<i32>,
        photo: PhotoSource,
        caption_text: String,
        is_nsfw: bool,
    ) -> ImageJobEffectFuture<'a, Result<i32, String>>;

    /// Best-effort delete of one placeholder frame.
    fn delete_placeholder_image<'a>(
        &'a self,
        chat_id: i64,
        placeholder_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()>;

    /// Best-effort: persist the delivered album frames for /delete_drawing.
    fn record_last_generation<'a>(
        &'a self,
        _chat_id: i64,
        _user_id: i64,
        _message_ids: Vec<i32>,
        _caption: String,
    ) -> ImageJobEffectFuture<'a, ()>
    where
        Self: Sync,
    {
        Box::pin(async {})
    }
}

/// Persists delivered album frames so /delete_drawing can act on them.
pub trait ImageJobLastGenerationWriter: Send + Sync + fmt::Debug {
    fn write_last_generation<'a>(
        &'a self,
        record: &'a openplotva_storage::LastGenerationRecord,
    ) -> ImageJobEffectFuture<'a, Result<(), String>>;
}

impl ImageJobLastGenerationWriter for openplotva_storage::RedisLastGenerationStore {
    fn write_last_generation<'a>(
        &'a self,
        record: &'a openplotva_storage::LastGenerationRecord,
    ) -> ImageJobEffectFuture<'a, Result<(), String>> {
        Box::pin(async move {
            self.set_last_generation(record)
                .await
                .map_err(|error| error.to_string())
        })
    }
}

/// Concrete image-job effects: drives the placeholder album (sendPhoto /
/// sendMediaGroup → per-frame editMessageMedia), the reaction lifecycle, and
/// the /delete_drawing last-generation record.
#[derive(Clone, Debug)]
pub struct TelegramImageJobEffects<Sender> {
    telegram: Sender,
    reactions: Option<crate::reactions::GenerationReactions>,
    last_generations: Option<Arc<dyn ImageJobLastGenerationWriter>>,
}

impl<Sender> TelegramImageJobEffects<Sender> {
    /// Build concrete image-job effects.
    #[must_use]
    pub fn new(telegram: Sender) -> Self {
        Self {
            telegram,
            reactions: None,
            last_generations: None,
        }
    }

    /// Attach the reaction-based lifecycle signaler.
    #[must_use]
    pub fn with_reaction_ux(mut self, reactions: crate::reactions::GenerationReactions) -> Self {
        self.reactions = Some(reactions);
        self
    }

    /// Attach the /delete_drawing last-generation writer.
    #[must_use]
    pub fn with_last_generation_writer(
        mut self,
        writer: Arc<dyn ImageJobLastGenerationWriter>,
    ) -> Self {
        self.last_generations = Some(writer);
        self
    }
}

impl<Sender> ImageJobEffects for TelegramImageJobEffects<Sender>
where
    Sender: ImageJobTelegramSender + Send + Sync,
{
    fn signal_draw_progress<'a>(
        &'a self,
        chat_id: i64,
        trigger_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            if let Some(reactions) = self.reactions.as_deref() {
                reactions
                    .set_progress(crate::reactions::GenerationReactionTarget {
                        chat_id,
                        message_id: trigger_message_id,
                    })
                    .await;
            }
        })
    }

    fn clear_draw_signal<'a>(
        &'a self,
        chat_id: i64,
        trigger_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            if let Some(reactions) = self.reactions.as_deref() {
                reactions
                    .clear(crate::reactions::GenerationReactionTarget {
                        chat_id,
                        message_id: trigger_message_id,
                    })
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

    fn send_initial_placeholders<'a>(
        &'a self,
        chat_id: i64,
        message_id: i32,
        thread_id: Option<i32>,
        caption_text: String,
        is_nsfw: bool,
        count: usize,
    ) -> ImageJobEffectFuture<'a, Result<Vec<i32>, String>> {
        Box::pin(async move {
            let count = count.clamp(1, TELEGRAM_MEDIA_GROUP_MAX_ITEMS);
            let response = if count == 1 {
                let request = placeholder_image_message_request(
                    chat_id,
                    message_id,
                    thread_id,
                    caption_text,
                    is_nsfw,
                );
                let method =
                    build_photo_message_method(&request).map_err(|error| error.to_string())?;
                self.telegram
                    .send_image_job_method(TelegramOutboundMethod::from(method))
                    .await?
            } else {
                let request = placeholder_image_media_group_request(
                    chat_id,
                    message_id,
                    thread_id,
                    caption_text,
                    is_nsfw,
                    count,
                );
                let method = build_media_group_message_method(&request)
                    .map_err(|error| error.to_string())?;
                self.telegram
                    .send_image_job_method(TelegramOutboundMethod::from(method))
                    .await?
            };
            let ids = image_job_response_message_ids(&response);
            if ids.is_empty() {
                return Err("placeholder send returned no message response".to_owned());
            }
            Ok(ids)
        })
    }

    fn replace_placeholder_image<'a>(
        &'a self,
        chat_id: i64,
        placeholder_message_id: i32,
        _thread_id: Option<i32>,
        photo: PhotoSource,
        caption_text: String,
        is_nsfw: bool,
    ) -> ImageJobEffectFuture<'a, Result<i32, String>> {
        Box::pin(async move {
            let request = replacement_image_message_request(
                chat_id,
                placeholder_message_id,
                photo,
                caption_text,
                is_nsfw,
            );
            let method =
                build_edit_media_message_method(&request).map_err(|error| error.to_string())?;
            let response = self
                .telegram
                .send_image_job_method(TelegramOutboundMethod::from(method))
                .await?;
            match response {
                TelegramOutboundResponse::EditMessage(_)
                | TelegramOutboundResponse::Message(_)
                | TelegramOutboundResponse::Boolean(true) => Ok(placeholder_message_id),
                TelegramOutboundResponse::Boolean(false) => {
                    Err("editMessageMedia returned false".to_owned())
                }
                TelegramOutboundResponse::Messages(_)
                | TelegramOutboundResponse::SentGuestMessage(_)
                | TelegramOutboundResponse::String(_) => {
                    Err("editMessageMedia returned unexpected response".to_owned())
                }
            }
        })
    }

    fn delete_placeholder_image<'a>(
        &'a self,
        chat_id: i64,
        placeholder_message_id: i32,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            self.delete_telegram_message(chat_id, i64::from(placeholder_message_id))
                .await;
        })
    }

    fn record_last_generation<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        message_ids: Vec<i32>,
        caption: String,
    ) -> ImageJobEffectFuture<'a, ()> {
        Box::pin(async move {
            let Some(writer) = self.last_generations.as_deref() else {
                return;
            };
            if message_ids.is_empty() {
                return;
            }
            let record = openplotva_storage::LastGenerationRecord {
                chat_id,
                user_id,
                message_ids: message_ids.into_iter().map(i64::from).collect(),
                caption,
                created_at: OffsetDateTime::now_utc().unix_timestamp(),
            };
            if let Err(error) = writer.write_last_generation(&record).await {
                tracing::warn!(chat_id, user_id, %error, "failed to persist last generation");
            }
        })
    }
}

impl<Sender> TelegramImageJobEffects<Sender>
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

fn generated_image_message_request(
    chat_id: i64,
    message_id: i32,
    thread_id: Option<i32>,
    caption_text: String,
    photo: PhotoSource,
) -> PhotoMessageRequest {
    let message_thread_id = i64::from(thread_id.unwrap_or_default());
    let is_topic_message = message_thread_id != 0;
    PhotoMessageRequest {
        chat: ChatRef {
            id: chat_id,
            is_forum: is_topic_message,
        },
        message_thread_id,
        disable_notification: false,
        photo,
        caption: caption_text,
        render_as: String::new(),
        has_spoiler: false,
        reply_parameters: Some(ReplyParametersPlan {
            message_id: i64::from(message_id),
            chat_id,
            allow_sending_without_reply: true,
        }),
    }
}

fn placeholder_image_message_request(
    chat_id: i64,
    message_id: i32,
    thread_id: Option<i32>,
    caption_text: String,
    is_nsfw: bool,
) -> PhotoMessageRequest {
    let mut request = generated_image_message_request(
        chat_id,
        message_id,
        thread_id,
        caption_text,
        PhotoSource::FileId(IMAGE_PLACEHOLDER_FILE_ID.to_owned()),
    );
    request.render_as = TELEGRAM_PARSE_MODE_HTML.to_owned();
    request.has_spoiler = is_nsfw;
    request
}

fn placeholder_image_media_group_request(
    chat_id: i64,
    message_id: i32,
    thread_id: Option<i32>,
    caption_text: String,
    is_nsfw: bool,
    count: usize,
) -> MediaGroupMessageRequest {
    let message_thread_id = i64::from(thread_id.unwrap_or_default());
    let is_topic_message = message_thread_id != 0;
    MediaGroupMessageRequest {
        chat: ChatRef {
            id: chat_id,
            is_forum: is_topic_message,
        },
        message_thread_id,
        disable_notification: false,
        items: (0..count)
            .map(|index| MediaGroupPhotoItem {
                photo: PhotoSource::FileId(IMAGE_PLACEHOLDER_FILE_ID.to_owned()),
                caption: if index == 0 {
                    caption_text.clone()
                } else {
                    String::new()
                },
                render_as: if index == 0 {
                    TELEGRAM_PARSE_MODE_HTML.to_owned()
                } else {
                    String::new()
                },
                has_spoiler: is_nsfw,
            })
            .collect(),
        reply_parameters: Some(ReplyParametersPlan {
            message_id: i64::from(message_id),
            chat_id,
            allow_sending_without_reply: true,
        }),
    }
}

fn replacement_image_message_request(
    chat_id: i64,
    placeholder_message_id: i32,
    photo: PhotoSource,
    caption_text: String,
    is_nsfw: bool,
) -> EditMediaMessageRequest {
    EditMediaMessageRequest {
        chat: ChatRef {
            id: chat_id,
            is_forum: false,
        },
        message_id: i64::from(placeholder_message_id),
        media: MediaGroupPhotoItem {
            photo,
            render_as: if caption_text.is_empty() {
                String::new()
            } else {
                TELEGRAM_PARSE_MODE_HTML.to_owned()
            },
            caption: caption_text,
            has_spoiler: is_nsfw,
        },
    }
}

fn image_job_response_message_ids(response: &TelegramOutboundResponse) -> Vec<i32> {
    match response {
        TelegramOutboundResponse::Message(message) => {
            i32::try_from(message.id).ok().into_iter().collect()
        }
        TelegramOutboundResponse::Messages(messages) => messages
            .iter()
            .filter_map(|message| i32::try_from(message.id).ok())
            .collect(),
        TelegramOutboundResponse::EditMessage(_)
        | TelegramOutboundResponse::Boolean(_)
        | TelegramOutboundResponse::SentGuestMessage(_)
        | TelegramOutboundResponse::String(_) => Vec::new(),
    }
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
    /// Generated image URLs when present.
    pub image_urls: Vec<String>,
    /// Telegram message ID of the delivered image.
    pub result_message_id: Option<i32>,
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
    /// Telegram message ID of the delivered image.
    pub result_message_id: Option<i32>,
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
    /// Best-effort Telegram activity pulse report for this active job.
    pub activity: TelegramActivitySnapshot,
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
    /// Best-effort Telegram activity pulse report for this active job.
    pub activity: TelegramActivitySnapshot,
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
    pub activity_pulses_sent: u64,
    pub activity_pulse_failures: u64,
    pub activity_pulse_skips: u64,
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
        self.activity_pulses_sent += report.activity.sent;
        self.activity_pulse_failures += report.activity.failed;
        self.activity_pulse_skips += report.activity.skipped;
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
    pub activity_pulses_sent: u64,
    pub activity_pulse_failures: u64,
    pub activity_pulse_skips: u64,
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
        self.activity_pulses_sent += report.activity.sent;
        self.activity_pulse_failures += report.activity.failed;
        self.activity_pulse_skips += report.activity.skipped;
    }
}

#[derive(Clone, Copy)]
pub struct ImageQueuePollOptions<'a> {
    pub max_llm_job_attempts: i32,
    pub activity_pulse: Option<&'a TelegramActivityPulse>,
    pub now: OffsetDateTime,
}

impl ImageQueuePollOptions<'_> {
    #[must_use]
    pub const fn at(now: OffsetDateTime) -> Self {
        Self {
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            activity_pulse: None,
            now,
        }
    }
}

#[derive(Clone, Copy)]
pub struct ImageWorkerRunOptions<'a> {
    pub interval: StdDuration,
    pub max_llm_job_attempts: i32,
    pub activity_pulse: Option<&'a TelegramActivityPulse>,
}

impl ImageWorkerRunOptions<'_> {
    #[must_use]
    pub const fn every(interval: StdDuration) -> Self {
        Self {
            interval,
            max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
            activity_pulse: None,
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
    let expected_image_count = generator
        .expected_image_count()
        .clamp(1, TELEGRAM_MEDIA_GROUP_MAX_ITEMS);
    let display_caption = build_image_generation_caption(
        &caption_text,
        &params.user_full_name,
        expected_image_count > 1,
        params.is_nsfw,
    );

    effects
        .signal_draw_progress(params.chat_id, params.message_id)
        .await;
    let placeholders = match effects
        .send_initial_placeholders(
            params.chat_id,
            params.message_id,
            params.thread_id,
            display_caption.clone(),
            params.is_nsfw,
            expected_image_count,
        )
        .await
    {
        Ok(placeholders) => placeholders,
        Err(error) => {
            return ImageGenJobExecutionReport {
                outcome: ImageGenJobExecutionOutcome::Failed,
                prompt,
                caption_text,
                image_url: None,
                image_urls: Vec::new(),
                result_message_id: None,
                error: Some(format!("send initial placeholders: {error}")),
            };
        }
    };

    let chat_id = params.chat_id;
    let thread_id = params.thread_id;
    let is_nsfw = params.is_nsfw;
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

    // Fill the album progressively: each streamed provider image lands in the
    // first unfilled placeholder as soon as it arrives (arrival order, not
    // provider slot order); the first fill carries the caption.
    let (progress_tx, mut progress_rx) =
        tokio::sync::mpsc::unbounded_channel::<ImageGenerationResult>();
    let generate = generator.generate_image_streaming(request, progress_tx);
    let fill = async {
        let mut filled = 0usize;
        while let Some(partial) = progress_rx.recv().await {
            for photo in image_generation_photo_sources(&partial) {
                if filled >= placeholders.len() {
                    break;
                }
                let caption = if filled == 0 {
                    display_caption.clone()
                } else {
                    String::new()
                };
                match effects
                    .replace_placeholder_image(
                        chat_id,
                        placeholders[filled],
                        thread_id,
                        photo,
                        caption,
                        is_nsfw,
                    )
                    .await
                {
                    Ok(_) => filled += 1,
                    Err(error) => {
                        tracing::debug!(%error, "progressive placeholder fill failed");
                    }
                }
            }
        }
        filled
    };
    let (result, filled) = tokio::join!(generate, fill);

    match result {
        Ok(result) => {
            let image_urls = image_generation_urls(&result);
            let image_url = image_urls.first().cloned();
            if filled == 0 {
                cleanup_image_placeholders(effects, chat_id, &placeholders).await;
                return ImageGenJobExecutionReport {
                    outcome: ImageGenJobExecutionOutcome::Failed,
                    prompt,
                    caption_text,
                    image_url: None,
                    image_urls,
                    result_message_id: None,
                    error: Some("image generation produced no image".to_owned()),
                };
            }
            for placeholder in placeholders[filled..].iter().rev() {
                effects
                    .delete_placeholder_image(chat_id, *placeholder)
                    .await;
            }
            effects
                .record_last_generation(
                    chat_id,
                    params.user_id,
                    placeholders[..filled].to_vec(),
                    display_caption,
                )
                .await;
            effects.clear_draw_signal(chat_id, params.message_id).await;
            ImageGenJobExecutionReport {
                outcome: ImageGenJobExecutionOutcome::Completed,
                prompt,
                caption_text,
                image_url,
                image_urls,
                result_message_id: placeholders.first().copied(),
                error: None,
            }
        }
        Err(ImageGenerationError::Forbidden) => {
            // The safety verdict is user-relevant detail the obligations
            // watcher's generic failure notice cannot provide — always shown.
            cleanup_image_placeholders(effects, chat_id, &placeholders).await;
            effects
                .send_nsfw_blocked_message(
                    chat_id,
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
                image_urls: Vec::new(),
                result_message_id: None,
                error: None,
            }
        }
        Err(error) => {
            cleanup_image_placeholders(effects, chat_id, &placeholders).await;
            ImageGenJobExecutionReport {
                outcome: ImageGenJobExecutionOutcome::Failed,
                prompt,
                caption_text,
                image_url: None,
                image_urls: Vec::new(),
                result_message_id: None,
                error: Some(error.message()),
            }
        }
    }
}

/// Delete album placeholder frames, last frame first.
async fn cleanup_image_placeholders<Effects>(effects: &Effects, chat_id: i64, placeholders: &[i32])
where
    Effects: ImageJobEffects + Sync,
{
    for placeholder in placeholders.iter().rev() {
        effects
            .delete_placeholder_image(chat_id, *placeholder)
            .await;
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
    let expected_image_count = editor
        .expected_image_count()
        .clamp(1, TELEGRAM_MEDIA_GROUP_MAX_ITEMS);
    let display_caption = build_image_generation_caption(
        &params.prompt,
        &params.user_full_name,
        expected_image_count > 1,
        false,
    );

    effects
        .signal_draw_progress(params.chat_id, params.message_id)
        .await;
    let placeholders = match effects
        .send_initial_placeholders(
            params.chat_id,
            params.message_id,
            params.thread_id,
            display_caption.clone(),
            false,
            expected_image_count,
        )
        .await
    {
        Ok(placeholders) => placeholders,
        Err(error) => {
            return ImageEditJobExecutionReport {
                outcome: ImageEditJobExecutionOutcome::Failed,
                prompt: params.prompt,
                image_urls: Vec::new(),
                result_message_id: None,
                error: Some(format!("send initial placeholders: {error}")),
            };
        }
    };
    let request = ImageEditRequest {
        chat_id: params.chat_id,
        message_id: params.message_id,
        user_id: params.user_id,
        user_full_name: params.user_full_name.clone(),
        thread_id: params.thread_id,
        prompt: params.prompt.clone(),
        photo_file_id: params.photo_file_id,
        photo_urls: params.photo_urls,
    };

    let result = editor.edit_image(request).await;

    match result {
        Ok(result) => {
            let image_bytes = result
                .image_bytes
                .into_iter()
                .filter(|image| !image.is_empty())
                .collect::<Vec<_>>();
            let image_urls = result
                .image_urls
                .into_iter()
                .filter_map(non_empty)
                .collect::<Vec<_>>();
            let photos = image_edit_photo_sources(&image_bytes, &image_urls)
                .into_iter()
                .take(placeholders.len())
                .collect::<Vec<_>>();
            if photos.is_empty() {
                cleanup_image_placeholders(effects, params.chat_id, &placeholders).await;
                return ImageEditJobExecutionReport {
                    outcome: ImageEditJobExecutionOutcome::Failed,
                    prompt: params.prompt,
                    image_urls,
                    result_message_id: None,
                    error: Some("image edit produced no image".to_owned()),
                };
            }
            let mut filled = 0usize;
            let mut last_fill_error: Option<String> = None;
            for photo in photos {
                if filled >= placeholders.len() {
                    break;
                }
                let caption = if filled == 0 {
                    display_caption.clone()
                } else {
                    String::new()
                };
                match effects
                    .replace_placeholder_image(
                        params.chat_id,
                        placeholders[filled],
                        params.thread_id,
                        photo,
                        caption,
                        false,
                    )
                    .await
                {
                    Ok(_) => filled += 1,
                    Err(error) => {
                        tracing::debug!(%error, "image edit placeholder fill failed");
                        last_fill_error = Some(error);
                    }
                }
            }
            if filled == 0 {
                cleanup_image_placeholders(effects, params.chat_id, &placeholders).await;
                let error = last_fill_error.unwrap_or_else(|| "no frame delivered".to_owned());
                return ImageEditJobExecutionReport {
                    outcome: ImageEditJobExecutionOutcome::Failed,
                    prompt: params.prompt,
                    image_urls,
                    result_message_id: None,
                    error: Some(format!("send generated image: {error}")),
                };
            }
            for placeholder in placeholders[filled..].iter().rev() {
                effects
                    .delete_placeholder_image(params.chat_id, *placeholder)
                    .await;
            }
            effects
                .record_last_generation(
                    params.chat_id,
                    params.user_id,
                    placeholders[..filled].to_vec(),
                    display_caption,
                )
                .await;
            effects
                .clear_draw_signal(params.chat_id, params.message_id)
                .await;
            ImageEditJobExecutionReport {
                outcome: ImageEditJobExecutionOutcome::Completed,
                prompt: params.prompt,
                image_urls,
                result_message_id: placeholders.first().copied(),
                error: None,
            }
        }
        Err(ImageEditError::Forbidden) => {
            cleanup_image_placeholders(effects, params.chat_id, &placeholders).await;
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
                result_message_id: None,
                error: None,
            }
        }
        Err(error) => {
            cleanup_image_placeholders(effects, params.chat_id, &placeholders).await;
            ImageEditJobExecutionReport {
                outcome: ImageEditJobExecutionOutcome::Failed,
                prompt: params.prompt,
                image_urls: Vec::new(),
                result_message_id: None,
                error: Some(error.message()),
            }
        }
    }
}

/// Build photo sources for an edit result, preferring inline bytes then remote URLs.
fn image_edit_photo_sources(image_bytes: &[Vec<u8>], image_urls: &[String]) -> Vec<PhotoSource> {
    let single = image_bytes.len() == 1 && image_urls.is_empty();
    let mut photos = Vec::with_capacity(image_bytes.len() + image_urls.len());
    for (index, bytes) in image_bytes.iter().enumerate() {
        photos.push(PhotoSource::Bytes {
            file_name: if single {
                "image.png".to_owned()
            } else {
                format!("image-{}.png", index + 1)
            },
            bytes: bytes.clone(),
        });
    }
    photos.extend(image_urls.iter().cloned().map(PhotoSource::Url));
    photos
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
        ImageQueuePollOptions::at(now),
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
    options: ImageQueuePollOptions<'_>,
) -> ImageGenQueuePollReport
where
    Generator: ImageGenerator + Sync,
    Effects: ImageJobEffects + Sync,
{
    let Some(work) = queue.dequeue_matching(queue_name, worker_id, options.now, |job| {
        job.data.job_type == JobType::ImageGen
    }) else {
        return ImageGenQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: None,
            outcome: ImageGenQueuePollOutcome::Idle,
            error: None,
            activity: TelegramActivitySnapshot::default(),
        };
    };

    let params = match image_gen_job_params_from_stateless_job(&work.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), options.now);
            return ImageGenQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageGenQueuePollOutcome::DecodeFailed,
                error: Some(error),
                activity: TelegramActivitySnapshot::default(),
            };
        }
    };

    let _ = queue.set_execution_started(work.id, options.now);
    let activity_guard = options.activity_pulse.map(|pulse| {
        pulse.start(
            params.chat_id,
            params.thread_id,
            TelegramActivityAction::UploadPhoto,
        )
    });
    let execution = execute_image_gen_job(generator, effects, params).await;
    let activity = activity_guard
        .as_ref()
        .map_or_else(TelegramActivitySnapshot::default, |guard| guard.snapshot());
    drop(activity_guard);
    match execution.outcome {
        ImageGenJobExecutionOutcome::Completed => {
            if !execution.image_urls.is_empty() {
                let _ = queue.set_job_image_urls(work.id, execution.image_urls);
            } else if let Some(image_url) = execution.image_url {
                let _ = queue.set_job_image_urls(work.id, vec![image_url]);
            }
            let _ = queue.update_job_result_message(work.id, execution.result_message_id);
            finalize_completed(
                queue,
                work.id,
                queue_name,
                ImageGenQueuePollOutcome::Completed,
                activity,
                options.now,
            )
        }
        ImageGenJobExecutionOutcome::SafetyBlocked => finalize_completed(
            queue,
            work.id,
            queue_name,
            ImageGenQueuePollOutcome::SafetyBlocked,
            activity,
            options.now,
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
                options.max_llm_job_attempts,
                activity.clone(),
                options.now,
            ) {
                return report;
            }
            let _ = queue.fail(work.id, error.clone(), options.now);
            ImageGenQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageGenQueuePollOutcome::Failed,
                error: Some(error),
                activity,
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
    activity: TelegramActivitySnapshot,
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
            activity,
        });
    }
    queue.requeue_job_to_queue(work.id, &target_queue).ok()?;
    Some(ImageGenQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(work.id),
        outcome: ImageGenQueuePollOutcome::RetryRequeued,
        error: None,
        activity,
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
        ImageQueuePollOptions::at(now),
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
    options: ImageQueuePollOptions<'_>,
) -> ImageEditQueuePollReport
where
    Editor: ImageEditor + Sync,
    Effects: ImageJobEffects + Sync,
{
    let Some(work) = queue.dequeue_matching(queue_name, worker_id, options.now, |job| {
        job.data.job_type == JobType::ImageEdit
    }) else {
        return ImageEditQueuePollReport {
            queue_name: queue_name.to_owned(),
            job_id: None,
            outcome: ImageEditQueuePollOutcome::Idle,
            error: None,
            activity: TelegramActivitySnapshot::default(),
        };
    };

    let params = match image_edit_job_params_from_stateless_job(&work.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            let _ = queue.fail(work.id, error.clone(), options.now);
            return ImageEditQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageEditQueuePollOutcome::DecodeFailed,
                error: Some(error),
                activity: TelegramActivitySnapshot::default(),
            };
        }
    };

    let _ = queue.set_execution_started(work.id, options.now);
    let activity_guard = options.activity_pulse.map(|pulse| {
        pulse.start(
            params.chat_id,
            params.thread_id,
            TelegramActivityAction::UploadPhoto,
        )
    });
    let execution = execute_image_edit_job(editor, effects, params).await;
    let activity = activity_guard
        .as_ref()
        .map_or_else(TelegramActivitySnapshot::default, |guard| guard.snapshot());
    drop(activity_guard);
    match execution.outcome {
        ImageEditJobExecutionOutcome::Completed => {
            if !execution.image_urls.is_empty() {
                let _ = queue.set_job_image_urls(work.id, execution.image_urls);
            }
            let _ = queue.update_job_result_message(work.id, execution.result_message_id);
            finalize_image_edit_completed(
                queue,
                work.id,
                queue_name,
                ImageEditQueuePollOutcome::Completed,
                activity,
                options.now,
            )
        }
        ImageEditJobExecutionOutcome::SafetyBlocked => finalize_image_edit_completed(
            queue,
            work.id,
            queue_name,
            ImageEditQueuePollOutcome::SafetyBlocked,
            activity,
            options.now,
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
                options.max_llm_job_attempts,
                activity.clone(),
                options.now,
            ) {
                return report;
            }
            let _ = queue.fail(work.id, error.clone(), options.now);
            ImageEditQueuePollReport {
                queue_name: queue_name.to_owned(),
                job_id: Some(work.id),
                outcome: ImageEditQueuePollOutcome::Failed,
                error: Some(error),
                activity,
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
    activity: TelegramActivitySnapshot,
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
            activity,
        });
    }
    queue.requeue_job_to_queue(work.id, &target_queue).ok()?;
    Some(ImageEditQueuePollReport {
        queue_name: queue_name.to_owned(),
        job_id: Some(work.id),
        outcome: ImageEditQueuePollOutcome::RetryRequeued,
        error: None,
        activity,
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
        ImageWorkerRunOptions::every(interval),
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
    options: ImageWorkerRunOptions<'_>,
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
            () = tokio::time::sleep(options.interval) => {
                for queue_name in queue_names {
                    let tick = run_image_gen_queue_once_with_max_attempts(
                        queue,
                        queue_name,
                        generator,
                        effects,
                        image_worker_id(queue_name),
                        ImageQueuePollOptions {
                            max_llm_job_attempts: options.max_llm_job_attempts,
                            activity_pulse: options.activity_pulse,
                            now: OffsetDateTime::now_utc(),
                        },
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
        ImageWorkerRunOptions::every(interval),
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
    options: ImageWorkerRunOptions<'_>,
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
            () = tokio::time::sleep(options.interval) => {
                for queue_name in queue_names {
                    let tick = run_image_edit_queue_once_with_max_attempts(
                        queue,
                        queue_name,
                        editor,
                        effects,
                        image_worker_id(queue_name),
                        ImageQueuePollOptions {
                            max_llm_job_attempts: options.max_llm_job_attempts,
                            activity_pulse: options.activity_pulse,
                            now: OffsetDateTime::now_utc(),
                        },
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
    activity: TelegramActivitySnapshot,
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
        activity,
    }
}

fn finalize_image_edit_completed(
    queue: &InMemoryTaskQueue,
    job_id: i64,
    queue_name: &str,
    outcome: ImageEditQueuePollOutcome,
    activity: TelegramActivitySnapshot,
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
        activity,
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
        activity_action = tick
            .activity
            .action
            .map(|action| action.as_telegram_action()),
        activity_pulses_sent = tick.activity.sent,
        activity_pulse_failures = tick.activity.failed,
        activity_pulse_skips = tick.activity.skipped,
        activity_pulse_last_error = tick.activity.last_error.as_deref(),
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

fn generated_boogu_session_hash() -> String {
    let mut random = [0_u8; 8];
    rand::rng().fill_bytes(&mut random);
    format!("openplotva-{}-{}", now_unix_millis(), hex::encode(random))
}

fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn boogu_generation_prompt(request: &ImageGenerationRequest) -> String {
    request
        .prompt_variants
        .iter()
        .find_map(|variant| non_empty(variant.clone()))
        .or_else(|| non_empty(request.prompt.clone()))
        .or_else(|| non_empty(request.caption_text.clone()))
        .unwrap_or_default()
}

fn boogu_generation_seed(seed: &str) -> (i64, bool) {
    match seed.trim().parse::<i64>() {
        Ok(seed) => (seed, false),
        Err(_) => (BOOGU_GRADIO_DEFAULT_SEED, true),
    }
}

fn boogu_endpoint(base_url: &str, path: &str) -> Result<String, String> {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return Err("base URL is empty".to_owned());
    }
    Ok(format!("{base_url}{path}"))
}

fn boogu_queue_data_url(base_url: &str, session_hash: &str) -> Result<String, String> {
    let mut url = url::Url::parse(&boogu_endpoint(base_url, "/gradio_api/queue/data")?)
        .map_err(|error| format!("invalid queue data URL: {error}"))?;
    url.query_pairs_mut()
        .append_pair("session_hash", session_hash);
    Ok(url.into())
}

fn boogu_headers(
    base_url: &str,
    content_type: Option<&str>,
    accept: Option<&str>,
) -> Result<BTreeMap<String, String>, String> {
    let mut headers = BTreeMap::new();
    headers.insert("User-Agent".to_owned(), BOOGU_GRADIO_USER_AGENT.to_owned());
    headers.insert("Origin".to_owned(), boogu_origin(base_url)?);
    headers.insert("Referer".to_owned(), boogu_referer(base_url)?);
    if let Some(content_type) = content_type {
        headers.insert("Content-Type".to_owned(), content_type.to_owned());
    }
    if let Some(accept) = accept {
        headers.insert("Accept".to_owned(), accept.to_owned());
    }
    Ok(headers)
}

fn boogu_origin(base_url: &str) -> Result<String, String> {
    let url = url::Url::parse(base_url.trim())
        .map_err(|error| format!("invalid Boogu base URL: {error}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| "Boogu base URL host is empty".to_owned())?;
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Ok(format!("{}://{}{}", url.scheme(), host, port))
}

fn boogu_referer(base_url: &str) -> Result<String, String> {
    Ok(format!("{}/", boogu_origin(base_url)?))
}

fn boogu_http_status_error(context: &str, status_code: u16, body: &[u8]) -> String {
    let reason = if status_code == 429 || status_code >= 500 {
        "provider_unavailable"
    } else {
        "request_failed"
    };
    let body = String::from_utf8_lossy(body);
    format!("{reason}: {context} status {status_code}: {}", body.trim())
}

fn parse_boogu_completed_url(sse: &str) -> Result<String, String> {
    let mut saw_completed = false;
    for data in boogu_sse_data_messages(sse) {
        let Ok(value) = serde_json::from_str::<Value>(&data) else {
            continue;
        };
        let completed = value
            .get("msg")
            .and_then(Value::as_str)
            .is_some_and(|msg| msg == "process_completed")
            || value.get("output").is_some();
        if !completed {
            continue;
        }
        saw_completed = true;
        if value.get("success").and_then(Value::as_bool) == Some(false) {
            return Err(format!("queue completed without success: {value}"));
        }
        if let Some(url) = value
            .get("output")
            .and_then(|output| output.get("data"))
            .and_then(find_first_boogu_url)
        {
            return Ok(url);
        }
    }
    if saw_completed {
        Err("queue completed without image URL".to_owned())
    } else {
        Err("queue data stream ended before process_completed".to_owned())
    }
}

fn boogu_sse_data_messages(sse: &str) -> Vec<String> {
    let mut messages = Vec::new();
    let mut current = Vec::new();
    for line in sse.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            if !current.is_empty() {
                messages.push(current.join("\n"));
                current.clear();
            }
            continue;
        }
        if let Some(data) = line.strip_prefix("data:") {
            current.push(data.trim_start().to_owned());
        }
    }
    if !current.is_empty() {
        messages.push(current.join("\n"));
    }
    messages
}

fn find_first_boogu_url(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => map
            .get("url")
            .and_then(Value::as_str)
            .and_then(|url| non_empty(url.to_owned()))
            .or_else(|| map.values().find_map(find_first_boogu_url)),
        Value::Array(items) => items.iter().find_map(find_first_boogu_url),
        _ => None,
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
                activity_action = tick
                    .activity
                    .action
                    .map(|action| action.as_telegram_action()),
                activity_pulses_sent = tick.activity.sent,
                activity_pulse_failures = tick.activity.failed,
                activity_pulse_skips = tick.activity.skipped,
                activity_pulse_last_error = tick.activity.last_error.as_deref(),
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
                activity_action = tick
                    .activity
                    .action
                    .map(|action| action.as_telegram_action()),
                activity_pulses_sent = tick.activity.sent,
                activity_pulse_failures = tick.activity.failed,
                activity_pulse_skips = tick.activity.skipped,
                activity_pulse_last_error = tick.activity.last_error.as_deref(),
                error = tick.error.as_deref().unwrap_or_default(),
                "image edit taskman job failed"
            );
        }
    }
}

#[must_use]
pub fn build_image_generation_caption(
    prompt: &str,
    author: &str,
    is_vip: bool,
    is_nsfw: bool,
) -> String {
    build_image_generation_caption_with_support(
        prompt,
        author,
        is_vip,
        is_nsfw,
        random_image_support_phrase(),
    )
}

#[must_use]
pub fn build_image_generation_caption_with_support(
    prompt: &str,
    author: &str,
    is_vip: bool,
    is_nsfw: bool,
    support_text: &str,
) -> String {
    let cleaned_prompt = ensure_telegram_safe_text(prompt.trim());
    let cleaned_author = ensure_telegram_safe_text(author.trim());
    let mut author_text = escape_telegram_html_text(&cleaned_author);
    if is_vip {
        author_text = format!("<a href=\"{DRAW_VIP_URL}\">VIP-персоны  👑</a> {author_text}");
    }
    if is_nsfw {
        author_text.push_str("\n#nsfw 18+");
    }
    let support_text = escape_telegram_html_text(support_text.trim());
    let prompt = if cleaned_prompt.is_empty() {
        "Изображение по запросу пользователя".to_owned()
    } else {
        cleaned_prompt
    };
    let expandable = draw_caption_prompt_word_count(&prompt) > DRAW_CAPTION_WORD_THRESHOLD;
    let prompt = trim_draw_caption_prompt(&prompt, expandable, &author_text, &support_text);
    format_draw_caption(&prompt, expandable, &author_text, &support_text)
}

fn draw_caption_prompt_word_count(prompt: &str) -> usize {
    prompt.split_whitespace().count()
}

fn trim_draw_caption_prompt(
    prompt: &str,
    expandable: bool,
    author_text: &str,
    support_text: &str,
) -> String {
    let caption = format_draw_caption(prompt, expandable, author_text, support_text);
    if telegram_caption_visible_len(&caption) <= TELEGRAM_CAPTION_MAX_VISIBLE {
        return prompt.to_owned();
    }

    let runes = prompt.chars().collect::<Vec<_>>();
    let mut best = DRAW_CAPTION_ELLIPSIS.to_owned();
    let mut low = 0usize;
    let mut high = runes.len();
    while low <= high {
        let mid = low + (high - low) / 2;
        let candidate = runes[..mid]
            .iter()
            .collect::<String>()
            .trim_end()
            .to_owned()
            + DRAW_CAPTION_ELLIPSIS;
        let caption = format_draw_caption(&candidate, expandable, author_text, support_text);
        if telegram_caption_visible_len(&caption) <= TELEGRAM_CAPTION_MAX_VISIBLE {
            best = candidate;
            low = mid.saturating_add(1);
        } else if mid == 0 {
            break;
        } else {
            high = mid - 1;
        }
    }
    best
}

fn format_draw_caption(
    prompt: &str,
    expandable: bool,
    author_text: &str,
    support_text: &str,
) -> String {
    let prompt_text = format_draw_caption_prompt(prompt, expandable);
    format!(
        "{prompt_text} \nза авторством <i>{author_text}</i>\n<a href=\"{DRAW_SUPPORT_ME_URL}\">{support_text}</a>"
    )
}

fn format_draw_caption_prompt(prompt: &str, expandable: bool) -> String {
    let escaped_prompt = escape_telegram_html_text(prompt);
    if expandable {
        return format!("<blockquote expandable>{escaped_prompt}</blockquote>");
    }
    format!("<code>{escaped_prompt}</code>")
}

fn telegram_caption_visible_len(caption: &str) -> usize {
    strip_telegram_html(caption).encode_utf16().count()
}

fn random_image_support_phrase() -> &'static str {
    let index = rand::rng().random_range(0..IMAGE_SUPPORT_PHRASES.len());
    IMAGE_SUPPORT_PHRASES[index]
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
    width: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<i32>,
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

fn draw_api_payload(
    prompt: &str,
    image_inputs: &[String],
    dimensions: Option<(i32, i32)>,
) -> serde_json::Result<Vec<u8>> {
    let (image_urls, image_b64) = split_draw_api_image_inputs(image_inputs);
    let image_url = one_or_many_strings(&image_urls);
    let image_b64 = if image_url.is_some() {
        None
    } else {
        one_or_many_strings(&image_b64)
    };
    serde_json::to_vec(&DrawApiGenerateRequest {
        prompt,
        width: dimensions.map(|(width, _)| width),
        height: dimensions.map(|(_, height)| height),
        image_url,
        image_b64,
    })
}

const DRAW_API_BASE_RESOLUTION: i32 = 1024;

/// Width/height for an explicit `N:M` aspect ratio, clamped to [1:2, 2:1] and
/// scaled around `DRAW_API_BASE_RESOLUTION` in 64px steps.
fn draw_api_dimensions(aspect_ratio: &str) -> Option<(i32, i32)> {
    let (hor_text, ver_text) = aspect_ratio.trim().split_once(':')?;
    let mut hor = hor_text.parse::<i32>().ok().filter(|value| *value > 0)?;
    let mut ver = ver_text.parse::<i32>().ok().filter(|value| *value > 0)?;
    let ratio = f64::from(hor) / f64::from(ver);
    if ratio > 2.0 {
        (hor, ver) = (2, 1);
    } else if ratio < 0.5 {
        (hor, ver) = (1, 2);
    }
    let divisor = gcd(hor, ver);
    hor /= divisor;
    ver /= divisor;
    let part = f64::from(DRAW_API_BASE_RESOLUTION * 2) / f64::from(hor + ver);
    let width = 64 * (part * f64::from(hor) / 64.0).floor() as i32;
    let height = 64 * (part * f64::from(ver) / 64.0).floor() as i32;
    (width > 0 && height > 0).then_some((width, height))
}

fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
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

fn image_generation_photo_sources(result: &ImageGenerationResult) -> Vec<PhotoSource> {
    let mut photos = Vec::new();
    let images = result
        .image_bytes
        .iter()
        .filter(|image| !image.is_empty())
        .collect::<Vec<_>>();
    let single_image = images.len() == 1 && image_generation_urls(result).is_empty();
    for (index, bytes) in images.into_iter().enumerate() {
        photos.push(PhotoSource::Bytes {
            file_name: if single_image {
                "image.png".to_owned()
            } else {
                format!("image-{}.png", index + 1)
            },
            bytes: bytes.clone(),
        });
    }
    photos.extend(
        image_generation_urls(result)
            .into_iter()
            .map(PhotoSource::Url),
    );
    photos
}

fn image_generation_urls(result: &ImageGenerationResult) -> Vec<String> {
    let mut urls = Vec::new();
    if let Some(url) = non_empty(result.image_url.clone()) {
        urls.push(url);
    }
    for url in result
        .image_urls
        .iter()
        .filter_map(|url| non_empty(url.clone()))
    {
        if !urls.iter().any(|existing| existing == &url) {
            urls.push(url);
        }
    }
    urls
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
            status.error.clone().unwrap_or_else(|| {
                "draw job failed without detail: service unavailable".to_owned()
            }),
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
    if draw_api_error_is_forbidden_minor_sexual(&message) {
        ImageGenerationError::Forbidden
    } else {
        ImageGenerationError::Provider(message)
    }
}

fn draw_api_error_is_forbidden_minor_sexual(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    if contains_any(
        &lower,
        &[
            "csam",
            "child sexual abuse",
            "sexual content involving minor",
            "sexual content involving child",
            "sexualized minor",
            "sexualized child",
            "underage nudity",
            "underage nude",
        ],
    ) {
        return true;
    }

    contains_any(
        &lower,
        &[
            "minor",
            "underage",
            "under-age",
            "child",
            "children",
            "prepubescent",
            "pre-pubescent",
        ],
    ) && contains_any(
        &lower,
        &[
            "sexual",
            "sex",
            "nude",
            "nudity",
            "porn",
            "erotic",
            "sexualized",
            "exploitation",
            "exploitative",
            "abuse",
        ],
    )
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(*needle))
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

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex, MutexGuard},
    };

    use openplotva_llm::aifarm::{AifarmHttpFuture, AifarmHttpResponse, CompletionError};
    use openplotva_llm::router::{
        BreakerSet, PoolRegistry, RouterHandle, RoutingTable, TriggerState,
    };
    use openplotva_taskman::{
        DEFAULT_LLM_JOB_MAX_ATTEMPTS, DEFAULT_PRIORITY, HIGHEST_PRIORITY, IMAGE_REGULAR_QUEUE_NAME,
        IMAGE_VIP_QUEUE_NAME, ImageEditJobParams, ImageGenJobParams, ImageJobData, JobPayload,
        JobStatus, LLM_JOB_RETRY_EXHAUSTED_STAGE, LLM_JOB_RETRY_STAGE, StatelessJobItem,
        TaskQueueJobEvent, TelegramData, new_image_edit_job_at, new_image_gen_job_at,
    };
    use openplotva_telegram::TelegramOutboundMethodKind;
    use serde_json::json;

    use super::*;

    #[derive(Debug)]
    struct RecordingActivityEffects {
        report: crate::telegram_activity::TelegramActivityReport,
        calls: Mutex<
            Vec<(
                i64,
                Option<i32>,
                crate::telegram_activity::TelegramActivityAction,
            )>,
        >,
    }

    impl RecordingActivityEffects {
        fn new(report: crate::telegram_activity::TelegramActivityReport) -> Self {
            Self {
                report,
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(
            &self,
        ) -> Vec<(
            i64,
            Option<i32>,
            crate::telegram_activity::TelegramActivityAction,
        )> {
            self.calls.lock().expect("activity calls").clone()
        }
    }

    impl crate::telegram_activity::TelegramActivityEffects for RecordingActivityEffects {
        fn send_activity_action<'a>(
            &'a self,
            chat_id: i64,
            thread_id: Option<i32>,
            action: crate::telegram_activity::TelegramActivityAction,
        ) -> crate::telegram_activity::TelegramActivityFuture<'a> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("activity calls")
                    .push((chat_id, thread_id, action));
                self.report.clone()
            })
        }
    }

    fn activity_pulse(
        effects: Arc<RecordingActivityEffects>,
    ) -> crate::telegram_activity::TelegramActivityPulse {
        crate::telegram_activity::TelegramActivityPulse::new(
            crate::telegram_activity::TelegramActivityPulseSettings::from_millis(true, 1_000, 0),
            effects,
        )
    }

    #[tokio::test]
    async fn execute_image_gen_job_matches_go_sanitize_prompt_caption_and_effect_order() {
        let generator = GeneratorStub::success(" https://img.test/1.png ");
        let effects = EffectsStub::new();
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
        assert_eq!(report.result_message_id, Some(888));
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
        let calls = effects.calls();
        assert_eq!(calls.len(), 5);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(
            calls[1].starts_with("send_placeholders:-100:20:9:1:<code>original caption</code>")
        );
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Url(\"https://img.test/1.png\"):<code>original caption</code>"
        ));
        assert!(
            calls[3].starts_with("record_last_gen:-100:30:[888]:<code>original caption</code>")
        );
        assert_eq!(calls[4], "react-clear:-100:20");
    }

    #[tokio::test]
    async fn reaction_ux_single_image_fills_placeholder_and_clears_reaction() {
        let generator = GeneratorStub::success("https://img.test/1.png");
        let effects = EffectsStub::new();
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "neon castle".to_owned(),
                original_text: "neon castle".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:1:"));
        assert!(
            calls[2].starts_with("replace_placeholder:-100:888:9:Url(\"https://img.test/1.png\")")
        );
        assert_eq!(
            calls.last().map(String::as_str),
            Some("react-clear:-100:20")
        );
    }

    #[tokio::test]
    async fn execute_image_gen_job_fills_first_placeholder_with_first_arrival() {
        // Parallel slots where slot 1 is gated on the first album edit: slot 2's
        // image arrives first and must land in the FIRST placeholder with the
        // caption (arrival order, not provider slot order).
        let gate = Arc::new(tokio::sync::Notify::new());
        let generator = ParallelImageGenerator::new(
            WaitingGeneratorStub::success("https://img.test/slow-1.png", Arc::clone(&gate)),
            GeneratorStub::success("https://img.test/fast-2.png"),
        );
        let effects = EffectsStub::new()
            .with_placeholder_ids(vec![888, 889])
            .with_replace_notify(Arc::clone(&gate));
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:2:"));
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Url(\"https://img.test/fast-2.png\"):<code>castle</code>"
        ));
        assert!(
            calls[3].starts_with(
                "replace_placeholder:-100:889:9:Url(\"https://img.test/slow-1.png\"):"
            )
        );
        assert!(calls[4].starts_with("record_last_gen:-100:30:[888, 889]:"));
    }

    #[tokio::test]
    async fn reaction_ux_failure_leaves_notice_to_watcher_and_keeps_reaction() {
        let generator = GeneratorStub::error("boom");
        let effects = EffectsStub::new();
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "neon castle".to_owned(),
                original_text: "neon castle".to_owned(),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Failed);
        assert_eq!(report.result_message_id, None);
        // The placeholders are withdrawn; the generic failure notice belongs to
        // the obligations watcher, and the reaction is never cleared on errors
        // (the watcher swaps it to the terminal signal).
        let calls = effects.calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:0:1:"));
        assert_eq!(calls[2], "delete_placeholder:-100:888");
    }

    #[tokio::test]
    async fn reaction_ux_safety_block_still_sends_specific_notice() {
        let generator = GeneratorStub::forbidden();
        let effects = EffectsStub::new();
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "neon castle".to_owned(),
                original_text: "neon castle".to_owned(),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::SafetyBlocked);
        assert_eq!(report.result_message_id, None);
        let calls = effects.calls();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:0:1:"));
        assert_eq!(calls[2], "delete_placeholder:-100:888");
        assert_eq!(calls[3], "send_nsfw:-100:20:30:0");
        assert!(
            calls.iter().all(|call| !call.starts_with("react-clear")),
            "reaction is cleared only on success: {calls:?}"
        );
    }

    #[tokio::test]
    async fn execute_image_gen_job_delivers_all_provider_images_as_captioned_album() {
        let generator = GeneratorStub::success_many(vec![
            " https://img.test/1.png ".to_owned(),
            "https://img.test/2.png".to_owned(),
        ])
        .with_expected_image_count(2);
        let effects = EffectsStub::new().with_placeholder_ids(vec![888, 889]);
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                original_text: "original caption".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert_eq!(calls.len(), 6);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(
            calls[1].starts_with("send_placeholders:-100:20:9:2:<code>original caption</code>")
        );
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Url(\"https://img.test/1.png\"):<code>original caption</code>"
        ));
        assert!(
            calls[3].starts_with("replace_placeholder:-100:889:9:Url(\"https://img.test/2.png\"):")
        );
        assert!(calls[4].starts_with("record_last_gen:-100:30:[888, 889]:"));
        assert_eq!(calls[5], "react-clear:-100:20");
    }

    #[tokio::test]
    async fn execute_image_gen_job_deletes_trailing_placeholders_from_the_end_on_shortfall() {
        // Three slots expected, one image delivered → the two unfilled trailing
        // frames are deleted last-first, and only the filled frame is recorded.
        let generator =
            GeneratorStub::success("https://img.test/1.png").with_expected_image_count(3);
        let effects = EffectsStub::new().with_placeholder_ids(vec![888, 889, 890]);
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert_eq!(calls.len(), 7);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:3:"));
        assert!(
            calls[2].starts_with("replace_placeholder:-100:888:9:Url(\"https://img.test/1.png\")")
        );
        assert_eq!(calls[3], "delete_placeholder:-100:890");
        assert_eq!(calls[4], "delete_placeholder:-100:889");
        assert!(calls[5].starts_with("record_last_gen:-100:30:[888]:"));
        assert_eq!(calls[6], "react-clear:-100:20");
    }

    #[tokio::test]
    async fn execute_image_gen_job_forbidden_after_partial_fill_deletes_filled_frames_too() {
        // Slot 2 streams an image and fills the first frame, then slot 1 ends
        // Forbidden → the combined verdict wins: every frame (filled included)
        // is deleted and the NSFW notice is sent.
        let gate = Arc::new(tokio::sync::Notify::new());
        let generator = ParallelImageGenerator::new(
            WaitingForbiddenGeneratorStub::new(Arc::clone(&gate)),
            GeneratorStub::success("https://img.test/fast-2.png"),
        );
        let effects = EffectsStub::new()
            .with_placeholder_ids(vec![888, 889])
            .with_replace_notify(Arc::clone(&gate));
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::SafetyBlocked);
        assert_eq!(report.result_message_id, None);
        let calls = effects.calls();
        assert_eq!(calls.len(), 6);
        assert!(
            calls[2]
                .starts_with("replace_placeholder:-100:888:9:Url(\"https://img.test/fast-2.png\")")
        );
        assert_eq!(calls[3], "delete_placeholder:-100:889");
        assert_eq!(calls[4], "delete_placeholder:-100:888");
        assert_eq!(calls[5], "send_nsfw:-100:20:30:9");
    }

    #[test]
    fn image_generation_caption_matches_go_draw_caption_contract() {
        let caption = build_image_generation_caption_with_support(
            "sunset over <mountains>",
            "Alice & Bob",
            true,
            true,
            "support",
        );

        assert!(caption.contains("<code>sunset over &lt;mountains&gt;</code>"));
        assert!(caption.contains(
            "<a href=\"https://t.me/PlotvoBot?start=vip\">VIP-персоны  👑</a> Alice &amp; Bob"
        ));
        assert!(caption.contains("#nsfw 18+"));
        assert!(caption.contains("<a href=\"https://t.me/PlotvoBot?start=donate\">support</a>"));
    }

    #[tokio::test]
    async fn execute_image_gen_job_fills_placeholders_progressively_per_slot() {
        // Two sequential slots → each streamed image is edited into its album
        // frame as it lands (first frame carries the caption), not only after
        // both complete.
        let generator = SequentialImageGenerator::new(
            GeneratorStub::success("https://img.test/s1.png"),
            GeneratorStub::success("https://img.test/s2.png"),
        );
        let effects = EffectsStub::new().with_placeholder_ids(vec![888, 889]);
        let report = execute_image_gen_job(
            &generator,
            &effects,
            ImageGenJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "castle".to_owned(),
                thread_id: Some(9),
                ..ImageGenJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageGenJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert_eq!(calls.len(), 6);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:2:<code>castle</code>"));
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Url(\"https://img.test/s1.png\"):<code>castle</code>"
        ));
        assert!(
            calls[3]
                .starts_with("replace_placeholder:-100:889:9:Url(\"https://img.test/s2.png\"):")
        );
        assert!(calls[4].starts_with("record_last_gen:-100:30:[888, 889]:"));
        assert_eq!(calls[5], "react-clear:-100:20");
    }

    #[tokio::test]
    async fn execute_image_gen_job_uses_meta_prompt_and_completes_safety_blocks() {
        let generator = GeneratorStub::forbidden();
        let effects = EffectsStub::new();
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
        let calls = effects.calls();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:0:1:"));
        assert_eq!(calls[2], "delete_placeholder:-100:888");
        assert_eq!(calls[3], "send_nsfw:-100:20:30:0");
    }

    #[tokio::test]
    async fn execute_image_edit_job_matches_go_sanitize_prompt_and_effect_order() {
        let editor = EditorStub::success(vec![
            " https://img.test/edit-1.png ".to_owned(),
            String::new(),
            "https://img.test/edit-2.png".to_owned(),
        ]);
        let effects = EffectsStub::new();
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
        assert_eq!(report.result_message_id, Some(888));
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
        let calls = effects.calls();
        assert_eq!(calls.len(), 5);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:1:<code>make it night</code>"));
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Url(\"https://img.test/edit-1.png\"):<code>make it night</code>"
        ));
        assert!(calls[3].starts_with("record_last_gen:-100:30:[888]:"));
        assert_eq!(calls[4], "react-clear:-100:20");
    }

    #[tokio::test]
    async fn execute_image_edit_job_delivers_parallel_editor_results_as_album() {
        // Two parallel editors → two placeholder frames, both filled; the album
        // machinery is editor-count-agnostic.
        let editor = ParallelImageEditor::new(
            EditorStub::success(vec!["https://img.test/edit-1.png".to_owned()]),
            EditorStub::success(vec!["https://img.test/edit-2.png".to_owned()]),
        );
        let effects = EffectsStub::new().with_placeholder_ids(vec![888, 889]);
        let report = execute_image_edit_job(
            &editor,
            &effects,
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
        )
        .await;

        assert_eq!(report.outcome, ImageEditJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert_eq!(calls.len(), 6);
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:2:"));
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Url(\"https://img.test/edit-1.png\"):<code>make it night</code>"
        ));
        assert!(
            calls[3].starts_with(
                "replace_placeholder:-100:889:9:Url(\"https://img.test/edit-2.png\"):"
            )
        );
        assert!(calls[4].starts_with("record_last_gen:-100:30:[888, 889]:"));
        assert_eq!(calls[5], "react-clear:-100:20");
    }

    #[tokio::test]
    async fn execute_image_edit_job_fails_when_no_frame_can_be_filled() {
        let editor = EditorStub::success(vec!["https://img.test/edit-1.png".to_owned()]);
        // Every editMessageMedia fails (e.g. the placeholder was deleted).
        let effects = EffectsStub::with_replace_error("Bad Request: message to edit not found");
        let report = execute_image_edit_job(
            &editor,
            &effects,
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
        )
        .await;

        assert_eq!(report.outcome, ImageEditJobExecutionOutcome::Failed);
        assert_eq!(report.result_message_id, None);
        assert!(
            report
                .error
                .as_deref()
                .is_some_and(|error| error.starts_with("send generated image:"))
        );
        let calls = effects.calls();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:1:"));
        assert!(calls[2].starts_with("replace_placeholder:-100:888:9:"));
        assert_eq!(calls[3], "delete_placeholder:-100:888");
    }

    #[tokio::test]
    async fn execute_image_edit_job_completes_safety_blocks() {
        let editor = EditorStub::forbidden();
        let effects = EffectsStub::new();
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
        let calls = effects.calls();
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:0:1:"));
        assert_eq!(calls[2], "delete_placeholder:-100:888");
        assert_eq!(calls[3], "send_nsfw:-100:20:30:0");
    }

    #[tokio::test]
    async fn execute_image_edit_job_deletes_placeholder_when_provider_fails() {
        let editor = EditorStub::error("draw api failed");
        let effects = EffectsStub::new();
        let report = execute_image_edit_job(
            &editor,
            &effects,
            ImageEditJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: " make it night ".to_owned(),
                thread_id: Some(9),
                ..ImageEditJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageEditJobExecutionOutcome::Failed);
        assert_eq!(report.prompt, "make it night");
        assert_eq!(report.error, Some("draw api failed".to_owned()));
        // The placeholder is withdrawn; the failure notice is the obligations
        // watcher's job and the lifecycle reaction stays on the trigger for the
        // watcher to overwrite.
        let calls = effects.calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:1:"));
        assert_eq!(calls[2], "delete_placeholder:-100:888");
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
                aspect_ratio: "1:1".to_owned(),
                seed: "seed 42".to_owned(),
                ..ImageGenerationRequest::default()
            }]
        );
    }

    #[tokio::test]
    async fn optimizing_image_generator_extracts_prompt_modifiers_like_go_part_image_prompt() {
        let generator = GeneratorStub::success("https://img.test/1.png");
        let optimizer =
            OptimizerStub::default().with_image_result(openplotva_media::ImageOptimize {
                input: "cat portrait | blurry lowres 16:9 123".to_owned(),
                outputs: vec!["cat portrait variant".to_owned()],
                aspect_ratio: String::new(),
                nsfw_result: openplotva_media::NsfwResult::Safe,
            });
        let optimizing = OptimizingImageGenerator::new(
            generator.clone(),
            crate::media::MediaPromptOptimizerService::new(Some(optimizer.clone())),
        );

        let result = optimizing
            .generate_image(ImageGenerationRequest {
                prompt: "cat portrait seed:123 16:9 | blurry lowres".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(
            optimizer.calls(),
            vec!["image:cat portrait | blurry lowres 16:9 123:1".to_owned()]
        );
        let request = &generator.requests()[0];
        assert_eq!(request.prompt, "cat portrait");
        assert_eq!(request.negative_prompt, "blurry lowres");
        assert_eq!(request.aspect_ratio, "16:9");
        assert_eq!(request.seed, "123");
    }

    #[tokio::test]
    async fn optimizing_image_generator_keeps_user_aspect_ratio_when_optimizer_suggests_another() {
        let generator = GeneratorStub::success("https://img.test/1.png");
        let optimizer =
            OptimizerStub::default().with_image_result(openplotva_media::ImageOptimize {
                input: "cat 16:9".to_owned(),
                outputs: vec!["cat variant".to_owned()],
                aspect_ratio: "2:3".to_owned(),
                nsfw_result: openplotva_media::NsfwResult::Safe,
            });
        let optimizing = OptimizingImageGenerator::new(
            generator.clone(),
            crate::media::MediaPromptOptimizerService::new(Some(optimizer.clone())),
        );

        let result = optimizing
            .generate_image(ImageGenerationRequest {
                prompt: "cat 16:9".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await;

        assert!(result.is_ok());
        let request = &generator.requests()[0];
        assert_eq!(request.prompt, "cat");
        assert_eq!(request.aspect_ratio, "16:9");
    }

    #[tokio::test]
    async fn optimizing_image_generator_uses_optimizer_aspect_ratio_when_user_omits_it() {
        let generator = GeneratorStub::success("https://img.test/1.png");
        let optimizer =
            OptimizerStub::default().with_image_result(openplotva_media::ImageOptimize {
                input: "cat".to_owned(),
                outputs: vec!["cat variant".to_owned()],
                aspect_ratio: "2:3".to_owned(),
                nsfw_result: openplotva_media::NsfwResult::Safe,
            });
        let optimizing = OptimizingImageGenerator::new(
            generator.clone(),
            crate::media::MediaPromptOptimizerService::new(Some(optimizer.clone())),
        );

        let result = optimizing
            .generate_image(ImageGenerationRequest {
                prompt: "cat".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(generator.requests()[0].aspect_ratio, "2:3");
    }

    #[tokio::test]
    async fn optimizing_image_generator_requests_variants_for_expected_image_slots() {
        let generator =
            GeneratorStub::success("https://img.test/1.png").with_expected_image_count(2);
        let optimizer =
            OptimizerStub::default().with_image_result(openplotva_media::ImageOptimize {
                input: "cat | blur 1:1 seed 42".to_owned(),
                outputs: vec![
                    "plotva swims near a roach".to_owned(),
                    "plotva sleeps near a roach".to_owned(),
                ],
                aspect_ratio: "16:9".to_owned(),
                nsfw_result: openplotva_media::NsfwResult::Safe,
            });
        let optimizing = OptimizingImageGenerator::new(
            generator.clone(),
            crate::media::MediaPromptOptimizerService::new(Some(optimizer.clone())),
        );

        let result = optimizing
            .generate_image(ImageGenerationRequest {
                prompt: "cat".to_owned(),
                negative_prompt: "blur".to_owned(),
                aspect_ratio: "1:1".to_owned(),
                seed: "seed 42".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(
            optimizer.calls(),
            vec!["image:cat | blur 1:1 seed 42:2".to_owned()]
        );
        assert_eq!(
            generator.requests()[0].prompt_variants,
            vec![
                "roach-fish swims near a roach-fish".to_owned(),
                "roach-fish sleeps near a roach-fish".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn sequential_image_generator_uses_prompt_slots_and_combines_results() {
        let first = GeneratorStub::success("https://img.test/first.png");
        let second = GeneratorStub::success("https://img.test/second.png");
        let sequential = SequentialImageGenerator::new(first.clone(), second.clone());

        let result = sequential
            .generate_image(ImageGenerationRequest {
                prompt: "fallback prompt".to_owned(),
                prompt_variants: vec!["first prompt".to_owned(), "second prompt".to_owned()],
                ..ImageGenerationRequest::default()
            })
            .await
            .expect("sequential result");

        assert_eq!(sequential.expected_image_count(), 2);
        assert_eq!(
            image_generation_urls(&result),
            vec![
                "https://img.test/first.png".to_owned(),
                "https://img.test/second.png".to_owned()
            ]
        );
        assert_eq!(
            first.requests()[0].prompt_variants,
            vec!["first prompt".to_owned()]
        );
        assert_eq!(
            second.requests()[0].prompt_variants,
            vec!["second prompt".to_owned()]
        );
    }

    #[tokio::test]
    async fn boogu_generation_submits_gradio_payload_and_reads_sse_url() {
        let transport = BooguTransportStub::new(vec![
            Ok(boogu_json_response(json!({"event_id": "evt-gen"}))),
            Ok(boogu_text_response(
                "event: process_completed\n\
                 data: {\"msg\":\"process_completed\",\"success\":true,\"output\":{\"data\":[{\"url\":\"https://demo-turbo.boogu.org/gradio_api/file=/tmp/gradio/image.png\"}]}}\n\n",
            )),
        ]);
        let probe = transport.clone();
        let client = BooguGradioImageClient::with_transport(
            boogu_image_test_config(),
            boogu_edit_test_config(),
            DataUrlStub::success("data:image/png;base64,ZmFrZQ=="),
            transport,
        );

        let result = client
            .generate_image_with_session_hash(
                ImageGenerationRequest {
                    prompt: "fallback prompt".to_owned(),
                    prompt_variants: vec!["  small red square icon ".to_owned()],
                    seed: "123".to_owned(),
                    ..ImageGenerationRequest::default()
                },
                "session-gen",
            )
            .await
            .expect("boogu generation result");

        assert_eq!(
            result.image_url,
            "https://demo-turbo.boogu.org/gradio_api/file=/tmp/gradio/image.png"
        );
        let requests = probe.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, BooguHttpMethod::Post);
        assert_eq!(
            requests[0].url,
            "https://demo-turbo.boogu.org/gradio_api/queue/join"
        );
        assert_eq!(requests[0].headers["Content-Type"], "application/json");
        assert_eq!(
            requests[0].headers["Origin"],
            "https://demo-turbo.boogu.org"
        );
        assert_eq!(
            requests[0].headers["Referer"],
            "https://demo-turbo.boogu.org/"
        );
        assert!(requests[0].headers["User-Agent"].contains("Mozilla/5.0"));
        let payload: Value = serde_json::from_slice(&requests[0].body).expect("join payload");
        assert_eq!(payload["fn_index"], 2);
        assert_eq!(payload["session_hash"], "session-gen");
        assert_eq!(
            payload["data"],
            json!([
                "small red square icon",
                123,
                false,
                false,
                "Recommended resolutions",
                "1024x1024 ( 1:1 )",
                1024,
                1024,
                3,
                null
            ])
        );
        assert_eq!(requests[1].method, BooguHttpMethod::Get);
        assert_eq!(
            requests[1].url,
            "https://demo-turbo.boogu.org/gradio_api/queue/data?session_hash=session-gen"
        );
        assert_eq!(requests[1].headers["Accept"], "text/event-stream");
    }

    #[tokio::test]
    async fn boogu_edit_uses_telegram_data_url_instead_of_photo_urls() {
        let transport = BooguTransportStub::new(vec![
            Ok(boogu_json_response(json!({"event_id": "evt-edit"}))),
            Ok(boogu_text_response(
                "event: process_completed\n\
                 data: {\"msg\":\"process_completed\",\"success\":true,\"output\":{\"data\":[{\"url\":\"https://demo-edit-turbo-1k5.boogu.org/gradio_api/file=/tmp/gradio/edit.png\"}]}}\n\n",
            )),
        ]);
        let probe = transport.clone();
        let data_urls = DataUrlStub::success("data:image/png;base64,ZmFrZS1wbmc=");
        let client = BooguGradioImageClient::with_transport(
            boogu_image_test_config(),
            boogu_edit_test_config(),
            data_urls.clone(),
            transport,
        );

        let result = client
            .edit_image_with_session_hash(
                ImageEditRequest {
                    prompt: " make the square blue ".to_owned(),
                    photo_file_id: "telegram-file-id".to_owned(),
                    photo_urls: vec![
                        "https://api.telegram.org/file/botSECRET/photos/leak.png".to_owned(),
                    ],
                    ..ImageEditRequest::default()
                },
                "session-edit",
            )
            .await
            .expect("boogu edit result");

        assert_eq!(
            result.image_urls,
            vec!["https://demo-edit-turbo-1k5.boogu.org/gradio_api/file=/tmp/gradio/edit.png"]
        );
        assert_eq!(data_urls.requested(), vec!["telegram-file-id"]);
        let requests = probe.requests();
        assert_eq!(requests[0].method, BooguHttpMethod::Post);
        assert_eq!(
            requests[0].url,
            "https://demo-edit-turbo-1k5.boogu.org/gradio_api/queue/join"
        );
        let body = String::from_utf8(requests[0].body.clone()).expect("utf8 body");
        assert!(!body.contains("api.telegram.org/file/botSECRET"));
        let payload: Value = serde_json::from_str(&body).expect("join payload");
        assert_eq!(payload["fn_index"], 2);
        assert_eq!(payload["session_hash"], "session-edit");
        assert_eq!(payload["data"][0], "make the square blue");
        assert_eq!(
            payload["data"][1]["url"],
            "data:image/png;base64,ZmFrZS1wbmc="
        );
        assert_eq!(payload["data"][1]["orig_name"], "image.png");
        assert_eq!(payload["data"][1]["meta"]["_type"], "gradio.FileData");
        assert_eq!(
            payload["data"],
            json!([
                "make the square blue",
                {
                    "url": "data:image/png;base64,ZmFrZS1wbmc=",
                    "orig_name": "image.png",
                    "meta": {"_type": "gradio.FileData"}
                },
                "",
                42,
                false,
                false,
                "1.5K",
                "Recommended resolutions",
                "1536x1536 ( 1:1 )",
                1536,
                1536,
                3,
                null
            ])
        );
    }

    #[tokio::test]
    async fn parallel_image_generator_streams_second_provider_without_waiting_for_first() {
        let first_notify = Arc::new(tokio::sync::Notify::new());
        let first =
            WaitingGeneratorStub::success("https://img.test/slow.png", Arc::clone(&first_notify));
        let second = GeneratorStub::success("https://img.test/fast.png");
        let generator = ParallelImageGenerator::new(first.clone(), second.clone());
        let (progress_tx, mut progress_rx) =
            tokio::sync::mpsc::unbounded_channel::<ImageGenerationResult>();

        let mut generate = generator.generate_image_streaming(
            ImageGenerationRequest {
                prompt_variants: vec!["slow prompt".to_owned(), "fast prompt".to_owned()],
                ..ImageGenerationRequest::default()
            },
            progress_tx,
        );

        let partial = tokio::time::timeout(StdDuration::from_millis(100), async {
            tokio::select! {
                partial = progress_rx.recv() => partial,
                result = &mut generate => panic!("parallel generation completed before slow provider was released: {result:?}"),
            }
        })
            .await
            .expect("fast provider should stream before slow provider is released")
            .expect("progress result");
        assert_eq!(partial.image_url, "https://img.test/fast.png");
        assert_eq!(first.requests().len(), 1);
        assert_eq!(second.requests().len(), 1);

        first_notify.notify_waiters();
        let result = generate.await.expect("parallel result");
        assert_eq!(
            image_generation_urls(&result),
            vec![
                "https://img.test/slow.png".to_owned(),
                "https://img.test/fast.png".to_owned()
            ]
        );
        assert_eq!(
            first.requests()[0].prompt_variants,
            vec!["slow prompt".to_owned()]
        );
        assert_eq!(
            second.requests()[0].prompt_variants,
            vec!["fast prompt".to_owned()]
        );
    }

    #[tokio::test]
    async fn parallel_image_generator_completes_with_partial_success_and_fails_when_all_fail() {
        let first = GeneratorStub::error("aifarm provider provider_unavailable: status 503");
        let second = GeneratorStub::success("https://img.test/boogu.png");
        let generator = ParallelImageGenerator::new(first.clone(), second.clone());

        let result = generator
            .generate_image(ImageGenerationRequest {
                prompt: "castle".to_owned(),
                ..ImageGenerationRequest::default()
            })
            .await
            .expect("partial success");

        assert_eq!(result.image_url, "https://img.test/boogu.png");
        assert_eq!(first.requests().len(), 1);
        assert_eq!(second.requests().len(), 1);

        let all_failed = ParallelImageGenerator::new(
            GeneratorStub::error("aifarm provider provider_unavailable: status 503"),
            GeneratorStub::error("boogu provider provider_unavailable: status 503"),
        )
        .generate_image(ImageGenerationRequest::default())
        .await;
        assert_eq!(
            all_failed,
            Err(ImageGenerationError::Provider(
                "boogu provider provider_unavailable: status 503".to_owned()
            ))
        );
    }

    #[tokio::test]
    async fn parallel_image_editor_combines_partial_success_and_preserves_forbidden() {
        let first = EditorStub::error("draw api failed");
        let second = EditorStub::success(vec!["https://img.test/boogu-edit.png".to_owned()]);
        let editor = ParallelImageEditor::new(first.clone(), second.clone());

        let result = editor
            .edit_image(ImageEditRequest {
                prompt: "make it night".to_owned(),
                ..ImageEditRequest::default()
            })
            .await
            .expect("partial edit success");

        assert_eq!(
            result.image_urls,
            vec!["https://img.test/boogu-edit.png".to_owned()]
        );
        assert_eq!(first.requests().len(), 1);
        assert_eq!(second.requests().len(), 1);

        let forbidden = ParallelImageEditor::new(
            EditorStub::forbidden(),
            EditorStub::success(vec!["https://img.test/ignored.png".to_owned()]),
        )
        .edit_image(ImageEditRequest::default())
        .await;
        assert_eq!(forbidden, Err(ImageEditError::Forbidden));
    }

    #[tokio::test]
    async fn parallel_image_editor_returns_both_slot_results_when_both_succeed() {
        let first = EditorStub::success(vec!["https://img.test/flux-edit.png".to_owned()]);
        let second = EditorStub::success(vec!["https://img.test/boogu-edit.png".to_owned()]);
        let editor = ParallelImageEditor::new(first.clone(), second.clone());

        let result = editor
            .edit_image(ImageEditRequest {
                prompt: "make it night".to_owned(),
                ..ImageEditRequest::default()
            })
            .await
            .expect("parallel edit result");

        assert_eq!(
            result.image_urls,
            vec![
                "https://img.test/flux-edit.png".to_owned(),
                "https://img.test/boogu-edit.png".to_owned()
            ]
        );
        assert_eq!(first.requests().len(), 1);
        assert_eq!(second.requests().len(), 1);
    }

    #[tokio::test]
    async fn parallel_image_provider_all_failure_requeues_retryable_jobs() {
        let queue = InMemoryTaskQueue::new();
        let now = OffsetDateTime::from_unix_timestamp(1_779_193_800).expect("time");
        queue.assign(
            IMAGE_VIP_QUEUE_NAME,
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
        let generator = ParallelImageGenerator::new(
            GeneratorStub::error("aifarm provider provider_unavailable: status 503"),
            GeneratorStub::error("boogu provider provider_unavailable: status 503"),
        );

        let report = run_image_gen_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &generator,
            &EffectsStub::new(),
            "image-worker-1",
            now,
        )
        .await;

        assert_eq!(report.outcome, ImageGenQueuePollOutcome::RetryRequeued);
        assert_eq!(queue.records()[0].status, JobStatus::Pending);
        assert_eq!(queue.records()[0].events[0].provider, "boogu");
    }

    #[test]
    fn vip_parallel_generation_has_two_slots_while_regular_stays_single_slot() {
        let vip = ParallelImageGenerator::new(
            GeneratorStub::success("https://img.test/flux.png"),
            GeneratorStub::success("https://img.test/boogu.png"),
        );
        let regular = GeneratorStub::success("https://img.test/regular.png");

        assert_eq!(vip.expected_image_count(), 2);
        assert_eq!(regular.expected_image_count(), 1);
    }

    fn empty_routed_attempt_walker() -> RoutedAttemptWalker {
        RoutedAttemptWalker::new(
            RouterHandle::new(RoutingTable::default()),
            Arc::new(BreakerSet::new()),
            Arc::new(TriggerState::new()),
            Arc::new(PoolRegistry::new()),
        )
    }

    #[test]
    fn routed_image_generator_accepts_slot_workflow_key() {
        let generator = RoutedImageGenerator::new(
            empty_routed_attempt_walker(),
            AifarmDrawApiConfig::default(),
        )
        .with_workflow_key(IMAGE_GENERATION_BOOGU_TURBO_WORKFLOW_KEY);

        assert_eq!(
            generator.workflow_key,
            IMAGE_GENERATION_BOOGU_TURBO_WORKFLOW_KEY
        );
    }

    #[test]
    fn routed_image_editor_accepts_slot_workflow_key() {
        let editor = RoutedImageEditor::new(
            empty_routed_attempt_walker(),
            DataUrlStub::success("data:image/png;base64,ZmFrZS1wbmc="),
            AifarmDrawApiConfig::default(),
        )
        .with_workflow_key(IMAGE_EDIT_BOOGU_TURBO_WORKFLOW_KEY);

        assert_eq!(editor.workflow_key, IMAGE_EDIT_BOOGU_TURBO_WORKFLOW_KEY);
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
            &EffectsStub::new(),
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
                activity: TelegramActivitySnapshot::default(),
            }
        );
        let record = &queue.records()[0];
        assert_eq!(record.status, JobStatus::Completed);
        assert_eq!(record.result_message_id, Some(888));
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

    #[tokio::test(start_paused = true)]
    async fn image_gen_queue_once_pulses_upload_photo_during_active_job() {
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
                    thread_id: Some(9),
                    ..ImageGenJobParams::default()
                },
                now,
            ),
        );
        let notify = Arc::new(tokio::sync::Notify::new());
        let generator =
            WaitingGeneratorStub::success("https://img.test/slow.png", Arc::clone(&notify));
        let effects = EffectsStub::new();
        let activity_effects = Arc::new(RecordingActivityEffects::new(
            crate::telegram_activity::TelegramActivityReport::Sent,
        ));
        let pulse = activity_pulse(Arc::clone(&activity_effects));

        let poll = run_image_gen_queue_once_with_max_attempts(
            &queue,
            IMAGE_REGULAR_QUEUE_NAME,
            &generator,
            &effects,
            "image-worker-1",
            ImageQueuePollOptions {
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                activity_pulse: Some(&pulse),
                now,
            },
        );
        tokio::pin!(poll);
        tokio::select! {
            _ = &mut poll => panic!("waiting generator should keep the job active"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
        }
        tokio::task::yield_now().await;

        assert_eq!(
            activity_effects.calls().first().copied(),
            Some((
                -100,
                Some(9),
                crate::telegram_activity::TelegramActivityAction::UploadPhoto,
            ))
        );
        notify.notify_one();
        let report = poll.await;

        assert_eq!(report.job_id, Some(job_id));
        assert_eq!(report.outcome, ImageGenQueuePollOutcome::Completed);
        assert!(
            report.activity.sent >= 1,
            "active image generation must report upload_photo pulse"
        );
        assert_eq!(queue.records()[0].status, JobStatus::Completed);
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
            &EffectsStub::new(),
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
            &EffectsStub::new(),
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
                activity: TelegramActivitySnapshot::default(),
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

    #[tokio::test(start_paused = true)]
    async fn image_edit_queue_once_pulses_upload_photo_during_active_job() {
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
        let notify = Arc::new(tokio::sync::Notify::new());
        let editor = WaitingEditorStub::success(
            vec!["https://img.test/edit-slow.png".to_owned()],
            Arc::clone(&notify),
        );
        let effects = EffectsStub::new();
        let activity_effects = Arc::new(RecordingActivityEffects::new(
            crate::telegram_activity::TelegramActivityReport::Sent,
        ));
        let pulse = activity_pulse(Arc::clone(&activity_effects));

        let poll = run_image_edit_queue_once_with_max_attempts(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &editor,
            &effects,
            "image-edit-worker-1",
            ImageQueuePollOptions {
                max_llm_job_attempts: DEFAULT_LLM_JOB_MAX_ATTEMPTS,
                activity_pulse: Some(&pulse),
                now,
            },
        );
        tokio::pin!(poll);
        tokio::select! {
            _ = &mut poll => panic!("waiting editor should keep the job active"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {}
        }
        tokio::task::yield_now().await;

        assert_eq!(
            activity_effects.calls().first().copied(),
            Some((
                -100,
                Some(9),
                crate::telegram_activity::TelegramActivityAction::UploadPhoto,
            ))
        );
        notify.notify_one();
        let report = poll.await;

        assert_eq!(report.job_id, Some(job_id));
        assert_eq!(report.outcome, ImageEditQueuePollOutcome::Completed);
        assert!(
            report.activity.sent >= 1,
            "active image edit must report upload_photo pulse"
        );
        assert_eq!(queue.records()[0].status, JobStatus::Completed);
        assert_eq!(editor.requests().len(), 1);
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
            &EffectsStub::new(),
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
            &EffectsStub::new(),
            "image-worker-1",
            ImageQueuePollOptions {
                max_llm_job_attempts: 2,
                activity_pulse: None,
                now,
            },
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
            &EffectsStub::new(),
            "image-edit-worker-1",
            ImageQueuePollOptions {
                max_llm_job_attempts: 2,
                activity_pulse: None,
                now,
            },
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
                    asr_data: None,
                    control_data: None,
                    agent_data: None,
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
            &EffectsStub::new(),
            "image-edit-worker-1",
            now,
        )
        .await;
        let provider_report = run_image_edit_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &EditorStub::error("provider down"),
            &EffectsStub::new(),
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
                    asr_data: None,
                    control_data: None,
                    agent_data: None,
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
                    asr_data: None,
                    control_data: None,
                    agent_data: None,
                },
            },
        );

        let decode_report = run_image_gen_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &GeneratorStub::success("ignored"),
            &EffectsStub::new(),
            "image-worker-1",
            now,
        )
        .await;
        let provider_report = run_image_gen_queue_once(
            &queue,
            IMAGE_VIP_QUEUE_NAME,
            &GeneratorStub::error("provider down"),
            &EffectsStub::new(),
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
                    asr_data: None,
                    control_data: None,
                    agent_data: None,
                },
            },
        );

        let report = run_regular_image_gen_queue_once(
            &queue,
            &GeneratorStub::success("ignored"),
            &EffectsStub::new(),
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
            activity: TelegramActivitySnapshot::default(),
        });
        report.record_poll(&ImageGenQueuePollReport {
            queue_name: IMAGE_REGULAR_QUEUE_NAME.to_owned(),
            job_id: Some(2),
            outcome: ImageGenQueuePollOutcome::Failed,
            error: Some("provider down".to_owned()),
            activity: TelegramActivitySnapshot::default(),
        });
        report.record_poll(&ImageGenQueuePollReport {
            queue_name: IMAGE_REGULAR_QUEUE_NAME.to_owned(),
            job_id: None,
            outcome: ImageGenQueuePollOutcome::Idle,
            error: None,
            activity: TelegramActivitySnapshot::default(),
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
                activity_pulses_sent: 0,
                activity_pulse_failures: 0,
                activity_pulse_skips: 0,
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
    fn draw_api_dimensions_match_go_set_aspect_ratio_math() {
        let cases: [(&str, Option<(i32, i32)>); 9] = [
            ("4:3", Some((1152, 832))),
            ("16:9", Some((1280, 704))),
            ("9:16", Some((704, 1280))),
            ("1:1", Some((1024, 1024))),
            ("2:1", Some((1344, 640))),
            ("3:1", Some((1344, 640))),
            ("1:3", Some((640, 1344))),
            ("", None),
            ("0:3", None),
        ];
        for (input, want) in cases {
            assert_eq!(draw_api_dimensions(input), want, "input: {input}");
        }
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

    #[test]
    fn draw_api_failed_without_detail_is_retryable_provider_failure() {
        let status = DrawApiJobStatus {
            status: "failed".to_owned(),
            error: None,
            result: None,
        };

        let DrawApiWaitDecision::Failed(message) = evaluate_draw_api_status(&status) else {
            panic!("terminal failed status must fail");
        };

        assert_eq!(
            openplotva_llm::retry::retryable_reason_from_message(&format!("job failed: {message}")),
            Some(openplotva_llm::retry::FailureReason::ProviderUnavailable)
        );
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
                service_name: AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
                endpoint_name: AIFARM_DRAW_API_ENDPOINT_NAME.to_owned(),
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
        assert_eq!(
            draw_request,
            json!({"prompt": "variant one", "width": 1280, "height": 704})
        );
        assert_eq!(requests[1].method, AifarmHttpMethod::Get);
        assert_eq!(requests[1].url, "https://draw.example.test/v1/jobs/job-1");
    }

    #[tokio::test]
    async fn aifarm_draw_api_generator_uses_configured_discovery_endpoint() {
        let draw_payload =
            serde_json::to_vec(&json!({"image_url": ["https://img.test/boogu.png"]}))
                .expect("draw payload");
        let response_body = general_purpose::STANDARD.encode(draw_payload);
        let transport = AifarmTransportStub::new(vec![
            Ok(json_response(
                json!({"job_id": "boogu-1", "state": "queued"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "boogu-1",
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
                service_name: AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
                endpoint_name: AIFARM_DRAW_API_BOOGU_TURBO_ENDPOINT_NAME.to_owned(),
                timeout: StdDuration::from_secs(5),
                poll_interval: StdDuration::from_nanos(1),
            },
            transport,
        );

        let _result = generator
            .generate_image_with_job_id(
                ImageGenerationRequest {
                    prompt: "boogu prompt".to_owned(),
                    caption_text: "boogu prompt".to_owned(),
                    ..ImageGenerationRequest::default()
                },
                "boogu-123",
            )
            .await
            .expect("draw result");

        let requests = probe.requests();
        let job: DiscoveryJobRequest =
            serde_json::from_slice(&requests[0].body).expect("job request");
        assert_eq!(job.invocation.service_name, AIFARM_DRAW_API_SERVICE_NAME);
        assert_eq!(
            job.invocation.endpoint_name,
            AIFARM_DRAW_API_BOOGU_TURBO_ENDPOINT_NAME
        );
    }

    #[test]
    fn routed_draw_api_config_prefers_model_endpoint_over_provider_endpoint() {
        let attempt = RoutedAttempt {
            provider_id: 1,
            model_id: 2,
            provider_name: "aifarm-draw".to_owned(),
            model_name: "boogu-image-turbo-sdnq".to_owned(),
            provider_endpoint: None,
            discovery_service_name: Some(AIFARM_DRAW_API_SERVICE_NAME.to_owned()),
            discovery_endpoint_name: Some(AIFARM_DRAW_API_ENDPOINT_NAME.to_owned()),
            provider_api_key_ref: None,
            provider_api_key_encrypted: None,
            model_base_url: None,
            embedding_dim: None,
            provider_config: json!({
                "service_name": AIFARM_DRAW_API_SERVICE_NAME,
                "endpoint_name": AIFARM_DRAW_API_ENDPOINT_NAME,
            }),
            model_config: json!({
                "service_name": AIFARM_DRAW_API_SERVICE_NAME,
                "endpoint_name": AIFARM_DRAW_API_BOOGU_TURBO_ENDPOINT_NAME,
            }),
            overrides: openplotva_llm::router::InferenceOverrides::default(),
            variant: None,
        };

        let cfg = draw_api_config_for_attempt(AifarmDrawApiConfig::default(), &attempt);

        assert_eq!(cfg.service_name, AIFARM_DRAW_API_SERVICE_NAME);
        assert_eq!(cfg.endpoint_name, AIFARM_DRAW_API_BOOGU_TURBO_ENDPOINT_NAME);
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
                service_name: AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
                endpoint_name: AIFARM_DRAW_API_ENDPOINT_NAME.to_owned(),
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
    async fn image_edit_job_delivers_draw_api_image_b64_result() {
        let image = vec![9_u8, 8, 7];
        let draw_payload = serde_json::to_vec(&json!({
            "image_b64": general_purpose::STANDARD.encode(&image)
        }))
        .expect("draw payload");
        let response_body = general_purpose::STANDARD.encode(draw_payload);
        let transport = AifarmTransportStub::new(vec![
            Ok(json_response(
                json!({"job_id": "edit-b64", "state": "queued"}),
            )),
            Ok(json_response(json!({
                "job": {
                    "job_id": "edit-b64",
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
        let editor = AifarmDrawApiImageGenerator::with_transport(
            AifarmDrawApiConfig {
                base_url: "https://draw.example.test".to_owned(),
                service_name: AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
                endpoint_name: AIFARM_DRAW_API_ENDPOINT_NAME.to_owned(),
                timeout: StdDuration::from_secs(5),
                poll_interval: StdDuration::from_nanos(1),
            },
            transport,
        );
        let effects = EffectsStub::new();

        let report = execute_image_edit_job(
            &editor,
            &effects,
            ImageEditJobParams {
                chat_id: -100,
                message_id: 20,
                user_id: 30,
                user_full_name: "Alice".to_owned(),
                prompt: "make it night".to_owned(),
                photo_urls: vec!["https://telegram.test/original.png".to_owned()],
                thread_id: Some(9),
                ..ImageEditJobParams::default()
            },
        )
        .await;

        assert_eq!(report.outcome, ImageEditJobExecutionOutcome::Completed);
        assert_eq!(report.result_message_id, Some(888));
        let calls = effects.calls();
        assert_eq!(calls.len(), 5);
        assert_eq!(calls[0], "react-progress:-100:20");
        assert!(calls[1].starts_with("send_placeholders:-100:20:9:1:"));
        assert!(calls[2].starts_with(
            "replace_placeholder:-100:888:9:Bytes { file_name: \"image.png\", bytes: [9, 8, 7] }:"
        ));
        assert!(calls[2].contains("make it night"));
        assert!(calls[3].starts_with("record_last_gen:-100:30:[888]:"));
        assert_eq!(calls[4], "react-clear:-100:20");
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
                    service_name: AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
                    endpoint_name: AIFARM_DRAW_API_ENDPOINT_NAME.to_owned(),
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

    #[derive(Debug, Default)]
    struct LastGenerationWriterStub {
        records: Mutex<Vec<openplotva_storage::LastGenerationRecord>>,
    }

    impl LastGenerationWriterStub {
        fn records(&self) -> Vec<openplotva_storage::LastGenerationRecord> {
            self.records.lock().expect("records").clone()
        }
    }

    impl ImageJobLastGenerationWriter for LastGenerationWriterStub {
        fn write_last_generation<'a>(
            &'a self,
            record: &'a openplotva_storage::LastGenerationRecord,
        ) -> ImageJobEffectFuture<'a, Result<(), String>> {
            self.records.lock().expect("records").push(record.clone());
            Box::pin(async { Ok(()) })
        }
    }

    #[tokio::test]
    async fn telegram_image_job_effects_send_initial_placeholders_uses_photo_for_single_slot() {
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Message(
            Box::new(telegram_message(-10042, 555)),
        ))]);
        let effects = TelegramImageJobEffects::new(telegram.clone());

        let result = effects
            .send_initial_placeholders(
                -10042,
                77,
                Some(9),
                "<code>caption</code>".to_owned(),
                false,
                1,
            )
            .await;

        assert_eq!(result, Ok(vec![555]));
        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::SendPhoto]
        );
    }

    #[tokio::test]
    async fn telegram_image_job_effects_send_initial_placeholders_uses_media_group_for_multi_slot()
    {
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Messages(vec![
            telegram_message(-10042, 555),
            telegram_message(-10042, 556),
        ]))]);
        let effects = TelegramImageJobEffects::new(telegram.clone());

        let result = effects
            .send_initial_placeholders(
                -10042,
                77,
                Some(9),
                "<code>caption</code>".to_owned(),
                true,
                2,
            )
            .await;

        assert_eq!(result, Ok(vec![555, 556]));
        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::SendMediaGroup]
        );
    }

    #[tokio::test]
    async fn telegram_image_job_effects_replace_placeholder_uses_edit_media() {
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Boolean(true))]);
        let effects = TelegramImageJobEffects::new(telegram.clone());

        let result = effects
            .replace_placeholder_image(
                -10042,
                555,
                Some(9),
                PhotoSource::Url("https://img.test/1.png".to_owned()),
                "<code>caption</code>".to_owned(),
                false,
            )
            .await;

        assert_eq!(result, Ok(555));
        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::EditMessageMedia]
        );
    }

    #[tokio::test]
    async fn telegram_image_job_effects_delete_placeholder_image_deletes_message() {
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Boolean(true))]);
        let effects = TelegramImageJobEffects::new(telegram.clone());

        effects.delete_placeholder_image(-10042, 555).await;

        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::DeleteMessage]
        );
    }

    #[tokio::test]
    async fn telegram_image_job_effects_send_nsfw_blocked_message_sends_reply() {
        let telegram = TelegramSenderStub::new(vec![Ok(TelegramOutboundResponse::Message(
            Box::new(telegram_message(-10042, 556)),
        ))]);
        let effects = TelegramImageJobEffects::new(telegram.clone());

        effects
            .send_nsfw_blocked_message(-10042, 77, 30, Some(9))
            .await;

        assert_eq!(
            telegram.kinds(),
            vec![TelegramOutboundMethodKind::SendMessage]
        );
    }

    #[tokio::test]
    async fn telegram_image_job_effects_record_last_generation_writes_redis_record() {
        let telegram = TelegramSenderStub::new(Vec::new());
        let writer = Arc::new(LastGenerationWriterStub::default());
        let effects = TelegramImageJobEffects::new(telegram.clone())
            .with_last_generation_writer(writer.clone());

        effects
            .record_last_generation(
                -10042,
                30,
                vec![555, 556],
                "<code>caption</code>".to_owned(),
            )
            .await;
        // Empty frame lists are never persisted.
        effects
            .record_last_generation(-10042, 30, Vec::new(), "<code>caption</code>".to_owned())
            .await;

        let records = writer.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].chat_id, -10042);
        assert_eq!(records[0].user_id, 30);
        assert_eq!(records[0].message_ids, vec![555i64, 556]);
        assert_eq!(records[0].caption, "<code>caption</code>");
        assert!(records[0].created_at > 0);
        assert!(telegram.kinds().is_empty());
    }

    #[derive(Clone, Debug)]
    struct BooguTransportStub {
        state: Arc<Mutex<BooguTransportState>>,
    }

    #[derive(Debug)]
    struct BooguTransportState {
        requests: Vec<BooguHttpRequest>,
        responses: VecDeque<Result<BooguHttpResponse, String>>,
    }

    impl BooguTransportStub {
        fn new(responses: Vec<Result<BooguHttpResponse, String>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(BooguTransportState {
                    requests: Vec::new(),
                    responses: responses.into(),
                })),
            }
        }

        fn requests(&self) -> Vec<BooguHttpRequest> {
            self.state.lock().expect("boogu state").requests.clone()
        }
    }

    impl BooguHttpTransport for BooguTransportStub {
        fn send<'a>(&'a self, request: BooguHttpRequest) -> BooguHttpFuture<'a> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("boogu state");
                state.requests.push(request);
                state
                    .responses
                    .pop_front()
                    .unwrap_or_else(|| Ok(BooguHttpResponse::default()))
            })
        }
    }

    fn boogu_json_response(value: Value) -> BooguHttpResponse {
        BooguHttpResponse {
            status_code: 200,
            body: serde_json::to_vec(&value).expect("json response"),
        }
    }

    fn boogu_text_response(value: &str) -> BooguHttpResponse {
        BooguHttpResponse {
            status_code: 200,
            body: value.as_bytes().to_vec(),
        }
    }

    fn boogu_image_test_config() -> BooguGradioImageConfig {
        BooguGradioImageConfig {
            enabled: true,
            base_url: "https://demo-turbo.boogu.org".to_owned(),
            timeout: StdDuration::from_secs(5),
            steps: 3,
            resolution: "1024x1024 ( 1:1 )".to_owned(),
            width: 1024,
            height: 1024,
        }
    }

    fn boogu_edit_test_config() -> BooguGradioEditConfig {
        BooguGradioEditConfig {
            enabled: true,
            base_url: "https://demo-edit-turbo-1k5.boogu.org".to_owned(),
            timeout: StdDuration::from_secs(5),
            steps: 3,
            resolution_category: "1.5K".to_owned(),
            resolution: "1536x1536 ( 1:1 )".to_owned(),
            width: 1536,
            height: 1536,
        }
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

        fn requested(&self) -> Vec<String> {
            self.requested.lock().expect("requested files").clone()
        }
    }

    impl TelegramVisionDataUrlProvider for DataUrlStub {
        type Error = StubError;

        fn telegram_file_data_url<'a>(
            &'a self,
            latest_file_id: &'a str,
            _media_kind: &'a str,
            _mime_type: Option<&'a str>,
        ) -> crate::vision::TelegramVisionDataUrlFuture<'a, Self::Error> {
            self.requested
                .lock()
                .expect("requested files")
                .push(latest_file_id.to_owned());
            let result = self.result.clone();
            Box::pin(async move { result })
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

    #[tokio::test]
    async fn fallback_image_generator_uses_primary_success_without_fallback() {
        let primary = GeneratorStub::success("https://primary-image.test/1.png");
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

        assert_eq!(result.image_url, "https://primary-image.test/1.png");
        assert_eq!(primary.requests().len(), 1);
        assert!(fallback.requests().is_empty());
    }

    #[tokio::test]
    async fn fallback_image_generator_falls_back_after_primary_provider_failure() {
        let primary = GeneratorStub::error("primary image provider unavailable");
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

    #[test]
    fn draw_api_error_classification_keeps_generic_safety_as_provider_error() {
        for message in [
            "job failed: safety filter blocked prompt",
            "job failed: nsfw content refused",
            "job failed: forbidden by provider policy",
        ] {
            assert_eq!(
                classify_draw_api_error(message.to_owned()),
                ImageGenerationError::Provider(message.to_owned())
            );
        }

        for message in [
            "job failed: CSAM content detected",
            "job failed: child sexual abuse material detected",
            "job failed: underage nudity detected",
        ] {
            assert_eq!(
                classify_draw_api_error(message.to_owned()),
                ImageGenerationError::Forbidden
            );
        }
    }

    #[derive(Clone, Debug)]
    struct GeneratorStub {
        result: Result<ImageGenerationResult, ImageGenerationError>,
        requests: Arc<Mutex<Vec<ImageGenerationRequest>>>,
        expected_image_count: usize,
    }

    impl GeneratorStub {
        fn success(image_url: impl Into<String>) -> Self {
            Self {
                result: Ok(ImageGenerationResult {
                    image_url: image_url.into(),
                    ..ImageGenerationResult::default()
                }),
                requests: Arc::new(Mutex::new(Vec::new())),
                expected_image_count: 1,
            }
        }

        fn success_many(image_urls: Vec<String>) -> Self {
            Self {
                result: Ok(ImageGenerationResult {
                    image_urls,
                    ..ImageGenerationResult::default()
                }),
                requests: Arc::new(Mutex::new(Vec::new())),
                expected_image_count: 1,
            }
        }

        fn forbidden() -> Self {
            Self {
                result: Err(ImageGenerationError::Forbidden),
                requests: Arc::new(Mutex::new(Vec::new())),
                expected_image_count: 1,
            }
        }

        fn error(message: impl Into<String>) -> Self {
            Self {
                result: Err(ImageGenerationError::Provider(message.into())),
                requests: Arc::new(Mutex::new(Vec::new())),
                expected_image_count: 1,
            }
        }

        fn with_expected_image_count(mut self, expected_image_count: usize) -> Self {
            self.expected_image_count = expected_image_count;
            self
        }

        fn requests(&self) -> Vec<ImageGenerationRequest> {
            self.requests.lock().expect("requests").clone()
        }
    }

    impl ImageGenerator for GeneratorStub {
        fn expected_image_count(&self) -> usize {
            self.expected_image_count
        }

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
    struct WaitingGeneratorStub {
        image_url: String,
        notify: Arc<tokio::sync::Notify>,
        requests: Arc<Mutex<Vec<ImageGenerationRequest>>>,
    }

    impl WaitingGeneratorStub {
        fn success(image_url: impl Into<String>, notify: Arc<tokio::sync::Notify>) -> Self {
            Self {
                image_url: image_url.into(),
                notify,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<ImageGenerationRequest> {
            self.requests.lock().expect("requests").clone()
        }
    }

    impl ImageGenerator for WaitingGeneratorStub {
        fn generate_image<'a>(
            &'a self,
            request: ImageGenerationRequest,
        ) -> ImageGenerationFuture<'a> {
            self.requests.lock().expect("requests").push(request);
            let image_url = self.image_url.clone();
            let notify = Arc::clone(&self.notify);
            Box::pin(async move {
                notify.notified().await;
                Ok(ImageGenerationResult {
                    image_url,
                    ..ImageGenerationResult::default()
                })
            })
        }
    }

    #[derive(Clone, Debug)]
    struct WaitingForbiddenGeneratorStub {
        notify: Arc<tokio::sync::Notify>,
    }

    impl WaitingForbiddenGeneratorStub {
        fn new(notify: Arc<tokio::sync::Notify>) -> Self {
            Self { notify }
        }
    }

    impl ImageGenerator for WaitingForbiddenGeneratorStub {
        fn generate_image<'a>(
            &'a self,
            _request: ImageGenerationRequest,
        ) -> ImageGenerationFuture<'a> {
            let notify = Arc::clone(&self.notify);
            Box::pin(async move {
                notify.notified().await;
                Err(ImageGenerationError::Forbidden)
            })
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
                result: Ok(ImageEditResult {
                    image_urls,
                    ..ImageEditResult::default()
                }),
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

    #[derive(Clone, Debug)]
    struct WaitingEditorStub {
        image_urls: Vec<String>,
        notify: Arc<tokio::sync::Notify>,
        requests: Arc<Mutex<Vec<ImageEditRequest>>>,
    }

    impl WaitingEditorStub {
        fn success(image_urls: Vec<String>, notify: Arc<tokio::sync::Notify>) -> Self {
            Self {
                image_urls,
                notify,
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn requests(&self) -> Vec<ImageEditRequest> {
            self.requests.lock().expect("image edit requests").clone()
        }
    }

    impl ImageEditor for WaitingEditorStub {
        fn edit_image<'a>(&'a self, request: ImageEditRequest) -> ImageEditFuture<'a> {
            self.requests
                .lock()
                .expect("image edit requests")
                .push(request);
            let image_urls = self.image_urls.clone();
            let notify = Arc::clone(&self.notify);
            Box::pin(async move {
                notify.notified().await;
                Ok(ImageEditResult {
                    image_urls,
                    ..ImageEditResult::default()
                })
            })
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

    #[derive(Debug, Default)]
    struct EffectsStub {
        calls: Mutex<Vec<String>>,
        placeholder_ids: Vec<i32>,
        replace_error: Option<String>,
        replace_notify: Option<Arc<tokio::sync::Notify>>,
    }

    impl EffectsStub {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                placeholder_ids: vec![888],
                replace_error: None,
                replace_notify: None,
            }
        }

        fn with_placeholder_ids(mut self, placeholder_ids: Vec<i32>) -> Self {
            self.placeholder_ids = placeholder_ids;
            self
        }

        fn with_replace_error(error: impl Into<String>) -> Self {
            Self {
                replace_error: Some(error.into()),
                ..Self::new()
            }
        }

        fn with_replace_notify(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
            self.replace_notify = Some(notify);
            self
        }

        fn calls(&self) -> Vec<String> {
            self.call_log().clone()
        }

        fn call_log(&self) -> MutexGuard<'_, Vec<String>> {
            self.calls.lock().expect("calls")
        }
    }

    impl ImageJobEffects for EffectsStub {
        fn signal_draw_progress<'a>(
            &'a self,
            chat_id: i64,
            trigger_message_id: i32,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log()
                .push(format!("react-progress:{chat_id}:{trigger_message_id}"));
            Box::pin(async {})
        }

        fn clear_draw_signal<'a>(
            &'a self,
            chat_id: i64,
            trigger_message_id: i32,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log()
                .push(format!("react-clear:{chat_id}:{trigger_message_id}"));
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
                "send_nsfw:{chat_id}:{message_id}:{user_id}:{}",
                thread_id.unwrap_or_default()
            ));
            Box::pin(async {})
        }

        fn send_initial_placeholders<'a>(
            &'a self,
            chat_id: i64,
            message_id: i32,
            thread_id: Option<i32>,
            caption_text: String,
            _is_nsfw: bool,
            count: usize,
        ) -> ImageJobEffectFuture<'a, Result<Vec<i32>, String>> {
            self.call_log().push(format!(
                "send_placeholders:{chat_id}:{message_id}:{}:{count}:{caption_text}",
                thread_id.unwrap_or_default()
            ));
            let ids = self
                .placeholder_ids
                .iter()
                .copied()
                .take(count)
                .collect::<Vec<_>>();
            Box::pin(async move { Ok(ids) })
        }

        fn replace_placeholder_image<'a>(
            &'a self,
            chat_id: i64,
            placeholder_message_id: i32,
            thread_id: Option<i32>,
            photo: PhotoSource,
            caption_text: String,
            _is_nsfw: bool,
        ) -> ImageJobEffectFuture<'a, Result<i32, String>> {
            self.call_log().push(format!(
                "replace_placeholder:{chat_id}:{placeholder_message_id}:{}:{photo:?}:{caption_text}",
                thread_id.unwrap_or_default()
            ));
            if let Some(notify) = &self.replace_notify {
                notify.notify_one();
            }
            let result = match &self.replace_error {
                Some(error) => Err(error.clone()),
                None => Ok(placeholder_message_id),
            };
            Box::pin(async move { result })
        }

        fn delete_placeholder_image<'a>(
            &'a self,
            chat_id: i64,
            placeholder_message_id: i32,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log().push(format!(
                "delete_placeholder:{chat_id}:{placeholder_message_id}"
            ));
            Box::pin(async {})
        }

        fn record_last_generation<'a>(
            &'a self,
            chat_id: i64,
            user_id: i64,
            message_ids: Vec<i32>,
            caption: String,
        ) -> ImageJobEffectFuture<'a, ()> {
            self.call_log().push(format!(
                "record_last_gen:{chat_id}:{user_id}:{message_ids:?}:{caption}"
            ));
            Box::pin(async {})
        }
    }
}
