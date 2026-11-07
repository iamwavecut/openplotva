//! App-level media provider wiring and prompt-optimizer orchestration.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use openplotva_config::AppConfig;
use openplotva_dialog::{PROVIDER_AIFARM, PROVIDER_NVIDIA, PROVIDER_VMLX};
use openplotva_llm::aifarm::{
    AifarmClientConfig, AifarmDialogConfig, AifarmHttpTransport, AifarmStructuredJsonConfig,
    AifarmStructuredJsonGenerator, StatusUpdate,
};
use openplotva_llm::gemini::{GeminiMediaPromptOptimizer, GeminiMediaPromptOptimizerConfig};
use openplotva_llm::retry::{FailureReason, retryable_reason_from_message};
use openplotva_media::{ImageEditOptimize, ImageOptimize, OptimizePromptOptions};
use thiserror::Error;

use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
};

/// Boxed image-prompt optimizer future.
pub type ImagePromptOptimizeFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ImageOptimize, MediaPromptOptimizerError>> + Send + 'a>>;

/// Boxed image-edit-prompt optimizer future.
pub type ImageEditPromptOptimizeFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ImageEditOptimize, MediaPromptOptimizerError>> + Send + 'a>>;

pub trait MediaPromptOptimizer: Send + Sync {
    /// Execute image prompt optimization.
    fn optimize_image_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImagePromptOptimizeFuture<'a>;

    /// Execute image edit-prompt optimization; `images` are data URLs of the pictures
    /// being edited, in the order the request numbers them.
    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
        images: &'a [String],
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct MediaPromptOptimization<T> {
    /// Optimized payload or original-prompt fallback.
    pub value: T,
    pub provider_error: Option<String>,
}

/// App-level prompt optimizer service.
#[derive(Clone, Debug)]
pub struct MediaPromptOptimizerService<Optimizer> {
    optimizer: Option<Optimizer>,
}

pub type AppMediaPromptOptimizer = Arc<dyn MediaPromptOptimizer>;

/// Error returned by app-level media optimizer service helpers.
#[derive(Debug, Error)]
pub enum MediaPromptOptimizerError {
    /// Concrete provider returned an error.
    #[error("{0}")]
    Provider(String),
}

impl<Optimizer> MediaPromptOptimizerService<Optimizer> {
    /// Build a service around an optional concrete optimizer.
    #[must_use]
    pub const fn new(optimizer: Option<Optimizer>) -> Self {
        Self { optimizer }
    }
}

#[derive(Clone)]
pub struct RoutedMediaPromptOptimizer {
    walker: RoutedAttemptWalker,
    config: AppConfig,
    prompt_store: Arc<openplotva_prompts::PromptStore>,
}

impl RoutedMediaPromptOptimizer {
    #[must_use]
    pub fn new(
        walker: RoutedAttemptWalker,
        config: &AppConfig,
        prompt_store: Arc<openplotva_prompts::PromptStore>,
    ) -> Self {
        Self {
            walker,
            config: config.clone(),
            prompt_store,
        }
    }
}

impl MediaPromptOptimizer for RoutedMediaPromptOptimizer {
    fn optimize_image_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImagePromptOptimizeFuture<'a> {
        Box::pin(async move {
            let config = self.config.clone();
            let prompt_store = Arc::clone(&self.prompt_store);
            let text = text.to_owned();
            let result = self
                .walker
                .run(
                    routed_media_context(),
                    move |attempt| {
                        let config = config.clone();
                        let prompt_store = prompt_store.clone();
                        let text = text.clone();
                        async move {
                            let optimizer = media_optimizer_for_attempt_with_prompt_store(
                                &config,
                                &attempt,
                                &prompt_store,
                            )?;
                            optimizer.optimize_image_prompt(&text, options).await
                        }
                    },
                    media_optimizer_retryable_reason,
                )
                .await;
            match result {
                Ok(value) => Ok(value),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(MediaPromptOptimizerError::Provider(error.to_string()))
                }
            }
        })
    }

    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
        images: &'a [String],
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        Box::pin(async move {
            let config = self.config.clone();
            let prompt_store = Arc::clone(&self.prompt_store);
            let text = text.to_owned();
            let images = images.to_vec();
            let result = self
                .walker
                .run(
                    routed_media_context(),
                    move |attempt| {
                        let config = config.clone();
                        let prompt_store = prompt_store.clone();
                        let text = text.clone();
                        let images = images.clone();
                        async move {
                            let optimizer = media_optimizer_for_attempt_with_prompt_store(
                                &config,
                                &attempt,
                                &prompt_store,
                            )?;
                            optimizer
                                .optimize_image_edit_prompt(&text, &images, options)
                                .await
                        }
                    },
                    media_optimizer_retryable_reason,
                )
                .await;
            match result {
                Ok(value) => Ok(value),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(MediaPromptOptimizerError::Provider(error.to_string()))
                }
            }
        })
    }
}

fn routed_media_context() -> RoutedRequestContext {
    RoutedRequestContext {
        workflow_key: "media_prompt_optimizer".to_owned(),
        suppress_all_attempts_exhausted_admin_report: true,
        suppressed_all_attempts_exhausted_reason: Some("prompt_optimizer_fallback".to_owned()),
        ..RoutedRequestContext::default()
    }
}

fn media_optimizer_for_attempt_with_prompt_store(
    config: &AppConfig,
    attempt: &RoutedAttempt,
    prompt_store: &Arc<openplotva_prompts::PromptStore>,
) -> Result<AppMediaPromptOptimizer, MediaPromptOptimizerError> {
    if attempt.protocol == Some(openplotva_llm::provider_schema::Protocol::Genkit) {
        let (api_key, base_url) =
            crate::provider_transport::gemini_endpoint_for_attempt(config, attempt)
                .map_err(|error| MediaPromptOptimizerError::Provider(error.to_string()))?;
        let optimizer = GeminiMediaPromptOptimizer::new(GeminiMediaPromptOptimizerConfig {
            api_key,
            base_url,
            model: attempt.model_name.clone(),
            request_timeout: Duration::from_secs(
                config.llm.dialog.request_timeout_seconds.max(1) as u64
            ),
        })
        .with_prompt_store(Arc::clone(prompt_store));
        return Ok(Arc::new(optimizer));
    }

    let cfg = aifarm_structured_json_config_for_attempt(config, attempt)?;
    let optimizer =
        AifarmStructuredJsonGenerator::new(cfg).with_prompt_store(Arc::clone(prompt_store));
    Ok(Arc::new(optimizer) as AppMediaPromptOptimizer)
}

pub(crate) fn aifarm_structured_json_config_for_attempt(
    config: &AppConfig,
    attempt: &RoutedAttempt,
) -> Result<AifarmStructuredJsonConfig, MediaPromptOptimizerError> {
    let mut cfg = aifarm_structured_json_config_from_app_config(config);
    cfg.client = crate::provider_transport::openai_client_for_attempt(config, attempt, cfg.client)
        .map_err(|error| MediaPromptOptimizerError::Provider(error.to_string()))?;
    cfg.model.clone_from(&cfg.client.default_model);
    if let Some(max_tokens) = attempt.overrides.max_tokens
        && max_tokens > 0
    {
        cfg.max_tokens = max_tokens;
    }
    Ok(cfg.with_defaults())
}

fn media_optimizer_retryable_reason(error: &MediaPromptOptimizerError) -> Option<FailureReason> {
    match error {
        MediaPromptOptimizerError::Provider(message) => retryable_reason_from_message(message),
    }
}

impl<T> MediaPromptOptimizer for Arc<T>
where
    T: MediaPromptOptimizer + ?Sized,
{
    fn optimize_image_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImagePromptOptimizeFuture<'a> {
        self.as_ref().optimize_image_prompt(text, options)
    }

    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
        images: &'a [String],
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        self.as_ref()
            .optimize_image_edit_prompt(text, images, options)
    }
}

impl<Optimizer> MediaPromptOptimizerService<Optimizer>
where
    Optimizer: MediaPromptOptimizer,
{
    /// Optimize `original_prompt`; on failure fall back to `fallback_prompt`,
    /// which must already be safe to send to the image model as-is.
    pub async fn enhance_image_prompt(
        &self,
        original_prompt: &str,
        fallback_prompt: &str,
        aspect_ratio: &str,
        options: OptimizePromptOptions,
    ) -> MediaPromptOptimization<ImageOptimize> {
        let fallback = ImageOptimize {
            input: original_prompt.to_owned(),
            outputs: vec![fallback_prompt.to_owned()],
            aspect_ratio: aspect_ratio.to_owned(),
            nsfw_result: openplotva_media::NsfwResult::Adult,
            ..ImageOptimize::default()
        };
        let Some(optimizer) = &self.optimizer else {
            return MediaPromptOptimization {
                value: fallback,
                provider_error: None,
            };
        };
        match optimizer
            .optimize_image_prompt(original_prompt, options)
            .await
        {
            Ok(mut value) => {
                if value.aspect_ratio.is_empty() {
                    value.aspect_ratio = aspect_ratio.to_owned();
                }
                MediaPromptOptimization {
                    value,
                    provider_error: None,
                }
            }
            Err(err) => MediaPromptOptimization {
                value: fallback,
                provider_error: Some(err.to_string()),
            },
        }
    }

    /// Optimize an edit request shown with `images`; on failure fall back to the
    /// original request.
    pub async fn enhance_image_edit_prompt(
        &self,
        original_prompt: &str,
        images: &[String],
        options: OptimizePromptOptions,
    ) -> MediaPromptOptimization<ImageEditOptimize> {
        let fallback = ImageEditOptimize {
            input: original_prompt.to_owned(),
            outputs: vec![original_prompt.to_owned()],
            nsfw_result: openplotva_media::NsfwResult::Adult,
            fibo_caption: None,
        };
        let Some(optimizer) = &self.optimizer else {
            return MediaPromptOptimization {
                value: fallback,
                provider_error: None,
            };
        };
        match optimizer
            .optimize_image_edit_prompt(original_prompt, images, options)
            .await
        {
            Ok(value) => MediaPromptOptimization {
                value,
                provider_error: None,
            },
            Err(err) => MediaPromptOptimization {
                value: fallback,
                provider_error: Some(err.to_string()),
            },
        }
    }
}

impl<T> MediaPromptOptimizer for AifarmStructuredJsonGenerator<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn optimize_image_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImagePromptOptimizeFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_status: StatusUpdate| {};
            AifarmStructuredJsonGenerator::optimize_image_prompt(
                self,
                text,
                options,
                &mut ignore_status,
            )
            .await
            .map_err(|err| MediaPromptOptimizerError::Provider(err.to_string()))
        })
    }

    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
        images: &'a [String],
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_status: StatusUpdate| {};
            AifarmStructuredJsonGenerator::optimize_image_edit_prompt(
                self,
                text,
                images,
                options,
                &mut ignore_status,
            )
            .await
            .map_err(|err| MediaPromptOptimizerError::Provider(err.to_string()))
        })
    }
}

impl<T> MediaPromptOptimizer for GeminiMediaPromptOptimizer<T>
where
    T: AifarmHttpTransport + Send + Sync,
{
    fn optimize_image_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImagePromptOptimizeFuture<'a> {
        Box::pin(async move {
            GeminiMediaPromptOptimizer::optimize_image_prompt(self, text, options)
                .await
                .map_err(|err| MediaPromptOptimizerError::Provider(err.to_string()))
        })
    }

    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
        images: &'a [String],
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        Box::pin(async move {
            GeminiMediaPromptOptimizer::optimize_image_edit_prompt(self, text, images, options)
                .await
                .map_err(|err| MediaPromptOptimizerError::Provider(err.to_string()))
        })
    }
}

#[must_use]
pub fn aifarm_dialog_config_from_app_config(config: &AppConfig) -> AifarmDialogConfig {
    let dialog = &config.llm.dialog;
    AifarmDialogConfig {
        provider_name: PROVIDER_AIFARM.to_owned(),
        client: dialog_client_config_from_app_config(
            config,
            &dialog.url,
            &dialog.api_key,
            &dialog.model,
        ),
        model: dialog.model.clone(),
        max_tokens: dialog.aifarm_max_tokens,
        temperature: Some(dialog.aifarm_temperature),
        repeat_penalty: Some(dialog.aifarm_repeat_penalty),
        frequency_penalty: Some(dialog.aifarm_frequency_penalty),
        presence_penalty: Some(dialog.aifarm_presence_penalty),
        dry_multiplier: Some(dialog.aifarm_dry_multiplier),
        dry_base: Some(dialog.aifarm_dry_base),
        dry_allowed_length: dialog.aifarm_dry_allowed_length,
        enable_thinking: Some(dialog.aifarm_enable_thinking),
        include_reasoning: Some(false),
        ..AifarmDialogConfig::default()
    }
}

#[must_use]
pub fn vmlx_dialog_config_from_app_config(config: &AppConfig) -> AifarmDialogConfig {
    let dialog = &config.llm.dialog;
    AifarmDialogConfig {
        provider_name: PROVIDER_VMLX.to_owned(),
        client: dialog_client_config_from_app_config(
            config,
            &dialog.vmlx_url,
            &dialog.vmlx_api_key,
            &dialog.vmlx_model,
        ),
        model: dialog.vmlx_model.clone(),
        ..AifarmDialogConfig::default()
    }
}

#[must_use]
pub fn nvidia_dialog_config_from_app_config(config: &AppConfig) -> AifarmDialogConfig {
    let dialog = &config.llm.dialog;
    AifarmDialogConfig {
        provider_name: PROVIDER_NVIDIA.to_owned(),
        client: dialog_client_config_from_app_config(
            config,
            &dialog.nvidia_url,
            &dialog.nvidia_api_key,
            &dialog.nvidia_model,
        ),
        model: dialog.nvidia_model.clone(),
        max_tokens: dialog.nvidia_max_tokens,
        temperature: Some(dialog.nvidia_temperature),
        top_p: Some(dialog.nvidia_top_p),
        enable_thinking: Some(dialog.nvidia_enable_thinking),
        include_reasoning: Some(dialog.nvidia_include_reasoning),
        ..AifarmDialogConfig::default()
    }
}

#[must_use]
pub fn aifarm_structured_json_config_from_app_config(
    config: &AppConfig,
) -> AifarmStructuredJsonConfig {
    let model = config.llm.dialog.model.clone();
    AifarmStructuredJsonConfig {
        client: discovery_client_config_from_app_config(config, &model),
        model,
        max_tokens: 4096,
    }
    .with_defaults()
}

fn dialog_client_config_from_app_config(
    config: &AppConfig,
    direct_url: &str,
    api_key: &str,
    model: &str,
) -> AifarmClientConfig {
    AifarmClientConfig {
        direct_url: direct_url.to_owned(),
        api_key: api_key.to_owned(),
        ..discovery_client_config_from_app_config(config, model)
    }
}

/// Build a single-completion client config for a named agentic provider, reusing
/// the shared Discovery base settings and overriding only the fields the provider
/// specifies. Keeps the Discovery payload shape identical to the dialog client.
#[must_use]
pub fn agent_client_config_from_named_provider(
    config: &AppConfig,
    spec: &openplotva_config::NamedProviderConfig,
) -> AifarmClientConfig {
    let mut client = discovery_client_config_from_app_config(config, &spec.model);
    if !spec.discovery_service_name.is_empty() {
        client.service_name = spec.discovery_service_name.clone();
    }
    if !spec.discovery_endpoint_name.is_empty() {
        client.endpoint_name = spec.discovery_endpoint_name.clone();
    }
    if !spec.base_url.is_empty() {
        client.base_url = spec.base_url.clone();
    }
    if !spec.url.is_empty() {
        client.direct_url = spec.url.clone();
    }
    if !spec.api_key.is_empty() {
        client.api_key = spec.api_key.clone();
    }
    if spec.task_timeout_seconds > 0 {
        client.task_timeout = positive_seconds(spec.task_timeout_seconds);
    }
    client.runtime_hint = discovery_runtime_hint(&client.service_name).to_owned();
    if client.runtime_hint == "ninfer" {
        client.supports_message_name = false;
    }
    client
}

fn discovery_runtime_hint(service_name: &str) -> &'static str {
    if service_name
        .trim()
        .eq_ignore_ascii_case(openplotva_config::DEFAULT_NINFER_DISCOVERY_SERVICE_NAME)
    {
        "ninfer"
    } else if service_name
        .trim()
        .eq_ignore_ascii_case(openplotva_config::DEFAULT_MAPLE_DISCOVERY_SERVICE_NAME)
    {
        "mlx"
    } else {
        ""
    }
}

fn discovery_client_config_from_app_config(config: &AppConfig, model: &str) -> AifarmClientConfig {
    let dialog = &config.llm.dialog;
    AifarmClientConfig {
        base_url: config.llm.discovery.base_url.clone(),
        service_name: dialog.discovery_service_name.clone(),
        endpoint_name: dialog.discovery_endpoint_name.clone(),
        request_timeout: positive_seconds(dialog.request_timeout_seconds),
        poll_interval: positive_seconds(dialog.poll_interval_seconds),
        task_timeout: positive_seconds(dialog.task_timeout_seconds),
        capacity_wait: positive_seconds(dialog.aifarm_capacity_wait_seconds),
        capacity_poll_interval: positive_seconds(dialog.aifarm_capacity_poll_seconds),
        default_model: model.to_owned(),
        ..AifarmClientConfig::default()
    }
}

fn positive_seconds(seconds: i32) -> Duration {
    if seconds <= 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(seconds as u64)
    }
}

#[cfg(test)]
mod tests {
    const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

    use std::sync::{Arc, Mutex};

    use super::*;

    fn test_walker() -> RoutedAttemptWalker {
        RoutedAttemptWalker::new(
            openplotva_llm::router::RouterHandle::new(
                openplotva_llm::router::RoutingTable::default(),
            ),
            Arc::new(openplotva_llm::router::BreakerSet::new()),
            Arc::new(openplotva_llm::router::TriggerState::new()),
            Arc::new(openplotva_llm::router::PoolRegistry::new()),
        )
    }

    #[test]
    fn routed_optimizer_keeps_application_prompt_store() {
        let prompts = Arc::new(openplotva_prompts::PromptStore::load().expect("prompt store"));
        let config = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");

        let optimizer =
            RoutedMediaPromptOptimizer::new(test_walker(), &config, Arc::clone(&prompts));

        assert!(Arc::ptr_eq(&optimizer.prompt_store, &prompts));
    }

    #[derive(Default)]
    struct FakeOptimizer {
        image_result: Mutex<Option<Result<ImageOptimize, MediaPromptOptimizerError>>>,
        edit_result: Mutex<Option<Result<ImageEditOptimize, MediaPromptOptimizerError>>>,
    }

    impl MediaPromptOptimizer for FakeOptimizer {
        fn optimize_image_prompt<'a>(
            &'a self,
            _text: &'a str,
            _options: OptimizePromptOptions,
        ) -> ImagePromptOptimizeFuture<'a> {
            Box::pin(async move {
                self.image_result
                    .lock()
                    .expect("image result lock")
                    .take()
                    .unwrap_or_else(|| Err(MediaPromptOptimizerError::Provider("boom".to_owned())))
            })
        }

        fn optimize_image_edit_prompt<'a>(
            &'a self,
            _text: &'a str,
            _images: &'a [String],
            _options: OptimizePromptOptions,
        ) -> ImageEditPromptOptimizeFuture<'a> {
            Box::pin(async move {
                self.edit_result
                    .lock()
                    .expect("edit result lock")
                    .take()
                    .unwrap_or_else(|| Err(MediaPromptOptimizerError::Provider("boom".to_owned())))
            })
        }
    }

    #[test]
    fn aifarm_structured_config_maps_go_dialog_env_and_clamps_task_timeout() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            discovery_base_url: Some("http://discovery.test".to_owned()),
            dialog_model: Some("model-x".to_owned()),
            dialog_discovery_service_name: Some("svc".to_owned()),
            dialog_discovery_endpoint_name: Some("endpoint".to_owned()),
            dialog_url: Some("https://direct.test/v1/chat/completions".to_owned()),
            dialog_api_key: Some("key".to_owned()),
            dialog_request_timeout_seconds: Some("44".to_owned()),
            dialog_task_timeout_seconds: Some("720".to_owned()),
            dialog_aifarm_capacity_wait_seconds: Some("11".to_owned()),
            dialog_aifarm_capacity_poll_seconds: Some("2".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = aifarm_structured_json_config_from_app_config(&config);

        assert_eq!(cfg.client.base_url, "http://discovery.test");
        assert_eq!(cfg.client.service_name, "svc");
        assert_eq!(cfg.client.endpoint_name, "endpoint");
        assert_eq!(cfg.client.direct_url, "");
        assert_eq!(cfg.client.api_key, "");
        assert_eq!(cfg.model, "model-x");
        assert_eq!(cfg.max_tokens, 4096);
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(125));
        assert_eq!(cfg.client.task_timeout, Duration::from_secs(120));
        assert_eq!(cfg.client.capacity_wait, Duration::from_secs(11));
        assert_eq!(cfg.client.capacity_poll_interval, Duration::from_secs(2));
        assert_eq!(
            cfg.client.priority,
            openplotva_llm::aifarm::DISCOVERY_PRIORITY_INTERACTIVE
        );
        assert_eq!(
            cfg.client.workload,
            openplotva_llm::aifarm::AIFARM_WORKLOAD_STRUCTURED
        );
    }

    #[test]
    fn aifarm_dialog_config_maps_go_provider_sampling_env() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("nvidia".to_owned()),
            dialog_url: Some("https://direct.test/v1/chat/completions".to_owned()),
            dialog_api_key: Some("key".to_owned()),
            dialog_aifarm_enable_thinking: Some("true".to_owned()),
            dialog_aifarm_max_tokens: Some("2048".to_owned()),
            dialog_aifarm_temperature: Some("0.4".to_owned()),
            dialog_aifarm_repeat_penalty: Some("1.2".to_owned()),
            dialog_aifarm_frequency_penalty: Some("0.3".to_owned()),
            dialog_aifarm_presence_penalty: Some("0.1".to_owned()),
            dialog_aifarm_dry_multiplier: Some("0.5".to_owned()),
            dialog_aifarm_dry_base: Some("1.5".to_owned()),
            dialog_aifarm_dry_allowed_length: Some("42".to_owned()),
            dialog_aifarm_pool_models: Some(" secondary-a, secondary-b ".to_owned()),
            dialog_aifarm_pool_base_urls: Some(
                " http://secondary-a.test/v1, http://secondary-b.test ".to_owned(),
            ),
            dialog_aifarm_pool_api_key: Some("pool-key".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = aifarm_dialog_config_from_app_config(&config).with_defaults();

        assert_eq!(cfg.provider_name, PROVIDER_AIFARM);
        assert_eq!(
            cfg.client.direct_url,
            "https://direct.test/v1/chat/completions"
        );
        assert_eq!(cfg.client.api_key, "key");
        assert_eq!(cfg.max_tokens, 2048);
        assert_eq!(cfg.temperature, Some(0.4));
        assert_eq!(cfg.repeat_penalty, Some(1.2));
        assert_eq!(cfg.frequency_penalty, Some(0.3));
        assert_eq!(cfg.presence_penalty, Some(0.1));
        assert_eq!(cfg.dry_multiplier, Some(0.5));
        assert_eq!(cfg.dry_base, Some(1.5));
        assert_eq!(cfg.dry_allowed_length, 42);
        assert_eq!(cfg.enable_thinking, Some(false));
        assert_eq!(cfg.include_reasoning, Some(false));
    }

    #[test]
    fn vmlx_dialog_config_maps_go_direct_provider_env() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_vmlx_url: Some("http://vmlx.test/v1/chat/completions".to_owned()),
            dialog_vmlx_api_key: Some("vmlx-key".to_owned()),
            dialog_vmlx_model: Some("vmlx-model".to_owned()),
            dialog_request_timeout_seconds: Some("44".to_owned()),
            dialog_task_timeout_seconds: Some("55".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = vmlx_dialog_config_from_app_config(&config).with_defaults();

        assert_eq!(cfg.provider_name, PROVIDER_VMLX);
        assert_eq!(
            cfg.client.direct_url,
            "http://vmlx.test/v1/chat/completions"
        );
        assert_eq!(cfg.client.api_key, "vmlx-key");
        assert_eq!(cfg.model, "vmlx-model");
        assert_eq!(cfg.client.default_model, "vmlx-model");
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(44));
        assert_eq!(cfg.client.task_timeout, Duration::from_secs(55));
        assert_eq!(cfg.max_tokens, 0);
        assert_eq!(cfg.temperature, None);
        assert_eq!(cfg.enable_thinking, None);
        assert_eq!(cfg.include_reasoning, None);
    }

    #[test]
    fn nvidia_dialog_config_maps_go_direct_provider_env() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_nvidia_url: Some("https://nvidia.test/v1/chat/completions".to_owned()),
            dialog_nvidia_api_key: Some("nvidia-key".to_owned()),
            dialog_nvidia_model: Some("nvidia-model".to_owned()),
            dialog_nvidia_max_tokens: Some("3333".to_owned()),
            dialog_nvidia_temperature: Some("0.8".to_owned()),
            dialog_nvidia_top_p: Some("0.9".to_owned()),
            dialog_nvidia_enable_thinking: Some("false".to_owned()),
            dialog_nvidia_include_reasoning: Some("true".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = nvidia_dialog_config_from_app_config(&config).with_defaults();

        assert_eq!(cfg.provider_name, PROVIDER_NVIDIA);
        assert_eq!(
            cfg.client.direct_url,
            "https://nvidia.test/v1/chat/completions"
        );
        assert_eq!(cfg.client.api_key, "nvidia-key");
        assert_eq!(cfg.model, "nvidia-model");
        assert_eq!(cfg.client.default_model, "nvidia-model");
        assert_eq!(cfg.max_tokens, 3333);
        assert_eq!(cfg.temperature, Some(0.8));
        assert_eq!(cfg.top_p, Some(0.9));
        assert_eq!(cfg.enable_thinking, Some(false));
        assert_eq!(cfg.include_reasoning, Some(true));
    }

    fn openrouter_attempt(overrides: serde_json::Value) -> RoutedAttempt {
        RoutedAttempt {
            protocol: Some(openplotva_llm::provider_schema::Protocol::OpenAiCompat),
            provider_id: 3,
            model_id: 5,
            provider_name: "openrouter".to_owned(),
            model_name: "vendor/model-flash".to_owned(),
            provider_runtime_hint: None,
            provider_endpoint: Some(OPENROUTER_CHAT_COMPLETIONS_URL.to_owned()),
            discovery_service_name: None,
            discovery_endpoint_name: None,
            provider_api_key_ref: None,
            provider_api_key_encrypted: None,
            model_base_url: None,
            embedding_dim: None,
            provider_config: serde_json::json!({}),
            model_config: serde_json::json!({}),
            overrides: openplotva_llm::router::InferenceOverrides {
                extra: overrides,
                ..Default::default()
            },
            variant: None,
        }
    }

    #[test]
    fn gateway_attempt_carries_routed_model_gateway_fields() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            openrouter_key: Some("openrouter-key".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let attempt = openrouter_attempt(serde_json::json!({
            "provider": {"require_parameters": true, "quantizations": ["fp8"]},
            "reasoning": {"enabled": false},
            "temperature": 0.4
        }));

        let cfg =
            aifarm_structured_json_config_for_attempt(&config, &attempt).expect("gateway config");

        assert_eq!(cfg.model, "vendor/model-flash");
        assert_eq!(cfg.client.direct_url, OPENROUTER_CHAT_COMPLETIONS_URL);
        assert_eq!(
            cfg.client.gateway_fields.provider,
            Some(serde_json::json!({"require_parameters": true, "quantizations": ["fp8"]}))
        );
        assert_eq!(
            cfg.client.gateway_fields.reasoning,
            Some(serde_json::json!({"enabled": false}))
        );

        let plain = aifarm_structured_json_config_for_attempt(
            &config,
            &openrouter_attempt(serde_json::json!({})),
        )
        .expect("gateway config");
        assert_eq!(
            plain.client.gateway_fields,
            openplotva_llm::aifarm::GatewayRequestFields::default()
        );
    }

    fn options(variant_count: usize) -> OptimizePromptOptions {
        OptimizePromptOptions {
            variant_count,
            ..OptimizePromptOptions::default()
        }
    }

    #[tokio::test]
    async fn media_optimizer_service_preserves_go_fallback_and_aspect_behavior() {
        let optimizer = FakeOptimizer::default();
        *optimizer.image_result.lock().expect("image result lock") = Some(Ok(ImageOptimize {
            input: "cat".to_owned(),
            outputs: vec!["cinematic cat".to_owned()],
            ..ImageOptimize::default()
        }));
        let service = MediaPromptOptimizerService::new(Some(optimizer));

        let got = service
            .enhance_image_prompt("cat", "cat", "16:9", options(2))
            .await;

        assert_eq!(got.provider_error, None);
        assert_eq!(got.value.outputs, vec!["cinematic cat"]);
        assert_eq!(got.value.aspect_ratio, "16:9");

        let got = service
            .enhance_image_prompt("cat", "cat", "1:1", options(1))
            .await;

        assert_eq!(got.provider_error.as_deref(), Some("boom"));
        assert_eq!(
            got.value,
            ImageOptimize {
                input: "cat".to_owned(),
                outputs: vec!["cat".to_owned()],
                aspect_ratio: "1:1".to_owned(),
                nsfw_result: openplotva_media::NsfwResult::Adult,
                ..ImageOptimize::default()
            }
        );
    }

    #[tokio::test]
    async fn media_optimizer_service_preserves_go_edit_fallback() {
        let optimizer = FakeOptimizer::default();
        let service = MediaPromptOptimizerService::new(Some(optimizer));

        let got = service
            .enhance_image_edit_prompt("make it day", &[], options(3))
            .await;

        assert_eq!(got.provider_error.as_deref(), Some("boom"));
        assert_eq!(
            got.value,
            ImageEditOptimize {
                input: "make it day".to_owned(),
                outputs: vec!["make it day".to_owned()],
                nsfw_result: openplotva_media::NsfwResult::Adult,
                fibo_caption: None,
            }
        );
    }
}
