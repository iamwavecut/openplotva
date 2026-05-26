//! App-level media provider wiring and prompt-optimizer orchestration.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use openplotva_config::AppConfig;
use openplotva_dialog::{PROVIDER_AIFARM, PROVIDER_NVIDIA, PROVIDER_VMLX};
use openplotva_llm::aifarm::{
    AifarmClientConfig, AifarmDialogConfig, AifarmHttpTransport, AifarmPoolConfig,
    AifarmStructuredJsonConfig, AifarmStructuredJsonGenerator, ReqwestAifarmTransport,
    StatusUpdate,
};
use openplotva_llm::gemini::{GeminiMediaPromptOptimizer, GeminiMediaPromptOptimizerConfig};
use openplotva_media::{ImageEditOptimize, ImageOptimize, OptimizePromptOptions};
use thiserror::Error;

use crate::runtime_gemini_cache::resolve_google_ai_key;

const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const TOGETHER_MODEL_PREFIX: &str = "together/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
const TOGETHER_CHAT_COMPLETIONS_URL: &str = "https://api.together.xyz/v1/chat/completions";

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

    /// Execute image edit-prompt optimization.
    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
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

#[derive(Clone, Debug)]
pub struct FallbackMediaPromptOptimizer<Primary, Fallback> {
    primary: Primary,
    fallback: Fallback,
    image_primary_label: String,
    image_fallback_label: String,
    edit_primary_label: String,
    edit_fallback_label: String,
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

impl<Primary, Fallback> FallbackMediaPromptOptimizer<Primary, Fallback> {
    /// Build a fallback optimizer with shared labels for image and edit errors.
    #[must_use]
    pub fn new(
        primary: Primary,
        fallback: Fallback,
        primary_label: impl Into<String>,
        fallback_label: impl Into<String>,
    ) -> Self {
        let primary_label = primary_label.into();
        let fallback_label = fallback_label.into();
        Self {
            primary,
            fallback,
            image_primary_label: primary_label.clone(),
            image_fallback_label: fallback_label.clone(),
            edit_primary_label: primary_label,
            edit_fallback_label: fallback_label,
        }
    }

    #[must_use]
    pub fn with_flow_labels(
        primary: Primary,
        fallback: Fallback,
        image_primary_label: impl Into<String>,
        image_fallback_label: impl Into<String>,
        edit_primary_label: impl Into<String>,
        edit_fallback_label: impl Into<String>,
    ) -> Self {
        Self {
            primary,
            fallback,
            image_primary_label: image_primary_label.into(),
            image_fallback_label: image_fallback_label.into(),
            edit_primary_label: edit_primary_label.into(),
            edit_fallback_label: edit_fallback_label.into(),
        }
    }
}

impl<Primary, Fallback> MediaPromptOptimizer for FallbackMediaPromptOptimizer<Primary, Fallback>
where
    Primary: MediaPromptOptimizer,
    Fallback: MediaPromptOptimizer,
{
    fn optimize_image_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImagePromptOptimizeFuture<'a> {
        Box::pin(async move {
            match self.primary.optimize_image_prompt(text, options).await {
                Ok(value) => Ok(value),
                Err(primary_error) => self
                    .fallback
                    .optimize_image_prompt(text, options)
                    .await
                    .map_err(|fallback_error| {
                        combined_optimizer_error(
                            &self.image_primary_label,
                            &primary_error,
                            &self.image_fallback_label,
                            &fallback_error,
                        )
                    }),
            }
        })
    }

    fn optimize_image_edit_prompt<'a>(
        &'a self,
        text: &'a str,
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        Box::pin(async move {
            match self.primary.optimize_image_edit_prompt(text, options).await {
                Ok(value) => Ok(value),
                Err(primary_error) => self
                    .fallback
                    .optimize_image_edit_prompt(text, options)
                    .await
                    .map_err(|fallback_error| {
                        combined_optimizer_error(
                            &self.edit_primary_label,
                            &primary_error,
                            &self.edit_fallback_label,
                            &fallback_error,
                        )
                    }),
            }
        })
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
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        self.as_ref().optimize_image_edit_prompt(text, options)
    }
}

fn combined_optimizer_error(
    primary_label: &str,
    primary_error: &MediaPromptOptimizerError,
    fallback_label: &str,
    fallback_error: &MediaPromptOptimizerError,
) -> MediaPromptOptimizerError {
    MediaPromptOptimizerError::Provider(format!(
        "{primary_label} failed: {primary_error}; {fallback_label} failed: {fallback_error}"
    ))
}

impl<Optimizer> MediaPromptOptimizerService<Optimizer>
where
    Optimizer: MediaPromptOptimizer,
{
    pub async fn enhance_image_prompt(
        &self,
        original_prompt: &str,
        aspect_ratio: &str,
        variant_count: usize,
    ) -> MediaPromptOptimization<ImageOptimize> {
        let fallback = ImageOptimize {
            input: original_prompt.to_owned(),
            outputs: vec![original_prompt.to_owned()],
            aspect_ratio: aspect_ratio.to_owned(),
            nsfw_result: openplotva_media::NsfwResult::Adult,
        };
        let Some(optimizer) = &self.optimizer else {
            return MediaPromptOptimization {
                value: fallback,
                provider_error: None,
            };
        };
        match optimizer
            .optimize_image_prompt(original_prompt, OptimizePromptOptions { variant_count })
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

    pub async fn enhance_image_edit_prompt(
        &self,
        original_prompt: &str,
        variant_count: usize,
    ) -> MediaPromptOptimization<ImageEditOptimize> {
        let fallback = ImageEditOptimize {
            input: original_prompt.to_owned(),
            outputs: vec![original_prompt.to_owned()],
            nsfw_result: openplotva_media::NsfwResult::Adult,
        };
        let Some(optimizer) = &self.optimizer else {
            return MediaPromptOptimization {
                value: fallback,
                provider_error: None,
            };
        };
        match optimizer
            .optimize_image_edit_prompt(original_prompt, OptimizePromptOptions { variant_count })
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
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_status: StatusUpdate| {};
            AifarmStructuredJsonGenerator::optimize_image_edit_prompt(
                self,
                text,
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
        options: OptimizePromptOptions,
    ) -> ImageEditPromptOptimizeFuture<'a> {
        Box::pin(async move {
            GeminiMediaPromptOptimizer::optimize_image_edit_prompt(self, text, options)
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
        use_tool_calls: Some(dialog.aifarm_use_tool_calls),
        enable_thinking: Some(dialog.aifarm_enable_thinking),
        include_reasoning: Some(false),
        pool: aifarm_pool_config_from_app_config(config),
        ..AifarmDialogConfig::default()
    }
}

#[must_use]
pub fn aifarm_pool_config_from_app_config(config: &AppConfig) -> AifarmPoolConfig {
    let dialog = &config.llm.dialog;
    AifarmPoolConfig::from_go_lists(
        &dialog.aifarm_pool_models,
        &dialog.aifarm_pool_base_urls,
        &dialog.aifarm_pool_api_key,
        Duration::from_millis(dialog.aifarm_pool_primary_capacity_wait_ms.max(0) as u64),
    )
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
    AifarmStructuredJsonConfig {
        client: discovery_client_config_from_app_config(config, &config.llm.dialog.model),
        model: config.llm.dialog.model.clone(),
        max_tokens: 1024,
    }
    .with_defaults()
}

/// Build the reqwest-backed AIFarm media prompt optimizer.
#[must_use]
pub fn aifarm_media_prompt_optimizer_from_app_config(
    config: &AppConfig,
) -> AifarmStructuredJsonGenerator<ReqwestAifarmTransport> {
    AifarmStructuredJsonGenerator::new(aifarm_structured_json_config_from_app_config(config))
}

/// Build the reqwest-backed Gemini media prompt optimizer when a Google AI key is configured.
#[must_use]
pub fn gemini_media_prompt_optimizer_from_app_config(
    config: &AppConfig,
) -> Option<GeminiMediaPromptOptimizer<ReqwestAifarmTransport>> {
    let api_key = resolve_google_ai_key(&config.google_ai);
    if api_key.trim().is_empty() {
        return None;
    }
    Some(GeminiMediaPromptOptimizer::new(
        GeminiMediaPromptOptimizerConfig {
            api_key,
            model: crate::memory_runtime::genkit_runtime_default_model(config),
            request_timeout: Duration::from_secs(
                config.llm.dialog.request_timeout_seconds.max(1) as u64
            ),
            ..GeminiMediaPromptOptimizerConfig::default()
        },
    ))
}

fn genkit_media_prompt_optimizer_from_app_config(
    config: &AppConfig,
) -> Option<(AppMediaPromptOptimizer, String)> {
    let api_key = resolve_google_ai_key(&config.google_ai);
    if api_key.trim().is_empty() {
        return None;
    }
    let model = crate::memory_runtime::genkit_runtime_default_model(config);
    if let Some((cfg, provider)) =
        genkit_openai_compatible_media_prompt_optimizer_config_from_app_config(config, &model)
    {
        return Some((
            Arc::new(AifarmStructuredJsonGenerator::new(cfg)) as AppMediaPromptOptimizer,
            provider.to_owned(),
        ));
    }
    gemini_media_prompt_optimizer_from_app_config(config).map(|optimizer| {
        (
            Arc::new(optimizer) as AppMediaPromptOptimizer,
            "gemini".to_owned(),
        )
    })
}

fn genkit_openai_compatible_media_prompt_optimizer_config_from_app_config(
    config: &AppConfig,
    model: &str,
) -> Option<(AifarmStructuredJsonConfig, &'static str)> {
    let model = model.trim();
    let (direct_url, api_key, model, request_timeout_seconds, provider) =
        if let Some(model) = strip_provider_prefix_fold(model, OPENROUTER_MODEL_PREFIX) {
            (
                OPENROUTER_CHAT_COMPLETIONS_URL,
                config.open_router.key.trim().to_owned(),
                model.trim().to_owned(),
                config.open_router.request_timeout_seconds,
                "openrouter",
            )
        } else if let Some(model) = strip_provider_prefix_fold(model, TOGETHER_MODEL_PREFIX) {
            (
                TOGETHER_CHAT_COMPLETIONS_URL,
                together_api_key(config),
                model.trim().to_owned(),
                config.llm.dialog.request_timeout_seconds,
                "together",
            )
        } else {
            return None;
        };
    if model.is_empty() || api_key.trim().is_empty() {
        return None;
    }
    let mut client = discovery_client_config_from_app_config(config, &model);
    client.direct_url = direct_url.to_owned();
    client.api_key = api_key;
    client.default_model = model.clone();
    client.request_timeout = positive_seconds(request_timeout_seconds);
    Some((
        AifarmStructuredJsonConfig {
            client,
            model,
            max_tokens: 1024,
        }
        .with_defaults(),
        provider,
    ))
}

#[must_use]
pub fn media_prompt_optimizer_from_app_config(
    config: &AppConfig,
) -> Option<AppMediaPromptOptimizer> {
    let primary = aifarm_media_prompt_optimizer_from_app_config(config);
    Some(
        match genkit_media_prompt_optimizer_from_app_config(config) {
            Some((fallback, provider)) => Arc::new(FallbackMediaPromptOptimizer::with_flow_labels(
                primary,
                fallback,
                "aifarm image prompt optimizer",
                format!("{provider} image prompt optimizer"),
                "aifarm edit prompt optimizer",
                format!("{provider} edit prompt optimizer"),
            )) as AppMediaPromptOptimizer,
            None => Arc::new(primary) as AppMediaPromptOptimizer,
        },
    )
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

fn strip_provider_prefix_fold<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

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
        assert_eq!(cfg.max_tokens, 1024);
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
            dialog_aifarm_use_tool_calls: Some("true".to_owned()),
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
            dialog_aifarm_pool_primary_capacity_wait_ms: Some("750".to_owned()),
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
        assert_eq!(cfg.use_tool_calls, Some(true));
        assert_eq!(cfg.enable_thinking, Some(false));
        assert_eq!(cfg.include_reasoning, Some(false));
        assert_eq!(cfg.pool.secondary_backends.len(), 2);
        assert_eq!(cfg.pool.secondary_backends[0].name, "secondary-a");
        assert_eq!(
            cfg.pool.secondary_backends[0].base_url,
            "http://secondary-a.test/v1"
        );
        assert_eq!(cfg.pool.secondary_backends[0].model, "secondary-a");
        assert_eq!(cfg.pool.secondary_api_key, "pool-key");
        assert_eq!(cfg.pool.primary_capacity_wait, Duration::from_millis(750));
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

    #[test]
    fn provider_media_genkit_fallback_routes_openrouter_default_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("google-key".to_owned()),
            genkit_default_model: Some(" openrouter/openai/gpt-4.1-mini ".to_owned()),
            openrouter_key: Some(" openrouter-key ".to_owned()),
            openrouter_request_timeout_seconds: Some("333".to_owned()),
            dialog_discovery_service_name: Some("svc".to_owned()),
            dialog_discovery_endpoint_name: Some("endpoint".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let (cfg, provider) =
            genkit_openai_compatible_media_prompt_optimizer_config_from_app_config(
                &config,
                "openrouter/openai/gpt-4.1-mini",
            )
            .expect("openrouter config");

        assert_eq!(provider, "openrouter");
        assert_eq!(cfg.model, "openai/gpt-4.1-mini");
        assert_eq!(cfg.client.direct_url, OPENROUTER_CHAT_COMPLETIONS_URL);
        assert_eq!(cfg.client.api_key, "openrouter-key");
        assert_eq!(cfg.client.default_model, "openai/gpt-4.1-mini");
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(333));
        assert_eq!(cfg.client.service_name, "svc");
        assert_eq!(cfg.client.endpoint_name, "endpoint");
    }

    #[test]
    fn provider_media_genkit_fallback_routes_together_default_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("google-key".to_owned()),
            genkit_default_model: Some(" together/meta-llama/Llama-4 ".to_owned()),
            together_keys: Some(" , together-key ".to_owned()),
            dialog_request_timeout_seconds: Some("222".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let (cfg, provider) =
            genkit_openai_compatible_media_prompt_optimizer_config_from_app_config(
                &config,
                "together/meta-llama/Llama-4",
            )
            .expect("together config");

        assert_eq!(provider, "together");
        assert_eq!(cfg.model, "meta-llama/Llama-4");
        assert_eq!(cfg.client.direct_url, TOGETHER_CHAT_COMPLETIONS_URL);
        assert_eq!(cfg.client.api_key, "together-key");
        assert_eq!(cfg.client.default_model, "meta-llama/Llama-4");
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(222));
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

        let got = service.enhance_image_prompt("cat", "16:9", 2).await;

        assert_eq!(got.provider_error, None);
        assert_eq!(got.value.outputs, vec!["cinematic cat"]);
        assert_eq!(got.value.aspect_ratio, "16:9");

        let got = service.enhance_image_prompt("cat", "1:1", 1).await;

        assert_eq!(got.provider_error.as_deref(), Some("boom"));
        assert_eq!(
            got.value,
            ImageOptimize {
                input: "cat".to_owned(),
                outputs: vec!["cat".to_owned()],
                aspect_ratio: "1:1".to_owned(),
                nsfw_result: openplotva_media::NsfwResult::Adult,
            }
        );
    }

    #[tokio::test]
    async fn media_optimizer_service_preserves_go_edit_fallback() {
        let optimizer = FakeOptimizer::default();
        let service = MediaPromptOptimizerService::new(Some(optimizer));

        let got = service.enhance_image_edit_prompt("make it day", 3).await;

        assert_eq!(got.provider_error.as_deref(), Some("boom"));
        assert_eq!(
            got.value,
            ImageEditOptimize {
                input: "make it day".to_owned(),
                outputs: vec!["make it day".to_owned()],
                nsfw_result: openplotva_media::NsfwResult::Adult,
            }
        );
    }

    #[tokio::test]
    async fn fallback_media_prompt_optimizer_uses_secondary_after_primary_failure() {
        let primary = FakeOptimizer::default();
        *primary.image_result.lock().expect("primary image result") = Some(Err(
            MediaPromptOptimizerError::Provider("aifarm unavailable".to_owned()),
        ));
        let secondary = FakeOptimizer::default();
        *secondary
            .image_result
            .lock()
            .expect("secondary image result") = Some(Ok(ImageOptimize {
            input: "cat".to_owned(),
            outputs: vec!["gemini cat".to_owned()],
            aspect_ratio: "1:1".to_owned(),
            nsfw_result: openplotva_media::NsfwResult::Safe,
        }));
        let optimizer = FallbackMediaPromptOptimizer::new(
            primary,
            secondary,
            "aifarm image prompt optimizer",
            "gemini image prompt optimizer",
        );

        let got = optimizer
            .optimize_image_prompt("cat", OptimizePromptOptions { variant_count: 1 })
            .await
            .expect("secondary result");

        assert_eq!(got.outputs, vec!["gemini cat"]);
        assert_eq!(got.nsfw_result, openplotva_media::NsfwResult::Safe);
    }

    #[tokio::test]
    async fn fallback_media_prompt_optimizer_uses_secondary_for_edit_after_primary_failure() {
        let primary = FakeOptimizer::default();
        *primary.edit_result.lock().expect("primary edit result") = Some(Err(
            MediaPromptOptimizerError::Provider("aifarm unavailable".to_owned()),
        ));
        let secondary = FakeOptimizer::default();
        *secondary.edit_result.lock().expect("secondary edit result") =
            Some(Ok(ImageEditOptimize {
                input: "make it day".to_owned(),
                outputs: vec!["gemini edit".to_owned()],
                nsfw_result: openplotva_media::NsfwResult::Safe,
            }));
        let optimizer = FallbackMediaPromptOptimizer::with_flow_labels(
            primary,
            secondary,
            "aifarm image prompt optimizer",
            "gemini image prompt optimizer",
            "aifarm edit prompt optimizer",
            "gemini edit prompt optimizer",
        );

        let got = optimizer
            .optimize_image_edit_prompt("make it day", OptimizePromptOptions { variant_count: 1 })
            .await
            .expect("secondary edit result");

        assert_eq!(got.outputs, vec!["gemini edit"]);
        assert_eq!(got.nsfw_result, openplotva_media::NsfwResult::Safe);
    }
}
