//! App-level dialog provider construction.

use std::{fmt, sync::Arc};

use openplotva_config::AppConfig;
use openplotva_dialog::{
    DialogInput, DialogOutput, DialogToolbox, PROVIDER_AIFARM, PROVIDER_GENKIT, PROVIDER_NVIDIA,
    PROVIDER_VMLX,
};
use openplotva_llm::{
    ChatProvider, ChatProviderError,
    aifarm::{
        AifarmClientConfig, AifarmDialogConfig, AifarmDialogProvider, ReqwestAifarmTransport,
    },
    gemini::{
        GeminiDialogConfig, GeminiDialogProvider, GeminiExplicitCacheConfig,
        is_gemini_provider_model,
    },
    retry::retryable_reason,
    whitecircle::{WhiteCircleClientConfig, WhiteCirclePreToolConfig},
    with_fallback,
};
use thiserror::Error;

use crate::agent_runtime;
use crate::media::{
    agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config,
    nvidia_dialog_config_from_app_config, vmlx_dialog_config_from_app_config,
};
use crate::runtime_gemini_cache::resolve_google_ai_key;

const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

/// Shared dialog provider handle.
pub type DialogProviderHandle = Arc<dyn ChatProvider>;

struct PoolGatedFallbackChatProvider {
    primary: DialogProviderHandle,
    fallback: DialogProviderHandle,
    provider_name: String,
}

#[derive(Debug)]
struct PoolGatedFallbackChatProviderError {
    primary_provider: String,
    primary_error: String,
    fallback_provider: String,
    fallback_error: String,
}

impl PoolGatedFallbackChatProvider {
    fn new(primary: DialogProviderHandle, fallback: DialogProviderHandle) -> Self {
        let provider_name = format!(
            "{}+pool-fallback:{}",
            primary.provider_name(),
            fallback.provider_name()
        );
        Self {
            primary,
            fallback,
            provider_name,
        }
    }

    async fn run_fallback(
        &self,
        input: DialogInput,
        primary_error: ChatProviderError,
    ) -> Result<DialogOutput, ChatProviderError> {
        match self.fallback.run_dialog(input).await {
            Ok(mut output) => {
                if output.provider.trim().is_empty() {
                    output.provider = self.fallback.provider_name().to_owned();
                }
                output.fallback_from = self.primary.provider_name().to_owned();
                output.fallback_error = primary_error.to_string();
                Ok(output)
            }
            Err(fallback_error) => Err(Box::new(PoolGatedFallbackChatProviderError {
                primary_provider: self.primary.provider_name().to_owned(),
                primary_error: primary_error.to_string(),
                fallback_provider: self.fallback.provider_name().to_owned(),
                fallback_error: fallback_error.to_string(),
            })),
        }
    }
}

impl fmt::Display for PoolGatedFallbackChatProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "primary provider {}: {}; pool fallback provider {}: {}",
            self.primary_provider, self.primary_error, self.fallback_provider, self.fallback_error
        )
    }
}

impl std::error::Error for PoolGatedFallbackChatProviderError {}

impl ChatProvider for PoolGatedFallbackChatProvider {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

    fn run_dialog<'a>(&'a self, input: DialogInput) -> openplotva_llm::ChatProviderFuture<'a> {
        Box::pin(async move {
            let mut output = match self.primary.run_dialog(input.clone()).await {
                Ok(output) => output,
                Err(primary_error) => {
                    if retryable_reason(primary_error.as_ref()).is_none()
                        || !openplotva_llm::aifarm::pool_enabled()
                    {
                        return Err(primary_error);
                    }
                    return self.run_fallback(input, primary_error).await;
                }
            };
            if output.provider.trim().is_empty() {
                output.provider = self.primary.provider_name().to_owned();
            }
            Ok(output)
        })
    }
}

/// Error returned while building the configured dialog provider.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DialogProviderBuildError {
    #[error("google ai key is required for genkit dialog provider")]
    GenkitGoogleAiKeyRequired,
    /// A GenKit OpenAI-compatible plugin route was selected without its provider key.
    #[error("{provider} api key is required for genkit dialog provider")]
    GenkitProviderApiKeyRequired {
        /// Provider name used in the model prefix.
        provider: &'static str,
    },
    /// A GenKit OpenAI-compatible plugin route was selected without a concrete model suffix.
    #[error("{provider} model is required for genkit dialog provider")]
    GenkitProviderModelRequired {
        /// Provider name used in the model prefix.
        provider: &'static str,
    },
    #[error("genkit fallback provider handle is required")]
    GenkitFallbackProviderRequired,
    #[error("unsupported dialog provider {provider:?}")]
    Unsupported {
        /// Raw provider name after trimming/lowercasing.
        provider: String,
    },
}

/// Build the currently configured dialog provider.
pub fn dialog_provider_from_app_config(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> Result<DialogProviderHandle, DialogProviderBuildError> {
    primary_dialog_provider_from_app_config(config, toolbox)
}

pub fn dialog_provider_from_app_config_with_fallback(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
    fallback: Option<DialogProviderHandle>,
) -> Result<DialogProviderHandle, DialogProviderBuildError> {
    let provider = configured_dialog_provider_name(config);
    let primary = primary_dialog_provider_from_app_config(config, Arc::clone(&toolbox))?;
    if provider == PROVIDER_AIFARM {
        if let Some(qwen) = qwen_dialog_provider_from_app_config(config, toolbox) {
            return Ok(Arc::new(PoolGatedFallbackChatProvider::new(primary, qwen)));
        }
        return Ok(primary);
    }
    if provider != PROVIDER_GENKIT
        && config
            .llm
            .dialog
            .fallback_provider
            .trim()
            .eq_ignore_ascii_case(PROVIDER_GENKIT)
    {
        let Some(fallback) = fallback else {
            return Err(DialogProviderBuildError::GenkitFallbackProviderRequired);
        };
        return Ok(with_fallback(Some(primary), Some(fallback))
            .expect("primary and fallback providers were supplied"));
    }
    Ok(primary)
}

pub fn genkit_dialog_provider_from_app_config(config: &AppConfig) -> Option<DialogProviderHandle> {
    genkit_dialog_provider_from_app_config_with_toolbox(config, None)
}

/// Build the Rust-native direct Gemini provider with the app-owned toolbox attached.
pub fn genkit_dialog_provider_from_app_config_with_toolbox(
    config: &AppConfig,
    toolbox: Option<Arc<dyn DialogToolbox>>,
) -> Option<DialogProviderHandle> {
    genkit_dialog_provider_result_from_app_config_with_toolbox(config, toolbox).ok()
}

fn genkit_dialog_provider_result_from_app_config_with_toolbox(
    config: &AppConfig,
    toolbox: Option<Arc<dyn DialogToolbox>>,
) -> Result<DialogProviderHandle, DialogProviderBuildError> {
    let api_key = resolve_google_ai_key(&config.google_ai);
    if api_key.trim().is_empty() {
        return Err(DialogProviderBuildError::GenkitGoogleAiKeyRequired);
    }
    if let Some(cfg) = genkit_openai_compatible_dialog_config_result_from_app_config(config)? {
        let provider = AifarmDialogProvider::new(cfg);
        let provider = if let Some(toolbox) = toolbox {
            provider.with_toolbox(toolbox)
        } else {
            provider
        };
        return Ok(Arc::new(provider));
    }
    let model = genkit_dialog_model_from_app_config(config);
    let provider = GeminiDialogProvider::new(GeminiDialogConfig {
        api_key,
        model: if is_gemini_provider_model(&model) {
            model
        } else {
            String::new()
        },
        request_timeout: std::time::Duration::from_secs(
            config.llm.dialog.request_timeout_seconds.max(1) as u64,
        ),
        cache: GeminiExplicitCacheConfig::chat_core_multi_turn(),
        ..GeminiDialogConfig::default()
    });
    let provider = if let Some(toolbox) = toolbox {
        provider.with_toolbox(toolbox)
    } else {
        provider
    };
    Ok(Arc::new(provider))
}

#[must_use]
pub fn genkit_openai_compatible_dialog_config_from_app_config(
    config: &AppConfig,
) -> Option<AifarmDialogConfig> {
    genkit_openai_compatible_dialog_config_result_from_app_config(config)
        .ok()
        .flatten()
}

fn genkit_openai_compatible_dialog_config_result_from_app_config(
    config: &AppConfig,
) -> Result<Option<AifarmDialogConfig>, DialogProviderBuildError> {
    let raw_model = genkit_dialog_model_from_app_config(config);
    let raw_model = raw_model.trim();
    let (provider, direct_url, api_key, model, request_timeout_seconds) =
        if let Some(model) = strip_provider_prefix_fold(raw_model, OPENROUTER_MODEL_PREFIX) {
            (
                "openrouter",
                OPENROUTER_CHAT_COMPLETIONS_URL,
                config.open_router.key.trim().to_owned(),
                model.trim().to_owned(),
                config.open_router.request_timeout_seconds,
            )
        } else {
            return Ok(None);
        };
    if api_key.is_empty() {
        return Err(DialogProviderBuildError::GenkitProviderApiKeyRequired { provider });
    }
    if model.is_empty() {
        return Err(DialogProviderBuildError::GenkitProviderModelRequired { provider });
    }
    Ok(Some(AifarmDialogConfig {
        provider_name: PROVIDER_GENKIT.to_owned(),
        client: AifarmClientConfig {
            direct_url: direct_url.to_owned(),
            api_key,
            request_timeout: positive_seconds(request_timeout_seconds),
            default_model: model.clone(),
            ..AifarmClientConfig::default()
        },
        model,
        max_tokens: 8192,
        temperature: Some(0.9),
        top_p: Some(0.97),
        use_tool_calls: Some(true),
        include_reasoning: Some(false),
        ..AifarmDialogConfig::default()
    }))
}

fn genkit_dialog_model_from_app_config(config: &AppConfig) -> String {
    let model = config.llm.dialog.model.trim();
    if model.is_empty() {
        crate::memory_runtime::genkit_runtime_default_model(config)
    } else {
        model.to_owned()
    }
}

fn primary_dialog_provider_from_app_config(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> Result<DialogProviderHandle, DialogProviderBuildError> {
    match configured_dialog_provider_name(config).as_str() {
        PROVIDER_AIFARM => Ok(Arc::new(aifarm_dialog_provider_from_app_config(
            config, toolbox,
        ))),
        PROVIDER_NVIDIA => Ok(Arc::new(nvidia_dialog_provider_from_app_config(
            config, toolbox,
        ))),
        PROVIDER_VMLX => Ok(Arc::new(vmlx_dialog_provider_from_app_config(
            config, toolbox,
        ))),
        PROVIDER_GENKIT => {
            genkit_dialog_provider_result_from_app_config_with_toolbox(config, Some(toolbox))
        }
        provider => Err(DialogProviderBuildError::Unsupported {
            provider: provider.to_owned(),
        }),
    }
}

/// Build the reqwest-backed AIFarm provider with the app-owned toolbox attached.
#[must_use]
pub fn aifarm_dialog_provider_from_app_config(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> AifarmDialogProvider<ReqwestAifarmTransport> {
    AifarmDialogProvider::new(aifarm_dialog_config_from_app_config(config)).with_toolbox(toolbox)
}

#[must_use]
pub fn qwen_dialog_config_from_app_config(config: &AppConfig) -> Option<AifarmDialogConfig> {
    let spec = agent_runtime::qwen_reasoner_named_provider_config(config);
    if !spec
        .kind
        .trim()
        .eq_ignore_ascii_case(openplotva_config::DEFAULT_LLM_PROVIDER_KIND)
    {
        return None;
    }
    Some(
        AifarmDialogConfig {
            provider_name: spec.name.clone(),
            client: agent_client_config_from_named_provider(config, &spec),
            model: spec.model.clone(),
            max_tokens: spec.max_tokens,
            temperature: spec.temperature,
            use_tool_calls: Some(false),
            enable_thinking: spec.enable_thinking,
            include_reasoning: spec.include_reasoning,
            ..AifarmDialogConfig::default()
        }
        .with_defaults(),
    )
}

fn qwen_dialog_provider_from_app_config(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> Option<DialogProviderHandle> {
    let cfg = qwen_dialog_config_from_app_config(config)?;
    Some(Arc::new(
        AifarmDialogProvider::new(cfg).with_toolbox(toolbox),
    ))
}

/// Build the reqwest-backed NVIDIA provider with the app-owned toolbox attached.
#[must_use]
pub fn nvidia_dialog_provider_from_app_config(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> AifarmDialogProvider<ReqwestAifarmTransport> {
    AifarmDialogProvider::new(nvidia_dialog_config_from_app_config(config)).with_toolbox(toolbox)
}

/// Build the reqwest-backed VMLX provider with the app-owned toolbox attached.
#[must_use]
pub fn vmlx_dialog_provider_from_app_config(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> AifarmDialogProvider<ReqwestAifarmTransport> {
    AifarmDialogProvider::new(vmlx_dialog_config_from_app_config(config)).with_toolbox(toolbox)
}

#[must_use]
pub fn configured_dialog_provider_name(config: &AppConfig) -> String {
    let provider = config.llm.dialog.provider.trim().to_ascii_lowercase();
    if provider.is_empty() {
        PROVIDER_GENKIT.to_owned()
    } else {
        provider
    }
}

#[must_use]
pub fn white_circle_client_config_from_app_config(config: &AppConfig) -> WhiteCircleClientConfig {
    WhiteCircleClientConfig {
        enabled: config.white_circle.enabled,
        api_key: config.white_circle.api_key.clone(),
        deployment_id: config.white_circle.deployment_id.clone(),
        ..WhiteCircleClientConfig::default()
    }
}

#[must_use]
pub fn white_circle_effective_enabled(config: &AppConfig) -> bool {
    white_circle_client_config_from_app_config(config).effective_enabled()
}

#[must_use]
pub fn white_circle_pre_tool_config_from_app_config(
    config: &AppConfig,
) -> WhiteCirclePreToolConfig {
    WhiteCirclePreToolConfig {
        mode: "legacy".to_owned(),
        flow: "telegram.chat".to_owned(),
        assistant_model: config.llm.dialog.model.clone(),
        ..WhiteCirclePreToolConfig::default()
    }
}

fn strip_provider_prefix_fold<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

fn positive_seconds(seconds: i32) -> std::time::Duration {
    if seconds <= 0 {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_secs(seconds as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use openplotva_dialog::{DialogInput, DialogOutput, DialogToolbox};
    use openplotva_llm::{
        ChatProviderError,
        retry::{FailureReason, ProviderError},
    };

    use super::*;

    #[derive(Default)]
    struct EmptyToolbox;

    impl DialogToolbox for EmptyToolbox {}

    #[test]
    fn configured_dialog_provider_name_matches_go_empty_to_genkit_fallback() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("  ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        assert_eq!(configured_dialog_provider_name(&config), PROVIDER_GENKIT);
    }

    #[test]
    fn dialog_provider_factory_builds_aifarm_provider_from_default_config() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);

        let provider = dialog_provider_from_app_config(&config, toolbox).expect("provider");

        assert_eq!(provider.provider_name(), PROVIDER_AIFARM);
    }

    #[test]
    fn dialog_provider_factory_builds_nvidia_and_vmlx_providers() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some(" NVIDIA ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);

        let provider = dialog_provider_from_app_config(&config, toolbox).expect("nvidia provider");

        assert_eq!(provider.provider_name(), PROVIDER_NVIDIA);

        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some(" VMLX ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);

        let provider = dialog_provider_from_app_config(&config, toolbox).expect("vmlx provider");

        assert_eq!(provider.provider_name(), PROVIDER_VMLX);
    }

    #[test]
    fn dialog_provider_factory_builds_genkit_primary_when_key_is_available() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            googleai_key: Some("gemini-key".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);

        let provider = dialog_provider_from_app_config(&config, toolbox).expect("provider");

        assert_eq!(provider.provider_name(), PROVIDER_GENKIT);
    }

    #[test]
    fn dialog_provider_factory_reports_unavailable_genkit_without_google_key() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);

        let error = dialog_provider_from_app_config(&config, toolbox).err();

        assert_eq!(
            error,
            Some(DialogProviderBuildError::GenkitGoogleAiKeyRequired)
        );
    }

    #[test]
    fn dialog_provider_factory_attaches_supplied_genkit_fallback_when_configured() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("nvidia".to_owned()),
            dialog_fallback_provider: Some(" GENKIT ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);
        let fallback: DialogProviderHandle = Arc::new(EmptyProvider {
            name: PROVIDER_GENKIT,
        });

        let provider =
            dialog_provider_from_app_config_with_fallback(&config, toolbox, Some(fallback))
                .expect("provider");

        assert_eq!(provider.provider_name(), "nvidia+fallback:genkit");
    }

    #[test]
    fn qwen_dialog_config_uses_default_reasoner_provider() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");

        let cfg = qwen_dialog_config_from_app_config(&config).expect("qwen config");

        assert_eq!(
            cfg.provider_name,
            openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER
        );
        assert_eq!(
            cfg.client.service_name,
            crate::agent_runtime::DEFAULT_QWEN_SERVICE_NAME
        );
        assert_eq!(cfg.model, crate::agent_runtime::DEFAULT_QWEN_MODEL);
        assert_eq!(cfg.include_reasoning, Some(false));
        assert_eq!(cfg.enable_thinking, Some(false));
    }

    #[test]
    fn qwen_dialog_config_prefers_explicit_named_provider() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            llm_provider_names: Some("qwen-reasoner".to_owned()),
            llm_provider_kinds: Some("aifarm".to_owned()),
            llm_provider_discovery_service_names: Some("custom-qwen".to_owned()),
            llm_provider_discovery_endpoint_names: Some("chat_completions".to_owned()),
            llm_provider_models: Some("custom-model".to_owned()),
            llm_provider_max_tokens: Some("4096".to_owned()),
            llm_provider_temperatures: Some("0.4".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = qwen_dialog_config_from_app_config(&config).expect("qwen config");

        assert_eq!(cfg.client.service_name, "custom-qwen");
        assert_eq!(cfg.model, "custom-model");
        assert_eq!(cfg.max_tokens, 4096);
        assert_eq!(cfg.temperature, Some(0.4));
    }

    #[tokio::test]
    async fn pool_gated_fallback_skips_qwen_when_pool_disabled() {
        let _guard = AIFARM_POOL_TEST_LOCK.lock().await;
        openplotva_llm::aifarm::set_pool_enabled(false);
        let primary: DialogProviderHandle = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Err(Box::new(ProviderError::new(
                "aifarm",
                FailureReason::CapacityUnavailable,
                "capacity unavailable",
            )))],
        ));
        let fallback = Arc::new(SequencedProvider::new(
            "qwen-reasoner",
            vec![Ok(DialogOutput {
                provider: "qwen-reasoner".to_owned(),
                answer: "fallback".to_owned(),
                ..DialogOutput::default()
            })],
        ));
        let fallback_dyn: DialogProviderHandle = fallback.clone();
        let provider = PoolGatedFallbackChatProvider::new(primary, fallback_dyn);

        let error = provider.run_dialog(DialogInput::default()).await.err();

        assert!(error.is_some());
        assert_eq!(fallback.calls(), 0);
        openplotva_llm::aifarm::set_pool_enabled(true);
    }

    #[tokio::test]
    async fn pool_gated_fallback_uses_qwen_after_retryable_primary_error_when_pool_enabled() {
        let _guard = AIFARM_POOL_TEST_LOCK.lock().await;
        openplotva_llm::aifarm::set_pool_enabled(true);
        let primary: DialogProviderHandle = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Err(Box::new(ProviderError::new(
                "aifarm",
                FailureReason::CapacityUnavailable,
                "capacity unavailable",
            )))],
        ));
        let fallback = Arc::new(SequencedProvider::new(
            "qwen-reasoner",
            vec![Ok(DialogOutput {
                provider: "qwen-reasoner".to_owned(),
                answer: "fallback".to_owned(),
                ..DialogOutput::default()
            })],
        ));
        let fallback_dyn: DialogProviderHandle = fallback.clone();
        let provider = PoolGatedFallbackChatProvider::new(primary, fallback_dyn);

        let output = provider
            .run_dialog(DialogInput::default())
            .await
            .expect("fallback output");

        assert_eq!(output.provider, "qwen-reasoner");
        assert_eq!(output.fallback_from, "aifarm");
        assert_eq!(fallback.calls(), 1);
    }

    #[test]
    fn dialog_provider_factory_requires_real_genkit_fallback_handle() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("nvidia".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);

        let error = dialog_provider_from_app_config_with_fallback(&config, toolbox, None).err();

        assert_eq!(
            error,
            Some(DialogProviderBuildError::GenkitFallbackProviderRequired)
        );
    }

    #[test]
    fn genkit_dialog_provider_factory_builds_direct_gemini_when_key_is_available() {
        let missing = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");
        assert!(genkit_dialog_provider_from_app_config(&missing).is_none());

        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some(" gemini-key ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let provider = genkit_dialog_provider_from_app_config(&config).expect("provider");

        assert_eq!(provider.provider_name(), PROVIDER_GENKIT);
    }

    #[test]
    fn genkit_dialog_provider_factory_builds_openrouter_plugin_route() {
        let openrouter = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            dialog_model: Some("openrouter/x-ai/grok-4.1-fast".to_owned()),
            googleai_key: Some("gemini-key".to_owned()),
            openrouter_key: Some(" openrouter-key ".to_owned()),
            openrouter_request_timeout_seconds: Some("321".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("openrouter config");

        let cfg = genkit_openai_compatible_dialog_config_from_app_config(&openrouter)
            .expect("openrouter config");

        assert_eq!(cfg.provider_name, PROVIDER_GENKIT);
        assert_eq!(cfg.model, "x-ai/grok-4.1-fast");
        assert_eq!(
            cfg.client.direct_url,
            "https://openrouter.ai/api/v1/chat/completions"
        );
        assert_eq!(cfg.client.api_key, "openrouter-key");
        assert_eq!(cfg.client.request_timeout.as_secs(), 321);
        assert_eq!(cfg.max_tokens, 8192);
        assert_eq!(cfg.temperature, Some(0.9));
        assert_eq!(cfg.top_p, Some(0.97));
        assert_eq!(cfg.use_tool_calls, Some(true));
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);
        let provider = dialog_provider_from_app_config(&openrouter, toolbox).expect("provider");
        assert_eq!(provider.provider_name(), PROVIDER_GENKIT);

        let openrouter_default = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            dialog_model: Some(" ".to_owned()),
            genkit_default_model: Some("openrouter/default-model".to_owned()),
            googleai_key: Some("gemini-key".to_owned()),
            openrouter_key: Some(" openrouter-key ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("openrouter default config");
        let cfg = genkit_openai_compatible_dialog_config_from_app_config(&openrouter_default)
            .expect("openrouter default config");
        assert_eq!(cfg.model, "default-model");
    }

    #[test]
    fn genkit_dialog_provider_factory_preserves_go_google_key_requirement_for_plugin_routes() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            dialog_model: Some("openrouter/x-ai/grok-4.1-fast".to_owned()),
            openrouter_key: Some("openrouter-key".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);
        let error = dialog_provider_from_app_config(&config, toolbox).err();

        assert_eq!(
            error,
            Some(DialogProviderBuildError::GenkitGoogleAiKeyRequired)
        );
    }

    #[test]
    fn genkit_dialog_provider_factory_reports_prefixed_missing_keys_without_gemini_fallthrough() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            dialog_model: Some("openrouter/x-ai/grok-4.1-fast".to_owned()),
            googleai_key: Some("gemini-key".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);
        let error = dialog_provider_from_app_config(&config, toolbox).err();

        assert_eq!(
            error,
            Some(DialogProviderBuildError::GenkitProviderApiKeyRequired {
                provider: "openrouter",
            })
        );
    }

    #[tokio::test]
    #[ignore = "live OpenRouter GenKit-compatible dialog smoke"]
    async fn live_genkit_openrouter_dialog_smoke_completes_minimal_prompt()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let openrouter_key = required_env("OPENROUTER_KEY")?;
        let googleai_key = optional_env("GOOGLEAI_KEY");
        let googleai_key_stats_file = optional_env("GOOGLEAI_KEY_STATS_FILE");
        if googleai_key.is_none() && googleai_key_stats_file.is_none() {
            return Err("GOOGLEAI_KEY or GOOGLEAI_KEY_STATS_FILE is required by the configured GenKit plugin route".into());
        }
        let model = std::env::var("OPENPLOTVA_OPENROUTER_SMOKE_MODEL")
            .unwrap_or_else(|_| "openai/gpt-4o-mini".to_owned());
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            dialog_model: Some(format!("openrouter/{model}")),
            googleai_key,
            googleai_key_stats_file,
            openrouter_key: Some(openrouter_key),
            dialog_request_timeout_seconds: Some("60".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;
        let toolbox: Arc<dyn DialogToolbox> = Arc::new(EmptyToolbox);
        let provider = dialog_provider_from_app_config(&config, toolbox)?;

        let output = provider.run_dialog(live_dialog_smoke_input()).await?;

        assert!(
            !output.answer.trim().is_empty(),
            "OpenRouter dialog answer must be non-empty"
        );
        Ok(())
    }

    fn live_dialog_smoke_input() -> DialogInput {
        let prompt =
            std::env::var("OPENPLOTVA_DIALOG_PROVIDER_SMOKE_PROMPT").unwrap_or_else(|_| {
                "Reply with one short sentence confirming this smoke works.".to_owned()
            });
        let mut input = DialogInput::default();
        input.context.bot_name = "Plotva".to_owned();
        input.context.chat_title = "OpenPlotva smoke".to_owned();
        input.context.locale = "en".to_owned();
        input.user.id = 42;
        input.user.full_name = "Smoke User".to_owned();
        input.message.id = 1;
        input.message.text = prompt.clone();
        input.message.normalized = prompt;
        input.disable_tools = true;
        input.max_output_tokens = 64;
        input
    }

    fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        optional_env(name).ok_or_else(|| format!("{name} is required").into())
    }

    fn optional_env(name: &str) -> Option<String> {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
    }

    struct EmptyProvider {
        name: &'static str,
    }

    impl ChatProvider for EmptyProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn run_dialog<'a>(
            &'a self,
            _input: openplotva_dialog::DialogInput,
        ) -> openplotva_llm::ChatProviderFuture<'a> {
            Box::pin(async move { Ok(openplotva_dialog::DialogOutput::default()) })
        }
    }

    static AIFARM_POOL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct SequencedProvider {
        name: &'static str,
        results: Mutex<Vec<Result<DialogOutput, ChatProviderError>>>,
        calls: Mutex<usize>,
    }

    impl SequencedProvider {
        fn new(
            name: &'static str,
            mut results: Vec<Result<DialogOutput, ChatProviderError>>,
        ) -> Self {
            results.reverse();
            Self {
                name,
                results: Mutex::new(results),
                calls: Mutex::new(0),
            }
        }

        fn calls(&self) -> usize {
            *self.calls.lock().expect("calls")
        }
    }

    impl ChatProvider for SequencedProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn run_dialog<'a>(&'a self, _input: DialogInput) -> openplotva_llm::ChatProviderFuture<'a> {
            Box::pin(async move {
                *self.calls.lock().expect("calls") += 1;
                self.results
                    .lock()
                    .expect("results")
                    .pop()
                    .unwrap_or_else(|| Ok(DialogOutput::default()))
            })
        }
    }
}
