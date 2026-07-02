//! App-level dialog provider construction.

use std::{collections::HashMap, sync::Arc};

use openplotva_config::AppConfig;
use openplotva_dialog::{
    DialogInput, DialogToolbox, PROVIDER_AIFARM, PROVIDER_GENKIT, PROVIDER_NVIDIA, PROVIDER_VMLX,
};
use openplotva_llm::{
    ChatProvider, ChatProviderError, ChatProviderFuture,
    aifarm::{
        AifarmClientConfig, AifarmDialogConfig, AifarmDialogProvider, ReqwestAifarmTransport,
        normalize_chat_completions_url,
    },
    gemini::{
        GeminiDialogConfig, GeminiDialogProvider, GeminiExplicitCacheConfig,
        is_gemini_provider_model,
    },
    retry::retryable_reason,
    router::{BreakerSet, RouterHandle, TriggerState},
    whitecircle::{WhiteCircleClientConfig, WhiteCirclePreToolConfig},
    with_fallback,
};
use thiserror::Error;

use crate::media::{
    agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config,
    nvidia_dialog_config_from_app_config, vmlx_dialog_config_from_app_config,
};
use crate::routed_attempts::{RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext};
use crate::runtime_gemini_cache::resolve_google_ai_key;

const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

/// Shared dialog provider handle.
pub type DialogProviderHandle = Arc<dyn ChatProvider>;

/// Workflow key the dialog worker routes through.
const DIALOG_WORKFLOW_KEY: &str = "dialog";

/// Dialog provider that selects its concrete backend per request from the
/// DB-backed routing table (weighted primaries, ordered fallback, trigger-engaged
/// overflow), recording circuit-breaker outcomes. Underlying transport clients are
/// reused from the existing per-provider factories, keyed by provider name.
pub struct RouterChatProvider {
    walker: RoutedAttemptWalker,
    clients: HashMap<String, DialogProviderHandle>,
    default_client: DialogProviderHandle,
    provider_name: String,
}

impl RouterChatProvider {
    #[must_use]
    pub fn new(
        handle: Arc<RouterHandle>,
        breakers: Arc<BreakerSet>,
        triggers: Arc<TriggerState>,
        clients: HashMap<String, DialogProviderHandle>,
        default_client: DialogProviderHandle,
    ) -> Self {
        Self {
            walker: RoutedAttemptWalker::new(handle, breakers, triggers),
            clients,
            default_client,
            provider_name: "router".to_owned(),
        }
    }

    #[must_use]
    pub fn with_routing_event_reporter(
        mut self,
        reporter: crate::runtime_routing::RoutingEventReporter,
    ) -> Self {
        self.walker = self.walker.with_reporter(reporter);
        self
    }
}

impl ChatProvider for RouterChatProvider {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

    fn run_dialog<'a>(&'a self, input: DialogInput) -> ChatProviderFuture<'a> {
        Box::pin(async move {
            let input_for_attempts = input.clone();
            let clients = self.clients.clone();
            let default_client = Arc::clone(&self.default_client);
            let result = self
                .walker
                .run(
                    RoutedRequestContext {
                        workflow_key: DIALOG_WORKFLOW_KEY.to_owned(),
                        queue_name: Some("dialog".to_owned()),
                        chat_id: (input.context.chat_id != 0).then_some(input.context.chat_id),
                        thread_id: input.context.thread_id,
                        message_id: (input.message.id != 0).then_some(input.message.id),
                        suppress_all_attempts_exhausted_admin_report: true,
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let client = clients
                            .get(&attempt.provider_name)
                            .cloned()
                            .unwrap_or_else(|| Arc::clone(&default_client));
                        let mut request = input_for_attempts.clone();
                        if !attempt.model_name.trim().is_empty() {
                            request.model = attempt.model_name.clone();
                        }
                        if let Some(max_tokens) = attempt.overrides.max_tokens
                            && max_tokens > 0
                        {
                            request.max_output_tokens = max_tokens;
                        }
                        if let Some(enable_thinking) = attempt
                            .overrides
                            .extra
                            .get("enable_thinking")
                            .and_then(serde_json::Value::as_bool)
                        {
                            request.enable_thinking = Some(enable_thinking);
                        }
                        async move {
                            match client.run_dialog(request).await {
                                Ok(mut output) => {
                                    if output.provider.trim().is_empty() {
                                        output.provider = client.provider_name().to_owned();
                                    }
                                    Ok(output)
                                }
                                Err(error) => Err(error),
                            }
                        }
                    },
                    |error| retryable_reason(error.as_ref()),
                )
                .await;
            match result {
                Ok(output) => Ok(output),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    let error: ChatProviderError = Box::new(error);
                    Err(error)
                }
            }
        })
    }
}

/// Build the DB-backed dialog provider, reusing the existing aifarm and genkit
/// transport clients keyed by provider name (`gemini` overflow shares the Google
/// AI / genkit client). Falls back to the aifarm primary when no route resolves.
pub fn router_dialog_provider(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
    handle: Arc<RouterHandle>,
    breakers: Arc<BreakerSet>,
    triggers: Arc<TriggerState>,
    genkit_fallback: Option<DialogProviderHandle>,
    routing_events: Option<crate::runtime_routing::RoutingEventReporter>,
) -> DialogProviderHandle {
    // Env pool secondaries are first-class DB models routed through their own
    // `vram-cloud` client below, so admin weights control them without a hidden pool.
    let aifarm_cfg = aifarm_dialog_config_from_app_config(config);
    let aifarm: DialogProviderHandle =
        Arc::new(AifarmDialogProvider::new(aifarm_cfg).with_toolbox(Arc::clone(&toolbox)));

    let genkit = genkit_fallback.or_else(|| {
        genkit_dialog_provider_from_app_config_with_toolbox(config, Some(Arc::clone(&toolbox)))
    });

    let mut clients: HashMap<String, DialogProviderHandle> = HashMap::new();
    clients.insert(PROVIDER_AIFARM.to_owned(), Arc::clone(&aifarm));
    if let Some(genkit) = genkit {
        clients.insert(PROVIDER_GENKIT.to_owned(), Arc::clone(&genkit));
        clients.insert("gemini".to_owned(), genkit);
    }
    if let Some(vram_cloud) = vram_cloud_dialog_provider(config, Arc::clone(&toolbox)) {
        clients.insert(VRAM_CLOUD_PROVIDER_NAME.to_owned(), vram_cloud);
    }
    if let Some(qwen_reasoner) = qwen_reasoner_dialog_provider(config, Arc::clone(&toolbox)) {
        clients.insert(qwen_reasoner.provider_name().to_owned(), qwen_reasoner);
    }

    let provider = RouterChatProvider::new(handle, breakers, triggers, clients, aifarm);
    let provider = match routing_events {
        Some(reporter) => provider.with_routing_event_reporter(reporter),
        None => provider,
    };
    Arc::new(provider)
}

/// Provider name for the direct OpenAI-compatible VRAM Cloud endpoint.
pub const VRAM_CLOUD_PROVIDER_NAME: &str = "vram-cloud";

/// Build a single-backend, direct OpenAI-compatible client for the configured
/// VRAM Cloud endpoint. Each DB model selects itself via the per-attempt model
/// override, so one client serves every model on that endpoint.
fn vram_cloud_dialog_provider(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> Option<DialogProviderHandle> {
    let cfg = vram_cloud_dialog_config(config)?;
    Some(Arc::new(
        AifarmDialogProvider::new(cfg).with_toolbox(toolbox),
    ))
}

fn vram_cloud_dialog_config(config: &AppConfig) -> Option<AifarmDialogConfig> {
    let dialog = &config.llm.dialog;
    let model = dialog.aifarm_pool_models.first()?;
    let base = dialog.aifarm_pool_base_urls.first()?;
    if model.trim().is_empty() || base.trim().is_empty() {
        return None;
    }
    let mut cfg = aifarm_dialog_config_from_app_config(config);
    cfg.provider_name = VRAM_CLOUD_PROVIDER_NAME.to_owned();
    cfg.client.direct_url = normalize_chat_completions_url(base);
    cfg.client.api_key = dialog.aifarm_pool_api_key.clone();
    cfg.client.default_model = model.clone();
    cfg.model = model.clone();
    // Pool models are reasoning models: the client ceiling must leave room for
    // thinking tokens, or max_tokens_for_input clamps routing overrides back to
    // the primary's small budget and the answer never materializes.
    cfg.max_tokens = dialog.aifarm_pool_max_tokens;
    Some(cfg)
}

fn qwen_reasoner_dialog_provider(
    config: &AppConfig,
    toolbox: Arc<dyn DialogToolbox>,
) -> Option<DialogProviderHandle> {
    let cfg = qwen_reasoner_dialog_config_from_app_config(config);
    if cfg.provider_name.eq_ignore_ascii_case(PROVIDER_AIFARM) {
        return None;
    }
    Some(Arc::new(
        AifarmDialogProvider::new(cfg).with_toolbox(toolbox),
    ))
}

fn qwen_reasoner_dialog_config_from_app_config(config: &AppConfig) -> AifarmDialogConfig {
    let spec = crate::agent_runtime::qwen_reasoner_named_provider_config(config);
    let mut cfg = aifarm_dialog_config_from_app_config(config);
    cfg.provider_name = aifarm_discovery_provider_name(&spec.discovery_service_name, config);
    cfg.client = agent_client_config_from_named_provider(config, &spec);
    if cfg.client.api_key.trim().is_empty() {
        cfg.client.api_key = config.llm.dialog.api_key.clone();
    }
    cfg.model = spec.model.clone();
    cfg.max_tokens = spec.max_tokens;
    cfg.temperature = spec.temperature;
    cfg.use_tool_calls = Some(true);
    cfg.enable_thinking = spec.enable_thinking;
    cfg.include_reasoning = spec.include_reasoning.or(Some(false));
    cfg.with_defaults()
}

fn aifarm_discovery_provider_name(service: &str, config: &AppConfig) -> String {
    let service = service.trim();
    if service.is_empty() || service == config.llm.dialog.discovery_service_name {
        return PROVIDER_AIFARM.to_owned();
    }
    format!(
        "aifarm-{}",
        service
            .trim_start_matches("llm-openai-")
            .trim_start_matches("llm-")
    )
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
    use openplotva_storage::llm_routing::{
        AssignmentRecord, ModelRecord, ProviderRecord, RoutingSnapshot, WorkflowRecord,
    };
    use serde_json::json;

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
    fn qwen_reasoner_dialog_config_targets_gpu_discovery_service_with_tools() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");

        let cfg = qwen_reasoner_dialog_config_from_app_config(&config);

        assert_eq!(cfg.provider_name, "aifarm-qwen27b-gguf");
        assert_eq!(cfg.client.service_name, "llm-openai-qwen27b-gguf");
        assert_eq!(cfg.client.endpoint_name, "chat_completions");
        assert_eq!(cfg.model, "qwen3.6-27b-moq");
        assert_eq!(cfg.client.default_model, "qwen3.6-27b-moq");
        assert_eq!(cfg.use_tool_calls, Some(true));
        assert_eq!(cfg.include_reasoning, Some(false));
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

    #[tokio::test]
    async fn router_chat_provider_emits_route_unavailable_without_default_fallback() {
        let default_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Ok(DialogOutput {
                provider: "aifarm".to_owned(),
                answer: "should not be used".to_owned(),
                ..DialogOutput::default()
            })],
        ));
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            std::time::Duration::from_secs(600),
        );
        let provider = router_provider_for_test(
            RoutingSnapshot::default(),
            default_provider.clone(),
            HashMap::new(),
            reporter.clone(),
        );

        let error = provider.run_dialog(DialogInput::default()).await.err();

        assert!(error.is_some());
        assert_eq!(default_provider.calls(), 0);
        let events = reporter.buffer().routing_events(10);
        assert_eq!(events[0].event_type, "route_unavailable");
        assert_eq!(events[0].workflow_key, "dialog");
    }

    #[tokio::test]
    async fn router_chat_provider_emits_no_candidates_for_empty_route() {
        let default_provider = Arc::new(SequencedProvider::new("aifarm", vec![]));
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            std::time::Duration::from_secs(600),
        );
        let mut snapshot = routed_dialog_snapshot();
        snapshot.providers[0].enabled = false;
        let provider = router_provider_for_test(
            snapshot,
            default_provider.clone(),
            HashMap::new(),
            reporter.clone(),
        );

        let error = provider.run_dialog(DialogInput::default()).await.err();

        assert!(error.is_some());
        assert_eq!(default_provider.calls(), 0);
        let events = reporter.buffer().routing_events(10);
        assert_eq!(events[0].event_type, "no_candidates");
        assert_eq!(events[0].workflow_key, "dialog");
    }

    #[tokio::test]
    async fn router_chat_provider_emits_all_attempts_exhausted() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let routed_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Err(Box::new(ProviderError::new(
                "aifarm",
                FailureReason::ProviderUnavailable,
                "provider unavailable",
            )))],
        ));
        let routed_provider_dyn: DialogProviderHandle = routed_provider.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_provider_dyn);
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            std::time::Duration::from_secs(600),
        );
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            reporter.clone(),
        );

        let error = provider.run_dialog(DialogInput::default()).await.err();

        assert!(error.is_some());
        assert_eq!(routed_provider.calls(), 1);
        let events = reporter.buffer().routing_events(10);
        assert_eq!(events[0].event_type, "all_attempts_exhausted");
        assert_eq!(events[0].severity, "warn");
        assert_eq!(events[0].workflow_key, "dialog");
        assert_eq!(events[0].provider_id, Some(1));
        assert_eq!(events[0].model_id, Some(10));
        assert_eq!(events[0].detail["admin_actionable"], false);
        assert_eq!(
            events[0].detail["admin_actionable_reason"],
            "handled_by_job_retry_budget"
        );
    }

    #[tokio::test]
    async fn router_chat_provider_marks_provider_capacity_cooldown() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let routed_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Err(Box::new(ProviderError::new(
                "aifarm",
                FailureReason::CapacityUnavailable,
                "capacity unavailable",
            )))],
        ));
        let routed_provider_dyn: DialogProviderHandle = routed_provider.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_provider_dyn);
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            std::time::Duration::from_secs(600),
        );
        let mut snapshot = routed_dialog_snapshot();
        snapshot.assignments.push(AssignmentRecord {
            id: 101,
            workflow_key: DIALOG_WORKFLOW_KEY.to_owned(),
            scope: "global".to_owned(),
            role: "overflow".to_owned(),
            provider_model_id: 10,
            weight: Some(100),
            fallback_order: None,
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        });
        snapshot
            .triggers
            .push(openplotva_storage::llm_routing::TriggerRecord {
                id: 200,
                workflow_key: DIALOG_WORKFLOW_KEY.to_owned(),
                trigger_type: "provider_capacity".to_owned(),
                engage_assignment_id: 101,
                enabled: true,
                queue_name: None,
                high_watermark: None,
                low_watermark: None,
                params: json!({
                    "provider_id": 1,
                    "model_id": 10,
                    "cooldown_ms": 30_000,
                }),
            });
        let triggers = Arc::new(TriggerState::new());
        let provider = RouterChatProvider::new(
            RouterHandle::new(crate::model_routing::build_routing_table(&snapshot)),
            Arc::new(BreakerSet::new()),
            Arc::clone(&triggers),
            clients,
            default_provider,
        )
        .with_routing_event_reporter(reporter.clone());

        let error = provider.run_dialog(DialogInput::default()).await.err();

        assert!(error.is_some());
        assert!(triggers.provider_capacity_unavailable(1, 10));
        let events = reporter.buffer().routing_events(10);
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "capacity_unavailable")
        );
    }

    #[tokio::test]
    async fn router_chat_provider_applies_model_config_overrides() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let routed_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Ok(DialogOutput {
                provider: "aifarm".to_owned(),
                answer: "ok".to_owned(),
                ..DialogOutput::default()
            })],
        ));
        let routed_provider_dyn: DialogProviderHandle = routed_provider.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_provider_dyn);
        let mut snapshot = routed_dialog_snapshot();
        snapshot.models[0].model_name = "openrouter/provider/model".to_owned();
        snapshot.models[0].config = json!({ "max_tokens": 123 });
        let provider = router_provider_for_test(
            snapshot,
            default_provider,
            clients,
            crate::runtime_routing::RoutingEventReporter::new(
                crate::runtime_routing::RoutingEventBuffer::new(8),
                None,
                None,
                std::time::Duration::from_secs(600),
            ),
        );

        let output = provider
            .run_dialog(DialogInput::default())
            .await
            .expect("dialog output");

        assert_eq!(output.answer, "ok");
        let inputs = routed_provider.inputs();
        assert_eq!(inputs[0].model, "openrouter/provider/model");
        assert_eq!(inputs[0].max_output_tokens, 123);
    }

    #[tokio::test]
    async fn router_chat_provider_applies_enable_thinking_override() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let routed_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Ok(DialogOutput {
                provider: "aifarm".to_owned(),
                answer: "ok".to_owned(),
                ..DialogOutput::default()
            })],
        ));
        let routed_provider_dyn: DialogProviderHandle = routed_provider.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_provider_dyn);
        let mut snapshot = routed_dialog_snapshot();
        snapshot.assignments[0].inference_overrides =
            json!({ "max_tokens": 4096, "enable_thinking": false });
        let provider = router_provider_for_test(
            snapshot,
            default_provider,
            clients,
            crate::runtime_routing::RoutingEventReporter::new(
                crate::runtime_routing::RoutingEventBuffer::new(8),
                None,
                None,
                std::time::Duration::from_secs(600),
            ),
        );

        let output = provider
            .run_dialog(DialogInput::default())
            .await
            .expect("dialog output");

        assert_eq!(output.answer, "ok");
        let inputs = routed_provider.inputs();
        assert_eq!(inputs[0].max_output_tokens, 4096);
        assert_eq!(inputs[0].enable_thinking, Some(false));
    }

    #[test]
    fn vram_cloud_dialog_config_uses_pool_reasoning_token_ceiling() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_aifarm_max_tokens: Some("1024".to_owned()),
            dialog_aifarm_pool_models: Some("qwen3.6-27b".to_owned()),
            dialog_aifarm_pool_base_urls: Some("https://pool.test/v1".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = vram_cloud_dialog_config(&config).expect("pool config");

        assert_eq!(cfg.provider_name, VRAM_CLOUD_PROVIDER_NAME);
        assert_eq!(cfg.max_tokens, 16384);

        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_aifarm_pool_models: Some("qwen3.6-27b".to_owned()),
            dialog_aifarm_pool_base_urls: Some("https://pool.test/v1".to_owned()),
            dialog_aifarm_pool_max_tokens: Some("8192".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = vram_cloud_dialog_config(&config).expect("pool config");

        assert_eq!(cfg.max_tokens, 8192);
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

    fn router_provider_for_test(
        snapshot: RoutingSnapshot,
        default_provider: Arc<SequencedProvider>,
        clients: HashMap<String, DialogProviderHandle>,
        reporter: crate::runtime_routing::RoutingEventReporter,
    ) -> RouterChatProvider {
        RouterChatProvider::new(
            RouterHandle::new(crate::model_routing::build_routing_table(&snapshot)),
            Arc::new(BreakerSet::new()),
            Arc::new(TriggerState::new()),
            clients,
            default_provider,
        )
        .with_routing_event_reporter(reporter)
    }

    fn routed_dialog_snapshot() -> RoutingSnapshot {
        RoutingSnapshot {
            providers: vec![ProviderRecord {
                id: 1,
                name: "aifarm".to_owned(),
                kind: "chat".to_owned(),
                endpoint: None,
                discovery_service_name: None,
                discovery_endpoint_name: None,
                api_key_ref: Some("REF".to_owned()),
                api_key_encrypted: None,
                enabled: true,
                config: json!({}),
            }],
            models: vec![ModelRecord {
                id: 10,
                provider_id: 1,
                model_name: "db/model".to_owned(),
                display_name: None,
                base_url: None,
                capabilities: vec!["chat".to_owned()],
                embedding_dim: None,
                enabled: true,
                config: json!({}),
            }],
            workflows: vec![WorkflowRecord {
                key: DIALOG_WORKFLOW_KEY.to_owned(),
                kind: "chat".to_owned(),
                full_routing: true,
                retry_max_hops: 1,
                retry_wall_ms: 60_000,
                enabled: true,
            }],
            assignments: vec![AssignmentRecord {
                id: 100,
                workflow_key: DIALOG_WORKFLOW_KEY.to_owned(),
                scope: "global".to_owned(),
                role: "primary".to_owned(),
                provider_model_id: 10,
                weight: Some(100),
                fallback_order: None,
                canary_percent: None,
                enabled: true,
                inference_overrides: json!({}),
                cb_failure_threshold: 5,
                cb_cooldown_ms: 30_000,
            }],
            triggers: vec![],
        }
    }

    struct SequencedProvider {
        name: &'static str,
        results: Mutex<Vec<Result<DialogOutput, ChatProviderError>>>,
        calls: Mutex<usize>,
        inputs: Mutex<Vec<DialogInput>>,
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
                inputs: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> usize {
            *self.calls.lock().expect("calls")
        }

        fn inputs(&self) -> Vec<DialogInput> {
            self.inputs.lock().expect("inputs").clone()
        }
    }

    impl ChatProvider for SequencedProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn run_dialog<'a>(&'a self, input: DialogInput) -> openplotva_llm::ChatProviderFuture<'a> {
            Box::pin(async move {
                *self.calls.lock().expect("calls") += 1;
                self.inputs.lock().expect("inputs").push(input);
                self.results
                    .lock()
                    .expect("results")
                    .pop()
                    .unwrap_or_else(|| Ok(DialogOutput::default()))
            })
        }
    }
}
