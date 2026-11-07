//! App-level dialog provider construction.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use openplotva_config::AppConfig;
use openplotva_dialog::{ChatStepRequest, PROVIDER_AIFARM, PROVIDER_GENKIT, ToolsMode};
use openplotva_llm::{
    ChatProvider, ChatProviderError, ChatStepFuture, ChatStepProvider,
    aifarm::{AifarmDialogConfig, AifarmDialogProvider, rejection_verdict_for},
    gemini::{GeminiDialogConfig, GeminiDialogProvider, GeminiExplicitCacheConfig},
    retry::{FailureReason, retryable_reason},
    router::{BreakerSet, PoolRegistry, RouterHandle, TriggerState},
    whitecircle::{WhiteCircleClientConfig, WhiteCirclePreToolConfig},
};

use crate::media::{agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config};
use crate::provider_transport::aifarm_discovery_provider_name;
use crate::routed_attempts::{RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext};

/// Shared dialog provider handle.
pub type DialogProviderHandle = Arc<dyn ChatProvider>;

/// Workflow key the dialog worker routes through.
const DIALOG_WORKFLOW_KEY: &str = "dialog";

#[derive(Clone, PartialEq)]
enum DialogClientConfig {
    OpenAi(Box<AifarmDialogConfig>),
    Gemini(GeminiDialogConfig),
}

/// Caches adapters for the selected route, including resolved credentials.
pub struct ChatClientFactory {
    config: AppConfig,
    template: AifarmDialogConfig,
    injected_clients: HashMap<String, DialogProviderHandle>,
    cache: Mutex<HashMap<(i64, i64), (DialogClientConfig, DialogProviderHandle)>>,
    prompt_store: Arc<openplotva_prompts::PromptStore>,
}

impl ChatClientFactory {
    #[must_use]
    pub fn new(
        config: &AppConfig,
        template: AifarmDialogConfig,
        injected_clients: HashMap<String, DialogProviderHandle>,
        prompt_store: Arc<openplotva_prompts::PromptStore>,
    ) -> Self {
        Self {
            config: config.clone(),
            template,
            injected_clients,
            cache: Mutex::new(HashMap::new()),
            prompt_store,
        }
    }

    fn resolve(
        &self,
        attempt: &crate::routed_attempts::RoutedAttempt,
    ) -> Result<DialogProviderHandle, openplotva_llm::retry::ProviderError> {
        if let Some(client) = self.injected_clients.get(&attempt.provider_name) {
            return Ok(Arc::clone(client));
        }
        let config = self.client_config(attempt)?;
        let key = (attempt.provider_id, attempt.model_id);
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((cached, client)) = cache.get(&key)
            && *cached == config
        {
            return Ok(Arc::clone(client));
        }
        let client: DialogProviderHandle = match &config {
            DialogClientConfig::OpenAi(config) => Arc::new(
                AifarmDialogProvider::new(*config.clone())
                    .with_prompt_store(Arc::clone(&self.prompt_store)),
            ),
            DialogClientConfig::Gemini(config) => Arc::new(
                GeminiDialogProvider::new(config.clone())
                    .with_prompt_store(Arc::clone(&self.prompt_store)),
            ),
        };
        cache.insert(key, (config, Arc::clone(&client)));
        Ok(client)
    }

    fn client_config(
        &self,
        attempt: &crate::routed_attempts::RoutedAttempt,
    ) -> Result<DialogClientConfig, openplotva_llm::retry::ProviderError> {
        if attempt.protocol == Some(openplotva_llm::provider_schema::Protocol::Genkit) {
            let (api_key, base_url) =
                crate::provider_transport::gemini_endpoint_for_attempt(&self.config, attempt)?;
            return Ok(DialogClientConfig::Gemini(GeminiDialogConfig {
                api_key,
                base_url,
                model: attempt.model_name.clone(),
                request_timeout: positive_seconds(self.config.llm.dialog.request_timeout_seconds),
                cache: GeminiExplicitCacheConfig::chat_core_multi_turn(),
                ..GeminiDialogConfig::default()
            }));
        }
        let local = local_reasoner_dialog_config_from_app_config(&self.config);
        let mut cfg = if attempt.provider_name == local.provider_name
            && local.provider_name != PROVIDER_AIFARM
        {
            local
        } else {
            self.template.clone()
        };
        cfg.client = crate::provider_transport::openai_client_for_attempt(
            &self.config,
            attempt,
            cfg.client,
        )?;
        cfg.provider_name.clone_from(&attempt.provider_name);
        cfg.model.clone_from(&cfg.client.default_model);
        Ok(DialogClientConfig::OpenAi(Box::new(cfg)))
    }
}

/// Dialog provider that selects its concrete backend per request from the
/// DB-backed routing table (weighted primaries, ordered fallback, trigger-engaged
/// overflow), recording circuit-breaker outcomes. Transport clients come from
/// the [`ChatClientFactory`]: env-built ones by name, admin-created ones built
/// on demand from their provider row.
pub struct RouterChatProvider {
    walker: RoutedAttemptWalker,
    factory: Arc<ChatClientFactory>,
    provider_name: String,
}

impl RouterChatProvider {
    #[must_use]
    pub fn new(
        handle: Arc<RouterHandle>,
        breakers: Arc<BreakerSet>,
        triggers: Arc<TriggerState>,
        pools: Arc<PoolRegistry>,
        factory: Arc<ChatClientFactory>,
    ) -> Self {
        Self {
            walker: RoutedAttemptWalker::new(handle, breakers, triggers, pools),
            factory,
            provider_name: "router".to_owned(),
        }
    }

    /// Extra samples from the same model after a validator rejects its output.
    #[must_use]
    pub fn with_model_output_retries(mut self, retries: usize) -> Self {
        self.walker = self.walker.with_model_output_retries(retries);
        self
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

    fn as_chat_step(&self) -> Option<&dyn ChatStepProvider> {
        Some(self)
    }
}

impl ChatStepProvider for RouterChatProvider {
    fn provider_name(&self) -> &str {
        &self.provider_name
    }

    /// Per-attempt: tool-less clients get `ToolsMode::FinalOnly` instead.
    fn supports_native_tools(&self) -> bool {
        true
    }

    fn run_chat_step<'a>(&'a self, request: ChatStepRequest) -> ChatStepFuture<'a> {
        Box::pin(async move {
            let request_for_attempts = request.clone();
            let required_capabilities = if request.input.multimodal_images.is_empty() {
                Vec::new()
            } else {
                vec!["vision".to_owned()]
            };
            let factory = Arc::clone(&self.factory);
            // Feedback for a re-sample, kept per model: the note tells the model that
            // rejected sample what was wrong with it, and means nothing to a different one.
            let rejected: Arc<Mutex<Option<(i64, String)>>> = Arc::new(Mutex::new(None));
            let result = self
                .walker
                .run(
                    RoutedRequestContext {
                        workflow_key: DIALOG_WORKFLOW_KEY.to_owned(),
                        queue_name: Some("dialog".to_owned()),
                        chat_id: (request.input.context.chat_id != 0)
                            .then_some(request.input.context.chat_id),
                        user_id: (request.input.user.id != 0).then_some(request.input.user.id),
                        thread_id: request.input.context.thread_id,
                        message_id: (request.input.message.id != 0)
                            .then_some(request.input.message.id),
                        deadline: crate::dialog_turn::current_turn_deadline(),
                        suppress_all_attempts_exhausted_admin_report: true,
                        required_capabilities,
                        preferred_target: request.preferred_target.clone(),
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let resolved = factory.resolve(&attempt);
                        let mut step_request = request_for_attempts.clone();
                        let rejected = Arc::clone(&rejected);
                        if let Some(verdict) =
                            rejected.lock().ok().and_then(|last| match last.as_ref() {
                                Some((model_id, verdict)) if *model_id == attempt.model_id => {
                                    Some(verdict.clone())
                                }
                                _ => None,
                            })
                        {
                            step_request.input.resample_verdict = verdict;
                        }
                        let attempt_model_id = attempt.model_id;
                        if !attempt.model_name.trim().is_empty() {
                            step_request.input.model = attempt.model_name.clone();
                        }
                        if let Some(max_tokens) = attempt.overrides.max_tokens
                            && max_tokens > 0
                        {
                            step_request.input.max_output_tokens = max_tokens;
                        }
                        if let Some(enable_thinking) = attempt
                            .overrides
                            .extra
                            .get("enable_thinking")
                            .and_then(serde_json::Value::as_bool)
                        {
                            step_request.input.enable_thinking = Some(enable_thinking);
                        }
                        step_request.input.sampling =
                            dialog_sampling_from_overrides(&attempt.overrides);
                        step_request.input.bad_words = openplotva_llm::aifarm::resolve_bad_words(
                            attempt.overrides.extra.get("bad_words"),
                        );
                        step_request.input.textual_tool_calls = attempt
                            .overrides
                            .extra
                            .get("textual_tool_calls")
                            .and_then(serde_json::Value::as_bool)
                            .unwrap_or(false);
                        async move {
                            let client = match resolved {
                                Ok(client) => client,
                                Err(error) => {
                                    let error: ChatProviderError = Box::new(error);
                                    return Err(error);
                                }
                            };
                            let Some(step_client) = client.as_chat_step() else {
                                // Retryable so the walker moves on to an
                                // echelon that can serve session steps.
                                let error: ChatProviderError =
                                    Box::new(openplotva_llm::retry::ProviderError::new(
                                        client.provider_name(),
                                        openplotva_llm::retry::FailureReason::ProviderUnavailable,
                                        "provider has no chat-step support",
                                    ));
                                return Err(error);
                            };
                            if !step_client.supports_native_tools()
                                && matches!(step_request.tools, ToolsMode::Native(_))
                            {
                                step_request.tools = ToolsMode::FinalOnly;
                            }
                            match step_client.run_chat_step(step_request).await {
                                Ok(mut output) => {
                                    if output.provider.trim().is_empty() {
                                        output.provider = client.provider_name().to_owned();
                                    }
                                    Ok(output)
                                }
                                Err(error) => {
                                    if retryable_reason(error.as_ref())
                                        == Some(FailureReason::ModelOutputRejected)
                                        && let Ok(mut last) = rejected.lock()
                                    {
                                        *last = Some((
                                            attempt_model_id,
                                            rejection_verdict_for(error.as_ref()).to_owned(),
                                        ));
                                    }
                                    Err(error)
                                }
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

/// Build the dialog executor from the shared routing policy.
pub fn router_dialog_provider(
    config: &AppConfig,
    handle: Arc<RouterHandle>,
    breakers: Arc<BreakerSet>,
    triggers: Arc<TriggerState>,
    pools: Arc<PoolRegistry>,
    routing_events: Option<crate::runtime_routing::RoutingEventReporter>,
    prompt_store: Arc<openplotva_prompts::PromptStore>,
) -> DialogProviderHandle {
    let factory = Arc::new(ChatClientFactory::new(
        config,
        aifarm_dialog_config_from_app_config(config),
        HashMap::new(),
        prompt_store,
    ));
    // A negative value is a config slip, not a way to disable the re-sampling;
    // zero disables it on purpose.
    let configured_retries = config.llm.dialog.model_output_retries;
    let model_output_retries = if configured_retries < 0 {
        openplotva_config::DEFAULT_DIALOG_MODEL_OUTPUT_RETRIES
    } else {
        configured_retries
    };
    let provider = RouterChatProvider::new(handle, breakers, triggers, pools, factory)
        .with_model_output_retries(usize::try_from(model_output_retries).unwrap_or(0));
    let provider = match routing_events {
        Some(reporter) => provider.with_routing_event_reporter(reporter),
        None => provider,
    };
    Arc::new(provider)
}

/// Provider name for the direct OpenAI-compatible VRAM Cloud endpoint.
pub const VRAM_CLOUD_PROVIDER_NAME: &str = "vram-cloud";

fn local_reasoner_dialog_config_from_app_config(config: &AppConfig) -> AifarmDialogConfig {
    let spec = crate::agent_runtime::local_reasoner_named_provider_config(config);
    let mut cfg = aifarm_dialog_config_from_app_config(config);
    cfg.provider_name = aifarm_discovery_provider_name(&spec.discovery_service_name, config);
    cfg.client = agent_client_config_from_named_provider(config, &spec);
    if cfg.client.api_key.trim().is_empty() {
        cfg.client.api_key = config.llm.dialog.api_key.clone();
    }
    cfg.model = spec.model.clone();
    cfg.max_tokens = spec.max_tokens;
    cfg.temperature = spec.temperature;
    cfg.enable_thinking = spec.enable_thinking;
    cfg.include_reasoning = spec.include_reasoning.or(Some(false));
    cfg.with_defaults()
}

/// Sampling a route sets for its turns, from the merged model and assignment config.
/// A value outside its valid range is ignored instead of failing the request.
fn dialog_sampling_from_overrides(
    overrides: &openplotva_llm::router::InferenceOverrides,
) -> openplotva_dialog::DialogSampling {
    let extra = |key: &str| overrides.extra.get(key).and_then(serde_json::Value::as_f64);
    let within = |value: Option<f64>, valid: fn(f64) -> bool| value.filter(|v| valid(*v));
    openplotva_dialog::DialogSampling {
        temperature: within(overrides.temperature, |v| (0.0..=2.0).contains(&v)),
        top_p: within(extra("top_p"), |v| v > 0.0 && v <= 1.0),
        top_k: within(extra("top_k"), |v| v >= 0.0),
        min_p: within(extra("min_p"), |v| (0.0..1.0).contains(&v)),
        repeat_penalty: within(overrides.repeat_penalty, |v| v > 0.0),
        frequency_penalty: within(overrides.frequency_penalty, |v| (-2.0..=2.0).contains(&v)),
        presence_penalty: within(overrides.presence_penalty, |v| (-2.0..=2.0).contains(&v)),
    }
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

    use openplotva_dialog::{DialogInput, DialogOutput};
    use openplotva_llm::{
        ChatProviderError,
        retry::{FailureReason, ProviderError},
    };
    use openplotva_storage::llm_routing::{
        AssignmentRecord, ModelRecord, PoolRecord, ProviderRecord, RoutingSnapshot, WorkflowRecord,
    };
    use serde_json::json;

    use super::*;

    #[test]
    fn chat_client_factory_keeps_application_prompt_store() {
        let prompts = Arc::new(openplotva_prompts::PromptStore::load().expect("prompt store"));
        let config = AppConfig::from_raw(Default::default()).expect("config");
        let factory = ChatClientFactory::new(
            &config,
            AifarmDialogConfig::default(),
            HashMap::new(),
            Arc::clone(&prompts),
        );
        assert!(Arc::ptr_eq(&factory.prompt_store, &prompts));
    }

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
    fn local_reasoner_dialog_config_targets_ninfer_with_tools() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");

        let cfg = local_reasoner_dialog_config_from_app_config(&config);

        assert_eq!(cfg.provider_name, "aifarm-ninfer-gpu2");
        assert_eq!(cfg.client.service_name, "llm-openai-qwen38-ninfer");
        assert_eq!(cfg.client.endpoint_name, "chat_completions");
        assert_eq!(cfg.client.runtime_hint, "ninfer");
        assert_eq!(cfg.model, "qwen3.8-27b");
        assert_eq!(cfg.client.default_model, "qwen3.8-27b");
        assert_eq!(cfg.include_reasoning, Some(false));
        assert_eq!(cfg.enable_thinking, Some(false));
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
        );
        let provider = router_provider_for_test(
            RoutingSnapshot::default(),
            default_provider.clone(),
            HashMap::new(),
            reporter.clone(),
        );

        let mut request = default_step_request();
        request.input.user.id = 42;
        request.input.user.full_name = "Alice Example".to_owned();
        let error = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(request)
            .await
            .err();

        assert!(error.is_some());
        assert_eq!(default_provider.calls(), 0);
        let events = reporter.buffer().routing_events(10);
        assert_eq!(events[0].event_type, "route_unavailable");
        assert_eq!(events[0].workflow_key, "dialog");
        assert_eq!(events[0].user_id, Some(42));
    }

    #[tokio::test(start_paused = true)]
    async fn interactive_router_waits_for_a_busy_gpu_until_the_turn_deadline() {
        let mut snapshot = routed_dialog_snapshot();
        snapshot.models[0].pool_id = Some(1);
        snapshot.pools.push(PoolRecord {
            id: 1,
            name: "shared-gpu".to_owned(),
            max_concurrency: Some(1),
            description: None,
            config: json!({}),
        });
        let table = crate::model_routing::build_routing_table(&snapshot);
        let handle = RouterHandle::new(table.clone());
        let pools = Arc::new(PoolRegistry::new());
        pools.apply(&table.pool_specs());
        let _busy = pools.try_acquire(Some(1)).expect("occupy the GPU pool");
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let provider = RouterChatProvider::new(
            Arc::clone(&handle),
            Arc::new(BreakerSet::new()),
            Arc::new(TriggerState::new()),
            pools,
            test_factory(HashMap::new()),
        )
        .with_routing_event_reporter(reporter.clone());
        let started = tokio::time::Instant::now();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);

        let error = crate::dialog_turn::TURN_DEADLINE
            .scope(
                Some(deadline),
                provider.run_chat_step(default_step_request()),
            )
            .await
            .expect_err("a permanently busy interactive route fails at the turn deadline");

        assert!(error.to_string().contains("capacity unavailable"));
        let waited = started.elapsed();
        assert!(
            waited > std::time::Duration::from_millis(150)
                && waited <= std::time::Duration::from_millis(200),
            "{waited:?}"
        );
        assert!(
            reporter
                .buffer()
                .routing_events(8)
                .iter()
                .any(|event| event.event_type == "capacity_wait_timeout")
        );
    }

    #[tokio::test]
    async fn router_chat_provider_emits_no_candidates_for_empty_route() {
        let default_provider = Arc::new(SequencedProvider::new("aifarm", vec![]));
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let mut snapshot = routed_dialog_snapshot();
        snapshot.providers[0].enabled = false;
        let provider = router_provider_for_test(
            snapshot,
            default_provider.clone(),
            HashMap::new(),
            reporter.clone(),
        );

        let error = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .err();

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
        );
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            reporter.clone(),
        );

        let error = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .err();

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
        let handle = RouterHandle::new(crate::model_routing::build_routing_table(&snapshot));
        let provider = RouterChatProvider::new(
            Arc::clone(&handle),
            Arc::new(BreakerSet::new()),
            Arc::clone(&triggers),
            Arc::new(PoolRegistry::new()),
            test_factory(clients),
        )
        .with_routing_event_reporter(reporter.clone());

        let error = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .err();

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
            ),
        );

        let output = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .expect("dialog output");

        assert_eq!(output.text, "ok");
        let inputs = routed_provider.inputs();
        assert_eq!(inputs[0].model, "openrouter/provider/model");
        assert_eq!(inputs[0].max_output_tokens, 123);
    }

    #[tokio::test]
    async fn route_sampling_reaches_the_turn_and_ignores_invalid_values() {
        let run = |config: serde_json::Value| async move {
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
            snapshot.models[0].config = config;
            let provider = router_provider_for_test(
                snapshot,
                default_provider,
                clients,
                crate::runtime_routing::RoutingEventReporter::new(
                    crate::runtime_routing::RoutingEventBuffer::new(8),
                    None,
                    None,
                ),
            );
            provider
                .as_chat_step()
                .expect("step seam")
                .run_chat_step(default_step_request())
                .await
                .expect("dialog output");
            routed_provider.inputs()[0].sampling.clone()
        };

        let card = run(json!({
            "temperature": 0.6, "top_p": 0.9, "top_k": 64, "min_p": 0.05, "repeat_penalty": 1.1
        }))
        .await;
        assert_eq!(
            card,
            openplotva_dialog::DialogSampling {
                temperature: Some(0.6),
                top_p: Some(0.9),
                top_k: Some(64.0),
                min_p: Some(0.05),
                repeat_penalty: Some(1.1),
                ..openplotva_dialog::DialogSampling::default()
            }
        );
        assert!(run(json!({})).await.is_empty());
        let invalid = run(json!({
            "temperature": 9.0, "top_p": 0.0, "top_k": -1, "min_p": 1.0, "repeat_penalty": 0.0
        }))
        .await;
        assert!(invalid.is_empty(), "{invalid:?}");
    }

    #[tokio::test]
    async fn model_config_lets_its_model_execute_textual_tool_calls() {
        let run = |config: serde_json::Value| async move {
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
            snapshot.models[0].config = config;
            let provider = router_provider_for_test(
                snapshot,
                default_provider,
                clients,
                crate::runtime_routing::RoutingEventReporter::new(
                    crate::runtime_routing::RoutingEventBuffer::new(8),
                    None,
                    None,
                ),
            );
            provider
                .as_chat_step()
                .expect("step seam")
                .run_chat_step(default_step_request())
                .await
                .expect("dialog output");
            routed_provider.inputs()[0].textual_tool_calls
        };

        assert!(run(json!({ "textual_tool_calls": true })).await);
        assert!(!run(json!({})).await);
    }

    #[tokio::test]
    async fn model_config_selects_the_bad_words_preset() {
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
        snapshot.models[0].config = json!({ "bad_words": "dialog_scaffolding" });
        let provider = router_provider_for_test(
            snapshot,
            default_provider,
            clients,
            crate::runtime_routing::RoutingEventReporter::new(
                crate::runtime_routing::RoutingEventBuffer::new(8),
                None,
                None,
            ),
        );

        provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .expect("dialog output");

        let inputs = routed_provider.inputs();
        assert!(
            inputs[0].bad_words.iter().any(|word| word == "<message"),
            "the model's own config decides whether its engine gets the control"
        );
    }

    #[tokio::test]
    async fn a_re_sample_carries_the_verdict_that_rejected_the_previous_sample() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let rejection: ChatProviderError =
            Box::new(openplotva_llm::aifarm::AifarmDialogError::FinalAnswerContextLeak);
        let routed_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![
                Err(rejection),
                Ok(DialogOutput {
                    provider: "aifarm".to_owned(),
                    answer: "ладно, по делу".to_owned(),
                    ..DialogOutput::default()
                }),
            ],
        ));
        let routed_provider_dyn: DialogProviderHandle = routed_provider.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_provider_dyn);
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            crate::runtime_routing::RoutingEventReporter::new(
                crate::runtime_routing::RoutingEventBuffer::new(8),
                None,
                None,
            ),
        )
        .with_model_output_retries(1);

        let output = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .expect("dialog output");

        assert_eq!(output.text, "ладно, по делу");
        let inputs = routed_provider.inputs();
        assert_eq!(inputs.len(), 2, "the rejected sample must be re-sampled");
        assert!(
            inputs[0].resample_verdict.is_empty(),
            "the first sample has nothing to be told about"
        );
        assert_eq!(
            inputs[1].resample_verdict, "context_leak",
            "the re-sample must carry what the guard rejected"
        );
    }

    #[tokio::test]
    async fn router_chat_provider_honors_turn_deadline_task_local() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let routed_provider = Arc::new(SequencedProvider::new(
            "aifarm",
            vec![Ok(DialogOutput {
                provider: "aifarm".to_owned(),
                answer: "should not run".to_owned(),
                ..DialogOutput::default()
            })],
        ));
        let routed_provider_dyn: DialogProviderHandle = routed_provider.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_provider_dyn);
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            crate::runtime_routing::RoutingEventReporter::new(
                crate::runtime_routing::RoutingEventBuffer::new(8),
                None,
                None,
            ),
        );

        let expired = std::time::Instant::now();
        let error = crate::dialog_turn::TURN_DEADLINE
            .scope(
                Some(expired),
                provider
                    .as_chat_step()
                    .expect("step seam")
                    .run_chat_step(default_step_request()),
            )
            .await
            .err();

        assert!(error.is_some(), "expired deadline must stop the walker");
        assert_eq!(routed_provider.calls(), 0);

        let output = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .expect("dialog output without a deadline");
        assert_eq!(output.text, "should not run");
        assert_eq!(routed_provider.calls(), 1);
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
            ),
        );

        let output = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(default_step_request())
            .await
            .expect("dialog output");

        assert_eq!(output.text, "ok");
        let inputs = routed_provider.inputs();
        assert_eq!(inputs[0].max_output_tokens, 4096);
        assert_eq!(inputs[0].enable_thinking, Some(false));
    }

    #[test]
    fn vram_cloud_dialog_config_uses_standard_dialog_ceiling() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_aifarm_max_tokens: Some("1024".to_owned()),
            dialog_aifarm_pool_models: Some("qwen3.6-27b".to_owned()),
            dialog_aifarm_pool_base_urls: Some("https://pool.test/v1".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let factory = ChatClientFactory::new(
            &config,
            aifarm_dialog_config_from_app_config(&config),
            HashMap::new(),
            Arc::new(openplotva_prompts::PromptStore::load().expect("prompts")),
        );
        let mut attempt = attempt_for(1, VRAM_CLOUD_PROVIDER_NAME);
        attempt.provider_endpoint = Some("https://pool.test/v1".into());
        let DialogClientConfig::OpenAi(cfg) = factory.client_config(&attempt).expect("config")
        else {
            panic!("openai")
        };

        assert_eq!(cfg.provider_name, VRAM_CLOUD_PROVIDER_NAME);
        assert_eq!(cfg.max_tokens, 1024);
        assert_eq!(cfg.enable_thinking, Some(false));
    }

    #[tokio::test]
    #[ignore = "live OpenRouter routed dialog smoke"]
    async fn live_genkit_openrouter_dialog_smoke_completes_minimal_prompt()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let openrouter_key = required_env("OPENROUTER_KEY")?;
        let model = std::env::var("OPENPLOTVA_OPENROUTER_SMOKE_MODEL")
            .unwrap_or_else(|_| "openai/gpt-4o-mini".to_owned());
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_provider: Some("genkit".to_owned()),
            dialog_model: Some(format!("openrouter/{model}")),
            openrouter_key: Some(openrouter_key),
            dialog_request_timeout_seconds: Some("60".to_owned()),
            ..openplotva_config::RawConfig::default()
        })?;
        let prompts = Arc::new(openplotva_prompts::PromptStore::load()?);
        let factory = ChatClientFactory::new(
            &config,
            aifarm_dialog_config_from_app_config(&config),
            HashMap::new(),
            prompts,
        );
        let mut attempt = attempt_for(1, "openrouter");
        attempt.model_name = model;
        let provider = factory.resolve(&attempt)?;

        let output = provider
            .as_chat_step()
            .expect("step seam")
            .run_chat_step(openplotva_dialog::ChatStepRequest {
                required_tool: None,
                preferred_target: None,
                input: live_dialog_smoke_input(),
                transcript: Vec::new(),
                tools: openplotva_dialog::ToolsMode::Disabled,
                iteration: 1,
            })
            .await?;

        assert!(
            !output.text.trim().is_empty(),
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

    fn router_provider_for_test(
        snapshot: RoutingSnapshot,
        _default_provider: Arc<SequencedProvider>,
        clients: HashMap<String, DialogProviderHandle>,
        reporter: crate::runtime_routing::RoutingEventReporter,
    ) -> RouterChatProvider {
        let handle = RouterHandle::new(crate::model_routing::build_routing_table(&snapshot));
        RouterChatProvider::new(
            Arc::clone(&handle),
            Arc::new(BreakerSet::new()),
            Arc::new(TriggerState::new()),
            Arc::new(PoolRegistry::new()),
            test_factory(clients),
        )
        .with_routing_event_reporter(reporter)
    }

    fn test_factory(clients: HashMap<String, DialogProviderHandle>) -> Arc<ChatClientFactory> {
        Arc::new(ChatClientFactory::new(
            &AppConfig::from_raw(Default::default()).expect("config"),
            AifarmDialogConfig::default(),
            clients,
            Arc::new(openplotva_prompts::PromptStore::load().expect("prompt store")),
        ))
    }

    fn attempt_for(provider_id: i64, provider_name: &str) -> crate::routed_attempts::RoutedAttempt {
        crate::routed_attempts::RoutedAttempt {
            protocol: Some(openplotva_llm::provider_schema::Protocol::OpenAiCompat),
            provider_id,
            model_id: 10,
            provider_name: provider_name.to_owned(),
            model_name: "db/model".to_owned(),
            provider_runtime_hint: None,
            provider_endpoint: None,
            discovery_service_name: None,
            discovery_endpoint_name: None,
            provider_api_key_ref: None,
            provider_api_key_encrypted: None,
            model_base_url: None,
            embedding_dim: None,
            provider_config: json!({}),
            model_config: json!({}),
            overrides: Default::default(),
            variant: None,
        }
    }

    #[test]
    fn factory_caches_the_captured_route_and_refreshes_when_it_changes() {
        let factory = test_factory(HashMap::new());
        let mut attempt = attempt_for(5, "custom");
        attempt.provider_endpoint = Some("https://gateway.test/v1".into());
        let first = factory.resolve(&attempt).expect("first");
        assert!(Arc::ptr_eq(
            &first,
            &factory.resolve(&attempt).expect("cached")
        ));
        attempt.provider_runtime_hint = Some("mlx".into());
        assert!(!Arc::ptr_eq(
            &first,
            &factory.resolve(&attempt).expect("changed")
        ));
        attempt.provider_config = json!({"supports_cache_control": false});
        let DialogClientConfig::OpenAi(cfg) = factory.client_config(&attempt).expect("config")
        else {
            panic!("openai")
        };
        assert!(!cfg.client.supports_cache_control);
        assert_eq!(
            cfg.client.direct_url,
            "https://gateway.test/v1/chat/completions"
        );
    }

    #[test]
    fn factory_rejects_unsupported_protocols_before_using_a_transport() {
        let factory = test_factory(HashMap::new());
        let mut attempt = attempt_for(5, "custom");
        attempt.protocol = Some(openplotva_llm::provider_schema::Protocol::AceStep);
        assert_eq!(
            retryable_reason(&factory.resolve(&attempt).err().expect("unsupported")),
            Some(FailureReason::ProviderUnavailable)
        );
    }

    #[test]
    fn factory_respects_explicit_openai_protocol_on_a_google_named_provider() {
        let factory = test_factory(HashMap::new());
        let mut attempt = attempt_for(5, "genkit");
        attempt.provider_endpoint = Some("https://custom.test/v1".into());
        let DialogClientConfig::OpenAi(cfg) = factory.client_config(&attempt).expect("config")
        else {
            panic!("openai")
        };
        assert_eq!(
            cfg.client.direct_url,
            "https://custom.test/v1/chat/completions"
        );
    }

    fn routed_dialog_snapshot() -> RoutingSnapshot {
        RoutingSnapshot {
            providers: vec![ProviderRecord {
                id: 1,
                name: "aifarm".to_owned(),
                kind: "chat".to_owned(),
                protocol: None,
                runtime_hint: None,
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
                pool_id: None,
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
                lane_priority: 0,
                fallback_reserve_ms: 30_000,
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
            pools: vec![],
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

        fn as_chat_step(&self) -> Option<&dyn ChatStepProvider> {
            Some(self)
        }
    }

    impl ChatStepProvider for SequencedProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn supports_native_tools(&self) -> bool {
            true
        }

        fn run_chat_step<'a>(
            &'a self,
            request: openplotva_dialog::ChatStepRequest,
        ) -> openplotva_llm::ChatStepFuture<'a> {
            Box::pin(async move {
                *self.calls.lock().expect("calls") += 1;
                self.inputs.lock().expect("inputs").push(request.input);
                match self
                    .results
                    .lock()
                    .expect("results")
                    .pop()
                    .unwrap_or_else(|| Ok(DialogOutput::default()))
                {
                    Ok(output) => Ok(openplotva_dialog::ChatStepOutput {
                        provider: output.provider.clone(),
                        text: if output.answer.trim().is_empty() {
                            output.response.clone()
                        } else {
                            output.answer.clone()
                        },
                        ..openplotva_dialog::ChatStepOutput::default()
                    }),
                    Err(error) => Err(error),
                }
            })
        }
    }

    fn default_step_request() -> openplotva_dialog::ChatStepRequest {
        openplotva_dialog::ChatStepRequest {
            required_tool: None,
            preferred_target: None,
            input: DialogInput::default(),
            transcript: Vec::new(),
            tools: openplotva_dialog::ToolsMode::Disabled,
            iteration: 1,
        }
    }

    struct StepStubProvider {
        name: &'static str,
        native_tools: bool,
        received: Mutex<Vec<openplotva_dialog::ToolsMode>>,
    }

    impl StepStubProvider {
        fn new(name: &'static str, native_tools: bool) -> Self {
            Self {
                name,
                native_tools,
                received: Mutex::new(Vec::new()),
            }
        }

        fn received(&self) -> Vec<openplotva_dialog::ToolsMode> {
            self.received.lock().expect("received").clone()
        }
    }

    impl ChatProvider for StepStubProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn as_chat_step(&self) -> Option<&dyn ChatStepProvider> {
            Some(self)
        }
    }

    impl ChatStepProvider for StepStubProvider {
        fn provider_name(&self) -> &str {
            self.name
        }

        fn supports_native_tools(&self) -> bool {
            self.native_tools
        }

        fn run_chat_step<'a>(
            &'a self,
            request: ChatStepRequest,
        ) -> openplotva_llm::ChatStepFuture<'a> {
            Box::pin(async move {
                self.received.lock().expect("received").push(request.tools);
                Ok(openplotva_dialog::ChatStepOutput {
                    provider: self.name.to_owned(),
                    text: "ok".to_owned(),
                    ..openplotva_dialog::ChatStepOutput::default()
                })
            })
        }
    }

    fn step_request() -> ChatStepRequest {
        ChatStepRequest {
            required_tool: None,
            preferred_target: None,
            input: DialogInput::default(),
            transcript: Vec::new(),
            tools: ToolsMode::Native(vec![json!({"type": "function"})]),
            iteration: 1,
        }
    }

    #[tokio::test]
    async fn router_chat_step_passes_native_tools_to_capable_clients() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let stub = Arc::new(StepStubProvider::new("aifarm", true));
        let stub_dyn: DialogProviderHandle = stub.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), stub_dyn);
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            reporter,
        );

        let output = provider
            .run_chat_step(step_request())
            .await
            .expect("routed step");

        assert_eq!(output.text, "ok");
        assert!(matches!(stub.received()[0], ToolsMode::Native(_)));
    }

    #[tokio::test]
    async fn router_chat_step_downgrades_tools_for_tool_less_clients() {
        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let stub = Arc::new(StepStubProvider::new("aifarm", false));
        let stub_dyn: DialogProviderHandle = stub.clone();
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), stub_dyn);
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            reporter,
        );

        let output = provider
            .run_chat_step(step_request())
            .await
            .expect("routed step");

        assert_eq!(output.text, "ok");
        assert_eq!(stub.received(), vec![ToolsMode::FinalOnly]);
    }

    #[tokio::test]
    async fn router_chat_step_fails_retryably_without_step_support() {
        struct StepLessProvider;

        impl ChatProvider for StepLessProvider {
            fn provider_name(&self) -> &str {
                "aifarm"
            }
        }

        let default_provider = Arc::new(SequencedProvider::new("unused", vec![]));
        let routed_dyn: DialogProviderHandle = Arc::new(StepLessProvider);
        let mut clients = HashMap::new();
        clients.insert("aifarm".to_owned(), routed_dyn);
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let provider = router_provider_for_test(
            routed_dialog_snapshot(),
            default_provider,
            clients,
            reporter,
        );

        let error = provider
            .run_chat_step(step_request())
            .await
            .expect_err("no step support");

        assert_eq!(
            retryable_reason(error.as_ref()),
            Some(FailureReason::ProviderUnavailable),
            "the walker (and job retry) must treat step-less providers as retryable"
        );
    }
}
