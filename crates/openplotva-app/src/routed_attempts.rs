use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use openplotva_llm::retry::FailureReason;
use openplotva_llm::router::{
    BreakerLiveness, BreakerSet, InferenceOverrides, RouterHandle, TriggerState, WorkflowRoute,
    select,
};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct RoutedAttemptWalker {
    handle: Arc<RouterHandle>,
    breakers: Arc<BreakerSet>,
    triggers: Arc<TriggerState>,
    reporter: Option<crate::runtime_routing::RoutingEventReporter>,
}

impl RoutedAttemptWalker {
    #[must_use]
    pub fn new(
        handle: Arc<RouterHandle>,
        breakers: Arc<BreakerSet>,
        triggers: Arc<TriggerState>,
    ) -> Self {
        Self {
            handle,
            breakers,
            triggers,
            reporter: None,
        }
    }

    #[must_use]
    pub fn with_reporter(mut self, reporter: crate::runtime_routing::RoutingEventReporter) -> Self {
        self.reporter = Some(reporter);
        self
    }

    #[must_use]
    pub fn with_reporter_opt(
        mut self,
        reporter: Option<crate::runtime_routing::RoutingEventReporter>,
    ) -> Self {
        self.reporter = reporter;
        self
    }

    pub async fn run<T, E, F, Fut, Retry>(
        &self,
        context: RoutedRequestContext,
        mut execute: F,
        retryable: Retry,
    ) -> Result<T, RoutedAttemptRunError<E>>
    where
        E: Send + Sync + 'static,
        F: FnMut(RoutedAttempt) -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        Retry: Fn(&E) -> Option<FailureReason> + Send,
    {
        let table = self.handle.snapshot();
        let Some(route) = table.resolve(&context.workflow_key, context.vip) else {
            self.record_event(routing_event(
                "route_unavailable",
                &context,
                None,
                None,
                "workflow route is unavailable",
                json!({ "reason": "missing_route" }),
            ));
            return Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                "workflow route is unavailable",
            )));
        };

        let selection_now = Instant::now();
        let attempts = {
            let liveness = BreakerLiveness::new(&self.breakers, selection_now);
            let mut rng = rand::rng();
            select(route, &liveness, self.triggers.as_ref(), &mut rng)
        };
        if attempts.is_empty() {
            self.record_event(routing_event(
                "no_candidates",
                &context,
                None,
                None,
                "workflow route has no eligible candidates",
                json!({ "reason": "zero_selected_attempts" }),
            ));
            return Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                "workflow route has no eligible candidates",
            )));
        }
        if attempts.iter().all(|attempt| {
            !self
                .breakers
                .is_live_at(attempt.provider, attempt.model, selection_now)
        }) {
            self.record_event(routing_event(
                "circuit_open_exhaustion",
                &context,
                None,
                None,
                "all routed attempts are inside circuit cooldown",
                json!({ "attempts": attempts.len() }),
            ));
        }

        let started = Instant::now();
        let mut last_error = None;
        let mut last_provider_model = None;
        let mut failed_attempts = 0usize;
        let mut last_reason = None;
        for attempt in attempts {
            if started.elapsed() > route.retry.wall_clock {
                break;
            }
            let provider = table.provider(attempt.provider);
            let model = table.model(attempt.model);
            let routed = RoutedAttempt {
                provider_id: attempt.provider,
                model_id: attempt.model,
                provider_name: provider.map(|row| row.name.clone()).unwrap_or_default(),
                model_name: model.map(|row| row.model_name.clone()).unwrap_or_default(),
                provider_endpoint: provider.and_then(|row| row.endpoint.clone()),
                discovery_service_name: provider.and_then(|row| row.discovery_service_name.clone()),
                discovery_endpoint_name: provider
                    .and_then(|row| row.discovery_endpoint_name.clone()),
                model_base_url: model.and_then(|row| row.base_url.clone()),
                embedding_dim: model.and_then(|row| row.embedding_dim),
                provider_config: provider
                    .map(|row| row.config.clone())
                    .unwrap_or_else(|| json!({})),
                model_config: model
                    .map(|row| row.config.clone())
                    .unwrap_or_else(|| json!({})),
                overrides: model
                    .map(|row| {
                        merge_model_and_assignment_overrides(&row.config, &attempt.overrides)
                    })
                    .unwrap_or_else(|| attempt.overrides.clone()),
                variant: attempt.variant.clone(),
            };
            last_provider_model = Some((routed.provider_id, routed.model_id));

            match execute(routed).await {
                Ok(output) => {
                    self.breakers
                        .record_success(attempt.provider, attempt.model);
                    return Ok(output);
                }
                Err(error) => {
                    let Some(reason) = retryable(&error) else {
                        return Err(RoutedAttemptRunError::Attempt(error));
                    };
                    self.breakers
                        .record_failure(attempt.provider, attempt.model, attempt.breaker);
                    if reason == FailureReason::CapacityUnavailable
                        && let Some(cooldown) =
                            provider_capacity_cooldown(route, attempt.provider, attempt.model)
                    {
                        self.triggers.mark_capacity_unavailable(
                            attempt.provider,
                            attempt.model,
                            cooldown,
                        );
                        self.record_event(routing_event(
                            "capacity_unavailable",
                            &context,
                            Some(attempt.provider),
                            Some(attempt.model),
                            "provider capacity unavailable",
                            json!({
                                "cooldown_ms": cooldown.as_millis(),
                                "retryable_reason": reason.as_str(),
                            }),
                        ));
                    }
                    failed_attempts += 1;
                    last_reason = Some(reason.as_str().to_owned());
                    last_error = Some(error);
                }
            }
        }

        let (provider_id, model_id) = last_provider_model.unwrap_or_default();
        self.record_event(routing_event(
            "all_attempts_exhausted",
            &context,
            (provider_id != 0).then_some(provider_id),
            (model_id != 0).then_some(model_id),
            "routed attempts exhausted",
            json!({
                "failed_attempts": failed_attempts,
                "last_retryable_reason": last_reason,
            }),
        ));
        match last_error {
            Some(error) => Err(RoutedAttemptRunError::Attempt(error)),
            None => Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                "routed attempts exhausted",
            ))),
        }
    }

    fn record_event(&self, event: crate::runtime_routing::RoutingEvent) {
        if let Some(reporter) = &self.reporter {
            reporter.record(event);
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RoutedRequestContext {
    pub workflow_key: String,
    pub queue_name: Option<String>,
    pub job_id: Option<i64>,
    pub chat_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub vip: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutedAttempt {
    pub provider_id: i64,
    pub model_id: i64,
    pub provider_name: String,
    pub model_name: String,
    pub provider_endpoint: Option<String>,
    pub discovery_service_name: Option<String>,
    pub discovery_endpoint_name: Option<String>,
    pub model_base_url: Option<String>,
    pub embedding_dim: Option<i32>,
    pub provider_config: Value,
    pub model_config: Value,
    pub overrides: InferenceOverrides,
    pub variant: Option<String>,
}

#[derive(Debug)]
pub enum RoutedAttemptRunError<E> {
    Routing(RoutedRoutingError),
    Attempt(E),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedRoutingError {
    message: String,
}

impl RoutedRoutingError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for RoutedRoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RoutedRoutingError {}

fn routing_event(
    event_type: &str,
    context: &RoutedRequestContext,
    provider_id: Option<i64>,
    model_id: Option<i64>,
    summary: &str,
    detail: Value,
) -> crate::runtime_routing::RoutingEvent {
    crate::runtime_routing::RoutingEvent {
        severity: "error".to_owned(),
        event_type: event_type.to_owned(),
        workflow_key: context.workflow_key.clone(),
        provider_id,
        model_id,
        queue_name: context.queue_name.clone(),
        job_id: context.job_id,
        chat_id: context.chat_id,
        thread_id: context.thread_id,
        message_id: context.message_id,
        dedupe_key: format!(
            "{event_type}:{}:{}:{}",
            context.workflow_key,
            provider_id.unwrap_or_default(),
            model_id.unwrap_or_default()
        ),
        summary: summary.to_owned(),
        detail,
    }
}

fn provider_capacity_cooldown(
    route: &WorkflowRoute,
    provider: i64,
    model: i64,
) -> Option<std::time::Duration> {
    route
        .triggers
        .iter()
        .find_map(|trigger| match &trigger.condition {
            openplotva_llm::router::TriggerCondition::ProviderCapacity {
                provider: trigger_provider,
                model: trigger_model,
                cooldown,
            } if *trigger_provider == provider && *trigger_model == model => Some(*cooldown),
            _ => None,
        })
}

fn merge_model_and_assignment_overrides(
    model_config: &Value,
    assignment_overrides: &InferenceOverrides,
) -> InferenceOverrides {
    let mut merged = model_config.as_object().cloned().unwrap_or_default();
    if let Some(assignment) = assignment_overrides.extra.as_object() {
        for (key, value) in assignment {
            merged.insert(key.clone(), value.clone());
        }
    }
    overrides_from_json(&Value::Object(merged))
}

fn overrides_from_json(value: &Value) -> InferenceOverrides {
    let object = value.as_object();
    let get_f64 = |key: &str| object.and_then(|map| map.get(key)).and_then(Value::as_f64);
    let get_i32 = |key: &str| {
        object
            .and_then(|map| map.get(key))
            .and_then(Value::as_i64)
            .map(|n| n as i32)
    };
    InferenceOverrides {
        temperature: get_f64("temperature"),
        max_tokens: get_i32("max_tokens"),
        frequency_penalty: get_f64("frequency_penalty"),
        presence_penalty: get_f64("presence_penalty"),
        repeat_penalty: get_f64("repeat_penalty"),
        extra: value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use openplotva_llm::router::{BreakerConfig, BreakerSet, RouterHandle, TriggerState};
    use openplotva_storage::llm_routing::{
        AssignmentRecord, ModelRecord, ProviderRecord, RoutingSnapshot, WorkflowRecord,
    };
    use serde_json::json;

    use super::*;

    fn provider(id: i64, name: &str) -> ProviderRecord {
        ProviderRecord {
            id,
            name: name.to_owned(),
            kind: "chat".to_owned(),
            endpoint: None,
            discovery_service_name: None,
            discovery_endpoint_name: None,
            api_key_ref: Some("REF".to_owned()),
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        }
    }

    fn model(id: i64, provider_id: i64, config: serde_json::Value) -> ModelRecord {
        ModelRecord {
            id,
            provider_id,
            model_name: "db/model".to_owned(),
            display_name: None,
            base_url: None,
            capabilities: vec!["chat".to_owned()],
            embedding_dim: None,
            enabled: true,
            config,
        }
    }

    fn assignment(overrides: serde_json::Value) -> AssignmentRecord {
        AssignmentRecord {
            id: 100,
            workflow_key: "dialog".to_owned(),
            scope: "global".to_owned(),
            role: "primary".to_owned(),
            provider_model_id: 10,
            weight: Some(100),
            fallback_order: None,
            canary_percent: None,
            enabled: true,
            inference_overrides: overrides,
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        }
    }

    fn snapshot(
        model_config: serde_json::Value,
        assignment_overrides: serde_json::Value,
    ) -> RoutingSnapshot {
        RoutingSnapshot {
            providers: vec![provider(1, "aifarm")],
            models: vec![model(10, 1, model_config)],
            workflows: vec![WorkflowRecord {
                key: "dialog".to_owned(),
                kind: "chat".to_owned(),
                full_routing: true,
                retry_max_hops: 1,
                retry_wall_ms: 60_000,
                enabled: true,
            }],
            assignments: vec![assignment(assignment_overrides)],
            triggers: vec![],
        }
    }

    fn walker_for(snapshot: RoutingSnapshot) -> RoutedAttemptWalker {
        walker_with_breakers(snapshot, Arc::new(BreakerSet::new()))
    }

    fn walker_with_breakers(
        snapshot: RoutingSnapshot,
        breakers: Arc<BreakerSet>,
    ) -> RoutedAttemptWalker {
        RoutedAttemptWalker::new(
            RouterHandle::new(crate::model_routing::build_routing_table(&snapshot)),
            breakers,
            Arc::new(TriggerState::new()),
        )
    }

    #[tokio::test]
    async fn walker_emits_route_unavailable_without_calling_adapter() {
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            std::time::Duration::from_secs(600),
        );
        let walker = walker_for(RoutingSnapshot::default()).with_reporter(reporter.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = Arc::clone(&calls);

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    queue_name: Some("dialog".to_owned()),
                    ..RoutedRequestContext::default()
                },
                move |_attempt| {
                    let call_count = Arc::clone(&call_count);
                    async move {
                        call_count.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, std::io::Error>("unused")
                    }
                },
                |_error| None,
            )
            .await;

        assert!(matches!(result, Err(RoutedAttemptRunError::Routing(_))));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        let events = reporter.buffer().routing_events(10);
        assert_eq!(events[0].event_type, "route_unavailable");
    }

    #[tokio::test]
    async fn walker_merges_model_config_before_assignment_overrides() {
        let walker = walker_for(snapshot(
            json!({
                "temperature": 0.7,
                "max_tokens": 100,
                "enable_thinking": true
            }),
            json!({
                "max_tokens": 40,
                "top_p": 0.9
            }),
        ));

        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_attempts = Arc::clone(&captured);
        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let captured_attempts = Arc::clone(&captured_attempts);
                    async move {
                        captured_attempts.lock().expect("attempts").push(attempt);
                        Ok::<_, std::io::Error>("ok")
                    }
                },
                |_error| None,
            )
            .await
            .expect("routed output");

        assert_eq!(output, "ok");
        let attempts = captured.lock().expect("attempts");
        assert_eq!(attempts[0].model_name, "db/model");
        assert_eq!(attempts[0].overrides.temperature, Some(0.7));
        assert_eq!(attempts[0].overrides.max_tokens, Some(40));
        assert_eq!(attempts[0].overrides.extra["enable_thinking"], json!(true));
        assert_eq!(attempts[0].overrides.extra["top_p"], json!(0.9));
    }

    #[tokio::test]
    async fn walker_emits_circuit_open_exhaustion_when_all_attempts_are_blocked() {
        let breakers = Arc::new(BreakerSet::new());
        breakers.record_failure(
            1,
            10,
            BreakerConfig {
                fail_threshold: 1,
                cooldown: std::time::Duration::from_secs(60),
            },
        );
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
            std::time::Duration::from_secs(600),
        );
        let walker = walker_with_breakers(snapshot(json!({}), json!({})), breakers)
            .with_reporter(reporter.clone());

        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                |_attempt| async { Ok::<_, std::io::Error>("probe") },
                |_error| None,
            )
            .await
            .expect("half-open probe output");

        assert_eq!(output, "probe");
        let events = reporter.buffer().routing_events(10);
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "circuit_open_exhaustion")
        );
    }
}
