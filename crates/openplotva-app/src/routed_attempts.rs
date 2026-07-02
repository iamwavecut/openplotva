use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openplotva_llm::retry::FailureReason;
use openplotva_llm::router::{
    BreakerLiveness, BreakerSet, InferenceOverrides, PoolRegistry, RouterHandle, TriggerState,
    WorkflowRoute, select_chain,
};
use serde_json::{Value, json};

/// Upper bound on how long one walker run waits for a capacity-pool slot when
/// every candidate's pool is full. Slot waiting deliberately does not count
/// against the workflow's `retry_wall_ms` (which budgets attempt execution):
/// pool queueing replaces the server-side queueing some backends did before,
/// so it is bounded only by the request deadline and this sanity cap.
const DEFAULT_MAX_SLOT_WAIT: Duration = Duration::from_secs(300);

/// Minimum share of the caller's budget an attempt must have received for a
/// deadline cut to count against the provider's circuit breaker.
const ATTEMPT_BREAKER_MIN_SLICE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct RoutedAttemptWalker {
    handle: Arc<RouterHandle>,
    breakers: Arc<BreakerSet>,
    triggers: Arc<TriggerState>,
    pools: Arc<PoolRegistry>,
    max_slot_wait: Duration,
    reporter: Option<crate::runtime_routing::RoutingEventReporter>,
}

impl RoutedAttemptWalker {
    #[must_use]
    pub fn new(
        handle: Arc<RouterHandle>,
        breakers: Arc<BreakerSet>,
        triggers: Arc<TriggerState>,
        pools: Arc<PoolRegistry>,
    ) -> Self {
        Self {
            handle,
            breakers,
            triggers,
            pools,
            max_slot_wait: DEFAULT_MAX_SLOT_WAIT,
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

    #[must_use]
    pub fn with_max_slot_wait(mut self, max_slot_wait: Duration) -> Self {
        self.max_slot_wait = max_slot_wait;
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
                RoutedRoutingErrorKind::RouteUnavailable,
                "workflow route is unavailable",
            )));
        };

        let started = Instant::now();
        let max_hops = route.retry.max_hops.max(1) as usize;
        let mut started_attempts = 0usize;
        let mut total_slot_wait = Duration::ZERO;
        let mut wait_reported = false;
        let mut first_pass = true;
        let mut last_error = None;
        let mut last_provider_model = None;
        let mut failed_attempts = 0usize;
        let mut last_reason = None;
        let mut deadline_cut = false;

        // Selection-pass loop: walk a fresh chain, skipping candidates whose
        // capacity pool is full. A busy skip touches neither the breaker nor
        // the hop budget (busy is not dead). When a whole pass is busy, wait
        // for any slot to free and re-select; only execution time counts
        // against the wall clock, so pool queueing never eats the retry budget.
        'passes: loop {
            let selection_now = Instant::now();
            let attempts = {
                let liveness = BreakerLiveness::new(&self.breakers, selection_now);
                let mut rng = rand::rng();
                select_chain(route, &liveness, self.triggers.as_ref(), &mut rng)
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
                    RoutedRoutingErrorKind::NoCandidates,
                    "workflow route has no eligible candidates",
                )));
            }
            if first_pass
                && attempts.iter().all(|attempt| {
                    !self
                        .breakers
                        .is_live_at(attempt.provider, attempt.model, selection_now)
                })
            {
                self.record_event(routing_event(
                    "circuit_open_exhaustion",
                    &context,
                    None,
                    None,
                    "all routed attempts are inside circuit cooldown",
                    json!({ "attempts": attempts.len() }),
                ));
            }
            first_pass = false;

            let mut busy_skips = 0usize;
            let mut executed_this_pass = 0usize;
            for attempt in attempts {
                if started.elapsed().saturating_sub(total_slot_wait) > route.retry.wall_clock {
                    break 'passes;
                }
                if context
                    .deadline
                    .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    break 'passes;
                }
                if started_attempts >= max_hops {
                    break 'passes;
                }
                let provider = table.provider(attempt.provider);
                let model = table.model(attempt.model);
                let Some(permit) = self.pools.try_acquire(model.and_then(|row| row.pool_id)) else {
                    busy_skips += 1;
                    continue;
                };
                let routed = RoutedAttempt {
                    provider_id: attempt.provider,
                    model_id: attempt.model,
                    provider_name: provider.map(|row| row.name.clone()).unwrap_or_default(),
                    model_name: model.map(|row| row.model_name.clone()).unwrap_or_default(),
                    provider_endpoint: provider.and_then(|row| row.endpoint.clone()),
                    discovery_service_name: provider
                        .and_then(|row| row.discovery_service_name.clone()),
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
                started_attempts += 1;
                executed_this_pass += 1;

                // A started attempt must not outlive the caller's deadline
                // either: a hung provider connection otherwise pins the worker
                // long past the turn budget (2026-07-02 latency incident).
                let result = match context.deadline {
                    Some(deadline) => {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        match tokio::time::timeout(remaining, execute(routed)).await {
                            Ok(result) => result,
                            Err(_) => {
                                drop(permit);
                                // Charge the breaker only when the provider had
                                // a meaningful slice of the budget; a fallback
                                // handed the last few seconds of a turn is not
                                // unhealthy, and penalizing it opens its circuit
                                // in lockstep with a degraded primary.
                                let charged = remaining >= ATTEMPT_BREAKER_MIN_SLICE;
                                if charged {
                                    self.breakers.record_failure(
                                        attempt.provider,
                                        attempt.model,
                                        attempt.breaker,
                                    );
                                }
                                failed_attempts += 1;
                                last_reason = Some("attempt_deadline_exceeded".to_owned());
                                deadline_cut = true;
                                self.record_event(routing_event(
                                    "attempt_deadline_exceeded",
                                    &context,
                                    Some(attempt.provider),
                                    Some(attempt.model),
                                    "attempt aborted at the caller deadline",
                                    json!({
                                        "failed_attempts": failed_attempts,
                                        "attempt_slice_ms": remaining.as_millis(),
                                        "breaker_charged": charged,
                                    }),
                                ));
                                break 'passes;
                            }
                        }
                    }
                    None => execute(routed).await,
                };
                drop(permit);
                match result {
                    Ok(output) => {
                        self.breakers
                            .record_success(attempt.provider, attempt.model);
                        return Ok(output);
                    }
                    Err(error) => {
                        let Some(reason) = retryable(&error) else {
                            return Err(RoutedAttemptRunError::Attempt(error));
                        };
                        self.breakers.record_failure(
                            attempt.provider,
                            attempt.model,
                            attempt.breaker,
                        );
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

            if busy_skips == 0 || started_attempts >= max_hops {
                break;
            }
            if executed_this_pass > 0 {
                // Something ran and failed while other candidates were busy:
                // re-select immediately, a slot may have freed meanwhile.
                continue;
            }

            // Every candidate in the pass was pool-busy: wait for any slot.
            let wait_budget = match context.deadline {
                Some(deadline) => deadline
                    .checked_duration_since(Instant::now())
                    .unwrap_or(Duration::ZERO)
                    .min(self.max_slot_wait),
                None => self.max_slot_wait,
            };
            if !wait_reported {
                wait_reported = true;
                self.record_event(routing_event_with_severity(
                    "capacity_pool_wait",
                    "warn",
                    &context,
                    None,
                    None,
                    "all candidate capacity pools are busy; waiting for a slot",
                    json!({
                        "busy_candidates": busy_skips,
                        "wait_budget_ms": wait_budget.as_millis(),
                    }),
                ));
            }
            let wait_started = Instant::now();
            let released = if wait_budget.is_zero() {
                false
            } else {
                self.pools.wait_for_release(wait_budget).await
            };
            total_slot_wait += wait_started.elapsed();
            if !released {
                self.record_event(routing_event(
                    "capacity_wait_timeout",
                    &context,
                    None,
                    None,
                    "workflow capacity unavailable: slot wait timed out",
                    json!({
                        "busy_candidates": busy_skips,
                        "waited_ms": total_slot_wait.as_millis(),
                    }),
                ));
                return Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                    RoutedRoutingErrorKind::CapacityWaitTimeout,
                    format!(
                        "workflow capacity unavailable: all pools busy after {}ms slot wait",
                        total_slot_wait.as_millis()
                    ),
                )));
            }
        }

        let (provider_id, model_id) = last_provider_model.unwrap_or_default();
        self.record_event(routing_event(
            "all_attempts_exhausted",
            &context,
            (provider_id != 0).then_some(provider_id),
            (model_id != 0).then_some(model_id),
            "routed attempts exhausted",
            all_attempts_exhausted_detail(&context, failed_attempts, last_reason),
        ));
        match last_error {
            Some(error) => Err(RoutedAttemptRunError::Attempt(error)),
            None if deadline_cut => Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                RoutedRoutingErrorKind::DeadlineExceeded,
                "routed attempt exceeded the caller deadline",
            ))),
            None => Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                RoutedRoutingErrorKind::Exhausted,
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

fn all_attempts_exhausted_detail(
    context: &RoutedRequestContext,
    failed_attempts: usize,
    last_reason: Option<String>,
) -> Value {
    let mut detail = json!({
        "failed_attempts": failed_attempts,
        "last_retryable_reason": last_reason,
    });
    if context.suppress_all_attempts_exhausted_admin_report
        && let Some(object) = detail.as_object_mut()
    {
        let reason = context
            .suppressed_all_attempts_exhausted_reason
            .as_deref()
            .unwrap_or("handled_by_job_retry_budget");
        object.insert("admin_actionable".to_owned(), json!(false));
        object.insert("admin_actionable_reason".to_owned(), json!(reason));
    }
    detail
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
    /// The walker will not start an attempt at or past this instant, so one
    /// call cannot overshoot the caller's turn budget by a full attempt.
    pub deadline: Option<Instant>,
    pub suppress_all_attempts_exhausted_admin_report: bool,
    pub suppressed_all_attempts_exhausted_reason: Option<String>,
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

/// Why the walker itself (not a provider attempt) failed a run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutedRoutingErrorKind {
    RouteUnavailable,
    NoCandidates,
    Exhausted,
    /// The in-flight attempt was aborted at the caller's deadline (turn
    /// budget); the worker is freed instead of pinned by a hung connection.
    DeadlineExceeded,
    /// Every candidate's capacity pool stayed full for the whole slot-wait
    /// budget. The `Display` message contains the phrase "capacity
    /// unavailable" on purpose: `retry.rs` message rules classify it as
    /// `FailureReason::CapacityUnavailable`, so every call site requeues.
    CapacityWaitTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedRoutingError {
    kind: RoutedRoutingErrorKind,
    message: String,
}

impl RoutedRoutingError {
    fn new(kind: RoutedRoutingErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> RoutedRoutingErrorKind {
        self.kind
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
    let severity = if event_type == "all_attempts_exhausted"
        && context.suppress_all_attempts_exhausted_admin_report
    {
        "warn"
    } else {
        "error"
    };
    routing_event_with_severity(
        event_type,
        severity,
        context,
        provider_id,
        model_id,
        summary,
        detail,
    )
}

fn routing_event_with_severity(
    event_type: &str,
    severity: &str,
    context: &RoutedRequestContext,
    provider_id: Option<i64>,
    model_id: Option<i64>,
    summary: &str,
    detail: Value,
) -> crate::runtime_routing::RoutingEvent {
    crate::runtime_routing::RoutingEvent {
        severity: severity.to_owned(),
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

    use openplotva_llm::router::{
        BreakerConfig, BreakerSet, PoolRegistry, RouterHandle, TriggerState,
    };
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
            protocol: None,
            runtime_hint: None,
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
            pool_id: None,
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
            pools: vec![],
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
            Arc::new(PoolRegistry::new()),
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
    async fn walker_does_not_start_attempts_past_the_deadline() {
        let walker = walker_for(snapshot(json!({}), json!({})));
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = Arc::clone(&calls);

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(std::time::Instant::now()),
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
    }

    #[tokio::test(start_paused = true)]
    async fn walker_aborts_an_in_flight_attempt_at_the_deadline_without_charging_a_tiny_slice() {
        let mut snap = snapshot(json!({}), json!({}));
        snap.assignments[0].cb_failure_threshold = 1;
        let breakers = Arc::new(BreakerSet::new());
        let walker = walker_with_breakers(snap, Arc::clone(&breakers));

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
                    ..RoutedRequestContext::default()
                },
                |_attempt| std::future::pending::<Result<&str, std::io::Error>>(),
                |_error| None,
            )
            .await;

        match result {
            Err(RoutedAttemptRunError::Routing(error)) => {
                assert_eq!(error.kind(), RoutedRoutingErrorKind::DeadlineExceeded);
                assert!(error.to_string().contains("deadline"), "{error}");
            }
            other => panic!("expected a deadline routing error, got {other:?}"),
        }
        assert!(
            breakers.is_live_at(1, 10, Instant::now()),
            "a five-second slice must not charge the breaker"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn walker_charges_the_breaker_when_a_deadline_cut_attempt_had_a_real_slice() {
        let mut snap = snapshot(json!({}), json!({}));
        snap.assignments[0].cb_failure_threshold = 1;
        let breakers = Arc::new(BreakerSet::new());
        let walker = walker_with_breakers(snap, Arc::clone(&breakers));

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(40)),
                    ..RoutedRequestContext::default()
                },
                |_attempt| std::future::pending::<Result<&str, std::io::Error>>(),
                |_error| None,
            )
            .await;

        assert!(matches!(result, Err(RoutedAttemptRunError::Routing(_))));
        assert!(
            !breakers.is_live_at(1, 10, Instant::now()),
            "a forty-second slice that never returned must open the breaker"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_cut_attempt_returns_its_pool_permit() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(std::time::Instant::now() + std::time::Duration::from_secs(5)),
                    ..RoutedRequestContext::default()
                },
                |_attempt| std::future::pending::<Result<&str, std::io::Error>>(),
                |_error| None,
            )
            .await;

        assert!(matches!(result, Err(RoutedAttemptRunError::Routing(_))));
        assert!(
            pools.try_acquire(Some(1)).is_some(),
            "the aborted attempt must free its slot"
        );
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

    use openplotva_storage::llm_routing::PoolRecord;

    /// Two chat primaries: model 10 draws from 1-slot pool 1, model 20 is
    /// unpooled. `max_hops` bounds started attempts.
    fn pooled_snapshot(max_hops: i32) -> RoutingSnapshot {
        let mut second = assignment(json!({}));
        second.id = 101;
        second.provider_model_id = 20;
        second.weight = Some(50);
        let mut first = assignment(json!({}));
        first.weight = Some(50);
        RoutingSnapshot {
            providers: vec![provider(1, "aifarm"), provider(2, "vram-cloud")],
            models: vec![
                ModelRecord {
                    pool_id: Some(1),
                    ..model(10, 1, json!({}))
                },
                ModelRecord {
                    model_name: "free/model".to_owned(),
                    ..model(20, 2, json!({}))
                },
            ],
            workflows: vec![WorkflowRecord {
                key: "dialog".to_owned(),
                kind: "chat".to_owned(),
                full_routing: true,
                retry_max_hops: max_hops,
                retry_wall_ms: 60_000,
                enabled: true,
            }],
            assignments: vec![first, second],
            triggers: vec![],
            pools: vec![PoolRecord {
                id: 1,
                name: "gpu".to_owned(),
                max_concurrency: Some(1),
                description: None,
            }],
        }
    }

    fn walker_with_pools(
        snapshot: &RoutingSnapshot,
        pools: Arc<PoolRegistry>,
    ) -> RoutedAttemptWalker {
        let table = crate::model_routing::build_routing_table(snapshot);
        pools.apply(&table.pool_specs());
        RoutedAttemptWalker::new(
            RouterHandle::new(table),
            Arc::new(BreakerSet::new()),
            Arc::new(TriggerState::new()),
            pools,
        )
    }

    #[tokio::test]
    async fn busy_pool_skips_to_free_candidate_without_breaker_mutation() {
        let snapshot = pooled_snapshot(3);
        let pools = Arc::new(PoolRegistry::new());
        let breakers = Arc::new(BreakerSet::new());
        let table = crate::model_routing::build_routing_table(&snapshot);
        pools.apply(&table.pool_specs());
        let walker = RoutedAttemptWalker::new(
            RouterHandle::new(table),
            Arc::clone(&breakers),
            Arc::new(TriggerState::new()),
            Arc::clone(&pools),
        );
        let _busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");

        let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let executed_models = Arc::clone(&executed);
        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let executed_models = Arc::clone(&executed_models);
                    async move {
                        executed_models
                            .lock()
                            .expect("models")
                            .push(attempt.model_id);
                        Ok::<_, std::io::Error>("ok")
                    }
                },
                |_error| None,
            )
            .await
            .expect("free candidate output");

        assert_eq!(output, "ok");
        assert_eq!(*executed.lock().expect("models"), vec![20]);
        assert!(
            breakers.is_live_at(1, 10, std::time::Instant::now()),
            "a busy skip must not trip the breaker"
        );
    }

    #[tokio::test]
    async fn busy_skips_do_not_consume_the_hop_budget() {
        let snapshot = pooled_snapshot(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");

        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = Arc::clone(&calls);
        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |_attempt| {
                    let call_count = Arc::clone(&call_count);
                    async move {
                        call_count.fetch_add(1, Ordering::Relaxed);
                        Ok::<_, std::io::Error>("ok")
                    }
                },
                |_error| None,
            )
            .await
            .expect("the single hop must go to the free candidate");

        assert_eq!(output, "ok");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn all_busy_waits_for_release_then_succeeds() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(busy);
        });

        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                |_attempt| async { Ok::<_, std::io::Error>("after wait") },
                |_error| None,
            )
            .await
            .expect("walker must wake on the released slot");

        assert_eq!(output, "after wait");
    }

    #[tokio::test(start_paused = true)]
    async fn slot_wait_timeout_classifies_as_capacity_unavailable() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools))
            .with_max_slot_wait(std::time::Duration::from_millis(50));
        let _busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                |_attempt| async { Ok::<_, std::io::Error>("unreachable") },
                |_error| None,
            )
            .await;

        let Err(RoutedAttemptRunError::Routing(error)) = result else {
            panic!("expected a routing error");
        };
        assert_eq!(error.kind(), RoutedRoutingErrorKind::CapacityWaitTimeout);
        // Contract with retry.rs: the rendered message classifies as a
        // retryable CapacityUnavailable, so every call site requeues the job.
        assert_eq!(
            openplotva_llm::retry::retryable_reason_from_message(&error.to_string()),
            Some(FailureReason::CapacityUnavailable)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_run_future_returns_the_permit() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));

        let run = walker.run(
            RoutedRequestContext {
                workflow_key: "dialog".to_owned(),
                ..RoutedRequestContext::default()
            },
            |_attempt| async {
                // Hold the permit across an await that never resolves.
                std::future::pending::<()>().await;
                Ok::<_, std::io::Error>("never")
            },
            |_error| None,
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), run)
                .await
                .is_err(),
            "run must still be executing when the timeout drops it"
        );

        assert!(
            pools.try_acquire(Some(1)).is_some(),
            "cancelling the in-flight attempt must free its slot"
        );
        assert_eq!(
            pools.occupancy(),
            vec![openplotva_llm::router::PoolOccupancy {
                id: 1,
                max_concurrency: Some(1),
                in_flight: 0,
            }]
        );
    }
}
