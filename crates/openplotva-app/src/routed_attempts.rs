use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use openplotva_llm::retry::FailureReason;
use openplotva_llm::router::{
    Attempt, BreakerLiveness, BreakerSet, InferenceOverrides, LanePriority, PoolId, PoolPermit,
    PoolRegistry, Role, RouterHandle, TriggerState, WorkflowRoute, select_chain,
};
use serde_json::{Value, json};

/// Minimum share of the caller's budget an attempt must have received for a
/// deadline cut to count against the provider's circuit breaker.
const ATTEMPT_BREAKER_MIN_SLICE: Duration = Duration::from_secs(30);
/// A re-sample is only worth starting when the turn still has room for it and for
/// the fallback behind it.
const MODEL_OUTPUT_RETRY_MIN_BUDGET: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct RoutedAttemptWalker {
    handle: Arc<RouterHandle>,
    breakers: Arc<BreakerSet>,
    triggers: Arc<TriggerState>,
    pools: Arc<PoolRegistry>,
    openrouter_free_gate: Option<Arc<crate::openrouter_free_pool::OpenRouterFreeQuotaGate>>,
    model_output_retries: usize,
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
            openrouter_free_gate: None,
            model_output_retries: 0,
            reporter: None,
        }
    }

    #[must_use]
    pub fn with_reporter(mut self, reporter: crate::runtime_routing::RoutingEventReporter) -> Self {
        self.reporter = Some(reporter);
        self
    }

    #[must_use]
    pub fn with_openrouter_free_gate(
        mut self,
        gate: Arc<crate::openrouter_free_pool::OpenRouterFreeQuotaGate>,
    ) -> Self {
        self.openrouter_free_gate = Some(gate);
        self
    }

    #[must_use]
    pub fn with_openrouter_free_gate_opt(
        mut self,
        gate: Option<Arc<crate::openrouter_free_pool::OpenRouterFreeQuotaGate>>,
    ) -> Self {
        self.openrouter_free_gate = gate;
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

    /// How many extra samples the same model gets when a validator rejects what it
    /// wrote, before the chain walks on to another provider. Zero (the default) keeps
    /// the immediate walk.
    #[must_use]
    pub fn with_model_output_retries(mut self, retries: usize) -> Self {
        self.model_output_retries = retries;
        self
    }

    /// Config of the workflow's heaviest global primary, for callers that shape
    /// the request to the model before the walk starts.
    #[must_use]
    pub fn primary_model_config(&self, workflow_key: &str) -> Option<Value> {
        let table = self.handle.load();
        let route = table.resolve(workflow_key, false)?;
        let candidate = route
            .primary
            .iter()
            .max_by_key(|candidate| candidate.weight)?;
        table
            .model(candidate.model)
            .map(|model| model.config.clone())
    }

    /// Admit one candidate through its lane queue. A free lane is taken at once.
    /// A candidate with an open breaker is never waited for. Otherwise the
    /// request waits in the lane queue until `deadline - reserve`, keeping the
    /// workflow's fallback reserve only while another candidate follows, and
    /// moves on at once when the forecast wait already exceeds that budget.
    /// Lane time is charged to the caller's deadline, not to the retry wall.
    #[allow(clippy::too_many_arguments)]
    async fn admit_lane(
        &self,
        pool: Option<PoolId>,
        live: bool,
        has_next_candidate: bool,
        route: &WorkflowRoute,
        context: &RoutedRequestContext,
        attempt: &Attempt,
        total_slot_wait: &mut Duration,
    ) -> Option<PoolPermit> {
        if let Some(permit) = self.pools.try_acquire(pool) {
            return Some(permit);
        }
        if !live {
            return None;
        }
        let priority = context.lane_priority.unwrap_or(route.lane_priority);
        let reserve = if has_next_candidate {
            route.fallback_reserve
        } else {
            Duration::ZERO
        };
        let budget = context.deadline.map(|deadline| {
            deadline
                .saturating_duration_since(Instant::now())
                .saturating_sub(reserve)
        });
        if let Some(budget) = budget {
            let forecast = self.pools.forecast_wait(pool, priority);
            if budget.is_zero() || forecast.is_some_and(|forecast| forecast > budget) {
                self.record_event(routing_event_with_severity(
                    "lane_bypass",
                    "info",
                    context,
                    Some(attempt.provider),
                    Some(attempt.model),
                    "lane wait would exceed the request budget; trying the next candidate",
                    json!({
                        "priority": priority.as_str(),
                        "budget_ms": budget.as_millis(),
                        "forecast_ms": forecast.map(|forecast| forecast.as_millis()),
                    }),
                ));
                return None;
            }
        }
        let started = Instant::now();
        let permit = self.pools.acquire(pool, priority, budget).await;
        let waited = started.elapsed();
        *total_slot_wait += waited;
        self.record_event(routing_event_with_severity(
            "lane_wait",
            "info",
            context,
            Some(attempt.provider),
            Some(attempt.model),
            if permit.is_some() {
                "lane acquired after waiting in its queue"
            } else {
                "lane wait reached the request budget"
            },
            json!({
                "priority": priority.as_str(),
                "waited_ms": waited.as_millis(),
                "budget_ms": budget.map(|budget| budget.as_millis()),
                "acquired": permit.is_some(),
            }),
        ));
        permit
    }

    /// Among the live weighted head of the chain (primaries and engaged
    /// overflow), move candidates with a free lane ahead of busy ones. Every
    /// other position keeps its order.
    fn prefer_free_lanes(
        &self,
        attempts: &mut [Attempt],
        table: &openplotva_llm::router::RoutingTable,
        now: Instant,
    ) {
        let positions: Vec<usize> = attempts
            .iter()
            .enumerate()
            .filter(|(_, attempt)| {
                matches!(attempt.role, Role::Primary | Role::Overflow)
                    && self
                        .breakers
                        .is_live_at(attempt.provider, attempt.model, now)
            })
            .map(|(index, _)| index)
            .collect();
        let mut head: Vec<Attempt> = positions
            .iter()
            .map(|index| attempts[*index].clone())
            .collect();
        head.sort_by_key(|attempt| {
            !self
                .pools
                .has_free_lane(table.model(attempt.model).and_then(|model| model.pool_id))
        });
        for (index, attempt) in positions.into_iter().zip(head) {
            attempts[index] = attempt;
        }
    }

    pub async fn run<T, E, F, Fut, Retry>(
        &self,
        context: RoutedRequestContext,
        mut execute: F,
        retryable: Retry,
    ) -> Result<T, RoutedAttemptRunError<E>>
    where
        E: std::fmt::Debug + Send + Sync + 'static,
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
        let mut first_pass = true;
        let mut last_error = None;
        let mut last_provider_model = None;
        let mut failed_attempts = 0usize;
        let mut last_reason = None;
        let mut deadline_cut = false;
        let mut attempted_targets = HashSet::new();
        // Re-sampling budget for the whole turn, not per candidate: the primary spends it
        // first, and whatever is left still covers a fallback that answers badly.
        let mut model_output_retry = 0usize;

        // Selection-pass loop: walk a fresh chain, admitting each candidate
        // through its lane queue. A candidate passed over for its lane (open
        // breaker, budget) touches neither the breaker nor the hop budget.
        // After an executed attempt fails, re-select so a passed-over
        // candidate can be tried with the time that is left; only execution
        // time counts against the wall clock, so lane queueing never eats the
        // retry budget.
        'passes: loop {
            let selection_now = Instant::now();
            let mut attempts = {
                let liveness = BreakerLiveness::new(&self.breakers, selection_now);
                let mut rng = rand::rng();
                select_chain(route, &liveness, self.triggers.as_ref(), &mut rng)
                    .into_iter()
                    .filter(|attempt| {
                        context.required_capabilities.iter().all(|capability| {
                            table
                                .model(attempt.model)
                                .is_some_and(|model| model.has_capability(capability))
                        })
                    })
                    .collect::<Vec<_>>()
            };
            self.prefer_free_lanes(&mut attempts, &table, selection_now);
            // The previous step's target is preferred only while its lane is
            // free: the preference saves a warm cache, not a queue position.
            if let Some((provider_name, model_name)) = &context.preferred_target
                && let Some(index) = attempts.iter().position(|attempt| {
                    table
                        .provider(attempt.provider)
                        .is_some_and(|provider| provider.name == *provider_name)
                        && table
                            .model(attempt.model)
                            .is_some_and(|model| model.model_name == *model_name)
                        && self
                            .breakers
                            .is_live_at(attempt.provider, attempt.model, selection_now)
                        && self.pools.has_free_lane(
                            table.model(attempt.model).and_then(|model| model.pool_id),
                        )
                })
            {
                let preferred = attempts.remove(index);
                attempts.insert(0, preferred);
            }
            if attempts.is_empty() {
                self.record_event(routing_event(
                    "no_candidates",
                    &context,
                    None,
                    None,
                    "workflow route has no eligible candidates",
                    json!({
                        "reason": "zero_selected_attempts",
                        "required_capabilities": &context.required_capabilities,
                    }),
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
            let mut quota_skips = 0usize;
            let mut executed_this_pass = 0usize;
            for (index, attempt) in attempts.iter().enumerate() {
                if attempted_targets.contains(&(attempt.provider, attempt.model)) {
                    continue;
                }
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
                let pool_id = model.and_then(|row| row.pool_id);
                let live =
                    self.breakers
                        .is_live_at(attempt.provider, attempt.model, Instant::now());
                let has_next_candidate = attempts[index + 1..]
                    .iter()
                    .any(|next| !attempted_targets.contains(&(next.provider, next.model)));
                // A rejected model output is not a sick provider: the same model gets a
                // bounded number of fresh samples before the chain walks on, so a guard
                // rejection does not hand a tool-shaped turn to a tool-less fallback.
                let mut first_execution = true;
                loop {
                    let Some(permit) = self
                        .admit_lane(
                            pool_id,
                            live,
                            has_next_candidate,
                            route,
                            &context,
                            attempt,
                            &mut total_slot_wait,
                        )
                        .await
                    else {
                        if first_execution {
                            busy_skips += 1;
                        }
                        break;
                    };
                    if let (Some(gate), Some(model)) = (&self.openrouter_free_gate, model) {
                        match gate.check_model(&model.model_name, &model.config).await {
                            crate::openrouter_free_pool::OpenRouterFreeQuotaDecision::Allowed => {}
                            crate::openrouter_free_pool::OpenRouterFreeQuotaDecision::Denied {
                                reason,
                                retry_after,
                            } => {
                                quota_skips += 1;
                                self.record_event(routing_event_with_severity(
                                "openrouter_free_quota_limited",
                                "warn",
                                &context,
                                Some(attempt.provider),
                                Some(attempt.model),
                                "OpenRouter Free pool unavailable; skipping to fallback",
                                json!({
                                    "reason": reason,
                                    "retry_after_ms": retry_after.map(|duration| duration.as_millis()),
                                }),
                            ));
                                break;
                            }
                        }
                    }
                    let routed = RoutedAttempt {
                        provider_id: attempt.provider,
                        model_id: attempt.model,
                        provider_name: provider.map(|row| row.name.clone()).unwrap_or_default(),
                        model_name: model.map(|row| row.model_name.clone()).unwrap_or_default(),
                        provider_runtime_hint: provider.and_then(|row| row.runtime_hint.clone()),
                        provider_endpoint: provider.and_then(|row| row.endpoint.clone()),
                        discovery_service_name: provider
                            .and_then(|row| row.discovery_service_name.clone()),
                        discovery_endpoint_name: provider
                            .and_then(|row| row.discovery_endpoint_name.clone()),
                        provider_api_key_ref: provider.and_then(|row| row.api_key_ref.clone()),
                        provider_api_key_encrypted: provider
                            .and_then(|row| row.api_key_encrypted.clone()),
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
                                merge_model_and_assignment_overrides(
                                    &row.config,
                                    &attempt.overrides,
                                )
                            })
                            .unwrap_or_else(|| attempt.overrides.clone()),
                        variant: attempt.variant.clone(),
                    };
                    attempted_targets.insert((routed.provider_id, routed.model_id));
                    last_provider_model = Some((routed.provider_id, routed.model_id));
                    if first_execution {
                        started_attempts += 1;
                        executed_this_pass += 1;
                        first_execution = false;
                    }

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
                            // A rejected output leaves the breaker alone: the provider answered
                            // in time, so counting these opens its circuit and exiles a healthy
                            // model from every turn for the cooldown.
                            if reason == FailureReason::ModelOutputRejected
                                && model_output_retry < self.model_output_retries
                                && context.deadline.is_none_or(|deadline| {
                                    deadline.saturating_duration_since(Instant::now())
                                        >= MODEL_OUTPUT_RETRY_MIN_BUDGET
                                })
                                && started.elapsed().saturating_sub(total_slot_wait)
                                    <= route.retry.wall_clock
                            {
                                model_output_retry += 1;
                                failed_attempts += 1;
                                last_reason = Some(reason.as_str().to_owned());
                                last_error = Some(error);
                                self.record_event(routing_event_with_severity(
                                    "model_output_retry",
                                    "info",
                                    &context,
                                    Some(attempt.provider),
                                    Some(attempt.model),
                                    "model output rejected; re-sampling the same model",
                                    json!({
                                        "retry": model_output_retry,
                                        "limit": self.model_output_retries,
                                        "retryable_reason": reason.as_str(),
                                    }),
                                ));
                                continue;
                            }
                            // Not even the sample that spends the budget charges the breaker:
                            // the model answered every time, and opening its circuit would
                            // hand the next turns to the fallback this policy exists to avoid.
                            if !matches!(
                                reason,
                                FailureReason::ModelOutputRejected
                                    | FailureReason::ContextLengthExceeded
                            ) {
                                self.breakers.record_failure(
                                    attempt.provider,
                                    attempt.model,
                                    attempt.breaker,
                                );
                            }
                            self.record_event(routing_event_with_severity(
                                "attempt_failed",
                                "info",
                                &context,
                                Some(attempt.provider),
                                Some(attempt.model),
                                "routed provider attempt failed",
                                json!({
                                    "attempt": failed_attempts + 1,
                                    "retryable_reason": reason.as_str(),
                                }),
                            ));
                            if reason == FailureReason::CapacityUnavailable
                                && let Some(cooldown) = provider_capacity_cooldown(
                                    route,
                                    attempt.provider,
                                    attempt.model,
                                )
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
                            if let (Some(gate), Some(model)) = (&self.openrouter_free_gate, model)
                                && model.config.get("managed_by").and_then(Value::as_str)
                                    == Some(crate::openrouter_free_pool::MANAGED_BY)
                            {
                                let message = format!("{error:?}");
                                if crate::openrouter_free_pool::message_indicates_openrouter_pool_cooldown(
                                &message,
                            ) {
                                let cooldown = match crate::openrouter_free_pool::retry_after_from_error_message(
                                    &message,
                                ) {
                                    Some(cooldown) => cooldown,
                                    None => gate.default_pool_cooldown().await,
                                };
                                gate.mark_pool_cooldown(cooldown);
                                self.record_event(routing_event(
                                    "openrouter_free_pool_cooldown",
                                    &context,
                                    Some(attempt.provider),
                                    Some(attempt.model),
                                    "OpenRouter Free pool entered cooldown after provider error",
                                    json!({
                                        "cooldown_ms": cooldown.as_millis(),
                                        "retryable_reason": reason.as_str(),
                                    }),
                                ));
                            } else if reason == FailureReason::CapacityUnavailable
                                || crate::openrouter_free_pool::message_indicates_openrouter_model_cooldown(&message)
                            {
                                let cooldown = match crate::openrouter_free_pool::retry_after_from_error_message(
                                    &message,
                                ) {
                                    Some(cooldown) => cooldown,
                                    None => gate.default_model_cooldown().await,
                                };
                                gate.mark_model_cooldown(&model.model_name, cooldown);
                                self.record_event(routing_event(
                                    "openrouter_free_model_cooldown",
                                    &context,
                                    Some(attempt.provider),
                                    Some(attempt.model),
                                    "OpenRouter Free model entered cooldown after provider error",
                                    json!({
                                        "model": model.model_name,
                                        "cooldown_ms": cooldown.as_millis(),
                                        "retryable_reason": reason.as_str(),
                                    }),
                                ));
                            }
                            }
                            failed_attempts += 1;
                            last_reason = Some(reason.as_str().to_owned());
                            last_error = Some(error);
                            break;
                        }
                    }
                }
            }

            if busy_skips == 0 || started_attempts >= max_hops {
                break;
            }
            if executed_this_pass > 0 {
                // Something ran and failed while other candidates were passed
                // over: re-select, a passed-over candidate may fit now.
                continue;
            }
            if last_error.is_some() || deadline_cut {
                break;
            }

            // No candidate got a lane within the request budget.
            self.record_event(routing_event(
                "capacity_wait_timeout",
                &context,
                None,
                None,
                "workflow capacity unavailable: no candidate lane within the request budget",
                json!({
                    "busy_candidates": busy_skips,
                    "quota_skips": quota_skips,
                    "waited_ms": total_slot_wait.as_millis(),
                }),
            ));
            return Err(RoutedAttemptRunError::Routing(RoutedRoutingError::new(
                RoutedRoutingErrorKind::CapacityWaitTimeout,
                format!(
                    "workflow capacity unavailable: no candidate lane after {}ms lane wait",
                    total_slot_wait.as_millis()
                ),
            )));
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
    pub user_id: Option<i64>,
    pub thread_id: Option<i32>,
    pub message_id: Option<i32>,
    pub vip: bool,
    /// Capabilities every selected provider model must advertise.
    pub required_capabilities: Vec<String>,
    /// Previous successful step target. Capacity and breaker guards still apply.
    pub preferred_target: Option<(String, String)>,
    /// The walker will not start an attempt at or past this instant, so one
    /// call cannot overshoot the caller's turn budget by a full attempt.
    pub deadline: Option<Instant>,
    /// Lane queue priority; `None` takes the workflow's `lane_priority`.
    pub lane_priority: Option<LanePriority>,
    pub suppress_all_attempts_exhausted_admin_report: bool,
    pub suppressed_all_attempts_exhausted_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RoutedAttempt {
    pub provider_id: i64,
    pub model_id: i64,
    pub provider_name: String,
    pub model_name: String,
    pub provider_runtime_hint: Option<String>,
    pub provider_endpoint: Option<String>,
    pub discovery_service_name: Option<String>,
    pub discovery_endpoint_name: Option<String>,
    pub provider_api_key_ref: Option<String>,
    pub provider_api_key_encrypted: Option<Vec<u8>>,
    pub model_base_url: Option<String>,
    pub embedding_dim: Option<i32>,
    pub provider_config: Value,
    pub model_config: Value,
    pub overrides: InferenceOverrides,
    pub variant: Option<String>,
}

impl RoutedAttempt {
    #[must_use]
    pub fn supports_message_name(&self) -> bool {
        self.provider_config
            .get("supports_message_name")
            .and_then(Value::as_bool)
            .unwrap_or(true)
    }

    #[must_use]
    pub fn supports_cache_control(&self) -> bool {
        provider_supports_cache_control(&self.provider_config)
    }
}

/// Whether a provider takes `cache_control` breakpoints on system messages; on unless its
/// config turns `supports_cache_control` off.
#[must_use]
pub fn provider_supports_cache_control(provider_config: &Value) -> bool {
    provider_config
        .get("supports_cache_control")
        .and_then(Value::as_bool)
        .unwrap_or(true)
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
        user_id: context.user_id,
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
                lane_priority: 0,
                fallback_reserve_ms: 30_000,
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

    fn snapshot_with_fallback() -> RoutingSnapshot {
        let mut snap = snapshot(json!({}), json!({}));
        snap.providers.push(provider(2, "genkit"));
        let mut fallback_model = model(20, 2, json!({}));
        fallback_model.model_name = "db/fallback".to_owned();
        snap.models.push(fallback_model);
        let mut fallback = assignment(json!({}));
        fallback.id = 200;
        fallback.role = "fallback".to_owned();
        fallback.provider_model_id = 20;
        fallback.weight = None;
        fallback.fallback_order = Some(0);
        snap.assignments.push(fallback);
        snap.workflows[0].retry_max_hops = 2;
        snap
    }

    #[tokio::test]
    async fn previous_successful_target_continues_with_normal_failure_fallback() {
        let walker = walker_for(snapshot_with_fallback());
        let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let calls = Arc::clone(&executed);
        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".into(),
                    preferred_target: Some(("genkit".into(), "db/fallback".into())),
                    ..Default::default()
                },
                move |attempt| {
                    let calls = Arc::clone(&calls);
                    async move {
                        calls.lock().expect("attempts").push(attempt.model_id);
                        if attempt.model_id == 20 {
                            Err(openplotva_llm::retry::ProviderError::new(
                                "genkit",
                                FailureReason::ProviderUnavailable,
                                "unavailable",
                            ))
                        } else {
                            Ok("recovered")
                        }
                    }
                },
                |error| Some(error.reason()),
            )
            .await
            .expect("fallback recovery");
        assert_eq!(output, "recovered");
        assert_eq!(*executed.lock().expect("attempts"), vec![20, 10]);
    }

    #[tokio::test]
    async fn unavailable_or_unknown_previous_target_keeps_the_live_primary() {
        for previous in ["db/fallback", "removed-model"] {
            let breakers = Arc::new(BreakerSet::new());
            breakers.record_failure(
                2,
                20,
                BreakerConfig {
                    fail_threshold: 1,
                    cooldown: Duration::from_secs(60),
                },
            );
            let walker = walker_with_breakers(snapshot_with_fallback(), breakers);
            let selected = walker
                .run(
                    RoutedRequestContext {
                        workflow_key: "dialog".into(),
                        preferred_target: Some(("genkit".into(), previous.into())),
                        ..Default::default()
                    },
                    |attempt| async move { Ok::<_, std::io::Error>(attempt.model_id) },
                    |_| None,
                )
                .await
                .expect("live primary");
            assert_eq!(selected, 10);
        }
    }

    #[tokio::test]
    async fn busy_previous_target_does_not_bypass_its_capacity_pool() {
        let mut snapshot = snapshot_with_fallback();
        snapshot.models[1].pool_id = Some(1);
        snapshot.pools.push(PoolRecord {
            id: 1,
            name: "fallback-pool".into(),
            max_concurrency: Some(1),
            description: None,
            config: json!({}),
        });
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _busy = pools.try_acquire(Some(1)).expect("occupy preferred pool");
        let selected = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".into(),
                    preferred_target: Some(("genkit".into(), "db/fallback".into())),
                    ..Default::default()
                },
                |attempt| async move { Ok::<_, std::io::Error>(attempt.model_id) },
                |_| None,
            )
            .await
            .expect("free primary");
        assert_eq!(selected, 10);
    }

    #[tokio::test]
    async fn walker_resamples_the_same_model_when_a_validator_rejects_its_output() {
        let mut snap = snapshot(json!({}), json!({}));
        snap.assignments[0].cb_failure_threshold = 1;
        let breakers = Arc::new(BreakerSet::new());
        let walker = walker_with_breakers(snap, Arc::clone(&breakers)).with_model_output_retries(2);
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
                        if call_count.fetch_add(1, Ordering::Relaxed) < 2 {
                            Err("rejected".to_owned())
                        } else {
                            Ok("answer")
                        }
                    }
                },
                |_error: &String| Some(FailureReason::ModelOutputRejected),
            )
            .await
            .expect("the re-sampled answer");

        assert_eq!(output, "answer");
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert!(
            breakers.is_live_at(1, 10, Instant::now()),
            "a rejected output leaves the provider healthy, so its circuit stays closed"
        );
    }

    #[tokio::test]
    async fn walker_walks_the_chain_once_re_sampling_is_exhausted() {
        let walker = walker_for(snapshot_with_fallback()).with_model_output_retries(1);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_models = Arc::clone(&seen);

        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let seen_models = Arc::clone(&seen_models);
                    async move {
                        seen_models.lock().expect("models").push(attempt.model_id);
                        if attempt.model_id == 10 {
                            Err("rejected".to_owned())
                        } else {
                            Ok("fallback answer")
                        }
                    }
                },
                |_error: &String| Some(FailureReason::ModelOutputRejected),
            )
            .await
            .expect("the fallback answer");

        assert_eq!(output, "fallback answer");
        assert_eq!(*seen.lock().expect("models"), vec![10, 10, 20]);
    }

    #[tokio::test]
    async fn walker_keeps_the_circuit_closed_after_the_re_sampling_budget_is_spent() {
        let mut snap = snapshot_with_fallback();
        snap.assignments[0].cb_failure_threshold = 1;
        let breakers = Arc::new(BreakerSet::new());
        let walker = walker_with_breakers(snap, Arc::clone(&breakers)).with_model_output_retries(1);

        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |attempt| async move {
                    if attempt.model_id == 10 {
                        Err("rejected".to_owned())
                    } else {
                        Ok("fallback answer")
                    }
                },
                |_error: &String| Some(FailureReason::ModelOutputRejected),
            )
            .await
            .expect("the fallback answer");

        assert_eq!(output, "fallback answer");
        assert!(
            breakers.is_live_at(1, 10, Instant::now()),
            "spending the re-sampling budget must not exile a model that kept answering"
        );
    }

    #[tokio::test]
    async fn walker_does_not_resample_an_infrastructure_failure() {
        let walker = walker_for(snapshot_with_fallback()).with_model_output_retries(3);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_models = Arc::clone(&seen);

        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let seen_models = Arc::clone(&seen_models);
                    async move {
                        seen_models.lock().expect("models").push(attempt.model_id);
                        if attempt.model_id == 10 {
                            Err("unavailable".to_owned())
                        } else {
                            Ok("fallback answer")
                        }
                    }
                },
                |_error: &String| Some(FailureReason::ProviderUnavailable),
            )
            .await
            .expect("the fallback answer");

        assert_eq!(output, "fallback answer");
        assert_eq!(
            *seen.lock().expect("models"),
            vec![10, 20],
            "a dead provider is walked past immediately, not re-sampled"
        );
    }

    #[tokio::test]
    async fn walker_falls_back_on_context_limit_without_opening_the_circuit() {
        let mut snap = snapshot_with_fallback();
        snap.assignments[0].cb_failure_threshold = 1;
        let breakers = Arc::new(BreakerSet::new());
        let walker = walker_with_breakers(snap, Arc::clone(&breakers)).with_model_output_retries(3);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_models = Arc::clone(&seen);
        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let seen_models = Arc::clone(&seen_models);
                    async move {
                        seen_models.lock().expect("models").push(attempt.model_id);
                        if attempt.model_id == 10 {
                            Err("upstream returned status 400: maximum context length".to_owned())
                        } else {
                            Ok("fallback answer")
                        }
                    }
                },
                |error: &String| openplotva_llm::retry::retryable_reason_from_message(error),
            )
            .await
            .expect("the larger-context fallback answers");

        assert_eq!(output, "fallback answer");
        assert_eq!(*seen.lock().expect("models"), vec![10, 20]);
        assert!(breakers.is_live_at(1, 10, Instant::now()));
    }

    #[tokio::test]
    async fn walker_skips_re_sampling_when_the_turn_budget_is_nearly_spent() {
        let walker = walker_for(snapshot_with_fallback()).with_model_output_retries(3);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_models = Arc::clone(&seen);

        let output = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(Instant::now() + Duration::from_secs(5)),
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let seen_models = Arc::clone(&seen_models);
                    async move {
                        seen_models.lock().expect("models").push(attempt.model_id);
                        if attempt.model_id == 10 {
                            Err("rejected".to_owned())
                        } else {
                            Ok("fallback answer")
                        }
                    }
                },
                |_error: &String| Some(FailureReason::ModelOutputRejected),
            )
            .await
            .expect("the fallback answer");

        assert_eq!(output, "fallback answer");
        assert_eq!(*seen.lock().expect("models"), vec![10, 20]);
    }

    #[tokio::test]
    async fn walker_emits_route_unavailable_without_calling_adapter() {
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
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
    async fn walker_excludes_models_without_required_capabilities() {
        let walker = walker_for(snapshot(json!({}), json!({})));
        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = Arc::clone(&calls);

        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    required_capabilities: vec!["vision".to_owned()],
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
                lane_priority: 0,
                fallback_reserve_ms: 30_000,
            }],
            assignments: vec![first, second],
            triggers: vec![],
            pools: vec![PoolRecord {
                id: 1,
                name: "gpu".to_owned(),
                max_concurrency: Some(1),
                description: None,
                config: serde_json::json!({}),
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
    async fn a_free_primary_sibling_is_taken_before_a_busy_one() {
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

        // The 50/50 weighted order puts the busy primary first in about half
        // of the runs; every run must still go to the free sibling.
        for _ in 0..16 {
            let (result, executed) =
                tokio::time::timeout(Duration::from_secs(2), run_recording(&walker, None, None))
                    .await
                    .expect("a free sibling must not wait behind a busy one");
            assert_eq!(result.expect("free candidate output"), "ok");
            assert_eq!(executed, vec![20]);
        }
        assert!(
            breakers.is_live_at(1, 10, std::time::Instant::now()),
            "a busy lane must not trip the breaker"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_target_is_not_reselected_while_fallback_pool_is_busy() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.assignments[0].weight = Some(100);
        snapshot.assignments[1].role = "fallback".to_owned();
        snapshot.assignments[1].weight = None;
        snapshot.assignments[1].fallback_order = Some(1);
        snapshot.models[0].pool_id = None;
        snapshot.models[1].pool_id = Some(1);

        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let busy_fallback = pools.try_acquire(Some(1)).expect("occupy fallback pool");
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(busy_fallback);
        });

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
                        if attempt.model_id == 10 {
                            Err(std::io::Error::other("primary protocol failure"))
                        } else {
                            Ok("fallback")
                        }
                    }
                },
                |_error| Some(FailureReason::ProviderProtocolError),
            )
            .await
            .expect("fallback must run after its pool is released");

        assert_eq!(output, "fallback");
        assert_eq!(*executed.lock().expect("models"), vec![10, 20]);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_retry_and_capacity_wait_events_are_informational() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.assignments[0].weight = Some(100);
        snapshot.assignments[1].role = "fallback".to_owned();
        snapshot.assignments[1].weight = None;
        snapshot.assignments[1].fallback_order = Some(1);
        snapshot.models[0].pool_id = None;
        snapshot.models[1].pool_id = Some(1);

        let pools = Arc::new(PoolRegistry::new());
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let walker =
            walker_with_pools(&snapshot, Arc::clone(&pools)).with_reporter(reporter.clone());
        let busy_fallback = pools.try_acquire(Some(1)).expect("occupy fallback pool");
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(busy_fallback);
        });

        walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                |attempt| async move {
                    if attempt.model_id == 10 {
                        Err(std::io::Error::other("retryable primary failure"))
                    } else {
                        Ok("fallback")
                    }
                },
                |_error| Some(FailureReason::ProviderProtocolError),
            )
            .await
            .expect("fallback succeeds after the transient events");

        let events = reporter.buffer().routing_events(10);
        for event_type in ["attempt_failed", "lane_wait"] {
            let event = events
                .iter()
                .find(|event| event.event_type == event_type)
                .unwrap_or_else(|| panic!("missing {event_type}"));
            assert_eq!(event.severity, "info", "{event_type} is not terminal");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_lane_is_waited_for_then_serves() {
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

    fn primary_with_fallback_snapshot() -> RoutingSnapshot {
        let mut snapshot = pooled_snapshot(3);
        let fallback = &mut snapshot.assignments[1];
        fallback.role = "fallback".to_owned();
        fallback.weight = None;
        fallback.fallback_order = Some(1);
        snapshot
    }

    #[tokio::test(start_paused = true)]
    async fn busy_primary_is_waited_for_before_the_fallback_tail() {
        let snapshot = primary_with_fallback_snapshot();
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(busy);
        });
        let started = tokio::time::Instant::now();

        let served_by = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    ..RoutedRequestContext::default()
                },
                |attempt| async move { Ok::<_, std::io::Error>(attempt.provider_id) },
                |_error| None,
            )
            .await
            .expect("the primary lane frees while the request waits");

        assert_eq!(served_by, 1, "the primary must serve once its slot frees");
        assert_eq!(started.elapsed(), std::time::Duration::from_millis(50));
    }

    #[tokio::test(start_paused = true)]
    async fn busy_primary_falls_back_when_its_lane_wait_reaches_the_budget() {
        let snapshot = primary_with_fallback_snapshot();
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");
        let started = tokio::time::Instant::now();
        // The 30 s fallback reserve leaves 200 ms of lane budget.
        let deadline = Instant::now() + Duration::from_millis(30_200);

        let served_by = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(deadline),
                    ..RoutedRequestContext::default()
                },
                |attempt| async move { Ok::<_, std::io::Error>(attempt.provider_id) },
                |_error| None,
            )
            .await
            .expect("the fallback serves after the primary lane budget");

        assert_eq!(
            served_by, 2,
            "the fallback serves a permanently busy primary"
        );
        let waited = started.elapsed();
        assert!(
            waited > Duration::from_millis(150) && waited <= Duration::from_millis(200),
            "{waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_lane_wait_that_reaches_the_deadline_classifies_as_capacity_unavailable() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _busy = pools.try_acquire(Some(1)).expect("occupy the gpu pool");

        // The only candidate keeps no fallback reserve: it waits until the deadline.
        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline: Some(Instant::now() + Duration::from_millis(50)),
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

        let occupancy = pools.occupancy();
        assert_eq!(occupancy.len(), 1);
        assert_eq!(occupancy[0].in_flight, 0);
        assert_eq!(occupancy[0].queued, [0, 0, 0]);
        assert!(
            pools.try_acquire(Some(1)).is_some(),
            "cancelling the in-flight attempt must free its lane"
        );
    }

    fn lane_reporter() -> crate::runtime_routing::RoutingEventReporter {
        crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(16),
            None,
            None,
        )
    }

    async fn run_recording(
        walker: &RoutedAttemptWalker,
        deadline: Option<Instant>,
        lane_priority: Option<LanePriority>,
    ) -> (
        Result<&'static str, RoutedAttemptRunError<std::io::Error>>,
        Vec<i64>,
    ) {
        let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let models = Arc::clone(&executed);
        let result = walker
            .run(
                RoutedRequestContext {
                    workflow_key: "dialog".to_owned(),
                    deadline,
                    lane_priority,
                    ..RoutedRequestContext::default()
                },
                move |attempt| {
                    let models = Arc::clone(&models);
                    async move {
                        models.lock().expect("models").push(attempt.model_id);
                        Ok::<_, std::io::Error>("ok")
                    }
                },
                |_error| None,
            )
            .await;
        let executed = executed.lock().expect("models").clone();
        (result, executed)
    }

    #[tokio::test]
    async fn a_forecast_beyond_the_budget_falls_back_without_waiting() {
        let snapshot = primary_with_fallback_snapshot();
        let pools = Arc::new(PoolRegistry::new());
        let reporter = lane_reporter();
        let walker =
            walker_with_pools(&snapshot, Arc::clone(&pools)).with_reporter(reporter.clone());
        let warmup = pools.try_acquire(Some(1)).expect("lane");
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(warmup);
        let _busy = pools.try_acquire(Some(1)).expect("occupy the primary lane");
        // The 30 s reserve leaves about 50 ms, below the 200 ms mean hold time.
        let started = Instant::now();
        let deadline = started + Duration::from_millis(30_050);

        let (result, executed) = run_recording(&walker, Some(deadline), None).await;

        assert_eq!(result.expect("fallback output"), "ok");
        assert_eq!(executed, vec![20]);
        assert!(
            started.elapsed() < Duration::from_millis(40),
            "no lane wait"
        );
        assert!(
            reporter
                .buffer()
                .routing_events(16)
                .iter()
                .any(|event| event.event_type == "lane_bypass")
        );
    }

    #[tokio::test]
    async fn a_candidate_with_an_open_breaker_is_never_waited_for() {
        let mut snapshot = primary_with_fallback_snapshot();
        snapshot.workflows[0].retry_max_hops = 3;
        let pools = Arc::new(PoolRegistry::new());
        let breakers = Arc::new(BreakerSet::new());
        let open = BreakerConfig {
            fail_threshold: 1,
            cooldown: Duration::from_secs(60),
        };
        breakers.record_failure(1, 10, open);
        breakers.record_failure(2, 20, open);
        let table = crate::model_routing::build_routing_table(&snapshot);
        pools.apply(&table.pool_specs());
        let walker = RoutedAttemptWalker::new(
            RouterHandle::new(table),
            breakers,
            Arc::new(TriggerState::new()),
            Arc::clone(&pools),
        );
        let _busy = pools.try_acquire(Some(1)).expect("occupy the primary lane");

        // Both candidates are in cooldown, so the chain probes them in order:
        // the busy primary is passed over instead of waited for.
        let (result, executed) =
            tokio::time::timeout(Duration::from_secs(5), run_recording(&walker, None, None))
                .await
                .expect("an open breaker must not queue the request");

        assert_eq!(result.expect("probe output"), "ok");
        assert_eq!(executed, vec![20]);
    }

    #[tokio::test]
    async fn an_expired_deadline_is_not_queued() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = walker_with_pools(&snapshot, Arc::clone(&pools));
        let _busy = pools.try_acquire(Some(1)).expect("occupy the only lane");

        let (result, executed) = tokio::time::timeout(
            Duration::from_secs(5),
            run_recording(&walker, Some(Instant::now()), None),
        )
        .await
        .expect("an expired deadline must not wait in the queue");

        assert!(result.is_err());
        assert!(executed.is_empty());
        assert_eq!(pools.occupancy()[0].queued, [0, 0, 0]);
    }

    #[tokio::test]
    async fn an_interactive_request_goes_ahead_of_queued_background_work() {
        let mut snapshot = pooled_snapshot(3);
        snapshot.models.truncate(1);
        snapshot.assignments.truncate(1);
        let pools = Arc::new(PoolRegistry::new());
        let walker = Arc::new(walker_with_pools(&snapshot, Arc::clone(&pools)));
        let running = pools
            .try_acquire(Some(1))
            .expect("a running job holds the lane");
        let order = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut runs = Vec::new();
        for (name, priority) in [
            ("queued draw", LanePriority::UserTool),
            ("transcription", LanePriority::Interactive),
        ] {
            let walker = Arc::clone(&walker);
            let order = Arc::clone(&order);
            runs.push(tokio::spawn(async move {
                let (result, _) = run_recording(&walker, None, Some(priority)).await;
                order.lock().expect("order").push(name);
                result.expect("served");
            }));
            tokio::task::yield_now().await;
        }
        assert_eq!(pools.occupancy()[0].queued, [1, 1, 0]);
        assert!(
            order.lock().expect("order").is_empty(),
            "the running job is never preempted"
        );
        drop(running);
        for run in runs {
            run.await.expect("run");
        }

        assert_eq!(
            *order.lock().expect("order"),
            vec!["transcription", "queued draw"]
        );
    }
}
