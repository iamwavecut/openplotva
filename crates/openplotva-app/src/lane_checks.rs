//! Checks that lane pools match the farm: every routed farm model sits in a
//! lane pool, a Discovery service never splits across pools, and a pool is not
//! wider than the services it spans. Mismatches are reported, never enforced:
//! routing keeps the bot's configured widths.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use openplotva_llm::router::{PoolId, RoutingTable};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneConfigIssue {
    pub kind: &'static str,
    pub pool_id: Option<PoolId>,
    pub detail: String,
}

/// Invariant violations of `table` given each Discovery service's
/// `max_concurrent_jobs`. Services missing from `discovery_widths` are not
/// width-checked.
#[must_use]
pub fn lane_config_issues(
    table: &RoutingTable,
    discovery_widths: &HashMap<String, u32>,
) -> Vec<LaneConfigIssue> {
    let mut issues = Vec::new();
    let mut pools_by_service: BTreeMap<String, BTreeSet<PoolId>> = BTreeMap::new();
    let mut services_by_pool: BTreeMap<PoolId, BTreeSet<String>> = BTreeMap::new();
    for model in table.routed_models().filter(|model| model.enabled) {
        let Some(service) = farm_service(table, model.provider) else {
            continue;
        };
        match model.pool_id {
            Some(pool) => {
                pools_by_service
                    .entry(service.clone())
                    .or_default()
                    .insert(pool);
                services_by_pool.entry(pool).or_default().insert(service);
            }
            None => issues.push(LaneConfigIssue {
                kind: "unpooled_farm_model",
                pool_id: None,
                detail: format!(
                    "model {} reaches {service} without a lane pool",
                    model.model_name
                ),
            }),
        }
    }
    for (service, pools) in &pools_by_service {
        if pools.len() > 1 {
            issues.push(LaneConfigIssue {
                kind: "split_service",
                pool_id: None,
                detail: format!("{service} is split across lane pools {pools:?}"),
            });
        }
    }
    for (pool, services) in &services_by_pool {
        let Some(width) = table.pool(*pool).and_then(|spec| spec.max_concurrency) else {
            continue;
        };
        let Some(narrowest) = services
            .iter()
            .filter_map(|service| discovery_widths.get(service).copied())
            .min()
        else {
            continue;
        };
        let mismatch = if services.len() == 1 {
            width != narrowest
        } else {
            width > narrowest
        };
        if mismatch {
            issues.push(LaneConfigIssue {
                kind: "width_mismatch",
                pool_id: Some(*pool),
                detail: format!(
                    "lane pool {pool} has {width} lanes; Discovery allows {narrowest} for {services:?}"
                ),
            });
        }
    }
    issues
}

/// Discovery services that enabled, routed models reach.
#[must_use]
pub fn routed_farm_services(table: &RoutingTable) -> Vec<String> {
    let services: BTreeSet<String> = table
        .routed_models()
        .filter(|model| model.enabled)
        .filter_map(|model| farm_service(table, model.provider))
        .collect();
    services.into_iter().collect()
}

fn farm_service(table: &RoutingTable, provider: i64) -> Option<String> {
    table
        .provider(provider)
        .filter(|provider| provider.enabled)
        .and_then(|provider| provider.discovery_service_name.as_deref())
        .map(str::trim)
        .filter(|service| !service.is_empty())
        .map(str::to_owned)
}

/// Runs the lane configuration check against the live Discovery widths and
/// records each mismatch as a `lane_config_mismatch` routing event.
#[derive(Clone)]
pub struct LaneCheck {
    pub discovery_base_url: String,
    pub reporter: Option<crate::runtime_routing::RoutingEventReporter>,
}

/// Result of one lane configuration check.
#[derive(Debug, Default)]
pub struct LaneCheckReport {
    pub issues: Vec<LaneConfigIssue>,
    /// Services whose width could not be read from Discovery.
    pub unreachable: Vec<String>,
}

impl LaneCheckReport {
    /// One-line readiness detail.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut summary = if self.issues.is_empty() {
            "lane pools match Discovery".to_owned()
        } else {
            let kinds: BTreeSet<&str> = self.issues.iter().map(|issue| issue.kind).collect();
            format!(
                "{} lane config mismatches ({})",
                self.issues.len(),
                kinds.into_iter().collect::<Vec<_>>().join(", ")
            )
        };
        if !self.unreachable.is_empty() {
            summary.push_str(&format!(
                "; widths unchecked for {}",
                self.unreachable.join(", ")
            ));
        }
        summary
    }
}

impl LaneCheck {
    pub async fn run(&self, table: &RoutingTable) -> LaneCheckReport {
        let services = routed_farm_services(table);
        let mut fetches = tokio::task::JoinSet::new();
        for service in services {
            let client = crate::runtime_llm_analytics::DiscoveryCapacityClient::new(
                self.discovery_base_url.clone(),
                service.clone(),
            );
            fetches.spawn(async move { (service, client.fetch().await) });
        }
        let mut widths = HashMap::new();
        let mut unreachable = Vec::new();
        while let Some(fetched) = fetches.join_next().await {
            let Ok((service, capacity)) = fetched else {
                continue;
            };
            match u32::try_from(capacity.max_concurrent_jobs) {
                Ok(width) if capacity.error.is_none() && width > 0 => {
                    widths.insert(service, width);
                }
                _ => unreachable.push(service),
            }
        }
        unreachable.sort();
        let issues = lane_config_issues(table, &widths);
        for issue in &issues {
            tracing::warn!(kind = issue.kind, pool_id = ?issue.pool_id, detail = %issue.detail, "lane pool configuration does not match the farm");
            if let Some(reporter) = &self.reporter {
                reporter.record(lane_config_mismatch_event(issue));
            }
        }
        LaneCheckReport {
            issues,
            unreachable,
        }
    }
}

/// Runtime API view of the live lane pools.
pub struct RuntimeLanes {
    pub handle: std::sync::Arc<openplotva_llm::router::RouterHandle>,
    pub pools: std::sync::Arc<openplotva_llm::router::PoolRegistry>,
}

impl openplotva_server::RuntimeLaneInspector for RuntimeLanes {
    fn lanes(&self) -> Vec<openplotva_server::RuntimeLaneData> {
        let table = self.handle.snapshot();
        let clamp = |value: u64| i32::try_from(value).unwrap_or(i32::MAX);
        self.pools
            .occupancy()
            .into_iter()
            .map(|lane| openplotva_server::RuntimeLaneData {
                id: lane.id,
                name: table.pool_name(lane.id).unwrap_or_default().to_owned(),
                max_concurrency: lane.max_concurrency.map(|width| clamp(u64::from(width))),
                in_flight: clamp(u64::from(lane.in_flight)),
                queued_interactive: clamp(u64::from(lane.queued[0])),
                queued_user_tool: clamp(u64::from(lane.queued[1])),
                queued_background: clamp(u64::from(lane.queued[2])),
                mean_hold_ms: lane.mean_hold_ms.map(clamp),
                wait_p50_ms: lane.wait_p50_ms.map(clamp),
                wait_p95_ms: lane.wait_p95_ms.map(clamp),
            })
            .collect()
    }
}

fn lane_config_mismatch_event(issue: &LaneConfigIssue) -> crate::runtime_routing::RoutingEvent {
    crate::runtime_routing::RoutingEvent {
        severity: "warn".to_owned(),
        event_type: "lane_config_mismatch".to_owned(),
        workflow_key: "lanes".to_owned(),
        provider_id: None,
        model_id: None,
        queue_name: None,
        job_id: None,
        chat_id: None,
        user_id: None,
        thread_id: None,
        message_id: None,
        dedupe_key: format!(
            "lane_config_mismatch:{}:{}",
            issue.kind,
            issue.pool_id.unwrap_or_default()
        ),
        summary: issue.detail.clone(),
        detail: serde_json::json!({
            "kind": issue.kind,
            "pool_id": issue.pool_id,
            "detail": issue.detail,
        }),
    }
}

#[cfg(test)]
mod tests {
    use openplotva_storage::llm_routing::{
        AssignmentRecord, ModelRecord, PoolRecord, ProviderRecord, RoutingSnapshot, WorkflowRecord,
    };
    use serde_json::json;

    use super::*;

    fn provider(id: i64, name: &str, kind: &str, service: Option<&str>) -> ProviderRecord {
        ProviderRecord {
            id,
            name: name.to_owned(),
            kind: kind.to_owned(),
            protocol: None,
            runtime_hint: None,
            endpoint: None,
            discovery_service_name: service.map(str::to_owned),
            discovery_endpoint_name: None,
            api_key_ref: None,
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        }
    }

    fn model(id: i64, provider_id: i64, capability: &str, pool_id: Option<i64>) -> ModelRecord {
        ModelRecord {
            id,
            provider_id,
            model_name: format!("model-{id}"),
            display_name: None,
            base_url: None,
            capabilities: vec![capability.to_owned()],
            embedding_dim: None,
            pool_id,
            enabled: true,
            config: json!({}),
        }
    }

    fn workflow(key: &str, kind: &str) -> WorkflowRecord {
        WorkflowRecord {
            key: key.to_owned(),
            kind: kind.to_owned(),
            full_routing: false,
            retry_max_hops: 2,
            retry_wall_ms: 60_000,
            enabled: true,
            lane_priority: 0,
            fallback_reserve_ms: 30_000,
        }
    }

    fn assignment(id: i64, workflow: &str, role: &str, model_id: i64) -> AssignmentRecord {
        AssignmentRecord {
            id,
            workflow_key: workflow.to_owned(),
            scope: "global".to_owned(),
            role: role.to_owned(),
            provider_model_id: model_id,
            weight: Some(100),
            fallback_order: Some(0),
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        }
    }

    fn pool(id: i64, name: &str, width: i32) -> PoolRecord {
        PoolRecord {
            id,
            name: name.to_owned(),
            max_concurrency: Some(width),
            description: None,
            config: json!({}),
        }
    }

    /// GPU0 dialog (llm-openai, 8 lanes) with a hosted fallback, GPU0 vision,
    /// and GPU1 draw plus speech recognition on one sequential lane.
    fn farm() -> RoutingSnapshot {
        RoutingSnapshot {
            providers: vec![
                provider(1, "aifarm-vllm-gpu0", "chat", Some("llm-openai")),
                provider(2, "aifarm-vision", "vision", Some("llm-openai")),
                provider(3, "openrouter", "chat", None),
                provider(4, "aifarm-draw", "image", Some("draw-api")),
                provider(5, "aifarm-asr", "asr", Some("asr-api")),
            ],
            models: vec![
                model(10, 1, "chat", Some(1)),
                model(20, 2, "vision", Some(1)),
                model(30, 3, "chat", None),
                model(40, 4, "image", Some(3)),
                model(50, 5, "asr", Some(3)),
            ],
            workflows: vec![
                workflow("dialog", "chat"),
                workflow("vision", "vision"),
                workflow("image_generation", "image"),
                workflow("voice_transcription", "asr"),
            ],
            assignments: vec![
                assignment(100, "dialog", "primary", 10),
                assignment(101, "dialog", "fallback", 30),
                assignment(102, "vision", "primary", 20),
                assignment(103, "image_generation", "primary", 40),
                assignment(104, "voice_transcription", "primary", 50),
            ],
            triggers: vec![],
            pools: vec![
                pool(1, "aifarm-gpu0-dialog", 8),
                pool(3, "aifarm-gpu1-media", 1),
            ],
        }
    }

    fn widths() -> HashMap<String, u32> {
        HashMap::from([
            ("llm-openai".to_owned(), 8),
            ("draw-api".to_owned(), 1),
            ("asr-api".to_owned(), 1),
        ])
    }

    fn issues(snapshot: &RoutingSnapshot, widths: &HashMap<String, u32>) -> Vec<&'static str> {
        let table = crate::model_routing::build_routing_table(snapshot);
        lane_config_issues(&table, widths)
            .into_iter()
            .map(|issue| issue.kind)
            .collect()
    }

    #[test]
    fn matching_lanes_and_a_shared_sequential_lane_are_valid() {
        assert!(issues(&farm(), &widths()).is_empty());
    }

    #[test]
    fn a_routed_farm_model_without_a_lane_pool_is_reported() {
        let mut snapshot = farm();
        snapshot.models[4].pool_id = None;
        assert_eq!(issues(&snapshot, &widths()), vec!["unpooled_farm_model"]);
    }

    #[test]
    fn a_service_split_across_pools_is_reported() {
        let mut snapshot = farm();
        snapshot.pools.push(pool(6, "aifarm-gpu2-utility", 8));
        snapshot.models[1].pool_id = Some(6);
        assert!(issues(&snapshot, &widths()).contains(&"split_service"));
    }

    #[test]
    fn a_shared_lane_wider_than_any_of_its_services_is_reported() {
        let mut snapshot = farm();
        snapshot.pools[1].max_concurrency = Some(2);
        assert_eq!(issues(&snapshot, &widths()), vec!["width_mismatch"]);
    }

    #[test]
    fn a_single_service_pool_must_match_its_width_exactly() {
        let mut snapshot = farm();
        snapshot.pools[0].max_concurrency = Some(4);
        assert_eq!(issues(&snapshot, &widths()), vec!["width_mismatch"]);
    }

    #[test]
    fn unknown_discovery_widths_are_not_checked() {
        let mut snapshot = farm();
        snapshot.pools[1].max_concurrency = Some(2);
        assert!(issues(&snapshot, &HashMap::new()).is_empty());
    }

    async fn discovery_stub(widths: &'static [(&'static str, u32)]) -> String {
        let app = axum::Router::new().route(
            "/v1/services/{name}/capacity",
            axum::routing::get(
                move |axum::extract::Path(name): axum::extract::Path<String>| async move {
                    match widths.iter().find(|(service, _)| *service == name) {
                        Some((_, width)) => (
                            axum::http::StatusCode::OK,
                            axum::Json(json!({
                                "capacity": {"service_name": name, "max_concurrent_jobs": width}
                            })),
                        ),
                        None => (
                            axum::http::StatusCode::NOT_FOUND,
                            axum::Json(json!({"detail": "service not found"})),
                        ),
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn the_check_reads_discovery_widths_and_records_each_mismatch() {
        let base_url = discovery_stub(&[("llm-openai", 8), ("draw-api", 1), ("asr-api", 1)]).await;
        let mut snapshot = farm();
        snapshot.pools[1].max_concurrency = Some(2);
        let reporter = crate::runtime_routing::RoutingEventReporter::new(
            crate::runtime_routing::RoutingEventBuffer::new(8),
            None,
            None,
        );
        let check = LaneCheck {
            discovery_base_url: base_url,
            reporter: Some(reporter.clone()),
        };

        let report = check
            .run(&crate::model_routing::build_routing_table(&snapshot))
            .await;

        assert_eq!(report.unreachable, Vec::<String>::new());
        assert_eq!(report.issues.len(), 1);
        assert_eq!(
            report.summary(),
            "1 lane config mismatches (width_mismatch)"
        );
        let events = reporter.buffer().routing_events(8);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "lane_config_mismatch");
        assert_eq!(events[0].severity, "warn");
    }

    #[tokio::test]
    async fn services_missing_from_discovery_are_left_unchecked() {
        let base_url = discovery_stub(&[("llm-openai", 8)]).await;
        let check = LaneCheck {
            discovery_base_url: base_url,
            reporter: None,
        };

        let report = check
            .run(&crate::model_routing::build_routing_table(&farm()))
            .await;

        assert!(report.issues.is_empty());
        assert_eq!(report.unreachable, vec!["asr-api", "draw-api"]);
        assert_eq!(
            report.summary(),
            "lane pools match Discovery; widths unchecked for asr-api, draw-api"
        );
    }

    #[tokio::test]
    async fn the_runtime_api_reports_lanes_by_name_with_their_queues() {
        let table = crate::model_routing::build_routing_table(&farm());
        let pools = std::sync::Arc::new(openplotva_llm::router::PoolRegistry::new());
        pools.apply(&table.pool_specs());
        let handle = openplotva_llm::router::RouterHandle::new(table);
        let _running = pools.try_acquire(Some(3)).expect("GPU1 lane");
        let queued = {
            let pools = std::sync::Arc::clone(&pools);
            tokio::spawn(async move {
                pools
                    .acquire(
                        Some(3),
                        openplotva_llm::router::LanePriority::UserTool,
                        None,
                    )
                    .await
            })
        };
        tokio::task::yield_now().await;

        let lanes = openplotva_server::RuntimeLaneInspector::lanes(&RuntimeLanes {
            handle,
            pools: std::sync::Arc::clone(&pools),
        });

        assert_eq!(lanes.len(), 2);
        assert_eq!(lanes[0].name, "aifarm-gpu0-dialog");
        assert_eq!(lanes[0].max_concurrency, Some(8));
        assert_eq!(lanes[1].name, "aifarm-gpu1-media");
        assert_eq!(lanes[1].in_flight, 1);
        assert_eq!(lanes[1].queued_user_tool, 1);
        queued.abort();
    }

    #[test]
    fn unrouted_and_disabled_models_are_ignored() {
        let mut snapshot = farm();
        snapshot.models.push(model(60, 5, "asr", None));
        snapshot.providers[4].enabled = false;
        snapshot.models[4].pool_id = None;
        assert!(issues(&snapshot, &widths()).is_empty());
    }
}
