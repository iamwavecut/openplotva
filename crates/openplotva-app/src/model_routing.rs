//! Bridge between the persisted routing snapshot and the in-memory control plane.
//!
//! [`build_routing_table`] turns a [`RoutingSnapshot`] (raw DB rows) into the
//! DB-free [`RoutingTable`] the router serves. It is a pure function so it can be
//! unit-tested without a database; [`load_routing_table`] and [`reload_router`]
//! wrap it with the storage read and the atomic `ArcSwap` publish.

use std::collections::HashMap;
use std::time::Duration;

use openplotva_config::AppConfig;
use openplotva_llm::router::table::Role;
use openplotva_llm::router::triggers::{TriggerCondition, TriggerSpec};
use openplotva_llm::router::{
    BreakerConfig, Candidate, InferenceOverrides, Kind, ModelRow, ProviderRow, RetryBudget,
    RouterHandle, RoutingTable, Scope, WorkflowRoute,
};
use openplotva_storage::StorageError;
use openplotva_storage::llm_routing::{
    AssignmentInput, AssignmentRecord, ModelInput, ModelRecord, ProviderInput, RoutingSnapshot,
    TriggerInput, TriggerRecord, WorkflowRecord, insert_assignment, insert_model, insert_provider,
    insert_trigger, load_snapshot,
};
use serde_json::{Value, json};
use sqlx::PgPool;

/// Read the snapshot and build a routing table in one step.
pub async fn load_routing_table(pool: &PgPool) -> Result<RoutingTable, StorageError> {
    let snapshot = load_snapshot(pool).await?;
    Ok(build_routing_table(&snapshot))
}

/// Rebuild the table from the database and publish it atomically.
pub async fn reload_router(handle: &RouterHandle, pool: &PgPool) -> Result<(), StorageError> {
    let table = load_routing_table(pool).await?;
    handle.store(table);
    Ok(())
}

const SEEDED_KEY: &str = "llm.routing.seeded";

async fn already_seeded(pool: &PgPool) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT value FROM app_settings WHERE key = $1")
        .bind(SEEDED_KEY)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

async fn mark_seeded(pool: &PgPool) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO app_settings (key, value, updated_at) VALUES ($1, '1', NOW()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(SEEDED_KEY)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert one provider plus one model and return their ids.
async fn seed_provider_model(
    pool: &PgPool,
    provider: ProviderInput,
    model_name: &str,
    base_url: Option<&str>,
    capabilities: &[&str],
    embedding_dim: Option<i32>,
) -> Result<i64, StorageError> {
    let provider_id = insert_provider(pool, &provider).await?;
    let model = ModelInput {
        provider_id,
        model_name: model_name.to_owned(),
        display_name: None,
        base_url: base_url.map(str::to_owned),
        capabilities: capabilities.iter().map(|c| (*c).to_owned()).collect(),
        embedding_dim,
        enabled: true,
        config: json!({}),
    };
    insert_model(pool, &model).await
}

fn chat_provider(
    name: &str,
    api_key_ref: &str,
    discovery: Option<(&str, &str, &str)>,
) -> ProviderInput {
    let (endpoint, service, ep) = match discovery {
        Some((base, svc, epn)) => (
            Some(base.to_owned()),
            Some(svc.to_owned()),
            Some(epn.to_owned()),
        ),
        None => (None, None, None),
    };
    ProviderInput {
        name: name.to_owned(),
        kind: "chat".to_owned(),
        endpoint,
        discovery_service_name: service,
        discovery_endpoint_name: ep,
        api_key_ref: Some(api_key_ref.to_owned()),
        api_key_encrypted: None,
        enabled: true,
        config: json!({}),
    }
}

async fn seed_primary(
    pool: &PgPool,
    workflow: &str,
    model_id: i64,
    weight: Option<i32>,
) -> Result<i64, StorageError> {
    insert_assignment(
        pool,
        &AssignmentInput {
            workflow_key: workflow.to_owned(),
            scope: "global".to_owned(),
            role: "primary".to_owned(),
            provider_model_id: model_id,
            weight,
            fallback_order: None,
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        },
    )
    .await
}

/// Populate the routing tables from the resolved env config on first boot so the
/// big-bang cutover is behavior-neutral. Guarded by `app_settings['llm.routing.seeded']`;
/// returns `Ok(true)` if it seeded, `Ok(false)` if already seeded. Reproduces the
/// current dialog behavior (aifarm primary, genkit fallback, genkit overflow at the
/// 30/20 watermark) and seeds a primary for every other workflow. A `gemini`
/// provider with `gemini-2.5-flash` is seeded but unassigned so an operator can
/// switch the dialog overflow to it from the admin panel in one step.
pub async fn seed_routing_from_env(
    pool: &PgPool,
    config: &AppConfig,
) -> Result<bool, StorageError> {
    if already_seeded(pool).await? {
        return Ok(false);
    }

    let discovery_base = config.llm.discovery.base_url.as_str();
    let dialog = &config.llm.dialog;

    // --- chat providers/models ---
    let aifarm_model = seed_provider_model(
        pool,
        chat_provider(
            "aifarm",
            "DIALOG_API_KEY",
            Some((
                discovery_base,
                dialog.discovery_service_name.as_str(),
                dialog.discovery_endpoint_name.as_str(),
            )),
        ),
        dialog.model.as_str(),
        None,
        &["chat", "tools"],
        None,
    )
    .await?;

    let genkit_model = seed_provider_model(
        pool,
        chat_provider("genkit", "GOOGLEAI_KEY", None),
        "gemini-2.5-flash",
        None,
        &["chat", "tools"],
        None,
    )
    .await?;

    // Proprietary Gemini overflow target, available for the operator to assign.
    let _gemini_model = seed_provider_model(
        pool,
        chat_provider("gemini", "GOOGLEAI_KEY", None),
        "gemini-2.5-flash",
        None,
        &["chat", "tools"],
        None,
    )
    .await?;

    // --- non-chat providers/models ---
    let vision_model = seed_provider_model(
        pool,
        ProviderInput {
            name: "aifarm-vision".to_owned(),
            kind: "vision".to_owned(),
            endpoint: Some(discovery_base.to_owned()),
            discovery_service_name: Some(config.vision.discovery_service_name.clone()),
            discovery_endpoint_name: Some(config.vision.discovery_endpoint_name.clone()),
            api_key_ref: Some("DIALOG_API_KEY".to_owned()),
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        },
        config.vision.model.as_str(),
        None,
        &["vision"],
        None,
    )
    .await?;

    let embedding_model = seed_provider_model(
        pool,
        ProviderInput {
            name: "aifarm-embed".to_owned(),
            kind: "embedding".to_owned(),
            endpoint: Some(discovery_base.to_owned()),
            discovery_service_name: Some(config.memory.embedder_service_name.clone()),
            discovery_endpoint_name: Some(config.memory.embedder_endpoint_name.clone()),
            api_key_ref: None,
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        },
        config.memory.embedding_model.as_str(),
        None,
        &["embedding"],
        Some(512),
    )
    .await?;

    let music_model = seed_provider_model(
        pool,
        ProviderInput {
            name: "acestep".to_owned(),
            kind: "music".to_owned(),
            endpoint: Some(config.music.acestep.base_url.clone()),
            discovery_service_name: None,
            discovery_endpoint_name: None,
            api_key_ref: Some("ACESTEP_API_KEY".to_owned()),
            api_key_encrypted: None,
            enabled: config.music.acestep.enabled,
            config: json!({ "api_mode": config.music.acestep.api_mode }),
        },
        config.music.acestep.model.as_str(),
        None,
        &["music"],
        None,
    )
    .await?;

    let image_model = seed_provider_model(
        pool,
        ProviderInput {
            name: "pruna".to_owned(),
            kind: "image".to_owned(),
            endpoint: Some(config.pruna.endpoint.clone()),
            discovery_service_name: None,
            discovery_endpoint_name: None,
            api_key_ref: Some("PRUNA_BEARER".to_owned()),
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        },
        config.pruna.model.as_str(),
        None,
        &["image"],
        None,
    )
    .await?;

    // --- dialog: weighted primary + ordered fallback + queue-depth overflow ---
    seed_primary(pool, "dialog", aifarm_model, Some(100)).await?;
    insert_assignment(
        pool,
        &AssignmentInput {
            workflow_key: "dialog".to_owned(),
            scope: "global".to_owned(),
            role: "fallback".to_owned(),
            provider_model_id: genkit_model,
            weight: None,
            fallback_order: Some(0),
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        },
    )
    .await?;
    let overflow_assignment = insert_assignment(
        pool,
        &AssignmentInput {
            workflow_key: "dialog".to_owned(),
            scope: "global".to_owned(),
            role: "overflow".to_owned(),
            provider_model_id: genkit_model,
            weight: Some(100),
            fallback_order: None,
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        },
    )
    .await?;
    insert_trigger(
        pool,
        &TriggerInput {
            workflow_key: "dialog".to_owned(),
            trigger_type: "queue_depth".to_owned(),
            engage_assignment_id: overflow_assignment,
            enabled: true,
            queue_name: Some("dialog-aifarm".to_owned()),
            high_watermark: Some(
                config
                    .persistent_queue
                    .dialog_aifarm_fallback_high_watermark,
            ),
            low_watermark: Some(config.persistent_queue.dialog_aifarm_fallback_low_watermark),
            params: json!({}),
        },
    )
    .await?;

    // --- one primary per remaining workflow (config-only kinds) ---
    seed_primary(pool, "vision", vision_model, None).await?;
    seed_primary(pool, "image_generation", image_model, None).await?;
    seed_primary(pool, "embedding", embedding_model, None).await?;
    seed_primary(pool, "music", music_model, None).await?;
    for workflow in [
        "memory_consolidation",
        "history_summary",
        "agentic_search_reasoner",
        "agentic_search_writer",
        "agentic_song",
        "agentic_image",
        "media_prompt_optimizer",
    ] {
        seed_primary(pool, workflow, aifarm_model, None).await?;
    }

    mark_seeded(pool).await?;
    tracing::info!("seeded LLM routing tables from env configuration");
    Ok(true)
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

fn experiment_variant(record: &AssignmentRecord) -> Option<String> {
    if let Some(label) = record
        .inference_overrides
        .as_object()
        .and_then(|map| map.get("variant"))
        .and_then(Value::as_str)
    {
        return Some(label.to_owned());
    }
    match record.role.as_str() {
        "canary" => Some("canary".to_owned()),
        "shadow" => Some("shadow".to_owned()),
        _ => None,
    }
}

fn candidate_from_assignment(
    record: &AssignmentRecord,
    models: &HashMap<i64, &ModelRecord>,
) -> Option<Candidate> {
    let model = models.get(&record.provider_model_id)?;
    let role = Role::from_db(&record.role)?;
    Some(Candidate {
        provider: model.provider_id,
        model: model.id,
        role,
        weight: record.weight.unwrap_or(0).max(0) as u32,
        fallback_order: record.fallback_order.unwrap_or(0),
        canary_percent: record.canary_percent.unwrap_or(0).max(0) as u32,
        overrides: overrides_from_json(&record.inference_overrides),
        experiment_variant: experiment_variant(record),
        breaker: BreakerConfig {
            fail_threshold: record.cb_failure_threshold.max(1) as u32,
            cooldown: Duration::from_millis(record.cb_cooldown_ms.max(0) as u64),
        },
    })
}

fn trigger_condition(record: &TriggerRecord) -> Option<TriggerCondition> {
    match record.trigger_type.as_str() {
        "queue_depth" => Some(TriggerCondition::QueueDepth {
            queue: record.queue_name.clone().unwrap_or_default(),
            high: record.high_watermark.unwrap_or(0).max(0) as usize,
            low: record.low_watermark.unwrap_or(0).max(0) as usize,
        }),
        "error_rate" => {
            let params = record.params.as_object();
            let provider = params
                .and_then(|map| map.get("provider_id"))
                .and_then(Value::as_i64)?;
            let model = params
                .and_then(|map| map.get("model_id"))
                .and_then(Value::as_i64)?;
            let threshold = params
                .and_then(|map| map.get("threshold"))
                .and_then(Value::as_f64)
                .unwrap_or(0.5);
            let window_s = params
                .and_then(|map| map.get("window_s"))
                .and_then(Value::as_u64)
                .unwrap_or(60);
            Some(TriggerCondition::ErrorRate {
                provider,
                model,
                threshold,
                window: Duration::from_secs(window_s),
            })
        }
        "time_of_day" => {
            let params = record.params.as_object();
            let start = params
                .and_then(|map| map.get("start"))
                .and_then(Value::as_str)
                .and_then(parse_hh_mm)?;
            let end = params
                .and_then(|map| map.get("end"))
                .and_then(Value::as_str)
                .and_then(parse_hh_mm)?;
            Some(TriggerCondition::TimeOfDay {
                start_minute: start,
                end_minute: end,
            })
        }
        _ => None,
    }
}

/// Parse `"HH:MM"` into minutes since midnight (0..1440).
fn parse_hh_mm(value: &str) -> Option<u32> {
    let (hh, mm) = value.split_once(':')?;
    let hours: u32 = hh.trim().parse().ok()?;
    let minutes: u32 = mm.trim().parse().ok()?;
    if hours >= 24 || minutes >= 60 {
        return None;
    }
    Some(hours * 60 + minutes)
}

/// Convert a persisted snapshot into the in-memory routing table.
#[must_use]
pub fn build_routing_table(snapshot: &RoutingSnapshot) -> RoutingTable {
    let mut providers: HashMap<i64, ProviderRow> = HashMap::new();
    for record in &snapshot.providers {
        let Some(kind) = Kind::from_db(&record.kind) else {
            tracing::warn!(provider = %record.name, kind = %record.kind, "skipping provider with unknown kind");
            continue;
        };
        providers.insert(
            record.id,
            ProviderRow {
                id: record.id,
                name: record.name.clone(),
                kind,
                endpoint: record.endpoint.clone(),
                discovery_service_name: record.discovery_service_name.clone(),
                discovery_endpoint_name: record.discovery_endpoint_name.clone(),
                enabled: record.enabled,
                config: record.config.clone(),
            },
        );
    }

    let mut model_rows: HashMap<i64, ModelRow> = HashMap::new();
    let model_records: HashMap<i64, &ModelRecord> =
        snapshot.models.iter().map(|m| (m.id, m)).collect();
    for record in &snapshot.models {
        model_rows.insert(
            record.id,
            ModelRow {
                id: record.id,
                provider: record.provider_id,
                model_name: record.model_name.clone(),
                base_url: record.base_url.clone(),
                capabilities: record.capabilities.clone(),
                embedding_dim: record.embedding_dim,
                enabled: record.enabled,
                config: record.config.clone(),
            },
        );
    }

    let workflows: HashMap<&str, &WorkflowRecord> = snapshot
        .workflows
        .iter()
        .map(|w| (w.key.as_str(), w))
        .collect();

    // Triggers grouped by workflow (the schema does not scope triggers).
    let mut triggers_by_workflow: HashMap<&str, Vec<&TriggerRecord>> = HashMap::new();
    for record in &snapshot.triggers {
        if record.enabled {
            triggers_by_workflow
                .entry(record.workflow_key.as_str())
                .or_default()
                .push(record);
        }
    }

    // Assignments indexed by (workflow, scope).
    let mut by_key_scope: HashMap<(&str, &str), Vec<&AssignmentRecord>> = HashMap::new();
    for record in &snapshot.assignments {
        if record.enabled {
            by_key_scope
                .entry((record.workflow_key.as_str(), record.scope.as_str()))
                .or_default()
                .push(record);
        }
    }

    let mut table = RoutingTable::new(providers, model_rows);

    for (&(workflow_key, scope_str), assignments) in &by_key_scope {
        let Some(workflow) = workflows.get(workflow_key) else {
            continue;
        };
        let scope = match scope_str {
            "vip" => Scope::Vip,
            _ => Scope::Global,
        };
        let Some(route) = build_route(workflow, assignments, &triggers_by_workflow, &model_records)
        else {
            continue;
        };
        table.set_route(scope, route);
    }

    table
}

fn build_route(
    workflow: &WorkflowRecord,
    assignments: &[&AssignmentRecord],
    triggers_by_workflow: &HashMap<&str, Vec<&TriggerRecord>>,
    models: &HashMap<i64, &ModelRecord>,
) -> Option<WorkflowRoute> {
    let kind = Kind::from_db(&workflow.kind)?;
    let mut primary = Vec::new();
    let mut fallback_tail = Vec::new();
    let mut canary = Vec::new();
    let mut shadow = Vec::new();

    for record in assignments {
        let Some(candidate) = candidate_from_assignment(record, models) else {
            continue;
        };
        match candidate.role {
            Role::Primary => primary.push(candidate),
            Role::Fallback => fallback_tail.push(candidate),
            // Config-only workflows ignore weighting/experiments entirely.
            Role::Canary if workflow.full_routing => canary.push(candidate),
            Role::Shadow if workflow.full_routing => shadow.push(candidate),
            // Overflow candidates are attached via triggers below.
            _ => {}
        }
    }

    // Resolve triggers to their engaged overflow candidates (full routing only).
    let mut triggers = Vec::new();
    if workflow.full_routing
        && let Some(records) = triggers_by_workflow.get(workflow.key.as_str())
    {
        let assignment_by_id: HashMap<i64, &AssignmentRecord> =
            assignments.iter().map(|a| (a.id, *a)).collect();
        for record in records {
            let Some(condition) = trigger_condition(record) else {
                continue;
            };
            let Some(engage_record) = assignment_by_id.get(&record.engage_assignment_id) else {
                // The engaged assignment may be a different scope; skip if absent here.
                continue;
            };
            let Some(mut engage) = candidate_from_assignment(engage_record, models) else {
                continue;
            };
            engage.role = Role::Overflow;
            triggers.push(TriggerSpec {
                id: record.id,
                condition,
                engage,
            });
        }
    }

    Some(WorkflowRoute {
        key: workflow.key.clone(),
        kind,
        full_routing: workflow.full_routing,
        primary,
        fallback_tail,
        canary,
        triggers,
        shadow,
        retry: RetryBudget {
            max_hops: workflow.retry_max_hops.max(1) as u32,
            wall_clock: Duration::from_millis(workflow.retry_wall_ms.max(0) as u64),
        },
    })
}

#[cfg(test)]
mod tests {
    use openplotva_llm::router::{BreakerLiveness, BreakerSet, TriggerState, select};
    use openplotva_storage::llm_routing::{
        AssignmentRecord, ModelRecord, ProviderRecord, RoutingSnapshot, TriggerRecord,
        WorkflowRecord,
    };
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use serde_json::json;

    use super::*;

    fn provider(id: i64, name: &str, kind: &str) -> ProviderRecord {
        ProviderRecord {
            id,
            name: name.to_owned(),
            kind: kind.to_owned(),
            endpoint: None,
            discovery_service_name: None,
            discovery_endpoint_name: None,
            api_key_ref: Some("REF".to_owned()),
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        }
    }

    fn model(id: i64, provider_id: i64, name: &str) -> ModelRecord {
        ModelRecord {
            id,
            provider_id,
            model_name: name.to_owned(),
            display_name: None,
            base_url: None,
            capabilities: vec!["chat".to_owned()],
            embedding_dim: None,
            enabled: true,
            config: json!({}),
        }
    }

    fn assignment(
        id: i64,
        workflow: &str,
        scope: &str,
        role: &str,
        model_id: i64,
        weight: Option<i32>,
    ) -> AssignmentRecord {
        AssignmentRecord {
            id,
            workflow_key: workflow.to_owned(),
            scope: scope.to_owned(),
            role: role.to_owned(),
            provider_model_id: model_id,
            weight,
            fallback_order: Some(0),
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        }
    }

    fn dialog_snapshot() -> RoutingSnapshot {
        RoutingSnapshot {
            providers: vec![
                provider(1, "aifarm", "chat"),
                provider(2, "genkit", "chat"),
                provider(3, "gemini", "chat"),
            ],
            models: vec![
                model(10, 1, "Gemma 4 26B Heretic"),
                model(20, 2, "genkit-default"),
                model(30, 3, "gemini-2.5-flash"),
            ],
            workflows: vec![WorkflowRecord {
                key: "dialog".to_owned(),
                kind: "chat".to_owned(),
                full_routing: true,
                retry_max_hops: 3,
                retry_wall_ms: 60_000,
                enabled: true,
            }],
            assignments: vec![
                assignment(100, "dialog", "global", "primary", 10, Some(100)),
                assignment(101, "dialog", "global", "fallback", 20, None),
                assignment(102, "dialog", "global", "overflow", 30, Some(100)),
            ],
            triggers: vec![TriggerRecord {
                id: 200,
                workflow_key: "dialog".to_owned(),
                trigger_type: "queue_depth".to_owned(),
                engage_assignment_id: 102,
                enabled: true,
                queue_name: Some("dialog-aifarm".to_owned()),
                high_watermark: Some(30),
                low_watermark: Some(20),
                params: json!({}),
            }],
        }
    }

    #[test]
    fn builds_dialog_route_with_primary_fallback_and_trigger() {
        let table = build_routing_table(&dialog_snapshot());
        let route = table.resolve("dialog", false).expect("dialog route");
        assert_eq!(route.primary.len(), 1);
        assert_eq!(route.primary[0].provider, 1);
        assert_eq!(route.fallback_tail.len(), 1);
        assert_eq!(route.fallback_tail[0].provider, 2);
        assert_eq!(route.triggers.len(), 1);
        assert_eq!(route.triggers[0].engage.provider, 3);
    }

    #[test]
    fn reproduces_current_behavior_without_trigger_then_overflow_when_engaged() {
        let table = build_routing_table(&dialog_snapshot());
        let route = table.resolve("dialog", false).expect("route");
        let breakers = BreakerSet::new();
        let liveness = BreakerLiveness::new(&breakers, std::time::Instant::now());
        let mut rng = StdRng::seed_from_u64(1);

        // No trigger engaged: aifarm primary, then genkit fallback. No gemini.
        let idle = TriggerState::new();
        let attempts = select(route, &liveness, &idle, &mut rng);
        assert_eq!(attempts[0].provider, 1);
        assert!(attempts.iter().all(|a| a.provider != 3));

        // Queue-depth trigger engaged: gemini overflow joins the pool.
        let engaged = TriggerState::new();
        engaged.set_engaged(200, true);
        let attempts = select(route, &liveness, &engaged, &mut rng);
        assert!(attempts.iter().any(|a| a.provider == 3));
    }

    #[test]
    fn vip_scope_overrides_global() {
        let mut snapshot = dialog_snapshot();
        snapshot
            .assignments
            .push(assignment(110, "dialog", "vip", "primary", 30, Some(100)));
        let table = build_routing_table(&snapshot);
        // VIP caller gets the gemini-backed vip primary; non-VIP gets aifarm.
        assert_eq!(
            table.resolve("dialog", true).expect("vip").primary[0].provider,
            3
        );
        assert_eq!(
            table.resolve("dialog", false).expect("global").primary[0].provider,
            1
        );
    }
}
