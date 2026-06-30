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
    AssignmentInput, AssignmentRecord, ModelInput, ModelRecord, ProviderInput, ProviderRecord,
    RoutingSnapshot, TriggerInput, TriggerRecord, WorkflowRecord, insert_assignment, insert_model,
    insert_provider, insert_trigger, insert_workflow_if_missing, list_assignments, list_models,
    list_providers, load_snapshot, update_assignment, update_model, update_provider,
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

async fn app_setting_present(pool: &PgPool, key: &str) -> Result<bool, StorageError> {
    let row = sqlx::query("SELECT value FROM app_settings WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(row.is_some())
}

async fn mark_app_setting(pool: &PgPool, key: &str) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO app_settings (key, value, updated_at) VALUES ($1, '1', NOW()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
    )
    .bind(key)
    .execute(pool)
    .await?;
    Ok(())
}

async fn already_seeded(pool: &PgPool) -> Result<bool, StorageError> {
    app_setting_present(pool, SEEDED_KEY).await
}

async fn mark_seeded(pool: &PgPool) -> Result<(), StorageError> {
    mark_app_setting(pool, SEEDED_KEY).await
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
/// 30/20 watermark) and seeds a primary for every other workflow. The genkit fallback
/// model mirrors `genkit_runtime_default_model` (the flash-lite default every other
/// genkit flow already uses), never the heavier `gemini-2.5-flash`. A `gemini` provider
/// mirroring that model is seeded but unassigned so an operator can switch the dialog
/// overflow to it from the admin panel in one step.
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

    // The genkit dialog fallback uses the same model every other genkit flow resolves to
    // (flash-lite by default), so the cutover never introduces the heavier gemini-2.5-flash.
    let genkit_default = crate::memory_runtime::genkit_runtime_default_model(config);
    let genkit_model = seed_provider_model(
        pool,
        chat_provider("genkit", "GOOGLEAI_KEY", None),
        genkit_default.as_str(),
        None,
        &["chat", "tools"],
        None,
    )
    .await?;

    // Proprietary Gemini overflow target, available for the operator to assign.
    let _gemini_model = seed_provider_model(
        pool,
        chat_provider("gemini", "GOOGLEAI_KEY", None),
        genkit_default.as_str(),
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
            name: DRAW_API_PROVIDER.to_owned(),
            kind: "image".to_owned(),
            endpoint: Some(config.llm.discovery.base_url.clone()),
            discovery_service_name: Some(
                crate::image_jobs::AIFARM_DRAW_API_SERVICE_NAME.to_owned(),
            ),
            discovery_endpoint_name: Some(
                crate::image_jobs::AIFARM_DRAW_API_ENDPOINT_NAME.to_owned(),
            ),
            api_key_ref: None,
            api_key_encrypted: None,
            enabled: true,
            config: json!({
                "service_name": crate::image_jobs::AIFARM_DRAW_API_SERVICE_NAME,
                "endpoint_name": crate::image_jobs::AIFARM_DRAW_API_ENDPOINT_NAME,
            }),
        },
        DRAW_API_MODEL,
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

/// Provider name for the direct OpenAI-compatible VRAM Cloud endpoint.
const VRAM_CLOUD_PROVIDER: &str = "vram-cloud";

/// Lift the env-configured VRAM Cloud model list (`DIALOG_AIFARM_POOL_*`) into
/// managed DB rows so the models show up in Studio and join the weighted dialog
/// allocation. Idempotent: skips once the provider exists, so it backfills an
/// already-seeded database on the next boot without duplicating rows. Imported
/// models default to weight 33 against the Gemma primary's 100 (~60/20/20); the
/// operator rebalances in Studio.
pub async fn backfill_vram_cloud_from_env(
    pool: &PgPool,
    config: &AppConfig,
) -> Result<bool, StorageError> {
    let dialog = &config.llm.dialog;
    if dialog.aifarm_pool_models.is_empty() {
        return Ok(false);
    }
    if list_providers(pool)
        .await?
        .iter()
        .any(|p| p.name == VRAM_CLOUD_PROVIDER)
    {
        return Ok(false);
    }

    let provider_id = insert_provider(
        pool,
        &ProviderInput {
            name: VRAM_CLOUD_PROVIDER.to_owned(),
            kind: "chat".to_owned(),
            endpoint: dialog.aifarm_pool_base_urls.first().cloned(),
            discovery_service_name: None,
            discovery_endpoint_name: None,
            api_key_ref: Some("DIALOG_AIFARM_POOL_API_KEY".to_owned()),
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        },
    )
    .await?;

    for (i, model_name) in dialog.aifarm_pool_models.iter().enumerate() {
        let base_url = dialog
            .aifarm_pool_base_urls
            .get(i)
            .or_else(|| dialog.aifarm_pool_base_urls.first())
            .cloned();
        let model_id = insert_model(
            pool,
            &ModelInput {
                provider_id,
                model_name: model_name.clone(),
                display_name: None,
                base_url,
                capabilities: vec!["chat".to_owned(), "tools".to_owned()],
                embedding_dim: None,
                enabled: true,
                config: json!({}),
            },
        )
        .await?;
        seed_primary(pool, "dialog", model_id, Some(33)).await?;
    }

    tracing::info!(
        models = dialog.aifarm_pool_models.len(),
        "backfilled VRAM Cloud models into routing tables"
    );
    Ok(true)
}

const GPU_BACKFILL_KEY: &str = "llm.routing.gpu_backfilled";
const DIALOG_QWEN_FALLBACK_KEY: &str = "llm.routing.dialog_qwen_fallback";

/// Provider name for a discovery service: the default dialog service stays on the
/// existing `aifarm` provider; other services (the GPU2 llama.cpp Qwen multiserver)
/// get a deterministic `aifarm-<service>` provider so their models are collision-free.
fn gpu_provider_name(service: &str, config: &AppConfig) -> String {
    if service == config.llm.dialog.discovery_service_name {
        "aifarm".to_owned()
    } else {
        format!(
            "aifarm-{}",
            service
                .trim_start_matches("llm-openai-")
                .trim_start_matches("llm-")
        )
    }
}

fn assignment_input_from_record(
    record: &AssignmentRecord,
    provider_model_id: i64,
) -> AssignmentInput {
    AssignmentInput {
        workflow_key: record.workflow_key.clone(),
        scope: record.scope.clone(),
        role: record.role.clone(),
        provider_model_id,
        weight: record.weight,
        fallback_order: record.fallback_order,
        canary_percent: record.canary_percent,
        enabled: record.enabled,
        inference_overrides: record.inference_overrides.clone(),
        cb_failure_threshold: record.cb_failure_threshold,
        cb_cooldown_ms: record.cb_cooldown_ms,
    }
}

/// Lift the GPU-served Qwen/llama.cpp models that the agentic reasoner and the memory
/// pipeline actually use into managed DB rows, and repoint those config-only workflows
/// at them. The initial seed flattened these to the dialog Gemma model; this corrects an
/// already-seeded database (idempotent via the `gpu_backfilled` flag) so the GPU2 models
/// (`vibethinker-3b` for memory/history, `qwen3.6-27b-moq` for the search reasoner)
/// show up in the admin and drive the right workflows. Values are read from config, so it
/// adapts to whatever each flow is configured to use.
pub async fn backfill_gpu_models(pool: &PgPool, config: &AppConfig) -> Result<bool, StorageError> {
    if app_setting_present(pool, GPU_BACKFILL_KEY).await? {
        return Ok(false);
    }

    let memory = &config.memory;
    let reasoner = crate::agent_runtime::qwen_reasoner_named_provider_config(config);
    // (workflow, discovery service, endpoint, model) each flow really resolves to.
    let targets: Vec<(&str, String, String, String)> = vec![
        (
            "memory_consolidation",
            memory.aifarm_service_name.clone(),
            memory.aifarm_endpoint_name.clone(),
            memory.consolidation_model.clone(),
        ),
        (
            "history_summary",
            memory.aifarm_service_name.clone(),
            memory.aifarm_endpoint_name.clone(),
            memory.consolidation_model.clone(),
        ),
        (
            "agentic_search_reasoner",
            reasoner.discovery_service_name.clone(),
            reasoner.discovery_endpoint_name.clone(),
            reasoner.model.clone(),
        ),
    ]
    .into_iter()
    .filter(|(_, service, _, model)| !service.trim().is_empty() && !model.trim().is_empty())
    .collect();
    if targets.is_empty() {
        return Ok(false);
    }

    let mut provider_ids: std::collections::HashMap<String, i64> = list_providers(pool)
        .await?
        .into_iter()
        .map(|p| (p.name, p.id))
        .collect();
    let mut model_ids: std::collections::HashMap<(i64, String), i64> = list_models(pool)
        .await?
        .into_iter()
        .map(|m| ((m.provider_id, m.model_name), m.id))
        .collect();
    let assignments = list_assignments(pool).await?;

    for (workflow, service, endpoint, model) in &targets {
        let provider_name = gpu_provider_name(service, config);
        let provider_id = match provider_ids.get(&provider_name) {
            Some(id) => *id,
            None => {
                let id = insert_provider(
                    pool,
                    &ProviderInput {
                        name: provider_name.clone(),
                        kind: "chat".to_owned(),
                        endpoint: None,
                        discovery_service_name: Some(service.clone()),
                        discovery_endpoint_name: Some(endpoint.clone()),
                        api_key_ref: Some("DIALOG_API_KEY".to_owned()),
                        api_key_encrypted: None,
                        enabled: true,
                        config: json!({}),
                    },
                )
                .await?;
                provider_ids.insert(provider_name.clone(), id);
                id
            }
        };

        let model_key = (provider_id, model.clone());
        let model_id = match model_ids.get(&model_key) {
            Some(id) => *id,
            None => {
                let id = insert_model(
                    pool,
                    &ModelInput {
                        provider_id,
                        model_name: model.clone(),
                        display_name: None,
                        base_url: None,
                        capabilities: vec!["chat".to_owned(), "tools".to_owned()],
                        embedding_dim: None,
                        enabled: true,
                        config: json!({}),
                    },
                )
                .await?;
                model_ids.insert(model_key, id);
                id
            }
        };

        if let Some(assignment) = assignments
            .iter()
            .find(|a| a.workflow_key == *workflow && a.scope == "global" && a.role == "primary")
            && assignment.provider_model_id != model_id
        {
            update_assignment(
                pool,
                assignment.id,
                &assignment_input_from_record(assignment, model_id),
            )
            .await?;
        }
    }

    mark_app_setting(pool, GPU_BACKFILL_KEY).await?;
    tracing::info!("backfilled GPU Qwen models and repointed config-only workflows");
    Ok(true)
}

/// Add the GPU2 Qwen reasoner as an ordered dialog fallback. The route can then
/// use a live local chat/tools model before probing dead external primaries.
pub async fn backfill_dialog_qwen_fallback(
    pool: &PgPool,
    config: &AppConfig,
) -> Result<bool, StorageError> {
    if app_setting_present(pool, DIALOG_QWEN_FALLBACK_KEY).await? {
        return Ok(false);
    }

    let reasoner = crate::agent_runtime::qwen_reasoner_named_provider_config(config);
    if reasoner.discovery_service_name.trim().is_empty() || reasoner.model.trim().is_empty() {
        return Ok(false);
    }

    let mut providers = list_providers(pool).await?;
    let mut models = list_models(pool).await?;
    let mut assignments = list_assignments(pool).await?;
    let provider_name = gpu_provider_name(&reasoner.discovery_service_name, config);
    if provider_name == "aifarm" {
        return Ok(false);
    }

    let provider_id = ensure_provider(
        pool,
        &mut providers,
        ProviderInput {
            name: provider_name,
            kind: "chat".to_owned(),
            endpoint: None,
            discovery_service_name: Some(reasoner.discovery_service_name),
            discovery_endpoint_name: Some(reasoner.discovery_endpoint_name),
            api_key_ref: Some("DIALOG_API_KEY".to_owned()),
            api_key_encrypted: None,
            enabled: true,
            config: json!({}),
        },
    )
    .await?;
    let model_id = ensure_model(
        pool,
        &mut models,
        ModelInput {
            provider_id,
            model_name: reasoner.model,
            display_name: None,
            base_url: None,
            capabilities: vec!["chat".to_owned(), "tools".to_owned()],
            embedding_dim: None,
            enabled: true,
            config: json!({}),
        },
    )
    .await?;
    ensure_assignment(
        pool,
        &mut assignments,
        AssignmentInput {
            workflow_key: "dialog".to_owned(),
            scope: "global".to_owned(),
            role: "fallback".to_owned(),
            provider_model_id: model_id,
            weight: None,
            fallback_order: Some(1),
            canary_percent: None,
            enabled: true,
            inference_overrides: json!({}),
            cb_failure_threshold: 5,
            cb_cooldown_ms: 30_000,
        },
    )
    .await?;

    mark_app_setting(pool, DIALOG_QWEN_FALLBACK_KEY).await?;
    tracing::info!("backfilled dialog GPU Qwen fallback");
    Ok(true)
}

const GENKIT_FLASH_FIX_KEY: &str = "llm.routing.genkit_flash_fixed";
const DECLARATIVE_V2_KEY: &str = "llm.routing.declarative_v2";
const IMAGE_DRAW_API_PRIMARY_KEY: &str = "llm.routing.image_draw_api_primary";
const DRAW_API_PROVIDER: &str = "aifarm-draw";
const DRAW_API_MODEL: &str = "draw-api";
const IMAGE_EDIT_WORKFLOW: &str = "image_edit";
const PRIVACY_FILTER_PROVIDER: &str = "privacy-filter";
const OPENROUTER_PROVIDER: &str = "openrouter";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

/// The non-lite model the original seed wrongly hardcoded for the dialog genkit fallback.
const STALE_GENKIT_FLASH_MODEL: &str = "gemini-2.5-flash";

fn model_input_from_record(record: &ModelRecord, model_name: String) -> ModelInput {
    ModelInput {
        provider_id: record.provider_id,
        model_name,
        display_name: record.display_name.clone(),
        base_url: record.base_url.clone(),
        capabilities: record.capabilities.clone(),
        embedding_dim: record.embedding_dim,
        enabled: record.enabled,
        config: record.config.clone(),
    }
}

/// Correct the dialog genkit fallback/overflow model that the original seed hardcoded to
/// the heavier `gemini-2.5-flash`. Every other genkit flow resolves its model via
/// `genkit_runtime_default_model` (flash-lite by default), so an already-seeded database
/// keeps serving `gemini-2.5-flash` on dialog fallback while the rest of the bot uses
/// flash-lite. This repoints the `genkit`/`gemini` provider models to the canonical value.
/// Idempotent via the `genkit_flash_fixed` flag; behavior-neutral on a fresh DB because the
/// seed now writes the correct model directly.
pub async fn backfill_genkit_flash_model(
    pool: &PgPool,
    config: &AppConfig,
) -> Result<bool, StorageError> {
    if app_setting_present(pool, GENKIT_FLASH_FIX_KEY).await? {
        return Ok(false);
    }
    let canonical = crate::memory_runtime::genkit_runtime_default_model(config);
    if canonical.trim().is_empty() || canonical == STALE_GENKIT_FLASH_MODEL {
        return Ok(false);
    }
    let provider_ids: std::collections::HashSet<i64> = list_providers(pool)
        .await?
        .into_iter()
        .filter(|p| p.name == "genkit" || p.name == "gemini")
        .map(|p| p.id)
        .collect();
    let mut fixed = 0usize;
    for model in list_models(pool).await? {
        if provider_ids.contains(&model.provider_id) && model.model_name == STALE_GENKIT_FLASH_MODEL
        {
            update_model(
                pool,
                model.id,
                &model_input_from_record(&model, canonical.clone()),
            )
            .await?;
            fixed += 1;
        }
    }
    mark_app_setting(pool, GENKIT_FLASH_FIX_KEY).await?;
    tracing::info!(
        fixed,
        canonical = %canonical,
        "corrected genkit dialog fallback off gemini-2.5-flash"
    );
    Ok(true)
}

fn existing_provider_id(providers: &[ProviderRecord], name: &str) -> Option<i64> {
    providers
        .iter()
        .find(|provider| provider.name == name)
        .map(|provider| provider.id)
}

fn existing_model_id(models: &[ModelRecord], provider_id: i64, model_name: &str) -> Option<i64> {
    models
        .iter()
        .find(|model| model.provider_id == provider_id && model.model_name == model_name)
        .map(|model| model.id)
}

fn assignment_exists(
    assignments: &[AssignmentRecord],
    workflow: &str,
    role: &str,
    model_id: i64,
) -> bool {
    assignments.iter().any(|assignment| {
        assignment.workflow_key == workflow
            && assignment.scope == "global"
            && assignment.role == role
            && assignment.provider_model_id == model_id
    })
}

async fn ensure_provider(
    pool: &PgPool,
    providers: &mut Vec<ProviderRecord>,
    input: ProviderInput,
) -> Result<i64, StorageError> {
    if let Some(id) = existing_provider_id(providers, &input.name) {
        return Ok(id);
    }
    let id = insert_provider(pool, &input).await?;
    providers.push(ProviderRecord {
        id,
        name: input.name,
        kind: input.kind,
        endpoint: input.endpoint,
        discovery_service_name: input.discovery_service_name,
        discovery_endpoint_name: input.discovery_endpoint_name,
        api_key_ref: input.api_key_ref,
        api_key_encrypted: input.api_key_encrypted,
        enabled: input.enabled,
        config: input.config,
    });
    Ok(id)
}

async fn ensure_model(
    pool: &PgPool,
    models: &mut Vec<ModelRecord>,
    input: ModelInput,
) -> Result<i64, StorageError> {
    if let Some(id) = existing_model_id(models, input.provider_id, &input.model_name) {
        return Ok(id);
    }
    let id = insert_model(pool, &input).await?;
    models.push(ModelRecord {
        id,
        provider_id: input.provider_id,
        model_name: input.model_name,
        display_name: input.display_name,
        base_url: input.base_url,
        capabilities: input.capabilities,
        embedding_dim: input.embedding_dim,
        enabled: input.enabled,
        config: input.config,
    });
    Ok(id)
}

async fn ensure_assignment(
    pool: &PgPool,
    assignments: &mut Vec<AssignmentRecord>,
    input: AssignmentInput,
) -> Result<i64, StorageError> {
    if let Some(existing) = assignments.iter().find(|assignment| {
        assignment.workflow_key == input.workflow_key
            && assignment.scope == input.scope
            && assignment.role == input.role
            && assignment.provider_model_id == input.provider_model_id
    }) {
        return Ok(existing.id);
    }
    let id = insert_assignment(pool, &input).await?;
    assignments.push(AssignmentRecord {
        id,
        workflow_key: input.workflow_key,
        scope: input.scope,
        role: input.role,
        provider_model_id: input.provider_model_id,
        weight: input.weight,
        fallback_order: input.fallback_order,
        canary_percent: input.canary_percent,
        enabled: input.enabled,
        inference_overrides: input.inference_overrides,
        cb_failure_threshold: input.cb_failure_threshold,
        cb_cooldown_ms: input.cb_cooldown_ms,
    });
    Ok(id)
}

async fn ensure_primary_assignment(
    pool: &PgPool,
    assignments: &mut Vec<AssignmentRecord>,
    workflow: &str,
    model_id: i64,
    weight: Option<i32>,
) -> Result<i64, StorageError> {
    ensure_assignment(
        pool,
        assignments,
        AssignmentInput {
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

async fn ensure_draw_api_model(
    pool: &PgPool,
    providers: &mut Vec<ProviderRecord>,
    models: &mut Vec<ModelRecord>,
    config: &AppConfig,
) -> Result<i64, StorageError> {
    let provider_input = ProviderInput {
        name: DRAW_API_PROVIDER.to_owned(),
        kind: "image".to_owned(),
        endpoint: Some(config.llm.discovery.base_url.clone()),
        discovery_service_name: Some(crate::image_jobs::AIFARM_DRAW_API_SERVICE_NAME.to_owned()),
        discovery_endpoint_name: Some(crate::image_jobs::AIFARM_DRAW_API_ENDPOINT_NAME.to_owned()),
        api_key_ref: None,
        api_key_encrypted: None,
        enabled: true,
        config: json!({
            "service_name": crate::image_jobs::AIFARM_DRAW_API_SERVICE_NAME,
            "endpoint_name": crate::image_jobs::AIFARM_DRAW_API_ENDPOINT_NAME,
        }),
    };
    let draw_provider_id = ensure_provider(pool, providers, provider_input.clone()).await?;
    update_provider(pool, draw_provider_id, &provider_input).await?;
    if let Some(provider) = providers
        .iter_mut()
        .find(|provider| provider.id == draw_provider_id)
    {
        provider.name = provider_input.name;
        provider.kind = provider_input.kind;
        provider.endpoint = provider_input.endpoint;
        provider.discovery_service_name = provider_input.discovery_service_name;
        provider.discovery_endpoint_name = provider_input.discovery_endpoint_name;
        provider.api_key_ref = provider_input.api_key_ref;
        provider.api_key_encrypted = provider_input.api_key_encrypted;
        provider.enabled = provider_input.enabled;
        provider.config = provider_input.config;
    }

    let model_input = ModelInput {
        provider_id: draw_provider_id,
        model_name: DRAW_API_MODEL.to_owned(),
        display_name: Some("AI Farm draw-api".to_owned()),
        base_url: None,
        capabilities: vec!["image".to_owned()],
        embedding_dim: None,
        enabled: true,
        config: json!({
            "service_name": crate::image_jobs::AIFARM_DRAW_API_SERVICE_NAME,
            "endpoint_name": crate::image_jobs::AIFARM_DRAW_API_ENDPOINT_NAME,
        }),
    };
    let draw_model_id = ensure_model(pool, models, model_input.clone()).await?;
    update_model(pool, draw_model_id, &model_input).await?;
    if let Some(model) = models.iter_mut().find(|model| model.id == draw_model_id) {
        model.provider_id = model_input.provider_id;
        model.model_name = model_input.model_name;
        model.display_name = model_input.display_name;
        model.base_url = model_input.base_url;
        model.capabilities = model_input.capabilities;
        model.embedding_dim = model_input.embedding_dim;
        model.enabled = model_input.enabled;
        model.config = model_input.config;
    }
    Ok(draw_model_id)
}

async fn ensure_image_generation_draw_api_primary_assignment(
    pool: &PgPool,
    assignments: &mut Vec<AssignmentRecord>,
    draw_model_id: i64,
) -> Result<(), StorageError> {
    ensure_primary_assignment(
        pool,
        assignments,
        "image_generation",
        draw_model_id,
        Some(100),
    )
    .await?;
    for assignment in assignments.iter_mut().filter(|assignment| {
        assignment.workflow_key == "image_generation"
            && assignment.scope == "global"
            && assignment.role == "primary"
    }) {
        let should_enable = assignment.provider_model_id == draw_model_id;
        if assignment.enabled == should_enable && (!should_enable || assignment.weight == Some(100))
        {
            continue;
        }
        let mut input = assignment_input_from_record(assignment, assignment.provider_model_id);
        input.enabled = should_enable;
        if should_enable {
            input.weight = Some(100);
        }
        update_assignment(pool, assignment.id, &input).await?;
        assignment.enabled = input.enabled;
        assignment.weight = input.weight;
    }
    Ok(())
}

fn trigger_exists(
    triggers: &[TriggerRecord],
    workflow: &str,
    trigger_type: &str,
    params: &Value,
) -> bool {
    triggers.iter().any(|trigger| {
        trigger.workflow_key == workflow
            && trigger.trigger_type == trigger_type
            && trigger.params.get("provider_id") == params.get("provider_id")
            && trigger.params.get("model_id") == params.get("model_id")
    })
}

/// Add the routing rows needed by the full declarative data-plane migration. Existing
/// operator-owned rows are preserved; this only fills gaps left by the earlier seed.
pub async fn backfill_declarative_v2(
    pool: &PgPool,
    config: &AppConfig,
) -> Result<bool, StorageError> {
    if app_setting_present(pool, DECLARATIVE_V2_KEY).await? {
        return Ok(false);
    }

    insert_workflow_if_missing(pool, IMAGE_EDIT_WORKFLOW, "image", true, 3, 60_000, true).await?;

    let mut providers = list_providers(pool).await?;
    let mut models = list_models(pool).await?;
    let mut assignments = list_assignments(pool).await?;
    let triggers = openplotva_storage::llm_routing::list_triggers(pool).await?;

    let draw_model_id = ensure_draw_api_model(pool, &mut providers, &mut models, config).await?;
    ensure_primary_assignment(
        pool,
        &mut assignments,
        IMAGE_EDIT_WORKFLOW,
        draw_model_id,
        Some(100),
    )
    .await?;
    ensure_image_generation_draw_api_primary_assignment(pool, &mut assignments, draw_model_id)
        .await?;
    if !assignment_exists(&assignments, "image_generation", "fallback", draw_model_id) {
        ensure_assignment(
            pool,
            &mut assignments,
            AssignmentInput {
                workflow_key: "image_generation".to_owned(),
                scope: "global".to_owned(),
                role: "fallback".to_owned(),
                provider_model_id: draw_model_id,
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
    }

    let redaction_service = config.memory.redaction_service_name.trim();
    if !redaction_service.is_empty() {
        let privacy_provider_id = ensure_provider(
            pool,
            &mut providers,
            ProviderInput {
                name: PRIVACY_FILTER_PROVIDER.to_owned(),
                kind: "privacy_filter".to_owned(),
                endpoint: Some(config.llm.discovery.base_url.clone()),
                discovery_service_name: Some(redaction_service.to_owned()),
                discovery_endpoint_name: Some(config.memory.redaction_endpoint_name.clone()),
                api_key_ref: None,
                api_key_encrypted: None,
                enabled: config.memory.redaction_enabled,
                config: json!({}),
            },
        )
        .await?;
        let privacy_model_id = ensure_model(
            pool,
            &mut models,
            ModelInput {
                provider_id: privacy_provider_id,
                model_name: redaction_service.to_owned(),
                display_name: Some("Discovery privacy filter".to_owned()),
                base_url: None,
                capabilities: vec!["privacy_filter".to_owned()],
                embedding_dim: None,
                enabled: config.memory.redaction_enabled,
                config: json!({}),
            },
        )
        .await?;
        ensure_primary_assignment(pool, &mut assignments, "redaction", privacy_model_id, None)
            .await?;
    }

    let openrouter_model = crate::memory_runtime::genkit_runtime_default_model(config)
        .strip_prefix("openrouter/")
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_owned);
    if let Some(openrouter_model) = openrouter_model {
        let openrouter_provider_id = ensure_provider(
            pool,
            &mut providers,
            ProviderInput {
                name: OPENROUTER_PROVIDER.to_owned(),
                kind: "chat".to_owned(),
                endpoint: Some(OPENROUTER_CHAT_COMPLETIONS_URL.to_owned()),
                discovery_service_name: None,
                discovery_endpoint_name: None,
                api_key_ref: Some("OPENROUTER_API_KEY".to_owned()),
                api_key_encrypted: None,
                enabled: !config.open_router.key.trim().is_empty(),
                config: json!({ "direct_url": OPENROUTER_CHAT_COMPLETIONS_URL }),
            },
        )
        .await?;
        let _ = ensure_model(
            pool,
            &mut models,
            ModelInput {
                provider_id: openrouter_provider_id,
                model_name: openrouter_model,
                display_name: Some("OpenRouter default".to_owned()),
                base_url: Some(OPENROUTER_CHAT_COMPLETIONS_URL.to_owned()),
                capabilities: vec!["chat".to_owned(), "tools".to_owned()],
                embedding_dim: None,
                enabled: !config.open_router.key.trim().is_empty(),
                config: json!({
                    "request_timeout_seconds": config.open_router.request_timeout_seconds,
                    "include_reasoning": false,
                }),
            },
        )
        .await?;
    }

    let overflow_assignment = assignments
        .iter()
        .find(|assignment| {
            assignment.workflow_key == "dialog"
                && assignment.scope == "global"
                && assignment.role == "overflow"
                && assignment.enabled
        })
        .map(|assignment| assignment.id);
    if let Some(engage_assignment_id) = overflow_assignment {
        let pool_provider = existing_provider_id(&providers, VRAM_CLOUD_PROVIDER);
        if let Some(pool_provider) = pool_provider {
            for model in models
                .iter()
                .filter(|model| model.provider_id == pool_provider && model.enabled)
            {
                let params = json!({
                    "provider_id": pool_provider,
                    "model_id": model.id,
                    "cooldown_ms": 30_000,
                });
                if !trigger_exists(&triggers, "dialog", "provider_capacity", &params) {
                    insert_trigger(
                        pool,
                        &TriggerInput {
                            workflow_key: "dialog".to_owned(),
                            trigger_type: "provider_capacity".to_owned(),
                            engage_assignment_id,
                            enabled: true,
                            queue_name: None,
                            high_watermark: None,
                            low_watermark: None,
                            params,
                        },
                    )
                    .await?;
                }
            }
        }
    }

    mark_app_setting(pool, DECLARATIVE_V2_KEY).await?;
    tracing::info!("backfilled declarative routing v2 rows");
    Ok(true)
}

pub async fn backfill_image_generation_draw_api_primary(
    pool: &PgPool,
    config: &AppConfig,
) -> Result<bool, StorageError> {
    if app_setting_present(pool, IMAGE_DRAW_API_PRIMARY_KEY).await? {
        return Ok(false);
    }

    insert_workflow_if_missing(pool, "image_generation", "image", true, 3, 60_000, true).await?;
    let mut providers = list_providers(pool).await?;
    let mut models = list_models(pool).await?;
    let mut assignments = list_assignments(pool).await?;
    let draw_model_id = ensure_draw_api_model(pool, &mut providers, &mut models, config).await?;
    ensure_image_generation_draw_api_primary_assignment(pool, &mut assignments, draw_model_id)
        .await?;

    mark_app_setting(pool, IMAGE_DRAW_API_PRIMARY_KEY).await?;
    tracing::info!("backfilled image_generation primary to draw-api");
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
    providers: &HashMap<i64, ProviderRow>,
    kind: Kind,
) -> Option<Candidate> {
    let model = models.get(&record.provider_model_id)?;
    if !model.enabled || !model_matches_kind(model, kind) {
        return None;
    }
    let provider = providers.get(&model.provider_id)?;
    if !provider.enabled || provider.kind != kind {
        return None;
    }
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

fn model_matches_kind(model: &ModelRecord, kind: Kind) -> bool {
    let capability = match kind {
        Kind::Chat => "chat",
        Kind::Vision => "vision",
        Kind::Embedding => "embedding",
        Kind::Image => "image",
        Kind::Music => "music",
        Kind::PrivacyFilter => "privacy_filter",
    };
    if !model.capabilities.iter().any(|c| c == capability) {
        return false;
    }
    if kind == Kind::Embedding && model.embedding_dim != Some(512) {
        return false;
    }
    true
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
        "provider_capacity" => {
            let params = record.params.as_object();
            let provider = params
                .and_then(|map| map.get("provider_id"))
                .and_then(Value::as_i64)?;
            let model = params
                .and_then(|map| map.get("model_id"))
                .and_then(Value::as_i64)?;
            let cooldown_ms = params
                .and_then(|map| map.get("cooldown_ms"))
                .and_then(Value::as_u64)
                .unwrap_or(30_000);
            Some(TriggerCondition::ProviderCapacity {
                provider,
                model,
                cooldown: Duration::from_millis(cooldown_ms),
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
        if !record.enabled {
            continue;
        }
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
        if !record.enabled || !providers.contains_key(&record.provider_id) {
            continue;
        }
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

    let mut table = RoutingTable::new(providers.clone(), model_rows);

    for (&(workflow_key, scope_str), assignments) in &by_key_scope {
        let Some(workflow) = workflows.get(workflow_key) else {
            continue;
        };
        if !workflow.enabled {
            continue;
        }
        let scope = match scope_str {
            "vip" => Scope::Vip,
            _ => Scope::Global,
        };
        let Some(route) = build_route(
            workflow,
            assignments,
            &triggers_by_workflow,
            &model_records,
            &providers,
        ) else {
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
    providers: &HashMap<i64, ProviderRow>,
) -> Option<WorkflowRoute> {
    let kind = Kind::from_db(&workflow.kind)?;
    let mut primary = Vec::new();
    let mut fallback_tail = Vec::new();
    let mut canary = Vec::new();
    let mut shadow = Vec::new();

    for record in assignments {
        let Some(candidate) = candidate_from_assignment(record, models, providers, kind) else {
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
            let Some(mut engage) =
                candidate_from_assignment(engage_record, models, providers, kind)
            else {
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

    #[test]
    fn disabled_workflow_is_not_routed() {
        let mut snapshot = dialog_snapshot();
        snapshot.workflows[0].enabled = false;

        let table = build_routing_table(&snapshot);

        assert!(table.resolve("dialog", false).is_none());
    }

    #[test]
    fn disabled_provider_is_not_eligible_for_route_candidates() {
        let mut snapshot = dialog_snapshot();
        snapshot.providers[0].enabled = false;

        let table = build_routing_table(&snapshot);
        let route = table.resolve("dialog", false).expect("dialog route");

        assert!(
            route
                .primary
                .iter()
                .all(|candidate| candidate.provider != 1)
        );
        assert!(
            route
                .triggers
                .iter()
                .all(|trigger| trigger.engage.provider != 1)
        );
        assert!(
            route
                .fallback_tail
                .iter()
                .all(|candidate| candidate.provider != 1)
        );
    }

    #[test]
    fn disabled_model_is_not_eligible_for_route_candidates() {
        let mut snapshot = dialog_snapshot();
        snapshot.models[0].enabled = false;

        let table = build_routing_table(&snapshot);
        let route = table.resolve("dialog", false).expect("dialog route");

        assert!(route.primary.iter().all(|candidate| candidate.model != 10));
        assert!(
            route
                .triggers
                .iter()
                .all(|trigger| trigger.engage.model != 10)
        );
        assert!(
            route
                .fallback_tail
                .iter()
                .all(|candidate| candidate.model != 10)
        );
    }

    #[test]
    fn workflow_kind_requires_matching_model_capability() {
        let mut snapshot = dialog_snapshot();
        snapshot.models[0].capabilities = vec!["embedding".to_owned()];

        let table = build_routing_table(&snapshot);
        let route = table.resolve("dialog", false).expect("dialog route");

        assert!(route.primary.iter().all(|candidate| candidate.model != 10));
    }

    #[test]
    fn embedding_workflow_requires_512_dimensions() {
        let mut snapshot = RoutingSnapshot {
            providers: vec![provider(1, "aifarm-embed", "embedding")],
            models: vec![ModelRecord {
                capabilities: vec!["embedding".to_owned()],
                embedding_dim: Some(768),
                ..model(10, 1, "embedder")
            }],
            workflows: vec![WorkflowRecord {
                key: "embedding".to_owned(),
                kind: "embedding".to_owned(),
                full_routing: false,
                retry_max_hops: 1,
                retry_wall_ms: 10_000,
                enabled: true,
            }],
            assignments: vec![assignment(100, "embedding", "global", "primary", 10, None)],
            triggers: vec![],
        };

        let table = build_routing_table(&snapshot);
        let route = table.resolve("embedding", false).expect("embedding route");
        assert!(route.primary.is_empty());

        snapshot.models[0].embedding_dim = Some(512);
        let table = build_routing_table(&snapshot);
        let route = table.resolve("embedding", false).expect("embedding route");
        assert_eq!(route.primary.len(), 1);
    }

    #[test]
    fn parses_provider_capacity_trigger_params() {
        let mut snapshot = dialog_snapshot();
        snapshot.triggers[0].trigger_type = "provider_capacity".to_owned();
        snapshot.triggers[0].queue_name = None;
        snapshot.triggers[0].high_watermark = None;
        snapshot.triggers[0].low_watermark = None;
        snapshot.triggers[0].params = json!({
            "provider_id": 1,
            "model_id": 10,
            "cooldown_ms": 45_000,
        });

        let table = build_routing_table(&snapshot);
        let route = table.resolve("dialog", false).expect("dialog route");

        match &route.triggers[0].condition {
            TriggerCondition::ProviderCapacity {
                provider,
                model,
                cooldown,
            } => {
                assert_eq!((*provider, *model), (1, 10));
                assert_eq!(*cooldown, Duration::from_millis(45_000));
            }
            other => panic!("unexpected trigger condition: {other:?}"),
        }
    }
}
