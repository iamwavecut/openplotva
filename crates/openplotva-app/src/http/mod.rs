//! HTTP state and feature-specific request adapters.

use openplotva_storage::database as sqlx;
pub(crate) mod assets;
pub(crate) mod auth;
pub(crate) mod common;
pub(crate) mod entities;
pub(crate) mod ingress;
pub(crate) mod memory;
pub(crate) mod observability;
pub(crate) mod redis;
pub(crate) mod routing;
pub(crate) mod settings;
pub(crate) mod shield;
pub(crate) mod taskman;

use crate::{
    dialog_workers, memory_runtime, model_routing, openrouter_free_pool, payments, runtime_cache,
    runtime_dispatcher, runtime_llm, runtime_llm_runs, runtime_routing, runtime_taskman,
};
use openplotva_storage::{
    PostgresChatMemberStore, PostgresChatSettingsStore, PostgresMemoryStore, PostgresShieldStore,
    PostgresVipStore, PostgresVirtualMessageStore,
};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub(crate) struct HttpRouterState {
    pub(crate) admin_ids: Arc<[i64]>,
    pub(crate) bot_token: Arc<str>,
    pub(crate) webapp_url: Arc<str>,
    pub(crate) bot_username: Arc<str>,
    pub(crate) bot_id: Option<i64>,
    pub(crate) default_log_level: Arc<str>,
    pub(crate) postgres: Option<PgPool>,
    pub(crate) redis: Option<::redis::Client>,
    pub(crate) log_buffer: Option<Arc<openplotva_observability::RuntimeLogBuffer>>,
    pub(crate) telegram: Option<openplotva_telegram::TelegramClient>,
    pub(crate) dispatcher_inspector: runtime_dispatcher::RuntimeDispatcherInspectorHandle,
    pub(crate) cache_inspector: runtime_cache::RuntimeCacheInspectorHandle,
    pub(crate) taskman_inspector: runtime_taskman::RuntimeTaskmanInspectorHandle,
    pub(crate) llm_trace_buffer: Option<runtime_llm::RuntimeLlmTraceBuffer>,
    pub(crate) llm_run_buffer: Option<runtime_llm_runs::RuntimeLlmRunBuffer>,
    pub(crate) gradius_audit_reader: Option<Arc<dyn openplotva_server::RuntimeGradiusAuditReader>>,
    pub(crate) routing_event_buffer: Option<runtime_routing::RoutingEventBuffer>,
    pub(crate) routing_event_reporter: Option<runtime_routing::RoutingEventReporter>,
    pub(crate) llm_discovery_base_url: Arc<str>,
    pub(crate) llm_discovery_service_name: Arc<str>,
    pub(crate) runtime_sql_timeout_ms: i32,
    pub(crate) state_store: Option<PostgresVirtualMessageStore>,
    pub(crate) settings_store: Option<PostgresChatSettingsStore>,
    pub(crate) member_store: Option<PostgresChatMemberStore>,
    pub(crate) vip_store: Option<PostgresVipStore>,
    pub(crate) vip_status: Option<Arc<dyn payments::VipStatusChecker + Send + Sync>>,
    pub(crate) memory_store: Option<PostgresMemoryStore>,
    pub(crate) memory_admin_enabled: bool,
    pub(crate) memory_retention: Duration,
    pub(crate) memory_enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy,
    pub(crate) memory_consolidation_model: Arc<str>,
    pub(crate) memory_max_input_tokens: i32,
    pub(crate) memory_max_messages_per_run: i32,
    pub(crate) memory_token_estimator_source: Arc<str>,
    pub(crate) memory_restart_trigger: Option<Arc<tokio::sync::Notify>>,
    pub(crate) memory_override_runtime: Option<memory::AdminMemoryOverrideRuntime>,
    pub(crate) shield_store: Option<PostgresShieldStore>,
    pub(crate) shield_options: openplotva_shield::Options,
    pub(crate) shield_embedder: Option<Arc<dyn memory_runtime::EmbeddingProvider>>,
    pub(crate) router_handle: Option<Arc<openplotva_llm::router::RouterHandle>>,
    pub(crate) router_breakers: Option<Arc<openplotva_llm::router::BreakerSet>>,
    pub(crate) router_triggers: Option<Arc<openplotva_llm::router::TriggerState>>,
    pub(crate) router_pools: Option<Arc<openplotva_llm::router::PoolRegistry>>,
    pub(crate) openrouter_free_gate: Option<Arc<openrouter_free_pool::OpenRouterFreeQuotaGate>>,
    pub(crate) router_runtime: Option<Arc<model_routing::RouterRuntime>>,
    pub(crate) dialog_worker_gauge: Option<Arc<dialog_workers::WorkerGauge>>,
}
