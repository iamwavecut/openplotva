//! Pure, DB-free in-memory snapshot of the routing configuration.
//!
//! The app loader builds a [`RoutingTable`] from a database snapshot and hands it
//! to the live [`super::handle::RouterHandle`]. Nothing here touches sqlx; the
//! control plane is engine-agnostic policy over abstract provider/model ids.

use std::collections::HashMap;

use serde_json::Value;

use super::breaker::BreakerConfig;
use super::policy::RetryBudget;
use super::triggers::TriggerSpec;

/// Stable database id of a provider row.
pub type ProviderId = i64;
/// Stable database id of a provider-model row.
pub type ModelId = i64;
/// Stable database id of a capacity-pool row.
pub type PoolId = i64;

/// Transport kind; selects the data-plane adapter the executor dispatches to.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Kind {
    Chat,
    Vision,
    Embedding,
    Image,
    Music,
    PrivacyFilter,
}

impl Kind {
    /// Parse the DB `kind` string. Unknown kinds map to `None` so the loader can
    /// skip a malformed row rather than guess.
    #[must_use]
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "chat" => Some(Self::Chat),
            "vision" => Some(Self::Vision),
            "embedding" => Some(Self::Embedding),
            "image" => Some(Self::Image),
            "music" => Some(Self::Music),
            "privacy_filter" => Some(Self::PrivacyFilter),
            _ => None,
        }
    }
}

/// Scope of an assignment; a VIP override takes precedence over the global default.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Scope {
    Global,
    Vip,
}

/// Routing role of an assignment within a workflow.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    Primary,
    Fallback,
    Overflow,
    Shadow,
    Canary,
}

impl Role {
    #[must_use]
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "primary" => Some(Self::Primary),
            "fallback" => Some(Self::Fallback),
            "overflow" => Some(Self::Overflow),
            "shadow" => Some(Self::Shadow),
            "canary" => Some(Self::Canary),
            _ => None,
        }
    }
}

/// Per-assignment inference overrides; `None` means inherit the model/provider default.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InferenceOverrides {
    pub temperature: Option<f64>,
    pub max_tokens: Option<i32>,
    pub frequency_penalty: Option<f64>,
    pub presence_penalty: Option<f64>,
    pub repeat_penalty: Option<f64>,
    /// Long-tail knobs (top_p, dry_*, enable_thinking) kept as raw JSON.
    pub extra: Value,
}

/// One provider+model bound to a workflow with its routing policy.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub provider: ProviderId,
    pub model: ModelId,
    pub role: Role,
    /// Weighted share for `Primary`/engaged `Overflow` candidates (0–100).
    pub weight: u32,
    /// Ascending position in the fallback tail for `Fallback` candidates.
    pub fallback_order: i32,
    /// Traffic fraction (0–100) routed to a `Canary` candidate.
    pub canary_percent: u32,
    pub overrides: InferenceOverrides,
    /// Experiment label tagged into observability `inference_params`.
    pub experiment_variant: Option<String>,
    pub breaker: BreakerConfig,
}

/// The resolved routing for one (workflow, scope) pair.
#[derive(Clone, Debug)]
pub struct WorkflowRoute {
    pub key: String,
    pub kind: Kind,
    /// `false` for config-only kinds (embedding/music): primary + ordered fallback,
    /// no weighting, no triggers, no experiments.
    pub full_routing: bool,
    pub primary: Vec<Candidate>,
    pub fallback_tail: Vec<Candidate>,
    pub canary: Vec<Candidate>,
    pub triggers: Vec<TriggerSpec>,
    pub shadow: Vec<Candidate>,
    pub retry: RetryBudget,
}

/// Per-workflow holder of the global default and optional VIP override.
#[derive(Clone, Debug, Default)]
struct ScopedRoutes {
    global: Option<WorkflowRoute>,
    vip: Option<WorkflowRoute>,
}

/// A provider endpoint as seen by the control plane. Credentials are resolved by
/// the app-built adapter registry, never stored in this pure table.
#[derive(Clone, Debug)]
pub struct ProviderRow {
    pub id: ProviderId,
    pub name: String,
    pub kind: Kind,
    pub endpoint: Option<String>,
    pub discovery_service_name: Option<String>,
    pub discovery_endpoint_name: Option<String>,
    pub enabled: bool,
    pub config: Value,
}

/// A model offered by a provider, with its (possibly per-endpoint) base url.
#[derive(Clone, Debug)]
pub struct ModelRow {
    pub id: ModelId,
    pub provider: ProviderId,
    pub model_name: String,
    pub base_url: Option<String>,
    pub capabilities: Vec<String>,
    pub embedding_dim: Option<i32>,
    /// Capacity pool this model draws slots from; `None` = unpooled/unlimited.
    pub pool_id: Option<PoolId>,
    pub enabled: bool,
    pub config: Value,
}

impl ModelRow {
    #[must_use]
    pub fn has_capability(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

/// Immutable snapshot the live handle swaps atomically.
#[derive(Clone, Debug, Default)]
pub struct RoutingTable {
    routes: HashMap<String, ScopedRoutes>,
    providers: HashMap<ProviderId, ProviderRow>,
    models: HashMap<ModelId, ModelRow>,
    pools: HashMap<PoolId, super::capacity::PoolSpec>,
}

impl RoutingTable {
    #[must_use]
    pub fn new(
        providers: HashMap<ProviderId, ProviderRow>,
        models: HashMap<ModelId, ModelRow>,
    ) -> Self {
        Self {
            routes: HashMap::new(),
            providers,
            models,
            pools: HashMap::new(),
        }
    }

    /// Replace the capacity-pool specs carried by this snapshot.
    pub fn set_pools(&mut self, pools: HashMap<PoolId, super::capacity::PoolSpec>) {
        self.pools = pools;
    }

    #[must_use]
    pub fn pool(&self, id: PoolId) -> Option<&super::capacity::PoolSpec> {
        self.pools.get(&id)
    }

    /// The pool specs to reconcile the live registry with after a reload.
    #[must_use]
    pub fn pool_specs(&self) -> Vec<super::capacity::PoolSpec> {
        self.pools.values().copied().collect()
    }

    /// Insert or replace the route for a (workflow, scope) pair.
    pub fn set_route(&mut self, scope: Scope, route: WorkflowRoute) {
        let entry = self.routes.entry(route.key.clone()).or_default();
        match scope {
            Scope::Global => entry.global = Some(route),
            Scope::Vip => entry.vip = Some(route),
        }
    }

    /// Resolve the active route for a workflow. A VIP caller prefers the VIP
    /// override and falls back to the global default; a non-VIP caller always
    /// gets the global default. Borrow-keyed, so no per-request allocation.
    #[must_use]
    pub fn resolve(&self, key: &str, vip: bool) -> Option<&WorkflowRoute> {
        let scoped = self.routes.get(key)?;
        if vip && let Some(route) = scoped.vip.as_ref() {
            return Some(route);
        }
        scoped.global.as_ref()
    }

    #[must_use]
    pub fn provider(&self, id: ProviderId) -> Option<&ProviderRow> {
        self.providers.get(&id)
    }

    #[must_use]
    pub fn model(&self, id: ModelId) -> Option<&ModelRow> {
        self.models.get(&id)
    }

    pub fn workflow_keys(&self) -> impl Iterator<Item = &str> {
        self.routes.keys().map(String::as_str)
    }
}
