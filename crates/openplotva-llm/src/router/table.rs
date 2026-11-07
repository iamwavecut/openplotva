//! Pure, DB-free in-memory snapshot of the routing configuration.
//!
//! The app loader builds a [`RoutingTable`] from a database snapshot and hands it
//! to the live [`super::handle::RouterHandle`]. Nothing here touches sqlx; the
//! control plane is engine-agnostic policy over abstract provider/model ids.

use std::collections::HashMap;
use std::time::Duration;

use serde_json::Value;

use super::breaker::BreakerConfig;
use super::capacity::LanePriority;
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
    Asr,
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
            "asr" => Some(Self::Asr),
            _ => None,
        }
    }
}

/// Interpret rows written before explicit protocols. Discovery-backed chat uses OpenAI payloads.
#[must_use]
pub fn legacy_provider_protocol(
    kind: Kind,
    name: &str,
    has_discovery: bool,
) -> crate::provider_schema::Protocol {
    use crate::provider_schema::Protocol;
    match kind {
        Kind::Chat | Kind::Vision
            if !has_discovery
                && (name.eq_ignore_ascii_case("genkit") || name.eq_ignore_ascii_case("gemini")) =>
        {
            Protocol::Genkit
        }
        Kind::Chat | Kind::Vision => Protocol::OpenAiCompat,
        Kind::Embedding => Protocol::DiscoveryJobs,
        Kind::Image => Protocol::DiscoveryDraw,
        Kind::Music => Protocol::AceStep,
        Kind::PrivacyFilter => Protocol::PrivacyFilter,
        Kind::Asr => Protocol::DiscoveryAsr,
    }
}

#[cfg(test)]
mod tests {
    use super::Kind;

    #[test]
    fn explicit_protocol_wins_over_provider_names_and_unknown_values_fail_closed() {
        use super::ProviderRow;
        use crate::provider_schema::Protocol;
        let mut row = ProviderRow {
            id: 1,
            name: "genkit".into(),
            kind: Kind::Chat,
            protocol: None,
            runtime_hint: None,
            endpoint: None,
            discovery_service_name: None,
            discovery_endpoint_name: None,
            api_key_ref: None,
            api_key_encrypted: None,
            enabled: true,
            config: serde_json::json!({}),
        };
        assert_eq!(row.resolved_protocol(), Some(Protocol::Genkit));
        row.protocol = Some("openai_compat".into());
        assert_eq!(row.resolved_protocol(), Some(Protocol::OpenAiCompat));
        row.name = "renamed-provider".into();
        row.protocol = Some("genkit".into());
        assert_eq!(row.resolved_protocol(), Some(Protocol::Genkit));
        row.discovery_service_name = Some("llm-openai".into());
        assert_eq!(row.resolved_protocol(), Some(Protocol::Genkit));
        row.protocol = None;
        assert_eq!(row.resolved_protocol(), Some(Protocol::OpenAiCompat));
        row.protocol = Some("unknown".into());
        assert_eq!(row.resolved_protocol(), None);
        row.protocol = None;
        row.kind = Kind::Asr;
        assert_eq!(row.resolved_protocol(), Some(Protocol::DiscoveryAsr));
    }

    #[test]
    fn legacy_protocol_classifies_seeded_providers() {
        // (kind, name, has_discovery) -> protocol, over the exact seeded rows.
        let cases = [
            ("chat", "aifarm", true, "openai_compat"),
            ("chat", "genkit", false, "genkit"),
            ("chat", "GENKIT", false, "genkit"),
            ("chat", "genkit", true, "openai_compat"),
            ("vision", "gemini", true, "openai_compat"),
            ("chat", "gemini", false, "genkit"),
            ("chat", "vram-cloud", false, "openai_compat"),
            ("chat", "openrouter", false, "openai_compat"),
            ("chat", "nvidia", false, "openai_compat"),
            ("chat", "vmlx", false, "openai_compat"),
            ("chat", "aifarm-llamacpp-gpu2", true, "openai_compat"),
            ("chat", "aifarm-maple", true, "openai_compat"),
            ("vision", "aifarm-vision", true, "openai_compat"),
            ("embedding", "aifarm-embed", true, "discovery_jobs"),
            ("image", "aifarm-draw", true, "discovery_draw"),
            ("music", "acestep", false, "acestep"),
            ("privacy_filter", "privacy-filter", true, "privacy_filter"),
            ("asr", "aifarm-asr", true, "discovery_asr"),
        ];
        for (kind, name, has_discovery, expected) in cases {
            assert_eq!(
                super::legacy_provider_protocol(
                    Kind::from_db(kind).expect("kind"),
                    name,
                    has_discovery
                )
                .as_str(),
                expected,
                "provider {name} (kind {kind})"
            );
        }
    }

    #[test]
    fn parses_asr_kind_from_database_value() {
        assert_eq!(Kind::from_db("asr"), Some(Kind::Asr));
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
    /// Place of this workflow's requests in their lane queues.
    pub lane_priority: LanePriority,
    /// Time kept for the next candidate when a lane wait has a deadline.
    pub fallback_reserve: Duration,
}

/// Per-workflow holder of the global default and optional VIP override.
#[derive(Clone, Debug, Default)]
struct ScopedRoutes {
    global: Option<WorkflowRoute>,
    vip: Option<WorkflowRoute>,
}

/// A provider endpoint as seen by the control plane. Plaintext credentials
/// never live here: `api_key_ref` names an env var and `api_key_encrypted`
/// is an AES-GCM sealed blob the app-side client factory opens under the
/// operator's master key.
#[derive(Clone, Debug)]
pub struct ProviderRow {
    pub id: ProviderId,
    pub name: String,
    pub kind: Kind,
    /// Wire protocol (`openai_compat`, `genkit`, ...); `None` on legacy rows.
    pub protocol: Option<String>,
    /// Serving engine hint used for request-shape normalization.
    pub runtime_hint: Option<String>,
    pub endpoint: Option<String>,
    pub discovery_service_name: Option<String>,
    pub discovery_endpoint_name: Option<String>,
    pub api_key_ref: Option<String>,
    pub api_key_encrypted: Option<Vec<u8>>,
    pub enabled: bool,
    pub config: Value,
}

impl ProviderRow {
    /// Explicit protocols win. Name inference exists only for rows written before protocols.
    #[must_use]
    pub fn resolved_protocol(&self) -> Option<crate::provider_schema::Protocol> {
        use crate::provider_schema::Protocol;
        if let Some(protocol) = &self.protocol {
            return Protocol::from_db(protocol);
        }
        Some(legacy_provider_protocol(
            self.kind,
            &self.name,
            self.discovery_service_name.is_some(),
        ))
    }
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
    pool_names: HashMap<PoolId, String>,
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
            pool_names: HashMap::new(),
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

    /// Replace the operator-facing pool names carried by this snapshot.
    pub fn set_pool_names(&mut self, names: HashMap<PoolId, String>) {
        self.pool_names = names;
    }

    #[must_use]
    pub fn pool_name(&self, id: PoolId) -> Option<&str> {
        self.pool_names.get(&id).map(String::as_str)
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

    /// Models any route can reach (primaries, fallbacks, canaries, shadows and
    /// trigger targets of every workflow and scope), each once, by id.
    pub fn routed_models(&self) -> impl Iterator<Item = &ModelRow> {
        let mut ids: Vec<ModelId> = self
            .routes
            .values()
            .flat_map(|scoped| scoped.global.iter().chain(scoped.vip.iter()))
            .flat_map(|route| {
                route
                    .primary
                    .iter()
                    .chain(route.fallback_tail.iter())
                    .chain(route.canary.iter())
                    .chain(route.shadow.iter())
                    .chain(route.triggers.iter().map(|trigger| &trigger.engage))
                    .map(|candidate| candidate.model)
            })
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter().filter_map(|id| self.models.get(&id))
    }
}
