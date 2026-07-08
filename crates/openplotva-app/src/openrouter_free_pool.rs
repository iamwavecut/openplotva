use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
    time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const MANAGED_BY: &str = "openrouter_free_pool";
pub const PROVIDER_NAME: &str = "openrouter-free";
pub const POOL_NAME: &str = "openrouter-free";
pub const FALLBACK_MODEL: &str = "openrouter/free";
const POOL_CONFIG_CACHE_TTL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq)]
pub struct OpenRouterFreeSource {
    pub updated_at: Option<String>,
    pub ranking_version: Option<String>,
    pub base_url: Option<String>,
    pub fallback_model: Option<String>,
    pub models: Vec<OpenRouterFreeCandidate>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenRouterFreeCandidate {
    pub id: String,
    pub display_name: Option<String>,
    pub score: Option<i64>,
    pub source_rank: usize,
    pub context_length: Option<i64>,
    pub max_completion_tokens: Option<i64>,
    pub supports_tools: bool,
    pub supports_structured_outputs: bool,
    pub supports_response_format: bool,
    pub supports_reasoning: bool,
    pub health_status: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OpenRouterFreePoolConfig {
    pub auto_refresh_enabled: bool,
    pub source_url: String,
    pub refresh_interval_seconds: u64,
    pub refresh_on_startup: bool,
    pub max_models: usize,
    pub rpm_limit: u32,
    pub daily_request_limit: u32,
    pub default_pool_cooldown_seconds: u64,
    pub model_cooldown_seconds: u64,
    pub fallback_model: String,
    pub target_workflows: Vec<String>,
}

impl Default for OpenRouterFreePoolConfig {
    fn default() -> Self {
        Self {
            auto_refresh_enabled: true,
            source_url: "https://shir-man.com/api/free-llm/top-models".to_owned(),
            refresh_interval_seconds: 86_400,
            refresh_on_startup: true,
            max_models: 6,
            rpm_limit: 16,
            daily_request_limit: 950,
            default_pool_cooldown_seconds: 3_600,
            model_cooldown_seconds: 900,
            fallback_model: FALLBACK_MODEL.to_owned(),
            target_workflows: vec![
                "history_summary".to_owned(),
                "agentic_search_reasoner".to_owned(),
                "agentic_search_writer".to_owned(),
                "agentic_song".to_owned(),
                "agentic_image".to_owned(),
                "media_prompt_optimizer".to_owned(),
                "youtube_summary".to_owned(),
            ],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedAssignmentPlan {
    pub workflow_key: String,
    pub model_name: String,
    pub role: String,
    pub fallback_order: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenRouterFreeRefreshReport {
    pub refreshed: bool,
    pub active_models: usize,
    pub disabled_models: u64,
    pub assignments: usize,
    pub ranking_version: Option<String>,
    pub source_updated_at: Option<String>,
    pub key_status_error: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenRouterFreeQuotaDecision {
    Allowed,
    Denied {
        reason: String,
        retry_after: Option<Duration>,
    },
}

pub struct OpenRouterFreeQuotaGate {
    redis: redis::Client,
    postgres: PgPool,
    pool_cooldown_until: Mutex<Option<Instant>>,
    model_cooldowns_until: Mutex<HashMap<String, Instant>>,
    pool_config_cache: Mutex<Option<CachedOpenRouterFreePoolConfig>>,
}

#[derive(Clone, Debug)]
struct CachedOpenRouterFreePoolConfig {
    config: OpenRouterFreePoolConfig,
    expires_at: Instant,
}

impl OpenRouterFreeQuotaGate {
    #[must_use]
    pub fn new(redis: redis::Client, postgres: PgPool) -> Self {
        Self {
            redis,
            postgres,
            pool_cooldown_until: Mutex::new(None),
            model_cooldowns_until: Mutex::new(HashMap::new()),
            pool_config_cache: Mutex::new(None),
        }
    }

    pub async fn check_model(
        &self,
        model_name: &str,
        model_config: &Value,
    ) -> OpenRouterFreeQuotaDecision {
        if model_config.get("managed_by").and_then(Value::as_str) != Some(MANAGED_BY) {
            return OpenRouterFreeQuotaDecision::Allowed;
        }
        if let Some(retry_after) = self.current_model_cooldown(model_name) {
            return OpenRouterFreeQuotaDecision::Denied {
                reason: "openrouter_free_model_cooldown".to_owned(),
                retry_after: Some(retry_after),
            };
        }
        if let Some(retry_after) = self.current_pool_cooldown() {
            return OpenRouterFreeQuotaDecision::Denied {
                reason: "openrouter_free_pool_cooldown".to_owned(),
                retry_after: Some(retry_after),
            };
        }
        let config = match self.cached_pool_config().await {
            Ok(config) => config,
            Err(error) => {
                return OpenRouterFreeQuotaDecision::Denied {
                    reason: format!("openrouter_free_config_unavailable: {error}"),
                    retry_after: None,
                };
            }
        };
        let mut connection = match self.redis.get_multiplexed_async_connection().await {
            Ok(connection) => connection,
            Err(error) => {
                return OpenRouterFreeQuotaDecision::Denied {
                    reason: format!("openrouter_free_redis_unavailable: {error}"),
                    retry_after: None,
                };
            }
        };
        let now = OffsetDateTime::now_utc();
        let (minute_key, day_key) = quota_counter_keys(model_name, now);
        let minute_count = match incr_with_ttl(&mut connection, &minute_key, 90).await {
            Ok(count) => count,
            Err(error) => {
                return OpenRouterFreeQuotaDecision::Denied {
                    reason: format!("openrouter_free_redis_unavailable: {error}"),
                    retry_after: None,
                };
            }
        };
        let day_count = match incr_with_ttl(&mut connection, &day_key, 172_800).await {
            Ok(count) => count,
            Err(error) => {
                return OpenRouterFreeQuotaDecision::Denied {
                    reason: format!("openrouter_free_redis_unavailable: {error}"),
                    retry_after: None,
                };
            }
        };
        if minute_count > u64::from(config.rpm_limit) {
            let retry_after = Duration::from_secs(60);
            self.mark_model_cooldown(model_name, retry_after);
            return OpenRouterFreeQuotaDecision::Denied {
                reason: "openrouter_free_model_rpm_limit".to_owned(),
                retry_after: Some(retry_after),
            };
        }
        if day_count > u64::from(config.daily_request_limit) {
            let retry_after = seconds_until_next_utc_day(now)
                .unwrap_or_else(|| Duration::from_secs(config.default_pool_cooldown_seconds));
            self.mark_model_cooldown(model_name, retry_after);
            return OpenRouterFreeQuotaDecision::Denied {
                reason: "openrouter_free_model_daily_limit".to_owned(),
                retry_after: Some(retry_after),
            };
        }
        OpenRouterFreeQuotaDecision::Allowed
    }

    pub fn mark_pool_cooldown(&self, retry_after: Duration) {
        let until = Instant::now() + retry_after;
        let mut guard = self
            .pool_cooldown_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(guard.map_or(until, |current| current.max(until)));
    }

    pub fn mark_model_cooldown(&self, model_name: &str, retry_after: Duration) {
        let until = Instant::now() + retry_after;
        let mut guard = self
            .model_cooldowns_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .entry(model_name.to_owned())
            .and_modify(|current| *current = (*current).max(until))
            .or_insert(until);
    }

    pub fn current_pool_cooldown(&self) -> Option<Duration> {
        let mut guard = self
            .pool_cooldown_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let until = (*guard)?;
        let now = Instant::now();
        if until <= now {
            *guard = None;
            None
        } else {
            Some(until.saturating_duration_since(now))
        }
    }

    pub fn current_model_cooldown(&self, model_name: &str) -> Option<Duration> {
        let mut guard = self
            .model_cooldowns_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let until = *guard.get(model_name)?;
        let now = Instant::now();
        if until <= now {
            guard.remove(model_name);
            None
        } else {
            Some(until.saturating_duration_since(now))
        }
    }

    pub async fn default_pool_cooldown(&self) -> Duration {
        self.cached_pool_config()
            .await
            .map(|config| Duration::from_secs(config.default_pool_cooldown_seconds))
            .unwrap_or_else(|_| Duration::from_secs(3_600))
    }

    pub async fn default_model_cooldown(&self) -> Duration {
        self.cached_pool_config()
            .await
            .map(|config| Duration::from_secs(config.model_cooldown_seconds))
            .unwrap_or_else(|_| Duration::from_secs(900))
    }

    pub async fn status_snapshot(&self) -> Value {
        let now = OffsetDateTime::now_utc();
        let mut status = serde_json::Map::new();
        status.insert("quota_scope".to_owned(), json!("per_model"));
        if let Ok(config) = self.cached_pool_config().await {
            status.insert("rpm_limit".to_owned(), json!(config.rpm_limit));
            status.insert(
                "daily_request_limit".to_owned(),
                json!(config.daily_request_limit),
            );
            status.insert(
                "refresh_interval_seconds".to_owned(),
                json!(config.refresh_interval_seconds),
            );
            status.insert(
                "auto_refresh_enabled".to_owned(),
                json!(config.auto_refresh_enabled),
            );
        }
        status.insert(
            "pool_cooldown_remaining_ms".to_owned(),
            json!(
                self.current_pool_cooldown()
                    .map(|duration| duration.as_millis())
            ),
        );
        status.insert(
            "model_cooldowns".to_owned(),
            json!(self.model_cooldowns_snapshot()),
        );
        match self.redis.get_multiplexed_async_connection().await {
            Ok(_) => {
                status.insert("redis_available".to_owned(), json!(true));
                status.insert(
                    "status_checked_at_unix".to_owned(),
                    json!(now.unix_timestamp()),
                );
            }
            Err(error) => {
                status.insert("redis_available".to_owned(), json!(false));
                status.insert("redis_error".to_owned(), json!(error.to_string()));
            }
        }
        Value::Object(status)
    }

    async fn cached_pool_config(
        &self,
    ) -> Result<OpenRouterFreePoolConfig, OpenRouterFreePoolError> {
        let now = Instant::now();
        {
            let guard = self
                .pool_config_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(cached) = guard.as_ref()
                && cached.expires_at > now
            {
                return Ok(cached.config.clone());
            }
        }
        let config = load_pool_config(&self.postgres).await?;
        let mut guard = self
            .pool_config_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Some(CachedOpenRouterFreePoolConfig {
            config: config.clone(),
            expires_at: Instant::now() + POOL_CONFIG_CACHE_TTL,
        });
        Ok(config)
    }

    fn model_cooldowns_snapshot(&self) -> Vec<Value> {
        let mut guard = self
            .model_cooldowns_until
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        guard.retain(|_, until| *until > now);
        guard
            .iter()
            .map(|(model, until)| {
                json!({
                    "model": model,
                    "remaining_ms": until.saturating_duration_since(now).as_millis(),
                })
            })
            .collect()
    }
}

async fn incr_with_ttl(
    connection: &mut redis::aio::MultiplexedConnection,
    key: &str,
    ttl_seconds: usize,
) -> redis::RedisResult<u64> {
    let count: u64 = redis::cmd("INCR").arg(key).query_async(connection).await?;
    let _: () = redis::cmd("EXPIRE")
        .arg(key)
        .arg(ttl_seconds)
        .query_async(connection)
        .await?;
    Ok(count)
}

fn quota_counter_keys(model_name: &str, now: OffsetDateTime) -> (String, String) {
    let model = quota_model_key(model_name);
    (
        format!(
            "openrouter_free:model:{model}:minute:{}",
            now.unix_timestamp().div_euclid(60)
        ),
        format!("openrouter_free:model:{model}:day:{}", now.date()),
    )
}

fn quota_model_key(model_name: &str) -> String {
    let trimmed = model_name.trim();
    if trimmed.is_empty() {
        return "empty".to_owned();
    }
    let mut out = String::with_capacity(trimmed.len() * 2);
    for byte in trimmed.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn seconds_until_next_utc_day(now: OffsetDateTime) -> Option<Duration> {
    let next_day = now.date().next_day()?.midnight().assume_utc();
    let seconds = next_day
        .unix_timestamp()
        .saturating_sub(now.unix_timestamp());
    u64::try_from(seconds).ok().map(Duration::from_secs)
}

pub fn retry_after_from_error_message(message: &str) -> Option<Duration> {
    let value = value_after_marker(message, "retry_after_seconds=")?;
    let seconds = value.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds))
}

pub fn message_indicates_openrouter_pool_cooldown(message: &str) -> bool {
    message.contains("openrouter_error_type=rate_limit_exceeded")
        || message.contains("status 429")
        || message.contains("status: 429")
        || message.to_ascii_lowercase().contains("quota")
}

pub fn message_indicates_openrouter_model_cooldown(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("status 503")
        || lower.contains("status: 503")
        || lower.contains("overload")
        || lower.contains("provider_error")
}

fn value_after_marker<'a>(message: &'a str, marker: &str) -> Option<&'a str> {
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let end = rest
        .find(|ch: char| ch.is_whitespace() || ch == ')' || ch == ',' || ch == ';')
        .unwrap_or(rest.len());
    let value = rest[..end].trim();
    (!value.is_empty()).then_some(value)
}

#[derive(Debug, thiserror::Error)]
pub enum OpenRouterFreePoolError {
    #[error("openrouter-free pool is missing")]
    MissingPool,
    #[error("openrouter-free provider is missing")]
    MissingProvider,
    #[error("openrouter-free pool is not managed by {MANAGED_BY}")]
    UnmanagedPool,
    #[error("invalid openrouter-free config: {0}")]
    InvalidConfig(String),
    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("storage operation failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("source parse failed: {0}")]
    Source(String),
}

#[derive(Clone, Debug)]
struct OpenRouterFreeContext {
    provider_id: i64,
    provider_endpoint: String,
    provider_api_key_ref: Option<String>,
    provider_api_key_encrypted: Option<Vec<u8>>,
    provider_config: Value,
    pool_id: i64,
    pool_config: OpenRouterFreePoolConfig,
}

pub async fn refresh_openrouter_free_pool(
    pool: &PgPool,
    http: &reqwest::Client,
) -> Result<OpenRouterFreeRefreshReport, OpenRouterFreePoolError> {
    let context = load_context(pool).await?;
    if !context.pool_config.auto_refresh_enabled {
        return Ok(OpenRouterFreeRefreshReport {
            refreshed: false,
            active_models: 0,
            disabled_models: 0,
            assignments: 0,
            ranking_version: None,
            source_updated_at: None,
            key_status_error: None,
            message: "openrouter-free auto refresh is disabled in pool config".to_owned(),
        });
    }
    let key_status_error = openrouter_key_status_error(http, &context).await;
    let source_json = fetch_json(http, &context.pool_config.source_url).await?;
    let source = parse_shirman_top_models(source_json).map_err(OpenRouterFreePoolError::Source)?;
    let models_url = context
        .provider_config
        .get("models_url")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("https://openrouter.ai/api/v1/models");
    let catalog = fetch_json(http, models_url).await?;
    let mut candidates = validate_free_candidates(&source.models, &catalog);
    candidates.truncate(context.pool_config.max_models);
    let mut report = apply_sync(pool, &context, &source, &candidates).await?;
    report.key_status_error = key_status_error;
    store_refresh_status(pool, &report).await?;
    Ok(report)
}

pub async fn load_pool_config(
    pool: &PgPool,
) -> Result<OpenRouterFreePoolConfig, OpenRouterFreePoolError> {
    load_context(pool).await.map(|context| context.pool_config)
}

async fn fetch_json(http: &reqwest::Client, url: &str) -> Result<Value, OpenRouterFreePoolError> {
    let response = http.get(url).send().await?.error_for_status()?;
    Ok(response.json::<Value>().await?)
}

async fn load_context(pool: &PgPool) -> Result<OpenRouterFreeContext, OpenRouterFreePoolError> {
    let pool_row =
        sqlx::query("SELECT id, config::text AS config FROM llm_capacity_pools WHERE name = $1")
            .bind(POOL_NAME)
            .fetch_optional(pool)
            .await?
            .ok_or(OpenRouterFreePoolError::MissingPool)?;
    let pool_id: i64 = pool_row.try_get("id")?;
    let pool_config_value = parse_json_text(pool_row.try_get("config")?)?;
    if pool_config_value.get("managed_by").and_then(Value::as_str) != Some(MANAGED_BY) {
        return Err(OpenRouterFreePoolError::UnmanagedPool);
    }
    let provider_row = sqlx::query(
        "SELECT id, endpoint, api_key_ref, api_key_encrypted, config::text AS config FROM llm_providers WHERE name = $1",
    )
    .bind(PROVIDER_NAME)
    .fetch_optional(pool)
    .await?
    .ok_or(OpenRouterFreePoolError::MissingProvider)?;
    let provider_id: i64 = provider_row.try_get("id")?;
    let provider_endpoint: Option<String> = provider_row.try_get("endpoint")?;
    let provider_api_key_ref: Option<String> = provider_row.try_get("api_key_ref")?;
    let provider_api_key_encrypted: Option<Vec<u8>> = provider_row.try_get("api_key_encrypted")?;
    Ok(OpenRouterFreeContext {
        provider_id,
        provider_endpoint: provider_endpoint
            .unwrap_or_else(|| "https://openrouter.ai/api/v1/chat/completions".to_owned()),
        provider_api_key_ref,
        provider_api_key_encrypted,
        provider_config: parse_json_text(provider_row.try_get("config")?)?,
        pool_id,
        pool_config: pool_config_from_value(&pool_config_value)?,
    })
}

fn pool_config_from_value(
    value: &Value,
) -> Result<OpenRouterFreePoolConfig, OpenRouterFreePoolError> {
    if !value.is_object() {
        return Err(OpenRouterFreePoolError::InvalidConfig(
            "pool config must be an object".to_owned(),
        ));
    }
    let defaults = OpenRouterFreePoolConfig::default();
    let target_workflows = value
        .get("target_workflows")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|items| !items.is_empty())
        .unwrap_or_else(|| defaults.target_workflows.clone());
    Ok(OpenRouterFreePoolConfig {
        auto_refresh_enabled: json_bool(
            value,
            "auto_refresh_enabled",
            defaults.auto_refresh_enabled,
        ),
        source_url: json_string(value, "source_url").unwrap_or(defaults.source_url),
        refresh_interval_seconds: json_u64(value, "refresh_interval_seconds")
            .unwrap_or(defaults.refresh_interval_seconds),
        refresh_on_startup: json_bool(value, "refresh_on_startup", defaults.refresh_on_startup),
        max_models: json_u64(value, "max_models")
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value > 0)
            .unwrap_or(defaults.max_models),
        rpm_limit: json_u64(value, "rpm_limit")
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
            .unwrap_or(defaults.rpm_limit),
        daily_request_limit: json_u64(value, "daily_request_limit")
            .and_then(|value| u32::try_from(value).ok())
            .filter(|value| *value > 0)
            .unwrap_or(defaults.daily_request_limit),
        default_pool_cooldown_seconds: json_u64(value, "default_pool_cooldown_seconds")
            .unwrap_or(defaults.default_pool_cooldown_seconds),
        model_cooldown_seconds: json_u64(value, "model_cooldown_seconds")
            .unwrap_or(defaults.model_cooldown_seconds),
        fallback_model: json_string(value, "fallback_model").unwrap_or(defaults.fallback_model),
        target_workflows,
    })
}

fn parse_json_text(raw: Option<String>) -> Result<Value, OpenRouterFreePoolError> {
    raw.as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| OpenRouterFreePoolError::InvalidConfig(error.to_string()))
        .map(|value| value.unwrap_or_else(|| json!({})))
}

async fn openrouter_key_status_error(
    http: &reqwest::Client,
    context: &OpenRouterFreeContext,
) -> Option<String> {
    let api_key = resolve_provider_api_key(
        context.provider_api_key_ref.as_deref(),
        context.provider_api_key_encrypted.as_deref(),
    )?;
    let url = context
        .provider_config
        .get("key_status_url")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("https://openrouter.ai/api/v1/key");
    match http.get(url).bearer_auth(api_key).send().await {
        Ok(response) if response.status().is_success() => None,
        Ok(response) => Some(format!("key status request returned {}", response.status())),
        Err(error) => Some(error.to_string()),
    }
}

fn resolve_provider_api_key(
    api_key_ref: Option<&str>,
    api_key_encrypted: Option<&[u8]>,
) -> Option<String> {
    if let Some(reference) = api_key_ref.filter(|reference| !reference.trim().is_empty()) {
        return std::env::var(reference.trim()).ok();
    }
    let sealed = api_key_encrypted?;
    let master = std::env::var("MASTER_KEY").unwrap_or_default();
    openplotva_storage::llm_routing::open_key(&master, sealed).ok()
}

fn json_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
}

fn json_bool(value: &Value, key: &str, default: bool) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(default)
}

fn json_u64(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

async fn apply_sync(
    pool: &PgPool,
    context: &OpenRouterFreeContext,
    source: &OpenRouterFreeSource,
    candidates: &[OpenRouterFreeCandidate],
) -> Result<OpenRouterFreeRefreshReport, OpenRouterFreePoolError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('openrouter_free_pool_refresh'))")
        .execute(&mut *tx)
        .await?;

    let workflow_keys =
        existing_target_workflows(&mut tx, &context.pool_config.target_workflows).await?;
    let mut all_candidates = candidates.to_vec();
    if !all_candidates
        .iter()
        .any(|candidate| candidate.id == context.pool_config.fallback_model)
    {
        all_candidates.push(fallback_candidate(&context.pool_config.fallback_model));
    }

    let mut model_ids = Vec::new();
    let mut model_names = Vec::new();
    for candidate in &all_candidates {
        let id = upsert_managed_model(&mut tx, context, source, candidate).await?;
        model_ids.push((candidate.id.clone(), id));
        model_names.push(candidate.id.clone());
    }
    let disabled_models = disable_stale_models(&mut tx, context.provider_id, &model_names).await?;
    delete_managed_assignments(&mut tx, &workflow_keys).await?;

    let mut assignment_config = context.pool_config.clone();
    assignment_config.target_workflows = workflow_keys;
    let plan = managed_assignment_plan(&assignment_config, &all_candidates);
    let mut inserted_assignments = 0;
    for assignment in &plan {
        let Some((_, model_id)) = model_ids
            .iter()
            .find(|(name, _)| name == &assignment.model_name)
        else {
            continue;
        };
        insert_managed_assignment(&mut tx, assignment, *model_id).await?;
        inserted_assignments += 1;
    }

    tx.commit().await?;
    Ok(OpenRouterFreeRefreshReport {
        refreshed: true,
        active_models: all_candidates.len(),
        disabled_models,
        assignments: inserted_assignments,
        ranking_version: source.ranking_version.clone(),
        source_updated_at: source.updated_at.clone(),
        key_status_error: None,
        message: "openrouter-free pool refreshed".to_owned(),
    })
}

async fn existing_target_workflows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    configured: &[String],
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT key FROM workflows WHERE key = ANY($1) AND key <> 'dialog' ORDER BY key ASC",
    )
    .bind(configured)
    .fetch_all(&mut **tx)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| row.try_get::<String, _>("key").ok())
        .collect())
}

async fn upsert_managed_model(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    context: &OpenRouterFreeContext,
    source: &OpenRouterFreeSource,
    candidate: &OpenRouterFreeCandidate,
) -> Result<i64, sqlx::Error> {
    let config = candidate_config(source, candidate).to_string();
    let row = sqlx::query(
        r#"
        INSERT INTO provider_models (
            provider_id,
            model_name,
            display_name,
            base_url,
            capabilities,
            pool_id,
            enabled,
            config
        )
        VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7::jsonb)
        ON CONFLICT (provider_id, model_name, base_url) DO UPDATE SET
            display_name = EXCLUDED.display_name,
            capabilities = EXCLUDED.capabilities,
            pool_id = EXCLUDED.pool_id,
            enabled = TRUE,
            config = provider_models.config || EXCLUDED.config
        RETURNING id
        "#,
    )
    .bind(context.provider_id)
    .bind(&candidate.id)
    .bind(candidate.display_name.as_deref())
    .bind(&context.provider_endpoint)
    .bind(Vec::<String>::from(["chat".to_owned()]))
    .bind(context.pool_id)
    .bind(config)
    .fetch_one(&mut **tx)
    .await?;
    row.try_get("id")
}

fn candidate_config(source: &OpenRouterFreeSource, candidate: &OpenRouterFreeCandidate) -> Value {
    json!({
        "managed_by": MANAGED_BY,
        "ranking_version": source.ranking_version,
        "source_updated_at": source.updated_at,
        "source_rank": candidate.source_rank,
        "score": candidate.score,
        "context_length": candidate.context_length,
        "max_completion_tokens": candidate.max_completion_tokens,
        "supports_tools": candidate.supports_tools,
        "supports_structured_outputs": candidate.supports_structured_outputs,
        "supports_response_format": candidate.supports_response_format,
        "supports_reasoning": candidate.supports_reasoning,
        "health_status": candidate.health_status,
        "last_validated_at": OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned()),
    })
}

async fn disable_stale_models(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    provider_id: i64,
    active_names: &[String],
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE provider_models SET enabled = FALSE WHERE provider_id = $1 AND config ->> 'managed_by' = $2 AND NOT (model_name = ANY($3))",
    )
    .bind(provider_id)
    .bind(MANAGED_BY)
    .bind(active_names)
    .execute(&mut **tx)
    .await?;
    Ok(result.rows_affected())
}

async fn delete_managed_assignments(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_keys: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "DELETE FROM workflow_assignments WHERE workflow_key = ANY($1) AND inference_overrides ->> 'managed_by' = $2",
    )
    .bind(workflow_keys)
    .bind(MANAGED_BY)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_managed_assignment(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    assignment: &ManagedAssignmentPlan,
    model_id: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO workflow_assignments (
            workflow_key,
            scope,
            role,
            provider_model_id,
            weight,
            fallback_order,
            enabled,
            inference_overrides,
            cb_failure_threshold,
            cb_cooldown_ms
        )
        VALUES ($1, 'global', $2, $3, $4, $5, TRUE, $6::jsonb, 3, 3600000)
        "#,
    )
    .bind(&assignment.workflow_key)
    .bind(&assignment.role)
    .bind(model_id)
    .bind(None::<i32>)
    .bind(assignment.fallback_order)
    .bind(
        json!({
            "managed_by": MANAGED_BY,
            "openrouter_free_model": assignment.model_name,
        })
        .to_string(),
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn fallback_candidate(model_name: &str) -> OpenRouterFreeCandidate {
    OpenRouterFreeCandidate {
        id: model_name.to_owned(),
        display_name: Some("OpenRouter Free Router".to_owned()),
        score: None,
        source_rank: usize::MAX,
        context_length: None,
        max_completion_tokens: None,
        supports_tools: true,
        supports_structured_outputs: true,
        supports_response_format: true,
        supports_reasoning: true,
        health_status: Some("fallback".to_owned()),
    }
}

async fn store_refresh_status(
    pool: &PgPool,
    report: &OpenRouterFreeRefreshReport,
) -> Result<(), OpenRouterFreePoolError> {
    sqlx::query(
        r#"
        INSERT INTO app_settings (key, value, updated_at)
        VALUES ($1, $2, now())
        ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value,
                updated_at = now()
        "#,
    )
    .bind("llm.routing.openrouter_free.status")
    .bind(
        json!({
            "refreshed": report.refreshed,
            "active_models": report.active_models,
            "disabled_models": report.disabled_models,
            "assignments": report.assignments,
            "ranking_version": report.ranking_version,
            "source_updated_at": report.source_updated_at,
            "key_status_error": report.key_status_error,
            "message": report.message,
            "updated_at": OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned()),
        })
        .to_string(),
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub fn managed_assignment_plan(
    config: &OpenRouterFreePoolConfig,
    candidates: &[OpenRouterFreeCandidate],
) -> Vec<ManagedAssignmentPlan> {
    let mut out = Vec::new();
    for workflow in config
        .target_workflows
        .iter()
        .filter(|workflow| workflow.as_str() != "dialog")
    {
        let mut compatible = candidates
            .iter()
            .filter(|candidate| candidate_is_compatible_with_workflow(candidate, workflow))
            .take(config.max_models)
            .collect::<Vec<_>>();
        compatible.sort_by_key(|candidate| candidate.source_rank);

        let mut seen_models = HashSet::new();
        let mut fallback_models = compatible
            .iter()
            .map(|candidate| candidate.id.clone())
            .filter(|model| model != &config.fallback_model)
            .filter(|model| seen_models.insert(model.clone()))
            .collect::<Vec<_>>();
        fallback_models.push(config.fallback_model.clone());

        for (index, model_name) in fallback_models.into_iter().enumerate() {
            out.push(ManagedAssignmentPlan {
                workflow_key: workflow.clone(),
                model_name,
                role: "fallback".to_owned(),
                fallback_order: Some(i32::try_from(index + 1).unwrap_or(i32::MAX)),
            });
        }
    }
    out
}

fn candidate_is_compatible_with_workflow(
    candidate: &OpenRouterFreeCandidate,
    workflow: &str,
) -> bool {
    if candidate.id == FALLBACK_MODEL {
        return true;
    }
    match workflow {
        "memory_consolidation" | "history_summary" | "media_prompt_optimizer" => {
            candidate.supports_response_format || candidate.supports_structured_outputs
        }
        "agentic_search_reasoner" | "agentic_search_writer" | "agentic_song" | "agentic_image" => {
            candidate.supports_tools
        }
        _ => true,
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ShirmanSource {
    updated_at: Option<String>,
    ranking_version: Option<String>,
    base_url: Option<String>,
    fallback: Option<ShirmanFallback>,
    #[serde(default)]
    models: Vec<ShirmanModel>,
}

#[derive(Deserialize)]
struct ShirmanFallback {
    id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ShirmanModel {
    id: String,
    name: Option<String>,
    score: Option<i64>,
    context_length: Option<i64>,
    max_completion_tokens: Option<i64>,
    #[serde(default)]
    supports_tools: bool,
    #[serde(default)]
    supports_structured_outputs: bool,
    #[serde(default)]
    supports_response_format: bool,
    #[serde(default)]
    supports_reasoning: bool,
    health_status: Option<String>,
}

pub fn parse_shirman_top_models(value: Value) -> Result<OpenRouterFreeSource, String> {
    let parsed: ShirmanSource =
        serde_json::from_value(value).map_err(|error| format!("invalid source: {error}"))?;
    let models = parsed
        .models
        .into_iter()
        .enumerate()
        .filter(|(_, model)| !model.id.trim().is_empty())
        .map(|(index, model)| OpenRouterFreeCandidate {
            id: model.id,
            display_name: model.name,
            score: model.score,
            source_rank: index + 1,
            context_length: model.context_length,
            max_completion_tokens: model.max_completion_tokens,
            supports_tools: model.supports_tools,
            supports_structured_outputs: model.supports_structured_outputs,
            supports_response_format: model.supports_response_format,
            supports_reasoning: model.supports_reasoning,
            health_status: model.health_status,
        })
        .collect();
    Ok(OpenRouterFreeSource {
        updated_at: parsed.updated_at,
        ranking_version: parsed.ranking_version,
        base_url: parsed.base_url,
        fallback_model: parsed.fallback.and_then(|fallback| fallback.id),
        models,
    })
}

pub fn validate_free_candidates(
    candidates: &[OpenRouterFreeCandidate],
    catalog: &Value,
) -> Vec<OpenRouterFreeCandidate> {
    candidates
        .iter()
        .filter(|candidate| model_id_is_free(&candidate.id))
        .filter(|candidate| catalog_model_is_free(catalog, &candidate.id))
        .cloned()
        .collect()
}

fn model_id_is_free(id: &str) -> bool {
    id == FALLBACK_MODEL || id.ends_with(":free")
}

fn catalog_model_is_free(catalog: &Value, id: &str) -> bool {
    catalog
        .get("data")
        .and_then(Value::as_array)
        .or_else(|| catalog.get("models").and_then(Value::as_array))
        .into_iter()
        .flatten()
        .find(|model| model.get("id").and_then(Value::as_str) == Some(id))
        .and_then(|model| model.get("pricing"))
        .is_some_and(pricing_is_zero)
}

fn pricing_is_zero(pricing: &Value) -> bool {
    ["prompt", "completion", "request"]
        .into_iter()
        .all(|key| price_component_is_zero(pricing.get(key)))
}

fn price_component_is_zero(value: Option<&Value>) -> bool {
    match value {
        None => true,
        Some(Value::String(raw)) => raw.parse::<f64>().is_ok_and(|price| price == 0.0),
        Some(Value::Number(number)) => number.as_f64() == Some(0.0),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shirman_source_parser_keeps_ranking_and_fallback() {
        let raw = json!({
            "updatedAt": "2026-07-07T03:17:45.078Z",
            "rankingVersion": "2026-07-07.v1",
            "baseUrl": "https://openrouter.ai/api/v1",
            "fallback": { "id": "openrouter/free" },
            "models": [{
                "id": "openai/gpt-oss-20b:free",
                "name": "GPT OSS 20B",
                "score": 1175,
                "contextLength": 131072,
                "maxCompletionTokens": 32768,
                "supportsTools": true,
                "supportsStructuredOutputs": true,
                "supportsResponseFormat": true,
                "supportsReasoning": true,
                "healthStatus": "ok"
            }]
        });

        let source = parse_shirman_top_models(raw).expect("valid source");

        assert_eq!(source.ranking_version.as_deref(), Some("2026-07-07.v1"));
        assert_eq!(source.fallback_model.as_deref(), Some("openrouter/free"));
        assert_eq!(source.models[0].id, "openai/gpt-oss-20b:free");
        assert!(source.models[0].supports_response_format);
    }

    #[test]
    fn openrouter_validation_accepts_only_free_models_with_zero_pricing() {
        let candidates = vec![
            OpenRouterFreeCandidate {
                id: "openai/gpt-oss-20b:free".to_owned(),
                display_name: Some("GPT OSS 20B".to_owned()),
                score: Some(1175),
                source_rank: 1,
                context_length: Some(131_072),
                max_completion_tokens: Some(32_768),
                supports_tools: true,
                supports_structured_outputs: true,
                supports_response_format: true,
                supports_reasoning: true,
                health_status: Some("ok".to_owned()),
            },
            OpenRouterFreeCandidate {
                id: "paid/model".to_owned(),
                display_name: None,
                score: None,
                source_rank: 2,
                context_length: None,
                max_completion_tokens: None,
                supports_tools: true,
                supports_structured_outputs: true,
                supports_response_format: true,
                supports_reasoning: false,
                health_status: None,
            },
        ];
        let catalog = json!({
            "data": [
                {
                    "id": "openai/gpt-oss-20b:free",
                    "pricing": { "prompt": "0", "completion": "0", "request": "0" },
                    "supported_parameters": ["tools", "response_format", "structured_outputs", "reasoning"]
                },
                {
                    "id": "paid/model",
                    "pricing": { "prompt": "0.1", "completion": "0", "request": "0" },
                    "supported_parameters": ["tools"]
                }
            ]
        });

        let validated = validate_free_candidates(&candidates, &catalog);

        assert_eq!(validated.len(), 1);
        assert_eq!(validated[0].id, "openai/gpt-oss-20b:free");
    }

    #[test]
    fn assignment_plan_uses_only_fallback_candidates_and_excludes_dialog() {
        let candidates = vec![
            OpenRouterFreeCandidate {
                id: "top/no-json:free".to_owned(),
                display_name: None,
                score: Some(2000),
                source_rank: 1,
                context_length: None,
                max_completion_tokens: None,
                supports_tools: true,
                supports_structured_outputs: false,
                supports_response_format: false,
                supports_reasoning: true,
                health_status: None,
            },
            OpenRouterFreeCandidate {
                id: "structured/model:free".to_owned(),
                display_name: None,
                score: Some(1000),
                source_rank: 2,
                context_length: None,
                max_completion_tokens: None,
                supports_tools: true,
                supports_structured_outputs: true,
                supports_response_format: true,
                supports_reasoning: true,
                health_status: None,
            },
            OpenRouterFreeCandidate {
                id: FALLBACK_MODEL.to_owned(),
                display_name: None,
                score: None,
                source_rank: 1,
                context_length: None,
                max_completion_tokens: None,
                supports_tools: true,
                supports_structured_outputs: true,
                supports_response_format: true,
                supports_reasoning: true,
                health_status: None,
            },
        ];
        let config = OpenRouterFreePoolConfig {
            target_workflows: vec!["dialog".to_owned(), "memory_consolidation".to_owned()],
            fallback_model: FALLBACK_MODEL.to_owned(),
            ..OpenRouterFreePoolConfig::default()
        };

        let plan = managed_assignment_plan(&config, &candidates);

        assert!(
            !plan
                .iter()
                .any(|assignment| assignment.workflow_key == "dialog")
        );
        let memory = plan
            .iter()
            .filter(|assignment| assignment.workflow_key == "memory_consolidation")
            .collect::<Vec<_>>();
        assert_eq!(memory[0].model_name, "structured/model:free");
        assert_eq!(memory[0].role, "fallback");
        assert_eq!(memory[0].fallback_order, Some(1));
        assert_eq!(memory.last().expect("fallback").model_name, FALLBACK_MODEL);
        assert_eq!(
            memory
                .iter()
                .filter(|assignment| assignment.model_name == FALLBACK_MODEL)
                .count(),
            1
        );
        assert!(
            memory
                .iter()
                .all(|assignment| assignment.role == "fallback")
        );
    }

    #[test]
    fn default_config_does_not_auto_target_memory_consolidation() {
        let config = OpenRouterFreePoolConfig::default();

        assert!(
            !config
                .target_workflows
                .iter()
                .any(|workflow| workflow == "memory_consolidation")
        );
    }

    #[test]
    fn retry_after_hint_marks_pool_cooldown_message() {
        let message = "direct dialog request: status 429: Rate limited (retry_after_seconds=3600 openrouter_error_type=rate_limit_exceeded)";

        assert_eq!(
            retry_after_from_error_message(message),
            Some(Duration::from_secs(3600))
        );
        assert!(message_indicates_openrouter_pool_cooldown(message));
    }

    #[test]
    fn provider_overload_marks_model_not_pool_cooldown() {
        let message = "direct dialog request: status 503: Provider overloaded (retry_after_seconds=900 openrouter_error_type=provider_error)";

        assert_eq!(
            retry_after_from_error_message(message),
            Some(Duration::from_secs(900))
        );
        assert!(!message_indicates_openrouter_pool_cooldown(message));
        assert!(message_indicates_openrouter_model_cooldown(message));
    }

    #[test]
    fn quota_counter_keys_are_scoped_per_model() {
        let now = OffsetDateTime::from_unix_timestamp(1_789_000_000).expect("valid test timestamp");

        let first = quota_counter_keys("cohere/north-mini-code:free", now);
        let second = quota_counter_keys("tencent/hy3:free", now);

        assert_ne!(first, second);
        assert!(first.0.starts_with("openrouter_free:model:"));
        assert!(first.1.starts_with("openrouter_free:model:"));
    }
}
