//! Read-only aggregation for the admin "External Requests" analytics dashboard.
//!
//! One snapshot over the recent window covering every outbound external call:
//! LLM providers (the rich group — prefill/decode/e2e latency, throughput, token
//! economy, request shape), shield/moderation, generation/job backends, routing
//! health, and reply outcomes. All percentiles are exact over raw rows within the
//! window; the window is capped at the raw-retention horizon so every number is
//! honest (no rollup approximation). Long-range historical trends stay on the
//! rollup-backed views elsewhere.

use std::time::Duration as StdDuration;

use serde_json::{Value, json};
use sqlx::{PgPool, Row, postgres::PgRow};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

/// Default window when the range is missing or unparseable.
const DEFAULT_RANGE: Duration = Duration::hours(24);
/// Hard ceiling — matches raw `llm_request_events` retention so aggregates stay exact.
const MAX_RANGE: Duration = Duration::days(14);

/// Resolved query window.
struct RangeSpec {
    duration: Duration,
    bucket: &'static str,
    since: OffsetDateTime,
}

fn parse_range(range: &str, now: OffsetDateTime) -> RangeSpec {
    let duration = parse_duration(range.trim())
        .filter(|d| *d > Duration::ZERO)
        .unwrap_or(DEFAULT_RANGE)
        .min(MAX_RANGE);
    // Keep every series chart to a readable point count.
    let bucket = if duration <= Duration::days(2) {
        "hour"
    } else {
        "day"
    };
    RangeSpec {
        duration,
        bucket,
        since: now - duration,
    }
}

fn parse_duration(value: &str) -> Option<Duration> {
    if let Some(days) = value.strip_suffix('d').and_then(|v| v.parse::<i64>().ok()) {
        return (days > 0).then(|| Duration::days(days));
    }
    if let Some(hours) = value.strip_suffix('h').and_then(|v| v.parse::<i64>().ok()) {
        return (hours > 0).then(|| Duration::hours(hours));
    }
    if let Some(mins) = value.strip_suffix('m').and_then(|v| v.parse::<i64>().ok()) {
        return (mins > 0).then(|| Duration::minutes(mins));
    }
    None
}

fn go_duration_string(duration: Duration) -> String {
    let secs = duration.whole_seconds().max(0);
    if secs % 86_400 == 0 {
        format!("{}d", secs / 86_400)
    } else if secs % 3_600 == 0 {
        format!("{}h", secs / 3_600)
    } else {
        format!("{}m", secs / 60)
    }
}

/// Postgres-backed reader for the analytics overview snapshot.
#[derive(Clone)]
pub struct AnalyticsOverviewReader {
    pool: PgPool,
    sql_timeout: StdDuration,
}

impl AnalyticsOverviewReader {
    #[must_use]
    pub fn new(pool: PgPool, sql_timeout_ms: i32) -> Self {
        let ms = u64::try_from(sql_timeout_ms)
            .unwrap_or(10_000)
            .clamp(1_000, 60_000);
        Self {
            pool,
            sql_timeout: StdDuration::from_millis(ms),
        }
    }

    /// Build the full snapshot, or a string error the admin handler surfaces.
    pub async fn overview(&self, range: &str) -> Result<Value, String> {
        match tokio::time::timeout(
            self.sql_timeout,
            self.build(range, OffsetDateTime::now_utc()),
        )
        .await
        {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("analytics overview query timed out".to_owned()),
        }
    }

    async fn build(&self, range: &str, now: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let spec = parse_range(range, now);
        let since = spec.since;
        // Whitelisted identifier — never user input.
        let bucket = spec.bucket;

        let llm_series = self.llm_series(since, bucket).await?;
        let llm_latency = self.llm_latency(since).await?;
        let llm_tps = self.llm_tps(since).await?;
        let llm_tokens = self.llm_token_series(since, bucket).await?;
        let llm_shape = self.llm_shape(since).await?;
        let llm_models = self.llm_models(since).await?;
        let llm_providers = self.llm_providers(since).await?;
        let llm_flows = self.llm_flows(since).await?;
        let health = self.llm_health(since).await?;
        let shield = self.shield(since, bucket).await?;
        let jobs = self.jobs(since).await?;
        let routing = self.routing(since).await?;

        Ok(json!({
            "range": go_duration_string(spec.duration),
            "bucket": bucket,
            "since": since.format(&Rfc3339).unwrap_or_default(),
            "generated_at": now.format(&Rfc3339).unwrap_or_default(),
            "health": health,
            "llm": {
                "series": llm_series,
                "latency": llm_latency,
                "tps": llm_tps,
                "tokens_series": llm_tokens,
                "shape": llm_shape,
                "models": llm_models,
                "providers": llm_providers,
                "flows": llm_flows,
            },
            "shield": shield,
            "jobs": jobs,
            "routing": routing,
        }))
    }

    async fn llm_health(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let row = sqlx::query(SQL_LLM_HEALTH)
            .bind(since)
            .fetch_one(&self.pool)
            .await?;
        let shield = sqlx::query(SQL_SHIELD_HEALTH)
            .bind(since)
            .fetch_one(&self.pool)
            .await?;
        let routing_open: i64 = sqlx::query(SQL_ROUTING_OPEN)
            .bind(since)
            .fetch_one(&self.pool)
            .await?
            .try_get("n")?;
        Ok(json!({
            "external_calls": get_i64(&row, "external_calls"),
            "error_pct": get_f64(&row, "error_pct"),
            "p95_e2e_ms": get_i64(&row, "p95_e2e_ms"),
            "ttft_p50_ms": get_i64(&row, "ttft_p50_ms"),
            "gen_tps_p50": get_f64(&row, "gen_tps_p50"),
            "tokens_in": get_i64(&row, "tokens_in"),
            "tokens_out": get_i64(&row, "tokens_out"),
            "cache_hit_pct": get_f64(&row, "cache_hit_pct"),
            "shield_checks": get_i64(&shield, "checks"),
            "shield_flag_pct": get_f64(&shield, "flag_pct"),
            "routing_incidents_open": routing_open,
        }))
    }

    async fn llm_series(&self, since: OffsetDateTime, bucket: &str) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_SERIES)
            .bind(since)
            .bind(bucket)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "ts": get_text(row, "ts"),
                        "provider": get_text(row, "provider"),
                        "request_count": get_i64(row, "request_count"),
                        "error_count": get_i64(row, "error_count"),
                        "tokens_out": get_i64(row, "tokens_out"),
                    })
                })
                .collect(),
        ))
    }

    async fn llm_latency(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_LATENCY)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "provider": get_text(row, "provider"),
                        "request_count": get_i64(row, "request_count"),
                        "ttft_p50": get_opt_i64(row, "ttft_p50"),
                        "ttft_p95": get_opt_i64(row, "ttft_p95"),
                        "decode_p50": get_opt_i64(row, "decode_p50"),
                        "decode_p95": get_opt_i64(row, "decode_p95"),
                        "e2e_p50": get_i64(row, "e2e_p50"),
                        "e2e_p95": get_i64(row, "e2e_p95"),
                    })
                })
                .collect(),
        ))
    }

    async fn llm_tps(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_TPS)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "model": get_text(row, "model"),
                        "gen_tps": get_f64(row, "gen_tps"),
                        "effective_tps": get_f64(row, "effective_tps"),
                        "request_count": get_i64(row, "request_count"),
                    })
                })
                .collect(),
        ))
    }

    async fn llm_token_series(
        &self,
        since: OffsetDateTime,
        bucket: &str,
    ) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_TOKEN_SERIES)
            .bind(since)
            .bind(bucket)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "ts": get_text(row, "ts"),
                        "input": get_i64(row, "input"),
                        "output": get_i64(row, "output"),
                        "cached": get_i64(row, "cached"),
                        "thoughts": get_i64(row, "thoughts"),
                    })
                })
                .collect(),
        ))
    }

    async fn llm_shape(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_SHAPE)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "flow": get_text(row, "flow"),
                        "request_count": get_i64(row, "request_count"),
                        "avg_prompt_chars": get_i64(row, "avg_prompt_chars"),
                        "p95_prompt_chars": get_i64(row, "p95_prompt_chars"),
                        "avg_prompt_messages": get_f64(row, "avg_prompt_messages"),
                        "avg_docs_chars": get_i64(row, "avg_docs_chars"),
                    })
                })
                .collect(),
        ))
    }

    async fn llm_models(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_MODELS)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "model": get_text(row, "model"),
                        "provider": get_text(row, "provider"),
                        "request_count": get_i64(row, "request_count"),
                        "error_count": get_i64(row, "error_count"),
                        "ttft_p50": get_opt_i64(row, "ttft_p50"),
                        "ttft_p95": get_opt_i64(row, "ttft_p95"),
                        "gen_tps": get_f64(row, "gen_tps"),
                        "tokens_in": get_i64(row, "tokens_in"),
                        "tokens_out": get_i64(row, "tokens_out"),
                        "cache_hit_pct": get_f64(row, "cache_hit_pct"),
                        "avg_iterations": get_f64(row, "avg_iterations"),
                    })
                })
                .collect(),
        ))
    }

    async fn llm_providers(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_PROVIDERS)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        let total: i64 = rows.iter().map(|row| get_i64(row, "request_count")).sum();
        let total = total.max(1) as f64;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    let n = get_i64(row, "request_count");
                    json!({
                        "provider": get_text(row, "provider"),
                        "request_count": n,
                        "error_count": get_i64(row, "error_count"),
                        "p95_e2e_ms": get_i64(row, "p95_e2e_ms"),
                        "gen_tps": get_f64(row, "gen_tps"),
                        "request_share": (n as f64 / total * 100.0 * 10.0).round() / 10.0,
                    })
                })
                .collect(),
        ))
    }

    async fn llm_flows(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_FLOWS)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "flow": get_text(row, "flow"),
                        "request_count": get_i64(row, "request_count"),
                        "error_count": get_i64(row, "error_count"),
                    })
                })
                .collect(),
        ))
    }

    async fn shield(&self, since: OffsetDateTime, bucket: &str) -> Result<Value, sqlx::Error> {
        let series = sqlx::query(SQL_SHIELD_SERIES)
            .bind(since)
            .bind(bucket)
            .fetch_all(&self.pool)
            .await?;
        let totals = sqlx::query(SQL_SHIELD_HEALTH)
            .bind(since)
            .fetch_one(&self.pool)
            .await?;
        Ok(json!({
            "total": get_i64(&totals, "checks"),
            "flag_pct": get_f64(&totals, "flag_pct"),
            "p95_duration_ms": get_i64(&totals, "p95_ms"),
            "series": Value::Array(series.iter().map(|row| json!({
                "ts": get_text(row, "ts"),
                "checks": get_i64(row, "checks"),
                "flagged": get_i64(row, "flagged"),
                "p95_duration_ms": get_i64(row, "p95_ms"),
            })).collect()),
        }))
    }

    async fn jobs(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let rows = sqlx::query(SQL_JOBS)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(Value::Array(
            rows.iter()
                .map(|row| {
                    json!({
                        "job_type": get_text(row, "job_type"),
                        "queue_name": get_text(row, "queue_name"),
                        "job_count": get_i64(row, "job_count"),
                        "completed": get_i64(row, "completed"),
                        "failed": get_i64(row, "failed"),
                        "p95_wait_ms": get_i64(row, "p95_wait_ms"),
                        "p95_processing_ms": get_i64(row, "p95_processing_ms"),
                    })
                })
                .collect(),
        ))
    }

    async fn routing(&self, since: OffsetDateTime) -> Result<Value, sqlx::Error> {
        let recent = sqlx::query(SQL_ROUTING_RECENT)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        let by_type = sqlx::query(SQL_ROUTING_BY_TYPE)
            .bind(since)
            .fetch_all(&self.pool)
            .await?;
        Ok(json!({
            "recent": Value::Array(recent.iter().map(|row| json!({
                "at": get_text(row, "at"),
                "severity": get_text(row, "severity"),
                "event_type": get_text(row, "event_type"),
                "provider_id": get_text(row, "provider_id"),
                "model_id": get_text(row, "model_id"),
                "summary": get_text(row, "summary"),
            })).collect()),
            "by_type": Value::Array(by_type.iter().map(|row| json!({
                "event_type": get_text(row, "event_type"),
                "n": get_i64(row, "n"),
            })).collect()),
        }))
    }
}

fn get_i64(row: &PgRow, col: &str) -> i64 {
    row.try_get::<Option<i64>, _>(col)
        .ok()
        .flatten()
        .unwrap_or(0)
}

fn get_opt_i64(row: &PgRow, col: &str) -> Value {
    match row.try_get::<Option<i64>, _>(col) {
        Ok(Some(v)) => json!(v),
        _ => Value::Null,
    }
}

fn get_f64(row: &PgRow, col: &str) -> f64 {
    let v = row
        .try_get::<Option<f64>, _>(col)
        .ok()
        .flatten()
        .unwrap_or(0.0);
    (v * 100.0).round() / 100.0
}

fn get_text(row: &PgRow, col: &str) -> String {
    row.try_get::<Option<String>, _>(col)
        .ok()
        .flatten()
        .unwrap_or_default()
}

// All aggregates run over RAW rows within the window (exact percentiles; window
// capped at raw retention). `$1` is the window start. `{bucket}` is a whitelisted
// identifier (`hour`|`day`), never user input.

const SQL_LLM_HEALTH: &str = r#"
SELECT
    count(*)::bigint AS external_calls,
    (count(*) FILTER (WHERE error IS NOT NULL AND error <> '')::float8
        / NULLIF(count(*), 0) * 100.0)::float8 AS error_pct,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::bigint AS p95_e2e_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY prompt_eval_ms)
        FILTER (WHERE prompt_eval_ms > 0)::bigint AS ttft_p50_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY generation_tps)
        FILTER (WHERE generation_tps > 0)::float8 AS gen_tps_p50,
    COALESCE(sum(input_tokens), 0)::bigint AS tokens_in,
    COALESCE(sum(output_tokens), 0)::bigint AS tokens_out,
    (COALESCE(sum(cached_tokens), 0)::float8
        / NULLIF(sum(input_tokens), 0) * 100.0)::float8 AS cache_hit_pct
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1"#;

const SQL_LLM_SERIES: &str = r#"
SELECT
    to_char(date_trunc($2, created_at), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ts,
    COALESCE(NULLIF(provider, ''), 'unknown')::text AS provider,
    count(*)::bigint AS request_count,
    count(*) FILTER (WHERE error IS NOT NULL AND error <> '')::bigint AS error_count,
    COALESCE(sum(output_tokens), 0)::bigint AS tokens_out
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1, 2
ORDER BY 1"#;

const SQL_LLM_LATENCY: &str = r#"
SELECT
    COALESCE(NULLIF(provider, ''), 'unknown')::text AS provider,
    count(*)::bigint AS request_count,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY prompt_eval_ms) FILTER (WHERE prompt_eval_ms > 0)::bigint AS ttft_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY prompt_eval_ms) FILTER (WHERE prompt_eval_ms > 0)::bigint AS ttft_p95,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY generation_ms) FILTER (WHERE generation_ms > 0)::bigint AS decode_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY generation_ms) FILTER (WHERE generation_ms > 0)::bigint AS decode_p95,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)::bigint AS e2e_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::bigint AS e2e_p95
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1
ORDER BY 2 DESC"#;

const SQL_LLM_TPS: &str = r#"
SELECT
    COALESCE(NULLIF(model, ''), 'unknown')::text AS model,
    COALESCE(avg(generation_tps) FILTER (WHERE generation_tps > 0), 0)::float8 AS gen_tps,
    COALESCE(avg(effective_output_tps) FILTER (WHERE effective_output_tps > 0), 0)::float8 AS effective_tps,
    count(*)::bigint AS request_count
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1
ORDER BY 4 DESC"#;

const SQL_LLM_TOKEN_SERIES: &str = r#"
SELECT
    to_char(date_trunc($2, created_at), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ts,
    COALESCE(sum(input_tokens), 0)::bigint AS input,
    COALESCE(sum(output_tokens), 0)::bigint AS output,
    COALESCE(sum(cached_tokens), 0)::bigint AS cached,
    COALESCE(sum(thoughts_tokens), 0)::bigint AS thoughts
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1
ORDER BY 1"#;

const SQL_LLM_SHAPE: &str = r#"
SELECT
    COALESCE(NULLIF(flow, ''), 'other')::text AS flow,
    count(*)::bigint AS request_count,
    COALESCE(avg(prompt_chars), 0)::bigint AS avg_prompt_chars,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY prompt_chars)::bigint AS p95_prompt_chars,
    COALESCE(avg(prompt_messages), 0)::float8 AS avg_prompt_messages,
    COALESCE(avg(docs_chars), 0)::bigint AS avg_docs_chars
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1
ORDER BY 2 DESC"#;

const SQL_LLM_MODELS: &str = r#"
SELECT
    COALESCE(NULLIF(model, ''), 'unknown')::text AS model,
    COALESCE(NULLIF(provider, ''), 'unknown')::text AS provider,
    count(*)::bigint AS request_count,
    count(*) FILTER (WHERE error IS NOT NULL AND error <> '')::bigint AS error_count,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY prompt_eval_ms) FILTER (WHERE prompt_eval_ms > 0)::bigint AS ttft_p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY prompt_eval_ms) FILTER (WHERE prompt_eval_ms > 0)::bigint AS ttft_p95,
    COALESCE(avg(generation_tps) FILTER (WHERE generation_tps > 0), 0)::float8 AS gen_tps,
    COALESCE(sum(input_tokens), 0)::bigint AS tokens_in,
    COALESCE(sum(output_tokens), 0)::bigint AS tokens_out,
    (COALESCE(sum(cached_tokens), 0)::float8 / NULLIF(sum(input_tokens), 0) * 100.0)::float8 AS cache_hit_pct,
    COALESCE(avg(iteration), 0)::float8 AS avg_iterations
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 40"#;

const SQL_LLM_PROVIDERS: &str = r#"
SELECT
    COALESCE(NULLIF(provider, ''), 'unknown')::text AS provider,
    count(*)::bigint AS request_count,
    count(*) FILTER (WHERE error IS NOT NULL AND error <> '')::bigint AS error_count,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::bigint AS p95_e2e_ms,
    COALESCE(avg(generation_tps) FILTER (WHERE generation_tps > 0), 0)::float8 AS gen_tps
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1
ORDER BY 2 DESC"#;

const SQL_LLM_FLOWS: &str = r#"
SELECT
    COALESCE(NULLIF(flow, ''), 'other')::text AS flow,
    count(*)::bigint AS request_count,
    count(*) FILTER (WHERE error IS NOT NULL AND error <> '')::bigint AS error_count
FROM llm_request_events
WHERE NOT is_rollup AND created_at >= $1
GROUP BY 1
ORDER BY 2 DESC"#;

const SQL_SHIELD_HEALTH: &str = r#"
SELECT
    count(*)::bigint AS checks,
    (count(*) FILTER (WHERE flagged)::float8 / NULLIF(count(*), 0) * 100.0)::float8 AS flag_pct,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::bigint AS p95_ms
FROM whitecircle_checks
WHERE created_at >= $1"#;

const SQL_SHIELD_SERIES: &str = r#"
SELECT
    to_char(date_trunc($2, created_at), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ts,
    count(*)::bigint AS checks,
    count(*) FILTER (WHERE flagged)::bigint AS flagged,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::bigint AS p95_ms
FROM whitecircle_checks
WHERE created_at >= $1
GROUP BY 1
ORDER BY 1"#;

const SQL_JOBS: &str = r#"
SELECT
    COALESCE(NULLIF(job_type, ''), 'unknown')::text AS job_type,
    COALESCE(NULLIF(queue_name, ''), '')::text AS queue_name,
    count(*)::bigint AS job_count,
    count(*) FILTER (WHERE status = 'completed')::bigint AS completed,
    count(*) FILTER (WHERE status = 'failed')::bigint AS failed,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (started_at - created_at)) * 1000
    ) FILTER (WHERE started_at IS NOT NULL)::bigint AS p95_wait_ms,
    percentile_cont(0.95) WITHIN GROUP (
        ORDER BY EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
    ) FILTER (WHERE completed_at IS NOT NULL AND started_at IS NOT NULL)::bigint AS p95_processing_ms
FROM taskman_jobs
WHERE COALESCE(completed_at, updated_at, created_at) >= $1
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20"#;

const SQL_ROUTING_OPEN: &str = r#"
SELECT count(*)::bigint AS n
FROM llm_routing_events
WHERE created_at >= $1 AND lower(severity) IN ('warn', 'warning', 'error', 'critical')"#;

const SQL_ROUTING_RECENT: &str = r#"
SELECT
    to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS at,
    COALESCE(NULLIF(severity, ''), 'info')::text AS severity,
    COALESCE(NULLIF(event_type, ''), '')::text AS event_type,
    COALESCE(provider_id::text, '')::text AS provider_id,
    COALESCE(model_id::text, '')::text AS model_id,
    COALESCE(NULLIF(summary, ''), '')::text AS summary
FROM llm_routing_events
WHERE created_at >= $1
ORDER BY created_at DESC
LIMIT 50"#;

const SQL_ROUTING_BY_TYPE: &str = r#"
SELECT
    COALESCE(NULLIF(event_type, ''), 'unknown')::text AS event_type,
    count(*)::bigint AS n
FROM llm_routing_events
WHERE created_at >= $1
GROUP BY 1
ORDER BY 2 DESC
LIMIT 12"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_parses_units_and_caps_at_retention() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::days(100);
        assert_eq!(parse_range("6h", now).duration, Duration::hours(6));
        assert_eq!(parse_range("3d", now).duration, Duration::days(3));
        assert_eq!(parse_range("90m", now).duration, Duration::minutes(90));
        // Empty / junk / over-cap all resolve safely.
        assert_eq!(parse_range("", now).duration, DEFAULT_RANGE);
        assert_eq!(parse_range("garbage", now).duration, DEFAULT_RANGE);
        assert_eq!(parse_range("30d", now).duration, MAX_RANGE);
    }

    #[test]
    fn bucket_widens_with_range() {
        let now = OffsetDateTime::UNIX_EPOCH + Duration::days(100);
        assert_eq!(parse_range("24h", now).bucket, "hour");
        assert_eq!(parse_range("7d", now).bucket, "day");
    }

    #[test]
    fn series_bucket_is_a_bound_param_not_interpolated() {
        // The bucket reaches Postgres as $2, never spliced into the SQL text.
        for sql in [SQL_LLM_SERIES, SQL_LLM_TOKEN_SERIES, SQL_SHIELD_SERIES] {
            assert!(sql.contains("date_trunc($2, created_at)"));
            assert!(!sql.contains("{bucket}"));
        }
        assert_eq!(parse_range("1h", OffsetDateTime::UNIX_EPOCH).bucket, "hour");
    }

    #[test]
    fn go_duration_normalizes_to_largest_clean_unit() {
        assert_eq!(go_duration_string(Duration::hours(24)), "1d");
        assert_eq!(go_duration_string(Duration::hours(6)), "6h");
        assert_eq!(go_duration_string(Duration::days(7)), "7d");
        assert_eq!(go_duration_string(Duration::minutes(90)), "90m");
    }

    #[test]
    fn jobs_query_filter_matches_the_migration_151_index() {
        // The dashboard reads taskman_jobs directly (the taskman rollup was
        // retired). Its window filter must use the exact expression migration 151
        // indexes, or the planner falls back to a full seq scan.
        const MIGRATION: &str =
            include_str!("../../../migrations/151_retire_taskman_job_rollup.up.sql");
        let indexed_expr = "COALESCE(completed_at, updated_at, created_at)";
        assert!(
            SQL_JOBS.contains(indexed_expr),
            "dashboard job filter must use the indexed terminal-time expression"
        );
        assert!(
            MIGRATION.contains(indexed_expr),
            "migration 151 must index the dashboard's terminal-time expression"
        );
        assert!(MIGRATION.contains("idx_taskman_jobs_terminal_time"));
        assert!(
            MIGRATION.contains("DELETE FROM telemetry_rollups WHERE source = 'taskman'"),
            "migration 151 must drop the retired taskman job rollup rows"
        );
    }
}
