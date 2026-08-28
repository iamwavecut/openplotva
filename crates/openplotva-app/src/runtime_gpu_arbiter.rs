//! Read-only snapshot for the admin "GPU1 arbiter" panel: the gpu-butler ledger
//! fetched through the Discovery gateway plus the ASR fallback trendline from
//! `telegram_files`. Every upstream failure is embedded as an `error` field so
//! the panel degrades per-section instead of blanking when the butler or the
//! database is unreachable.

use std::collections::BTreeMap;
use std::time::Duration as StdDuration;

use base64::Engine as _;
use base64::engine::general_purpose;
use openplotva_llm::aifarm::{
    DiscoveryInvocation, DiscoveryJobEnvelope, DiscoveryJobRequest, decode_discovery_body,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::Row as _;
use sqlx::postgres::PgRow;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

const GPU_BUTLER_SERVICE_NAME: &str = "gpu-butler";
const GPU_BUTLER_ENDPOINT_NAME: &str = "status";
const GPU_BUTLER_HTTP_TIMEOUT: StdDuration = StdDuration::from_secs(3);
const GPU_BUTLER_UPSTREAM_TIMEOUT_MS: i32 = 2_500;
const DEFAULT_RANGE: time::Duration = time::Duration::hours(72);
const MAX_RANGE: time::Duration = time::Duration::days(14);
const HOUR_BUCKET_MAX: time::Duration = time::Duration::days(2);

const SQL_ASR_FALLBACK_TOTALS: &str = r#"
SELECT
    count(*)::bigint AS transcripts,
    count(*) FILTER (WHERE asr_fallback_used)::bigint AS fallbacks,
    (count(*) FILTER (WHERE asr_fallback_used)::float8
        / NULLIF(count(*), 0) * 100.0)::float8 AS fallback_pct,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY asr_latency_ms)::bigint AS p95_latency_ms
FROM telegram_files
WHERE asr_completed_at >= $1 AND asr_fallback_used IS NOT NULL"#;

const SQL_ASR_FALLBACK_SERIES: &str = r#"
SELECT
    to_char(date_trunc($2, asr_completed_at), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS ts,
    count(*)::bigint AS transcripts,
    count(*) FILTER (WHERE asr_fallback_used)::bigint AS fallbacks
FROM telegram_files
WHERE asr_completed_at >= $1 AND asr_fallback_used IS NOT NULL
GROUP BY 1
ORDER BY 1"#;

pub struct GpuArbiterReader {
    discovery_base_url: String,
    pool: Option<PgPool>,
    sql_timeout: StdDuration,
    http: reqwest::Client,
}

impl GpuArbiterReader {
    #[must_use]
    pub fn new(discovery_base_url: String, pool: Option<PgPool>, sql_timeout_ms: i32) -> Self {
        let ms = u64::try_from(sql_timeout_ms)
            .unwrap_or(10_000)
            .clamp(1_000, 60_000);
        Self {
            discovery_base_url,
            pool,
            sql_timeout: StdDuration::from_millis(ms),
            http: reqwest::Client::new(),
        }
    }

    pub async fn snapshot(&self, range: &str) -> Value {
        let (window, bucket) = parse_range(range);
        let since = OffsetDateTime::now_utc() - window;
        let (arbiter, asr_fallback) =
            tokio::join!(self.arbiter_status(), self.asr_fallback(since, bucket));
        json!({
            "arbiter": arbiter,
            "asr_fallback": asr_fallback,
            "range_hours": window.whole_hours(),
            "generated_at": OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_default(),
        })
    }

    async fn arbiter_status(&self) -> Value {
        let request = DiscoveryJobRequest {
            invocation: DiscoveryInvocation {
                service_name: GPU_BUTLER_SERVICE_NAME.to_owned(),
                endpoint_name: GPU_BUTLER_ENDPOINT_NAME.to_owned(),
                headers: BTreeMap::new(),
                query: BTreeMap::new(),
                body: general_purpose::STANDARD.encode(b""),
                content_type: "application/json".to_owned(),
                timeout_ms: GPU_BUTLER_UPSTREAM_TIMEOUT_MS,
            },
            idempotency_key: format!(
                "gpu1-admin-status-{}",
                OffsetDateTime::now_utc().unix_timestamp_nanos()
            ),
            priority: 0,
            wait_for_capacity_ms: 0,
            capacity_poll_ms: 0,
        };
        let url = format!(
            "{}/v1/jobs/blocking",
            self.discovery_base_url.trim_end_matches('/')
        );
        let response = match self
            .http
            .post(url)
            .timeout(GPU_BUTLER_HTTP_TIMEOUT)
            .json(&request)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => return section_error(format!("discovery request failed: {error}")),
        };
        if !response.status().is_success() {
            return section_error(format!("discovery answered {}", response.status()));
        }
        let envelope = match response.json::<DiscoveryJobEnvelope>().await {
            Ok(envelope) => envelope,
            Err(error) => return section_error(format!("decode discovery envelope: {error}")),
        };
        let job = envelope.resolve_job();
        let state = job.resolved_status().to_ascii_uppercase();
        if !state.contains("SUCC") {
            return section_error(format!("discovery job state {state}"));
        }
        let Some(payload) = job
            .result
            .as_ref()
            .and_then(|result| result.response.clone())
        else {
            return section_error("discovery job succeeded without a response payload");
        };
        if payload.status_code >= 300 {
            return section_error(format!("gpu-butler answered {}", payload.status_code));
        }
        let bytes = match decode_discovery_body(&payload.body) {
            Ok(bytes) => bytes,
            Err(error) => return section_error(format!("decode gpu-butler body: {error}")),
        };
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(value) => value,
            Err(error) => section_error(format!("decode gpu-butler status: {error}")),
        }
    }

    async fn asr_fallback(&self, since: OffsetDateTime, bucket: &str) -> Value {
        let Some(pool) = self.pool.clone() else {
            return section_error("database is not configured");
        };
        let query = async {
            let totals = sqlx::query(SQL_ASR_FALLBACK_TOTALS)
                .bind(since)
                .fetch_one(&pool)
                .await?;
            let series = sqlx::query(SQL_ASR_FALLBACK_SERIES)
                .bind(since)
                .bind(bucket)
                .fetch_all(&pool)
                .await?;
            Ok::<Value, sqlx::Error>(json!({
                "transcripts": get_i64(&totals, "transcripts"),
                "fallbacks": get_i64(&totals, "fallbacks"),
                "fallback_pct": get_f64(&totals, "fallback_pct"),
                "p95_latency_ms": get_i64(&totals, "p95_latency_ms"),
                "bucket": bucket,
                "series": Value::Array(series.iter().map(|row| json!({
                    "ts": get_text(row, "ts"),
                    "transcripts": get_i64(row, "transcripts"),
                    "fallbacks": get_i64(row, "fallbacks"),
                })).collect()),
            }))
        };
        match tokio::time::timeout(self.sql_timeout, query).await {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => section_error(format!("asr fallback query failed: {error}")),
            Err(_) => section_error("asr fallback query timed out"),
        }
    }
}

fn section_error(message: impl Into<String>) -> Value {
    json!({ "error": message.into() })
}

fn parse_range(range: &str) -> (time::Duration, &'static str) {
    let trimmed = range.trim().to_ascii_lowercase();
    let window = parse_duration(&trimmed)
        .unwrap_or(DEFAULT_RANGE)
        .min(MAX_RANGE);
    let bucket = if window <= HOUR_BUCKET_MAX {
        "hour"
    } else {
        "day"
    };
    (window, bucket)
}

fn parse_duration(value: &str) -> Option<time::Duration> {
    // Cap before constructing: time::Duration::hours/days panic on i64 overflow,
    // and everything above the cap collapses to MAX_RANGE anyway.
    const MAX_AMOUNT: i64 = 100_000;
    let (number, unit) = value.split_at(value.len().checked_sub(1)?);
    let amount = number
        .parse::<i64>()
        .ok()
        .filter(|amount| (1..=MAX_AMOUNT).contains(amount))?;
    match unit {
        "h" => Some(time::Duration::hours(amount)),
        "d" => Some(time::Duration::days(amount)),
        _ => None,
    }
}

fn get_i64(row: &PgRow, column: &str) -> i64 {
    row.try_get::<Option<i64>, _>(column)
        .ok()
        .flatten()
        .unwrap_or(0)
}

fn get_f64(row: &PgRow, column: &str) -> f64 {
    let value = row
        .try_get::<Option<f64>, _>(column)
        .ok()
        .flatten()
        .unwrap_or(0.0);
    (value * 100.0).round() / 100.0
}

fn get_text(row: &PgRow, column: &str) -> String {
    row.try_get::<Option<String>, _>(column)
        .ok()
        .flatten()
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_defaults_to_72_hours_with_day_buckets() {
        let (window, bucket) = parse_range("");
        assert_eq!(window, time::Duration::hours(72));
        assert_eq!(bucket, "day");
    }

    #[test]
    fn short_ranges_bucket_by_hour_and_long_ranges_are_capped() {
        assert_eq!(parse_range("24h"), (time::Duration::hours(24), "hour"));
        assert_eq!(parse_range("48h"), (time::Duration::hours(48), "hour"));
        assert_eq!(parse_range("7d"), (time::Duration::days(7), "day"));
        assert_eq!(parse_range("90d"), (time::Duration::days(14), "day"));
        assert_eq!(parse_range("nonsense"), (time::Duration::hours(72), "day"));
        assert_eq!(
            parse_range("999999999999999999d"),
            (time::Duration::hours(72), "day")
        );
    }

    #[test]
    fn section_errors_carry_only_an_error_field() {
        let value = section_error("gpu-butler answered 502");
        assert_eq!(value["error"], "gpu-butler answered 502");
        assert_eq!(value.as_object().map(serde_json::Map::len), Some(1));
    }
}
