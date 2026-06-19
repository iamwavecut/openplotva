//! Runtime API LLM analytics reader backed by Postgres.

use std::{collections::BTreeMap, sync::Arc, time::Duration as StdDuration};

use openplotva_server::{
    RuntimeAifarmCapacitySnapshotData, RuntimeJobAnalyticsStatData, RuntimeLlmAnalyticsData,
    RuntimeLlmAnalyticsInferenceParamStatData, RuntimeLlmAnalyticsModelSeriesPointData,
    RuntimeLlmAnalyticsModelStatData, RuntimeLlmAnalyticsProviderStatData,
    RuntimeLlmAnalyticsReader, RuntimeLlmAnalyticsReaderFuture, RuntimeLlmAnalyticsSeriesPointData,
    RuntimeLlmAnalyticsStageMetricData, RuntimeLlmAnalyticsTopChatData,
    RuntimeLlmAnalyticsTotalsData, RuntimeTaskmanInspector, RuntimeTaskmanJobData,
    RuntimeTaskmanJobsFilter,
};
use openplotva_taskman::{IMAGE_REGULAR_QUEUE_NAME, IMAGE_VIP_QUEUE_NAME, MUSIC_VIP_QUEUE_NAME};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Row};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

const MAX_ANALYTICS_RANGE: Duration = Duration::days(30);
const DEFAULT_ANALYTICS_RANGE: Duration = Duration::hours(24);
const DISCOVERY_CAPACITY_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const DEFAULT_SQL_TIMEOUT: StdDuration = StdDuration::from_secs(10);

const SQL_LLM_TOTALS: &str = r#"
SELECT
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS total_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')"#;

const SQL_LLM_SERIES_MINUTE: &str = r#"
SELECT
    date_trunc('minute', created_at)::timestamptz AS bucket,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS total_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')
GROUP BY bucket
ORDER BY bucket"#;

const SQL_LLM_SERIES_HOUR: &str = r#"
SELECT
    date_trunc('hour', created_at)::timestamptz AS bucket,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS total_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')
GROUP BY bucket
ORDER BY bucket"#;

const SQL_LLM_TOP_CHATS: &str = r#"
SELECT
    e.chat_id,
    c.title,
    c.username,
    COALESCE(SUM(CASE WHEN e.is_rollup THEN e.request_count ELSE 1 END), 0)::int AS request_count
FROM llm_request_events e
LEFT JOIN chats c ON c.id = e.chat_id
WHERE e.created_at >= $1
  AND (NOT e.is_rollup OR e.rollup_granularity = 'hour')
GROUP BY e.chat_id, c.title, c.username
ORDER BY request_count DESC
LIMIT 20"#;

const SQL_LLM_MODELS: &str = r#"
SELECT
    COALESCE(NULLIF(model, ''), 'unknown')::text AS model,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS request_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms,
    COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p50_duration_ms ELSE duration_ms END), 0)::int AS p50_duration_ms,
    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p95_duration_ms ELSE duration_ms END), 0)::int AS p95_duration_ms,
    COALESCE(SUM(CASE WHEN is_rollup THEN input_tokens_sum ELSE COALESCE(input_tokens, 0)::bigint END), 0)::bigint AS input_tokens,
    COALESCE(SUM(CASE WHEN is_rollup THEN output_tokens_sum ELSE COALESCE(output_tokens, 0)::bigint END), 0)::bigint AS output_tokens,
    COALESCE(SUM(CASE WHEN is_rollup THEN total_tokens_sum ELSE COALESCE(total_tokens, 0)::bigint END), 0)::bigint AS total_tokens,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN generation_tps_sum ELSE COALESCE(generation_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN generation_tps_count ELSE CASE WHEN generation_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_generation_tps,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN effective_output_tps_sum ELSE COALESCE(effective_output_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN effective_output_tps_count ELSE CASE WHEN effective_output_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_effective_output_tps,
    COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p50_effective_output_tps ELSE effective_output_tps END) FILTER (WHERE (is_rollup AND effective_output_tps_count > 0) OR effective_output_tps IS NOT NULL), 0)::double precision AS p50_effective_output_tps,
    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p95_effective_output_tps ELSE effective_output_tps END) FILTER (WHERE (is_rollup AND effective_output_tps_count > 0) OR effective_output_tps IS NOT NULL), 0)::double precision AS p95_effective_output_tps
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')
GROUP BY COALESCE(NULLIF(model, ''), 'unknown')::text
ORDER BY request_count DESC"#;

const SQL_LLM_MODEL_SERIES_HOUR: &str = r#"
SELECT
    date_trunc('hour', created_at)::timestamptz AS bucket,
    COALESCE(NULLIF(model, ''), 'unknown')::text AS model,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS request_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN generation_tps_sum ELSE COALESCE(generation_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN generation_tps_count ELSE CASE WHEN generation_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_generation_tps,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN effective_output_tps_sum ELSE COALESCE(effective_output_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN effective_output_tps_count ELSE CASE WHEN effective_output_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_effective_output_tps,
    COALESCE(SUM(CASE WHEN is_rollup THEN output_tokens_sum ELSE COALESCE(output_tokens, 0)::bigint END), 0)::bigint AS output_tokens
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')
GROUP BY bucket, COALESCE(NULLIF(model, ''), 'unknown')::text
ORDER BY bucket, request_count DESC"#;

const SQL_LLM_PROVIDERS: &str = r#"
WITH normalized AS (
    SELECT
        CASE
            WHEN COALESCE(NULLIF(provider, ''), '') <> ''
                THEN COALESCE(NULLIF(provider, ''), 'unknown')
            WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'genkit'
                OR lower(COALESCE(NULLIF(source, ''), 'unknown')) LIKE 'chat\_flow\_%' ESCAPE '\'
                THEN 'Gemini/GenKit'
            WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'aifarm'
                THEN 'AI Farm'
            WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) LIKE 'aifarm+fallback:%'
                THEN 'AI Farm + fallback: ' || COALESCE(NULLIF(substr(COALESCE(NULLIF(source, ''), 'unknown'), length('aifarm+fallback:') + 1), ''), 'unknown')
            WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'nvidia'
                THEN 'NVIDIA'
            WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'vmlx'
                THEN 'VMLX'
            ELSE COALESCE(NULLIF(source, ''), 'unknown')
        END::text AS provider,
        COALESCE(NULLIF(source, ''), 'unknown')::text AS source,
        duration_ms,
        error,
        input_tokens,
        output_tokens,
        total_tokens,
        generation_tps,
        effective_output_tps,
        is_rollup,
        request_count,
        error_count,
        duration_ms_sum,
        p50_duration_ms,
        p95_duration_ms,
        input_tokens_sum,
        output_tokens_sum,
        total_tokens_sum,
        generation_tps_sum,
        generation_tps_count,
        effective_output_tps_sum,
        effective_output_tps_count
    FROM llm_request_events
    WHERE created_at >= $1
      AND (NOT is_rollup OR rollup_granularity = 'hour')
)
SELECT
    provider,
    string_agg(DISTINCT source, ', ' ORDER BY source)::text AS source,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS request_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms,
    COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p50_duration_ms ELSE duration_ms END), 0)::int AS p50_duration_ms,
    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p95_duration_ms ELSE duration_ms END), 0)::int AS p95_duration_ms,
    COALESCE(SUM(CASE WHEN is_rollup THEN input_tokens_sum ELSE COALESCE(input_tokens, 0)::bigint END), 0)::bigint AS input_tokens,
    COALESCE(SUM(CASE WHEN is_rollup THEN output_tokens_sum ELSE COALESCE(output_tokens, 0)::bigint END), 0)::bigint AS output_tokens,
    COALESCE(SUM(CASE WHEN is_rollup THEN total_tokens_sum ELSE COALESCE(total_tokens, 0)::bigint END), 0)::bigint AS total_tokens,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN generation_tps_sum ELSE COALESCE(generation_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN generation_tps_count ELSE CASE WHEN generation_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_generation_tps,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN effective_output_tps_sum ELSE COALESCE(effective_output_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN effective_output_tps_count ELSE CASE WHEN effective_output_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_effective_output_tps
FROM normalized
GROUP BY provider
ORDER BY request_count DESC"#;

const SQL_LLM_SOURCES: &str = r#"
SELECT
    COALESCE(NULLIF(source, ''), 'unknown')::text AS source,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS request_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms,
    COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p50_duration_ms ELSE duration_ms END), 0)::int AS p50_duration_ms,
    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN is_rollup THEN p95_duration_ms ELSE duration_ms END), 0)::int AS p95_duration_ms,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN iteration_sum ELSE iteration::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_iteration,
    COALESCE(MAX(CASE WHEN is_rollup THEN iteration_max ELSE iteration END), 0)::int AS max_iteration
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')
GROUP BY COALESCE(NULLIF(source, ''), 'unknown')::text
ORDER BY request_count DESC"#;

const SQL_LLM_INFERENCE_PARAMS: &str = r#"
SELECT
    CASE
        WHEN COALESCE(NULLIF(provider, ''), '') <> ''
            THEN COALESCE(NULLIF(provider, ''), 'unknown')
        WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'genkit'
            OR lower(COALESCE(NULLIF(source, ''), 'unknown')) LIKE 'chat\_flow\_%' ESCAPE '\'
            THEN 'Gemini/GenKit'
        WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'aifarm'
            THEN 'AI Farm'
        WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) LIKE 'aifarm+fallback:%'
            THEN 'AI Farm + fallback: ' || COALESCE(NULLIF(substr(COALESCE(NULLIF(source, ''), 'unknown'), length('aifarm+fallback:') + 1), ''), 'unknown')
        WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'nvidia'
            THEN 'NVIDIA'
        WHEN lower(COALESCE(NULLIF(source, ''), 'unknown')) = 'vmlx'
            THEN 'VMLX'
        ELSE COALESCE(NULLIF(source, ''), 'unknown')
    END::text AS provider,
    COALESCE(NULLIF(source, ''), 'unknown')::text AS source,
    COALESCE(NULLIF(model, ''), 'unknown')::text AS model,
    max_tokens,
    temperature,
    top_p,
    top_k,
    candidate_count,
    COALESCE(NULLIF(tool_mode, ''), 'default')::text AS tool_mode,
    COALESCE(NULLIF(response_format, ''), 'text')::text AS response_format,
    COALESCE(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0)::int AS request_count,
    COALESCE(SUM(CASE WHEN is_rollup THEN error_count ELSE CASE WHEN error IS NOT NULL AND error <> '' THEN 1 ELSE 0 END END), 0)::int AS error_count,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN duration_ms_sum ELSE duration_ms::bigint END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN request_count ELSE 1 END), 0),
        0
    )::int AS avg_duration_ms,
    COALESCE(
        SUM(CASE WHEN is_rollup THEN effective_output_tps_sum ELSE COALESCE(effective_output_tps, 0) END)
        / NULLIF(SUM(CASE WHEN is_rollup THEN effective_output_tps_count ELSE CASE WHEN effective_output_tps IS NULL THEN 0 ELSE 1 END END), 0),
        0
    )::double precision AS avg_effective_output_tps
FROM llm_request_events
WHERE created_at >= $1
  AND (NOT is_rollup OR rollup_granularity = 'hour')
GROUP BY
    provider,
    source,
    model,
    max_tokens,
    temperature,
    top_p,
    top_k,
    candidate_count,
    tool_mode,
    response_format
ORDER BY request_count DESC
LIMIT 50"#;

#[derive(Clone, Debug)]
struct AnalyticsRange {
    duration: Duration,
    bucket: String,
    since_time: OffsetDateTime,
}

/// SQLx-backed runtime API LLM analytics reader.
#[derive(Clone)]
pub struct PostgresRuntimeLlmAnalyticsReader {
    pool: PgPool,
    capacity: Option<RuntimeAifarmCapacitySnapshotData>,
    capacity_client: Option<DiscoveryCapacityClient>,
    taskman: Option<Arc<dyn RuntimeTaskmanInspector>>,
    sql_timeout: StdDuration,
}

impl PostgresRuntimeLlmAnalyticsReader {
    /// Build a reader over an existing Postgres pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            capacity: None,
            capacity_client: None,
            taskman: None,
            sql_timeout: DEFAULT_SQL_TIMEOUT,
        }
    }

    pub fn with_discovery_capacity(mut self, base_url: String, service_name: String) -> Self {
        if !base_url.trim().is_empty() && !service_name.trim().is_empty() {
            self.capacity_client = Some(DiscoveryCapacityClient::new(base_url, service_name));
        }
        self
    }

    /// Attach a live Rust taskman diagnostics source for runtime job analytics.
    pub fn with_taskman(mut self, taskman: Arc<dyn RuntimeTaskmanInspector>) -> Self {
        self.taskman = Some(taskman);
        self
    }

    pub fn with_sql_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.sql_timeout = positive_millis(timeout_ms, DEFAULT_SQL_TIMEOUT);
        self
    }
}

impl RuntimeLlmAnalyticsReader for PostgresRuntimeLlmAnalyticsReader {
    fn llm_analytics<'a>(&'a self, range: &'a str) -> RuntimeLlmAnalyticsReaderFuture<'a> {
        Box::pin(async move {
            match tokio::time::timeout(self.sql_timeout, self.build_summary(range)).await {
                Ok(Ok(summary)) => Ok(summary),
                Ok(Err(error)) => Err(error_text(error)),
                Err(_) => Err(format!(
                    "runtime LLM analytics timed out after {}ms",
                    self.sql_timeout.as_millis()
                )),
            }
        })
    }
}

impl PostgresRuntimeLlmAnalyticsReader {
    async fn build_summary(&self, range: &str) -> Result<RuntimeLlmAnalyticsData, sqlx::Error> {
        let spec = parse_analytics_range_at(range, OffsetDateTime::now_utc());
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT set_config('statement_timeout', $1, true)")
            .bind(statement_timeout_value(self.sql_timeout))
            .execute(&mut *tx)
            .await?;
        let totals = self.load_totals(&mut *tx, spec.since_time).await?;
        let series = self.load_series(&mut *tx, &spec).await?;
        let model_series = self.load_model_series(&mut *tx, spec.since_time).await?;
        let top_chats = self.load_top_chats(&mut *tx, spec.since_time).await?;
        let models = self.load_models(&mut *tx, spec.since_time).await?;
        let providers = self.load_providers(&mut *tx, spec.since_time).await?;
        let inference_params = self
            .load_inference_params(&mut *tx, spec.since_time)
            .await?;
        let stage_metrics = self.load_stage_metrics(&mut *tx, spec.since_time).await?;
        tx.commit().await?;
        let (runtime_jobs, runtime_jobs_error) = self.load_runtime_jobs(&spec);
        let ai_farm_capacity = self.load_capacity().await;

        Ok(RuntimeLlmAnalyticsData {
            range: go_duration_string(spec.duration),
            bucket: spec.bucket,
            since: format_rfc3339(spec.since_time),
            totals,
            series,
            model_series,
            top_chats,
            models,
            providers,
            inference_params,
            stage_metrics,
            runtime_jobs,
            runtime_jobs_error,
            ai_farm_capacity,
        })
    }

    async fn load_totals(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<RuntimeLlmAnalyticsTotalsData, sqlx::Error> {
        let row = sqlx::query(SQL_LLM_TOTALS)
            .bind(since)
            .fetch_one(&mut *conn)
            .await?;
        Ok(RuntimeLlmAnalyticsTotalsData {
            total_count: row.try_get("total_count")?,
            error_count: row.try_get("error_count")?,
            avg_duration_ms: row.try_get("avg_duration_ms")?,
        })
    }

    async fn load_series(
        &self,
        conn: &mut PgConnection,
        spec: &AnalyticsRange,
    ) -> Result<Vec<RuntimeLlmAnalyticsSeriesPointData>, sqlx::Error> {
        let sql = if spec.bucket == "hour" {
            SQL_LLM_SERIES_HOUR
        } else {
            SQL_LLM_SERIES_MINUTE
        };
        let rows = sqlx::query(sql)
            .bind(spec.since_time)
            .fetch_all(&mut *conn)
            .await?;
        rows.into_iter().map(series_point_from_row).collect()
    }

    async fn load_model_series(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<Vec<RuntimeLlmAnalyticsModelSeriesPointData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_MODEL_SERIES_HOUR)
            .bind(since)
            .fetch_all(&mut *conn)
            .await?;
        rows.into_iter().map(model_series_point_from_row).collect()
    }

    async fn load_top_chats(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<Vec<RuntimeLlmAnalyticsTopChatData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_TOP_CHATS)
            .bind(since)
            .fetch_all(&mut *conn)
            .await?;
        let mut chats = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(chat) = top_chat_from_row(row)? {
                chats.push(chat);
            }
        }
        Ok(chats)
    }

    async fn load_models(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<Vec<RuntimeLlmAnalyticsModelStatData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_MODELS)
            .bind(since)
            .fetch_all(&mut *conn)
            .await?;
        rows.into_iter().map(model_stat_from_row).collect()
    }

    async fn load_providers(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<Vec<RuntimeLlmAnalyticsProviderStatData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_PROVIDERS)
            .bind(since)
            .fetch_all(&mut *conn)
            .await?;
        rows.into_iter().map(provider_stat_from_row).collect()
    }

    async fn load_inference_params(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<Vec<RuntimeLlmAnalyticsInferenceParamStatData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_INFERENCE_PARAMS)
            .bind(since)
            .fetch_all(&mut *conn)
            .await?;
        rows.into_iter()
            .map(inference_param_stat_from_row)
            .collect()
    }

    async fn load_stage_metrics(
        &self,
        conn: &mut PgConnection,
        since: OffsetDateTime,
    ) -> Result<Vec<RuntimeLlmAnalyticsStageMetricData>, sqlx::Error> {
        let rows = sqlx::query(SQL_LLM_SOURCES)
            .bind(since)
            .fetch_all(&mut *conn)
            .await?;
        let mut metrics = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(metric) = stage_metric_from_row(row)? {
                metrics.push(metric);
            }
        }
        Ok(metrics)
    }

    fn load_runtime_jobs(
        &self,
        spec: &AnalyticsRange,
    ) -> (Vec<RuntimeJobAnalyticsStatData>, Option<String>) {
        let Some(taskman) = self.taskman.as_deref() else {
            return (Vec::new(), None);
        };
        match load_runtime_job_details(taskman) {
            Ok(jobs) => (
                aggregate_runtime_job_analytics(&jobs, spec.since_time),
                None,
            ),
            Err(error) => (Vec::new(), Some(error)),
        }
    }

    async fn load_capacity(&self) -> Option<RuntimeAifarmCapacitySnapshotData> {
        if let Some(client) = &self.capacity_client {
            return Some(client.fetch().await);
        }
        self.capacity.clone()
    }
}

#[derive(Clone)]
struct DiscoveryCapacityClient {
    base_url: String,
    service_name: String,
    http: reqwest::Client,
}

impl DiscoveryCapacityClient {
    fn new(base_url: String, service_name: String) -> Self {
        Self {
            base_url,
            service_name,
            http: reqwest::Client::new(),
        }
    }

    async fn fetch(&self) -> RuntimeAifarmCapacitySnapshotData {
        let mut snapshot = RuntimeAifarmCapacitySnapshotData {
            service: self.service_name.trim().to_owned(),
            observed_at: format_rfc3339(OffsetDateTime::now_utc()),
            ..RuntimeAifarmCapacitySnapshotData::default()
        };

        let endpoint = match discovery_capacity_endpoint(&self.base_url, &self.service_name) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                snapshot.error = Some(error);
                return snapshot;
            }
        };
        let response = match self
            .http
            .get(endpoint)
            .timeout(DISCOVERY_CAPACITY_TIMEOUT)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                snapshot.error = Some(error.to_string());
                return snapshot;
            }
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = match response.bytes().await {
                Ok(bytes) => {
                    let end = bytes.len().min(512);
                    String::from_utf8_lossy(&bytes[..end]).trim().to_owned()
                }
                Err(_) => String::new(),
            };
            snapshot.error = Some(format!("Discovery capacity returned HTTP {status}: {body}"));
            return snapshot;
        }
        let payload = match response.json::<DiscoveryCapacityPayload>().await {
            Ok(payload) => payload,
            Err(error) => {
                snapshot.error = Some(error.to_string());
                return snapshot;
            }
        };
        apply_discovery_capacity_payload(&mut snapshot, &payload);
        snapshot
    }
}

#[derive(Debug, Default, Deserialize)]
struct DiscoveryCapacityPayload {
    #[serde(default)]
    service: String,
    #[serde(default)]
    service_name: String,
    #[serde(default)]
    max_concurrent_jobs: i32,
    #[serde(default)]
    running: i32,
    #[serde(default)]
    running_jobs: i32,
    #[serde(default)]
    queued: i32,
    #[serde(default)]
    queued_jobs: i32,
    #[serde(default)]
    available: i32,
    #[serde(default)]
    available_jobs: i32,
    #[serde(default)]
    available_slots: i32,
    #[serde(default)]
    locked: bool,
    #[serde(default)]
    ready: bool,
    #[serde(default)]
    capacity: Option<Box<DiscoveryCapacityPayload>>,
}

fn discovery_capacity_endpoint(base_url: &str, service_name: &str) -> Result<String, String> {
    let service_name = service_name.trim();
    if service_name.is_empty() {
        return Err("discovery service name is empty".to_owned());
    }
    let mut parsed =
        Url::parse(base_url.trim()).map_err(|_| "invalid Discovery base URL".to_owned())?;
    if parsed.host_str().is_none() {
        return Err("invalid Discovery base URL".to_owned());
    }
    parsed.set_query(None);
    parsed
        .path_segments_mut()
        .map_err(|_| "invalid Discovery base URL".to_owned())?
        .pop_if_empty()
        .extend(&["v1", "services", service_name, "capacity"]);
    Ok(parsed.to_string())
}

fn apply_discovery_capacity_payload(
    snapshot: &mut RuntimeAifarmCapacitySnapshotData,
    payload: &DiscoveryCapacityPayload,
) {
    let payload = payload.capacity.as_deref().unwrap_or(payload);
    if !payload.service_name.is_empty() {
        snapshot.service.clone_from(&payload.service_name);
    } else if !payload.service.is_empty() {
        snapshot.service.clone_from(&payload.service);
    }
    snapshot.max_concurrent_jobs = payload.max_concurrent_jobs;
    snapshot.running = first_nonzero_i32([payload.running, payload.running_jobs]);
    snapshot.queued = first_nonzero_i32([payload.queued, payload.queued_jobs]);
    snapshot.available = first_nonzero_i32([
        payload.available,
        payload.available_jobs,
        payload.available_slots,
    ]);
    snapshot.locked = payload.locked;
    snapshot.ready = payload.ready;
}

fn first_nonzero_i32<const N: usize>(values: [i32; N]) -> i32 {
    values.into_iter().find(|value| *value != 0).unwrap_or(0)
}

fn load_runtime_job_details(
    taskman: &dyn RuntimeTaskmanInspector,
) -> Result<Vec<RuntimeTaskmanJobDetails>, String> {
    let result = taskman.list_jobs(RuntimeTaskmanJobsFilter {
        queue: vec![
            IMAGE_REGULAR_QUEUE_NAME.to_owned(),
            IMAGE_VIP_QUEUE_NAME.to_owned(),
            MUSIC_VIP_QUEUE_NAME.to_owned(),
        ],
        sort_by: "id".to_owned(),
        sort_dir: "asc".to_owned(),
        limit: 1000,
        ..RuntimeTaskmanJobsFilter::default()
    })?;
    let mut jobs = Vec::with_capacity(result.items.len());
    for item in result.items {
        let job_type = item.job_type;
        if let Some(details) = taskman.job(item.id)? {
            jobs.push(RuntimeTaskmanJobDetails {
                job_type,
                job: details.job,
                events: details.events,
            });
        }
    }
    Ok(jobs)
}

#[derive(Clone, Debug)]
struct RuntimeTaskmanJobDetails {
    job_type: String,
    job: RuntimeTaskmanJobData,
    events: Option<Value>,
}

#[derive(Default)]
struct RuntimeJobAggregate {
    stat: RuntimeJobAnalyticsStatData,
    wait_ms: Vec<i32>,
    processing_ms: Vec<i32>,
}

fn aggregate_runtime_job_analytics(
    jobs: &[RuntimeTaskmanJobDetails],
    since: OffsetDateTime,
) -> Vec<RuntimeJobAnalyticsStatData> {
    let mut aggregates = BTreeMap::<(String, String, String), RuntimeJobAggregate>::new();

    for details in jobs {
        let job = &details.job;
        let created_at = parse_rfc3339(&job.created_at);
        let completed_at = job.completed_at.as_deref().and_then(parse_rfc3339);
        let created_in_range = created_at.is_some_and(|created_at| created_at >= since);
        let completed_in_range = completed_at.is_some_and(|completed_at| completed_at >= since);
        if !is_runtime_analytics_job(&details.job_type)
            || (!created_in_range && !completed_in_range)
        {
            continue;
        }

        let provider = runtime_job_provider(&details.job_type, details.events.as_ref());
        let key = (
            details.job_type.clone(),
            job.queue_name.clone(),
            provider.clone(),
        );
        let aggregate = aggregates
            .entry(key)
            .or_insert_with(|| RuntimeJobAggregate {
                stat: RuntimeJobAnalyticsStatData {
                    job_type: details.job_type.clone(),
                    queue_name: job.queue_name.clone(),
                    provider,
                    ..RuntimeJobAnalyticsStatData::default()
                },
                ..RuntimeJobAggregate::default()
            });
        if created_in_range {
            aggregate.stat.created_count += 1;
        }
        if completed_in_range {
            aggregate_completed_job(aggregate, job, completed_at);
        }
    }

    aggregates
        .into_values()
        .map(RuntimeJobAggregate::finalize)
        .collect()
}

fn aggregate_completed_job(
    aggregate: &mut RuntimeJobAggregate,
    job: &RuntimeTaskmanJobData,
    completed_at: Option<OffsetDateTime>,
) {
    match job.status.as_str() {
        "completed" => aggregate.stat.completed_count += 1,
        "failed" => aggregate.stat.failed_count += 1,
        _ => {}
    }
    if let Some(wait_ms) = runtime_job_wait_ms(job) {
        aggregate.wait_ms.push(wait_ms);
    }
    if let Some(processing_ms) = runtime_job_processing_ms(job, completed_at) {
        aggregate.processing_ms.push(processing_ms);
    }
}

impl RuntimeJobAggregate {
    fn finalize(mut self) -> RuntimeJobAnalyticsStatData {
        self.stat.avg_wait_ms = average_i32(&self.wait_ms);
        self.stat.p95_wait_ms = percentile_i32(&self.wait_ms, 0.95);
        self.stat.avg_processing_ms = average_i32(&self.processing_ms);
        self.stat.p95_processing_ms = percentile_i32(&self.processing_ms, 0.95);
        self.stat
    }
}

fn is_runtime_analytics_job(job_type: &str) -> bool {
    matches!(job_type, "image_gen" | "image_edit" | "music_gen")
}

fn runtime_job_provider(job_type: &str, events: Option<&Value>) -> String {
    if job_type == "music_gen" {
        return "acestep".to_owned();
    }
    let mut provider = String::new();
    if let Some(events) = events.and_then(Value::as_array) {
        for event in events {
            let current = event
                .get("provider")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            if current.is_empty() {
                continue;
            }
            match event
                .get("stage")
                .and_then(Value::as_str)
                .unwrap_or_default()
            {
                "image_gen.provider_success" => return current.to_owned(),
                "image_gen.provider_error" | "image_gen.provider_attempt" => {
                    provider = current.to_owned();
                }
                _ => {}
            }
        }
    }
    if provider.is_empty() {
        "unknown".to_owned()
    } else {
        provider
    }
}

fn runtime_job_wait_ms(job: &RuntimeTaskmanJobData) -> Option<i32> {
    let created_at = parse_rfc3339(&job.created_at)?;
    let started_at = job.started_at.as_deref().and_then(parse_rfc3339)?;
    (started_at >= created_at)
        .then(|| clamp_i128_to_i32((started_at - created_at).whole_milliseconds()))
}

fn runtime_job_processing_ms(
    job: &RuntimeTaskmanJobData,
    completed_at: Option<OffsetDateTime>,
) -> Option<i32> {
    let started_at = job.started_at.as_deref().and_then(parse_rfc3339)?;
    let completed_at = completed_at?;
    (completed_at >= started_at)
        .then(|| clamp_i128_to_i32((completed_at - started_at).whole_milliseconds()))
}

fn series_point_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeLlmAnalyticsSeriesPointData, sqlx::Error> {
    let bucket: OffsetDateTime = row.try_get("bucket")?;
    Ok(RuntimeLlmAnalyticsSeriesPointData {
        ts: format_rfc3339(bucket),
        total_count: row.try_get("total_count")?,
        error_count: row.try_get("error_count")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
    })
}

fn model_series_point_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeLlmAnalyticsModelSeriesPointData, sqlx::Error> {
    let bucket: OffsetDateTime = row.try_get("bucket")?;
    Ok(RuntimeLlmAnalyticsModelSeriesPointData {
        ts: format_rfc3339(bucket),
        model: row.try_get("model")?,
        request_count: row.try_get("request_count")?,
        error_count: row.try_get("error_count")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
        avg_generation_tps: row.try_get("avg_generation_tps")?,
        avg_effective_output_tps: row.try_get("avg_effective_output_tps")?,
        output_tokens: row.try_get("output_tokens")?,
    })
}

fn top_chat_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<Option<RuntimeLlmAnalyticsTopChatData>, sqlx::Error> {
    let Some(chat_id) = row.try_get("chat_id")? else {
        return Ok(None);
    };
    Ok(Some(RuntimeLlmAnalyticsTopChatData {
        chat_id,
        title: row.try_get("title")?,
        username: row.try_get("username")?,
        request_count: row.try_get("request_count")?,
    }))
}

fn model_stat_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeLlmAnalyticsModelStatData, sqlx::Error> {
    Ok(RuntimeLlmAnalyticsModelStatData {
        model: row.try_get("model")?,
        request_count: row.try_get("request_count")?,
        error_count: row.try_get("error_count")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
        p50_duration_ms: row.try_get("p50_duration_ms")?,
        p95_duration_ms: row.try_get("p95_duration_ms")?,
        input_tokens: row.try_get("input_tokens")?,
        output_tokens: row.try_get("output_tokens")?,
        total_tokens: row.try_get("total_tokens")?,
        avg_generation_tps: row.try_get("avg_generation_tps")?,
        avg_effective_output_tps: row.try_get("avg_effective_output_tps")?,
        p50_effective_output_tps: row.try_get("p50_effective_output_tps")?,
        p95_effective_output_tps: row.try_get("p95_effective_output_tps")?,
    })
}

fn provider_stat_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeLlmAnalyticsProviderStatData, sqlx::Error> {
    Ok(RuntimeLlmAnalyticsProviderStatData {
        provider: row.try_get("provider")?,
        source: row.try_get("source")?,
        request_count: row.try_get("request_count")?,
        error_count: row.try_get("error_count")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
        p50_duration_ms: row.try_get("p50_duration_ms")?,
        p95_duration_ms: row.try_get("p95_duration_ms")?,
        input_tokens: row.try_get("input_tokens")?,
        output_tokens: row.try_get("output_tokens")?,
        total_tokens: row.try_get("total_tokens")?,
        avg_generation_tps: row.try_get("avg_generation_tps")?,
        avg_effective_output_tps: row.try_get("avg_effective_output_tps")?,
    })
}

fn inference_param_stat_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<RuntimeLlmAnalyticsInferenceParamStatData, sqlx::Error> {
    Ok(RuntimeLlmAnalyticsInferenceParamStatData {
        provider: row.try_get("provider")?,
        source: row.try_get("source")?,
        model: row.try_get("model")?,
        max_tokens: row.try_get("max_tokens")?,
        temperature: row.try_get("temperature")?,
        top_p: row.try_get("top_p")?,
        top_k: row.try_get("top_k")?,
        candidate_count: row.try_get("candidate_count")?,
        tool_mode: row.try_get("tool_mode")?,
        response_format: row.try_get("response_format")?,
        request_count: row.try_get("request_count")?,
        error_count: row.try_get("error_count")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
        avg_effective_output_tps: row.try_get("avg_effective_output_tps")?,
    })
}

fn stage_metric_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<Option<RuntimeLlmAnalyticsStageMetricData>, sqlx::Error> {
    let source: String = row.try_get("source")?;
    let Some(stage) = source.strip_prefix("chat_flow_") else {
        return Ok(None);
    };
    let stage = stage.trim();
    Ok(Some(RuntimeLlmAnalyticsStageMetricData {
        stage: if stage.is_empty() {
            source.clone()
        } else {
            stage.to_owned()
        },
        source,
        request_count: row.try_get("request_count")?,
        error_count: row.try_get("error_count")?,
        avg_duration_ms: row.try_get("avg_duration_ms")?,
        p50_duration_ms: row.try_get("p50_duration_ms")?,
        p95_duration_ms: row.try_get("p95_duration_ms")?,
        avg_iteration: row.try_get("avg_iteration")?,
        max_iteration: row.try_get("max_iteration")?,
    }))
}

fn parse_analytics_range_at(range: &str, now: OffsetDateTime) -> AnalyticsRange {
    let duration = analytics_range_duration(range);
    AnalyticsRange {
        duration,
        bucket: analytics_range_bucket(duration).to_owned(),
        since_time: now - duration,
    }
}

fn analytics_range_duration(range: &str) -> Duration {
    let trimmed = range.trim();
    let parsed = parse_days(trimmed).or_else(|| parse_go_duration(trimmed));
    parsed
        .filter(|duration| *duration > Duration::ZERO)
        .unwrap_or(DEFAULT_ANALYTICS_RANGE)
        .min(MAX_ANALYTICS_RANGE)
}

fn parse_days(value: &str) -> Option<Duration> {
    let days = value.strip_suffix('d')?.parse::<i64>().ok()?;
    (days > 0).then(|| Duration::days(days))
}

fn parse_go_duration(value: &str) -> Option<Duration> {
    if value.is_empty() {
        return None;
    }

    let mut rest = value;
    let mut total_ms = 0_f64;
    while !rest.is_empty() {
        let split = rest
            .char_indices()
            .find(|(_, ch)| !ch.is_ascii_digit() && *ch != '.')
            .map(|(idx, _)| idx)?;
        if split == 0 {
            return None;
        }
        let amount = rest[..split].parse::<f64>().ok()?;
        rest = &rest[split..];
        let (unit, next) = duration_unit(rest)?;
        total_ms += amount * unit;
        rest = next;
    }
    Some(Duration::milliseconds(total_ms.round() as i64))
}

fn duration_unit(value: &str) -> Option<(f64, &str)> {
    if let Some(next) = value.strip_prefix("ms") {
        return Some((1.0, next));
    }
    if let Some(next) = value.strip_prefix('h') {
        return Some((60.0 * 60.0 * 1000.0, next));
    }
    if let Some(next) = value.strip_prefix('m') {
        return Some((60.0 * 1000.0, next));
    }
    if let Some(next) = value.strip_prefix('s') {
        return Some((1000.0, next));
    }
    None
}

fn analytics_range_bucket(duration: Duration) -> &'static str {
    if duration > Duration::hours(48) {
        "hour"
    } else {
        "minute"
    }
}

fn positive_millis(value: i32, default: StdDuration) -> StdDuration {
    if value <= 0 {
        default
    } else {
        StdDuration::from_millis(value as u64)
    }
}

fn statement_timeout_value(timeout: StdDuration) -> String {
    format!("{}ms", timeout.as_millis().max(1))
}

fn go_duration_string(duration: Duration) -> String {
    let total_ms = duration.whole_milliseconds().max(0);
    let total_seconds = total_ms / 1000;
    let milliseconds = total_ms % 1000;
    if total_seconds >= 3600 {
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        return format!("{hours}h{minutes}m{seconds}s");
    }
    if total_seconds >= 60 {
        let minutes = total_seconds / 60;
        let seconds = total_seconds % 60;
        return format!("{minutes}m{seconds}s");
    }
    if total_seconds > 0 {
        return format!("{total_seconds}s");
    }
    format!("{milliseconds}ms")
}

fn parse_rfc3339(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

fn average_i32(values: &[i32]) -> i32 {
    if values.is_empty() {
        return 0;
    }
    let total: i64 = values.iter().map(|value| i64::from(*value)).sum();
    clamp_i64_to_i32(total / values.len() as i64)
}

fn percentile_i32(values: &[i32], p: f64) -> i32 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let index = ((sorted.len() as f64) * p + 0.999_999_999) as usize;
    sorted[index.saturating_sub(1).min(sorted.len() - 1)]
}

fn clamp_i128_to_i32(value: i128) -> i32 {
    if value > i128::from(i32::MAX) {
        i32::MAX
    } else if value < i128::from(i32::MIN) {
        i32::MIN
    } else {
        value as i32
    }
}

fn clamp_i64_to_i32(value: i64) -> i32 {
    if value > i64::from(i32::MAX) {
        i32::MAX
    } else if value < i64::from(i32::MIN) {
        i32::MIN
    } else {
        value as i32
    }
}

fn format_rfc3339(value: OffsetDateTime) -> String {
    value.format(&Rfc3339).unwrap_or_else(|_| value.to_string())
}

fn error_text<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use openplotva_server::RuntimeTaskmanJobData;
    use serde_json::json;
    use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

    use super::{
        DiscoveryCapacityPayload, RuntimeTaskmanJobDetails, aggregate_runtime_job_analytics,
        analytics_range_bucket, analytics_range_duration, apply_discovery_capacity_payload,
        discovery_capacity_endpoint, go_duration_string, parse_analytics_range_at, positive_millis,
        statement_timeout_value,
    };

    #[test]
    fn runtime_llm_analytics_range_matches_go_defaults_and_clamp() {
        assert_eq!(analytics_range_duration(""), Duration::hours(24));
        assert_eq!(analytics_range_duration(" 3d "), Duration::days(3));
        assert_eq!(analytics_range_duration("90m"), Duration::minutes(90));
        assert_eq!(analytics_range_duration("1h30m"), Duration::minutes(90));
        assert_eq!(analytics_range_duration("30d"), Duration::days(30));
        assert_eq!(analytics_range_duration("200d"), Duration::days(30));
        assert_eq!(analytics_range_duration("garbage"), Duration::hours(24));
        assert_eq!(analytics_range_bucket(Duration::hours(48)), "minute");
        assert_eq!(analytics_range_bucket(Duration::hours(49)), "hour");
    }

    #[test]
    fn runtime_llm_analytics_range_formats_like_go_duration() {
        assert_eq!(go_duration_string(Duration::hours(24)), "24h0m0s");
        assert_eq!(go_duration_string(Duration::days(3)), "72h0m0s");
        assert_eq!(go_duration_string(Duration::minutes(90)), "1h30m0s");
        assert_eq!(go_duration_string(Duration::seconds(45)), "45s");
        assert_eq!(go_duration_string(Duration::milliseconds(500)), "500ms");
    }

    #[test]
    fn runtime_llm_analytics_statement_timeout_uses_runtime_api_timeout_ms() {
        let timeout = positive_millis(1500, std::time::Duration::from_secs(10));

        assert_eq!(statement_timeout_value(timeout), "1500ms");
        assert_eq!(
            statement_timeout_value(positive_millis(0, std::time::Duration::from_secs(10))),
            "10000ms"
        );
    }

    #[test]
    fn runtime_llm_analytics_range_carries_since_and_bucket() {
        let now = OffsetDateTime::parse("2026-05-21T12:00:00Z", &Rfc3339)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let parsed = parse_analytics_range_at("3d", now);
        assert_eq!(parsed.bucket, "hour");
        assert_eq!(
            parsed.since_time.format(&Rfc3339).unwrap_or_default(),
            "2026-05-18T12:00:00Z"
        );
    }

    #[test]
    fn runtime_job_analytics_aggregate_matches_go_grouping_and_timings() {
        let since = OffsetDateTime::parse("2026-05-21T11:00:00Z", &Rfc3339)
            .unwrap_or(OffsetDateTime::UNIX_EPOCH);
        let jobs = vec![
            RuntimeTaskmanJobDetails {
                job_type: "image_gen".to_owned(),
                job: job(
                    1,
                    "image gen",
                    "image-regular",
                    "completed",
                    "2026-05-21T12:00:00Z",
                    Some("2026-05-21T12:00:10Z"),
                    Some("2026-05-21T12:01:00Z"),
                ),
                events: Some(json!([
                    {"stage": "image_gen.provider_attempt", "provider": "fallback"},
                    {"stage": "image_gen.provider_success", "provider": "aifarm"}
                ])),
            },
            RuntimeTaskmanJobDetails {
                job_type: "image_gen".to_owned(),
                job: job(
                    2,
                    "image gen",
                    "image-vip",
                    "failed",
                    "2026-05-21T10:00:00Z",
                    Some("2026-05-21T10:00:05Z"),
                    Some("2026-05-21T12:02:00Z"),
                ),
                events: Some(json!([
                    {"stage": "image_gen.provider_attempt", "provider": "draw-api"},
                    {"stage": "image_gen.provider_error", "provider": "draw-api"}
                ])),
            },
            RuntimeTaskmanJobDetails {
                job_type: "music_gen".to_owned(),
                job: job(
                    3,
                    "music gen",
                    "music-vip",
                    "pending",
                    "2026-05-21T12:03:00Z",
                    None,
                    None,
                ),
                events: None,
            },
            RuntimeTaskmanJobDetails {
                job_type: "dialog".to_owned(),
                job: job(
                    4,
                    "dialog",
                    "text",
                    "completed",
                    "2026-05-21T12:04:00Z",
                    Some("2026-05-21T12:04:01Z"),
                    Some("2026-05-21T12:04:03Z"),
                ),
                events: None,
            },
        ];

        let stats = aggregate_runtime_job_analytics(&jobs, since);

        assert_eq!(stats.len(), 3);
        assert_eq!(stats[0].job_type, "image_gen");
        assert_eq!(stats[0].queue_name, "image-regular");
        assert_eq!(stats[0].provider, "aifarm");
        assert_eq!(stats[0].created_count, 1);
        assert_eq!(stats[0].completed_count, 1);
        assert_eq!(stats[0].failed_count, 0);
        assert_eq!(stats[0].avg_wait_ms, 10_000);
        assert_eq!(stats[0].p95_processing_ms, 50_000);

        assert_eq!(stats[1].queue_name, "image-vip");
        assert_eq!(stats[1].provider, "draw-api");
        assert_eq!(stats[1].created_count, 0);
        assert_eq!(stats[1].failed_count, 1);
        assert_eq!(stats[1].avg_processing_ms, 7_315_000);

        assert_eq!(stats[2].job_type, "music_gen");
        assert_eq!(stats[2].provider, "acestep");
        assert_eq!(stats[2].created_count, 1);
        assert_eq!(stats[2].completed_count, 0);
    }

    #[test]
    fn discovery_capacity_endpoint_matches_go_join_and_escape() {
        assert_eq!(
            discovery_capacity_endpoint(
                "https://discovery.example.test/root/?stale=1",
                " llm/open ai "
            ),
            Ok(
                "https://discovery.example.test/root/v1/services/llm%2Fopen%20ai/capacity"
                    .to_owned()
            )
        );
        assert_eq!(
            discovery_capacity_endpoint("http://discovery.example.test", "llm-openai"),
            Ok("http://discovery.example.test/v1/services/llm-openai/capacity".to_owned())
        );
        assert_eq!(
            discovery_capacity_endpoint("discovery.example.test", "llm-openai"),
            Err("invalid Discovery base URL".to_owned())
        );
        assert_eq!(
            discovery_capacity_endpoint("https://discovery.example.test", "  "),
            Err("discovery service name is empty".to_owned())
        );
    }

    #[test]
    fn discovery_capacity_payload_matches_go_alias_and_nested_rules()
    -> Result<(), serde_json::Error> {
        let payload: DiscoveryCapacityPayload = serde_json::from_value(json!({
            "service": "outer",
            "capacity": {
                "service_name": "dialog-aifarm",
                "max_concurrent_jobs": 2,
                "running_jobs": 2,
                "queued_jobs": 3,
                "available_slots": 4,
                "locked": true,
                "ready": false
            }
        }))?;
        let mut snapshot = openplotva_server::RuntimeAifarmCapacitySnapshotData {
            service: "configured".to_owned(),
            observed_at: "2026-05-21T12:00:00Z".to_owned(),
            ..openplotva_server::RuntimeAifarmCapacitySnapshotData::default()
        };

        apply_discovery_capacity_payload(&mut snapshot, &payload);

        assert_eq!(snapshot.service, "dialog-aifarm");
        assert_eq!(snapshot.max_concurrent_jobs, 2);
        assert_eq!(snapshot.running, 2);
        assert_eq!(snapshot.queued, 3);
        assert_eq!(snapshot.available, 4);
        assert!(snapshot.locked);
        assert!(!snapshot.ready);
        assert_eq!(snapshot.error, None);
        Ok(())
    }

    fn job(
        id: i64,
        job_type: &str,
        queue_name: &str,
        status: &str,
        created_at: &str,
        started_at: Option<&str>,
        completed_at: Option<&str>,
    ) -> RuntimeTaskmanJobData {
        RuntimeTaskmanJobData {
            id,
            queue_name: queue_name.to_owned(),
            priority: 0,
            title: job_type.to_owned(),
            payload: None,
            status: status.to_owned(),
            user_id: 7,
            chat_id: -100,
            trigger_message_id: 11,
            thread_message_id: None,
            progress_message_id: None,
            queue_position_message_id: None,
            result_message_id: None,
            worker_id: None,
            created_at: created_at.to_owned(),
            started_at: started_at.map(str::to_owned),
            completed_at: completed_at.map(str::to_owned),
            error_message: None,
            processing_timeout_seconds: 0,
            prompt_hash: None,
            estimated_processing_time: None,
            actual_processing_time: None,
        }
    }
}
