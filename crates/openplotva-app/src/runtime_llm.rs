//! Runtime API LLM trace buffer and dialog provider recorder.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use openplotva_dialog::DialogTraceArtifacts;
use openplotva_server::{
    RuntimeLlmRequestChatData, RuntimeLlmRequestData, RuntimeLlmRequestMessageData,
    RuntimeLlmRequestResultData, RuntimeLlmRequestUserData, RuntimeLlmRequestsFilter,
    RuntimeLlmTraceInspector,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, QueryBuilder};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};

const GO_LLM_TRACE_BUFFER_CAPACITY: usize = 1_000;
const GO_RESPONSE_PREVIEW_SIZE: usize = 500;
const LLM_EVENT_WRITER_CHANNEL_CAPACITY: usize = 10_000;
const LLM_EVENT_WRITER_BATCH_SIZE: usize = 100;
const LLM_EVENT_WRITER_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
pub const LLM_REQUEST_EVENTS_CLEANUP_BATCH_SIZE: i64 = 10_000;
pub const LLM_REQUEST_EVENTS_CLEANUP_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const SQL_INSERT_LLM_REQUEST_EVENTS_PREFIX: &str = r#"INSERT INTO llm_request_events (
    created_at,
    provider,
    request_kind,
    source,
    flow,
    chat_id,
    thread_id,
    message_id,
    user_id,
    model,
    iteration,
    prompt_chars,
    prompt_messages,
    docs_chars,
    duration_ms,
    input_tokens,
    output_tokens,
    total_tokens,
    cached_tokens,
    thoughts_tokens,
    tool_use_prompt_tokens,
    prompt_eval_tokens,
    prompt_eval_ms,
    prompt_tps,
    generation_tokens,
    generation_ms,
    generation_tps,
    effective_output_tps,
    effective_total_tps,
    max_tokens,
    temperature,
    top_p,
    top_k,
    candidate_count,
    tool_mode,
    response_format,
    inference_params,
    error,
    run_id,
    run_seq,
    raw_request,
    raw_response
)"#;
const SQL_DELETE_OLD_LLM_REQUEST_EVENTS_BATCH: &str = r#"
WITH doomed AS (
    SELECT id
    FROM llm_request_events
    WHERE NOT is_rollup
      AND created_at < now() - ($1::int * interval '1 day')
    ORDER BY created_at ASC
    LIMIT $2
)
DELETE FROM llm_request_events e
USING doomed
WHERE e.id = doomed.id"#;
const SQL_SCRUB_OLD_LLM_RAW_BODIES: &str = r#"
WITH doomed AS (
    SELECT id
    FROM llm_request_events
    WHERE (raw_request IS NOT NULL OR raw_response IS NOT NULL)
      AND created_at < now() - ($1::int * interval '1 hour')
    ORDER BY created_at ASC
    LIMIT $2
)
UPDATE llm_request_events e
SET raw_request = NULL,
    raw_response = NULL
FROM doomed
WHERE e.id = doomed.id"#;
const SQL_TRY_ANALYTICS_ROLLUP_LOCK: &str =
    "SELECT pg_try_advisory_xact_lock(hashtext('openplotva.analytics_rollup_cleanup'))";
const SQL_ROLLUP_OLD_LLM_REQUEST_EVENTS: &str = r#"
WITH grouped AS (
    SELECT
        date_trunc($1, created_at)::timestamptz AS bucket_start,
        COALESCE(NULLIF(source, ''), 'unknown')::text AS source,
        NULLIF(provider, '')::text AS provider,
        COALESCE(NULLIF(model, ''), 'unknown')::text AS model,
        chat_id,
        max_tokens,
        temperature,
        top_p,
        top_k,
        candidate_count,
        COALESCE(NULLIF(tool_mode, ''), 'default')::text AS tool_mode,
        COALESCE(NULLIF(response_format, ''), 'text')::text AS response_format,
        count(*)::int AS request_count,
        count(*) FILTER (WHERE error IS NOT NULL AND error <> '')::int AS error_count,
        COALESCE(sum(duration_ms::bigint), 0)::bigint AS duration_ms_sum,
        COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY duration_ms), 0)::int AS p50_duration_ms,
        COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms), 0)::int AS p95_duration_ms,
        COALESCE(sum(input_tokens), 0)::bigint AS input_tokens_sum,
        COALESCE(sum(output_tokens), 0)::bigint AS output_tokens_sum,
        COALESCE(sum(total_tokens), 0)::bigint AS total_tokens_sum,
        COALESCE(sum(generation_tps), 0)::double precision AS generation_tps_sum,
        count(generation_tps)::int AS generation_tps_count,
        COALESCE(sum(effective_output_tps), 0)::double precision AS effective_output_tps_sum,
        count(effective_output_tps)::int AS effective_output_tps_count,
        COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY effective_output_tps) FILTER (WHERE effective_output_tps IS NOT NULL), 0)::double precision AS p50_effective_output_tps,
        COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY effective_output_tps) FILTER (WHERE effective_output_tps IS NOT NULL), 0)::double precision AS p95_effective_output_tps,
        COALESCE(sum(iteration::bigint), 0)::bigint AS iteration_sum,
        COALESCE(max(iteration), 0)::int AS iteration_max
    FROM llm_request_events
    WHERE NOT is_rollup
      AND created_at < now() - ($2::int * interval '1 day')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)
INSERT INTO llm_request_events (
    created_at,
    provider,
    source,
    chat_id,
    model,
    iteration,
    prompt_chars,
    prompt_messages,
    docs_chars,
    duration_ms,
    max_tokens,
    temperature,
    top_p,
    top_k,
    candidate_count,
    tool_mode,
    response_format,
    inference_params,
    error,
    is_rollup,
    rollup_granularity,
    bucket_start,
    bucket_end,
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
    effective_output_tps_count,
    p50_effective_output_tps,
    p95_effective_output_tps,
    iteration_sum,
    iteration_max
)
SELECT
    bucket_start,
    provider,
    source,
    chat_id,
    model,
    iteration_max,
    0,
    0,
    0,
    CASE WHEN request_count > 0 THEN (duration_ms_sum / request_count)::int ELSE 0 END,
    max_tokens,
    temperature,
    top_p,
    top_k,
    candidate_count,
    tool_mode,
    response_format,
    '{}'::jsonb,
    '',
    TRUE,
    $1,
    bucket_start,
    bucket_start + CASE WHEN $1 = 'day' THEN interval '1 day' ELSE interval '1 hour' END,
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
    effective_output_tps_count,
    p50_effective_output_tps,
    p95_effective_output_tps,
    iteration_sum,
    iteration_max
FROM grouped
ON CONFLICT DO NOTHING"#;
const SQL_ROLLUP_MEMORY_RUNS: &str = r#"
WITH grouped AS (
    SELECT
        date_trunc($1, range_start_at)::timestamptz AS bucket_start,
        status,
        prompt_version,
        count(*)::int AS run_count,
        COALESCE(sum(message_count), 0)::int AS message_count,
        COALESCE(sum(cards_inserted), 0)::int AS cards_inserted,
        COALESCE(sum(cards_updated), 0)::int AS cards_updated,
        COALESCE(sum(cards_superseded), 0)::int AS cards_superseded,
        COALESCE(sum(episodes_inserted), 0)::int AS episodes_inserted,
        COALESCE(sum(input_token_estimate), 0)::int AS input_tokens,
        COALESCE(sum(output_token_estimate), 0)::int AS output_tokens
    FROM memory_runs
    WHERE range_start_at < now() - ($2::int * interval '1 day')
    GROUP BY 1, status, prompt_version
),
prepared AS (
    SELECT
        'memory'::text AS source,
        'run'::text AS kind,
        $1::text AS granularity,
        bucket_start,
        bucket_start + CASE WHEN $1 = 'day' THEN interval '1 day' ELSE interval '1 hour' END AS bucket_end,
        jsonb_build_object('status', status, 'prompt_version', prompt_version) AS dimensions,
        jsonb_build_object(
            'run_count', run_count,
            'message_count', message_count,
            'cards_inserted', cards_inserted,
            'cards_updated', cards_updated,
            'cards_superseded', cards_superseded,
            'episodes_inserted', episodes_inserted,
            'input_tokens', input_tokens,
            'output_tokens', output_tokens
        ) AS metrics
    FROM grouped
)
INSERT INTO telemetry_rollups (
    source, kind, granularity, bucket_start, bucket_end, dimensions_hash, dimensions, metrics
)
SELECT source, kind, granularity, bucket_start, bucket_end, md5(dimensions::text), dimensions, metrics
FROM prepared
ON CONFLICT (source, kind, granularity, bucket_start, dimensions_hash) DO UPDATE SET
    bucket_end = EXCLUDED.bucket_end,
    dimensions = EXCLUDED.dimensions,
    metrics = EXCLUDED.metrics,
    updated_at = CURRENT_TIMESTAMP"#;
// Purge terminal memory_runs after they are rolled up. Cutoff = retention_days,
// which (default 14) MUST stay >= the memory pipeline's ensure_daily window
// (MEMORY_RETENTION_HOURS=168=7d) so re-creation never violates the runs' UNIQUE
// idempotency constraint. Never touches queued/processing runs.
const SQL_DELETE_OLD_MEMORY_RUNS: &str = r#"
DELETE FROM memory_runs
WHERE status IN ('completed','skipped','failed')
  AND range_start_at < now() - ($1::int * interval '1 day')"#;
const SQL_ROLLUP_CHAT_HISTORY_INTERESTS: &str = r#"
WITH grouped AS (
    SELECT
        date_trunc($1, e.range_end_at)::timestamptz AS bucket_start,
        e.chat_id,
        e.thread_id,
        topic.topic,
        count(*)::int AS episode_count,
        COALESCE(sum(e.message_count), 0)::int AS message_count
    FROM memory_episodes e
    CROSS JOIN LATERAL unnest(e.topics) AS topic(topic)
    WHERE e.range_end_at < now() - ($2::int * interval '1 day')
      AND btrim(topic.topic) <> ''
    GROUP BY 1, e.chat_id, e.thread_id, topic.topic
),
prepared AS (
    SELECT
        'chat_history'::text AS source,
        'interest'::text AS kind,
        $1::text AS granularity,
        bucket_start,
        bucket_start + CASE WHEN $1 = 'day' THEN interval '1 day' ELSE interval '1 hour' END AS bucket_end,
        jsonb_build_object('chat_id', chat_id, 'thread_id', thread_id, 'topic', topic) AS dimensions,
        jsonb_build_object('episode_count', episode_count, 'message_count', message_count) AS metrics
    FROM grouped
)
INSERT INTO telemetry_rollups (
    source, kind, granularity, bucket_start, bucket_end, dimensions_hash, dimensions, metrics
)
SELECT source, kind, granularity, bucket_start, bucket_end, md5(dimensions::text), dimensions, metrics
FROM prepared
ON CONFLICT (source, kind, granularity, bucket_start, dimensions_hash) DO UPDATE SET
    bucket_end = EXCLUDED.bucket_end,
    dimensions = EXCLUDED.dimensions,
    metrics = EXCLUDED.metrics,
    updated_at = CURRENT_TIMESTAMP"#;

#[derive(Clone, Debug)]
pub struct RuntimeLlmTraceBuffer {
    inner: Arc<Mutex<RuntimeLlmTraceBufferInner>>,
    next_id: Arc<AtomicI64>,
}

#[derive(Debug)]
struct RuntimeLlmTraceBufferInner {
    ring: Vec<Option<RuntimeLlmRequestData>>,
    write: usize,
    count: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeLlmRequestEventCleanupReport {
    pub enabled: bool,
    pub ticks: u64,
    pub deleted: u64,
    pub errors: u64,
}

impl Default for RuntimeLlmTraceBuffer {
    fn default() -> Self {
        Self::new(GO_LLM_TRACE_BUFFER_CAPACITY)
    }
}

impl RuntimeLlmTraceBuffer {
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 {
            GO_LLM_TRACE_BUFFER_CAPACITY
        } else {
            capacity
        };
        Self {
            inner: Arc::new(Mutex::new(RuntimeLlmTraceBufferInner {
                ring: vec![None; capacity],
                write: 0,
                count: 0,
            })),
            next_id: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn record(&self, mut trace: RuntimeLlmRequestData) -> i64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        trace.id = id;
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.ring.is_empty() {
            return id;
        }
        let write = inner.write;
        inner.ring[write] = Some(trace);
        inner.write = (write + 1) % inner.ring.len();
        inner.count = inner.count.saturating_add(1).min(inner.ring.len());
        id
    }

    /// Fetch one live trace by its ring id (admin raw-payload resolution).
    pub fn get(&self, id: i64) -> Option<RuntimeLlmRequestData> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        inner
            .ring
            .iter()
            .flatten()
            .find(|trace| trace.id == id)
            .cloned()
    }

    pub fn prune_chat(&self, chat_id: i64) -> i32 {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if chat_id == 0 || inner.count == 0 || inner.ring.is_empty() {
            return 0;
        }

        let capacity = inner.ring.len();
        let start = if inner.count == capacity {
            inner.write
        } else {
            0
        };
        let mut kept = Vec::with_capacity(inner.count);
        let mut removed = 0i32;
        for offset in 0..inner.count {
            let idx = (start + offset) % capacity;
            let Some(trace) = inner.ring[idx].take() else {
                continue;
            };
            if trace.chat.chat_id == chat_id {
                removed = removed.saturating_add(1);
            } else {
                kept.push(trace);
            }
        }

        inner.ring.fill(None);
        inner.write = 0;
        inner.count = 0;
        for trace in kept {
            let write = inner.write;
            inner.ring[write] = Some(trace);
            inner.write = (write + 1) % capacity;
            inner.count += 1;
        }
        removed
    }

    fn list(&self) -> Vec<RuntimeLlmRequestData> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.count == 0 || inner.ring.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(inner.count);
        let mut idx = inner.write.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        for _ in 0..inner.count {
            if let Some(trace) = inner.ring[idx].clone() {
                out.push(trace);
            }
            idx = idx.checked_sub(1).unwrap_or(inner.ring.len() - 1);
        }
        out
    }
}

/// Whether and how large raw request/response bodies are persisted alongside
/// the analytics row (short-lived; an hourly scrub NULLs them after
/// `LLM_RAW_BODY_RETENTION_HOURS`).
#[derive(Clone, Copy, Debug)]
pub struct RawBodyPolicy {
    pub enabled: bool,
    pub max_bytes: usize,
}

impl Default for RawBodyPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_bytes: 65_536,
        }
    }
}

/// SQLx-backed LLM analytics event recorder.
#[derive(Clone, Debug)]
pub struct PostgresRuntimeLlmEventRecorder {
    sender: mpsc::Sender<RuntimeLlmRequestData>,
}

impl PostgresRuntimeLlmEventRecorder {
    /// Build a recorder and background buffered writer over an existing Postgres pool.
    pub fn spawn(
        pool: PgPool,
        stop: watch::Receiver<bool>,
        raw_bodies: RawBodyPolicy,
    ) -> (Self, JoinHandle<()>) {
        let (sender, receiver) = mpsc::channel(LLM_EVENT_WRITER_CHANNEL_CAPACITY);
        let handle = tokio::spawn(run_llm_request_event_writer(
            pool,
            receiver,
            stop,
            LLM_EVENT_WRITER_BATCH_SIZE,
            LLM_EVENT_WRITER_FLUSH_INTERVAL,
            raw_bodies,
        ));
        (Self { sender }, handle)
    }

    fn enqueue(&self, trace: RuntimeLlmRequestData) {
        if let Err(error) = self.sender.try_send(trace) {
            match error {
                mpsc::error::TrySendError::Full(trace) => {
                    tracing::warn!(
                        source = %trace.source,
                        "dropping llm request event because writer channel is full"
                    );
                }
                mpsc::error::TrySendError::Closed(trace) => {
                    tracing::debug!(
                        source = %trace.source,
                        "dropping llm request event because writer is stopped"
                    );
                }
            }
        }
    }
}

/// Observer that turns low-level `openplotva-llm` call records into runtime trace rows.
/// Registered process-wide so every model round-trip (dialog, memory, history, prompt
/// optimizers, and each aifarm pool attempt) becomes one `llm_request_events` row — the
/// single source of those rows, replacing the provider-level `TracingChatProvider`.
#[derive(Clone)]
pub struct RuntimeLlmObserver {
    buffer: RuntimeLlmTraceBuffer,
    recorder: Option<PostgresRuntimeLlmEventRecorder>,
    runs: Option<crate::runtime_llm_runs::RuntimeLlmRunBuffer>,
}

impl RuntimeLlmObserver {
    /// Build an observer feeding the live ring buffer and, when present, the Postgres
    /// analytics recorder.
    pub fn new(
        buffer: RuntimeLlmTraceBuffer,
        recorder: Option<PostgresRuntimeLlmEventRecorder>,
    ) -> Self {
        Self {
            buffer,
            recorder,
            runs: None,
        }
    }

    /// Additionally fan every observation out into the agent-run buffer:
    /// scoped calls become rounds of their run, unscoped calls become
    /// single-round one-off records.
    #[must_use]
    pub fn with_run_buffer(mut self, runs: crate::runtime_llm_runs::RuntimeLlmRunBuffer) -> Self {
        self.runs = Some(runs);
        self
    }
}

fn usage_i32(trace: &RuntimeLlmRequestData, key: &str) -> Option<i32> {
    trace
        .usage
        .as_ref()
        .and_then(|usage| usage.get(key))
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
}

fn run_round_from_trace(
    trace: &RuntimeLlmRequestData,
    trace_id: i64,
    is_aux: bool,
) -> crate::runtime_llm_runs::RunRound {
    let response_text = trace
        .raw_response
        .as_ref()
        .and_then(trace_response_text)
        .and_then(|text| non_empty(&text))
        .or_else(|| trace.result.response_text_preview.clone());
    crate::runtime_llm_runs::RunRound {
        seq: 0,
        trace_id,
        at: trace.at.clone(),
        provider: trace.provider.clone(),
        model: trace.model.clone(),
        flow: trace.flow.clone(),
        is_aux,
        iteration: trace.iteration,
        duration_ms: trace.result.duration_ms.max(trace.duration_ms),
        input_tokens: usage_i32(trace, "input_tokens"),
        output_tokens: usage_i32(trace, "output_tokens"),
        total_tokens: usage_i32(trace, "total_tokens"),
        error: trace.result.error.clone(),
        response_text,
        sent: crate::runtime_llm_runs::RunRoundSent::No,
        tool_calls: Vec::new(),
    }
}

impl openplotva_llm::LlmCallObserver for RuntimeLlmObserver {
    fn observe(&self, record: openplotva_llm::LlmCallRecord) {
        let mut trace = trace_from_record(
            &record.context,
            &record.artifact,
            record.duration_ms,
            record.run.as_ref(),
        );
        let trace_id = self.buffer.record(trace.clone());
        if let Some(runs) = &self.runs {
            match record.run.as_ref() {
                Some(scope) => {
                    // Inside a dialog session, non-dialog flows are nested
                    // auxiliary calls (vision, history summaries, …).
                    let is_aux = scope.run_kind == "dialog"
                        && trace.flow.as_deref().is_some_and(|flow| flow != "dialog");
                    let round = run_round_from_trace(&trace, trace_id, is_aux);
                    trace.run_seq = runs.record_round(&scope.run_id, round);
                }
                None => {
                    let kind = trace
                        .flow
                        .clone()
                        .filter(|flow| !flow.is_empty())
                        .unwrap_or_else(|| trace.source.clone());
                    let origin = crate::runtime_llm_runs::RunOrigin {
                        chat_id: record.context.chat_id,
                        thread_id: record.context.thread_id,
                        chat_title: non_empty(&record.context.chat_title),
                        user_id: record.context.user_id,
                        user_full_name: non_empty(&record.context.full_name),
                        trigger_message_id: record.context.message_id,
                        ..crate::runtime_llm_runs::RunOrigin::default()
                    };
                    let round = run_round_from_trace(&trace, trace_id, false);
                    runs.record_one_off(&kind, origin, round, OffsetDateTime::now_utc());
                }
            }
        }
        if let Some(recorder) = &self.recorder {
            recorder.enqueue(trace);
        }
    }
}

fn trace_from_record(
    context: &openplotva_llm::LlmCallContext,
    artifact: &DialogTraceArtifacts,
    duration_ms: i32,
    run: Option<&openplotva_llm::LlmRunScope>,
) -> RuntimeLlmRequestData {
    let mut trace = RuntimeLlmRequestData {
        at: format_ts(OffsetDateTime::now_utc()),
        duration_ms,
        chat: RuntimeLlmRequestChatData {
            chat_id: context.chat_id,
            thread_id: context.thread_id,
            chat_title: non_empty(&context.chat_title),
        },
        user: RuntimeLlmRequestUserData {
            user_id: context.user_id,
            full_name: non_empty(&context.full_name),
        },
        message: RuntimeLlmRequestMessageData {
            message_id: context.message_id,
        },
        run_id: run.map(|scope| scope.run_id.clone()),
        ..RuntimeLlmRequestData::default()
    };
    apply_dialog_trace_artifact(&mut trace, artifact);
    // A client stamps the provider it happened to route through (the generic
    // "aifarm" pool, a static client, a dynamic router client), so the same
    // upstream model appears under different provider names across call paths.
    // Re-derive the provider from the model (the authoritative field) so every
    // path — dialog, memory extraction, optimizers — reports the same canonical
    // provider. Mirrors provider_models; migration 150 backfills history the same
    // way. Unknown models keep the client-stamped provider.
    if let Some(canonical) = canonical_provider_for_model(
        trace.model.as_deref().unwrap_or_default(),
        trace.flow.as_deref().unwrap_or_default(),
    ) {
        trace.provider = Some(canonical.to_owned());
    }
    let preview = artifact
        .raw_response
        .as_ref()
        .and_then(trace_response_text)
        .map(|text| compact_preview(&text, GO_RESPONSE_PREVIEW_SIZE))
        .and_then(|text| non_empty(&text));
    trace.result = RuntimeLlmRequestResultData {
        duration_ms,
        error: non_empty(&artifact.error),
        response_text_preview: preview,
    };
    trace
}

/// Canonical routing provider for a model, mirroring the `provider_models`
/// registry. Returns `None` for models not in the map so the client-stamped
/// provider is left untouched (new models keep working until they are added).
/// `flow` disambiguates models registered under more than one provider — only
/// "Gemma 4 26B Heretic" today: vision calls belong to `aifarm-vision`, the rest
/// to the vLLM chat provider. Migration 150 backfills history with this same map.
fn canonical_provider_for_model(model: &str, flow: &str) -> Option<&'static str> {
    let model = model.trim();
    if model.to_ascii_lowercase().starts_with("vram.cloud/") {
        return Some("vram-cloud");
    }
    match model {
        "vibethinker-3b" | "qwen3.6-27b-moq" => Some("aifarm-llamacpp-gpu2"),
        "Gemma 4 26B Heretic" => Some(if flow.eq_ignore_ascii_case("vision") {
            "aifarm-vision"
        } else {
            "aifarm-vllm-gpu0"
        }),
        _ => None,
    }
}

impl RuntimeLlmTraceInspector for RuntimeLlmTraceBuffer {
    fn llm_requests(
        &self,
        filter: RuntimeLlmRequestsFilter,
    ) -> Result<Vec<RuntimeLlmRequestData>, String> {
        let limit = filter.limit.max(1) as usize;
        let mut out = Vec::with_capacity(limit.min(GO_LLM_TRACE_BUFFER_CAPACITY));
        for trace in self.list() {
            if llm_trace_matches_filter(&trace, &filter) {
                out.push(trace);
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }
}

async fn run_llm_request_event_writer(
    pool: PgPool,
    mut receiver: mpsc::Receiver<RuntimeLlmRequestData>,
    mut stop: watch::Receiver<bool>,
    batch_size: usize,
    flush_interval: Duration,
    raw_bodies: RawBodyPolicy,
) {
    let batch_size = batch_size.max(1);
    let mut pending = Vec::with_capacity(batch_size);
    let mut interval = tokio::time::interval(flush_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if *stop.borrow() {
            drain_and_flush_llm_request_event_batches(
                &pool,
                &mut receiver,
                &mut pending,
                batch_size,
                raw_bodies,
            )
            .await;
            break;
        }

        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    drain_and_flush_llm_request_event_batches(
                        &pool,
                        &mut receiver,
                        &mut pending,
                        batch_size,
                        raw_bodies,
                    )
                    .await;
                    break;
                }
            }
            maybe_trace = receiver.recv() => {
                let Some(trace) = maybe_trace else {
                    flush_llm_request_event_batch(&pool, &mut pending, raw_bodies).await;
                    break;
                };
                pending.push(trace);
                if pending.len() >= batch_size {
                    flush_llm_request_event_batch(&pool, &mut pending, raw_bodies).await;
                }
            }
            _ = interval.tick() => {
                flush_llm_request_event_batch(&pool, &mut pending, raw_bodies).await;
            }
        }
    }
}

async fn drain_and_flush_llm_request_event_batches(
    pool: &PgPool,
    receiver: &mut mpsc::Receiver<RuntimeLlmRequestData>,
    pending: &mut Vec<RuntimeLlmRequestData>,
    batch_size: usize,
    raw_bodies: RawBodyPolicy,
) {
    loop {
        drain_llm_request_event_channel(receiver, pending, batch_size);
        if pending.is_empty() {
            break;
        }
        flush_llm_request_event_batch(pool, pending, raw_bodies).await;
    }
}

fn drain_llm_request_event_channel(
    receiver: &mut mpsc::Receiver<RuntimeLlmRequestData>,
    pending: &mut Vec<RuntimeLlmRequestData>,
    batch_size: usize,
) {
    while pending.len() < batch_size {
        match receiver.try_recv() {
            Ok(trace) => pending.push(trace),
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }
}

async fn flush_llm_request_event_batch(
    pool: &PgPool,
    pending: &mut Vec<RuntimeLlmRequestData>,
    raw_bodies: RawBodyPolicy,
) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    if let Err(error) = insert_llm_request_events(pool, &batch, raw_bodies).await {
        let sources = batch
            .iter()
            .map(|trace| trace.source.as_str())
            .collect::<Vec<_>>()
            .join(",");
        tracing::warn!(%error, sources, count = batch.len(), "failed to insert llm request event batch");
    }
}

async fn insert_llm_request_events(
    pool: &PgPool,
    traces: &[RuntimeLlmRequestData],
    raw_bodies: RawBodyPolicy,
) -> Result<(), sqlx::Error> {
    if traces.is_empty() {
        return Ok(());
    }
    let events = traces
        .iter()
        .map(|trace| LlmRequestEvent::from_trace(trace, raw_bodies))
        .collect::<Vec<_>>();
    let mut builder = llm_request_event_insert_builder(&events);
    builder.build().execute(pool).await?;
    Ok(())
}

pub async fn delete_old_llm_request_events_batch(
    pool: &PgPool,
    retention_days: i32,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    if retention_days <= 0 || batch_size <= 0 {
        return Ok(0);
    }
    let mut tx = pool.begin().await?;
    let locked: bool = sqlx::query_scalar(SQL_TRY_ANALYTICS_ROLLUP_LOCK)
        .fetch_one(&mut *tx)
        .await?;
    if !locked {
        tx.commit().await?;
        return Ok(0);
    }
    for granularity in ["hour", "day"] {
        sqlx::query(SQL_ROLLUP_OLD_LLM_REQUEST_EVENTS)
            .bind(granularity)
            .bind(retention_days)
            .execute(&mut *tx)
            .await?;
        sqlx::query(SQL_ROLLUP_MEMORY_RUNS)
            .bind(granularity)
            .bind(retention_days)
            .execute(&mut *tx)
            .await?;
        sqlx::query(SQL_ROLLUP_CHAT_HISTORY_INTERESTS)
            .bind(granularity)
            .bind(retention_days)
            .execute(&mut *tx)
            .await?;
    }
    sqlx::query(SQL_DELETE_OLD_MEMORY_RUNS)
        .bind(retention_days)
        .execute(&mut *tx)
        .await?;
    let result = sqlx::query(SQL_DELETE_OLD_LLM_REQUEST_EVENTS_BATCH)
        .bind(retention_days)
        .bind(batch_size)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(result.rows_affected())
}

pub async fn run_llm_request_event_cleanup_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_days: i32,
    batch_size: i64,
    stop: Stop,
) -> RuntimeLlmRequestEventCleanupReport
where
    Stop: std::future::Future<Output = ()>,
{
    let mut report = RuntimeLlmRequestEventCleanupReport {
        enabled: retention_days > 0,
        ..RuntimeLlmRequestEventCleanupReport::default()
    };
    if !report.enabled {
        return report;
    }

    let stop = stop;
    tokio::pin!(stop);
    loop {
        match delete_old_llm_request_events_batch(&pool, retention_days, batch_size).await {
            Ok(deleted) => {
                report.deleted += deleted;
                tracing::debug!(
                    deleted,
                    retention_days,
                    "deleted old llm_request_events batch"
                );
            }
            Err(error) => {
                report.errors += 1;
                tracing::warn!(%error, retention_days, "failed to delete old llm_request_events batch");
            }
        }
        report.ticks += 1;

        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

const SQL_SELECT_LLM_RAW_BODIES_FOR_RUN: &str = r#"
SELECT run_seq, raw_request, raw_response
FROM llm_request_events
WHERE run_id = $1
  AND run_seq IS NOT NULL
  AND NOT is_rollup
  AND (raw_request IS NOT NULL OR raw_response IS NOT NULL)"#;

/// Persisted raw bodies of one run keyed by `run_seq` (admin detail fallback
/// when the live trace ring already rotated the round out).
type PersistedRawBodyRow = (
    Option<i32>,
    Option<sqlx::types::Json<Value>>,
    Option<sqlx::types::Json<Value>>,
);

pub async fn fetch_llm_raw_bodies_for_run(
    pool: &PgPool,
    run_id: &str,
) -> Result<std::collections::HashMap<i32, (Option<Value>, Option<Value>)>, sqlx::Error> {
    let rows: Vec<PersistedRawBodyRow> = sqlx::query_as(SQL_SELECT_LLM_RAW_BODIES_FOR_RUN)
        .bind(run_id)
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(seq, request, response)| {
            seq.map(|seq| {
                (
                    seq,
                    (request.map(|json| json.0), response.map(|json| json.0)),
                )
            })
        })
        .collect())
}

/// NULL raw bodies older than the retention window, oldest first. Returns the
/// number of scrubbed rows; call repeatedly until it returns less than
/// `batch_size`.
pub async fn scrub_old_llm_raw_bodies_batch(
    pool: &PgPool,
    retention_hours: i32,
    batch_size: i64,
) -> Result<u64, sqlx::Error> {
    if retention_hours <= 0 || batch_size <= 0 {
        return Ok(0);
    }
    let result = sqlx::query(SQL_SCRUB_OLD_LLM_RAW_BODIES)
        .bind(retention_hours)
        .bind(batch_size)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Hourly scrub worker keeping `llm_request_events` raw bodies short-lived.
pub async fn run_llm_raw_body_scrub_worker_until<Stop>(
    pool: PgPool,
    interval: Duration,
    retention_hours: i32,
    batch_size: i64,
    stop: Stop,
) -> RuntimeLlmRequestEventCleanupReport
where
    Stop: std::future::Future<Output = ()>,
{
    let mut report = RuntimeLlmRequestEventCleanupReport {
        enabled: retention_hours > 0,
        ..RuntimeLlmRequestEventCleanupReport::default()
    };
    if !report.enabled {
        return report;
    }

    let stop = stop;
    tokio::pin!(stop);
    loop {
        loop {
            match scrub_old_llm_raw_bodies_batch(&pool, retention_hours, batch_size).await {
                Ok(scrubbed) => {
                    report.deleted += scrubbed;
                    if scrubbed > 0 {
                        tracing::debug!(
                            scrubbed,
                            retention_hours,
                            "scrubbed old llm raw bodies batch"
                        );
                    }
                    if scrubbed < batch_size.max(1) as u64 {
                        break;
                    }
                }
                Err(error) => {
                    report.errors += 1;
                    tracing::warn!(%error, retention_hours, "failed to scrub old llm raw bodies batch");
                    break;
                }
            }
        }
        report.ticks += 1;

        let sleep = tokio::time::sleep(interval);
        tokio::pin!(sleep);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {}
        }
    }
    report
}

fn llm_request_event_insert_builder(events: &[LlmRequestEvent]) -> QueryBuilder<Postgres> {
    let mut builder = QueryBuilder::new(SQL_INSERT_LLM_REQUEST_EVENTS_PREFIX);
    builder.push(" ");
    builder.push_values(events.iter(), |mut row, event| {
        row.push_bind(event.created_at)
            .push_bind(event.provider.clone())
            .push_bind(event.request_kind.clone())
            .push_bind(event.source.clone())
            .push_bind(event.flow.clone())
            .push_bind(event.chat_id)
            .push_bind(event.thread_id)
            .push_bind(event.message_id)
            .push_bind(event.user_id)
            .push_bind(event.model.clone())
            .push_bind(event.iteration)
            .push_bind(event.prompt_chars)
            .push_bind(event.prompt_messages)
            .push_bind(event.docs_chars)
            .push_bind(event.duration_ms)
            .push_bind(event.input_tokens)
            .push_bind(event.output_tokens)
            .push_bind(event.total_tokens)
            .push_bind(event.cached_tokens)
            .push_bind(event.thoughts_tokens)
            .push_bind(event.tool_use_prompt_tokens)
            .push_bind(event.prompt_eval_tokens)
            .push_bind(event.prompt_eval_ms)
            .push_bind(event.prompt_tps)
            .push_bind(event.generation_tokens)
            .push_bind(event.generation_ms)
            .push_bind(event.generation_tps)
            .push_bind(event.effective_output_tps)
            .push_bind(event.effective_total_tps)
            .push_bind(event.max_tokens)
            .push_bind(event.temperature)
            .push_bind(event.top_p)
            .push_bind(event.top_k)
            .push_bind(event.candidate_count)
            .push_bind(event.tool_mode.clone())
            .push_bind(event.response_format.clone())
            .push_bind(sqlx::types::Json(event.inference_params.clone()))
            .push_bind(event.error.clone())
            .push_bind(event.run_id.clone())
            .push_bind(event.run_seq)
            .push_bind(event.raw_request.clone().map(sqlx::types::Json))
            .push_bind(event.raw_response.clone().map(sqlx::types::Json));
    });
    builder
}

#[cfg(test)]
fn llm_request_event_batch_insert_sql_for_test(traces: &[RuntimeLlmRequestData]) -> String {
    let events = traces
        .iter()
        .map(|trace| LlmRequestEvent::from_trace(trace, RawBodyPolicy::default()))
        .collect::<Vec<_>>();
    llm_request_event_insert_builder(&events).into_string()
}

#[cfg(test)]
fn llm_request_event_shutdown_batch_sizes_for_test(
    mut receiver: mpsc::Receiver<RuntimeLlmRequestData>,
    batch_size: usize,
) -> Vec<usize> {
    let batch_size = batch_size.max(1);
    let mut pending = Vec::with_capacity(batch_size);
    let mut batch_sizes = Vec::new();
    loop {
        drain_llm_request_event_channel(&mut receiver, &mut pending, batch_size);
        if pending.is_empty() {
            break;
        }
        batch_sizes.push(pending.len());
        pending.clear();
    }
    batch_sizes
}

#[derive(Debug, PartialEq)]
struct LlmRequestEvent {
    created_at: OffsetDateTime,
    provider: Option<String>,
    request_kind: Option<String>,
    source: String,
    flow: Option<String>,
    chat_id: Option<i64>,
    thread_id: Option<i32>,
    message_id: Option<i32>,
    user_id: Option<i64>,
    model: Option<String>,
    iteration: i32,
    prompt_chars: i32,
    prompt_messages: i32,
    docs_chars: i32,
    duration_ms: i32,
    input_tokens: Option<i32>,
    output_tokens: Option<i32>,
    total_tokens: Option<i32>,
    cached_tokens: Option<i32>,
    thoughts_tokens: Option<i32>,
    tool_use_prompt_tokens: Option<i32>,
    prompt_eval_tokens: Option<i32>,
    prompt_eval_ms: Option<f64>,
    prompt_tps: Option<f64>,
    generation_tokens: Option<i32>,
    generation_ms: Option<f64>,
    generation_tps: Option<f64>,
    effective_output_tps: Option<f64>,
    effective_total_tps: Option<f64>,
    max_tokens: Option<i32>,
    temperature: Option<f64>,
    top_p: Option<f64>,
    top_k: Option<i32>,
    candidate_count: Option<i32>,
    tool_mode: Option<String>,
    response_format: Option<String>,
    inference_params: Value,
    error: Option<String>,
    run_id: Option<String>,
    run_seq: Option<i32>,
    raw_request: Option<Value>,
    raw_response: Option<Value>,
}

/// A raw body is persisted only under the policy and only when its serialized
/// size fits the cap; oversized bodies degrade to NULL, not truncated JSON.
fn raw_body_within_policy(value: Option<&Value>, policy: RawBodyPolicy) -> Option<Value> {
    if !policy.enabled {
        return None;
    }
    let value = value?;
    if value.is_null() {
        return None;
    }
    let size = serde_json::to_string(value).map(|json| json.len()).ok()?;
    (size <= policy.max_bytes).then(|| value.clone())
}

impl LlmRequestEvent {
    fn from_trace(trace: &RuntimeLlmRequestData, raw_bodies: RawBodyPolicy) -> Self {
        let inference_params = trace
            .inference_params
            .clone()
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        let usage = trace.usage.as_ref();
        let timings = trace.timings.as_ref();
        let duration_ms = if trace.result.duration_ms > 0 {
            trace.result.duration_ms
        } else {
            trace.duration_ms
        };

        Self {
            created_at: parse_rfc3339(&trace.at).unwrap_or_else(OffsetDateTime::now_utc),
            provider: non_empty_opt(trace.provider.as_deref()),
            request_kind: non_empty_opt(trace.request_kind.as_deref()),
            source: non_empty(&trace.source).unwrap_or_else(|| "unknown".to_owned()),
            flow: non_empty_opt(trace.flow.as_deref()),
            chat_id: nonzero_i64(trace.chat.chat_id),
            thread_id: trace.chat.thread_id,
            message_id: nonzero_i32(trace.message.message_id),
            user_id: nonzero_i64(trace.user.user_id),
            model: non_empty_opt(trace.model.as_deref()),
            iteration: trace.iteration.max(1),
            prompt_chars: trace.prompt_chars.max(0),
            prompt_messages: trace.prompt_messages.max(0),
            docs_chars: trace.docs_chars.max(0),
            duration_ms: duration_ms.max(0),
            input_tokens: positive_json_i32(usage, "input_tokens"),
            output_tokens: positive_json_i32(usage, "output_tokens"),
            total_tokens: positive_json_i32(usage, "total_tokens"),
            cached_tokens: positive_json_i32(usage, "cached_tokens"),
            thoughts_tokens: positive_json_i32(usage, "thoughts_tokens"),
            tool_use_prompt_tokens: positive_json_i32(usage, "tool_use_prompt_tokens"),
            prompt_eval_tokens: positive_json_i32(timings, "prompt_eval_tokens"),
            prompt_eval_ms: positive_json_f64(timings, "prompt_eval_ms"),
            prompt_tps: positive_json_f64(timings, "prompt_tps"),
            generation_tokens: positive_json_i32(timings, "generation_tokens"),
            generation_ms: positive_json_f64(timings, "generation_ms"),
            generation_tps: positive_json_f64(timings, "generation_tps"),
            effective_output_tps: positive_json_f64(timings, "effective_output_tps")
                .or_else(|| derived_tps(positive_json_i32(usage, "output_tokens"), duration_ms)),
            effective_total_tps: positive_json_f64(timings, "effective_total_tps")
                .or_else(|| derived_tps(positive_json_i32(usage, "total_tokens"), duration_ms)),
            max_tokens: positive_json_i32(Some(&inference_params), "max_tokens")
                .or_else(|| positive_i32(trace.gen_config.max_output_tokens)),
            temperature: positive_json_f64(Some(&inference_params), "temperature")
                .or_else(|| positive_f64(trace.gen_config.temperature)),
            top_p: positive_json_f64(Some(&inference_params), "top_p")
                .or_else(|| positive_f64(trace.gen_config.top_p)),
            top_k: positive_json_i32(Some(&inference_params), "top_k")
                .or_else(|| positive_i32(trace.gen_config.top_k)),
            candidate_count: positive_json_i32(Some(&inference_params), "candidate_count"),
            tool_mode: string_json_field(&inference_params, "tool_mode"),
            response_format: string_json_field(&inference_params, "response_format"),
            inference_params,
            error: non_empty_opt(trace.result.error.as_deref()),
            run_id: trace.run_id.clone(),
            run_seq: trace.run_seq,
            raw_request: raw_body_within_policy(trace.raw_request.as_ref(), raw_bodies),
            raw_response: raw_body_within_policy(trace.raw_response.as_ref(), raw_bodies),
        }
    }
}

fn apply_dialog_trace_artifact(trace: &mut RuntimeLlmRequestData, artifact: &DialogTraceArtifacts) {
    if !artifact.provider.trim().is_empty() {
        trace.provider = Some(artifact.provider.trim().to_owned());
    }
    if !artifact.request_kind.trim().is_empty() {
        trace.request_kind = Some(artifact.request_kind.trim().to_owned());
    }
    if !artifact.source.trim().is_empty() {
        trace.source = artifact.source.trim().to_owned();
    }
    if !artifact.mode.trim().is_empty() {
        trace.mode = Some(artifact.mode.trim().to_owned());
    }
    if !artifact.flow.trim().is_empty() {
        trace.flow = Some(artifact.flow.trim().to_owned());
    }
    if artifact.iteration > 0 {
        trace.iteration = artifact.iteration;
    }
    if !artifact.model.trim().is_empty() {
        trace.model = Some(artifact.model.trim().to_owned());
    }
    if let Some(raw_request) = artifact.raw_request.clone() {
        trace.raw_request = Some(raw_request);
    }
    if let Some(raw_response) = artifact.raw_response.clone() {
        trace.raw_response = Some(raw_response);
    }
    if let Some(resolved_cache_content) = artifact.resolved_cache_content.clone() {
        trace.resolved_cache_content = Some(resolved_cache_content);
    }
    if let Some(transport) = artifact.transport.clone() {
        trace.transport = Some(transport);
    }
    if let Some(inference_params) = artifact.inference_params.clone() {
        trace.inference_params = Some(inference_params);
    }
    trace.usage = artifact
        .usage
        .as_ref()
        .and_then(|usage| serde_json::to_value(usage).ok());
    if let Some(timings) = artifact.timings.clone() {
        trace.timings = Some(timings);
    }
    if artifact.prompt_chars > 0 {
        trace.prompt_chars = artifact.prompt_chars;
    }
    if artifact.prompt_messages > 0 {
        trace.prompt_messages = artifact.prompt_messages;
    }
    if artifact.docs_chars > 0 {
        trace.docs_chars = artifact.docs_chars;
    }
}

fn llm_trace_matches_filter(
    trace: &RuntimeLlmRequestData,
    filter: &RuntimeLlmRequestsFilter,
) -> bool {
    (filter.source.is_empty()
        || trace.source == filter.source
        || trace.provider.as_deref() == Some(filter.source.as_str()))
        && (filter.model.is_empty() || trace.model.as_deref() == Some(filter.model.as_str()))
        && filter
            .chat_id
            .is_none_or(|chat_id| trace.chat.chat_id == chat_id)
        && filter
            .user_id
            .is_none_or(|user_id| trace.user.user_id == user_id)
        && filter
            .message_id
            .is_none_or(|message_id| trace.message.message_id == message_id)
        && (!filter.error_only
            || trace
                .result
                .error
                .as_deref()
                .is_some_and(|error| !error.trim().is_empty()))
        && (!filter.empty_only
            || (trace
                .result
                .response_text_preview
                .as_deref()
                .unwrap_or_default()
                .trim()
                .is_empty()
                && trace
                    .result
                    .error
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .is_empty()))
        && (filter.q.is_empty() || llm_trace_matches_query(trace, &filter.q))
}

fn llm_trace_matches_query(trace: &RuntimeLlmRequestData, q: &str) -> bool {
    [
        trace.provider.as_deref().unwrap_or_default(),
        trace.request_kind.as_deref().unwrap_or_default(),
        &trace.source,
        trace.mode.as_deref().unwrap_or_default(),
        trace.flow.as_deref().unwrap_or_default(),
        trace.model.as_deref().unwrap_or_default(),
        trace.chat.chat_title.as_deref().unwrap_or_default(),
        trace.user.full_name.as_deref().unwrap_or_default(),
        trace
            .result
            .response_text_preview
            .as_deref()
            .unwrap_or_default(),
    ]
    .into_iter()
    .any(|field| contains_fold(field, q))
        || trace
            .raw_request
            .as_ref()
            .is_some_and(|value| contains_fold(&value.to_string(), q))
        || trace
            .raw_response
            .as_ref()
            .is_some_and(|value| contains_fold(&value.to_string(), q))
}

fn trace_response_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => non_empty(text),
        Value::Array(items) => items.iter().find_map(trace_response_text),
        Value::Object(map) => {
            for key in ["content", "text", "reasoning"] {
                if let Some(text) = map.get(key).and_then(Value::as_str).and_then(non_empty) {
                    return Some(text);
                }
            }
            for key in [
                "choices",
                "candidates",
                "message",
                "delta",
                "parts",
                "output",
                "response",
                "result",
                "data",
            ] {
                if let Some(text) = map.get(key).and_then(trace_response_text) {
                    return Some(text);
                }
            }
            None
        }
        _ => None,
    }
}

fn compact_preview(value: &str, limit: usize) -> String {
    let mut out = String::new();
    for part in value.split_whitespace() {
        if out.is_empty() {
            out.push_str(part);
        } else {
            out.push(' ');
            out.push_str(part);
        }
        if out.chars().count() >= limit {
            return out.chars().take(limit).collect();
        }
    }
    out
}

fn non_empty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

fn non_empty_opt(value: Option<&str>) -> Option<String> {
    value.and_then(non_empty)
}

fn nonzero_i32(value: i32) -> Option<i32> {
    (value != 0).then_some(value)
}

fn nonzero_i64(value: i64) -> Option<i64> {
    (value != 0).then_some(value)
}

fn positive_i32(value: i32) -> Option<i32> {
    (value > 0).then_some(value)
}

fn positive_f64(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}

fn positive_json_i32(value: Option<&Value>, key: &str) -> Option<i32> {
    json_i32(value?.get(key)?).and_then(positive_i32)
}

fn positive_json_f64(value: Option<&Value>, key: &str) -> Option<f64> {
    json_f64(value?.get(key)?).and_then(positive_f64)
}

fn json_i32(value: &Value) -> Option<i32> {
    if let Some(value) = value.as_i64() {
        return i32::try_from(value).ok();
    }
    if let Some(value) = value.as_u64() {
        return i32::try_from(value).ok();
    }
    if let Some(value) = value.as_f64().filter(|value| value.is_finite()) {
        return i32::try_from(value.round() as i64).ok();
    }
    value.as_str()?.trim().parse::<i32>().ok()
}

fn json_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
}

fn string_json_field(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).and_then(non_empty)
}

fn derived_tps(tokens: Option<i32>, duration_ms: i32) -> Option<f64> {
    let tokens = tokens?;
    if duration_ms <= 0 {
        return None;
    }
    positive_f64(f64::from(tokens) / (f64::from(duration_ms) / 1000.0))
}

fn contains_fold(value: &str, needle: &str) -> bool {
    value.to_lowercase().contains(&needle.to_lowercase())
}

fn parse_rfc3339(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

fn format_ts(ts: OffsetDateTime) -> String {
    ts.format(&Rfc3339).unwrap_or_else(|_| ts.to_string())
}

#[cfg(test)]
mod tests {
    use openplotva_server::RuntimeLlmGenConfigData;

    use super::*;

    #[test]
    fn runtime_observer_converts_record_to_row_and_buffers() {
        let buffer = RuntimeLlmTraceBuffer::new(8);
        let observer = RuntimeLlmObserver::new(buffer.clone(), None);
        openplotva_llm::LlmCallObserver::observe(
            &observer,
            openplotva_llm::LlmCallRecord {
                context: openplotva_llm::LlmCallContext {
                    chat_id: -100,
                    user_id: 7,
                    message_id: 77,
                    ..openplotva_llm::LlmCallContext::default()
                },
                artifact: openplotva_dialog::DialogTraceArtifacts {
                    provider: "aifarm".to_owned(),
                    source: "aifarm_memory_extractor".to_owned(),
                    flow: "memory_extraction".to_owned(),
                    model: "Gemma".to_owned(),
                    request_kind: "openai.chat.completions".to_owned(),
                    ..openplotva_dialog::DialogTraceArtifacts::default()
                },
                duration_ms: 123,
                run: None,
            },
        );
        let rows = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 10,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].chat.chat_id, -100);
        assert_eq!(rows[0].source, "aifarm_memory_extractor");
        assert_eq!(
            rows[0].request_kind.as_deref(),
            Some("openai.chat.completions")
        );
        assert_eq!(rows[0].model.as_deref(), Some("Gemma"));
        assert_eq!(rows[0].result.duration_ms, 123);
    }

    #[test]
    fn canonical_provider_maps_registry_models_and_flow() {
        assert_eq!(
            canonical_provider_for_model("vram.cloud/qwen3.6-35b-a3b", "memory_extraction"),
            Some("vram-cloud")
        );
        assert_eq!(
            canonical_provider_for_model("VRAM.CLOUD/qwen3.6-27b", "dialog"),
            Some("vram-cloud")
        );
        assert_eq!(
            canonical_provider_for_model("vibethinker-3b", "memory_extraction"),
            Some("aifarm-llamacpp-gpu2")
        );
        // Gemma is registered under two providers; flow disambiguates.
        assert_eq!(
            canonical_provider_for_model("Gemma 4 26B Heretic", "dialog"),
            Some("aifarm-vllm-gpu0")
        );
        assert_eq!(
            canonical_provider_for_model("Gemma 4 26B Heretic", "vision"),
            Some("aifarm-vision")
        );
        // Unknown models are left to the client-stamped provider.
        assert_eq!(
            canonical_provider_for_model("some-new-model", "dialog"),
            None
        );
        assert_eq!(canonical_provider_for_model("", "dialog"), None);
    }

    #[test]
    fn migration_150_backfill_mirrors_the_canonical_provider_map() {
        // The forward fix (this module) and the historical backfill (migration
        // 150) must converge on the same provider names, or old and new rows
        // disagree. Pin every canonical target the code emits into the migration.
        const MIGRATION: &str = include_str!(
            "../../../migrations/150_llm_request_events_provider_canonicalization.up.sql"
        );
        for (model, flow) in [
            ("vram.cloud/qwen3.6-35b-a3b", "memory_extraction"),
            ("vibethinker-3b", "memory_extraction"),
            ("qwen3.6-27b-moq", "dialog"),
            ("Gemma 4 26B Heretic", "dialog"),
            ("Gemma 4 26B Heretic", "vision"),
        ] {
            let canonical = canonical_provider_for_model(model, flow)
                .expect("registry model maps to a provider");
            assert!(
                MIGRATION.contains(canonical),
                "migration 150 must backfill to {canonical} (model {model}, flow {flow})"
            );
        }
        // Raw rows only, and idempotent on already-correct data.
        assert!(MIGRATION.contains("NOT is_rollup"));
        assert!(MIGRATION.contains("IS DISTINCT FROM"));
    }

    #[test]
    fn trace_canonicalizes_provider_from_model_for_every_path() {
        let buffer = RuntimeLlmTraceBuffer::new(8);
        let observer = RuntimeLlmObserver::new(buffer.clone(), None);
        // The memory extractor stamps the generic "aifarm", but the model is
        // owned by vram-cloud — the row must report the canonical provider.
        openplotva_llm::LlmCallObserver::observe(
            &observer,
            openplotva_llm::LlmCallRecord {
                context: openplotva_llm::LlmCallContext::default(),
                artifact: openplotva_dialog::DialogTraceArtifacts {
                    provider: "aifarm".to_owned(),
                    source: "aifarm_memory_extractor".to_owned(),
                    flow: "memory_extraction".to_owned(),
                    model: "vram.cloud/qwen3.6-35b-a3b".to_owned(),
                    request_kind: "openai.chat.completions".to_owned(),
                    ..openplotva_dialog::DialogTraceArtifacts::default()
                },
                duration_ms: 10,
                run: None,
            },
        );
        // An unmapped model keeps whatever the client stamped.
        openplotva_llm::LlmCallObserver::observe(
            &observer,
            openplotva_llm::LlmCallRecord {
                context: openplotva_llm::LlmCallContext::default(),
                artifact: openplotva_dialog::DialogTraceArtifacts {
                    provider: "genkit".to_owned(),
                    source: "genkit".to_owned(),
                    flow: "dialog".to_owned(),
                    model: "googleai/gemini-2.5-flash-lite".to_owned(),
                    request_kind: "gemini.generateContent".to_owned(),
                    ..openplotva_dialog::DialogTraceArtifacts::default()
                },
                duration_ms: 10,
                run: None,
            },
        );
        let rows = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 10,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("rows");
        let by_model = |needle: &str| {
            rows.iter()
                .find(|row| row.model.as_deref().is_some_and(|m| m.contains(needle)))
                .and_then(|row| row.provider.clone())
        };
        assert_eq!(by_model("vram.cloud").as_deref(), Some("vram-cloud"));
        assert_eq!(by_model("gemini").as_deref(), Some("genkit"));
    }

    #[test]
    fn runtime_llm_trace_buffer_lists_newest_and_filters_like_go() {
        let buffer = RuntimeLlmTraceBuffer::new(2);
        buffer.record(RuntimeLlmRequestData {
            provider: Some("aifarm".to_owned()),
            source: "dialog".to_owned(),
            model: Some("old".to_owned()),
            chat: RuntimeLlmRequestChatData {
                chat_id: 1,
                ..RuntimeLlmRequestChatData::default()
            },
            user: RuntimeLlmRequestUserData {
                user_id: 7,
                ..RuntimeLlmRequestUserData::default()
            },
            result: RuntimeLlmRequestResultData {
                response_text_preview: Some("old answer".to_owned()),
                ..RuntimeLlmRequestResultData::default()
            },
            ..RuntimeLlmRequestData::default()
        });
        buffer.record(RuntimeLlmRequestData {
            provider: Some("aifarm".to_owned()),
            source: "dialog".to_owned(),
            model: Some("new".to_owned()),
            raw_response: Some(json!({"content": "needle"})),
            chat: RuntimeLlmRequestChatData {
                chat_id: 2,
                ..RuntimeLlmRequestChatData::default()
            },
            user: RuntimeLlmRequestUserData {
                user_id: 7,
                ..RuntimeLlmRequestUserData::default()
            },
            ..RuntimeLlmRequestData::default()
        });

        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                q: "needle".to_owned(),
                source: "aifarm".to_owned(),
                user_id: Some(7),
                limit: 100,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list");

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].model.as_deref(), Some("new"));
        assert_eq!(traces[0].id, 2);

        buffer.record(RuntimeLlmRequestData {
            source: "dialog".to_owned(),
            ..RuntimeLlmRequestData::default()
        });
        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 100,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list after rerecord");
        assert_eq!(traces[0].id, 3);
    }

    #[test]
    fn runtime_llm_trace_buffer_prunes_virtual_chat_traces() {
        let buffer = RuntimeLlmTraceBuffer::new(4);
        for chat_id in [10, 20, 10] {
            buffer.record(RuntimeLlmRequestData {
                source: "dialog".to_owned(),
                chat: RuntimeLlmRequestChatData {
                    chat_id,
                    ..RuntimeLlmRequestChatData::default()
                },
                ..RuntimeLlmRequestData::default()
            });
        }

        assert_eq!(buffer.prune_chat(10), 2);
        let traces = buffer
            .llm_requests(RuntimeLlmRequestsFilter {
                limit: 100,
                ..RuntimeLlmRequestsFilter::default()
            })
            .expect("trace list after prune");

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].chat.chat_id, 20);
        assert_eq!(traces[0].id, 2);
        assert_eq!(buffer.prune_chat(10), 0);
    }

    #[test]
    fn llm_request_event_from_trace_matches_go_persistence_shape() {
        let trace = RuntimeLlmRequestData {
            at: "2026-06-03T12:00:00Z".to_owned(),
            provider: Some(" aifarm ".to_owned()),
            request_kind: Some("openai.chat.completions".to_owned()),
            source: "aifarm".to_owned(),
            flow: Some("dialog".to_owned()),
            iteration: 2,
            model: Some("model-a".to_owned()),
            chat: RuntimeLlmRequestChatData {
                chat_id: -100,
                thread_id: Some(5),
                ..RuntimeLlmRequestChatData::default()
            },
            user: RuntimeLlmRequestUserData {
                user_id: 7,
                ..RuntimeLlmRequestUserData::default()
            },
            message: RuntimeLlmRequestMessageData { message_id: 77 },
            gen_config: RuntimeLlmGenConfigData {
                max_output_tokens: 512,
                ..RuntimeLlmGenConfigData::default()
            },
            inference_params: Some(json!({
                "max_tokens": 768,
                "temperature": 0.7,
                "top_p": 0.9,
                "top_k": 40,
                "candidate_count": 2,
                "tool_mode": "auto",
                "response_format": "json_schema",
            })),
            usage: Some(json!({
                "input_tokens": 11,
                "output_tokens": 7,
                "total_tokens": 18,
                "cached_tokens": 3,
                "thoughts_tokens": 2,
                "tool_use_prompt_tokens": 1,
            })),
            timings: Some(json!({
                "prompt_eval_tokens": 11,
                "prompt_eval_ms": 40.5,
                "prompt_tps": 275.0,
                "generation_tokens": 7,
                "generation_ms": 350.0,
                "generation_tps": 20.0,
            })),
            prompt_chars: 123,
            prompt_messages: 4,
            docs_chars: 9,
            duration_ms: 400,
            result: RuntimeLlmRequestResultData {
                error: Some("rate limited".to_owned()),
                ..RuntimeLlmRequestResultData::default()
            },
            ..RuntimeLlmRequestData::default()
        };

        let event = LlmRequestEvent::from_trace(&trace, RawBodyPolicy::default());

        assert_eq!(
            event.created_at.format(&Rfc3339).unwrap_or_default(),
            "2026-06-03T12:00:00Z"
        );
        assert_eq!(event.provider.as_deref(), Some("aifarm"));
        assert_eq!(event.chat_id, Some(-100));
        assert_eq!(event.thread_id, Some(5));
        assert_eq!(event.message_id, Some(77));
        assert_eq!(event.user_id, Some(7));
        assert_eq!(event.iteration, 2);
        assert_eq!(event.prompt_chars, 123);
        assert_eq!(event.input_tokens, Some(11));
        assert_eq!(event.output_tokens, Some(7));
        assert_eq!(event.generation_tps, Some(20.0));
        assert_eq!(event.effective_output_tps, Some(17.5));
        assert_eq!(event.max_tokens, Some(768));
        assert_eq!(event.top_k, Some(40));
        assert_eq!(event.tool_mode.as_deref(), Some("auto"));
        assert_eq!(event.response_format.as_deref(), Some("json_schema"));
        assert_eq!(event.inference_params["temperature"], json!(0.7));
        assert_eq!(event.error.as_deref(), Some("rate limited"));
    }

    #[test]
    fn llm_request_event_batch_insert_sql_uses_one_multi_row_statement() {
        let first = RuntimeLlmRequestData {
            source: "dialog".to_owned(),
            model: Some("model-a".to_owned()),
            ..RuntimeLlmRequestData::default()
        };
        let second = RuntimeLlmRequestData {
            source: "dialog".to_owned(),
            model: Some("model-b".to_owned()),
            ..RuntimeLlmRequestData::default()
        };

        let sql = llm_request_event_batch_insert_sql_for_test(&[first, second]);
        assert!(sql.contains("run_id"));
        assert!(sql.contains("run_seq"));
        assert!(sql.contains("raw_request"));
        assert!(sql.contains("raw_response"));

        assert!(sql.starts_with("INSERT INTO llm_request_events"));
        assert!(sql.contains("), ("));
        assert!(!sql.contains("VALUES (\n    $1,"));
    }

    #[test]
    fn raw_bodies_persist_only_within_policy() {
        let body = json!({"messages": [{"role": "user", "content": "привет"}]});
        let trace = RuntimeLlmRequestData {
            source: "dialog".to_owned(),
            raw_request: Some(body.clone()),
            raw_response: Some(body.clone()),
            ..RuntimeLlmRequestData::default()
        };

        let event = LlmRequestEvent::from_trace(&trace, RawBodyPolicy::default());
        assert_eq!(event.raw_request.as_ref(), Some(&body));
        assert_eq!(event.raw_response.as_ref(), Some(&body));

        let disabled = LlmRequestEvent::from_trace(
            &trace,
            RawBodyPolicy {
                enabled: false,
                max_bytes: 65_536,
            },
        );
        assert_eq!(disabled.raw_request, None);
        assert_eq!(disabled.raw_response, None);

        let capped = LlmRequestEvent::from_trace(
            &trace,
            RawBodyPolicy {
                enabled: true,
                max_bytes: 8,
            },
        );
        assert_eq!(capped.raw_request, None, "oversized bodies degrade to NULL");
        assert_eq!(capped.raw_response, None);
    }

    #[test]
    fn raw_body_scrub_updates_only_aged_rows_with_bodies() {
        assert!(SQL_SCRUB_OLD_LLM_RAW_BODIES.contains("UPDATE llm_request_events"));
        assert!(!SQL_SCRUB_OLD_LLM_RAW_BODIES.contains("DELETE"));
        assert!(
            SQL_SCRUB_OLD_LLM_RAW_BODIES
                .contains("raw_request IS NOT NULL OR raw_response IS NOT NULL")
        );
        assert!(SQL_SCRUB_OLD_LLM_RAW_BODIES.contains("interval '1 hour'"));
        assert!(SQL_SCRUB_OLD_LLM_RAW_BODIES.contains("SET raw_request = NULL"));
    }

    #[test]
    fn raw_body_run_lookup_skips_rollups_and_empty_rows() {
        assert!(SQL_SELECT_LLM_RAW_BODIES_FOR_RUN.contains("NOT is_rollup"));
        assert!(SQL_SELECT_LLM_RAW_BODIES_FOR_RUN.contains("run_seq IS NOT NULL"));
        assert!(
            SQL_SELECT_LLM_RAW_BODIES_FOR_RUN
                .contains("raw_request IS NOT NULL OR raw_response IS NOT NULL")
        );
    }

    #[test]
    fn trace_ring_serves_raw_payloads_by_id() {
        let buffer = RuntimeLlmTraceBuffer::new(4);
        let id = buffer.record(RuntimeLlmRequestData {
            source: "dialog".to_owned(),
            raw_request: Some(json!({"q": 1})),
            ..RuntimeLlmRequestData::default()
        });
        let trace = buffer.get(id).expect("live trace by id");
        assert_eq!(trace.raw_request, Some(json!({"q": 1})));
        assert!(buffer.get(id + 100).is_none());
    }

    #[test]
    fn llm_request_event_cleanup_keeps_archived_rollups() {
        assert!(SQL_DELETE_OLD_LLM_REQUEST_EVENTS_BATCH.contains("NOT is_rollup"));
    }

    #[test]
    fn delete_old_memory_runs_targets_only_terminal_runs() {
        assert!(SQL_DELETE_OLD_MEMORY_RUNS.contains("status IN ('completed','skipped','failed')"));
        assert!(SQL_DELETE_OLD_MEMORY_RUNS.contains("range_start_at <"));
    }

    #[test]
    fn llm_request_event_shutdown_drain_flushes_all_queued_batches() {
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        for index in 0..5 {
            sender
                .try_send(RuntimeLlmRequestData {
                    source: format!("dialog-{index}"),
                    ..RuntimeLlmRequestData::default()
                })
                .expect("test channel should accept all queued traces");
        }
        drop(sender);

        let batch_sizes = llm_request_event_shutdown_batch_sizes_for_test(receiver, 2);

        assert_eq!(batch_sizes, vec![2, 2, 1]);
    }
}
