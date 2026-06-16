//! Discovery-backed embedder client with a shared, cooling circuit breaker.
//!
//! The embedder runs on the AI Farm GPU server and is reached only through
//! Discovery (`/v1/jobs/blocking` + poll), the same path the LLM uses. A
//! process-shared breaker stops hammering Discovery while the embedder is down:
//! after `failure_threshold` consecutive failures it opens for `cooldown`, and
//! every embed call short-circuits to [`EmbedderClientError::Unavailable`] until
//! the cooldown expires and a single probe is allowed through.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose};
use openplotva_llm::aifarm::{
    DiscoveryInvocation, DiscoveryJob, DiscoveryJobEnvelope, DiscoveryJobRequest,
    DiscoveryJobResponse,
};
use openplotva_storage::pg_embedding_vector;

use crate::memory_runtime::{
    EmbedderClientError, EmbedderEncodeRequest, EmbedderEncodeResponse, EmbeddingBatchFuture,
    EmbeddingProvider, EmbeddingProviderFuture,
};

pub const DEFAULT_EMBEDDER_BREAKER_FAILURE_THRESHOLD: usize = 5;
pub const DEFAULT_EMBEDDER_BREAKER_COOLDOWN: Duration = Duration::from_secs(300);
pub const DEFAULT_EMBEDDER_DISCOVERY_SERVICE_NAME: &str = "embedder";
pub const DEFAULT_EMBEDDER_DISCOVERY_ENDPOINT_NAME: &str = "encode";
const EMBEDDER_DISCOVERY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const EMBEDDER_DISCOVERY_CAPACITY_WAIT: Duration = Duration::from_secs(2);
const EMBEDDER_DISCOVERY_CONTENT_TYPE: &str = "application/json";

/// Process-shared, cooling circuit breaker for the embedder service.
///
/// State is kept in atomics so it can be read and updated across worker tasks
/// without holding a lock across `.await`.
#[derive(Debug)]
pub struct EmbedderCircuitBreaker {
    failure_threshold: usize,
    cooldown: Duration,
    consecutive_failures: AtomicUsize,
    open_until_unix_ms: AtomicI64,
}

impl EmbedderCircuitBreaker {
    #[must_use]
    pub fn new(failure_threshold: usize, cooldown: Duration) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            cooldown,
            consecutive_failures: AtomicUsize::new(0),
            open_until_unix_ms: AtomicI64::new(0),
        }
    }

    fn now_unix_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }

    /// Whether the embedder may be called now. While open, returns false without
    /// any network traffic; once the cooldown elapses, a probe is allowed.
    #[must_use]
    pub fn is_available(&self) -> bool {
        let open_until = self.open_until_unix_ms.load(Ordering::Relaxed);
        open_until == 0 || Self::now_unix_ms() >= open_until
    }

    /// Record a successful call: close the breaker and reset the failure streak.
    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.open_until_unix_ms.store(0, Ordering::Relaxed);
    }

    /// Record a failed call: open the breaker once the streak hits the threshold.
    pub fn record_failure(&self) {
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= self.failure_threshold {
            let cooldown_ms = i64::try_from(self.cooldown.as_millis()).unwrap_or(i64::MAX);
            self.open_until_unix_ms.store(
                Self::now_unix_ms().saturating_add(cooldown_ms),
                Ordering::Relaxed,
            );
        }
    }
}

static SHARED_BREAKER: OnceLock<Arc<EmbedderCircuitBreaker>> = OnceLock::new();

/// The single process-wide embedder breaker, shared by every embedder consumer
/// (memory retrieval, shield retrieval, and consolidation).
///
/// Thresholds may be tuned with `EMBEDDER_BREAKER_FAILURE_THRESHOLD` and
/// `EMBEDDER_BREAKER_COOLDOWN_SECONDS`; they are read once on first use.
pub fn shared_embedder_breaker() -> Arc<EmbedderCircuitBreaker> {
    SHARED_BREAKER
        .get_or_init(|| {
            let threshold = std::env::var("EMBEDDER_BREAKER_FAILURE_THRESHOLD")
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_EMBEDDER_BREAKER_FAILURE_THRESHOLD);
            let cooldown = std::env::var("EMBEDDER_BREAKER_COOLDOWN_SECONDS")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .filter(|value| *value > 0)
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_EMBEDDER_BREAKER_COOLDOWN);
            Arc::new(EmbedderCircuitBreaker::new(threshold, cooldown))
        })
        .clone()
}

/// Discovery-backed embedder configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryEmbedderConfig {
    /// Discovery API base URL (`DISCOVERY_BASE_URL`).
    pub base_url: String,
    /// Registered Discovery service name (default `embedder`).
    pub service_name: String,
    /// Registered Discovery endpoint name (default `encode`).
    pub endpoint_name: String,
    /// Per-HTTP-call timeout against Discovery.
    pub request_timeout: Duration,
    /// Upstream task timeout advertised to Discovery and the poll deadline.
    pub task_timeout: Duration,
    /// Poll cadence while waiting for a job result.
    pub poll_interval: Duration,
    /// How long Discovery may wait for a free slot before queueing.
    pub capacity_wait: Duration,
}

/// Embedder client that routes `/encode` calls through Discovery and is guarded
/// by the shared circuit breaker.
#[derive(Clone, Debug)]
pub struct DiscoveryEmbedderClient {
    cfg: DiscoveryEmbedderConfig,
    client: reqwest::Client,
    breaker: Arc<EmbedderCircuitBreaker>,
}

impl DiscoveryEmbedderClient {
    /// Build a Discovery embedder client sharing the process-wide breaker.
    pub fn new(cfg: DiscoveryEmbedderConfig) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout.max(Duration::from_secs(1)))
            .build()?;
        Ok(Self {
            cfg,
            client,
            breaker: shared_embedder_breaker(),
        })
    }

    /// Build a Discovery embedder for a service/endpoint with a latency budget,
    /// filling in the default poll cadence and capacity wait. Returns `None`
    /// when the discovery URL or service name is blank (embedder disabled).
    pub fn with_budget(
        base_url: &str,
        service_name: &str,
        endpoint_name: &str,
        budget: Duration,
    ) -> Result<Option<Self>, reqwest::Error> {
        let base_url = base_url.trim();
        let service_name = service_name.trim();
        if base_url.is_empty() || service_name.is_empty() {
            return Ok(None);
        }
        let endpoint = endpoint_name.trim();
        let endpoint = if endpoint.is_empty() {
            DEFAULT_EMBEDDER_DISCOVERY_ENDPOINT_NAME
        } else {
            endpoint
        };
        Self::new(DiscoveryEmbedderConfig {
            base_url: base_url.to_owned(),
            service_name: service_name.to_owned(),
            endpoint_name: endpoint.to_owned(),
            request_timeout: budget,
            task_timeout: budget,
            poll_interval: EMBEDDER_DISCOVERY_POLL_INTERVAL,
            capacity_wait: EMBEDDER_DISCOVERY_CAPACITY_WAIT,
        })
        .map(Some)
    }

    /// Access the resolved configuration.
    #[must_use]
    pub fn config(&self) -> &DiscoveryEmbedderConfig {
        &self.cfg
    }

    /// Access the shared breaker (used by the consolidation health gate).
    #[must_use]
    pub fn breaker(&self) -> &Arc<EmbedderCircuitBreaker> {
        &self.breaker
    }

    async fn encode(
        &self,
        request: &EmbedderEncodeRequest,
    ) -> Result<EmbedderEncodeResponse, EmbedderClientError> {
        if request.prompts.is_empty() {
            return Err(EmbedderClientError::EmptyPrompts);
        }
        if !self.breaker.is_available() {
            return Err(EmbedderClientError::Unavailable);
        }
        match self.encode_through_discovery(request).await {
            Ok(response) => {
                self.breaker.record_success();
                Ok(response)
            }
            Err(error) => {
                if error.is_availability_failure() {
                    self.breaker.record_failure();
                }
                Err(error)
            }
        }
    }

    async fn encode_through_discovery(
        &self,
        request: &EmbedderEncodeRequest,
    ) -> Result<EmbedderEncodeResponse, EmbedderClientError> {
        let payload = serde_json::to_vec(request)
            .map_err(|error| EmbedderClientError::discovery(format!("encode request: {error}")))?;
        let job_id = next_job_id();
        let job_request = DiscoveryJobRequest {
            invocation: DiscoveryInvocation {
                service_name: self.cfg.service_name.clone(),
                endpoint_name: self.cfg.endpoint_name.clone(),
                headers: BTreeMap::new(),
                query: BTreeMap::new(),
                body: general_purpose::STANDARD.encode(payload),
                content_type: EMBEDDER_DISCOVERY_CONTENT_TYPE.to_owned(),
                timeout_ms: duration_ms(self.cfg.task_timeout),
            },
            idempotency_key: job_id.clone(),
            priority: 0,
            wait_for_capacity_ms: duration_ms(self.cfg.capacity_wait),
            capacity_poll_ms: duration_ms(self.cfg.poll_interval),
        };

        let envelope = self.submit(&job_request).await?;
        let mut job = envelope.resolve_job();
        let resolved_id = first_non_empty(&job.resolved_id(), &job_id);
        let deadline = Instant::now() + self.cfg.task_timeout.max(Duration::from_secs(1));
        loop {
            if let Some(response) = terminal_response(&job)? {
                return decode_encode_response(&response);
            }
            if Instant::now() >= deadline {
                return Err(EmbedderClientError::discovery(format!(
                    "discovery job {resolved_id} did not complete within {:?}",
                    self.cfg.task_timeout
                )));
            }
            tokio::time::sleep(self.cfg.poll_interval).await;
            job = self.poll(&resolved_id).await?.resolve_job();
        }
    }

    async fn submit(
        &self,
        job: &DiscoveryJobRequest,
    ) -> Result<DiscoveryJobEnvelope, EmbedderClientError> {
        let url = self.endpoint("/v1/jobs/blocking");
        let response = self
            .client
            .post(url)
            .json(job)
            .send()
            .await
            .map_err(|error| EmbedderClientError::discovery(format!("submit: {error}")))?;
        self.read_envelope(response, "submit").await
    }

    async fn poll(&self, job_id: &str) -> Result<DiscoveryJobEnvelope, EmbedderClientError> {
        let url = self.endpoint(&format!("/v1/jobs/{}", job_id.trim()));
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|error| EmbedderClientError::discovery(format!("poll: {error}")))?;
        self.read_envelope(response, "poll").await
    }

    async fn read_envelope(
        &self,
        response: reqwest::Response,
        stage: &str,
    ) -> Result<DiscoveryJobEnvelope, EmbedderClientError> {
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| EmbedderClientError::discovery(format!("{stage} body: {error}")))?;
        if !status.is_success() {
            return Err(EmbedderClientError::discovery(format!(
                "{stage} status {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&bytes)
            )));
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| EmbedderClientError::discovery(format!("decode {stage}: {error}")))
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.cfg.base_url.trim_end_matches('/'), path)
    }
}

impl EmbeddingProvider for DiscoveryEmbedderClient {
    fn embed_one<'a>(
        &'a self,
        text: &'a str,
        dimension: i32,
        task_description: &'a str,
    ) -> EmbeddingProviderFuture<'a> {
        Box::pin(async move {
            let text = text.trim();
            if text.is_empty() {
                return Ok(None);
            }
            let response = self
                .encode(&EmbedderEncodeRequest {
                    prompts: vec![text.to_owned()],
                    dimension,
                    task_description: task_description.trim().to_owned(),
                })
                .await?;
            Ok(response.embeddings.into_iter().next().map(|values| {
                pg_embedding_vector(values.into_iter().map(|value| value as f32).collect())
            }))
        })
    }

    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
        dimension: i32,
        task_description: &'a str,
    ) -> EmbeddingBatchFuture<'a> {
        Box::pin(async move {
            let mut slots = Vec::new();
            let mut prompts = Vec::new();
            for (index, text) in texts.iter().enumerate() {
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    slots.push(index);
                    prompts.push(trimmed.to_owned());
                }
            }
            let mut out = vec![None; texts.len()];
            if prompts.is_empty() {
                return Ok(out);
            }
            let response = self
                .encode(&EmbedderEncodeRequest {
                    prompts,
                    dimension,
                    task_description: task_description.trim().to_owned(),
                })
                .await?;
            for (slot, values) in slots.into_iter().zip(response.embeddings) {
                out[slot] = Some(pg_embedding_vector(
                    values.into_iter().map(|value| value as f32).collect(),
                ));
            }
            Ok(out)
        })
    }

    fn is_available(&self) -> bool {
        self.breaker.is_available()
    }
}

fn terminal_response(
    job: &DiscoveryJob,
) -> Result<Option<DiscoveryJobResponse>, EmbedderClientError> {
    let state = job.resolved_status().to_ascii_uppercase();
    if state.contains("SUCC") {
        return match job
            .result
            .as_ref()
            .and_then(|result| result.response.clone())
        {
            Some(response) => Ok(Some(response)),
            None => Err(EmbedderClientError::discovery(
                "discovery job succeeded without a response payload".to_owned(),
            )),
        };
    }
    if state.contains("FAIL") || state.contains("CANCEL") {
        let detail = job
            .error
            .as_ref()
            .map(std::string::ToString::to_string)
            .unwrap_or_default();
        return Err(EmbedderClientError::discovery(format!(
            "discovery job {}: {detail}",
            state.to_ascii_lowercase()
        )));
    }
    Ok(None)
}

fn decode_encode_response(
    response: &DiscoveryJobResponse,
) -> Result<EmbedderEncodeResponse, EmbedderClientError> {
    if response.status_code != 0 && response.status_code != 200 {
        let body = general_purpose::STANDARD
            .decode(response.body.as_bytes())
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|| response.body.clone());
        return Err(EmbedderClientError::Status {
            status: response.status_code,
            detail: body,
        });
    }
    let raw = general_purpose::STANDARD
        .decode(response.body.as_bytes())
        .map_err(|error| {
            EmbedderClientError::discovery(format!("decode response body: {error}"))
        })?;
    serde_json::from_slice::<EmbedderEncodeResponse>(&raw)
        .map_err(|error| EmbedderClientError::discovery(format!("decode embeddings: {error}")))
}

fn first_non_empty(primary: &str, fallback: &str) -> String {
    if primary.trim().is_empty() {
        fallback.trim().to_owned()
    } else {
        primary.trim().to_owned()
    }
}

fn duration_ms(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

fn next_job_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    format!("embed-{nanos}-{counter}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_llm::aifarm::{DiscoveryJob, DiscoveryJobResult};

    /// Live round-trip against a reachable Discovery + embedder. Ignored by
    /// default; run with `EMBEDDER_E2E_DISCOVERY_URL=http://127.0.0.1:50051
    /// cargo test -p openplotva-app -- --ignored embed_roundtrip_through_discovery`
    /// (e.g. over an SSH tunnel to the AI Farm Discovery).
    #[tokio::test]
    #[ignore = "requires a reachable Discovery (set EMBEDDER_E2E_DISCOVERY_URL)"]
    async fn embed_roundtrip_through_discovery() {
        let base = std::env::var("EMBEDDER_E2E_DISCOVERY_URL")
            .expect("EMBEDDER_E2E_DISCOVERY_URL must be set");
        let client = DiscoveryEmbedderClient::with_budget(
            &base,
            "embedder",
            "encode",
            Duration::from_secs(15),
        )
        .expect("build client")
        .expect("client configured");
        let vector = client
            .embed_one("rust end-to-end embedding check", 512, "retrieval")
            .await
            .expect("embed succeeds")
            .expect("embedding present");
        assert_eq!(vector.as_slice().len(), 512);
        assert!(client.breaker().is_available());
    }

    fn succeeded_job(status_code: u16, body_json: &str) -> DiscoveryJob {
        DiscoveryJob {
            state: "JOB_STATE_SUCCEEDED".to_owned(),
            result: Some(DiscoveryJobResult {
                response: Some(DiscoveryJobResponse {
                    status_code,
                    body: general_purpose::STANDARD.encode(body_json),
                    content_type: "application/json".to_owned(),
                }),
            }),
            ..DiscoveryJob::default()
        }
    }

    #[test]
    fn breaker_opens_after_threshold_and_resets_on_success() {
        let breaker = EmbedderCircuitBreaker::new(3, Duration::from_secs(300));
        assert!(breaker.is_available());
        breaker.record_failure();
        breaker.record_failure();
        assert!(breaker.is_available(), "two failures stay closed");
        breaker.record_failure();
        assert!(!breaker.is_available(), "third failure opens the breaker");
        breaker.record_success();
        assert!(breaker.is_available(), "success closes the breaker");
    }

    #[test]
    fn breaker_reopens_on_single_failure_after_streak() {
        let breaker = EmbedderCircuitBreaker::new(2, Duration::from_millis(40));
        breaker.record_failure();
        breaker.record_failure();
        assert!(!breaker.is_available());
        std::thread::sleep(Duration::from_millis(60));
        assert!(breaker.is_available(), "cooldown elapsed; probe allowed");
        // A failed probe re-opens immediately (streak is still past threshold).
        breaker.record_failure();
        assert!(!breaker.is_available());
    }

    #[test]
    fn decode_reads_normalized_embeddings() {
        let job = succeeded_job(200, r#"{"embeddings":[[0.5,0.5]],"dimension":2,"count":1}"#);
        let response = terminal_response(&job)
            .expect("no error")
            .expect("terminal response present");
        let decoded = decode_encode_response(&response).expect("decode");
        assert_eq!(decoded.count, 1);
        assert_eq!(decoded.dimension, 2);
        assert_eq!(decoded.embeddings, vec![vec![0.5, 0.5]]);
    }

    #[test]
    fn decode_maps_upstream_5xx_to_status_error() {
        let job = succeeded_job(503, r#"{"detail":"model loading"}"#);
        let response = terminal_response(&job).expect("no error").expect("present");
        let error = decode_encode_response(&response).expect_err("upstream 503");
        assert!(
            error.is_availability_failure(),
            "5xx is an availability failure"
        );
    }

    #[test]
    fn failed_discovery_state_is_an_availability_failure() {
        let job = DiscoveryJob {
            state: "JOB_STATE_FAILED".to_owned(),
            error: Some(serde_json::json!({"code": "UPSTREAM_TIMEOUT"})),
            ..DiscoveryJob::default()
        };
        let error = terminal_response(&job).expect_err("failed job is an error");
        assert!(error.is_availability_failure());
    }

    #[test]
    fn running_job_is_not_terminal() {
        let job = DiscoveryJob {
            state: "JOB_STATE_RUNNING".to_owned(),
            ..DiscoveryJob::default()
        };
        assert!(terminal_response(&job).expect("no error").is_none());
    }
}
