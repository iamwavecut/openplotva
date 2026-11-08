//! Discovery transport for privacy-filter redaction.

use crate::aifarm::DISCOVERY_PRIORITY_MEMORY;
use base64::{Engine as _, engine::general_purpose};
use openplotva_memory::{TextRedactor, TextRedactorFuture, default_memory_redaction_categories};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashMap,
    time::{Duration as StdDuration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use url::Url;

pub const DEFAULT_DISCOVERY_BASE_URL: &str = "http://127.0.0.1:50051";
pub const DEFAULT_MEMORY_REDACTION_SERVICE_NAME: &str = "privacy-filter";
pub const DEFAULT_MEMORY_REDACTION_ENDPOINT_NAME: &str = "redact";
pub const DEFAULT_MEMORY_REDACTION_TIMEOUT: StdDuration = StdDuration::from_secs(35);
pub const DEFAULT_MEMORY_REDACTION_POLL_INTERVAL: StdDuration = StdDuration::from_secs(1);
pub const DEFAULT_MEMORY_REDACTION_CAPACITY_WAIT: StdDuration = StdDuration::from_secs(30);
/// Discovery caps `/v1/jobs/sync` and long-poll result waits at two minutes.
const DISCOVERY_MAX_RESULT_WAIT: StdDuration = StdDuration::from_secs(120);
/// A long-poll answer faster than this means the server ignored `wait_ms`.
const DISCOVERY_LONG_POLL_MIN_HOLD: StdDuration = StdDuration::from_millis(50);

/// Memory redaction transport error.
#[derive(Debug, Error)]
pub enum DiscoveryRedactorError {
    /// Request body JSON serialization failed.
    #[error("marshal memory redaction request: {0}")]
    RequestBody(serde_json::Error),
    /// Job body JSON serialization failed.
    #[error("marshal memory redaction job: {0}")]
    JobBody(serde_json::Error),
    /// HTTP request failed.
    #[error("{phase} memory redaction job: {source}")]
    Request {
        /// Request phase.
        phase: &'static str,
        /// Source error.
        #[source]
        source: reqwest::Error,
    },
    /// Discovery returned non-2xx.
    #[error("{phase} memory redaction job: status {status}: {body}")]
    Status {
        /// Request phase.
        phase: &'static str,
        /// HTTP status.
        status: u16,
        /// Trimmed response body.
        body: String,
    },
    /// Discovery response JSON decode failed.
    #[error("decode memory redaction {phase} response: {source}")]
    DecodeEnvelope {
        /// Request phase.
        phase: &'static str,
        /// Decode error.
        #[source]
        source: reqwest::Error,
    },
    /// Waiting timed out.
    #[error("waiting for memory redaction job {job_id}: task timeout")]
    Timeout {
        /// Job ID.
        job_id: String,
    },
    /// Job failed.
    #[error("memory redaction job failed: {0}")]
    JobFailed(String),
    /// Job failed without message.
    #[error("memory redaction job failed with status {0:?}")]
    JobFailedStatus(String),
    /// Unknown terminal status.
    #[error("unknown memory redaction job status {0:?}")]
    UnknownStatus(String),
    /// Success lacked a response.
    #[error("memory redaction job succeeded without response")]
    MissingResponse,
    /// Upstream redactor returned non-2xx.
    #[error("memory redaction upstream returned status {0}")]
    UpstreamStatus(u16),
    /// Redaction payload JSON decode failed.
    #[error("decode memory redaction payload: {0}")]
    DecodePayload(serde_json::Error),
    /// Upstream did not return redacted text.
    #[error("memory redaction response has no redacted_text")]
    EmptyRedactedText,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryRedactorConfig {
    /// Discovery base URL.
    pub base_url: String,
    /// Discovery service name.
    pub service_name: String,
    /// Discovery endpoint name.
    pub endpoint_name: String,
    /// HTTP timeout.
    pub timeout: StdDuration,
    /// Discovery task timeout.
    pub task_timeout: StdDuration,
    /// Poll interval.
    pub poll_interval: StdDuration,
    /// Capacity wait.
    pub capacity_wait: StdDuration,
    /// Capacity poll interval.
    pub capacity_poll_interval: StdDuration,
    /// Redaction categories.
    pub categories: Vec<String>,
}

impl Default for DiscoveryRedactorConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_DISCOVERY_BASE_URL.to_owned(),
            service_name: DEFAULT_MEMORY_REDACTION_SERVICE_NAME.to_owned(),
            endpoint_name: DEFAULT_MEMORY_REDACTION_ENDPOINT_NAME.to_owned(),
            timeout: DEFAULT_MEMORY_REDACTION_TIMEOUT,
            task_timeout: DEFAULT_MEMORY_REDACTION_TIMEOUT,
            poll_interval: DEFAULT_MEMORY_REDACTION_POLL_INTERVAL,
            capacity_wait: DEFAULT_MEMORY_REDACTION_CAPACITY_WAIT,
            capacity_poll_interval: DEFAULT_MEMORY_REDACTION_POLL_INTERVAL,
            categories: default_memory_redaction_categories(),
        }
    }
}

impl DiscoveryRedactorConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.base_url = default_trimmed_string(&self.base_url, DEFAULT_DISCOVERY_BASE_URL);
        self.service_name =
            default_trimmed_string(&self.service_name, DEFAULT_MEMORY_REDACTION_SERVICE_NAME);
        self.endpoint_name =
            default_trimmed_string(&self.endpoint_name, DEFAULT_MEMORY_REDACTION_ENDPOINT_NAME);
        self.timeout = default_duration(self.timeout, DEFAULT_MEMORY_REDACTION_TIMEOUT);
        self.task_timeout = default_duration(self.task_timeout, DEFAULT_MEMORY_REDACTION_TIMEOUT);
        self.poll_interval =
            default_duration(self.poll_interval, DEFAULT_MEMORY_REDACTION_POLL_INTERVAL);
        self.capacity_wait =
            default_duration(self.capacity_wait, DEFAULT_MEMORY_REDACTION_CAPACITY_WAIT);
        self.capacity_poll_interval = default_duration(
            self.capacity_poll_interval,
            DEFAULT_MEMORY_REDACTION_POLL_INTERVAL,
        );
        if self.categories.is_empty() {
            self.categories = default_memory_redaction_categories();
        }
        self
    }
}

#[derive(Debug)]
pub struct DiscoveryRedactor {
    base_url: String,
    service_name: String,
    endpoint_name: String,
    task_timeout: StdDuration,
    poll_interval: StdDuration,
    capacity_wait: StdDuration,
    capacity_poll_interval: StdDuration,
    timeout: StdDuration,
    categories: Vec<String>,
    client: reqwest::Client,
}

impl DiscoveryRedactor {
    /// Build a reqwest-backed Discovery redactor.
    pub fn new(cfg: DiscoveryRedactorConfig) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder().build()?;
        Ok(Self::with_client(cfg, client))
    }

    /// Build a redactor on a shared HTTP client so consecutive calls reuse
    /// pooled keep-alive connections instead of paying a new handshake each.
    #[must_use]
    pub fn with_client(cfg: DiscoveryRedactorConfig, client: reqwest::Client) -> Self {
        let cfg = cfg.with_defaults();
        Self {
            base_url: cfg.base_url.trim().trim_end_matches('/').to_owned(),
            service_name: cfg.service_name.trim().to_owned(),
            endpoint_name: cfg.endpoint_name.trim().to_owned(),
            task_timeout: cfg.task_timeout,
            poll_interval: cfg.poll_interval,
            capacity_wait: cfg.capacity_wait,
            capacity_poll_interval: cfg.capacity_poll_interval,
            timeout: cfg.timeout,
            categories: cfg.categories,
            client,
        }
    }

    pub async fn redact_text(&self, text: &str) -> Result<String, DiscoveryRedactorError> {
        self.redact_text_with_optional_mode(text, None).await
    }

    pub async fn redact_text_with_mode(
        &self,
        text: &str,
        replacement_mode: RedactionReplacementMode,
    ) -> Result<String, DiscoveryRedactorError> {
        self.redact_text_with_optional_mode(text, Some(replacement_mode))
            .await
    }

    async fn redact_text_with_optional_mode(
        &self,
        text: &str,
        replacement_mode: Option<RedactionReplacementMode>,
    ) -> Result<String, DiscoveryRedactorError> {
        let request_id = memory_redact_id();
        let job_id = memory_redact_id();
        let (body, mut job_id) = self.redaction_job_body_with_ids_and_optional_mode(
            text,
            &request_id,
            &job_id,
            replacement_mode,
        )?;
        let envelope = self.submit_redaction_job(body).await?;
        if let Some(resolved) = envelope.resolved_job_id() {
            job_id = resolved;
        }
        match redaction_result_from_envelope(&envelope, false)? {
            RedactionEnvelopeResult::Done(result) => Ok(result),
            RedactionEnvelopeResult::Pending => self.poll_result(&job_id).await,
        }
    }

    pub fn redaction_job_body(
        &self,
        text: &str,
    ) -> Result<(Vec<u8>, String), DiscoveryRedactorError> {
        let request_id = memory_redact_id();
        let job_id = memory_redact_id();
        self.redaction_job_body_with_ids(text, &request_id, &job_id)
    }

    pub fn redaction_job_body_with_ids(
        &self,
        text: &str,
        request_id: &str,
        job_id: &str,
    ) -> Result<(Vec<u8>, String), DiscoveryRedactorError> {
        self.redaction_job_body_with_ids_and_optional_mode(text, request_id, job_id, None)
    }

    pub fn redaction_job_body_with_ids_and_mode(
        &self,
        text: &str,
        request_id: &str,
        job_id: &str,
        replacement_mode: RedactionReplacementMode,
    ) -> Result<(Vec<u8>, String), DiscoveryRedactorError> {
        self.redaction_job_body_with_ids_and_optional_mode(
            text,
            request_id,
            job_id,
            Some(replacement_mode),
        )
    }

    fn redaction_job_body_with_ids_and_optional_mode(
        &self,
        text: &str,
        request_id: &str,
        job_id: &str,
        replacement_mode: Option<RedactionReplacementMode>,
    ) -> Result<(Vec<u8>, String), DiscoveryRedactorError> {
        let payload = serde_json::to_vec(&RedactionRequest {
            request_id: request_id.to_owned(),
            text: text.to_owned(),
            categories: self.categories.clone(),
            replacement_mode,
        })
        .map_err(DiscoveryRedactorError::RequestBody)?;

        let job_id = job_id.to_owned();
        let job_req = DiscoveryRedactionJobRequest {
            invocation: DiscoveryRedactionInvocation {
                service_name: self.service_name.clone(),
                endpoint_name: self.endpoint_name.clone(),
                body: general_purpose::STANDARD.encode(payload),
                content_type: "application/json".to_owned(),
                timeout_ms: duration_ms(self.task_timeout),
                ..DiscoveryRedactionInvocation::default()
            },
            idempotency_key: job_id.clone(),
            priority: DISCOVERY_PRIORITY_MEMORY,
            wait_for_capacity_ms: duration_ms(self.capacity_wait),
            capacity_poll_ms: duration_ms(self.capacity_poll_interval),
            wait_for_result_ms: duration_ms(self.result_wait()),
        };
        let body = serde_json::to_vec(&job_req).map_err(DiscoveryRedactorError::JobBody)?;
        Ok((body, job_id))
    }

    async fn submit_redaction_job(
        &self,
        body: Vec<u8>,
    ) -> Result<DiscoveryRedactionEnvelope, DiscoveryRedactorError> {
        let response = self
            .client
            .post(self.endpoint("/v1/jobs/sync"))
            .header("Content-Type", "application/json")
            .timeout(
                self.capacity_wait
                    .saturating_add(self.result_wait())
                    .saturating_add(self.timeout),
            )
            .body(body)
            .send()
            .await
            .map_err(|source| DiscoveryRedactorError::Request {
                phase: "submit",
                source,
            })?;
        decode_redaction_envelope_response(response, "submit").await
    }

    async fn poll_result(&self, job_id: &str) -> Result<String, DiscoveryRedactorError> {
        let deadline = Instant::now() + self.task_timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(DiscoveryRedactorError::Timeout {
                    job_id: job_id.to_owned(),
                });
            }
            let wait = deadline
                .saturating_duration_since(Instant::now())
                .min(DISCOVERY_MAX_RESULT_WAIT);
            let asked = Instant::now();
            let envelope = self.fetch_job(job_id, wait).await?;
            match redaction_result_from_envelope(&envelope, true)? {
                RedactionEnvelopeResult::Done(result) => return Ok(result),
                RedactionEnvelopeResult::Pending => {
                    if asked.elapsed() < DISCOVERY_LONG_POLL_MIN_HOLD {
                        // An older Discovery ignores `wait_ms`; fall back to interval polling.
                        tokio::time::sleep(self.poll_interval).await;
                    }
                }
            }
        }
    }

    fn result_wait(&self) -> StdDuration {
        self.task_timeout.min(DISCOVERY_MAX_RESULT_WAIT)
    }

    async fn fetch_job(
        &self,
        job_id: &str,
        wait: StdDuration,
    ) -> Result<DiscoveryRedactionEnvelope, DiscoveryRedactorError> {
        let response = self
            .client
            .get(self.endpoint(&format!(
                "/v1/jobs/{}?wait_ms={}",
                job_id.trim(),
                wait.as_millis()
            )))
            .timeout(wait.saturating_add(self.timeout))
            .send()
            .await
            .map_err(|source| DiscoveryRedactorError::Request {
                phase: "check",
                source,
            })?;
        decode_redaction_envelope_response(response, "check").await
    }

    fn endpoint(&self, path: &str) -> String {
        endpoint_join(&self.base_url, path)
    }
}

impl TextRedactor for DiscoveryRedactor {
    type Error = DiscoveryRedactorError;

    fn redact_text<'a>(&'a self, text: String) -> TextRedactorFuture<'a, Self::Error> {
        Box::pin(async move { DiscoveryRedactor::redact_text(self, &text).await })
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RedactionRequest {
    /// Request ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_id: String,
    /// Text to redact.
    pub text: String,
    /// Redaction categories.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub categories: Vec<String>,
    /// Redaction replacement style. Omitted requests preserve the legacy ellipsis output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement_mode: Option<RedactionReplacementMode>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RedactionReplacementMode {
    Ellipsis,
    TypedPlaceholders,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RedactionResponse {
    /// Redacted text.
    pub redacted_text: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRedactionJobRequest {
    /// Discovery invocation.
    pub invocation: DiscoveryRedactionInvocation,
    /// Idempotency key.
    pub idempotency_key: String,
    /// Priority.
    pub priority: i32,
    /// Wait-for-capacity milliseconds.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub wait_for_capacity_ms: i32,
    /// Capacity-poll milliseconds.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub capacity_poll_ms: i32,
    /// How long `/v1/jobs/sync` holds the submit open for the result.
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub wait_for_result_ms: i32,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRedactionInvocation {
    /// Discovery service name.
    pub service_name: String,
    /// Discovery endpoint name.
    pub endpoint_name: String,
    /// Headers.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Query values.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub query: HashMap<String, String>,
    /// Base64 body.
    pub body: String,
    /// Content type.
    pub content_type: String,
    /// Timeout milliseconds.
    pub timeout_ms: i32,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DiscoveryRedactionEnvelope {
    /// Nested job payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<DiscoveryRedactionJob>,
    /// Top-level ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Top-level job ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub job_id: String,
    /// Top-level status.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    /// Top-level state.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state: String,
    /// Top-level error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    /// Top-level result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<DiscoveryRedactionResult>,
}

impl DiscoveryRedactionEnvelope {
    fn normalized(self) -> Self {
        if let Some(job) = &self.job {
            Self {
                id: job.id.clone(),
                job_id: job.job_id.clone(),
                status: job.status.clone(),
                state: job.state.clone(),
                error: job.error.clone(),
                result: job.result.clone(),
                job: self.job.clone(),
            }
        } else {
            self
        }
    }

    fn resolved_job_id(&self) -> Option<String> {
        if let Some(job) = &self.job {
            let job_id = job.job_id.trim();
            if !job_id.is_empty() {
                return Some(job_id.to_owned());
            }
            let id = job.id.trim();
            return (!id.is_empty()).then(|| id.to_owned());
        }
        let job_id = self.job_id.trim();
        if !job_id.is_empty() {
            return Some(job_id.to_owned());
        }
        let id = self.id.trim();
        (!id.is_empty()).then(|| id.to_owned())
    }

    fn resolved_status(&self) -> String {
        if let Some(job) = &self.job {
            let state = job.state.trim();
            if !state.is_empty() {
                return state.to_owned();
            }
            return job.status.trim().to_owned();
        }
        let state = self.state.trim();
        if !state.is_empty() {
            return state.to_owned();
        }
        self.status.trim().to_owned()
    }

    fn error_message(&self) -> String {
        if let Some(result) = &self.result
            && let Some(error) = &result.error
        {
            return error.message.clone();
        }
        let raw = self
            .job
            .as_ref()
            .and_then(|job| job.error.as_ref())
            .or(self.error.as_ref());
        let Some(raw) = raw else {
            return String::new();
        };
        let parsed = serde_json::from_value::<DiscoveryRedactionErrorPayload>(raw.clone());
        if let Ok(payload) = parsed {
            return [payload.message, payload.detail, payload.error]
                .join(" ")
                .trim()
                .to_owned();
        }
        raw.to_string()
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DiscoveryRedactionJob {
    /// Job ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Job ID alias.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub job_id: String,
    /// Status.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
    /// State.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub state: String,
    /// Error payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    /// Result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<DiscoveryRedactionResult>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRedactionResult {
    /// HTTP response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<DiscoveryRedactionHttpResponse>,
    /// Discovery error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<DiscoveryRedactionResultError>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRedactionHttpResponse {
    /// Status code.
    #[serde(default, skip_serializing_if = "is_zero_u16")]
    pub status_code: u16,
    /// Body.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub body: String,
    /// Content type.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content_type: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DiscoveryRedactionResultError {
    /// Error code.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    /// Error message.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
struct DiscoveryRedactionErrorPayload {
    #[serde(default)]
    message: String,
    #[serde(default)]
    detail: String,
    #[serde(default)]
    error: String,
}

enum RedactionEnvelopeResult {
    Pending,
    Done(String),
}

async fn decode_redaction_envelope_response(
    response: reqwest::Response,
    phase: &'static str,
) -> Result<DiscoveryRedactionEnvelope, DiscoveryRedactorError> {
    let status = response.status();
    if !status.is_success() {
        let status_code = status.as_u16();
        let body = response.text().await.unwrap_or_default().trim().to_owned();
        return Err(DiscoveryRedactorError::Status {
            phase,
            status: status_code,
            body,
        });
    }
    response
        .json::<DiscoveryRedactionEnvelope>()
        .await
        .map(DiscoveryRedactionEnvelope::normalized)
        .map_err(|source| DiscoveryRedactorError::DecodeEnvelope { phase, source })
}

fn redaction_result_from_envelope(
    envelope: &DiscoveryRedactionEnvelope,
    unknown_is_error: bool,
) -> Result<RedactionEnvelopeResult, DiscoveryRedactorError> {
    match normalize_discovery_status(&envelope.resolved_status()).as_str() {
        "queued" | "running" => return Ok(RedactionEnvelopeResult::Pending),
        "succeeded" => {
            return decode_redaction_result(envelope.result.as_ref())
                .map(RedactionEnvelopeResult::Done);
        }
        "failed" | "canceled" | "cancelled" => return Err(redaction_job_failed_error(envelope)),
        _ => {}
    }
    if envelope
        .result
        .as_ref()
        .and_then(|result| result.response.as_ref())
        .is_some()
    {
        return decode_redaction_result(envelope.result.as_ref())
            .map(RedactionEnvelopeResult::Done);
    }
    if unknown_is_error {
        return Err(DiscoveryRedactorError::UnknownStatus(
            envelope.resolved_status(),
        ));
    }
    Ok(RedactionEnvelopeResult::Pending)
}

fn redaction_job_failed_error(envelope: &DiscoveryRedactionEnvelope) -> DiscoveryRedactorError {
    let message = envelope.error_message();
    if message.trim().is_empty() {
        DiscoveryRedactorError::JobFailedStatus(envelope.resolved_status())
    } else {
        DiscoveryRedactorError::JobFailed(message)
    }
}

fn decode_redaction_result(
    result: Option<&DiscoveryRedactionResult>,
) -> Result<String, DiscoveryRedactorError> {
    let response = result
        .and_then(|result| result.response.as_ref())
        .ok_or(DiscoveryRedactorError::MissingResponse)?;
    if !(200..300).contains(&response.status_code) {
        return Err(DiscoveryRedactorError::UpstreamStatus(response.status_code));
    }
    let body = decode_discovery_body(&response.body);
    let parsed = serde_json::from_slice::<RedactionResponse>(&body)
        .map_err(DiscoveryRedactorError::DecodePayload)?;
    if parsed.redacted_text.is_empty() {
        return Err(DiscoveryRedactorError::EmptyRedactedText);
    }
    Ok(parsed.redacted_text)
}

fn decode_discovery_body(value: &str) -> Vec<u8> {
    let value = value.trim();
    if value.is_empty() {
        return Vec::new();
    }
    if value.starts_with('{') || value.starts_with('[') {
        return value.as_bytes().to_vec();
    }
    for encoding in [
        &general_purpose::STANDARD,
        &general_purpose::STANDARD_NO_PAD,
        &general_purpose::URL_SAFE,
        &general_purpose::URL_SAFE_NO_PAD,
    ] {
        if let Ok(decoded) = encoding.decode(value) {
            return decoded;
        }
    }
    value.as_bytes().to_vec()
}

fn normalize_discovery_status(value: &str) -> String {
    let mut value = value.trim();
    if value.len() >= "job_state_".len()
        && value[.."job_state_".len()].eq_ignore_ascii_case("job_state_")
    {
        value = &value["job_state_".len()..];
    }
    for known in [
        "queued",
        "running",
        "succeeded",
        "failed",
        "canceled",
        "cancelled",
    ] {
        if value.eq_ignore_ascii_case(known) {
            return known.to_owned();
        }
    }
    value.to_lowercase()
}

fn endpoint_join(base_url: &str, path: &str) -> String {
    let path = clean_url_path(path);
    let Ok(mut base) = Url::parse(base_url) else {
        return format!("{}{}", base_url.trim_end_matches('/'), path);
    };
    let joined_path = join_url_paths(base.path(), &path);
    base.set_path(&joined_path);
    base.to_string()
}

fn join_url_paths(base_path: &str, path: &str) -> String {
    let mut parts = Vec::new();
    for part in base_path.split('/').chain(path.split('/')) {
        match part {
            "" | "." => {}
            ".." => {
                let _ = parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        "/".to_owned()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn clean_url_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("/{path}")
    }
}

fn default_duration(value: StdDuration, fallback: StdDuration) -> StdDuration {
    if value.is_zero() { fallback } else { value }
}

fn default_trimmed_string(value: &str, fallback: &str) -> String {
    if value.trim().is_empty() {
        fallback.to_owned()
    } else {
        value.to_owned()
    }
}

fn duration_ms(value: StdDuration) -> i32 {
    if value.is_zero() {
        0
    } else {
        i32::try_from(value.as_millis()).unwrap_or(i32::MAX)
    }
}

fn memory_redact_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("memory-redact-{nanos}")
}

const fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}
const fn is_zero_u16(value: &u16) -> bool {
    *value == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Debug)]
    struct RecordedRequest {
        line: String,
        body: Value,
    }

    async fn run_redactor_fixture(
        replies: Vec<(u16, Value)>,
    ) -> (Result<String, DiscoveryRedactorError>, Vec<RecordedRequest>) {
        use std::io::{BufRead as _, Read as _, Write as _};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("fixture listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking listener");
        let address = listener.local_addr().expect("fixture address");
        let server = std::thread::spawn(move || -> Result<Vec<RecordedRequest>, String> {
            let deadline = Instant::now() + StdDuration::from_secs(10);
            let mut seen = Vec::new();
            for (status, body) in replies {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= deadline {
                                return Err("fixture request did not arrive".into());
                            }
                            std::thread::sleep(StdDuration::from_millis(1));
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                };
                stream
                    .set_nonblocking(false)
                    .map_err(|error| error.to_string())?;
                stream
                    .set_read_timeout(Some(StdDuration::from_secs(2)))
                    .map_err(|error| error.to_string())?;
                stream
                    .set_write_timeout(Some(StdDuration::from_secs(2)))
                    .map_err(|error| error.to_string())?;
                let mut reader = std::io::BufReader::new(&mut stream);
                let mut line = String::new();
                reader
                    .read_line(&mut line)
                    .map_err(|error| error.to_string())?;
                let mut length = 0_usize;
                loop {
                    let mut header = String::new();
                    if reader
                        .read_line(&mut header)
                        .map_err(|error| error.to_string())?
                        == 0
                    {
                        return Err("request headers ended early".into());
                    }
                    if header == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = header.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        length = value
                            .trim()
                            .parse::<usize>()
                            .map_err(|error| error.to_string())?;
                    }
                }
                let mut bytes = vec![0; length];
                reader
                    .read_exact(&mut bytes)
                    .map_err(|error| error.to_string())?;
                drop(reader);
                seen.push(RecordedRequest {
                    line: line.trim().to_owned(),
                    body: if bytes.is_empty() {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?
                    },
                });
                let body = body.to_string();
                write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).map_err(|error| error.to_string())?;
                stream.flush().map_err(|error| error.to_string())?;
            }
            Ok(seen)
        });
        let redactor = DiscoveryRedactor::new(DiscoveryRedactorConfig {
            base_url: format!("http://{address}/root"),
            timeout: StdDuration::from_secs(2),
            task_timeout: StdDuration::from_secs(2),
            poll_interval: StdDuration::from_millis(1),
            ..Default::default()
        })
        .expect("redactor");
        let result = redactor.redact_text("alice@example.test").await;
        let seen = server
            .join()
            .expect("fixture thread")
            .expect("fixture requests");
        (result, seen)
    }

    fn redaction_success() -> Value {
        serde_json::json!({"job":{"job_id":"server-job", "state":"JOB_STATE_SUCCEEDED", "result":{"response":{"status_code":200,"body":general_purpose::STANDARD.encode(r#"{"redacted_text":"[redacted]"}"#)}}}})
    }

    #[tokio::test]
    async fn sync_success_completes_without_polling() {
        let (result, seen) = run_redactor_fixture(vec![(200, redaction_success())]).await;
        assert_eq!(result.expect("redacted text"), "[redacted]");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].line, "POST /root/v1/jobs/sync HTTP/1.1");
        assert_eq!(
            seen[0].body["invocation"]["service_name"],
            DEFAULT_MEMORY_REDACTION_SERVICE_NAME
        );
        assert!(
            !seen[0].body["idempotency_key"]
                .as_str()
                .expect("job key")
                .is_empty()
        );
        let payload = general_purpose::STANDARD
            .decode(seen[0].body["invocation"]["body"].as_str().expect("body"))
            .expect("payload");
        let payload: Value = serde_json::from_slice(&payload).expect("request");
        assert_eq!(payload["text"], "alice@example.test");
    }

    #[tokio::test]
    async fn queued_and_running_poll_the_resolved_job_without_resubmission() {
        let (result, seen) = run_redactor_fixture(vec![
            (
                200,
                serde_json::json!({"job":{"job_id":"server-job", "state":"JOB_STATE_QUEUED"}}),
            ),
            (
                200,
                serde_json::json!({"job":{"job_id":"server-job", "state":"JOB_STATE_RUNNING"}}),
            ),
            (200, redaction_success()),
        ])
        .await;
        assert_eq!(result.expect("redacted text"), "[redacted]");
        assert_eq!(seen.len(), 3);
        assert_eq!(
            seen.iter()
                .filter(|request| request.line.starts_with("POST "))
                .count(),
            1
        );
        for request in &seen[1..] {
            assert!(
                request.line.starts_with("GET /root/v1/jobs/server-job"),
                "{}",
                request.line
            );
        }
    }

    #[tokio::test]
    async fn polling_surfaces_terminal_job_failure_without_resubmission() {
        let (result, seen) = run_redactor_fixture(vec![
            (
                200,
                serde_json::json!({"job_id":"server-job", "state":"queued"}),
            ),
            (
                200,
                serde_json::json!({"state":"failed", "error":{"message":"cannot redact"}}),
            ),
        ])
        .await;
        assert!(
            matches!(result, Err(DiscoveryRedactorError::JobFailed(message)) if message == "cannot redact")
        );
        assert_eq!(seen.len(), 2);
    }

    #[tokio::test]
    async fn submission_and_poll_rate_limits_keep_their_error_phase() {
        for (replies, phase, request_count) in [
            (
                vec![(429, serde_json::json!({"error":"busy"}))],
                "submit",
                1,
            ),
            (
                vec![
                    (
                        200,
                        serde_json::json!({"job_id":"server-job", "state":"queued"}),
                    ),
                    (429, serde_json::json!({"error":"busy"})),
                ],
                "check",
                2,
            ),
        ] {
            let (result, seen) = run_redactor_fixture(replies).await;
            assert!(
                matches!(result, Err(DiscoveryRedactorError::Status {phase: actual, status: 429, body}) if actual == phase && body.contains("busy"))
            );
            assert_eq!(seen.len(), request_count);
        }
    }

    #[test]
    fn discovery_redactor_defaults_match_go_privacy_filter_config() {
        let cfg = DiscoveryRedactorConfig {
            base_url: "  ".to_owned(),
            service_name: String::new(),
            endpoint_name: String::new(),
            timeout: StdDuration::ZERO,
            task_timeout: StdDuration::ZERO,
            poll_interval: StdDuration::ZERO,
            capacity_wait: StdDuration::ZERO,
            capacity_poll_interval: StdDuration::ZERO,
            categories: Vec::new(),
        }
        .with_defaults();

        assert_eq!(cfg.base_url, DEFAULT_DISCOVERY_BASE_URL);
        assert_eq!(cfg.service_name, DEFAULT_MEMORY_REDACTION_SERVICE_NAME);
        assert_eq!(cfg.endpoint_name, DEFAULT_MEMORY_REDACTION_ENDPOINT_NAME);
        assert_eq!(cfg.timeout, DEFAULT_MEMORY_REDACTION_TIMEOUT);
        assert_eq!(cfg.task_timeout, DEFAULT_MEMORY_REDACTION_TIMEOUT);
        assert_eq!(cfg.poll_interval, DEFAULT_MEMORY_REDACTION_POLL_INTERVAL);
        assert_eq!(cfg.capacity_wait, DEFAULT_MEMORY_REDACTION_CAPACITY_WAIT);
        assert_eq!(
            cfg.capacity_poll_interval,
            DEFAULT_MEMORY_REDACTION_POLL_INTERVAL
        );
        assert_eq!(cfg.categories, default_memory_redaction_categories());
    }

    #[test]
    fn discovery_redactor_job_body_matches_go_discovery_shape() {
        let redactor = DiscoveryRedactor::new(DiscoveryRedactorConfig {
            base_url: "http://discovery.test/root/".to_owned(),
            service_name: "privacy-filter".to_owned(),
            endpoint_name: "redact".to_owned(),
            timeout: StdDuration::from_secs(5),
            task_timeout: StdDuration::from_secs(35),
            poll_interval: StdDuration::from_secs(1),
            capacity_wait: StdDuration::from_secs(30),
            capacity_poll_interval: StdDuration::from_secs(1),
            categories: vec!["private_email".to_owned(), "secret".to_owned()],
        })
        .expect("redactor");

        let (body, job_id) = redactor
            .redaction_job_body_with_ids(
                "email alice@example.com",
                "memory-redact-111",
                "memory-redact-222",
            )
            .expect("body");
        let submitted =
            serde_json::from_slice::<DiscoveryRedactionJobRequest>(&body).expect("job request");
        let payload_bytes = general_purpose::STANDARD
            .decode(&submitted.invocation.body)
            .expect("payload b64");
        let payload = serde_json::from_slice::<RedactionRequest>(&payload_bytes).expect("payload");

        assert_eq!(job_id, "memory-redact-222");
        assert_eq!(submitted.idempotency_key, "memory-redact-222");
        assert_eq!(submitted.priority, DISCOVERY_PRIORITY_MEMORY);
        assert_eq!(submitted.wait_for_capacity_ms, 30_000);
        assert_eq!(submitted.capacity_poll_ms, 1_000);
        assert_eq!(submitted.invocation.service_name, "privacy-filter");
        assert_eq!(submitted.invocation.endpoint_name, "redact");
        assert_eq!(submitted.invocation.content_type, "application/json");
        assert_eq!(submitted.invocation.timeout_ms, 35_000);
        assert_eq!(payload.request_id, "memory-redact-111");
        assert_eq!(payload.text, "email alice@example.com");
        assert_eq!(payload.categories, vec!["private_email", "secret"]);
        assert_eq!(payload.replacement_mode, None);
        assert_eq!(
            redactor.endpoint("/v1/jobs/blocking"),
            "http://discovery.test/root/v1/jobs/blocking"
        );
    }

    #[test]
    fn discovery_redactor_job_body_supports_typed_placeholders() {
        let redactor =
            DiscoveryRedactor::new(DiscoveryRedactorConfig::default()).expect("redactor");

        let (body, _) = redactor
            .redaction_job_body_with_ids_and_mode(
                "email alice@example.com",
                "gradius-redact-111",
                "gradius-redact-222",
                RedactionReplacementMode::TypedPlaceholders,
            )
            .expect("body");
        let submitted =
            serde_json::from_slice::<DiscoveryRedactionJobRequest>(&body).expect("job request");
        let payload_bytes = general_purpose::STANDARD
            .decode(&submitted.invocation.body)
            .expect("payload b64");
        let payload = serde_json::from_slice::<RedactionRequest>(&payload_bytes).expect("payload");

        assert_eq!(
            payload.replacement_mode,
            Some(RedactionReplacementMode::TypedPlaceholders)
        );
    }

    #[test]
    fn discovery_redactor_result_decodes_status_body_and_errors_like_go() {
        let plain = DiscoveryRedactionEnvelope {
            status: "JOB_STATE_SUCCEEDED".to_owned(),
            result: Some(DiscoveryRedactionResult {
                response: Some(DiscoveryRedactionHttpResponse {
                    status_code: 200,
                    body: serde_json::json!({"redacted_text": "[redacted]"}).to_string(),
                    ..DiscoveryRedactionHttpResponse::default()
                }),
                ..DiscoveryRedactionResult::default()
            }),
            ..DiscoveryRedactionEnvelope::default()
        };
        let b64 = DiscoveryRedactionEnvelope {
            state: "succeeded".to_owned(),
            result: Some(DiscoveryRedactionResult {
                response: Some(DiscoveryRedactionHttpResponse {
                    status_code: 200,
                    body: general_purpose::URL_SAFE_NO_PAD.encode(r#"{"redacted_text":"ok"}"#),
                    ..DiscoveryRedactionHttpResponse::default()
                }),
                ..DiscoveryRedactionResult::default()
            }),
            ..DiscoveryRedactionEnvelope::default()
        };
        let nested = DiscoveryRedactionEnvelope {
            job: Some(DiscoveryRedactionJob {
                id: "id-1".to_owned(),
                job_id: "job-1".to_owned(),
                state: "running".to_owned(),
                ..DiscoveryRedactionJob::default()
            }),
            ..DiscoveryRedactionEnvelope::default()
        }
        .normalized();
        let failed = DiscoveryRedactionEnvelope {
            status: "failed".to_owned(),
            error: Some(serde_json::json!({"message":"bad","detail":"input"})),
            ..DiscoveryRedactionEnvelope::default()
        };

        match redaction_result_from_envelope(&plain, true).expect("plain") {
            RedactionEnvelopeResult::Done(value) => assert_eq!(value, "[redacted]"),
            RedactionEnvelopeResult::Pending => panic!("plain should be done"),
        }
        match redaction_result_from_envelope(&b64, true).expect("b64") {
            RedactionEnvelopeResult::Done(value) => assert_eq!(value, "ok"),
            RedactionEnvelopeResult::Pending => panic!("b64 should be done"),
        }
        assert!(matches!(
            redaction_result_from_envelope(&nested, true).expect("nested"),
            RedactionEnvelopeResult::Pending
        ));
        assert_eq!(nested.resolved_job_id().as_deref(), Some("job-1"));
        assert!(matches!(
            redaction_result_from_envelope(&failed, true),
            Err(DiscoveryRedactorError::JobFailed(message)) if message == "bad input"
        ));
        assert!(matches!(
            redaction_result_from_envelope(&DiscoveryRedactionEnvelope::default(), true),
            Err(DiscoveryRedactorError::UnknownStatus(status)) if status.is_empty()
        ));
    }
}
