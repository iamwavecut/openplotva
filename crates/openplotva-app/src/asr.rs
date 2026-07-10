use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose};
use futures_util::StreamExt;
use openplotva_core::{ChatAttachment, ChatMessageMeta};
use openplotva_dialog::DialogInput;
use openplotva_llm::{
    aifarm::{
        DiscoveryInvocation, DiscoveryJob, DiscoveryJobEnvelope, DiscoveryJobRequest,
        DiscoveryJobResponse, decode_discovery_body,
    },
    retry::FailureReason,
};
pub use openplotva_storage::TelegramFileAsrUpdate;
use openplotva_storage::{
    PostgresTelegramFileStore, TELEGRAM_FILE_ASR_STATUS_COMPLETED, TELEGRAM_FILE_ASR_STATUS_FAILED,
    TelegramFileRecord,
};
use openplotva_telegram::TelegramClient;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;

use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
};

pub const ASR_WORKFLOW_KEY: &str = "voice_transcription";
pub const DEFAULT_ASR_DISCOVERY_SERVICE_NAME: &str = "asr-api";
pub const DEFAULT_ASR_DISCOVERY_ENDPOINT_NAME: &str = "transcribe";
const ASR_DISCOVERY_CONTENT_TYPE: &str = "application/json";
const ASR_DISCOVERY_POLL_INTERVAL: Duration = Duration::from_millis(100);
const ASR_DISCOVERY_CAPACITY_WAIT: Duration = Duration::from_secs(2);
const ASR_DISCOVERY_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const ASR_DISCOVERY_REQUEST_RETRIES: u8 = 5;
const ASR_TERMINAL_PAYLOAD_RETRIES: u8 = 2;

pub type TelegramFileAsrGetFuture<'a, Error> =
    Pin<Box<dyn Future<Output = Result<Option<TelegramFileRecord>, Error>> + Send + 'a>>;
pub type TelegramFileAsrClaimFuture<'a, Error> =
    Pin<Box<dyn Future<Output = Result<Option<TelegramFileRecord>, Error>> + Send + 'a>>;
pub type TelegramFileAsrUpdateFuture<'a, Error> =
    Pin<Box<dyn Future<Output = Result<TelegramFileRecord, Error>> + Send + 'a>>;
pub type TelegramVoiceDownloadFuture<'a, Error> =
    Pin<Box<dyn Future<Output = Result<Vec<u8>, Error>> + Send + 'a>>;
pub type AsrTranscribeFuture<'a, Error> =
    Pin<Box<dyn Future<Output = Result<AsrTranscript, Error>> + Send + 'a>>;
pub type DialogAsrInputFuture<'a> = Pin<Box<dyn Future<Output = DialogInput> + Send + 'a>>;

pub trait DialogAsrInputMaterializer: fmt::Debug + Send + Sync {
    fn materialize_dialog_asr_input<'a>(
        &'a self,
        input: DialogInput,
        now: OffsetDateTime,
    ) -> DialogAsrInputFuture<'a>;
}

pub trait TelegramFileAsrStore {
    type Error: fmt::Display + Send + Sync + 'static;

    fn get_file<'a>(&'a self, file_unique_id: &'a str)
    -> TelegramFileAsrGetFuture<'a, Self::Error>;

    fn claim_asr_processing<'a>(
        &'a self,
        file_unique_id: &'a str,
        requested_at: OffsetDateTime,
    ) -> TelegramFileAsrClaimFuture<'a, Self::Error>;

    fn update_asr<'a>(
        &'a self,
        params: &'a TelegramFileAsrUpdate,
    ) -> TelegramFileAsrUpdateFuture<'a, Self::Error>;
}

impl TelegramFileAsrStore for PostgresTelegramFileStore {
    type Error = openplotva_storage::StorageError;

    fn get_file<'a>(
        &'a self,
        file_unique_id: &'a str,
    ) -> TelegramFileAsrGetFuture<'a, Self::Error> {
        Box::pin(PostgresTelegramFileStore::get_file(self, file_unique_id))
    }

    fn claim_asr_processing<'a>(
        &'a self,
        file_unique_id: &'a str,
        requested_at: OffsetDateTime,
    ) -> TelegramFileAsrClaimFuture<'a, Self::Error> {
        Box::pin(PostgresTelegramFileStore::claim_asr_processing(
            self,
            file_unique_id,
            requested_at,
        ))
    }

    fn update_asr<'a>(
        &'a self,
        params: &'a TelegramFileAsrUpdate,
    ) -> TelegramFileAsrUpdateFuture<'a, Self::Error> {
        Box::pin(PostgresTelegramFileStore::update_asr(self, params))
    }
}

pub trait TelegramVoiceDownloader {
    type Error: fmt::Display + Send + Sync + 'static;

    fn download_voice<'a>(
        &'a self,
        latest_file_id: &'a str,
    ) -> TelegramVoiceDownloadFuture<'a, Self::Error>;
}

pub trait AsrTranscriber {
    type Error: fmt::Display + Send + Sync + 'static;

    fn transcribe_voice<'a>(
        &'a self,
        audio: &'a [u8],
        request: AsrRequest,
    ) -> AsrTranscribeFuture<'a, Self::Error>;
}

#[derive(Clone)]
pub struct TelegramClientVoiceDownloader {
    client: TelegramClient,
}

impl TelegramClientVoiceDownloader {
    #[must_use]
    pub fn new(client: TelegramClient) -> Self {
        Self { client }
    }
}

impl fmt::Debug for TelegramClientVoiceDownloader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramClientVoiceDownloader")
            .finish_non_exhaustive()
    }
}

impl TelegramVoiceDownloader for TelegramClientVoiceDownloader {
    type Error = TelegramVoiceDownloadError;

    fn download_voice<'a>(
        &'a self,
        latest_file_id: &'a str,
    ) -> TelegramVoiceDownloadFuture<'a, Self::Error> {
        Box::pin(async move {
            let file: carapax::types::File = self
                .client
                .execute(carapax::types::GetFile::new(latest_file_id))
                .await
                .map_err(TelegramVoiceDownloadError::GetFile)?;
            let file_path = file
                .file_path
                .as_deref()
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .ok_or(TelegramVoiceDownloadError::MissingFilePath)?;
            let mut stream = self
                .client
                .download_file(file_path)
                .await
                .map_err(TelegramVoiceDownloadError::Download)?;
            let mut data = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(TelegramVoiceDownloadError::DownloadChunk)?;
                data.extend_from_slice(&chunk);
            }
            if data.is_empty() {
                return Err(TelegramVoiceDownloadError::EmptyFile);
            }
            Ok(data)
        })
    }
}

#[derive(Debug, Error)]
pub enum TelegramVoiceDownloadError {
    #[error("get telegram file: {0}")]
    GetFile(#[source] carapax::api::ExecuteError),
    #[error("telegram file has no downloadable path")]
    MissingFilePath,
    #[error("download telegram file: {0}")]
    Download(#[source] carapax::api::DownloadFileError),
    #[error("download telegram file chunk: {0}")]
    DownloadChunk(#[source] reqwest::Error),
    #[error("empty telegram voice file")]
    EmptyFile,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsrRequest {
    pub chat_id: i64,
    pub message_id: i32,
    pub file_unique_id: String,
    pub mime_type: Option<String>,
    pub duration_seconds: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AsrTranscript {
    pub text: String,
    pub provider: String,
    pub model: String,
    pub latency_ms: i32,
    pub fallback_used: bool,
    pub chunks: i32,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveryAsrConfig {
    pub base_url: String,
    pub service_name: String,
    pub endpoint_name: String,
    pub request_timeout: Duration,
    pub task_timeout: Duration,
    pub poll_interval: Duration,
    pub capacity_wait: Duration,
}

#[derive(Clone, Debug)]
pub struct DiscoveryAsrClient {
    cfg: DiscoveryAsrConfig,
    client: reqwest::Client,
}

impl DiscoveryAsrClient {
    pub fn new(cfg: DiscoveryAsrConfig) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout.max(Duration::from_secs(1)))
            .build()?;
        Ok(Self { cfg, client })
    }

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
        let endpoint_name = endpoint_name.trim();
        let endpoint_name = if endpoint_name.is_empty() {
            DEFAULT_ASR_DISCOVERY_ENDPOINT_NAME
        } else {
            endpoint_name
        };
        Self::new(DiscoveryAsrConfig {
            base_url: base_url.to_owned(),
            service_name: service_name.to_owned(),
            endpoint_name: endpoint_name.to_owned(),
            request_timeout: budget,
            task_timeout: budget,
            poll_interval: ASR_DISCOVERY_POLL_INTERVAL,
            capacity_wait: ASR_DISCOVERY_CAPACITY_WAIT,
        })
        .map(Some)
    }

    async fn transcribe_bytes(
        &self,
        audio: &[u8],
        request: AsrRequest,
    ) -> Result<AsrTranscript, AsrClientError> {
        let payload = AsrServiceRequest {
            request_id: format!(
                "voice-{}-{}-{}",
                request.chat_id, request.message_id, request.file_unique_id
            ),
            audio_b64: general_purpose::STANDARD.encode(audio),
            mime_type: request.mime_type,
            file_name: Some(format!("{}.ogg", request.file_unique_id)),
            duration_seconds: request.duration_seconds,
            language: Some("ru".to_owned()),
        };
        let body = serde_json::to_vec(&payload)
            .map_err(|error| AsrClientError::Discovery(format!("encode request: {error}")))?;
        let job_id = next_asr_job_id();
        let job_request = DiscoveryJobRequest {
            invocation: DiscoveryInvocation {
                service_name: self.cfg.service_name.clone(),
                endpoint_name: self.cfg.endpoint_name.clone(),
                headers: BTreeMap::new(),
                query: BTreeMap::new(),
                body: general_purpose::STANDARD.encode(body),
                content_type: ASR_DISCOVERY_CONTENT_TYPE.to_owned(),
                timeout_ms: duration_ms(self.cfg.task_timeout),
            },
            idempotency_key: job_id.clone(),
            priority: 0,
            wait_for_capacity_ms: duration_ms(self.cfg.capacity_wait),
            capacity_poll_ms: duration_ms(self.cfg.poll_interval),
        };
        let deadline = Instant::now() + self.cfg.task_timeout.max(Duration::from_secs(1));
        let envelope = self
            .submit_with_retry(&job_request, deadline, &job_id)
            .await?;
        let mut job = envelope.resolve_job();
        let resolved_id = first_non_empty(&job.resolved_id(), &job_id);
        let mut terminal_payload_retries = 0_u8;
        loop {
            if let Some(response) = terminal_response(&job)? {
                match decode_asr_response(&response) {
                    Ok(transcript) => return Ok(transcript),
                    Err(error)
                        if terminal_payload_retries < ASR_TERMINAL_PAYLOAD_RETRIES
                            && retryable_terminal_payload_error(&error) =>
                    {
                        terminal_payload_retries += 1;
                    }
                    Err(error) => return Err(error),
                }
            }
            let remaining = self.remaining_budget(deadline, &resolved_id)?;
            tokio::time::sleep(self.cfg.poll_interval.min(remaining)).await;
            job = self
                .poll_with_retry(&resolved_id, deadline)
                .await?
                .resolve_job();
        }
    }

    async fn submit_with_retry(
        &self,
        job: &DiscoveryJobRequest,
        deadline: Instant,
        job_id: &str,
    ) -> Result<DiscoveryJobEnvelope, AsrClientError> {
        let mut retries = 0_u8;
        loop {
            match self.with_deadline(self.submit(job), deadline, job_id).await {
                Ok(envelope) => return Ok(envelope),
                Err(error)
                    if retries < ASR_DISCOVERY_REQUEST_RETRIES
                        && retryable_discovery_request_error(&error) =>
                {
                    retries += 1;
                    self.sleep_before_retry(deadline, job_id).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn poll_with_retry(
        &self,
        job_id: &str,
        deadline: Instant,
    ) -> Result<DiscoveryJobEnvelope, AsrClientError> {
        let mut retries = 0_u8;
        loop {
            match self
                .with_deadline(self.poll(job_id), deadline, job_id)
                .await
            {
                Ok(envelope) => return Ok(envelope),
                Err(error)
                    if retries < ASR_DISCOVERY_REQUEST_RETRIES
                        && retryable_discovery_request_error(&error) =>
                {
                    retries += 1;
                    self.sleep_before_retry(deadline, job_id).await?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn sleep_before_retry(
        &self,
        deadline: Instant,
        job_id: &str,
    ) -> Result<(), AsrClientError> {
        let remaining = self.remaining_budget(deadline, job_id)?;
        tokio::time::sleep(ASR_DISCOVERY_RETRY_BACKOFF.min(remaining)).await;
        self.remaining_budget(deadline, job_id).map(|_| ())
    }

    async fn with_deadline<T, Fut>(
        &self,
        future: Fut,
        deadline: Instant,
        job_id: &str,
    ) -> Result<T, AsrClientError>
    where
        Fut: Future<Output = Result<T, AsrClientError>>,
    {
        let remaining = self.remaining_budget(deadline, job_id)?;
        match tokio::time::timeout(remaining, future).await {
            Ok(result) => result,
            Err(_) => Err(self.deadline_error(job_id)),
        }
    }

    fn remaining_budget(
        &self,
        deadline: Instant,
        job_id: &str,
    ) -> Result<Duration, AsrClientError> {
        deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or_else(|| self.deadline_error(job_id))
    }

    fn deadline_error(&self, job_id: &str) -> AsrClientError {
        AsrClientError::Discovery(format!(
            "discovery job {job_id} did not complete within {:?}",
            self.cfg.task_timeout
        ))
    }

    async fn submit(
        &self,
        job: &DiscoveryJobRequest,
    ) -> Result<DiscoveryJobEnvelope, AsrClientError> {
        let response = self
            .client
            .post(self.endpoint("/v1/jobs/blocking"))
            .json(job)
            .send()
            .await
            .map_err(AsrClientError::Request)?;
        self.read_envelope(response, "submit").await
    }

    async fn poll(&self, job_id: &str) -> Result<DiscoveryJobEnvelope, AsrClientError> {
        let response = self
            .client
            .get(self.endpoint(&format!("/v1/jobs/{}", job_id.trim())))
            .send()
            .await
            .map_err(AsrClientError::Request)?;
        self.read_envelope(response, "poll").await
    }

    async fn read_envelope(
        &self,
        response: reqwest::Response,
        stage: &str,
    ) -> Result<DiscoveryJobEnvelope, AsrClientError> {
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| AsrClientError::Discovery(format!("{stage} body: {error}")))?;
        if !status.is_success() {
            return Err(AsrClientError::Status {
                status: status.as_u16(),
                detail: String::from_utf8_lossy(&bytes).into_owned(),
            });
        }
        serde_json::from_slice(&bytes)
            .map_err(|error| AsrClientError::Discovery(format!("decode {stage}: {error}")))
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.cfg.base_url.trim_end_matches('/'), path)
    }
}

impl AsrTranscriber for DiscoveryAsrClient {
    type Error = AsrClientError;

    fn transcribe_voice<'a>(
        &'a self,
        audio: &'a [u8],
        request: AsrRequest,
    ) -> AsrTranscribeFuture<'a, Self::Error> {
        Box::pin(async move { self.transcribe_bytes(audio, request).await })
    }
}

#[derive(Clone)]
pub struct RoutedAsrTranscriber {
    walker: RoutedAttemptWalker,
    base_config: DiscoveryAsrConfig,
}

impl fmt::Debug for RoutedAsrTranscriber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutedAsrTranscriber")
            .field("base_url", &self.base_config.base_url)
            .field("service_name", &self.base_config.service_name)
            .field("endpoint_name", &self.base_config.endpoint_name)
            .finish_non_exhaustive()
    }
}

impl RoutedAsrTranscriber {
    #[must_use]
    pub fn new(walker: RoutedAttemptWalker, base_config: DiscoveryAsrConfig) -> Self {
        Self {
            walker,
            base_config,
        }
    }
}

impl AsrTranscriber for RoutedAsrTranscriber {
    type Error = AsrClientError;

    fn transcribe_voice<'a>(
        &'a self,
        audio: &'a [u8],
        request: AsrRequest,
    ) -> AsrTranscribeFuture<'a, Self::Error> {
        Box::pin(async move {
            let audio = audio.to_vec();
            let base_config = self.base_config.clone();
            let result = self
                .walker
                .run(
                    RoutedRequestContext {
                        workflow_key: ASR_WORKFLOW_KEY.to_owned(),
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let audio = audio.clone();
                        let request = request.clone();
                        let base_config = base_config.clone();
                        async move {
                            let client = routed_asr_client(base_config, &attempt)?;
                            client.transcribe_bytes(&audio, request).await
                        }
                    },
                    asr_retryable_reason,
                )
                .await;
            match result {
                Ok(result) => Ok(result),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(AsrClientError::Discovery(error.to_string()))
                }
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum AsrClientError {
    #[error("asr request: {0}")]
    Request(#[source] reqwest::Error),
    #[error("asr upstream status {status}: {detail}")]
    Status { status: u16, detail: String },
    #[error("asr discovery: {0}")]
    Discovery(String),
}

#[derive(Serialize)]
struct AsrServiceRequest {
    request_id: String,
    audio_b64: String,
    mime_type: Option<String>,
    file_name: Option<String>,
    duration_seconds: Option<i64>,
    language: Option<String>,
}

#[derive(Deserialize)]
struct AsrServiceResponse {
    text: String,
    engine: String,
    model: String,
    latency_ms: i32,
    #[serde(default)]
    fallback_used: bool,
    #[serde(default)]
    chunks: i32,
    #[serde(default)]
    warnings: Vec<String>,
}

fn routed_asr_client(
    mut config: DiscoveryAsrConfig,
    attempt: &RoutedAttempt,
) -> Result<DiscoveryAsrClient, AsrClientError> {
    if let Some(endpoint) = attempt
        .model_base_url
        .as_deref()
        .or(attempt.provider_endpoint.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        config.base_url = endpoint.to_owned();
    }
    if let Some(service_name) = attempt
        .discovery_service_name
        .as_deref()
        .or_else(|| {
            attempt
                .provider_config
                .get("service_name")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        config.service_name = service_name.to_owned();
    }
    if let Some(endpoint_name) = attempt
        .discovery_endpoint_name
        .as_deref()
        .or_else(|| {
            attempt
                .provider_config
                .get("endpoint_name")
                .and_then(serde_json::Value::as_str)
        })
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        config.endpoint_name = endpoint_name.to_owned();
    }
    if attempt.model_name.trim().is_empty() {
        return Err(AsrClientError::Discovery(
            "ASR route model is empty".to_owned(),
        ));
    }
    DiscoveryAsrClient::new(config)
        .map_err(|error| AsrClientError::Discovery(format!("build routed ASR client: {error}")))
}

fn asr_retryable_reason(error: &AsrClientError) -> Option<FailureReason> {
    match error {
        AsrClientError::Status { status, .. } if *status >= 500 => {
            Some(FailureReason::ProviderUnavailable)
        }
        AsrClientError::Discovery(_) => Some(FailureReason::ProviderUnavailable),
        AsrClientError::Request(source) if source.is_timeout() => {
            Some(FailureReason::ProviderTimeout)
        }
        AsrClientError::Request(source) if source.is_connect() => {
            Some(FailureReason::ProviderUnavailable)
        }
        AsrClientError::Request(_) | AsrClientError::Status { .. } => None,
    }
}

fn retryable_discovery_request_error(error: &AsrClientError) -> bool {
    match error {
        AsrClientError::Request(_) => true,
        AsrClientError::Status { status, .. } => *status >= 500,
        AsrClientError::Discovery(message) => {
            message.starts_with("submit body:")
                || message.starts_with("poll body:")
                || message.starts_with("decode submit:")
                || message.starts_with("decode poll:")
        }
    }
}

fn retryable_terminal_payload_error(error: &AsrClientError) -> bool {
    matches!(
        error,
        AsrClientError::Discovery(message)
            if message.starts_with("decode response body:")
                || message.starts_with("decode ASR response:")
    )
}

fn terminal_response(job: &DiscoveryJob) -> Result<Option<DiscoveryJobResponse>, AsrClientError> {
    let state = job.resolved_status().to_ascii_uppercase();
    if state.contains("SUCC") {
        return match job
            .result
            .as_ref()
            .and_then(|result| result.response.clone())
        {
            Some(response) => Ok(Some(response)),
            None => Err(AsrClientError::Discovery(
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
        return Err(AsrClientError::Discovery(format!(
            "discovery job {}: {detail}",
            state.to_ascii_lowercase()
        )));
    }
    Ok(None)
}

fn decode_asr_response(response: &DiscoveryJobResponse) -> Result<AsrTranscript, AsrClientError> {
    if response.status_code != 0 && response.status_code != 200 {
        let body = decode_discovery_body(&response.body)
            .ok()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|| response.body.clone());
        return Err(AsrClientError::Status {
            status: response.status_code,
            detail: body,
        });
    }
    let raw = decode_discovery_body(&response.body)
        .map_err(|error| AsrClientError::Discovery(format!("decode response body: {error}")))?;
    let decoded: AsrServiceResponse = serde_json::from_slice(&raw)
        .map_err(|error| AsrClientError::Discovery(format!("decode ASR response: {error}")))?;
    Ok(AsrTranscript {
        text: decoded.text,
        provider: decoded.engine,
        model: decoded.model,
        latency_ms: decoded.latency_ms,
        fallback_used: decoded.fallback_used,
        chunks: decoded.chunks,
        warnings: decoded.warnings,
    })
}

fn duration_ms(duration: Duration) -> i32 {
    i32::try_from(duration.as_millis()).unwrap_or(i32::MAX)
}

fn first_non_empty(primary: &str, fallback: &str) -> String {
    if primary.trim().is_empty() {
        fallback.trim().to_owned()
    } else {
        primary.trim().to_owned()
    }
}

fn next_asr_job_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    format!("asr-{nanos}-{counter}")
}

#[derive(Clone, Debug)]
pub struct TelegramDialogAsrInputMaterializer<Store, Downloader, Transcriber> {
    store: Store,
    downloader: Downloader,
    transcriber: Transcriber,
}

impl<Store, Downloader, Transcriber>
    TelegramDialogAsrInputMaterializer<Store, Downloader, Transcriber>
{
    #[must_use]
    pub const fn new(store: Store, downloader: Downloader, transcriber: Transcriber) -> Self {
        Self {
            store,
            downloader,
            transcriber,
        }
    }
}

impl<Store, Downloader, Transcriber>
    TelegramDialogAsrInputMaterializer<Store, Downloader, Transcriber>
where
    Store: TelegramFileAsrStore + Send + Sync,
    Downloader: TelegramVoiceDownloader + Send + Sync,
    Transcriber: AsrTranscriber + Send + Sync,
{
    pub fn materialize_dialog_asr_input<'a>(
        &'a self,
        mut input: DialogInput,
        now: OffsetDateTime,
    ) -> DialogAsrInputFuture<'a> {
        Box::pin(async move {
            let Some(candidate) = select_voice_candidate(&input.message.meta) else {
                return input;
            };
            let Some(record) = self.lookup_record(&candidate.file_unique_id).await else {
                return input;
            };

            if let Some(text) = cached_asr_text(&record) {
                apply_transcript(&mut input, candidate.attachment_index, &text);
                return input;
            }

            let Some(record) = self.claim_processing(&record, now).await else {
                if let Some(record) = self.lookup_record(&candidate.file_unique_id).await
                    && let Some(text) = cached_asr_text(&record)
                {
                    apply_transcript(&mut input, candidate.attachment_index, &text);
                }
                return input;
            };

            let latest_file_id = record.latest_file_id.trim();
            if latest_file_id.is_empty() {
                self.mark_failed_best_effort(&record, "missing latest Telegram file id")
                    .await;
                return input;
            }
            let audio = match self.downloader.download_voice(latest_file_id).await {
                Ok(audio) => audio,
                Err(error) => {
                    self.mark_failed_best_effort(&record, &error.to_string())
                        .await;
                    return input;
                }
            };
            let request = AsrRequest {
                chat_id: input.context.chat_id,
                message_id: input.message.id,
                file_unique_id: record.file_unique_id.clone(),
                mime_type: record.mime_type.clone(),
                duration_seconds: (candidate.duration_seconds > 0)
                    .then_some(candidate.duration_seconds),
            };
            let transcript = match self.transcriber.transcribe_voice(&audio, request).await {
                Ok(transcript) => transcript,
                Err(error) => {
                    self.mark_failed_best_effort(&record, &error.to_string())
                        .await;
                    return input;
                }
            };
            let text = transcript.text.trim().to_owned();
            if text.is_empty() {
                self.mark_failed_best_effort(&record, "empty transcript")
                    .await;
                return input;
            }
            if let Err(error) = self.store_completed(&record, &text, &transcript, now).await {
                tracing::warn!(
                    error = error.to_string(),
                    file_unique_id = record.file_unique_id,
                    "failed to store Telegram voice ASR transcript"
                );
            }
            apply_transcript(&mut input, candidate.attachment_index, &text);
            input
        })
    }

    async fn lookup_record(&self, file_unique_id: &str) -> Option<TelegramFileRecord> {
        match self.store.get_file(file_unique_id).await {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(
                    error = error.to_string(),
                    file_unique_id,
                    "failed to load Telegram voice file metadata for ASR"
                );
                None
            }
        }
    }

    async fn claim_processing(
        &self,
        record: &TelegramFileRecord,
        now: OffsetDateTime,
    ) -> Option<TelegramFileRecord> {
        match self
            .store
            .claim_asr_processing(&record.file_unique_id, now)
            .await
        {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(
                    error = error.to_string(),
                    file_unique_id = record.file_unique_id,
                    "failed to claim Telegram voice ASR processing"
                );
                None
            }
        }
    }

    async fn mark_failed_best_effort(&self, record: &TelegramFileRecord, error: &str) {
        if let Err(update_error) = self
            .store
            .update_asr(&TelegramFileAsrUpdate {
                file_unique_id: record.file_unique_id.clone(),
                asr_status: TELEGRAM_FILE_ASR_STATUS_FAILED.to_owned(),
                asr_error: Some(error.to_owned()),
                ..TelegramFileAsrUpdate::default()
            })
            .await
        {
            tracing::warn!(
                error = update_error.to_string(),
                file_unique_id = record.file_unique_id,
                "failed to mark Telegram voice ASR as failed"
            );
        }
    }

    async fn store_completed(
        &self,
        record: &TelegramFileRecord,
        text: &str,
        transcript: &AsrTranscript,
        now: OffsetDateTime,
    ) -> Result<TelegramFileRecord, Store::Error> {
        self.store
            .update_asr(&TelegramFileAsrUpdate {
                file_unique_id: record.file_unique_id.clone(),
                asr_status: TELEGRAM_FILE_ASR_STATUS_COMPLETED.to_owned(),
                asr_text: Some(text.to_owned()),
                asr_provider: Some(transcript.provider.clone()),
                asr_model: Some(transcript.model.clone()),
                asr_latency_ms: Some(transcript.latency_ms),
                asr_fallback_used: Some(transcript.fallback_used),
                asr_chunks: Some(transcript.chunks),
                asr_warnings: Some(transcript.warnings.clone()),
                asr_error: None,
                asr_completed_at: Some(now),
                ..TelegramFileAsrUpdate::default()
            })
            .await
    }
}

impl<Store, Downloader, Transcriber> DialogAsrInputMaterializer
    for TelegramDialogAsrInputMaterializer<Store, Downloader, Transcriber>
where
    Store: TelegramFileAsrStore + Send + Sync + fmt::Debug,
    Downloader: TelegramVoiceDownloader + Send + Sync + fmt::Debug,
    Transcriber: AsrTranscriber + Send + Sync + fmt::Debug,
{
    fn materialize_dialog_asr_input<'a>(
        &'a self,
        input: DialogInput,
        now: OffsetDateTime,
    ) -> DialogAsrInputFuture<'a> {
        TelegramDialogAsrInputMaterializer::materialize_dialog_asr_input(self, input, now)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VoiceCandidate {
    attachment_index: usize,
    file_unique_id: String,
    duration_seconds: i64,
}

fn select_voice_candidate(meta: &ChatMessageMeta) -> Option<VoiceCandidate> {
    meta.attachments
        .iter()
        .enumerate()
        .find_map(|(index, attachment)| voice_candidate(index, attachment))
}

fn voice_candidate(index: usize, attachment: &ChatAttachment) -> Option<VoiceCandidate> {
    if attachment.kind.trim() != "voice" || attachment.source.trim() != "message" {
        return None;
    }
    let file_unique_id = attachment.file_unique_id.trim();
    if file_unique_id.is_empty() {
        return None;
    }
    Some(VoiceCandidate {
        attachment_index: index,
        file_unique_id: file_unique_id.to_owned(),
        duration_seconds: attachment.duration_seconds,
    })
}

fn cached_asr_text(record: &TelegramFileRecord) -> Option<String> {
    if record.asr_status != TELEGRAM_FILE_ASR_STATUS_COMPLETED {
        return None;
    }
    record
        .asr_text
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn apply_transcript(input: &mut DialogInput, attachment_index: usize, text: &str) {
    let materialized = merge_existing_text_with_transcript(&input.message.text, text);
    input.message.text.clone_from(&materialized);
    input.message.normalized = materialized;
    if let Some(attachment) = input.message.meta.attachments.get_mut(attachment_index) {
        attachment.content = text.to_owned();
    }
}

fn merge_existing_text_with_transcript(existing: &str, transcript: &str) -> String {
    let existing = existing.trim();
    let transcript = transcript.trim();
    if existing.is_empty() || existing == transcript {
        return transcript.to_owned();
    }
    if transcript.is_empty() {
        return existing.to_owned();
    }
    format!("{existing}\n\n{transcript}")
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{
        Json, Router,
        extract::State,
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use openplotva_core::{ChatAttachment, ChatMessageMeta};
    use openplotva_dialog::{DialogContext, DialogInput, DialogMessage};
    use openplotva_storage::{
        TELEGRAM_FILE_ASR_STATUS_COMPLETED, TELEGRAM_FILE_ASR_STATUS_PENDING,
        TELEGRAM_FILE_ASR_STATUS_PROCESSING, TelegramFileRecord,
    };
    use time::OffsetDateTime;

    use super::*;

    #[derive(Clone, Debug)]
    struct FakeStore {
        record: Arc<Mutex<TelegramFileRecord>>,
        updates: Arc<Mutex<Vec<TelegramFileAsrUpdate>>>,
    }

    #[derive(Clone, Debug, Default)]
    struct FakeDownloader {
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone, Debug)]
    struct FakeTranscriber {
        result: Result<AsrTranscript, String>,
        calls: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    fn is_fresh_processing_record(record: &TelegramFileRecord, now: OffsetDateTime) -> bool {
        record.asr_status == TELEGRAM_FILE_ASR_STATUS_PROCESSING
            && record
                .asr_requested_at
                .is_some_and(|requested_at| requested_at >= now - time::Duration::minutes(10))
    }

    impl TelegramFileAsrStore for FakeStore {
        type Error = String;

        fn get_file<'a>(
            &'a self,
            file_unique_id: &'a str,
        ) -> TelegramFileAsrGetFuture<'a, Self::Error> {
            Box::pin(async move {
                let record = self.record.lock().expect("lock").clone();
                Ok((record.file_unique_id == file_unique_id).then_some(record))
            })
        }

        fn claim_asr_processing<'a>(
            &'a self,
            file_unique_id: &'a str,
            requested_at: OffsetDateTime,
        ) -> TelegramFileAsrClaimFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut record = self.record.lock().expect("lock");
                if record.file_unique_id != file_unique_id
                    || record.asr_status == TELEGRAM_FILE_ASR_STATUS_COMPLETED
                    || is_fresh_processing_record(&record, requested_at)
                {
                    return Ok(None);
                }
                record.asr_status = TELEGRAM_FILE_ASR_STATUS_PROCESSING.to_owned();
                record.asr_error = None;
                record.asr_requested_at = Some(requested_at);
                let update = TelegramFileAsrUpdate {
                    file_unique_id: record.file_unique_id.clone(),
                    asr_status: record.asr_status.clone(),
                    asr_requested_at: record.asr_requested_at,
                    ..TelegramFileAsrUpdate::default()
                };
                self.updates.lock().expect("lock").push(update);
                Ok(Some(record.clone()))
            })
        }

        fn update_asr<'a>(
            &'a self,
            params: &'a TelegramFileAsrUpdate,
        ) -> TelegramFileAsrUpdateFuture<'a, Self::Error> {
            Box::pin(async move {
                self.updates.lock().expect("lock").push(params.clone());
                let mut record = self.record.lock().expect("lock");
                record.asr_status = params.asr_status.clone();
                if let Some(text) = &params.asr_text {
                    record.asr_text = Some(text.clone());
                }
                record.asr_provider = params
                    .asr_provider
                    .clone()
                    .or_else(|| record.asr_provider.clone());
                record.asr_model = params
                    .asr_model
                    .clone()
                    .or_else(|| record.asr_model.clone());
                record.asr_latency_ms = params.asr_latency_ms.or(record.asr_latency_ms);
                record.asr_fallback_used = params.asr_fallback_used.or(record.asr_fallback_used);
                record.asr_chunks = params.asr_chunks.or(record.asr_chunks);
                record.asr_warnings = params
                    .asr_warnings
                    .clone()
                    .or_else(|| record.asr_warnings.clone());
                record.asr_error = params.asr_error.clone();
                record.asr_requested_at = params.asr_requested_at.or(record.asr_requested_at);
                record.asr_completed_at = params.asr_completed_at.or(record.asr_completed_at);
                Ok(record.clone())
            })
        }
    }

    impl TelegramVoiceDownloader for FakeDownloader {
        type Error = String;

        fn download_voice<'a>(
            &'a self,
            latest_file_id: &'a str,
        ) -> TelegramVoiceDownloadFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("lock")
                    .push(latest_file_id.to_owned());
                Ok(vec![1, 2, 3])
            })
        }
    }

    impl AsrTranscriber for FakeTranscriber {
        type Error = String;

        fn transcribe_voice<'a>(
            &'a self,
            audio: &'a [u8],
            request: AsrRequest,
        ) -> AsrTranscribeFuture<'a, Self::Error> {
            Box::pin(async move {
                assert_eq!(request.file_unique_id, "voice-u");
                self.calls.lock().expect("lock").push(audio.to_vec());
                self.result.clone()
            })
        }
    }

    #[tokio::test]
    async fn materializer_uses_cached_voice_transcript() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let mut record = voice_record(now);
        record.asr_status = TELEGRAM_FILE_ASR_STATUS_COMPLETED.to_owned();
        record.asr_text = Some("cached voice text".to_owned());
        let store = FakeStore {
            record: Arc::new(Mutex::new(record)),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let downloader = FakeDownloader::default();
        let transcriber = FakeTranscriber {
            result: Ok(transcript("generated")),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store.clone(),
            downloader.clone(),
            transcriber.clone(),
        );

        let input = materializer
            .materialize_dialog_asr_input(dialog_input_with_voice(), now)
            .await;

        assert_eq!(input.message.text, "cached voice text");
        assert_eq!(input.message.normalized, "cached voice text");
        assert_eq!(
            input.message.meta.attachments[0].content,
            "cached voice text"
        );
        assert!(store.updates.lock().expect("lock").is_empty());
        assert!(downloader.calls.lock().expect("lock").is_empty());
        assert!(transcriber.calls.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn materializer_downloads_transcribes_and_stores_voice_transcript() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let record = voice_record(now);
        let store = FakeStore {
            record: Arc::new(Mutex::new(record)),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let downloader = FakeDownloader::default();
        let transcriber = FakeTranscriber {
            result: Ok(transcript("fresh voice text")),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store.clone(),
            downloader.clone(),
            transcriber.clone(),
        );

        let input = materializer
            .materialize_dialog_asr_input(dialog_input_with_voice(), now)
            .await;

        assert_eq!(input.message.text, "fresh voice text");
        assert_eq!(
            downloader.calls.lock().expect("lock").as_slice(),
            ["voice-file"]
        );
        assert_eq!(
            transcriber.calls.lock().expect("lock").as_slice(),
            [vec![1, 2, 3]]
        );
        let updates = store.updates.lock().expect("lock").clone();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].asr_status, "processing");
        assert_eq!(updates[0].asr_requested_at, Some(now));
        assert_eq!(updates[1].asr_status, "completed");
        assert_eq!(updates[1].asr_text.as_deref(), Some("fresh voice text"));
        assert_eq!(updates[1].asr_provider.as_deref(), Some("gigaam"));
        assert_eq!(updates[1].asr_model.as_deref(), Some("gigaam-v3"));
        assert_eq!(updates[1].asr_fallback_used, Some(false));
        assert_eq!(updates[1].asr_chunks, Some(1));
        assert_eq!(updates[1].asr_warnings.as_deref(), Some([].as_slice()));
    }

    #[tokio::test]
    async fn materializer_preserves_existing_message_caption_with_transcript() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let store = FakeStore {
            record: Arc::new(Mutex::new(voice_record(now))),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store,
            FakeDownloader::default(),
            FakeTranscriber {
                result: Ok(transcript("fresh voice text")),
                calls: Arc::new(Mutex::new(Vec::new())),
            },
        );
        let mut input = dialog_input_with_voice();
        input.message.text = "caption text".to_owned();
        input.message.normalized = "caption text".to_owned();

        let input = materializer.materialize_dialog_asr_input(input, now).await;

        assert_eq!(input.message.text, "caption text\n\nfresh voice text");
        assert_eq!(input.message.normalized, "caption text\n\nfresh voice text");
        assert_eq!(
            input.message.meta.attachments[0].content,
            "fresh voice text"
        );
    }

    #[tokio::test]
    async fn materializer_does_not_duplicate_work_when_processing_claim_is_lost() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let mut record = voice_record(now);
        record.asr_status = TELEGRAM_FILE_ASR_STATUS_PROCESSING.to_owned();
        record.asr_requested_at = Some(now);
        let store = FakeStore {
            record: Arc::new(Mutex::new(record)),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let downloader = FakeDownloader::default();
        let transcriber = FakeTranscriber {
            result: Ok(transcript("unused")),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store.clone(),
            downloader.clone(),
            transcriber.clone(),
        );

        let input = materializer
            .materialize_dialog_asr_input(dialog_input_with_voice(), now)
            .await;

        assert!(input.message.text.is_empty());
        assert!(store.updates.lock().expect("lock").is_empty());
        assert!(downloader.calls.lock().expect("lock").is_empty());
        assert!(transcriber.calls.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn materializer_reclaims_stale_processing_claim() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let mut record = voice_record(now);
        record.asr_status = TELEGRAM_FILE_ASR_STATUS_PROCESSING.to_owned();
        record.asr_requested_at = Some(now - time::Duration::minutes(11));
        let store = FakeStore {
            record: Arc::new(Mutex::new(record)),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let downloader = FakeDownloader::default();
        let transcriber = FakeTranscriber {
            result: Ok(transcript("reclaimed voice text")),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store.clone(),
            downloader.clone(),
            transcriber.clone(),
        );

        let input = materializer
            .materialize_dialog_asr_input(dialog_input_with_voice(), now)
            .await;

        assert_eq!(input.message.text, "reclaimed voice text");
        assert_eq!(
            downloader.calls.lock().expect("lock").as_slice(),
            ["voice-file"]
        );
        assert_eq!(
            transcriber.calls.lock().expect("lock").as_slice(),
            [vec![1, 2, 3]]
        );
        let updates = store.updates.lock().expect("lock").clone();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].asr_status, "processing");
        assert_eq!(updates[0].asr_requested_at, Some(now));
        assert_eq!(updates[1].asr_status, "completed");
    }

    #[tokio::test]
    async fn materializer_fails_open_when_transcription_fails() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let record = voice_record(now);
        let store = FakeStore {
            record: Arc::new(Mutex::new(record)),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store.clone(),
            FakeDownloader::default(),
            FakeTranscriber {
                result: Err("asr down".to_owned()),
                calls: Arc::new(Mutex::new(Vec::new())),
            },
        );

        let input = materializer
            .materialize_dialog_asr_input(dialog_input_with_voice(), now)
            .await;

        assert!(input.message.text.is_empty());
        let updates = store.updates.lock().expect("lock").clone();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[1].asr_status, "failed");
        assert_eq!(updates[1].asr_error.as_deref(), Some("asr down"));
    }

    #[tokio::test]
    async fn materializer_marks_missing_latest_file_id_as_failed() {
        let now = OffsetDateTime::from_unix_timestamp(1_770_000_000).expect("timestamp");
        let mut record = voice_record(now);
        record.latest_file_id.clear();
        let store = FakeStore {
            record: Arc::new(Mutex::new(record)),
            updates: Arc::new(Mutex::new(Vec::new())),
        };
        let downloader = FakeDownloader::default();
        let transcriber = FakeTranscriber {
            result: Ok(transcript("unused")),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let materializer = TelegramDialogAsrInputMaterializer::new(
            store.clone(),
            downloader.clone(),
            transcriber.clone(),
        );

        let input = materializer
            .materialize_dialog_asr_input(dialog_input_with_voice(), now)
            .await;

        assert!(input.message.text.is_empty());
        assert!(downloader.calls.lock().expect("lock").is_empty());
        assert!(transcriber.calls.lock().expect("lock").is_empty());
        let updates = store.updates.lock().expect("lock").clone();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].asr_status, "processing");
        assert_eq!(updates[1].asr_status, "failed");
        assert_eq!(
            updates[1].asr_error.as_deref(),
            Some("missing latest Telegram file id")
        );
    }

    #[tokio::test]
    async fn discovery_client_deadline_wraps_pending_work() {
        let client = DiscoveryAsrClient::new(DiscoveryAsrConfig {
            base_url: "http://127.0.0.1:1".to_owned(),
            service_name: DEFAULT_ASR_DISCOVERY_SERVICE_NAME.to_owned(),
            endpoint_name: DEFAULT_ASR_DISCOVERY_ENDPOINT_NAME.to_owned(),
            request_timeout: Duration::from_secs(5),
            task_timeout: Duration::from_millis(1),
            poll_interval: Duration::from_millis(1),
            capacity_wait: Duration::from_millis(1),
        })
        .expect("client");

        let error = client
            .with_deadline(
                future::pending::<Result<(), AsrClientError>>(),
                Instant::now() + Duration::from_millis(1),
                "asr-job",
            )
            .await
            .expect_err("deadline");

        assert!(
            error
                .to_string()
                .contains("discovery job asr-job did not complete")
        );
    }

    #[derive(Clone, Default)]
    struct RetryServerState {
        polls: Arc<AtomicUsize>,
    }

    async fn retry_server_submit() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "job_id": "retry-job",
            "state": "JOB_STATE_RUNNING"
        }))
    }

    async fn retry_server_poll(State(state): State<RetryServerState>) -> Response {
        let poll = state.polls.fetch_add(1, Ordering::SeqCst);
        if poll == 0 {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "temporary"})),
            )
                .into_response();
        }
        let body = if poll == 1 {
            "not valid base64!".to_owned()
        } else {
            general_purpose::URL_SAFE_NO_PAD.encode(
                serde_json::json!({
                    "text": "recovered transcript",
                    "engine": "vosk",
                    "model": "vosk-model",
                    "fallback_used": true,
                    "chunks": 1,
                    "latency_ms": 321,
                    "warnings": ["primary_failed:gigaam:GPU lock is busy"]
                })
                .to_string(),
            )
        };
        Json(serde_json::json!({
            "job_id": "retry-job",
            "state": "JOB_STATE_SUCCEEDED",
            "result": {
                "response": {
                    "status_code": 200,
                    "body": body,
                    "content_type": "application/json"
                }
            }
        }))
        .into_response()
    }

    #[tokio::test]
    async fn discovery_client_retries_poll_and_terminal_payload_failures() {
        let state = RetryServerState::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind retry server");
        let address = listener.local_addr().expect("retry server address");
        let app = Router::new()
            .route("/v1/jobs/blocking", post(retry_server_submit))
            .route("/v1/jobs/{job_id}", get(retry_server_poll))
            .with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve retry server");
        });
        let client = DiscoveryAsrClient::new(DiscoveryAsrConfig {
            base_url: format!("http://{address}"),
            service_name: DEFAULT_ASR_DISCOVERY_SERVICE_NAME.to_owned(),
            endpoint_name: DEFAULT_ASR_DISCOVERY_ENDPOINT_NAME.to_owned(),
            request_timeout: Duration::from_secs(2),
            task_timeout: Duration::from_secs(2),
            poll_interval: Duration::from_millis(1),
            capacity_wait: Duration::from_millis(1),
        })
        .expect("client");

        let transcript = client
            .transcribe_bytes(
                &[1, 2, 3],
                AsrRequest {
                    chat_id: 1,
                    message_id: 2,
                    file_unique_id: "voice".to_owned(),
                    mime_type: Some("audio/ogg".to_owned()),
                    duration_seconds: Some(1),
                },
            )
            .await
            .expect("transcript after retries");

        server.abort();
        assert_eq!(state.polls.load(Ordering::SeqCst), 3);
        assert_eq!(transcript.text, "recovered transcript");
        assert!(transcript.fallback_used);
        assert_eq!(transcript.chunks, 1);
        assert_eq!(
            transcript.warnings,
            ["primary_failed:gigaam:GPU lock is busy"]
        );
    }

    #[test]
    fn decode_asr_response_accepts_url_safe_discovery_body() {
        let payload = r#"{"text":"Процесс биологической экспансии завершён.","engine":"gigaam","model":"gigaam-v3","latency_ms":123}"#;
        let response = DiscoveryJobResponse {
            status_code: 200,
            body: general_purpose::URL_SAFE_NO_PAD.encode(payload),
            content_type: "application/json".to_owned(),
        };

        let decoded = decode_asr_response(&response).expect("decode url-safe ASR body");

        assert_eq!(decoded.text, "Процесс биологической экспансии завершён.");
        assert_eq!(decoded.provider, "gigaam");
        assert_eq!(decoded.model, "gigaam-v3");
        assert_eq!(decoded.latency_ms, 123);
        assert!(!decoded.fallback_used);
        assert_eq!(decoded.chunks, 0);
        assert!(decoded.warnings.is_empty());
    }

    fn dialog_input_with_voice() -> DialogInput {
        DialogInput {
            context: DialogContext {
                chat_id: -100,
                ..DialogContext::default()
            },
            message: DialogMessage {
                id: 42,
                meta: ChatMessageMeta {
                    attachments: vec![ChatAttachment {
                        kind: "voice".to_owned(),
                        source: "message".to_owned(),
                        file_unique_id: "voice-u".to_owned(),
                        duration_seconds: 4,
                        ..ChatAttachment::default()
                    }],
                    ..ChatMessageMeta::default()
                },
                ..DialogMessage::default()
            },
            ..DialogInput::default()
        }
    }

    fn voice_record(now: OffsetDateTime) -> TelegramFileRecord {
        TelegramFileRecord {
            file_unique_id: "voice-u".to_owned(),
            latest_file_id: "voice-file".to_owned(),
            media_kind: "voice".to_owned(),
            mime_type: Some("audio/ogg".to_owned()),
            width: None,
            height: None,
            file_size: Some(123),
            first_seen_chat_id: Some(-100),
            first_seen_message_id: Some(42),
            last_seen_chat_id: Some(-100),
            last_seen_message_id: Some(42),
            last_seen_at: now,
            vision_status: "pending".to_owned(),
            vision_caption: None,
            vision_model: None,
            vision_latency_ms: None,
            recognition_requested_at: None,
            recognition_completed_at: None,
            asr_status: TELEGRAM_FILE_ASR_STATUS_PENDING.to_owned(),
            asr_text: None,
            asr_provider: None,
            asr_model: None,
            asr_latency_ms: None,
            asr_fallback_used: None,
            asr_chunks: None,
            asr_warnings: None,
            asr_error: None,
            asr_requested_at: None,
            asr_completed_at: None,
            extra: serde_json::json!({}),
            created_at: now,
            updated_at: now,
        }
    }

    fn transcript(text: &str) -> AsrTranscript {
        AsrTranscript {
            text: text.to_owned(),
            provider: "gigaam".to_owned(),
            model: "gigaam-v3".to_owned(),
            latency_ms: 1200,
            fallback_used: false,
            chunks: 1,
            warnings: Vec::new(),
        }
    }
}
