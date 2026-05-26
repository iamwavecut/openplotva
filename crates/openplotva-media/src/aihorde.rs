//! AIHorde image provider client.

use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://aihorde.net";
pub const DEFAULT_CLIENT_AGENT: &str = "openplotva:dev";
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(3);
pub const API_BASE_PATH: &str = "/api";
pub const MAX_IMAGES_PER_REQUEST: u32 = 1;
pub const DEFAULT_STEPS: u32 = 20;
pub const DEFAULT_SAMPLER: &str = "k_euler";
pub const DEFAULT_REPLACEMENT_FILTER: bool = true;

/// AIHorde client configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AIHordeConfig {
    /// API key sent as the `apikey` header.
    pub api_key: String,
    /// Base API URL. `/api` is appended unless already present.
    pub base_url: String,
    /// AIHorde `Client-Agent` header.
    pub client_agent: String,
    /// HTTP request timeout.
    pub request_timeout: Duration,
    /// Poll interval for async generation checks.
    pub poll_interval: Duration,
}

impl Default for AIHordeConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            base_url: String::new(),
            client_agent: String::new(),
            request_timeout: Duration::ZERO,
            poll_interval: Duration::ZERO,
        }
    }
}

impl AIHordeConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.api_key = self.api_key.trim().to_owned();
        self.base_url = if self.base_url.trim().is_empty() {
            DEFAULT_BASE_URL.to_owned()
        } else {
            self.base_url.trim().trim_end_matches('/').to_owned()
        };
        self.client_agent = if self.client_agent.trim().is_empty() {
            DEFAULT_CLIENT_AGENT.to_owned()
        } else {
            self.client_agent.trim().to_owned()
        };
        if self.request_timeout == Duration::ZERO {
            self.request_timeout = DEFAULT_REQUEST_TIMEOUT;
        }
        if self.poll_interval == Duration::ZERO {
            self.poll_interval = DEFAULT_POLL_INTERVAL;
        }
        self
    }

    #[must_use]
    pub fn configured(&self) -> bool {
        !self.api_key.trim().is_empty()
    }
}

/// AIHorde image generation request.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AIHordeRequest {
    /// Positive prompt.
    pub prompt: String,
    /// Negative prompt joined with `###` when present.
    pub negative_prompt: String,
    /// Requested width.
    pub width: u32,
    /// Requested height.
    pub height: u32,
    /// Requested step count. Defaults to 20 when zero.
    pub steps: u32,
    /// CFG scale, omitted when zero.
    pub cfg_scale: f64,
    /// Sampler name. Defaults to `k_euler` when empty.
    pub sampler_name: String,
    /// Seed, omitted when zero.
    pub seed: i64,
    /// Optional model list.
    pub models: Vec<String>,
    /// AIHorde `nsfw` flag.
    pub nsfw: bool,
    /// AIHorde `censor_nsfw` flag.
    pub censor_nsfw: bool,
    /// AIHorde `trusted_workers` flag.
    pub trusted_workers: bool,
    /// Per-request poll interval. Defaults to config when zero.
    pub poll_interval: Duration,
}

/// AIHorde image generation result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AIHordeResult {
    /// Downloaded image bytes.
    pub images: Vec<Vec<u8>>,
}

/// AIHorde async submit response.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct AIHordeAsyncResponse {
    /// Request ID used for polling.
    #[serde(default)]
    pub id: String,
    /// Kudos estimate.
    #[serde(default)]
    pub kudos: f64,
    /// Provider message.
    #[serde(default)]
    pub message: String,
    /// Provider warnings.
    #[serde(default)]
    pub warnings: Vec<AIHordeWarning>,
}

/// AIHorde warning payload.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct AIHordeWarning {
    /// Warning code.
    #[serde(default)]
    pub code: String,
    /// Warning message.
    #[serde(default)]
    pub message: String,
}

/// AIHorde check/status common polling payload.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct AIHordeStatusCheck {
    /// Finished generation count.
    #[serde(default)]
    pub finished: u32,
    /// Processing generation count.
    #[serde(default)]
    pub processing: u32,
    /// Restarted generation count.
    #[serde(default)]
    pub restarted: u32,
    /// Waiting generation count.
    #[serde(default)]
    pub waiting: u32,
    /// Whether the request is complete.
    #[serde(default)]
    pub done: bool,
    /// Whether the request faulted.
    #[serde(default)]
    pub faulted: bool,
    /// Server-advised wait time in seconds.
    #[serde(default)]
    pub wait_time: u32,
    /// Queue position.
    #[serde(default)]
    pub queue_position: u32,
    /// Kudos charged or estimated.
    #[serde(default)]
    pub kudos: f64,
    /// Whether generation is possible.
    #[serde(default)]
    pub is_possible: bool,
}

/// AIHorde final status payload.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct AIHordeRequestStatus {
    /// Common status fields.
    #[serde(flatten)]
    pub check: AIHordeStatusCheck,
    /// Finished generations.
    #[serde(default)]
    pub generations: Vec<AIHordeGeneration>,
    /// Shared flag.
    #[serde(default)]
    pub shared: bool,
}

/// AIHorde generation payload.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct AIHordeGeneration {
    /// Worker ID.
    #[serde(default)]
    pub worker_id: String,
    /// Worker name.
    #[serde(default)]
    pub worker_name: String,
    /// Model name.
    #[serde(default)]
    pub model: String,
    /// Generation state.
    #[serde(default)]
    pub state: String,
    /// Image URL.
    #[serde(default)]
    pub img: String,
    /// Provider seed value.
    #[serde(default)]
    pub seed: Value,
    /// Generation ID.
    #[serde(default)]
    pub id: String,
    /// Whether this generation was censored.
    #[serde(default)]
    pub censored: bool,
}

/// AIHorde provider error response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AIHordeProviderError {
    /// HTTP status.
    pub status: u16,
    /// Provider message.
    pub message: String,
    /// Provider reason code.
    pub rc: String,
    /// Retry delay, when known.
    pub retry_after: Duration,
}

impl fmt::Display for AIHordeProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.message.is_empty() {
            return f.write_str(&self.message);
        }
        if !self.rc.is_empty() {
            return f.write_str(&self.rc);
        }
        if self.status != 0 {
            return write!(f, "aihorde error status {}", self.status);
        }
        f.write_str("aihorde error")
    }
}

impl std::error::Error for AIHordeProviderError {}

/// AIHorde client errors.
#[derive(Debug, Error)]
pub enum AIHordeError {
    /// HTTP client construction failed.
    #[error("build aihorde HTTP client: {0}")]
    BuildHttpClient(reqwest::Error),
    /// Client is missing credentials.
    #[error("aihorde client is not configured")]
    NotConfigured,
    /// Prompt was empty after trimming.
    #[error("prompt is empty")]
    EmptyPrompt,
    /// Provider returned an async response without an ID.
    #[error("aihorde returned empty id")]
    EmptyId,
    /// Request faulted during polling.
    #[error("aihorde request faulted")]
    Faulted,
    /// Final status omitted generations.
    #[error("aihorde returned empty generations list")]
    EmptyGenerationsList,
    /// Final status had no image URLs.
    #[error("aihorde status has no image urls")]
    StatusHasNoImageUrls,
    /// All image downloads were empty.
    #[error("no images downloaded from aihorde")]
    NoDownloadedImages,
    /// HTTP request failed.
    #[error("send aihorde {operation} request: {source}")]
    Http {
        /// Operation name.
        operation: &'static str,
        /// Reqwest error.
        source: reqwest::Error,
    },
    /// Response JSON failed to decode.
    #[error("decode aihorde {operation} response: {source}")]
    Json {
        /// Operation name.
        operation: &'static str,
        /// JSON error.
        source: serde_json::Error,
    },
    /// Provider returned a non-success status.
    #[error(transparent)]
    Provider(#[from] AIHordeProviderError),
    /// Image download returned a non-success status.
    #[error("aihorde image download failed with status {status}: {body}")]
    DownloadStatus {
        /// HTTP status.
        status: u16,
        /// Response body preview.
        body: String,
    },
}

/// Reqwest-backed AIHorde client.
#[derive(Clone, Debug)]
pub struct AIHordeClient {
    cfg: AIHordeConfig,
    http: reqwest::Client,
}

impl AIHordeClient {
    /// Build a reqwest-backed AIHorde client.
    pub fn new(cfg: AIHordeConfig) -> Result<Self, AIHordeError> {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .map_err(AIHordeError::BuildHttpClient)?;
        Ok(Self { cfg, http })
    }

    pub async fn generate(&self, req: AIHordeRequest) -> Result<AIHordeResult, AIHordeError> {
        if !self.cfg.configured() {
            return Err(AIHordeError::NotConfigured);
        }
        let prompt = req.prompt.trim();
        if prompt.is_empty() {
            return Err(AIHordeError::EmptyPrompt);
        }

        let task_id = self
            .start_generation(build_aihorde_payload(prompt, &req))
            .await?;
        let poll_interval = request_poll_interval(req.poll_interval, self.cfg.poll_interval);
        self.wait_for_completion(&task_id, poll_interval).await?;
        let status = self.fetch_status(&task_id).await?;
        if status.generations.is_empty() {
            return Err(AIHordeError::EmptyGenerationsList);
        }
        let urls = aihorde_image_urls(&status.generations);
        if urls.is_empty() {
            return Err(AIHordeError::StatusHasNoImageUrls);
        }
        let images = self.download_images(urls).await?;
        Ok(AIHordeResult { images })
    }

    /// Poll the lightweight AIHorde check endpoint.
    pub async fn check_status(&self, id: &str) -> Result<AIHordeStatusCheck, AIHordeError> {
        self.get_json(&format!("/v2/generate/check/{id}"), "check")
            .await
    }

    /// Fetch the final AIHorde status endpoint.
    pub async fn fetch_status(&self, id: &str) -> Result<AIHordeRequestStatus, AIHordeError> {
        self.get_json(&format!("/v2/generate/status/{id}"), "status")
            .await
    }

    async fn start_generation(
        &self,
        payload: AIHordeGeneratePayload,
    ) -> Result<String, AIHordeError> {
        let response = self
            .http
            .post(self.api_url("/v2/generate/async"))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .header("apikey", &self.cfg.api_key)
            .header("Client-Agent", &self.cfg.client_agent)
            .json(&payload)
            .send()
            .await
            .map_err(|source| AIHordeError::Http {
                operation: "async",
                source,
            })?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("Retry-After")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response
            .bytes()
            .await
            .map_err(|source| AIHordeError::Http {
                operation: "async",
                source,
            })?
            .to_vec();
        if status != 202 {
            return Err(parse_aihorde_error(status, &retry_after, &body).into());
        }
        let decoded: AIHordeAsyncResponse =
            serde_json::from_slice(&body).map_err(|source| AIHordeError::Json {
                operation: "async",
                source,
            })?;
        let id = decoded.id.trim();
        if id.is_empty() {
            return Err(AIHordeError::EmptyId);
        }
        Ok(id.to_owned())
    }

    async fn wait_for_completion(
        &self,
        task_id: &str,
        poll_interval: Duration,
    ) -> Result<(), AIHordeError> {
        loop {
            let check = self.check_status(task_id).await?;
            if check.faulted {
                return Err(AIHordeError::Faulted);
            }
            if check.done {
                return Ok(());
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn get_json<T>(&self, path: &str, operation: &'static str) -> Result<T, AIHordeError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = self
            .http
            .get(self.api_url(path))
            .header("Accept", "application/json")
            .header("apikey", &self.cfg.api_key)
            .header("Client-Agent", &self.cfg.client_agent)
            .send()
            .await
            .map_err(|source| AIHordeError::Http { operation, source })?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("Retry-After")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response
            .bytes()
            .await
            .map_err(|source| AIHordeError::Http { operation, source })?
            .to_vec();
        if status != 200 {
            return Err(parse_aihorde_error(status, &retry_after, &body).into());
        }
        serde_json::from_slice(&body).map_err(|source| AIHordeError::Json { operation, source })
    }

    async fn download_images(&self, urls: Vec<String>) -> Result<Vec<Vec<u8>>, AIHordeError> {
        let mut images = Vec::new();
        for url in urls {
            let image = self.download_image(&url).await?;
            if !image.is_empty() {
                images.push(image);
            }
        }
        if images.is_empty() {
            return Err(AIHordeError::NoDownloadedImages);
        }
        Ok(images)
    }

    async fn download_image(&self, url: &str) -> Result<Vec<u8>, AIHordeError> {
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|source| AIHordeError::Http {
                operation: "download",
                source,
            })?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|source| AIHordeError::Http {
                operation: "download",
                source,
            })?
            .to_vec();
        if status >= 300 {
            return Err(AIHordeError::DownloadStatus {
                status,
                body: body_preview(&body),
            });
        }
        Ok(body)
    }

    fn api_url(&self, path: &str) -> String {
        if self.cfg.base_url.ends_with(API_BASE_PATH) {
            format!("{}{}", self.cfg.base_url, path)
        } else {
            format!("{}{}{}", self.cfg.base_url, API_BASE_PATH, path)
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct AIHordeGeneratePayload {
    prompt: String,
    params: AIHordeModelParams,
    #[serde(skip_serializing_if = "is_false")]
    nsfw: bool,
    #[serde(skip_serializing_if = "is_false")]
    trusted_workers: bool,
    #[serde(skip_serializing_if = "is_false")]
    censor_nsfw: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    models: Vec<String>,
    #[serde(skip_serializing_if = "is_false")]
    r2: bool,
    #[serde(skip_serializing_if = "is_false")]
    shared: bool,
    #[serde(skip_serializing_if = "is_false")]
    replacement_filter: bool,
}

#[derive(Clone, Debug, Serialize)]
struct AIHordeModelParams {
    #[serde(skip_serializing_if = "is_zero_u32")]
    width: u32,
    #[serde(skip_serializing_if = "is_zero_u32")]
    height: u32,
    #[serde(skip_serializing_if = "is_zero_u32")]
    steps: u32,
    #[serde(skip_serializing_if = "is_zero_u32")]
    n: u32,
    #[serde(skip_serializing_if = "is_zero_f64")]
    cfg_scale: f64,
    #[serde(skip_serializing_if = "String::is_empty")]
    sampler_name: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    seed: i64,
}

fn build_aihorde_payload(prompt: &str, req: &AIHordeRequest) -> AIHordeGeneratePayload {
    AIHordeGeneratePayload {
        prompt: format_aihorde_prompt(prompt, &req.negative_prompt),
        params: build_aihorde_params(req),
        nsfw: req.nsfw,
        trusted_workers: req.trusted_workers,
        censor_nsfw: req.censor_nsfw,
        models: trim_strings(&req.models),
        r2: true,
        shared: false,
        replacement_filter: DEFAULT_REPLACEMENT_FILTER,
    }
}

fn build_aihorde_params(req: &AIHordeRequest) -> AIHordeModelParams {
    AIHordeModelParams {
        width: req.width,
        height: req.height,
        steps: if req.steps == 0 {
            DEFAULT_STEPS
        } else {
            req.steps
        },
        n: MAX_IMAGES_PER_REQUEST,
        cfg_scale: req.cfg_scale,
        sampler_name: if req.sampler_name.trim().is_empty() {
            DEFAULT_SAMPLER.to_owned()
        } else {
            req.sampler_name.trim().to_owned()
        },
        seed: req.seed,
    }
}

#[must_use]
pub fn format_aihorde_prompt(prompt: &str, negative_prompt: &str) -> String {
    let prompt = prompt.trim();
    let negative_prompt = negative_prompt.trim();
    if prompt.is_empty() {
        return String::new();
    }
    if negative_prompt.is_empty() {
        return prompt.to_owned();
    }
    format!("{prompt}###{negative_prompt}")
}

/// Return non-empty AIHorde image URLs from final generations.
#[must_use]
pub fn aihorde_image_urls(generations: &[AIHordeGeneration]) -> Vec<String> {
    generations
        .iter()
        .filter_map(|generation| {
            let img = generation.img.trim();
            (!img.is_empty()).then(|| img.to_owned())
        })
        .collect()
}

#[must_use]
pub fn parse_aihorde_error(
    status: u16,
    retry_after_header: &str,
    body: &[u8],
) -> AIHordeProviderError {
    let decoded = serde_json::from_slice::<AIHordeRequestError>(body).unwrap_or_default();
    AIHordeProviderError {
        status,
        message: decoded.message.trim().to_owned(),
        rc: decoded.rc.trim().to_owned(),
        retry_after: parse_retry_after(retry_after_header),
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct AIHordeRequestError {
    #[serde(default)]
    message: String,
    #[serde(default)]
    rc: String,
}

fn request_poll_interval(requested: Duration, fallback: Duration) -> Duration {
    if requested == Duration::ZERO {
        fallback
    } else {
        requested
    }
}

fn trim_strings(values: &[String]) -> Vec<String> {
    values
        .iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
        .collect()
}

fn parse_retry_after(value: &str) -> Duration {
    let value = value.trim();
    if value.is_empty() {
        return Duration::ZERO;
    }
    if let Ok(seconds) = value.parse::<u64>()
        && seconds > 0
    {
        return Duration::from_secs(seconds);
    }
    parse_http_retry_after(value).unwrap_or(Duration::ZERO)
}

fn parse_http_retry_after(value: &str) -> Option<Duration> {
    let (_, rest) = value.split_once(',')?;
    let mut parts = rest.split_whitespace();
    let day = parts.next()?.parse::<u32>().ok()?;
    let month = month_number(parts.next()?)?;
    let year = parts.next()?.parse::<i32>().ok()?;
    let mut time_parts = parts.next()?.split(':');
    let hour = time_parts.next()?.parse::<u32>().ok()?;
    let minute = time_parts.next()?.parse::<u32>().ok()?;
    let second = time_parts.next()?.parse::<u32>().ok()?;
    let target_seconds = unix_seconds(year, month, day, hour, minute, second)?;
    let target = UNIX_EPOCH + Duration::from_secs(target_seconds);
    target.duration_since(SystemTime::now()).ok()
}

fn month_number(month: &str) -> Option<u32> {
    match month {
        "Jan" => Some(1),
        "Feb" => Some(2),
        "Mar" => Some(3),
        "Apr" => Some(4),
        "May" => Some(5),
        "Jun" => Some(6),
        "Jul" => Some(7),
        "Aug" => Some(8),
        "Sep" => Some(9),
        "Oct" => Some(10),
        "Nov" => Some(11),
        "Dec" => Some(12),
        _ => None,
    }
}

fn unix_seconds(
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<u64> {
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let days = days_from_civil(year, month, day);
    if days < 0 {
        return None;
    }
    Some(
        days as u64 * 86_400 + u64::from(hour) * 3_600 + u64::from(minute) * 60 + u64::from(second),
    )
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i32;
    let day = day as i32;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    i64::from(era * 146_097 + doe - 719_468)
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_u32(value: &u32) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn body_preview(body: &[u8]) -> String {
    let preview = String::from_utf8_lossy(body).trim().to_owned();
    if preview.len() > 400 {
        preview[..400].to_owned()
    } else {
        preview
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
        time::Duration as StdDuration,
    };

    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn aihorde_generate_sends_go_payload_polls_and_downloads_image() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
        let addr = listener.local_addr().expect("local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            let image_url = format!("http://{addr}/image.webp");
            for step in 0..5 {
                let (mut stream, _) = listener.accept().expect("accept request");
                let request = read_request(&mut stream);
                captured.lock().expect("requests").push(request);
                match step {
                    0 => {
                        write_response(&mut stream, 202, "application/json", br#"{"id":"task-1"}"#)
                    }
                    1 => write_response(
                        &mut stream,
                        200,
                        "application/json",
                        br#"{"done":false,"wait_time":1}"#,
                    ),
                    2 => write_response(
                        &mut stream,
                        200,
                        "application/json",
                        br#"{"done":true,"wait_time":0}"#,
                    ),
                    3 => write_response(
                        &mut stream,
                        200,
                        "application/json",
                        format!(r#"{{"done":true,"generations":[{{"img":"{image_url}"}}]}}"#)
                            .as_bytes(),
                    ),
                    _ => write_response(&mut stream, 200, "image/webp", b"image-bytes"),
                }
            }
        });

        let client = AIHordeClient::new(AIHordeConfig {
            api_key: "test-key".to_owned(),
            base_url: format!("http://{addr}"),
            client_agent: "test-agent".to_owned(),
            request_timeout: StdDuration::from_secs(5),
            poll_interval: StdDuration::from_millis(1),
        })
        .expect("client");

        let result = client
            .generate(AIHordeRequest {
                prompt: " cat ".to_owned(),
                negative_prompt: " blur ".to_owned(),
                width: 512,
                height: 512,
                steps: 7,
                cfg_scale: 6.5,
                sampler_name: " k_euler_a ".to_owned(),
                seed: 42,
                models: vec![" Z-Image-Turbo ".to_owned(), " ".to_owned()],
                nsfw: true,
                censor_nsfw: true,
                trusted_workers: true,
                poll_interval: StdDuration::from_millis(1),
            })
            .await
            .expect("generate");

        handle.join().expect("server thread");
        assert_eq!(result.images, vec![b"image-bytes".to_vec()]);

        let requests = requests.lock().expect("requests");
        assert_eq!(requests.len(), 5);
        assert!(requests[0].starts_with("POST /api/v2/generate/async HTTP/1.1"));
        assert!(contains_header(&requests[0], "apikey", "test-key"));
        assert!(contains_header(&requests[0], "client-agent", "test-agent"));
        assert!(contains_header(
            &requests[0],
            "content-type",
            "application/json"
        ));
        assert!(contains_header(&requests[0], "accept", "application/json"));
        let body = requests[0].split("\r\n\r\n").nth(1).expect("body");
        let payload: Value = serde_json::from_str(body).expect("payload");
        assert_eq!(payload["prompt"], "cat###blur");
        assert_eq!(payload["params"]["width"], 512);
        assert_eq!(payload["params"]["height"], 512);
        assert_eq!(payload["params"]["steps"], 7);
        assert_eq!(payload["params"]["n"], MAX_IMAGES_PER_REQUEST);
        assert_eq!(payload["params"]["cfg_scale"].as_f64(), Some(6.5));
        assert_eq!(payload["params"]["sampler_name"], "k_euler_a");
        assert_eq!(payload["params"]["seed"], 42);
        assert_eq!(payload["models"], serde_json::json!(["Z-Image-Turbo"]));
        assert_eq!(payload["nsfw"], true);
        assert_eq!(payload["censor_nsfw"], true);
        assert_eq!(payload["trusted_workers"], true);
        assert_eq!(payload["r2"], true);
        assert_eq!(payload["replacement_filter"], true);
        assert!(payload.get("shared").is_none());

        assert!(requests[1].starts_with("GET /api/v2/generate/check/task-1 HTTP/1.1"));
        assert!(requests[2].starts_with("GET /api/v2/generate/check/task-1 HTTP/1.1"));
        assert!(requests[3].starts_with("GET /api/v2/generate/status/task-1 HTTP/1.1"));
        assert!(requests[4].starts_with("GET /image.webp HTTP/1.1"));
    }

    #[tokio::test]
    async fn aihorde_status_endpoints_decode_go_shapes() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
        let addr = listener.local_addr().expect("local addr");
        let handle = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept request");
                let request = read_request(&mut stream);
                if request.starts_with("GET /api/v2/generate/check/task-1 HTTP/1.1") {
                    write_response(
                        &mut stream,
                        200,
                        "application/json",
                        br#"{"done":true,"wait_time":2}"#,
                    );
                } else if request.starts_with("GET /api/v2/generate/status/task-1 HTTP/1.1") {
                    write_response(
                        &mut stream,
                        200,
                        "application/json",
                        br#"{"done":true,"generations":[{"img":"https://example.test/image.webp"}]}"#,
                    );
                } else {
                    write_response(&mut stream, 404, "text/plain", b"not found");
                }
            }
        });
        let client = AIHordeClient::new(AIHordeConfig {
            api_key: "test-key".to_owned(),
            base_url: format!("http://{addr}"),
            client_agent: "test-agent".to_owned(),
            request_timeout: StdDuration::from_secs(5),
            poll_interval: StdDuration::from_millis(1),
        })
        .expect("client");

        let check = client.check_status("task-1").await.expect("check");
        assert!(check.done);
        assert_eq!(check.wait_time, 2);
        let status = client.fetch_status("task-1").await.expect("status");
        assert_eq!(status.generations.len(), 1);
        assert_eq!(status.generations[0].img, "https://example.test/image.webp");
        handle.join().expect("server thread");
    }

    #[test]
    fn aihorde_error_and_prompt_helpers_match_go_contract() {
        assert_eq!(format_aihorde_prompt(" cat ", " blur "), "cat###blur");
        assert_eq!(format_aihorde_prompt(" cat ", " "), "cat");
        assert_eq!(format_aihorde_prompt(" ", " blur "), "");

        let err = parse_aihorde_error(
            429,
            "7",
            br#"{"message":"slow down","rc":"TooManyRequests"}"#,
        );
        assert_eq!(err.status, 429);
        assert_eq!(err.message, "slow down");
        assert_eq!(err.rc, "TooManyRequests");
        assert_eq!(err.retry_after, StdDuration::from_secs(7));
        assert_eq!(err.to_string(), "slow down");

        let err = parse_aihorde_error(500, "", br#"{"rc":"ServerDown"}"#);
        assert_eq!(err.to_string(), "ServerDown");
    }

    fn read_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(StdDuration::from_secs(2)))
            .expect("read timeout");
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read request");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if request_complete(&buffer) {
                break;
            }
        }
        String::from_utf8(buffer).expect("utf8 request")
    }

    fn request_complete(buffer: &[u8]) -> bool {
        let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
            return false;
        };
        let headers = String::from_utf8_lossy(&buffer[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        buffer.len() >= header_end + 4 + content_length
    }

    fn write_response(stream: &mut TcpStream, status: u16, content_type: &str, body: &[u8]) {
        write!(
            stream,
            "HTTP/1.1 {status} OK\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .expect("write headers");
        stream.write_all(body).expect("write body");
        stream.flush().expect("flush");
    }

    fn contains_header(request: &str, name: &str, expected: &str) -> bool {
        request.lines().any(|line| {
            let Some((header, value)) = line.split_once(':') else {
                return false;
            };
            header.eq_ignore_ascii_case(name) && value.trim() == expected
        })
    }
}
