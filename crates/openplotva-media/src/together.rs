//! Together.ai image provider client.

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::time::Instant;

pub const DEFAULT_BASE_URL: &str = "https://api.together.xyz/v1";
pub const DEFAULT_DRAW_MODEL: &str = "black-forest-labs/FLUX.1-schnell-Free";
pub const DEFAULT_SIDE: u32 = 1024;
pub const DEFAULT_STEPS: u32 = 4;
pub const DEFAULT_N: u32 = 1;
pub const DEFAULT_RATE_LIMIT: Duration = Duration::from_secs(11);
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(110);
pub const DEFAULT_USER_AGENTS: [&str; 8] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/125.0.0.0 Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
];

const TOGETHER_NSFW_MESSAGE: &str =
    "🙈 Кисти покраснели: запрос похоже NSFW. Подкинь менее пикантный промпт";

/// Together client configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TogetherConfig {
    /// Bearer API key.
    pub api_key: String,
    /// API base URL ending in `/v1`.
    pub base_url: String,
    /// Local fallback wait used when Together rate-limit headers do not provide a longer delay.
    pub rate_limit_duration: Duration,
    /// HTTP request timeout.
    pub request_timeout: Duration,
    /// Browser-like User-Agent rotation list.
    pub user_agents: Vec<String>,
}

impl Default for TogetherConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            base_url: String::new(),
            rate_limit_duration: Duration::ZERO,
            request_timeout: Duration::ZERO,
            user_agents: Vec::new(),
        }
    }
}

impl TogetherConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.api_key = self.api_key.trim().to_owned();
        self.base_url = if self.base_url.trim().is_empty() {
            DEFAULT_BASE_URL.to_owned()
        } else {
            self.base_url.trim().trim_end_matches('/').to_owned()
        };
        if self.rate_limit_duration == Duration::ZERO {
            self.rate_limit_duration = DEFAULT_RATE_LIMIT;
        }
        if self.request_timeout == Duration::ZERO {
            self.request_timeout = DEFAULT_REQUEST_TIMEOUT;
        }
        if self.user_agents.is_empty() {
            self.user_agents = DEFAULT_USER_AGENTS
                .iter()
                .map(ToString::to_string)
                .collect();
        }
        self
    }

    #[must_use]
    pub fn configured(&self) -> bool {
        !self.api_key.trim().is_empty()
    }
}

/// Together image generation request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TogetherImageRequest {
    /// Image prompt.
    pub prompt: String,
    pub model: String,
    pub steps: Option<u32>,
    /// Optional seed.
    pub seed: Option<i64>,
    /// Optional requested image count. Defaults to one.
    pub n: Option<u32>,
    /// Optional height. Defaults to 1024.
    pub height: Option<u32>,
    /// Optional width. Defaults to 1024.
    pub width: Option<u32>,
    /// Optional negative prompt.
    pub negative_prompt: String,
    /// Per-request timeout. Defaults to config when zero.
    pub timeout: Duration,
    /// If true, return a rate-limit error immediately instead of waiting for the local gate.
    pub no_wait: bool,
}

/// Together image generation result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TogetherImageResult {
    /// First returned image URL.
    pub url: String,
    /// Full decoded Together response.
    pub response: TogetherImageResponse,
}

/// Together `/images/generations` response.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TogetherImageResponse {
    /// Provider request ID.
    #[serde(default)]
    pub id: String,
    /// Provider model name.
    #[serde(default)]
    pub model: String,
    /// Provider object type.
    #[serde(default)]
    pub object: String,
    /// Image choices.
    #[serde(default)]
    pub data: Vec<TogetherImageChoice>,
}

/// Together image choice payload.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TogetherImageChoice {
    /// Choice index.
    #[serde(default)]
    pub index: i32,
    /// Base64 image payload when returned inline.
    #[serde(default)]
    pub b64_json: String,
    /// Image URL when returned remotely.
    #[serde(default)]
    pub url: String,
}

/// Together client errors.
#[derive(Debug, Error)]
pub enum TogetherError {
    /// HTTP client construction failed.
    #[error("build together HTTP client: {0}")]
    BuildHttpClient(reqwest::Error),
    /// Missing API key.
    #[error("no together api key configured")]
    NotConfigured,
    /// HTTP request failed.
    #[error("send together request: {0}")]
    Http(reqwest::Error),
    /// Response JSON failed to decode.
    #[error("decode together response: {0}")]
    Json(serde_json::Error),
    /// Provider returned a non-success status or local rate gate rejected the request.
    #[error("{message}")]
    ProviderStatus {
        status: u16,
        /// User/operator-facing message.
        message: String,
        /// Retry delay, when known.
        retry_after: Duration,
    },
    /// Successful response omitted the first image URL.
    #[error("no image URL in response")]
    MissingImageUrl,
}

impl TogetherError {
    #[must_use]
    pub const fn status(&self) -> Option<u16> {
        match self {
            Self::ProviderStatus { status, .. } => Some(*status),
            _ => None,
        }
    }

    #[must_use]
    pub const fn retry_after(&self) -> Duration {
        match self {
            Self::ProviderStatus { retry_after, .. } => *retry_after,
            _ => Duration::ZERO,
        }
    }
}

/// Reqwest-backed Together image client.
#[derive(Clone, Debug)]
pub struct TogetherClient {
    cfg: TogetherConfig,
    http: reqwest::Client,
    next_ready_at: Arc<Mutex<Option<Instant>>>,
    user_agent_index: Arc<AtomicUsize>,
}

impl TogetherClient {
    /// Build a reqwest-backed Together client.
    pub fn new(cfg: TogetherConfig) -> Result<Self, TogetherError> {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .map_err(TogetherError::BuildHttpClient)?;
        Ok(Self {
            cfg,
            http,
            next_ready_at: Arc::new(Mutex::new(None)),
            user_agent_index: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Return the local rate gate instant, if the client is currently waiting.
    #[must_use]
    pub fn next_allowed_at(&self) -> Option<Instant> {
        let ready_at = *self.next_ready_at.lock().expect("together rate mutex");
        ready_at.filter(|value| *value > Instant::now())
    }

    /// Generate one image URL through Together's synchronous image endpoint.
    pub async fn images_generate(
        &self,
        req: TogetherImageRequest,
    ) -> Result<TogetherImageResult, TogetherError> {
        if !self.cfg.configured() {
            return Err(TogetherError::NotConfigured);
        }
        self.wait_until_ready(req.no_wait).await?;

        let payload = together_image_payload(&req);
        let timeout = if req.timeout == Duration::ZERO {
            self.cfg.request_timeout
        } else {
            req.timeout
        };
        let response = self
            .http
            .post(format!("{}/images/generations", self.cfg.base_url))
            .header("Authorization", format!("Bearer {}", self.cfg.api_key))
            .header("User-Agent", self.next_user_agent())
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .timeout(timeout)
            .json(&payload)
            .send()
            .await
            .map_err(TogetherError::Http)?;

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .bytes()
            .await
            .map_err(TogetherError::Http)?
            .to_vec();
        if status != 200 {
            let err = decode_together_error(status, &headers, &body, self.cfg.rate_limit_duration);
            let wait = err.retry_after().max(exhausted_rate_limit_wait(
                &headers,
                self.cfg.rate_limit_duration,
            ));
            self.remember_next_ready(wait);
            return Err(err);
        }

        let decoded: TogetherImageResponse =
            serde_json::from_slice(&body).map_err(TogetherError::Json)?;
        let url = decoded
            .data
            .first()
            .map(|choice| choice.url.trim())
            .unwrap_or_default();
        if url.is_empty() {
            return Err(TogetherError::MissingImageUrl);
        }
        self.remember_next_ready(exhausted_rate_limit_wait(
            &headers,
            self.cfg.rate_limit_duration,
        ));
        Ok(TogetherImageResult {
            url: url.to_owned(),
            response: decoded,
        })
    }

    async fn wait_until_ready(&self, no_wait: bool) -> Result<(), TogetherError> {
        let ready_at = *self.next_ready_at.lock().expect("together rate mutex");
        let Some(ready_at) = ready_at else {
            return Ok(());
        };
        let now = Instant::now();
        if ready_at <= now {
            return Ok(());
        }
        let wait = ready_at.duration_since(now);
        if no_wait {
            return Err(TogetherError::ProviderStatus {
                status: 429,
                message: "rate limited".to_owned(),
                retry_after: wait,
            });
        }
        tokio::time::sleep_until(ready_at).await;
        Ok(())
    }

    fn next_user_agent(&self) -> String {
        let index = self.user_agent_index.fetch_add(1, Ordering::Relaxed);
        self.cfg.user_agents[index % self.cfg.user_agents.len()].clone()
    }

    fn remember_next_ready(&self, wait: Duration) {
        if wait == Duration::ZERO {
            return;
        }
        *self.next_ready_at.lock().expect("together rate mutex") = Some(Instant::now() + wait);
    }
}

#[derive(Clone, Debug, Serialize)]
struct TogetherImagePayload {
    prompt: String,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    steps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    n: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    width: Option<u32>,
    #[serde(skip_serializing_if = "String::is_empty")]
    negative_prompt: String,
}

fn together_image_payload(req: &TogetherImageRequest) -> TogetherImagePayload {
    TogetherImagePayload {
        prompt: req.prompt.clone(),
        model: if req.model.is_empty() {
            DEFAULT_DRAW_MODEL.to_owned()
        } else {
            req.model.clone()
        },
        steps: Some(req.steps.unwrap_or(DEFAULT_STEPS)),
        seed: req.seed,
        n: Some(req.n.unwrap_or(DEFAULT_N)),
        height: Some(req.height.unwrap_or(DEFAULT_SIDE)),
        width: Some(req.width.unwrap_or(DEFAULT_SIDE)),
        negative_prompt: req.negative_prompt.clone(),
    }
}

#[must_use]
pub fn decode_together_error(
    status: u16,
    headers: &HeaderMap,
    body: &[u8],
    default_rate_limit: Duration,
) -> TogetherError {
    if status == 429 {
        return TogetherError::ProviderStatus {
            status,
            message: "rate limited by Together.ai".to_owned(),
            retry_after: rate_limit_retry_after(headers, default_rate_limit),
        };
    }
    if status == 422 && together_nsfw_error(body) {
        return TogetherError::ProviderStatus {
            status: 420,
            message: TOGETHER_NSFW_MESSAGE.to_owned(),
            retry_after: Duration::ZERO,
        };
    }
    TogetherError::ProviderStatus {
        status,
        message: together_status_description(status).to_owned(),
        retry_after: Duration::ZERO,
    }
}

#[must_use]
pub fn rate_limit_retry_after(headers: &HeaderMap, default_rate_limit: Duration) -> Duration {
    let mut retry_after = if default_rate_limit == Duration::ZERO {
        DEFAULT_RATE_LIMIT
    } else {
        default_rate_limit
    };
    if let Some(seconds) = header_duration_seconds(headers, "Retry-After") {
        retry_after = seconds;
    }
    if let Some(reset) = header_duration_seconds(headers, "x-ratelimit-reset") {
        retry_after = retry_after.max(reset);
    }
    retry_after
}

#[must_use]
pub fn exhausted_rate_limit_wait(headers: &HeaderMap, default_rate_limit: Duration) -> Duration {
    let Some(remaining) = header_i64(headers, "x-ratelimit-remaining") else {
        return Duration::ZERO;
    };
    if remaining > 0 {
        return Duration::ZERO;
    }
    let mut wait = if default_rate_limit == Duration::ZERO {
        DEFAULT_RATE_LIMIT
    } else {
        default_rate_limit
    };
    if let Some(reset) = header_duration_seconds(headers, "x-ratelimit-reset") {
        wait = wait.max(reset);
    }
    wait
}

#[must_use]
pub fn together_nsfw_error(body: &[u8]) -> bool {
    let Ok(api_err) = serde_json::from_slice::<TogetherApiError>(body) else {
        return false;
    };
    together_api_error_nsfw(&api_err)
}

#[must_use]
pub const fn together_status_description(status: u16) -> &'static str {
    match status {
        400 => "🎨 Попросили квадратный круг. Подправим запрос?",
        401 => "🔑 Ключ от мастерской потерялся. Проверь доступ.",
        402 => "💸 Моне намекает: нужен платёж.",
        403 => "🚫 Палитра запрещённых оттенков. Доступ закрыт.",
        404 => "🕳️ Холст испарился. Не найдено.",
        500 => "🤒 Мастерская приболела. Сервер сломался.",
        503 => "🧹 Все кисти заняты. Попробуем позже.",
        _ => "что-то пошло не так",
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TogetherApiError {
    #[serde(default)]
    error: TogetherApiErrorBody,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct TogetherApiErrorBody {
    #[serde(default)]
    message: String,
    #[serde(rename = "type", default)]
    kind: String,
}

fn together_api_error_nsfw(api_err: &TogetherApiError) -> bool {
    api_err.error.kind == "invalid_request_error"
        || contains_ascii_fold(&api_err.error.message, "nsfw")
}

fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
    let needle = needle.as_bytes();
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    haystack
        .as_bytes()
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn header_duration_seconds(headers: &HeaderMap, name: &str) -> Option<Duration> {
    let seconds = header_i64(headers, name)?;
    (seconds > 0).then(|| Duration::from_secs(seconds as u64))
}

fn header_i64(headers: &HeaderMap, name: &str) -> Option<i64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
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

    use reqwest::header::{HeaderMap, HeaderValue};
    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn together_generate_sends_go_payload_headers_and_decodes_first_url() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture");
        let addr = listener.local_addr().expect("local addr");
        let captured = Arc::new(Mutex::new(String::new()));
        let server_request = Arc::clone(&captured);
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            let request = read_request(&mut stream);
            *server_request.lock().expect("request") = request;
            write_response(
                &mut stream,
                200,
                &[
                    ("content-type", "application/json"),
                    ("x-ratelimit-remaining", "0"),
                    ("x-ratelimit-reset", "13"),
                ],
                br#"{"data":[{"index":0,"url":"https://example.test/image.webp"}]}"#,
            );
        });

        let client = TogetherClient::new(TogetherConfig {
            api_key: "test-key".to_owned(),
            base_url: format!("http://{addr}/v1"),
            rate_limit_duration: StdDuration::from_secs(11),
            request_timeout: StdDuration::from_secs(5),
            user_agents: vec!["test-agent".to_owned()],
        })
        .expect("client");

        let result = client
            .images_generate(TogetherImageRequest {
                prompt: "cat".to_owned(),
                negative_prompt: "blur".to_owned(),
                width: Some(512),
                height: Some(768),
                seed: Some(42),
                ..TogetherImageRequest::default()
            })
            .await
            .expect("generate");

        handle.join().expect("server thread");
        assert_eq!(result.url, "https://example.test/image.webp");
        assert!(client.next_allowed_at().is_some());

        let request = captured.lock().expect("request");
        assert!(request.starts_with("POST /v1/images/generations HTTP/1.1"));
        assert!(contains_header(
            &request,
            "authorization",
            "Bearer test-key"
        ));
        assert!(contains_header(&request, "user-agent", "test-agent"));
        assert!(contains_header(
            &request,
            "content-type",
            "application/json"
        ));
        assert!(contains_header(&request, "accept", "application/json"));
        let body = request.split("\r\n\r\n").nth(1).expect("body");
        let payload: Value = serde_json::from_str(body).expect("payload");
        assert_eq!(payload["prompt"], "cat");
        assert_eq!(payload["negative_prompt"], "blur");
        assert_eq!(payload["model"], DEFAULT_DRAW_MODEL);
        assert_eq!(payload["steps"], DEFAULT_STEPS);
        assert_eq!(payload["n"], DEFAULT_N);
        assert_eq!(payload["width"], 512);
        assert_eq!(payload["height"], 768);
        assert_eq!(payload["seed"], 42);
    }

    #[tokio::test]
    #[ignore = "live Together image smoke"]
    async fn live_together_image_smoke_generates_first_url()
    -> Result<(), Box<dyn std::error::Error>> {
        let api_key = together_live_key()
            .ok_or("TOGETHER_KEY or TOGETHER_KEYS is required for live Together image smoke")?;
        let model = std::env::var("OPENPLOTVA_TOGETHER_IMAGE_SMOKE_MODEL")
            .unwrap_or_else(|_| DEFAULT_DRAW_MODEL.to_owned());
        let prompt = std::env::var("OPENPLOTVA_TOGETHER_IMAGE_SMOKE_PROMPT")
            .unwrap_or_else(|_| "tiny clean line drawing of a fish-shaped compass".to_owned());
        let client = TogetherClient::new(TogetherConfig {
            api_key,
            request_timeout: StdDuration::from_secs(120),
            ..TogetherConfig::default()
        })?;

        let result = client
            .images_generate(TogetherImageRequest {
                prompt,
                model,
                width: Some(512),
                height: Some(512),
                steps: Some(DEFAULT_STEPS),
                n: Some(1),
                no_wait: true,
                ..TogetherImageRequest::default()
            })
            .await?;

        assert!(
            result.url.starts_with("http://") || result.url.starts_with("https://"),
            "Together image URL must be absolute: {}",
            result.url
        );
        Ok(())
    }

    #[test]
    fn together_rate_limit_headers_match_go_delay_rules() {
        let mut headers = HeaderMap::new();
        headers.insert("Retry-After", HeaderValue::from_static("3"));
        headers.insert("x-ratelimit-reset", HeaderValue::from_static("17"));
        assert_eq!(
            rate_limit_retry_after(&headers, StdDuration::from_secs(11)),
            StdDuration::from_secs(17)
        );

        let mut not_exhausted = HeaderMap::new();
        not_exhausted.insert("x-ratelimit-remaining", HeaderValue::from_static("1"));
        assert_eq!(
            exhausted_rate_limit_wait(&not_exhausted, StdDuration::from_secs(11)),
            StdDuration::ZERO
        );

        let mut exhausted = HeaderMap::new();
        exhausted.insert("x-ratelimit-remaining", HeaderValue::from_static("0"));
        exhausted.insert("x-ratelimit-reset", HeaderValue::from_static("13"));
        assert_eq!(
            exhausted_rate_limit_wait(&exhausted, StdDuration::from_secs(11)),
            StdDuration::from_secs(13)
        );
    }

    #[test]
    fn together_error_decode_preserves_go_nsfw_and_status_messages() {
        assert!(together_nsfw_error(
            br#"{"error":{"message":"ordinary validation issue","type":"invalid_request_error"}}"#
        ));
        assert!(!together_nsfw_error(
            br#"{"error":{"message":"ordinary validation issue","type":"server_error"}}"#
        ));
        assert!(together_nsfw_error(
            br#"{"error":{"message":"Potential NSFW content","type":"server_error"}}"#
        ));

        let headers = HeaderMap::new();
        let err = decode_together_error(
            422,
            &headers,
            br#"{"error":{"message":"ordinary validation issue","type":"invalid_request_error"}}"#,
            StdDuration::from_secs(11),
        );
        assert_eq!(err.status(), Some(420));
        assert_eq!(err.to_string(), TOGETHER_NSFW_MESSAGE);
        assert_eq!(
            together_status_description(401),
            "🔑 Ключ от мастерской потерялся. Проверь доступ."
        );
        assert_eq!(together_status_description(418), "что-то пошло не так");
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

    fn write_response(stream: &mut TcpStream, status: u16, headers: &[(&str, &str)], body: &[u8]) {
        write!(
            stream,
            "HTTP/1.1 {status} OK\r\ncontent-length: {}\r\nconnection: close\r\n",
            body.len()
        )
        .expect("write status");
        for (name, value) in headers {
            write!(stream, "{name}: {value}\r\n").expect("write header");
        }
        stream.write_all(b"\r\n").expect("write header end");
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

    fn together_live_key() -> Option<String> {
        std::env::var("TOGETHER_KEY")
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var("TOGETHER_KEYS").ok().and_then(|keys| {
                    keys.split(',')
                        .map(str::trim)
                        .find(|key| !key.is_empty())
                        .map(ToOwned::to_owned)
                })
            })
    }
}
