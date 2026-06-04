//! Pruna image provider client.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const DEFAULT_ENDPOINT: &str = "";
pub const DEFAULT_MODEL: &str = "prunaai/p-image";
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_ORIGIN: &str = "https://advent-of-pruna.lovable.app";
pub const DEFAULT_REFERER: &str = "https://advent-of-pruna.lovable.app/";
pub const DEFAULT_USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36";

/// Pruna client configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrunaConfig {
    /// Supabase function endpoint.
    pub endpoint: String,
    /// Replicate model endpoint passed inside the JSON envelope.
    pub model: String,
    /// Supabase API key.
    pub api_key: String,
    pub bearer: String,
    /// Request timeout.
    pub timeout: Duration,
}

impl Default for PrunaConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            model: String::new(),
            api_key: String::new(),
            bearer: String::new(),
            timeout: Duration::ZERO,
        }
    }
}

impl PrunaConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.endpoint = if self.endpoint.trim().is_empty() {
            DEFAULT_ENDPOINT.to_owned()
        } else {
            self.endpoint.trim().to_owned()
        };
        self.model = if self.model.trim().is_empty() {
            DEFAULT_MODEL.to_owned()
        } else {
            self.model.trim().to_owned()
        };
        self.api_key = self.api_key.trim().to_owned();
        self.bearer = if self.bearer.trim().is_empty() {
            self.api_key.clone()
        } else {
            self.bearer.trim().to_owned()
        };
        if self.timeout == Duration::ZERO {
            self.timeout = DEFAULT_TIMEOUT;
        }
        self
    }

    #[must_use]
    pub fn configured(&self) -> bool {
        !self.api_key.trim().is_empty() && !self.bearer.trim().is_empty()
    }
}

/// Pruna generate request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrunaRequest {
    /// Image prompt.
    pub prompt: String,
    /// Requested aspect ratio, snapped to Pruna's supported set.
    pub aspect_ratio: String,
    /// Optional visitor ID.
    pub visitor_id: String,
}

/// Pruna image result.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrunaResult {
    /// Remote image URL when returned.
    pub url: String,
    /// Decoded image bytes when returned inline.
    pub image: Vec<u8>,
    /// Remote image URLs when the provider returns multiple outputs.
    pub urls: Vec<String>,
    /// Decoded image bytes when the provider returns multiple inline outputs.
    pub images: Vec<Vec<u8>>,
}

/// HTTP Pruna client.
#[derive(Clone, Debug)]
pub struct PrunaClient {
    cfg: PrunaConfig,
    http: reqwest::Client,
}

impl PrunaClient {
    /// Build a reqwest-backed Pruna client.
    pub fn new(cfg: PrunaConfig) -> Result<Self, PrunaError> {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(PrunaError::BuildHttpClient)?;
        Ok(Self { cfg, http })
    }

    /// Generate one image.
    pub async fn generate(&self, req: PrunaRequest) -> Result<PrunaResult, PrunaError> {
        if !self.cfg.configured() {
            return Err(PrunaError::NotConfigured);
        }

        let visitor_id = if req.visitor_id.trim().is_empty() {
            generated_visitor_id()
        } else {
            req.visitor_id.trim().to_owned()
        };
        let payload = PrunaPayload {
            endpoint: self.cfg.model.clone(),
            inputs: PrunaInputs {
                prompt: req.prompt,
                aspect_ratio: snap_pruna_aspect_ratio(&req.aspect_ratio),
                disable_safety_checker: true,
            },
            visitor_id,
        };

        let response = self
            .http
            .post(&self.cfg.endpoint)
            .header("Content-Type", "application/json")
            .header("apikey", &self.cfg.api_key)
            .header("Authorization", format!("Bearer {}", self.cfg.bearer))
            .header("Origin", DEFAULT_ORIGIN)
            .header("Referer", DEFAULT_REFERER)
            .header("User-Agent", DEFAULT_USER_AGENT)
            .json(&payload)
            .send()
            .await
            .map_err(PrunaError::Http)?;
        let status = response.status().as_u16();
        let body = response.bytes().await.map_err(PrunaError::Http)?.to_vec();
        let decoded = decode_generate_response(status, &body)?;
        let (urls, images) = decoded.images();
        if urls.is_empty() && images.is_empty() {
            return Err(PrunaError::MissingImage(body_preview(&body)));
        }
        Ok(PrunaResult {
            url: urls.first().cloned().unwrap_or_default(),
            image: images.first().cloned().unwrap_or_default(),
            urls,
            images,
        })
    }
}

#[derive(Debug, Error)]
pub enum PrunaError {
    /// HTTP client construction failed.
    #[error("build pruna HTTP client: {0}")]
    BuildHttpClient(reqwest::Error),
    /// Client is missing credentials.
    #[error("pruna client is not configured")]
    NotConfigured,
    /// HTTP request failed.
    #[error("send pruna request: {0}")]
    Http(reqwest::Error),
    /// Non-success HTTP status.
    #[error("pruna status {status}: {body}")]
    Status {
        /// HTTP status.
        status: u16,
        /// Response body preview.
        body: String,
    },
    /// Response JSON failed to decode.
    #[error("decode pruna response: {0}")]
    Json(serde_json::Error),
    /// Provider returned an error payload.
    #[error("pruna response error: {0}")]
    Response(String),
    /// Response did not contain a supported image candidate.
    #[error("pruna response missing image data: {0}")]
    MissingImage(String),
}

#[derive(Clone, Debug, Serialize)]
struct PrunaPayload {
    endpoint: String,
    inputs: PrunaInputs,
    #[serde(rename = "visitorId")]
    visitor_id: String,
}

#[derive(Clone, Debug, Serialize)]
struct PrunaInputs {
    prompt: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    aspect_ratio: String,
    #[serde(skip_serializing_if = "is_false")]
    disable_safety_checker: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PrunaResponse {
    #[serde(default)]
    output: StringList,
    #[serde(default)]
    outputs: StringList,
    #[serde(default)]
    results: StringList,
    #[serde(default)]
    image: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    result: String,
    data: Option<PrunaResponseData>,
    prediction: Option<PrunaPrediction>,
    error: Option<Value>,
    #[serde(default)]
    detail: String,
    #[serde(default)]
    message: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PrunaResponseData {
    #[serde(default)]
    output: StringList,
    #[serde(default)]
    url: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct PrunaPrediction {
    #[serde(default)]
    output: StringList,
    error: Option<Value>,
}

#[derive(Clone, Debug, Default)]
struct StringList(Vec<String>);

impl<'de> Deserialize<'de> for StringList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Option::<Value>::deserialize(deserializer)?;
        let Some(value) = value else {
            return Ok(Self::default());
        };
        match value {
            Value::String(value) => Ok(Self(vec![value])),
            Value::Array(values) => Ok(Self(
                values
                    .into_iter()
                    .filter_map(|value| match value {
                        Value::String(value) => Some(value),
                        _ => None,
                    })
                    .collect(),
            )),
            other => Err(serde::de::Error::custom(format!(
                "expected string or array, got {other}"
            ))),
        }
    }
}

fn decode_generate_response(status: u16, body: &[u8]) -> Result<PrunaResponse, PrunaError> {
    if status >= 300 {
        return Err(PrunaError::Status {
            status,
            body: body_preview(body),
        });
    }
    let decoded: PrunaResponse = serde_json::from_slice(body).map_err(PrunaError::Json)?;
    if let Some(error) = decoded.error_message() {
        return Err(PrunaError::Response(error));
    }
    Ok(decoded)
}

const fn is_false(value: &bool) -> bool {
    !*value
}

impl PrunaResponse {
    fn error_message(&self) -> Option<String> {
        stringify_value(self.error.as_ref())
            .or_else(|| non_empty(&self.detail))
            .or_else(|| non_empty(&self.message))
            .or_else(|| {
                self.prediction
                    .as_ref()
                    .and_then(|prediction| stringify_value(prediction.error.as_ref()))
            })
    }

    fn candidates(&self) -> Vec<&str> {
        let mut candidates = Vec::with_capacity(12);
        candidates.extend(self.output.0.iter().map(String::as_str));
        candidates.extend(self.outputs.0.iter().map(String::as_str));
        candidates.extend(self.results.0.iter().map(String::as_str));
        if let Some(data) = &self.data {
            candidates.extend(data.output.0.iter().map(String::as_str));
            if !data.url.is_empty() {
                candidates.push(&data.url);
            }
        }
        if let Some(prediction) = &self.prediction {
            candidates.extend(prediction.output.0.iter().map(String::as_str));
        }
        for value in [&self.url, &self.image, &self.result] {
            if !value.is_empty() {
                candidates.push(value);
            }
        }
        candidates
    }

    fn images(&self) -> (Vec<String>, Vec<Vec<u8>>) {
        let mut urls = Vec::new();
        let mut images = Vec::new();
        for candidate in self.candidates() {
            let (url, data) = decode_image_candidate(candidate);
            if !url.is_empty() && !urls.iter().any(|existing| existing == &url) {
                urls.push(url);
            }
            if !data.is_empty() {
                images.push(data);
            }
        }
        (urls, images)
    }
}

#[must_use]
pub fn snap_pruna_aspect_ratio(input: &str) -> String {
    const CANDIDATES: [(&str, f64); 7] = [
        ("1:1", 1.0),
        ("16:9", 16.0 / 9.0),
        ("9:16", 9.0 / 16.0),
        ("4:3", 4.0 / 3.0),
        ("3:4", 3.0 / 4.0),
        ("3:2", 3.0 / 2.0),
        ("2:3", 2.0 / 3.0),
    ];

    let trimmed = input.trim();
    if trimmed.is_empty() {
        return CANDIDATES[0].0.to_owned();
    }
    for (value, _) in CANDIDATES {
        if trimmed == value {
            return value.to_owned();
        }
    }
    let Some(ratio) = parse_aspect_ratio(trimmed) else {
        return CANDIDATES[0].0.to_owned();
    };
    let mut best = CANDIDATES[0];
    let mut best_dist = (ratio - best.1).abs();
    for candidate in CANDIDATES.into_iter().skip(1) {
        let dist = (ratio - candidate.1).abs();
        if dist < best_dist {
            best = candidate;
            best_dist = dist;
        }
    }
    best.0.to_owned()
}

fn parse_aspect_ratio(input: &str) -> Option<f64> {
    let normalized = input
        .trim()
        .to_ascii_lowercase()
        .replace(' ', "")
        .replace('×', "x");
    if let Some((left, right)) = normalized.split_once(':') {
        return parse_ratio_pair(left, right);
    }
    if let Some((left, right)) = normalized.split_once('x') {
        return parse_ratio_pair(left, right);
    }
    parse_positive_ratio(&normalized)
}

fn parse_ratio_pair(left: &str, right: &str) -> Option<f64> {
    Some(parse_positive_ratio(left)? / parse_positive_ratio(right)?)
}

fn parse_positive_ratio(value: &str) -> Option<f64> {
    let ratio = value.parse::<f64>().ok()?;
    (ratio > 0.0).then_some(ratio)
}

fn decode_image_candidate(value: &str) -> (String, Vec<u8>) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return (String::new(), Vec::new());
    }
    if has_prefix_fold(trimmed, "http://") || has_prefix_fold(trimmed, "https://") {
        return (trimmed.to_owned(), Vec::new());
    }
    if has_prefix_fold(trimmed, "data:")
        && let Some(decoded) = decode_data_uri_image(trimmed)
    {
        return (String::new(), decoded);
    }
    if let Some(decoded) = decode_base64_image(trimmed) {
        return (String::new(), decoded);
    }
    (String::new(), Vec::new())
}

fn has_prefix_fold(value: &str, prefix: &str) -> bool {
    value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix)
}

fn decode_data_uri_image(value: &str) -> Option<Vec<u8>> {
    let (_, payload) = value.split_once(',')?;
    (!payload.is_empty())
        .then_some(payload)
        .and_then(decode_base64_image)
}

fn decode_base64_image(value: &str) -> Option<Vec<u8>> {
    general_purpose::STANDARD.decode(value).ok()
}

fn stringify_value(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::Null => None,
        Value::String(value) => non_empty(value),
        value => Some(value.to_string()),
    }
}

fn non_empty(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn body_preview(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let mut preview: String = text.chars().take(500).collect();
    if text.chars().nth(500).is_some() {
        preview.push_str("...");
    }
    preview
}

fn generated_visitor_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("visitor_{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    #[test]
    fn snap_pruna_aspect_ratio_matches_go_cases() {
        for (input, want) in [
            ("", "1:1"),
            ("2:3", "2:3"),
            ("4:7", "9:16"),
            ("7:4", "16:9"),
            ("1024x1792", "9:16"),
            (" 1024 x 1792 ", "9:16"),
            ("1792×1024", "16:9"),
            ("1.7", "16:9"),
            ("nope", "1:1"),
        ] {
            assert_eq!(snap_pruna_aspect_ratio(input), want);
        }
    }

    #[test]
    fn decode_image_candidate_keeps_go_accepted_forms() {
        let (url, data) = decode_image_candidate(" https://example.test/image.png ");
        assert_eq!(url, "https://example.test/image.png");
        assert!(data.is_empty());

        let (url, data) = decode_image_candidate("data:image/png;base64,cHJ1bmEtYnl0ZXM=");
        assert!(url.is_empty());
        assert_eq!(data, b"pruna-bytes");

        let (url, data) = decode_image_candidate("cHJ1bmEtYnl0ZXM=");
        assert!(url.is_empty());
        assert_eq!(data, b"pruna-bytes");

        let (url, data) = decode_image_candidate(" ");
        assert!(url.is_empty());
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn client_sends_go_payload_headers_and_decodes_data_url() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout");
            let mut bytes = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buf).expect("read request");
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..read]);
                if String::from_utf8_lossy(&bytes).contains("\"visitorId\":\"visitor-fixed\"") {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&bytes).into_owned();
            let body = "{\"output\":[\"data:image/png;base64,cHJ1bmEtYnl0ZXM=\"],\"remainingCalls\":3,\"metrics\":{\"total_time\":1.2}}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
            request
        });

        let client = PrunaClient::new(PrunaConfig {
            endpoint: format!("http://{addr}"),
            model: "test-model".to_owned(),
            api_key: "test-key".to_owned(),
            bearer: "test-bearer".to_owned(),
            timeout: Duration::from_secs(5),
        })
        .expect("client");

        let result = client
            .generate(PrunaRequest {
                prompt: "draw a cat".to_owned(),
                aspect_ratio: "1024x1792".to_owned(),
                visitor_id: "visitor-fixed".to_owned(),
            })
            .await
            .expect("generate");

        assert_eq!(result.image, b"pruna-bytes");
        assert!(result.url.is_empty());

        let request = handle.join().expect("server thread");
        assert!(request.starts_with("POST / HTTP/1.1"));
        assert!(request.contains("apikey: test-key"));
        assert!(request.contains("authorization: Bearer test-bearer"));
        assert!(request.contains("origin: https://advent-of-pruna.lovable.app"));
        assert!(request.contains("referer: https://advent-of-pruna.lovable.app/"));
        assert!(request.contains("\"endpoint\":\"test-model\""));
        assert!(request.contains("\"prompt\":\"draw a cat\""));
        assert!(request.contains("\"aspect_ratio\":\"9:16\""));
        assert!(request.contains("\"disable_safety_checker\":true"));
        assert!(request.contains("\"visitorId\":\"visitor-fixed\""));
    }

    #[tokio::test]
    async fn client_preserves_multiple_output_urls() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let addr = listener.local_addr().expect("local addr");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout");
            let mut bytes = Vec::new();
            let mut buf = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buf).expect("read request");
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..read]);
                if String::from_utf8_lossy(&bytes).contains("\"visitorId\":\"visitor-fixed\"") {
                    break;
                }
            }
            let body = "{\"output\":[\"https://img.test/1.png\",\"https://img.test/2.png\"]}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream
                .write_all(response.as_bytes())
                .expect("write response");
        });

        let client = PrunaClient::new(PrunaConfig {
            endpoint: format!("http://{addr}"),
            model: "test-model".to_owned(),
            api_key: "test-key".to_owned(),
            bearer: "test-bearer".to_owned(),
            timeout: Duration::from_secs(5),
        })
        .expect("client");

        let result = client
            .generate(PrunaRequest {
                prompt: "draw a cat".to_owned(),
                aspect_ratio: "1:1".to_owned(),
                visitor_id: "visitor-fixed".to_owned(),
            })
            .await
            .expect("generate");

        handle.join().expect("server thread");
        assert_eq!(result.url, "https://img.test/1.png");
        assert_eq!(
            result.urls,
            vec![
                "https://img.test/1.png".to_owned(),
                "https://img.test/2.png".to_owned()
            ]
        );
        assert!(result.images.is_empty());
    }
}
