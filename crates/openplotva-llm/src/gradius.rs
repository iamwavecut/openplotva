//! Gradius privacy-safe request primitives.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

use openplotva_memory::{
    DiscoveryRedactor, DiscoveryRedactorConfig, DiscoveryRedactorError, RedactionReplacementMode,
};
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::gradius_vip_hints::GRADIUS_VIP_HINTS;

const USER_ID_SCOPE: &str = "gradius:v1:user";
const CHAT_ID_SCOPE: &str = "gradius:v1:chat";
const VIP_HINT_SCOPE: &str = "gradius:v1:vip-hint";
const GRADIUS_BASE_URL_DEFAULT: &str = "https://api.adlean.pro";
const GRADIUS_REQUEST_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);
const GRADIUS_DIALOGUE_PATH: &str = "/v1/native/dialogue_model/chat";
pub const GRADIUS_RAW_BODY_MAX_BYTES: usize = 65_536;
const GRADIUS_REDACTION_CATEGORIES: [&str; 8] = [
    "account_number",
    "private_address",
    "private_date",
    "private_email",
    "private_person",
    "private_phone",
    "private_url",
    "secret",
];

#[derive(Clone, Eq, PartialEq)]
pub struct GradiusClientConfig {
    pub enabled: bool,
    pub api_key: String,
    pub base_url: String,
    pub request_timeout: Duration,
}

impl Default for GradiusClientConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key: String::new(),
            base_url: GRADIUS_BASE_URL_DEFAULT.to_owned(),
            request_timeout: GRADIUS_REQUEST_TIMEOUT_DEFAULT,
        }
    }
}

impl GradiusClientConfig {
    fn with_defaults(mut self) -> Self {
        if self.base_url.trim().is_empty() {
            self.base_url = GRADIUS_BASE_URL_DEFAULT.to_owned();
        }
        if self.request_timeout.is_zero() {
            self.request_timeout = GRADIUS_REQUEST_TIMEOUT_DEFAULT;
        }
        self
    }

    #[must_use]
    pub fn effective_enabled(&self) -> bool {
        self.enabled && !self.api_key.trim().is_empty()
    }
}

impl fmt::Debug for GradiusClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GradiusClientConfig")
            .field("enabled", &self.enabled)
            .field("api_key", &"[redacted]")
            .field("base_url", &self.base_url)
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GradiusDialogueRole {
    User,
    Assistant,
}

impl GradiusDialogueRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

/// Stable Gradius product surface key used by policy, persistence, and diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GradiusIntegrationKind {
    NativeDialogue,
    NativeGeneration,
    NativeUtility,
    DialogueThinking,
}

impl GradiusIntegrationKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeDialogue => "native_dialogue",
            Self::NativeGeneration => "native_generation",
            Self::NativeUtility => "native_utility",
            Self::DialogueThinking => "dialogue_thinking",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusDialogueTurn {
    pub chat_id: String,
    pub user_id: String,
    pub role: GradiusDialogueRole,
    pub language: String,
    pub model_version: Option<String>,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GradiusDialogueAd {
    pub insert_index: usize,
    pub markdown: String,
    pub show_price: Option<f64>,
    pub click_price: Option<f64>,
}

/// Typed selected placement while retaining the complete raw provider response separately.
#[derive(Clone, Debug, PartialEq)]
pub enum GradiusPlacement {
    NativeDialogue(GradiusDialogueAd),
    Standalone {
        markdown: String,
        show_price: Option<f64>,
        click_price: Option<f64>,
        ad_context: Option<serde_json::Value>,
        cta_text: Option<String>,
        cta_link: Option<String>,
    },
}

/// Classification of one backend call for durable audit and aggregate reporting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GradiusCallOutcome {
    Ad,
    NoAd,
    HttpError,
    TransportError,
    DecodeError,
    ResponseTooLarge,
}

impl GradiusCallOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ad => "ad",
            Self::NoAd => "no_ad",
            Self::HttpError => "http_error",
            Self::TransportError => "transport_error",
            Self::DecodeError => "decode_error",
            Self::ResponseTooLarge => "response_too_large",
        }
    }
}

/// Privacy-safe request/response envelope. Debug output intentionally omits payload bodies.
#[derive(Clone, PartialEq)]
pub struct GradiusApiExchange {
    pub integration_kind: GradiusIntegrationKind,
    pub role: Option<GradiusDialogueRole>,
    pub endpoint: String,
    pub request_body: serde_json::Value,
    pub status: Option<u16>,
    pub response_body: Option<String>,
    pub response_json: Option<serde_json::Value>,
    pub response_truncated: bool,
    pub duration_ms: i64,
    pub outcome: GradiusCallOutcome,
}

impl fmt::Debug for GradiusApiExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GradiusApiExchange")
            .field("integration_kind", &self.integration_kind)
            .field("role", &self.role)
            .field("endpoint", &self.endpoint)
            .field("request_body_bytes", &json_bytes(&self.request_body))
            .field("status", &self.status)
            .field(
                "response_body_bytes",
                &self.response_body.as_ref().map(String::len),
            )
            .field("response_truncated", &self.response_truncated)
            .field("duration_ms", &self.duration_ms)
            .field("outcome", &self.outcome)
            .finish()
    }
}

#[derive(Clone, PartialEq)]
pub struct GradiusApiResult {
    pub placement: Option<GradiusPlacement>,
    pub exchange: GradiusApiExchange,
}

impl fmt::Debug for GradiusApiResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GradiusApiResult")
            .field(
                "placement_kind",
                &self.placement.as_ref().map(|placement| match placement {
                    GradiusPlacement::NativeDialogue(_) => "native_dialogue",
                    GradiusPlacement::Standalone { .. } => "standalone",
                }),
            )
            .field("exchange", &self.exchange)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct GradiusHttpRequest {
    pub endpoint: String,
    pub api_key: String,
    pub body: Vec<u8>,
    pub timeout: Duration,
}

impl fmt::Debug for GradiusHttpRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GradiusHttpRequest")
            .field("endpoint", &self.endpoint)
            .field("api_key", &"[redacted]")
            .field("body_bytes", &self.body.len())
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusHttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

pub type GradiusTransportFuture<'a> =
    Pin<Box<dyn Future<Output = Result<GradiusHttpResponse, GradiusTransportError>> + Send + 'a>>;

pub trait GradiusTransport: Clone + Send + Sync + 'static {
    fn post_json<'a>(&'a self, request: GradiusHttpRequest) -> GradiusTransportFuture<'a>;
}

#[derive(Debug, Error)]
pub enum GradiusTransportError {
    #[error("HTTP transport failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("{0}")]
    Other(String),
}

#[derive(Clone)]
pub struct ReqwestGradiusTransport {
    client: reqwest::Client,
}

impl Default for ReqwestGradiusTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl fmt::Debug for ReqwestGradiusTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestGradiusTransport")
            .finish_non_exhaustive()
    }
}

impl GradiusTransport for ReqwestGradiusTransport {
    fn post_json<'a>(&'a self, request: GradiusHttpRequest) -> GradiusTransportFuture<'a> {
        Box::pin(async move {
            let response = self
                .client
                .post(request.endpoint)
                .header("Auth", request.api_key)
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, "application/json")
                .timeout(request.timeout)
                .body(request.body)
                .send()
                .await?;
            let status = response.status().as_u16();
            let body = response.bytes().await?.to_vec();
            Ok(GradiusHttpResponse { status, body })
        })
    }
}

#[derive(Debug, Error)]
pub enum GradiusClientError {
    #[error("Gradius is disabled or has no API key")]
    Disabled,
    #[error("invalid Gradius dialogue turn field: {0}")]
    InvalidTurn(&'static str),
    #[error("invalid Gradius base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    #[error("failed to encode Gradius request: {0}")]
    Encode(#[from] serde_json::Error),
    #[error("Gradius request failed: {0}")]
    Request(#[from] GradiusTransportError),
    #[error("Gradius returned HTTP {status}")]
    Status { status: u16 },
    #[error("failed to decode Gradius response: {0}")]
    Decode(serde_json::Error),
    #[error("Gradius response is too large: {bytes} bytes exceeds {max_bytes}")]
    ResponseTooLarge { bytes: usize, max_bytes: usize },
}

#[derive(Debug)]
pub struct GradiusClientFailure {
    pub error: GradiusClientError,
    pub exchange: Option<Box<GradiusApiExchange>>,
}

impl fmt::Display for GradiusClientFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for GradiusClientFailure {}

#[derive(Clone, Debug)]
pub struct GradiusClient<T = ReqwestGradiusTransport> {
    config: GradiusClientConfig,
    transport: T,
}

impl GradiusClient<ReqwestGradiusTransport> {
    #[must_use]
    pub fn new(config: GradiusClientConfig) -> Self {
        Self::with_transport(config, ReqwestGradiusTransport::default())
    }
}

impl<T> GradiusClient<T>
where
    T: GradiusTransport,
{
    #[must_use]
    pub fn with_transport(config: GradiusClientConfig, transport: T) -> Self {
        Self {
            config: config.with_defaults(),
            transport,
        }
    }

    pub async fn dialogue(
        &self,
        turn: GradiusDialogueTurn,
    ) -> Result<GradiusApiResult, GradiusClientFailure> {
        if !self.config.effective_enabled() {
            return Err(failure(GradiusClientError::Disabled, None));
        }
        validate_dialogue_turn(&turn).map_err(|error| failure(error, None))?;

        let endpoint = dialogue_endpoint(&self.config.base_url, &turn)
            .map_err(|error| failure(error.into(), None))?;
        let request_body = serde_json::to_value(GradiusDialogueBody {
            text: &turn.text,
            user_metadata: serde_json::Map::new(),
        })
        .map_err(|error| failure(error.into(), None))?;
        let body =
            serde_json::to_vec(&request_body).map_err(|error| failure(error.into(), None))?;
        let started_at = Instant::now();
        let response = match self
            .transport
            .post_json(GradiusHttpRequest {
                endpoint: endpoint.clone(),
                api_key: self.config.api_key.clone(),
                body,
                timeout: self.config.request_timeout,
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                let exchange = exchange(
                    &turn,
                    endpoint,
                    request_body,
                    None,
                    None,
                    None,
                    false,
                    started_at,
                    GradiusCallOutcome::TransportError,
                );
                return Err(failure(error.into(), Some(exchange)));
            }
        };
        let response_truncated = response.body.len() > GRADIUS_RAW_BODY_MAX_BYTES;
        let captured = &response.body[..response.body.len().min(GRADIUS_RAW_BODY_MAX_BYTES)];
        let response_body = String::from_utf8_lossy(captured).into_owned();
        let response_json = (!response_truncated)
            .then(|| serde_json::from_slice::<serde_json::Value>(&response.body).ok())
            .flatten();
        if response_truncated {
            let exchange = exchange(
                &turn,
                endpoint,
                request_body,
                Some(response.status),
                Some(response_body),
                None,
                true,
                started_at,
                GradiusCallOutcome::ResponseTooLarge,
            );
            return Err(failure(
                GradiusClientError::ResponseTooLarge {
                    bytes: response.body.len(),
                    max_bytes: GRADIUS_RAW_BODY_MAX_BYTES,
                },
                Some(exchange),
            ));
        }
        if !(200..300).contains(&response.status) {
            let exchange = exchange(
                &turn,
                endpoint,
                request_body,
                Some(response.status),
                Some(response_body),
                response_json,
                false,
                started_at,
                GradiusCallOutcome::HttpError,
            );
            return Err(failure(
                GradiusClientError::Status {
                    status: response.status,
                },
                Some(exchange),
            ));
        }
        let ad = match decode_dialogue_ad(&response.body) {
            Ok(ad) => ad,
            Err(error) => {
                let exchange = exchange(
                    &turn,
                    endpoint,
                    request_body,
                    Some(response.status),
                    Some(response_body),
                    response_json,
                    false,
                    started_at,
                    GradiusCallOutcome::DecodeError,
                );
                return Err(failure(error, Some(exchange)));
            }
        };
        let outcome = if ad.is_some() {
            GradiusCallOutcome::Ad
        } else {
            GradiusCallOutcome::NoAd
        };
        Ok(GradiusApiResult {
            placement: ad.map(GradiusPlacement::NativeDialogue),
            exchange: exchange(
                &turn,
                endpoint,
                request_body,
                Some(response.status),
                Some(response_body),
                response_json,
                false,
                started_at,
                outcome,
            ),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn exchange(
    turn: &GradiusDialogueTurn,
    endpoint: String,
    request_body: serde_json::Value,
    status: Option<u16>,
    response_body: Option<String>,
    response_json: Option<serde_json::Value>,
    response_truncated: bool,
    started_at: Instant,
    outcome: GradiusCallOutcome,
) -> GradiusApiExchange {
    GradiusApiExchange {
        integration_kind: GradiusIntegrationKind::NativeDialogue,
        role: Some(turn.role),
        endpoint,
        request_body,
        status,
        response_body,
        response_json,
        response_truncated,
        duration_ms: started_at.elapsed().as_millis().min(i64::MAX as u128) as i64,
        outcome,
    }
}

fn failure(
    error: GradiusClientError,
    exchange: Option<GradiusApiExchange>,
) -> GradiusClientFailure {
    GradiusClientFailure {
        error,
        exchange: exchange.map(Box::new),
    }
}

fn json_bytes(value: &serde_json::Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

fn validate_dialogue_turn(turn: &GradiusDialogueTurn) -> Result<(), GradiusClientError> {
    for (field, value) in [
        ("chat_id", turn.chat_id.as_str()),
        ("user_id", turn.user_id.as_str()),
        ("language", turn.language.as_str()),
        ("text", turn.text.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(GradiusClientError::InvalidTurn(field));
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct GradiusDialogueBody<'a> {
    text: &'a str,
    user_metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct GradiusNativeTextAd {
    content: GradiusNativeTextAdContent,
    #[serde(default)]
    show_price: Option<f64>,
    #[serde(default)]
    click_price: Option<f64>,
}

#[derive(Deserialize)]
struct GradiusNativeTextAdContent {
    insert_index: usize,
    content: String,
}

fn dialogue_endpoint(
    base_url: &str,
    turn: &GradiusDialogueTurn,
) -> Result<String, url::ParseError> {
    let mut endpoint = Url::parse(&format!(
        "{}{}",
        base_url.trim().trim_end_matches('/'),
        GRADIUS_DIALOGUE_PATH
    ))?;
    let mut query = endpoint.query_pairs_mut();
    query.append_pair("chat_id", &turn.chat_id);
    query.append_pair("user_id", &turn.user_id);
    query.append_pair("role", turn.role.as_str());
    query.append_pair("lang", &turn.language);
    if let Some(model_version) = turn
        .model_version
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        query.append_pair("model_version", model_version);
    }
    drop(query);
    Ok(endpoint.into())
}

fn decode_dialogue_ad(body: &[u8]) -> Result<Option<GradiusDialogueAd>, GradiusClientError> {
    let entries = serde_json::from_slice::<Vec<serde_json::Value>>(body)
        .map_err(GradiusClientError::Decode)?;
    let Some(entry) = entries.into_iter().find(|entry| {
        entry.get("type").and_then(serde_json::Value::as_str) == Some("native-text-ad")
    }) else {
        return Ok(None);
    };
    let ad =
        serde_json::from_value::<GradiusNativeTextAd>(entry).map_err(GradiusClientError::Decode)?;
    Ok(Some(GradiusDialogueAd {
        insert_index: ad.content.insert_index,
        markdown: ad.content.content,
        show_price: ad.show_price,
        click_price: ad.click_price,
    }))
}

/// Stable one-way identifiers sent to Gradius instead of Telegram IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GradiusSyntheticIds {
    /// Synthetic dialogue identifier, including Telegram topic identity.
    pub chat_id: String,
    /// Synthetic user identifier, stable across dialogues.
    pub user_id: String,
}

impl GradiusSyntheticIds {
    /// Derive both required Gradius IDs, or return `None` for missing source IDs.
    #[must_use]
    pub fn derive(chat_id: i64, thread_id: Option<i32>, user_id: i64) -> Option<Self> {
        if chat_id == 0 || user_id == 0 {
            return None;
        }
        let thread_id = thread_id.filter(|value| *value > 0).unwrap_or_default();
        Some(Self {
            chat_id: synthetic_id(
                "chat",
                &format!("{CHAT_ID_SCOPE}:{chat_id}:thread:{thread_id}"),
            ),
            user_id: synthetic_id("user", &format!("{USER_ID_SCOPE}:{user_id}")),
        })
    }
}

fn synthetic_id(prefix: &str, source: &str) -> String {
    let digest = Sha256::digest(source.as_bytes());
    format!("{prefix}_{}", hex::encode(digest))
}

/// Select a stable plain-text VIP hint for one Gradius impression.
///
/// The application layer is responsible for HTML escaping and wrapping the result in
/// `<tg-spoiler>` after the untouched ad body.
#[must_use]
pub fn vip_hint_for_impression(impression_key: &str) -> Option<&'static str> {
    let impression_key = impression_key.trim();
    if impression_key.is_empty() {
        return None;
    }

    let digest = Sha256::digest(format!("{VIP_HINT_SCOPE}:{impression_key}").as_bytes());
    let mut selector = [0_u8; 8];
    selector.copy_from_slice(&digest[..8]);
    let index = u64::from_be_bytes(selector) as usize % GRADIUS_VIP_HINTS.len();
    Some(GRADIUS_VIP_HINTS[index])
}

/// Privacy-filter client fixed to the complete PII set and readable placeholders.
#[derive(Debug)]
pub struct GradiusPrivacyRedactor {
    redactor: DiscoveryRedactor,
}

impl GradiusPrivacyRedactor {
    /// Build a Gradius redactor from the shared Discovery transport configuration.
    pub fn new(mut config: DiscoveryRedactorConfig) -> Result<Self, reqwest::Error> {
        config.categories = Self::categories()
            .into_iter()
            .map(ToOwned::to_owned)
            .collect();
        Ok(Self {
            redactor: DiscoveryRedactor::new(config)?,
        })
    }

    /// Redact outbound Gradius text. Errors are returned so callers can skip the ad request.
    pub async fn redact_text(&self, text: &str) -> Result<String, DiscoveryRedactorError> {
        self.redactor
            .redact_text_with_mode(text, Self::replacement_mode())
            .await
    }

    #[must_use]
    const fn categories() -> [&'static str; 8] {
        GRADIUS_REDACTION_CATEGORIES
    }

    #[must_use]
    const fn replacement_mode() -> RedactionReplacementMode {
        RedactionReplacementMode::TypedPlaceholders
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::Duration,
    };

    #[derive(Clone, Default)]
    struct FakeGradiusTransport {
        requests: Arc<Mutex<Vec<GradiusHttpRequest>>>,
        responses: Arc<Mutex<VecDeque<Result<GradiusHttpResponse, GradiusTransportError>>>>,
    }

    impl GradiusTransport for FakeGradiusTransport {
        fn post_json<'a>(&'a self, request: GradiusHttpRequest) -> GradiusTransportFuture<'a> {
            Box::pin(async move {
                self.requests.lock().expect("request lock").push(request);
                self.responses
                    .lock()
                    .expect("response lock")
                    .pop_front()
                    .expect("fake response")
            })
        }
    }

    #[tokio::test]
    async fn dialogue_client_sends_the_documented_wire_contract() {
        let transport = FakeGradiusTransport::default();
        transport
            .responses
            .lock()
            .expect("response lock")
            .push_back(Ok(GradiusHttpResponse {
                status: 200,
                body: r#"[{"type":"native-text-ad","content":{"insert_index":18,"content":"**Реклама** [сюда](https://ads.example/r/42)"},"show_price":1.2,"click_price":45}]"#.as_bytes().to_vec(),
            }));
        let client = GradiusClient::with_transport(
            GradiusClientConfig {
                enabled: true,
                api_key: "server-secret".to_owned(),
                base_url: "https://api.adlean.pro/".to_owned(),
                request_timeout: Duration::from_secs(4),
            },
            transport.clone(),
        );

        let result = client
            .dialogue(GradiusDialogueTurn {
                chat_id: "chat_synthetic".to_owned(),
                user_id: "user_synthetic".to_owned(),
                role: GradiusDialogueRole::Assistant,
                language: "ru".to_owned(),
                model_version: Some("plotva-model".to_owned()),
                text: "Очищенный ответ".to_owned(),
            })
            .await
            .expect("dialogue response");
        let GradiusPlacement::NativeDialogue(ad) = result.placement.expect("native text ad") else {
            panic!("expected native dialogue placement");
        };

        assert_eq!(ad.insert_index, 18);
        assert_eq!(ad.markdown, "**Реклама** [сюда](https://ads.example/r/42)");
        assert_eq!(ad.show_price, Some(1.2));
        assert_eq!(ad.click_price, Some(45.0));
        assert_eq!(
            result.exchange.integration_kind,
            GradiusIntegrationKind::NativeDialogue
        );
        assert_eq!(result.exchange.role, Some(GradiusDialogueRole::Assistant));
        assert_eq!(result.exchange.status, Some(200));
        assert_eq!(result.exchange.outcome, GradiusCallOutcome::Ad);
        assert_eq!(
            result.exchange.request_body,
            serde_json::json!({"text": "Очищенный ответ", "user_metadata": {}})
        );
        assert_eq!(
            result.exchange.response_json,
            Some(serde_json::json!([{
                "type": "native-text-ad",
                "content": {
                    "insert_index": 18,
                    "content": "**Реклама** [сюда](https://ads.example/r/42)"
                },
                "show_price": 1.2,
                "click_price": 45
            }]))
        );
        assert_eq!(
            result.exchange.response_body.as_deref(),
            Some(
                r#"[{"type":"native-text-ad","content":{"insert_index":18,"content":"**Реклама** [сюда](https://ads.example/r/42)"},"show_price":1.2,"click_price":45}]"#
            )
        );
        assert!(!result.exchange.response_truncated);
        let exchange_debug = format!("{:?}", result.exchange);
        assert!(!exchange_debug.contains("Очищенный ответ"));
        assert!(!exchange_debug.contains("Реклама"));

        let requests = transport.requests.lock().expect("request lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].api_key, "server-secret");
        assert_eq!(requests[0].timeout, Duration::from_secs(4));
        assert_eq!(
            requests[0].endpoint,
            "https://api.adlean.pro/v1/native/dialogue_model/chat?chat_id=chat_synthetic&user_id=user_synthetic&role=assistant&lang=ru&model_version=plotva-model"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&requests[0].body).expect("request JSON"),
            serde_json::json!({"text": "Очищенный ответ", "user_metadata": {}})
        );
    }

    fn enabled_test_client(transport: FakeGradiusTransport) -> GradiusClient<FakeGradiusTransport> {
        GradiusClient::with_transport(
            GradiusClientConfig {
                enabled: true,
                api_key: "server-secret".to_owned(),
                ..Default::default()
            },
            transport,
        )
    }

    fn test_turn(role: GradiusDialogueRole) -> GradiusDialogueTurn {
        GradiusDialogueTurn {
            chat_id: "chat_synthetic".to_owned(),
            user_id: "user_synthetic".to_owned(),
            role,
            language: "ru".to_owned(),
            model_version: None,
            text: "Очищенный текст".to_owned(),
        }
    }

    #[tokio::test]
    async fn dialogue_client_rejects_missing_synthetic_identity_without_transport() {
        let transport = FakeGradiusTransport::default();
        let client = enabled_test_client(transport.clone());
        let mut turn = test_turn(GradiusDialogueRole::User);
        turn.user_id.clear();

        assert!(matches!(
            client.dialogue(turn).await,
            Err(GradiusClientFailure {
                error: GradiusClientError::InvalidTurn("user_id"),
                exchange: None,
            })
        ));
        assert!(transport.requests.lock().expect("request lock").is_empty());
    }

    #[tokio::test]
    async fn dialogue_client_treats_empty_and_unknown_responses_as_no_ad() {
        let transport = FakeGradiusTransport::default();
        {
            let mut responses = transport.responses.lock().expect("response lock");
            responses.push_back(Ok(GradiusHttpResponse {
                status: 200,
                body: b"[]".to_vec(),
            }));
            responses.push_back(Ok(GradiusHttpResponse {
                status: 200,
                body: br#"[{"type":"future-ad","content":{"anything":true}}]"#.to_vec(),
            }));
        }
        let client = enabled_test_client(transport.clone());

        assert_eq!(
            client
                .dialogue(test_turn(GradiusDialogueRole::User))
                .await
                .expect("empty response")
                .placement,
            None
        );
        assert_eq!(
            client
                .dialogue(test_turn(GradiusDialogueRole::Assistant))
                .await
                .expect("unknown response")
                .placement,
            None
        );
        let requests = transport.requests.lock().expect("request lock");
        assert!(requests[0].endpoint.contains("role=user"));
        assert!(requests[1].endpoint.contains("role=assistant"));
        assert!(!requests[0].endpoint.contains("model_version"));
    }

    #[tokio::test]
    async fn dialogue_client_selects_first_native_placement_and_preserves_all_raw_entries() {
        let transport = FakeGradiusTransport::default();
        transport
            .responses
            .lock()
            .expect("response lock")
            .push_back(Ok(GradiusHttpResponse {
                status: 200,
                body: br#"[
                    {"type":"future-ad","content":{"value":1}},
                    {"type":"native-text-ad","content":{"insert_index":3,"content":"first"}},
                    {"type":"native-text-ad","content":{"insert_index":4,"content":"second"}}
                ]"#
                .to_vec(),
            }));
        let result = enabled_test_client(transport)
            .dialogue(test_turn(GradiusDialogueRole::Assistant))
            .await
            .expect("multi-placement response");
        let Some(GradiusPlacement::NativeDialogue(ad)) = result.placement else {
            panic!("native placement expected");
        };
        assert_eq!(ad.markdown, "first");
        assert_eq!(
            result
                .exchange
                .response_json
                .as_ref()
                .and_then(serde_json::Value::as_array)
                .map(Vec::len),
            Some(3)
        );
    }

    #[tokio::test]
    async fn dialogue_client_reports_status_and_decode_failures() {
        let transport = FakeGradiusTransport::default();
        {
            let mut responses = transport.responses.lock().expect("response lock");
            responses.push_back(Ok(GradiusHttpResponse {
                status: 429,
                body: b"rate limited".to_vec(),
            }));
            responses.push_back(Ok(GradiusHttpResponse {
                status: 200,
                body: b"not-json".to_vec(),
            }));
        }
        let client = enabled_test_client(transport);

        let status_failure = client
            .dialogue(test_turn(GradiusDialogueRole::Assistant))
            .await
            .expect_err("HTTP status error");
        assert!(matches!(
            status_failure.error,
            GradiusClientError::Status { status: 429 }
        ));
        let status_exchange = status_failure.exchange.expect("auditable status exchange");
        assert_eq!(status_exchange.status, Some(429));
        assert_eq!(
            status_exchange.response_body.as_deref(),
            Some("rate limited")
        );
        assert_eq!(status_exchange.outcome, GradiusCallOutcome::HttpError);

        let decode_failure = client
            .dialogue(test_turn(GradiusDialogueRole::Assistant))
            .await
            .expect_err("decode error");
        assert!(matches!(
            decode_failure.error,
            GradiusClientError::Decode(_)
        ));
        let decode_exchange = decode_failure.exchange.expect("auditable decode exchange");
        assert_eq!(decode_exchange.status, Some(200));
        assert_eq!(decode_exchange.response_body.as_deref(), Some("not-json"));
        assert_eq!(decode_exchange.outcome, GradiusCallOutcome::DecodeError);
    }

    #[tokio::test]
    async fn dialogue_client_rejects_oversized_response_but_keeps_audit_prefix() {
        let transport = FakeGradiusTransport::default();
        transport
            .responses
            .lock()
            .expect("response lock")
            .push_back(Ok(GradiusHttpResponse {
                status: 200,
                body: vec![b'x'; GRADIUS_RAW_BODY_MAX_BYTES + 1],
            }));
        let client = enabled_test_client(transport);

        let failure = client
            .dialogue(test_turn(GradiusDialogueRole::Assistant))
            .await
            .expect_err("oversized response must fail");

        assert!(matches!(
            failure.error,
            GradiusClientError::ResponseTooLarge {
                bytes,
                max_bytes: GRADIUS_RAW_BODY_MAX_BYTES,
            } if bytes == GRADIUS_RAW_BODY_MAX_BYTES + 1
        ));
        let exchange = failure.exchange.expect("auditable exchange");
        assert_eq!(exchange.outcome, GradiusCallOutcome::ResponseTooLarge);
        assert_eq!(
            exchange.response_body.expect("body prefix").len(),
            GRADIUS_RAW_BODY_MAX_BYTES
        );
        assert!(exchange.response_truncated);
    }

    #[test]
    fn synthetic_ids_are_stable_and_domain_separated() {
        let ids = GradiusSyntheticIds::derive(-100, Some(9), 200).expect("valid ids");

        assert_eq!(
            ids.chat_id,
            "chat_316c49b9f77f2743bb1ccadfb76bc12c2bc84bdc7c35e297546e9839d09d2494"
        );
        assert_eq!(
            ids.user_id,
            "user_96513f4d8d56fa336569cc54cd4048e63fc74067069b5a55090e235c4d7c72c8"
        );
        assert_eq!(
            GradiusSyntheticIds::derive(-100, Some(9), 200),
            Some(ids.clone())
        );
        assert_ne!(
            GradiusSyntheticIds::derive(200, None, 200)
                .expect("private chat ids")
                .chat_id,
            ids.user_id
        );
    }

    #[test]
    fn synthetic_ids_change_with_identity_or_dialogue() {
        let base = GradiusSyntheticIds::derive(-100, Some(9), 200).expect("base ids");
        let other_user = GradiusSyntheticIds::derive(-100, Some(9), 201).expect("other user");
        let other_thread = GradiusSyntheticIds::derive(-100, Some(10), 200).expect("other thread");

        assert_ne!(base.user_id, other_user.user_id);
        assert_eq!(base.chat_id, other_user.chat_id);
        assert_ne!(base.chat_id, other_thread.chat_id);
        assert_eq!(base.user_id, other_thread.user_id);
    }

    #[test]
    fn synthetic_ids_reject_missing_real_identity() {
        assert_eq!(GradiusSyntheticIds::derive(0, None, 200), None);
        assert_eq!(GradiusSyntheticIds::derive(-100, None, 0), None);
    }

    #[test]
    fn privacy_redactor_uses_all_labels_and_typed_placeholders() {
        assert_eq!(
            GradiusPrivacyRedactor::categories(),
            [
                "account_number",
                "private_address",
                "private_date",
                "private_email",
                "private_person",
                "private_phone",
                "private_url",
                "secret",
            ]
        );
        assert_eq!(
            GradiusPrivacyRedactor::replacement_mode(),
            openplotva_memory::RedactionReplacementMode::TypedPlaceholders
        );
    }

    #[test]
    fn vip_hint_catalog_is_large_unique_and_telegram_safe() {
        use std::collections::HashSet;

        assert!(GRADIUS_VIP_HINTS.len() >= 50);
        assert_eq!(
            GRADIUS_VIP_HINTS
                .iter()
                .copied()
                .collect::<HashSet<_>>()
                .len(),
            GRADIUS_VIP_HINTS.len()
        );
        for hint in GRADIUS_VIP_HINTS {
            assert!(!hint.trim().is_empty());
            assert!(hint.contains("VIP"), "missing VIP marker: {hint}");
            assert!(hint.chars().count() <= 100, "hint is too long: {hint}");
            assert!(
                !hint.contains(['<', '>', '&']),
                "hint must be plain Telegram-safe text: {hint}"
            );
        }
    }

    #[test]
    fn vip_hint_selection_is_stable_and_well_distributed() {
        use std::collections::HashSet;

        assert_eq!(vip_hint_for_impression(""), None);
        assert_eq!(vip_hint_for_impression("   "), None);
        assert_eq!(
            vip_hint_for_impression("impression-42"),
            vip_hint_for_impression("impression-42")
        );

        let selected = (0..2_048)
            .filter_map(|index| vip_hint_for_impression(&format!("impression-{index}")))
            .collect::<HashSet<_>>();
        assert!(selected.len() >= 40);
    }

    #[tokio::test]
    async fn privacy_redactor_returns_errors_instead_of_unredacted_text() {
        let config = openplotva_memory::DiscoveryRedactorConfig {
            base_url: "not a url".to_owned(),
            ..Default::default()
        };
        let redactor = GradiusPrivacyRedactor::new(config).expect("client");

        assert!(redactor.redact_text("alice@example.test").await.is_err());
    }
}
