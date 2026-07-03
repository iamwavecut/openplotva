//! WhiteCircle safety-check provider boundary.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use openplotva_core::ChatAttachment;
use openplotva_dialog::{
    ChatStepRequest, DEFAULT_CONTEXT_HISTORY_LIMIT, DialogInput, ROLE_MODEL, ROLE_USER,
    select_history_messages_for_context,
};

use crate::{ChatProvider, ChatProviderHandle, ChatStepFuture, ChatStepProvider};

const WHITE_CIRCLE_ENDPOINT_DEFAULT: &str = "https://eu.whitecircle.ai/api/session/check";
const WHITE_CIRCLE_VERSION_DEFAULT: &str = "2025-12-01";
const WHITE_CIRCLE_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);
const SIGNATURE_SALT: &str = "plotva-signature-salt-2024";

const SCOPE_SESSION_ID: &str = "whitecircle.session_id";
const SCOPE_USER_ID: &str = "whitecircle.user_id";
const SCOPE_USER_NAME: &str = "whitecircle.user_name";
const SCOPE_CHAT_ID: &str = "whitecircle.chat_id";
const SCOPE_THREAD_ID: &str = "whitecircle.thread_id";

/// WhiteCircle client configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct WhiteCircleClientConfig {
    /// Whether checks are enabled.
    pub enabled: bool,
    /// Bearer API key.
    pub api_key: String,
    /// Deployment ID sent to WhiteCircle.
    pub deployment_id: String,
    /// Check endpoint URL.
    pub endpoint: String,
    /// WhiteCircle API version header.
    pub version: String,
    /// Per-request timeout.
    pub request_timeout: Duration,
}

impl Default for WhiteCircleClientConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_key: String::new(),
            deployment_id: String::new(),
            endpoint: WHITE_CIRCLE_ENDPOINT_DEFAULT.to_owned(),
            version: WHITE_CIRCLE_VERSION_DEFAULT.to_owned(),
            request_timeout: WHITE_CIRCLE_TIMEOUT_DEFAULT,
        }
    }
}

impl WhiteCircleClientConfig {
    fn with_defaults(mut self) -> Self {
        if self.endpoint.trim().is_empty() {
            self.endpoint = WHITE_CIRCLE_ENDPOINT_DEFAULT.to_owned();
        }
        if self.version.trim().is_empty() {
            self.version = WHITE_CIRCLE_VERSION_DEFAULT.to_owned();
        }
        if self.request_timeout.is_zero() {
            self.request_timeout = WHITE_CIRCLE_TIMEOUT_DEFAULT;
        }
        self
    }

    #[must_use]
    pub fn effective_enabled(&self) -> bool {
        self.enabled && !self.api_key.trim().is_empty() && !self.deployment_id.trim().is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WhiteCircleMessage {
    /// Message role.
    pub role: String,
    /// Message content, either string or multimodal parts.
    pub content: Value,
    /// Message metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<WhiteCircleMessageMetadata>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhiteCircleContentPart {
    /// Part type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Text payload.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    /// Image URL payload.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_url: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct WhiteCircleMessageMetadata {
    /// Assistant metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assistant: Option<WhiteCircleMessageMetadataAssistant>,
    /// Telegram message metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<WhiteCircleMessageMetadataMessage>,
    /// Obfuscated user metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<WhiteCircleMessageMetadataUser>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct WhiteCircleMessageMetadataAssistant {
    /// Assistant model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<f64>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhiteCircleMessageMetadataMessage {
    /// Message ID as a string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Message timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhiteCircleMessageMetadataUser {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// Obfuscated user ID alias.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    /// Obfuscated user name alias.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct WhiteCircleSessionMetadata {
    /// Environment metadata.
    #[serde(skip_serializing_if = "Map::is_empty")]
    pub environment: Map<String, Value>,
    /// Session metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<WhiteCircleSessionMetadataDetail>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WhiteCircleSessionMetadataDetail {
    /// Session timestamp.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub timestamp: String,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct WhiteCircleCheckRequest {
    /// WhiteCircle deployment ID.
    pub deployment_id: String,
    /// Messages to check.
    pub messages: Vec<WhiteCircleMessage>,
    /// Stable external session ID.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub external_session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_context: Option<bool>,
    /// Session metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<WhiteCircleSessionMetadata>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct WhiteCircleCheckResponse {
    /// Flagged result.
    pub flagged: bool,
    /// Internal WhiteCircle session ID.
    #[serde(default)]
    pub internal_session_id: String,
    /// External session ID echo.
    #[serde(default)]
    pub external_session_id: Option<String>,
    /// Policy results.
    #[serde(default)]
    pub policies: Map<String, Value>,
}

/// WhiteCircle HTTP request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhiteCircleHttpRequest {
    /// Endpoint URL.
    pub endpoint: String,
    /// Bearer token.
    pub api_key: String,
    /// API version header.
    pub version: String,
    /// JSON body.
    pub body: Vec<u8>,
    /// Request timeout.
    pub timeout: Duration,
}

/// WhiteCircle HTTP response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhiteCircleHttpResponse {
    /// HTTP status.
    pub status: u16,
    /// Response body bytes.
    pub body: Vec<u8>,
}

/// Boxed WhiteCircle transport future.
pub type WhiteCircleTransportFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<WhiteCircleHttpResponse, WhiteCircleTransportError>> + Send + 'a,
    >,
>;

/// WhiteCircle transport boundary.
pub trait WhiteCircleTransport: Clone + Send + Sync + 'static {
    /// POST one JSON request.
    fn post_json<'a>(&'a self, request: WhiteCircleHttpRequest) -> WhiteCircleTransportFuture<'a>;
}

/// Reqwest-backed WhiteCircle transport.
#[derive(Clone)]
pub struct ReqwestWhiteCircleTransport {
    client: reqwest::Client,
}

impl Default for ReqwestWhiteCircleTransport {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl WhiteCircleTransport for ReqwestWhiteCircleTransport {
    fn post_json<'a>(&'a self, request: WhiteCircleHttpRequest) -> WhiteCircleTransportFuture<'a> {
        Box::pin(async move {
            let response = self
                .client
                .post(request.endpoint)
                .bearer_auth(request.api_key)
                .header(CONTENT_TYPE, "application/json")
                .header("whitecircle-version", request.version)
                .timeout(request.timeout)
                .body(request.body)
                .send()
                .await
                .map_err(WhiteCircleTransportError::Http)?;
            let status = response.status().as_u16();
            let body = response
                .bytes()
                .await
                .map_err(WhiteCircleTransportError::Http)?
                .to_vec();
            Ok(WhiteCircleHttpResponse { status, body })
        })
    }
}

/// WhiteCircle transport error.
#[derive(Debug, Error)]
pub enum WhiteCircleTransportError {
    /// HTTP transport failed.
    #[error(transparent)]
    Http(reqwest::Error),
    /// Test/custom transport failed.
    #[error("{0}")]
    Custom(String),
}

impl From<String> for WhiteCircleTransportError {
    fn from(value: String) -> Self {
        Self::Custom(value)
    }
}

impl From<&str> for WhiteCircleTransportError {
    fn from(value: &str) -> Self {
        Self::Custom(value.to_owned())
    }
}

impl fmt::Debug for ReqwestWhiteCircleTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReqwestWhiteCircleTransport")
            .finish_non_exhaustive()
    }
}

/// WhiteCircle client result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WhiteCircleCheckResult {
    /// Parsed response when available.
    pub response: Option<WhiteCircleCheckResponse>,
    /// Request JSON payload.
    pub request_json: Value,
    /// Response JSON payload when response body existed.
    pub response_json: Option<Value>,
    /// Request duration in milliseconds.
    pub duration_ms: i32,
}

/// WhiteCircle client error.
#[derive(Debug, Error)]
pub enum WhiteCircleCheckError {
    /// Client disabled.
    #[error("whitecircle is disabled")]
    Disabled,
    /// Empty message list.
    #[error("whitecircle messages are empty")]
    EmptyMessages,
    /// Missing deployment ID.
    #[error("whitecircle deployment id is empty")]
    EmptyDeploymentId,
    /// Request serialization failed.
    #[error("whitecircle marshal request: {0}")]
    Marshal(serde_json::Error),
    /// HTTP request failed.
    #[error("whitecircle request failed: {source}")]
    Request {
        /// Partial result.
        result: Box<WhiteCircleCheckResult>,
        /// Transport error.
        source: WhiteCircleTransportError,
    },
    /// Non-success response.
    #[error("whitecircle status {status}: {body}")]
    Status {
        /// Partial result.
        result: Box<WhiteCircleCheckResult>,
        /// HTTP status.
        status: u16,
        /// Truncated body.
        body: String,
    },
    /// Response decoding failed.
    #[error("whitecircle decode response: {source}")]
    Decode {
        /// Partial result.
        result: Box<WhiteCircleCheckResult>,
        /// Decode error.
        source: serde_json::Error,
    },
}

impl WhiteCircleCheckError {
    /// Partial HTTP result, when a request reached the transport.
    #[must_use]
    pub fn partial_result(&self) -> Option<&WhiteCircleCheckResult> {
        match self {
            Self::Request { result, .. }
            | Self::Status { result, .. }
            | Self::Decode { result, .. } => Some(result.as_ref()),
            Self::Disabled | Self::EmptyMessages | Self::EmptyDeploymentId | Self::Marshal(_) => {
                None
            }
        }
    }
}

/// WhiteCircle client.
#[derive(Clone)]
pub struct WhiteCircleClient<T = ReqwestWhiteCircleTransport> {
    cfg: WhiteCircleClientConfig,
    transport: T,
}

impl WhiteCircleClient<ReqwestWhiteCircleTransport> {
    /// Build a reqwest-backed client.
    #[must_use]
    pub fn new(cfg: WhiteCircleClientConfig) -> Self {
        Self {
            cfg: cfg.with_defaults(),
            transport: ReqwestWhiteCircleTransport::default(),
        }
    }
}

impl<T> WhiteCircleClient<T>
where
    T: WhiteCircleTransport,
{
    /// Build a client with custom transport.
    #[must_use]
    pub fn with_transport(cfg: WhiteCircleClientConfig, transport: T) -> Self {
        Self {
            cfg: cfg.with_defaults(),
            transport,
        }
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.cfg.effective_enabled()
    }

    /// Deployment ID.
    #[must_use]
    pub fn deployment_id(&self) -> &str {
        self.cfg.deployment_id.trim()
    }

    /// Request timeout.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.cfg.request_timeout
    }

    pub async fn check_session(
        &self,
        payload: WhiteCircleCheckRequest,
    ) -> Result<WhiteCircleCheckResult, WhiteCircleCheckError> {
        let payload = self.with_default_deployment_id(payload);
        self.validate_payload(&payload)?;

        let request_body = serde_json::to_vec(&payload).map_err(WhiteCircleCheckError::Marshal)?;
        let request_json = serde_json::from_slice(&request_body).unwrap_or(Value::Null);
        let started = Instant::now();
        let response = self
            .transport
            .post_json(WhiteCircleHttpRequest {
                endpoint: self.cfg.endpoint.clone(),
                api_key: self.cfg.api_key.trim().to_owned(),
                version: self.cfg.version.clone(),
                body: request_body,
                timeout: self.cfg.request_timeout,
            })
            .await
            .map_err(|source| WhiteCircleCheckError::Request {
                result: Box::new(WhiteCircleCheckResult {
                    request_json: request_json.clone(),
                    duration_ms: duration_ms(started),
                    ..WhiteCircleCheckResult::default()
                }),
                source,
            })?;

        let mut result = WhiteCircleCheckResult {
            request_json,
            response_json: json_from_body(&response.body),
            duration_ms: duration_ms(started),
            response: None,
        };

        if !(200..300).contains(&response.status) {
            return Err(WhiteCircleCheckError::Status {
                result: Box::new(result),
                status: response.status,
                body: white_circle_status_body(&response.body),
            });
        }

        let parsed = serde_json::from_slice::<WhiteCircleCheckResponse>(&response.body).map_err(
            |source| WhiteCircleCheckError::Decode {
                result: Box::new(result.clone()),
                source,
            },
        )?;
        result.response = Some(parsed);
        Ok(result)
    }

    fn with_default_deployment_id(
        &self,
        mut payload: WhiteCircleCheckRequest,
    ) -> WhiteCircleCheckRequest {
        if payload.deployment_id.trim().is_empty() {
            payload.deployment_id = self.cfg.deployment_id.trim().to_owned();
        }
        payload
    }

    fn validate_payload(
        &self,
        payload: &WhiteCircleCheckRequest,
    ) -> Result<(), WhiteCircleCheckError> {
        if !self.enabled() {
            return Err(WhiteCircleCheckError::Disabled);
        }
        if payload.messages.is_empty() {
            return Err(WhiteCircleCheckError::EmptyMessages);
        }
        if payload.deployment_id.trim().is_empty() {
            return Err(WhiteCircleCheckError::EmptyDeploymentId);
        }
        Ok(())
    }
}

/// WhiteCircle request-build options.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhiteCircleRequestBuildConfig {
    pub flow_name: String,
    pub max_history: usize,
}

impl Default for WhiteCircleRequestBuildConfig {
    fn default() -> Self {
        Self {
            flow_name: "telegram.chat".to_owned(),
            max_history: DEFAULT_CONTEXT_HISTORY_LIMIT,
        }
    }
}

#[must_use]
pub fn build_white_circle_request_for_check(
    cfg: &WhiteCircleRequestBuildConfig,
    input: &DialogInput,
    assistant_model: &str,
) -> WhiteCircleCheckRequest {
    let mut request = WhiteCircleCheckRequest {
        messages: build_white_circle_messages_for_check(cfg, input, assistant_model),
        metadata: Some(build_white_circle_session_metadata(
            cfg,
            input,
            assistant_model,
        )),
        ..WhiteCircleCheckRequest::default()
    };
    let external_session_id =
        obfuscated_white_circle_session_id(input.context.chat_id, input.context.thread_id);
    if !external_session_id.is_empty() {
        request.external_session_id = external_session_id;
        request.include_context = Some(false);
    }
    request
}

fn build_white_circle_messages_for_check(
    cfg: &WhiteCircleRequestBuildConfig,
    input: &DialogInput,
    assistant_model: &str,
) -> Vec<WhiteCircleMessage> {
    let thread_id = input.context.thread_id.unwrap_or_default();
    let request_timestamp = input.timestamp.or(input.message.timestamp);
    let history = select_history_messages_for_context(
        &input.history,
        cfg.max_history,
        input.message.id,
        thread_id,
    );
    let mut out = Vec::with_capacity(history.len() + 1);
    for turn in history {
        let Some(role) = map_white_circle_role(&turn.role) else {
            continue;
        };
        let content = if turn.text.trim().is_empty() {
            turn.original_text.trim()
        } else {
            turn.text.trim()
        };
        let Some(payload) = build_white_circle_content_payload(content, &turn.meta.attachments)
        else {
            continue;
        };
        out.push(WhiteCircleMessage {
            role: role.to_owned(),
            content: payload,
            metadata: build_white_circle_message_metadata(
                role,
                turn.message_id,
                turn.timestamp.or(request_timestamp),
                turn.user_id,
                &turn.name,
                assistant_model,
                &turn.meta,
            ),
        });
    }

    let current_message = selecting_message_text(input);
    if let Some(payload) =
        build_white_circle_content_payload(&current_message, &input.message.meta.attachments)
    {
        out.push(WhiteCircleMessage {
            role: "user".to_owned(),
            content: payload,
            metadata: build_white_circle_message_metadata(
                "user",
                input.message.id,
                input.message.timestamp.or(request_timestamp),
                input.user.id,
                &input.user.full_name,
                assistant_model,
                &input.message.meta,
            ),
        });
    }

    out
}

fn build_white_circle_session_metadata(
    cfg: &WhiteCircleRequestBuildConfig,
    input: &DialogInput,
    assistant_model: &str,
) -> WhiteCircleSessionMetadata {
    let ts = input
        .timestamp
        .or(input.message.timestamp)
        .unwrap_or_else(OffsetDateTime::now_utc);
    let mut environment = Map::new();
    environment.insert("platform".to_owned(), json!("telegram"));
    if !cfg.flow_name.trim().is_empty() {
        environment.insert("flow".to_owned(), json!(cfg.flow_name.trim()));
    }
    if !assistant_model.trim().is_empty() {
        environment.insert("model_name".to_owned(), json!(assistant_model.trim()));
    }
    WhiteCircleSessionMetadata {
        environment,
        session: Some(WhiteCircleSessionMetadataDetail {
            timestamp: rfc3339(ts),
        }),
    }
}

fn build_white_circle_message_metadata(
    role: &str,
    message_id: i32,
    timestamp: Option<OffsetDateTime>,
    user_id: i64,
    user_name: &str,
    assistant_model: &str,
    meta: &openplotva_core::ChatMessageMeta,
) -> Option<WhiteCircleMessageMetadata> {
    let message = white_circle_message_meta(message_id, timestamp);
    let user = (role == "user")
        .then(|| white_circle_user_meta(user_id, user_name, meta))
        .flatten();
    let assistant = (role == "assistant")
        .then(|| white_circle_assistant_meta(assistant_model, meta))
        .flatten();
    (message.is_some() || user.is_some() || assistant.is_some()).then_some(
        WhiteCircleMessageMetadata {
            assistant,
            message,
            user,
        },
    )
}

fn white_circle_message_meta(
    message_id: i32,
    timestamp: Option<OffsetDateTime>,
) -> Option<WhiteCircleMessageMetadataMessage> {
    let id = (message_id > 0).then(|| message_id.to_string());
    let timestamp = timestamp.map(rfc3339);
    (id.is_some() || timestamp.is_some())
        .then_some(WhiteCircleMessageMetadataMessage { id, timestamp })
}

fn white_circle_user_meta(
    user_id: i64,
    user_name: &str,
    meta: &openplotva_core::ChatMessageMeta,
) -> Option<WhiteCircleMessageMetadataUser> {
    let resolved_user_id = if user_id == 0 {
        meta.sender_id
    } else {
        user_id
    };
    let resolved_name = if user_name.trim().is_empty() {
        meta.sender_name.trim()
    } else {
        user_name.trim()
    };
    let alias = obfuscated_white_circle_user_alias(resolved_user_id, resolved_name);
    (!alias.is_empty()).then_some(WhiteCircleMessageMetadataUser {
        id: Some(alias.clone()),
        name: Some(alias),
        ..WhiteCircleMessageMetadataUser::default()
    })
}

fn white_circle_assistant_meta(
    assistant_model: &str,
    meta: &openplotva_core::ChatMessageMeta,
) -> Option<WhiteCircleMessageMetadataAssistant> {
    let model = if assistant_model.trim().is_empty() {
        meta.annotation.trim()
    } else {
        assistant_model.trim()
    };
    (!model.is_empty()).then_some(WhiteCircleMessageMetadataAssistant {
        model_name: Some(model.to_owned()),
        ..WhiteCircleMessageMetadataAssistant::default()
    })
}

fn build_white_circle_content_payload(text: &str, attachments: &[ChatAttachment]) -> Option<Value> {
    let text = text.trim();
    let image_parts = build_white_circle_image_parts(attachments);
    if image_parts.is_empty() {
        return (!text.is_empty()).then(|| json!(text));
    }
    let mut parts = Vec::with_capacity(image_parts.len() + usize::from(!text.is_empty()));
    if !text.is_empty() {
        parts.push(WhiteCircleContentPart {
            kind: "input_text".to_owned(),
            text: text.to_owned(),
            image_url: String::new(),
        });
    }
    parts.extend(image_parts);
    serde_json::to_value(parts).ok()
}

fn build_white_circle_image_parts(attachments: &[ChatAttachment]) -> Vec<WhiteCircleContentPart> {
    let mut out = Vec::new();
    let mut seen = Vec::<String>::new();
    for attachment in attachments {
        let image_url = resolve_white_circle_image_url(attachment);
        if image_url.is_empty() || seen.iter().any(|seen| seen == &image_url) {
            continue;
        }
        seen.push(image_url.clone());
        out.push(WhiteCircleContentPart {
            kind: "input_image".to_owned(),
            text: String::new(),
            image_url,
        });
    }
    out
}

fn resolve_white_circle_image_url(attachment: &ChatAttachment) -> String {
    let mime_type = attachment.mime_type.trim();
    let kind = attachment.kind.trim();
    resolve_white_circle_image_url_candidate(attachment.source.trim(), mime_type, kind)
        .or_else(|| {
            resolve_white_circle_image_url_candidate(attachment.content.trim(), mime_type, kind)
        })
        .unwrap_or_default()
}

fn resolve_white_circle_image_url_candidate(
    candidate: &str,
    mime_type: &str,
    kind: &str,
) -> Option<String> {
    if candidate.is_empty() {
        return None;
    }
    if has_prefix_fold(candidate, "data:image/") {
        return Some(candidate.to_owned());
    }
    if !has_prefix_fold(candidate, "http://") && !has_prefix_fold(candidate, "https://") {
        return None;
    }
    (has_prefix_fold(mime_type, "image/")
        || kind.eq_ignore_ascii_case("image")
        || kind.eq_ignore_ascii_case("photo")
        || looks_like_image_url(candidate))
    .then(|| candidate.to_owned())
}

fn looks_like_image_url(url: &str) -> bool {
    let base = url.split(['?', '#']).next().unwrap_or(url);
    [
        ".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff", ".avif",
    ]
    .iter()
    .any(|suffix| has_suffix_fold(base, suffix))
}

fn map_white_circle_role(role: &str) -> Option<&'static str> {
    match role {
        ROLE_USER => Some("user"),
        ROLE_MODEL => Some("assistant"),
        _ => None,
    }
}

fn selecting_message_text(input: &DialogInput) -> String {
    if !input.message.normalized.trim().is_empty() {
        return input.message.normalized.trim().to_owned();
    }
    if !input.message.text.trim().is_empty() {
        return input.message.text.trim().to_owned();
    }
    input.message.original_text.trim().to_owned()
}

/// WhiteCircle check event recorder.
pub trait WhiteCircleCheckEventRecorder: Send + Sync {
    /// Enqueue one event for persistence.
    fn enqueue_white_circle_check(&self, event: WhiteCircleCheckEvent);
}

#[derive(Clone, Debug, PartialEq)]
pub struct WhiteCircleCheckEvent {
    /// Creation timestamp.
    pub created_at: OffsetDateTime,
    /// Trace source.
    pub source: String,
    /// Flow mode.
    pub mode: Option<String>,
    /// Flow name.
    pub flow: Option<String>,
    /// Obfuscated chat ID.
    pub chat_id: Option<i64>,
    /// Obfuscated thread ID.
    pub thread_id: Option<i32>,
    /// Telegram message ID.
    pub message_id: Option<i32>,
    /// Obfuscated user ID.
    pub user_id: Option<i64>,
    /// WhiteCircle deployment.
    pub deployment_id: String,
    /// External session ID.
    pub external_session_id: Option<String>,
    /// Request messages JSON.
    pub request_messages: Value,
    /// Flagged result.
    pub flagged: Option<bool>,
    /// Internal WhiteCircle session ID.
    pub internal_session_id: Option<String>,
    /// Policy result JSON.
    pub policies: Option<Value>,
    /// Raw response JSON.
    pub response_json: Option<Value>,
    /// Duration in milliseconds.
    pub duration_ms: i32,
    /// Error text.
    pub error: Option<String>,
}

/// WhiteCircle pre-tool wrapper configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WhiteCirclePreToolConfig {
    pub mode: String,
    pub flow: String,
    /// Assistant model.
    pub assistant_model: String,
    /// Maximum history messages for the check.
    pub max_history: usize,
}

impl Default for WhiteCirclePreToolConfig {
    fn default() -> Self {
        Self {
            mode: "legacy".to_owned(),
            flow: "telegram.chat".to_owned(),
            assistant_model: String::new(),
            max_history: DEFAULT_CONTEXT_HISTORY_LIMIT,
        }
    }
}

#[derive(Clone)]
pub struct WhiteCirclePreToolChatProvider<T = ReqwestWhiteCircleTransport> {
    inner: ChatProviderHandle,
    client: WhiteCircleClient<T>,
    recorder: Option<Arc<dyn WhiteCircleCheckEventRecorder>>,
    cfg: WhiteCirclePreToolConfig,
}

impl<T> WhiteCirclePreToolChatProvider<T>
where
    T: WhiteCircleTransport,
{
    /// Build a pre-tool-check wrapper.
    #[must_use]
    pub fn new(
        inner: ChatProviderHandle,
        client: WhiteCircleClient<T>,
        recorder: Option<Arc<dyn WhiteCircleCheckEventRecorder>>,
        cfg: WhiteCirclePreToolConfig,
    ) -> Self {
        Self {
            inner,
            client,
            recorder,
            cfg,
        }
    }

    fn dispatch_pre_tool_check(&self, input: DialogInput) {
        if input.disable_tools || !self.client.enabled() {
            return;
        }
        let assistant_model = if input.model.trim().is_empty() {
            self.cfg.assistant_model.trim().to_owned()
        } else {
            input.model.trim().to_owned()
        };
        let request_cfg = WhiteCircleRequestBuildConfig {
            flow_name: self.cfg.flow.clone(),
            max_history: self.cfg.max_history,
        };
        let mut request =
            build_white_circle_request_for_check(&request_cfg, &input, &assistant_model);
        if request.messages.is_empty() {
            return;
        }
        request.deployment_id = self.client.deployment_id().to_owned();
        if request.deployment_id.trim().is_empty() {
            return;
        }
        let client = self.client.clone();
        let recorder = self.recorder.clone();
        let cfg = self.cfg.clone();
        tokio::spawn(async move {
            let event = run_white_circle_pre_tool_check(client, cfg, input, request).await;
            if let Some(recorder) = recorder {
                recorder.enqueue_white_circle_check(event);
            }
        });
    }
}

impl<T> ChatProvider for WhiteCirclePreToolChatProvider<T>
where
    T: WhiteCircleTransport,
{
    fn provider_name(&self) -> &str {
        self.inner.provider_name()
    }

    fn as_chat_step(&self) -> Option<&dyn ChatStepProvider> {
        // The audit wrap must not hide the inner step seam: without this
        // passthrough the session engine silently falls back to the legacy
        // loop whenever WhiteCircle is enabled.
        self.inner.as_chat_step()?;
        Some(self)
    }
}

impl<T> ChatStepProvider for WhiteCirclePreToolChatProvider<T>
where
    T: WhiteCircleTransport,
{
    fn provider_name(&self) -> &str {
        self.inner.provider_name()
    }

    fn supports_native_tools(&self) -> bool {
        self.inner
            .as_chat_step()
            .is_some_and(ChatStepProvider::supports_native_tools)
    }

    fn run_chat_step<'a>(&'a self, request: ChatStepRequest) -> ChatStepFuture<'a> {
        Box::pin(async move {
            let Some(step) = self.inner.as_chat_step() else {
                let error: crate::ChatProviderError = Box::new(crate::retry::ProviderError::new(
                    self.inner.provider_name(),
                    crate::retry::FailureReason::ProviderUnavailable,
                    "provider has no chat-step support",
                ));
                return Err(error);
            };
            // One fire-and-forget check per session, matching the single
            // per-turn check the legacy run_dialog path dispatches. Gate on
            // the iteration counter, not the transcript: injected inbox
            // messages can pre-populate the transcript before the first step.
            if request.iteration <= 1 {
                self.dispatch_pre_tool_check(request.input.clone());
            }
            step.run_chat_step(request).await
        })
    }
}

async fn run_white_circle_pre_tool_check<T>(
    client: WhiteCircleClient<T>,
    cfg: WhiteCirclePreToolConfig,
    input: DialogInput,
    request: WhiteCircleCheckRequest,
) -> WhiteCircleCheckEvent
where
    T: WhiteCircleTransport,
{
    let created_at = OffsetDateTime::now_utc();
    let result = client.check_session(request.clone()).await;
    let (result, error) = match result {
        Ok(result) => (Some(result), None),
        Err(error) => (error.partial_result().cloned(), Some(error.to_string())),
    };
    white_circle_event_from_result(created_at, &cfg, &input, &request, result.as_ref(), error)
}

fn white_circle_event_from_result(
    created_at: OffsetDateTime,
    cfg: &WhiteCirclePreToolConfig,
    input: &DialogInput,
    request: &WhiteCircleCheckRequest,
    result: Option<&WhiteCircleCheckResult>,
    error: Option<String>,
) -> WhiteCircleCheckEvent {
    let response = result.and_then(|result| result.response.as_ref());
    WhiteCircleCheckEvent {
        created_at,
        source: chat_trace_source(&cfg.mode, "pre_tool_loop"),
        mode: non_empty_string(cfg.mode.trim()),
        flow: non_empty_string(cfg.flow.trim()),
        chat_id: non_zero_i64(obfuscated_white_circle_chat_id(input.context.chat_id)),
        thread_id: obfuscated_white_circle_thread_id(input.context.thread_id),
        message_id: (input.message.id != 0).then_some(input.message.id),
        user_id: non_zero_i64(obfuscated_white_circle_user_id(input.user.id)),
        deployment_id: request.deployment_id.trim().to_owned(),
        external_session_id: response
            .and_then(|response| response.external_session_id.as_deref())
            .or_else(|| non_empty_str(request.external_session_id.trim()))
            .map(ToOwned::to_owned),
        request_messages: serde_json::to_value(&request.messages).unwrap_or_else(|_| json!([])),
        flagged: response.map(|response| response.flagged),
        internal_session_id: response
            .and_then(|response| non_empty_string(&response.internal_session_id)),
        policies: response.and_then(|response| {
            (!response.policies.is_empty()).then(|| Value::Object(response.policies.clone()))
        }),
        response_json: result.and_then(|result| result.response_json.clone()),
        duration_ms: result.map(|result| result.duration_ms).unwrap_or_default(),
        error: error.and_then(|error| non_empty_string(&error)),
    }
}

fn chat_trace_source(mode: &str, step: &str) -> String {
    let base = format!("chat_flow_{}", mode.trim());
    if step.trim().is_empty() {
        base
    } else {
        format!("{base}_{}", step.trim())
    }
}

fn obfuscated_white_circle_session_id(chat_id: i64, thread_id: Option<i32>) -> String {
    if chat_id == 0 {
        return String::new();
    }
    let mut source = format!("chat:{chat_id}");
    if let Some(thread_id) = thread_id.filter(|thread_id| *thread_id > 0) {
        source = format!("{source}:thread:{thread_id}");
    }
    deterministic_alias("session", SCOPE_SESSION_ID, &source)
}

fn obfuscated_white_circle_user_alias(user_id: i64, user_name: &str) -> String {
    if user_id > 0 {
        return deterministic_alias("user", SCOPE_USER_ID, &user_id.to_string());
    }
    if user_name.trim().is_empty() {
        return String::new();
    }
    deterministic_alias("user", SCOPE_USER_NAME, user_name)
}

fn obfuscated_white_circle_chat_id(chat_id: i64) -> i64 {
    if chat_id == 0 {
        0
    } else {
        deterministic_i64(SCOPE_CHAT_ID, chat_id)
    }
}

fn obfuscated_white_circle_user_id(user_id: i64) -> i64 {
    if user_id == 0 {
        0
    } else {
        deterministic_i64(SCOPE_USER_ID, user_id)
    }
}

fn obfuscated_white_circle_thread_id(thread_id: Option<i32>) -> Option<i32> {
    let thread_id = thread_id.filter(|thread_id| *thread_id > 0)?;
    let obfuscated = deterministic_i64(SCOPE_THREAD_ID, i64::from(thread_id));
    (obfuscated > 0).then(|| i32::try_from(obfuscated).unwrap_or(i32::MAX))
}

fn deterministic_alias(prefix: &str, scope: &str, value: &str) -> String {
    let token = deterministic_token(scope, value);
    if token.is_empty() {
        return String::new();
    }
    let short = &token[..token.len().min(12)];
    if prefix.trim().is_empty() {
        short.to_owned()
    } else {
        format!("{}_{short}", prefix.trim())
    }
}

fn deterministic_i64(scope: &str, value: i64) -> i64 {
    if value == 0 {
        return 0;
    }
    let token = deterministic_token(scope, &value.to_string());
    if token.len() < 16 {
        return 0;
    }
    let Ok(mut raw) = u64::from_str_radix(&token[..16], 16) else {
        return 0;
    };
    raw &= i64::MAX as u64;
    if raw == 0 {
        raw = 1;
    }
    i64::try_from(raw).unwrap_or(i64::MAX)
}

fn deterministic_token(scope: &str, value: &str) -> String {
    let scope = scope.trim();
    let value = value.trim();
    if scope.is_empty() || value.is_empty() {
        return String::new();
    }
    let mut hasher = Sha256::new();
    hasher.update(format!("{scope}:{value}:{SIGNATURE_SALT}"));
    hex::encode(hasher.finalize())
}

fn duration_ms(started: Instant) -> i32 {
    i32::try_from(started.elapsed().as_millis()).unwrap_or(i32::MAX)
}

fn json_from_body(body: &[u8]) -> Option<Value> {
    if body.is_empty() {
        return None;
    }
    Some(
        serde_json::from_slice(body)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).trim().to_owned())),
    )
}

fn white_circle_status_body(body: &[u8]) -> String {
    let mut body = String::from_utf8_lossy(body).trim().to_owned();
    if body.len() > 400 {
        body.truncate(400);
    }
    if body.is_empty() {
        "empty body".to_owned()
    } else {
        body
    }
}

fn rfc3339(ts: OffsetDateTime) -> String {
    ts.format(&Rfc3339).unwrap_or_else(|_| ts.to_string())
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

fn non_empty_str(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

fn non_zero_i64(value: i64) -> Option<i64> {
    (value != 0).then_some(value)
}

fn has_prefix_fold(value: &str, prefix: &str) -> bool {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn has_suffix_fold(value: &str, suffix: &str) -> bool {
    value
        .get(value.len().saturating_sub(suffix.len())..)
        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use openplotva_core::{ChatAttachment, ChatMessageMeta};
    use openplotva_dialog::{
        DialogContext, DialogMessage, DialogUser, HistoryMessage, ROLE_MODEL, ROLE_USER,
    };

    use super::*;

    #[derive(Clone, Default)]
    struct FakeTransport {
        requests: Arc<Mutex<Vec<WhiteCircleHttpRequest>>>,
        response: Arc<Mutex<Option<Result<WhiteCircleHttpResponse, WhiteCircleTransportError>>>>,
    }

    impl WhiteCircleTransport for FakeTransport {
        fn post_json<'a>(
            &'a self,
            request: WhiteCircleHttpRequest,
        ) -> WhiteCircleTransportFuture<'a> {
            Box::pin(async move {
                self.requests.lock().expect("requests").push(request);
                self.response
                    .lock()
                    .expect("response")
                    .take()
                    .unwrap_or_else(|| Err("missing fake response".into()))
            })
        }
    }

    #[test]
    fn whitecircle_request_builder_matches_go_payload_shape_and_privacy() {
        let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp");
        let input = DialogInput {
            context: DialogContext {
                chat_id: 100,
                thread_id: Some(9),
                chat_title: "chat".to_owned(),
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 200,
                full_name: "tester".to_owned(),
            },
            message: DialogMessage {
                id: 300,
                normalized: " current ".to_owned(),
                timestamp: Some(ts),
                meta: ChatMessageMeta {
                    attachments: vec![
                        ChatAttachment {
                            source: "HTTPS://example.com/Sunset.JPG?size=large#view".to_owned(),
                            mime_type: "text/plain".to_owned(),
                            ..ChatAttachment::default()
                        },
                        ChatAttachment {
                            source: "HTTPS://example.com/Sunset.JPG?size=large#view".to_owned(),
                            kind: "photo".to_owned(),
                            ..ChatAttachment::default()
                        },
                    ],
                    ..ChatMessageMeta::default()
                },
                ..DialogMessage::default()
            },
            history: vec![
                HistoryMessage {
                    role: ROLE_USER.to_owned(),
                    text: "history user".to_owned(),
                    message_id: 10,
                    thread_id: 9,
                    user_id: 200,
                    name: "tester".to_owned(),
                    timestamp: Some(ts),
                    ..HistoryMessage::default()
                },
                HistoryMessage {
                    role: ROLE_MODEL.to_owned(),
                    text: "history assistant".to_owned(),
                    message_id: 11,
                    thread_id: 9,
                    timestamp: Some(ts),
                    meta: ChatMessageMeta {
                        annotation: "fallback-model".to_owned(),
                        ..ChatMessageMeta::default()
                    },
                    ..HistoryMessage::default()
                },
            ],
            timestamp: Some(ts),
            ..DialogInput::default()
        };

        let request = build_white_circle_request_for_check(
            &WhiteCircleRequestBuildConfig {
                flow_name: "telegram.chat".to_owned(),
                max_history: 10,
            },
            &input,
            "gpt-4o-mini",
        );

        assert_eq!(request.external_session_id, "session_6d3b8277e01e");
        assert_eq!(request.include_context, Some(false));
        assert_eq!(request.messages.len(), 3);
        let assistant = request
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .expect("assistant history message");
        assert_eq!(
            assistant
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.assistant.as_ref())
                .and_then(|assistant| assistant.model_name.as_deref()),
            Some("gpt-4o-mini")
        );
        let current = &request.messages[2];
        assert_eq!(current.role, "user");
        assert!(current.content.is_array());
        let parts = current.content.as_array().expect("parts");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["type"], "input_text");
        assert_eq!(parts[1]["type"], "input_image");
        assert_eq!(
            parts[1]["image_url"],
            "HTTPS://example.com/Sunset.JPG?size=large#view"
        );
        let user_alias = current
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.user.as_ref())
            .and_then(|user| user.id.as_deref())
            .expect("user alias");
        assert!(user_alias.starts_with("user_"));
        assert_ne!(user_alias, "200");
    }

    #[tokio::test]
    async fn whitecircle_client_sends_go_headers_and_decodes_event_result() {
        let transport = FakeTransport::default();
        *transport.response.lock().expect("response") = Some(Ok(WhiteCircleHttpResponse {
            status: 200,
            body: br#"{"flagged":true,"internal_session_id":"session-1","external_session_id":"session-x","policies":{"policy-1":{"flagged":true,"flagged_source":["text"],"name":"Drugs"}}}"#.to_vec(),
        }));
        let client = WhiteCircleClient::with_transport(
            WhiteCircleClientConfig {
                enabled: true,
                api_key: "test-key".to_owned(),
                deployment_id: "deployment-1".to_owned(),
                endpoint: "https://whitecircle.test/check".to_owned(),
                version: "2025-12-01".to_owned(),
                request_timeout: Duration::from_secs(2),
            },
            transport.clone(),
        );

        let result = client
            .check_session(WhiteCircleCheckRequest {
                messages: vec![WhiteCircleMessage {
                    role: "user".to_owned(),
                    content: json!("hello"),
                    metadata: None,
                }],
                ..WhiteCircleCheckRequest::default()
            })
            .await
            .expect("check result");

        let requests = transport.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].endpoint, "https://whitecircle.test/check");
        assert_eq!(requests[0].api_key, "test-key");
        assert_eq!(requests[0].version, "2025-12-01");
        assert_eq!(result.request_json["deployment_id"], "deployment-1");
        assert!(result.response.as_ref().expect("response").flagged);
        assert_eq!(
            result.response_json.as_ref().expect("response json")["internal_session_id"],
            "session-1"
        );
    }

    #[test]
    fn whitecircle_event_uses_go_source_and_obfuscated_runtime_identity() {
        let input = DialogInput {
            context: DialogContext {
                chat_id: 100,
                thread_id: None,
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 200,
                full_name: "tester".to_owned(),
            },
            message: DialogMessage {
                id: 300,
                normalized: "hello".to_owned(),
                ..DialogMessage::default()
            },
            ..DialogInput::default()
        };
        let cfg = WhiteCirclePreToolConfig::default();
        let request = WhiteCircleCheckRequest {
            deployment_id: "deployment-1".to_owned(),
            external_session_id: "session-x".to_owned(),
            messages: vec![WhiteCircleMessage {
                role: "user".to_owned(),
                content: json!("hello"),
                metadata: None,
            }],
            ..WhiteCircleCheckRequest::default()
        };
        let result = WhiteCircleCheckResult {
            response: Some(WhiteCircleCheckResponse {
                flagged: true,
                internal_session_id: "internal-1".to_owned(),
                external_session_id: Some("session-x".to_owned()),
                policies: Map::from_iter([("policy".to_owned(), json!({"flagged": true}))]),
            }),
            request_json: json!({}),
            response_json: Some(json!({"flagged": true})),
            duration_ms: 7,
        };

        let event = white_circle_event_from_result(
            OffsetDateTime::UNIX_EPOCH,
            &cfg,
            &input,
            &request,
            Some(&result),
            None,
        );

        assert_eq!(event.source, "chat_flow_legacy_pre_tool_loop");
        assert_eq!(event.mode.as_deref(), Some("legacy"));
        assert_eq!(event.flow.as_deref(), Some("telegram.chat"));
        assert_eq!(event.flagged, Some(true));
        assert_eq!(event.external_session_id.as_deref(), Some("session-x"));
        assert_ne!(event.chat_id, Some(100));
        assert_ne!(event.user_id, Some(200));
    }

    #[derive(Default)]
    struct SteppedInnerProvider {
        step_calls: Arc<Mutex<usize>>,
    }

    impl ChatProvider for SteppedInnerProvider {
        fn provider_name(&self) -> &str {
            "stepped-inner"
        }

        fn as_chat_step(&self) -> Option<&dyn ChatStepProvider> {
            Some(self)
        }
    }

    impl ChatStepProvider for SteppedInnerProvider {
        fn provider_name(&self) -> &str {
            "stepped-inner"
        }

        fn supports_native_tools(&self) -> bool {
            true
        }

        fn run_chat_step<'a>(&'a self, _request: ChatStepRequest) -> ChatStepFuture<'a> {
            Box::pin(async move {
                *self.step_calls.lock().expect("step calls") += 1;
                Ok(openplotva_dialog::ChatStepOutput {
                    provider: "stepped-inner".to_owned(),
                    text: "step reply".to_owned(),
                    ..openplotva_dialog::ChatStepOutput::default()
                })
            })
        }
    }

    struct SteplessInnerProvider;

    impl ChatProvider for SteplessInnerProvider {
        fn provider_name(&self) -> &str {
            "stepless-inner"
        }
    }

    fn pre_tool_provider_with_inner(
        inner: ChatProviderHandle,
        transport: FakeTransport,
    ) -> WhiteCirclePreToolChatProvider<FakeTransport> {
        WhiteCirclePreToolChatProvider::new(
            inner,
            WhiteCircleClient::with_transport(
                WhiteCircleClientConfig {
                    enabled: true,
                    api_key: "test-key".to_owned(),
                    deployment_id: "deployment-1".to_owned(),
                    endpoint: "https://whitecircle.test/check".to_owned(),
                    version: "2025-12-01".to_owned(),
                    request_timeout: Duration::from_secs(2),
                },
                transport,
            ),
            None,
            WhiteCirclePreToolConfig::default(),
        )
    }

    fn step_check_input() -> DialogInput {
        DialogInput {
            context: DialogContext {
                chat_id: 100,
                ..DialogContext::default()
            },
            user: DialogUser {
                id: 200,
                full_name: "tester".to_owned(),
            },
            message: DialogMessage {
                id: 300,
                normalized: "hello".to_owned(),
                ..DialogMessage::default()
            },
            ..DialogInput::default()
        }
    }

    async fn wait_for_dispatched_checks(transport: &FakeTransport, expected: usize) -> usize {
        for _ in 0..100 {
            let seen = transport.requests.lock().expect("requests").len();
            if seen >= expected {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        transport.requests.lock().expect("requests").len()
    }

    #[test]
    fn whitecircle_wrapper_step_seam_mirrors_inner_provider() {
        let stepped = pre_tool_provider_with_inner(
            Arc::new(SteppedInnerProvider::default()),
            FakeTransport::default(),
        );
        let seam = stepped.as_chat_step().expect("step seam passes through");
        assert!(seam.supports_native_tools());

        let stepless =
            pre_tool_provider_with_inner(Arc::new(SteplessInnerProvider), FakeTransport::default());
        assert!(stepless.as_chat_step().is_none());
    }

    #[tokio::test]
    async fn whitecircle_step_delegates_and_checks_only_the_first_iteration() {
        let inner = Arc::new(SteppedInnerProvider::default());
        let step_calls = Arc::clone(&inner.step_calls);
        let transport = FakeTransport::default();
        *transport.response.lock().expect("response") = Some(Ok(WhiteCircleHttpResponse {
            status: 200,
            body: br#"{"flagged":false,"internal_session_id":"s","policies":{}}"#.to_vec(),
        }));
        let provider = pre_tool_provider_with_inner(inner, transport.clone());
        let seam = provider.as_chat_step().expect("step seam");

        // Injected inbox messages can land in the transcript before the very
        // first step; the audit must still dispatch for that turn.
        let first = seam
            .run_chat_step(ChatStepRequest {
                input: step_check_input(),
                transcript: vec![openplotva_dialog::SessionMessage::InjectedUser {
                    rendered: "injected before the first step".to_owned(),
                }],
                tools: openplotva_dialog::ToolsMode::Disabled,
                iteration: 1,
            })
            .await
            .expect("first step");
        assert_eq!(first.text, "step reply");
        assert_eq!(wait_for_dispatched_checks(&transport, 1).await, 1);

        let second = seam
            .run_chat_step(ChatStepRequest {
                input: step_check_input(),
                transcript: vec![openplotva_dialog::SessionMessage::Assistant {
                    text: "step reply".to_owned(),
                    tool_calls: Vec::new(),
                }],
                tools: openplotva_dialog::ToolsMode::Disabled,
                iteration: 2,
            })
            .await
            .expect("second step");
        assert_eq!(second.text, "step reply");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            transport.requests.lock().expect("requests").len(),
            1,
            "later iterations must not re-dispatch the audit check"
        );
        assert_eq!(*step_calls.lock().expect("step calls"), 2);
    }
}
