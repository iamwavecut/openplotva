//! HTTP server surfaces for OpenPlotva.

mod runtime_graphql;

use std::{
    env,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, LazyLock},
    time::Instant as StdInstant,
};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
pub use openplotva_core::{ChatSettings, ChatSettingsUpdate};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use x509_parser::parse_x509_certificate;

pub use runtime_graphql::{
    RuntimeAifarmCapacitySnapshotData, RuntimeApiGraphqlSnapshot, RuntimeApiLiveDiagnostics,
    RuntimeApiSchema, RuntimeCacheInspector, RuntimeCacheSnapshotData, RuntimeCacheStatsData,
    RuntimeChatConnectionData, RuntimeChatData, RuntimeChatMemberData,
    RuntimeChatMemberWithUserData, RuntimeChatsFilter, RuntimeDispatchFailureData,
    RuntimeDispatcherFailureInspector, RuntimeDispatcherInspector, RuntimeDispatcherStatsData,
    RuntimeEntityReader, RuntimeEntityReaderFuture, RuntimeGeminiCachePurgeResultData,
    RuntimeGeminiCachePurger, RuntimeGeminiCachePurgerFuture, RuntimeIngestionGatesData,
    RuntimeJobAnalyticsStatData, RuntimeLlmAnalyticsData,
    RuntimeLlmAnalyticsInferenceParamStatData, RuntimeLlmAnalyticsModelSeriesPointData,
    RuntimeLlmAnalyticsModelStatData, RuntimeLlmAnalyticsProviderStatData,
    RuntimeLlmAnalyticsReader, RuntimeLlmAnalyticsReaderFuture, RuntimeLlmAnalyticsSeriesPointData,
    RuntimeLlmAnalyticsStageMetricData, RuntimeLlmAnalyticsTopChatData,
    RuntimeLlmAnalyticsTotalsData, RuntimeLlmGenConfigData, RuntimeLlmRequestChatData,
    RuntimeLlmRequestData, RuntimeLlmRequestMessageData, RuntimeLlmRequestResultData,
    RuntimeLlmRequestUserData, RuntimeLlmRequestsFilter, RuntimeLlmRunInspector,
    RuntimeLlmRunsFilter, RuntimeLlmTraceInspector, RuntimeLogEntry, RuntimeLogInspector,
    RuntimeLogQuery, RuntimeMemoryRestartFuture, RuntimeMemoryRestartResultData,
    RuntimeMemoryRestarter, RuntimeRedisInspector, RuntimeRedisInspectorFuture,
    RuntimeRedisPrefixGroup, RuntimeRedisValue, RuntimeRoutingEventData,
    RuntimeRoutingEventInspector, RuntimeRoutingEventsFilter, RuntimeSafetyCheckConnectionData,
    RuntimeSafetyCheckData, RuntimeSafetyCheckReader, RuntimeSafetyCheckReaderFuture,
    RuntimeSafetyChecksFilter, RuntimeSqlReadRequest, RuntimeSqlReadResult, RuntimeSqlReader,
    RuntimeSqlReaderFuture, RuntimeSubscriptionData, RuntimeTaskmanDiagnosticsData,
    RuntimeTaskmanInspector, RuntimeTaskmanJobData, RuntimeTaskmanJobDetailsData,
    RuntimeTaskmanJobFuture, RuntimeTaskmanJobListEntryData, RuntimeTaskmanJobListResultData,
    RuntimeTaskmanJobMessageData, RuntimeTaskmanJobSummaryData, RuntimeTaskmanJobsFilter,
    RuntimeTaskmanQueueDiagnosticsData, RuntimeTelegramDeliveryFuture,
    RuntimeTelegramDeliveryInspector, RuntimeTelegramDeliveryListFilter,
    RuntimeTelegramOutboxAttemptData, RuntimeTelegramOutboxItemData,
    RuntimeTelegramOutboxMutationResultData, RuntimeTelegramOutboxRetryRequest,
    RuntimeTelegramOutboxStatsData, RuntimeTelegramUpdateAttemptData,
    RuntimeTelegramUpdateInboxItemData, RuntimeTelegramUpdateInboxStatsData,
    RuntimeTurnOutcomeData, RuntimeTurnOutcomeInspector, RuntimeTurnOutcomesFilter,
    RuntimeUpdatesInspector, RuntimeUpdatesInspectorFuture, RuntimeUpdatesRuntimeData,
    RuntimeUpdatesTaskData, RuntimeUserConnectionData, RuntimeUserData, RuntimeUserDetailsData,
    RuntimeUserLookup, RuntimeUsersFilter, RuntimeVipCacheData, RuntimeVipEventData,
    RuntimeVipSummaryData, RuntimeVirtualDialogData, RuntimeVirtualDialogDeleteResultData,
    RuntimeVirtualDialogFuture, RuntimeVirtualDialogManager, RuntimeVirtualDialogMessageData,
    RuntimeVirtualDialogSendRequest, RuntimeVirtualDialogStartRequest,
    RuntimeVirtualDialogToolMode, runtime_api_graphql_schema,
    runtime_api_graphql_schema_with_diagnostics, runtime_api_graphql_schema_with_live_diagnostics,
    runtime_api_graphql_schema_with_redis,
};

/// Health response returned by `/api/health`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthResponse {
    /// Service status.
    pub status: &'static str,
    /// Service name.
    pub service: &'static str,
}

/// Readiness response returned by `/api/ready`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadinessResponse {
    /// Overall readiness status.
    pub status: &'static str,
    /// Service name.
    pub service: &'static str,
    /// Dependency checks captured during startup.
    pub checks: Vec<ReadinessCheck>,
}

/// Individual readiness check.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReadinessCheck {
    /// Dependency or invariant name.
    pub name: String,
    /// Check status.
    pub status: &'static str,
    /// Human-readable detail.
    pub detail: String,
}

pub const RATE_LIMITED_CHAT_KEY_PREFIX: &str = "plotva:rate_limited_chat:";

pub const RUNTIME_API_AUTHENTICATE_HEADER: &str = r#"Bearer realm="plotva-runtime-api""#;

pub const RUNTIME_API_TOKEN_TTL: TimeDuration = TimeDuration::hours(24);

pub const RUNTIME_API_TOKEN_PREFIX: &str = "prt";

pub const RUNTIME_API_TOKEN_ID_BYTES: usize = 8;

pub const RUNTIME_API_TOKEN_SECRET_SIZE: usize = 32;

pub const RUNTIME_API_PPROF_PROFILES: [&str; 9] = [
    "allocs",
    "block",
    "cmdline",
    "goroutine",
    "heap",
    "mutex",
    "profile",
    "symbol",
    "trace",
];

pub const RUNTIME_API_PPROF_SUBTREE_PROFILES: [&str; 10] = [
    "allocs",
    "block",
    "cmdline",
    "goroutine",
    "heap",
    "mutex",
    "profile",
    "symbol",
    "threadcreate",
    "trace",
];

pub const ACTION_SEND_TEXT: &str = "send_text";
pub const ACTION_SEND_IMAGE: &str = "send_image";
pub const ACTION_SEND_STICKER: &str = "send_sticker";
pub const ACTION_SEND_VIDEO: &str = "send_video";
pub const ACTION_SEND_AUDIO: &str = "send_audio";
pub const ACTION_SEND_FILE: &str = "send_file";
pub const ACTION_SEND_POLL: &str = "send_poll";
pub const ACTION_SEND_MESSAGE: &str = "send_message";
pub const ACTION_PIN_MESSAGE: &str = "pin_message";
pub const ACTION_EDIT_MESSAGE: &str = "edit_message";

/// Runtime API token metadata stored in Postgres.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeToken {
    /// Token ID, without the `prt_` prefix.
    pub id: String,
    /// Token creation timestamp. Missing or invalid timestamps are expired.
    pub created_at: Option<OffsetDateTime>,
}

/// Parsed runtime API token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedRuntimeToken {
    /// Token ID, without the `prt_` prefix.
    pub id: String,
    /// Token secret after the first dot separator.
    pub secret: String,
}

/// Runtime token parse failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RuntimeTokenParseError {
    /// Token is missing the `.` separator or has an empty secret.
    #[error("invalid runtime token format")]
    InvalidFormat,
    /// Token is missing the `prt_` prefix or has an empty token ID.
    #[error("invalid runtime token prefix")]
    InvalidPrefix,
}

/// Boxed future returned by runtime API token validators.
pub type RuntimeTokenValidationFuture<'a> = Pin<Box<dyn Future<Output = bool> + Send + 'a>>;

/// Minimal token-validation boundary for the runtime API auth middleware.
pub trait RuntimeTokenValidator {
    /// Validate a presented `prt_<id>.<secret>` token.
    fn validate_runtime_token<'a>(&'a self, token: &'a str) -> RuntimeTokenValidationFuture<'a>;
}

/// Runtime API TLS material load failure.
#[derive(Debug, Error)]
pub enum RuntimeApiTlsError {
    /// Certificate PEM could not be decoded.
    #[error("parse runtime api certificate pem: {0}")]
    CertificatePem(#[source] rustls_pki_types::pem::Error),
    /// Certificate DER could not be decoded as X.509.
    #[error("parse runtime api x509 certificate: {0}")]
    CertificateX509(String),
    /// Certificate chain is empty.
    #[error("runtime api certificate pem is empty")]
    EmptyCertificateChain,
    /// Self-signed TLS material could not be generated.
    #[error("generate runtime api self-signed certificate: {0}")]
    Generate(#[from] rcgen::Error),
    /// Private key PEM could not be decoded.
    #[error("parse runtime api private key pem: {0}")]
    PrivateKeyPem(#[source] rustls_pki_types::pem::Error),
    /// Private key PEM is missing.
    #[error("runtime api private key pem is empty")]
    MissingPrivateKey,
    /// Rustls rejected the certificate/key pair.
    #[error("build runtime api tls config: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Self-signed runtime API TLS material generated for the current process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeApiGeneratedTlsMaterial {
    /// PEM encoded certificate chain.
    pub cert_pem: Vec<u8>,
    /// PEM encoded private key.
    pub key_pem: Vec<u8>,
    /// Public-key pin computed from the certificate subject public key info.
    pub tls_public_key_pin: String,
}

pub struct RuntimeApiTlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl RuntimeApiTlsListener {
    /// Wrap a TCP listener with the runtime API TLS acceptor.
    #[must_use]
    pub fn new(listener: TcpListener, acceptor: TlsAcceptor) -> Self {
        Self { listener, acceptor }
    }
}

impl axum::serve::Listener for RuntimeApiTlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::error!(%error, "runtime API TCP accept error");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };

            match self.acceptor.accept(stream).await {
                Ok(stream) => return (stream, addr),
                Err(error) => {
                    tracing::debug!(%error, %addr, "runtime API TLS handshake failed");
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}

#[derive(Clone)]
struct RuntimeApiState<Validator> {
    validator: Arc<Validator>,
    graphql: RuntimeApiSchema,
}

static RUNTIME_API_DIAGNOSTICS_STARTED: LazyLock<StdInstant> = LazyLock::new(StdInstant::now);

pub fn runtime_api_tls_acceptor_from_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<TlsAcceptor, RuntimeApiTlsError> {
    Ok(TlsAcceptor::from(Arc::new(
        runtime_api_tls_config_from_pem(cert_pem, key_pem)?,
    )))
}

pub fn runtime_api_tls_config_from_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> Result<rustls::ServerConfig, RuntimeApiTlsError> {
    let certs = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(RuntimeApiTlsError::CertificatePem)?;
    if certs.is_empty() {
        return Err(RuntimeApiTlsError::EmptyCertificateChain);
    }

    let key = PrivateKeyDer::from_pem_slice(key_pem).map_err(|error| match error {
        rustls_pki_types::pem::Error::NoItemsFound => RuntimeApiTlsError::MissingPrivateKey,
        error => RuntimeApiTlsError::PrivateKeyPem(error),
    })?;

    let mut config = rustls::ServerConfig::builder_with_protocol_versions(&[
        &rustls::version::TLS13,
        &rustls::version::TLS12,
    ])
    .with_no_client_auth()
    .with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// Compute the browser-style SHA-256 public-key pin from the first certificate in a PEM chain.
pub fn runtime_api_tls_public_key_pin_from_pem(
    cert_pem: &[u8],
) -> Result<String, RuntimeApiTlsError> {
    let certs = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(RuntimeApiTlsError::CertificatePem)?;
    let cert = certs
        .first()
        .ok_or(RuntimeApiTlsError::EmptyCertificateChain)?;
    let (_, parsed) = parse_x509_certificate(cert.as_ref())
        .map_err(|error| RuntimeApiTlsError::CertificateX509(format!("{error:?}")))?;
    let digest = Sha256::digest(parsed.tbs_certificate.subject_pki.raw);
    Ok(format!("sha256//{}", BASE64_STANDARD.encode(digest)))
}

/// Generate self-signed runtime API TLS material and its matching public-key pin.
pub fn generate_runtime_api_tls_material<I, S>(
    subject_alt_names: I,
) -> Result<RuntimeApiGeneratedTlsMaterial, RuntimeApiTlsError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut names = subject_alt_names
        .into_iter()
        .map(Into::into)
        .filter(|name| !name.trim().is_empty())
        .collect::<Vec<_>>();
    if names.is_empty() {
        names.push("localhost".to_owned());
    }
    let CertifiedKey { cert, signing_key } = generate_simple_self_signed(names)?;
    let cert_pem = cert.pem().into_bytes();
    let key_pem = signing_key.serialize_pem().into_bytes();
    let tls_public_key_pin = runtime_api_tls_public_key_pin_from_pem(&cert_pem)?;
    Ok(RuntimeApiGeneratedTlsMaterial {
        cert_pem,
        key_pem,
        tls_public_key_pin,
    })
}

#[must_use]
pub fn parse_bearer_token(header: &str) -> Option<&str> {
    let header = header.trim();
    let (scheme, value) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return None;
    }
    let value = value.trim();
    if value.is_empty() || value.contains(' ') {
        return None;
    }
    Some(value)
}

#[must_use]
pub fn format_runtime_token(token_id: &str, secret: &str) -> String {
    format!("{RUNTIME_API_TOKEN_PREFIX}_{token_id}.{secret}")
}

pub fn parse_runtime_token(token: &str) -> Result<ParsedRuntimeToken, RuntimeTokenParseError> {
    let (prefix, secret) = token
        .trim()
        .split_once('.')
        .ok_or(RuntimeTokenParseError::InvalidFormat)?;
    if secret.is_empty() {
        return Err(RuntimeTokenParseError::InvalidFormat);
    }
    let token_prefix = format!("{RUNTIME_API_TOKEN_PREFIX}_");
    let token_id = prefix
        .strip_prefix(&token_prefix)
        .ok_or(RuntimeTokenParseError::InvalidPrefix)?;
    if token_id.is_empty() {
        return Err(RuntimeTokenParseError::InvalidPrefix);
    }

    Ok(ParsedRuntimeToken {
        id: token_id.to_owned(),
        secret: secret.to_owned(),
    })
}

#[must_use]
pub fn hash_runtime_token_secret(secret: &str) -> Vec<u8> {
    Sha256::digest(secret.as_bytes()).to_vec()
}

#[must_use]
pub fn runtime_token_secret_hash_matches(expected_hash: &[u8], secret: &str) -> bool {
    let provided_hash = hash_runtime_token_secret(secret);
    if expected_hash.len() != provided_hash.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (expected, provided) in expected_hash.iter().zip(provided_hash) {
        diff |= expected ^ provided;
    }
    diff == 0
}

#[must_use]
pub fn runtime_token_expired_at(created_at: Option<OffsetDateTime>, now: OffsetDateTime) -> bool {
    let Some(created_at) = created_at else {
        return true;
    };
    now > created_at + RUNTIME_API_TOKEN_TTL
}

/// Build the auth-protected runtime API router.
///
pub fn runtime_api_router<Validator>(validator: Validator) -> Router
where
    Validator: RuntimeTokenValidator + Send + Sync + 'static,
{
    runtime_api_router_with_graphql(validator, RuntimeApiGraphqlSnapshot::default())
}

/// Build the auth-protected runtime API router with a diagnostic GraphQL snapshot.
pub fn runtime_api_router_with_graphql<Validator>(
    validator: Validator,
    graphql_snapshot: RuntimeApiGraphqlSnapshot,
) -> Router
where
    Validator: RuntimeTokenValidator + Send + Sync + 'static,
{
    runtime_api_router_with_graphql_and_redis(validator, graphql_snapshot, None)
}

/// Build the auth-protected runtime API router with GraphQL and live Redis diagnostics.
pub fn runtime_api_router_with_graphql_and_redis<Validator>(
    validator: Validator,
    graphql_snapshot: RuntimeApiGraphqlSnapshot,
    redis_inspector: Option<Arc<dyn RuntimeRedisInspector>>,
) -> Router
where
    Validator: RuntimeTokenValidator + Send + Sync + 'static,
{
    runtime_api_router_with_graphql_diagnostics(validator, graphql_snapshot, redis_inspector, None)
}

/// Build the auth-protected runtime API router with GraphQL and live diagnostics.
pub fn runtime_api_router_with_graphql_diagnostics<Validator>(
    validator: Validator,
    graphql_snapshot: RuntimeApiGraphqlSnapshot,
    redis_inspector: Option<Arc<dyn RuntimeRedisInspector>>,
    sql_reader: Option<Arc<dyn RuntimeSqlReader>>,
) -> Router
where
    Validator: RuntimeTokenValidator + Send + Sync + 'static,
{
    runtime_api_router_with_graphql_live_diagnostics(
        validator,
        graphql_snapshot,
        RuntimeApiLiveDiagnostics {
            redis_inspector,
            sql_reader,
            ..RuntimeApiLiveDiagnostics::default()
        },
    )
}

/// Build the auth-protected runtime API router with all currently live diagnostics.
pub fn runtime_api_router_with_graphql_live_diagnostics<Validator>(
    validator: Validator,
    graphql_snapshot: RuntimeApiGraphqlSnapshot,
    diagnostics: RuntimeApiLiveDiagnostics,
) -> Router
where
    Validator: RuntimeTokenValidator + Send + Sync + 'static,
{
    let state = Arc::new(RuntimeApiState {
        validator: Arc::new(validator),
        graphql: runtime_api_graphql_schema_with_live_diagnostics(graphql_snapshot, diagnostics),
    });
    Router::new()
        .route("/graphql", any(runtime_api_graphql::<Validator>))
        .route("/debug/pprof/", any(runtime_api_pprof_index::<Validator>))
        .route(
            "/debug/pprof/{*path}",
            any(runtime_api_pprof_path::<Validator>),
        )
        .fallback(runtime_api_not_found::<Validator>)
        .with_state(state)
}

async fn runtime_api_graphql<Validator>(
    State(state): State<Arc<RuntimeApiState<Validator>>>,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    Validator: RuntimeTokenValidator + Send + Sync,
{
    if let Err(response) = runtime_api_authorize(state.as_ref(), &headers).await {
        return response;
    }
    if method != Method::POST {
        return runtime_api_method_not_allowed_response();
    }
    let request = match serde_json::from_slice::<async_graphql::Request>(&body) {
        Ok(request) => request,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("{{\"errors\":[{{\"message\":\"invalid graphql request: {error}\"}}]}}\n"),
            )
                .into_response();
        }
    };
    Json(state.graphql.execute(request).await).into_response()
}

async fn runtime_api_pprof_index<Validator>(
    State(state): State<Arc<RuntimeApiState<Validator>>>,
    headers: HeaderMap,
) -> Response
where
    Validator: RuntimeTokenValidator + Send + Sync,
{
    if let Err(response) = runtime_api_authorize(state.as_ref(), &headers).await {
        return response;
    }
    runtime_api_pprof_response("")
}

async fn runtime_api_pprof_path<Validator>(
    State(state): State<Arc<RuntimeApiState<Validator>>>,
    Path(path): Path<String>,
    headers: HeaderMap,
) -> Response
where
    Validator: RuntimeTokenValidator + Send + Sync,
{
    if let Err(response) = runtime_api_authorize(state.as_ref(), &headers).await {
        return response;
    }
    runtime_api_pprof_response(&path)
}

async fn runtime_api_not_found<Validator>(
    State(state): State<Arc<RuntimeApiState<Validator>>>,
    headers: HeaderMap,
) -> Response
where
    Validator: RuntimeTokenValidator + Send + Sync,
{
    if let Err(response) = runtime_api_authorize(state.as_ref(), &headers).await {
        return response;
    }
    StatusCode::NOT_FOUND.into_response()
}

async fn runtime_api_authorize<Validator>(
    state: &RuntimeApiState<Validator>,
    headers: &HeaderMap,
) -> Result<(), Response>
where
    Validator: RuntimeTokenValidator + Send + Sync,
{
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_bearer_token)
        .ok_or_else(runtime_api_unauthorized_response)?;
    if state.validator.validate_runtime_token(token).await {
        Ok(())
    } else {
        Err(runtime_api_unauthorized_response())
    }
}

#[must_use]
pub fn runtime_api_unauthorized_response() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, RUNTIME_API_AUTHENTICATE_HEADER)],
        "unauthorized\n",
    )
        .into_response()
}

#[must_use]
pub fn runtime_api_method_not_allowed_response() -> Response {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        "{\"error\":\"method not allowed\"}\n",
    )
        .into_response()
}

#[must_use]
pub fn runtime_api_pprof_response(path: &str) -> Response {
    let profile = path.trim_matches('/');
    if profile.is_empty() {
        return runtime_api_pprof_index_response();
    }
    match profile {
        "cmdline" => runtime_api_pprof_cmdline_response(),
        "profile" => runtime_api_pprof_cpu_profile_response(),
        "trace" => runtime_api_pprof_text_response(
            "trace",
            "scheduler tracing is not collected by the Rust runtime shell yet",
        ),
        "symbol" => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            "num_symbols: 0\n",
        )
            .into_response(),
        "goroutine" => runtime_api_pprof_text_response(
            "goroutine",
            "Rust exposes OS threads/tasks differently; current process/thread metadata follows",
        ),
        "heap" | "allocs" => runtime_api_pprof_text_response(
            profile,
            "allocator profile capture is not enabled; process metadata follows",
        ),
        "mutex" | "block" => runtime_api_pprof_text_response(
            profile,
            "contention profile capture is not enabled; process metadata follows",
        ),
        "threadcreate" => runtime_api_pprof_text_response(
            "threadcreate",
            "Rust thread creation profiling is not enabled; process metadata follows",
        ),
        _ => (
            StatusCode::NOT_FOUND,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            format!("unknown profile: {profile}\n"),
        )
            .into_response(),
    }
}

fn runtime_api_pprof_index_response() -> Response {
    let links = RUNTIME_API_PPROF_SUBTREE_PROFILES
        .iter()
        .map(|profile| format!("<li><a href=\"{profile}\">{profile}</a></li>"))
        .collect::<Vec<_>>()
        .join("\n");
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        format!(
            "<html><head><title>/debug/pprof/</title></head><body>\
             <h1>OpenPlotva runtime diagnostics</h1>\
             <p>Runtime process diagnostics endpoint.</p>\
             <ul>{links}</ul>\
             </body></html>\n"
        ),
    )
        .into_response()
}

fn runtime_api_pprof_cmdline_response() -> Response {
    let mut body = env::args().collect::<Vec<_>>().join("\0");
    body.push('\0');
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response()
}

fn runtime_api_pprof_cpu_profile_response() -> Response {
    runtime_api_pprof_text_response(
        "profile",
        "CPU profile capture is intentionally unavailable; process metadata follows",
    )
}

fn runtime_api_pprof_text_response(profile: &str, note: &str) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        runtime_api_pprof_text(profile, note),
    )
        .into_response()
}

fn runtime_api_pprof_text(profile: &str, note: &str) -> String {
    let uptime = RUNTIME_API_DIAGNOSTICS_STARTED.elapsed();
    let parallelism = std::thread::available_parallelism().map_or(0, usize::from);
    let executable = env::current_exe()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "<unknown>".to_owned());
    let current_dir = env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| "<unknown>".to_owned());
    format!(
        "openplotva runtime profile analogue\n\
         profile: {profile}\n\
         note: {note}\n\
         pid: {}\n\
         uptime_seconds: {}\n\
         available_parallelism: {parallelism}\n\
         executable: {executable}\n\
         current_dir: {current_dir}\n\
         target: {}-{}\n",
        std::process::id(),
        uptime.as_secs(),
        env::consts::OS,
        env::consts::ARCH
    )
}

#[derive(Clone, Debug)]
struct AppState {
    readiness: ReadinessResponse,
}

/// Build the HTTP router for the current app shell.
pub fn router() -> Router {
    router_with_readiness(ReadinessResponse::ready(Vec::new()))
}

/// Build the HTTP router with a startup readiness snapshot.
pub fn router_with_readiness(readiness: ReadinessResponse) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/ready", get(ready))
        .with_state(Arc::new(AppState { readiness }))
}

/// Build the health payload without transport details.
pub fn health_response() -> HealthResponse {
    HealthResponse {
        status: "ok",
        service: openplotva_core::PROJECT_NAME,
    }
}

pub fn rate_limit_key(chat_id: i64) -> String {
    format!("rate_limit:{chat_id}")
}

pub fn rate_limited_chat_key(chat_id: i64) -> String {
    format!("{RATE_LIMITED_CHAT_KEY_PREFIX}{chat_id}")
}

pub fn permission_cache_key(chat_id: i64, action: &str) -> String {
    format!("perm_check:{chat_id}:{action}")
}

pub fn vip_cache_key(user_id: i64) -> String {
    format!("vip:{user_id}")
}

pub fn can_perform_action(chat_type: &str, settings: Option<&ChatSettings>, action: &str) -> bool {
    if chat_type == "private" {
        return true;
    }
    let Some(settings) = settings else {
        return true;
    };

    match action {
        ACTION_SEND_TEXT | ACTION_EDIT_MESSAGE | ACTION_SEND_MESSAGE => {
            settings.enable_global_text_reply
        }
        ACTION_SEND_IMAGE | ACTION_SEND_STICKER | ACTION_SEND_AUDIO => {
            settings.enable_global_draw_reply
        }
        _ => false,
    }
}

pub fn permission_error_chat_type(chat_type: Option<&str>) -> String {
    match chat_type
        .map(str::trim)
        .filter(|chat_type| !chat_type.is_empty())
    {
        Some(chat_type) => chat_type.to_owned(),
        None => "private".to_owned(),
    }
}

pub fn permission_error_settings_update(
    settings: &ChatSettings,
    chat_type: &str,
    is_media_permission_error: bool,
) -> ChatSettingsUpdate {
    let enable_daily_game = settings.enable_daily_game.unwrap_or(true);
    let daily_game_theme = settings
        .daily_game_theme
        .as_deref()
        .filter(|theme| !theme.is_empty())
        .unwrap_or("auto")
        .to_owned();
    let mut update = ChatSettingsUpdate {
        chat_id: settings.chat_id,
        chat_type: chat_type.to_owned(),
        mood_alignment: settings.mood_alignment.clone(),
        custom_persona: settings.custom_persona.clone(),
        reactivity_percentage: settings.reactivity_percentage,
        proactivity_percentage: settings.proactivity_percentage,
        enable_global_text_reply: settings.enable_global_text_reply,
        enable_global_draw_reply: settings.enable_global_draw_reply,
        enable_obscenifier: settings.enable_obscenifier,
        enable_profanity: settings.enable_profanity,
        enable_greet_joiners: settings.enable_greet_joiners,
        enable_daily_game,
        daily_game_theme,
        greeting_html: None,
    };

    if is_media_permission_error {
        update.enable_global_draw_reply = false;
    } else {
        update.enable_global_text_reply = false;
    }
    update
}

impl ReadinessResponse {
    /// Build a ready response from startup checks.
    pub fn ready(checks: Vec<ReadinessCheck>) -> Self {
        Self {
            status: "ok",
            service: openplotva_core::PROJECT_NAME,
            checks,
        }
    }
}

impl ReadinessCheck {
    /// Build an OK readiness check.
    pub fn ok(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "ok",
            detail: detail.into(),
        }
    }

    /// Build a skipped readiness check.
    pub fn skipped(name: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "skipped",
            detail: detail.into(),
        }
    }
}

async fn health() -> Json<HealthResponse> {
    Json(health_response())
}

async fn ready(State(state): State<Arc<AppState>>) -> (StatusCode, Json<ReadinessResponse>) {
    (StatusCode::OK, Json(state.readiness.clone()))
}

#[cfg(test)]
mod tests {
    use super::{
        ACTION_EDIT_MESSAGE, ACTION_SEND_STICKER, ACTION_SEND_TEXT, ACTION_SEND_VIDEO,
        HealthResponse, ReadinessCheck, ReadinessResponse, RuntimeAifarmCapacitySnapshotData,
        RuntimeApiGraphqlSnapshot, RuntimeApiLiveDiagnostics, RuntimeCacheInspector,
        RuntimeCacheSnapshotData, RuntimeCacheStatsData, RuntimeChatConnectionData,
        RuntimeChatData, RuntimeChatMemberData, RuntimeChatMemberWithUserData, RuntimeChatsFilter,
        RuntimeDispatchFailureData, RuntimeDispatcherFailureInspector, RuntimeDispatcherInspector,
        RuntimeDispatcherStatsData, RuntimeEntityReader, RuntimeEntityReaderFuture,
        RuntimeGeminiCachePurgeResultData, RuntimeGeminiCachePurger,
        RuntimeGeminiCachePurgerFuture, RuntimeJobAnalyticsStatData, RuntimeLlmAnalyticsData,
        RuntimeLlmAnalyticsModelStatData, RuntimeLlmAnalyticsProviderStatData,
        RuntimeLlmAnalyticsReader, RuntimeLlmAnalyticsReaderFuture,
        RuntimeLlmAnalyticsSeriesPointData, RuntimeLlmAnalyticsStageMetricData,
        RuntimeLlmAnalyticsTopChatData, RuntimeLlmAnalyticsTotalsData, RuntimeLlmGenConfigData,
        RuntimeLlmRequestChatData, RuntimeLlmRequestData, RuntimeLlmRequestMessageData,
        RuntimeLlmRequestResultData, RuntimeLlmRequestUserData, RuntimeLlmRequestsFilter,
        RuntimeLlmTraceInspector, RuntimeLogEntry, RuntimeLogInspector, RuntimeLogQuery,
        RuntimeMemoryRestartFuture, RuntimeMemoryRestartResultData, RuntimeMemoryRestarter,
        RuntimeRedisInspector, RuntimeRedisInspectorFuture, RuntimeRedisPrefixGroup,
        RuntimeRedisValue, RuntimeRoutingEventData, RuntimeRoutingEventInspector,
        RuntimeRoutingEventsFilter, RuntimeSafetyCheckConnectionData, RuntimeSafetyCheckData,
        RuntimeSafetyCheckReader, RuntimeSafetyCheckReaderFuture, RuntimeSafetyChecksFilter,
        RuntimeSqlReadRequest, RuntimeSqlReadResult, RuntimeSqlReader, RuntimeSqlReaderFuture,
        RuntimeSubscriptionData, RuntimeTaskmanDiagnosticsData, RuntimeTaskmanInspector,
        RuntimeTaskmanJobData, RuntimeTaskmanJobDetailsData, RuntimeTaskmanJobFuture,
        RuntimeTaskmanJobListEntryData, RuntimeTaskmanJobListResultData,
        RuntimeTaskmanJobMessageData, RuntimeTaskmanJobSummaryData, RuntimeTaskmanJobsFilter,
        RuntimeTaskmanQueueDiagnosticsData, RuntimeTelegramDeliveryFuture,
        RuntimeTelegramDeliveryInspector, RuntimeTelegramDeliveryListFilter,
        RuntimeTelegramOutboxAttemptData, RuntimeTelegramOutboxItemData,
        RuntimeTelegramOutboxMutationResultData, RuntimeTelegramOutboxRetryRequest,
        RuntimeTelegramOutboxStatsData, RuntimeTelegramUpdateAttemptData,
        RuntimeTelegramUpdateInboxItemData, RuntimeTelegramUpdateInboxStatsData,
        RuntimeTokenParseError, RuntimeUpdatesInspector, RuntimeUpdatesInspectorFuture,
        RuntimeUpdatesRuntimeData, RuntimeUpdatesTaskData, RuntimeUserConnectionData,
        RuntimeUserData, RuntimeUserDetailsData, RuntimeUserLookup, RuntimeUsersFilter,
        RuntimeVipCacheData, RuntimeVipEventData, RuntimeVipSummaryData, RuntimeVirtualDialogData,
        RuntimeVirtualDialogDeleteResultData, RuntimeVirtualDialogFuture,
        RuntimeVirtualDialogManager, RuntimeVirtualDialogMessageData,
        RuntimeVirtualDialogSendRequest, RuntimeVirtualDialogStartRequest,
        RuntimeVirtualDialogToolMode, can_perform_action, format_runtime_token,
        hash_runtime_token_secret, health_response, parse_bearer_token, parse_runtime_token,
        permission_cache_key, permission_error_chat_type, permission_error_settings_update,
        rate_limit_key, rate_limited_chat_key, runtime_api_graphql_schema,
        runtime_api_graphql_schema_with_diagnostics,
        runtime_api_graphql_schema_with_live_diagnostics, runtime_api_graphql_schema_with_redis,
        runtime_api_method_not_allowed_response, runtime_api_pprof_response,
        runtime_api_tls_config_from_pem, runtime_api_unauthorized_response,
        runtime_token_expired_at, runtime_token_secret_hash_matches, vip_cache_key,
    };
    use axum::{body::to_bytes, http::header};
    use std::sync::Arc;
    use time::{Duration as TimeDuration, OffsetDateTime};

    #[test]
    fn health_payload_names_the_service() {
        assert_eq!(
            health_response(),
            HealthResponse {
                status: "ok",
                service: "openplotva"
            }
        );
    }

    #[test]
    fn readiness_payload_names_startup_checks() {
        let response = ReadinessResponse::ready(vec![
            ReadinessCheck::ok("runtime", "ready"),
            ReadinessCheck::skipped("postgres", "disabled"),
        ]);

        assert_eq!(response.status, "ok");
        assert_eq!(response.service, "openplotva");
        assert_eq!(response.checks.len(), 2);
    }

    #[test]
    fn rate_limit_keys_match_go_policy_cache_keys() {
        assert_eq!(rate_limit_key(42), "rate_limit:42");
        assert_eq!(rate_limited_chat_key(42), "plotva:rate_limited_chat:42");
    }

    #[test]
    fn permission_and_vip_keys_match_go_policy_cache_keys() {
        assert_eq!(
            permission_cache_key(42, ACTION_SEND_TEXT),
            "perm_check:42:send_text"
        );
        assert_eq!(vip_cache_key(42), "vip:42");
    }

    #[test]
    fn parse_bearer_token_matches_go_runtime_api_auth_rules() {
        assert_eq!(
            parse_bearer_token("Bearer prt_test.valid-token"),
            Some("prt_test.valid-token")
        );
        assert_eq!(
            parse_bearer_token(" bearer    prt_test.valid-token "),
            Some("prt_test.valid-token")
        );
        assert_eq!(parse_bearer_token("Basic token"), None);
        assert_eq!(parse_bearer_token("Bearer"), None);
        assert_eq!(parse_bearer_token("Bearer "), None);
        assert_eq!(parse_bearer_token("Bearer token with-space"), None);
    }

    #[test]
    fn runtime_api_tls_config_rejects_empty_material() {
        let error = runtime_api_tls_config_from_pem(b"", b"")
            .expect_err("empty TLS material should be rejected");
        assert!(matches!(
            error,
            super::RuntimeApiTlsError::EmptyCertificateChain
        ));
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_ported_readonly_diagnostics() {
        let schema = runtime_api_graphql_schema(RuntimeApiGraphqlSnapshot {
            log_level: "debug".to_owned(),
            web_port: 8080,
            runtime_api_enabled: true,
            runtime_api_port: 9091,
            db_status: "ok".to_owned(),
            redis_status: "ok".to_owned(),
            sql_timeout_ms: 1000,
            sql_row_limit: 100,
            sql_result_bytes_limit: 4096,
            ..RuntimeApiGraphqlSnapshot::default()
        });
        let response = schema
            .execute(
                r#"
                query {
                    runtimeState { logLevel dispatcher { regularQueueSize } cache { size } }
                    healthSnapshot { db { status } redis { status } updatesQueueLength }
                    configSnapshot { runtimeApiEnabled runtimeApiPort sqlTimeoutMs sqlRowLimit sqlResultBytesLimit }
                    taskmanJobs { total summary { byStatus byQueue } items { id } }
                    updatesRuntime { queueLen active }
                    logs(limit: 10) { count items { level message attrs } }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["runtimeState"]["logLevel"], "debug");
        assert_eq!(payload["data"]["healthSnapshot"]["db"]["status"], "ok");
        assert_eq!(payload["data"]["configSnapshot"]["runtimeApiPort"], 9091);
        assert_eq!(payload["data"]["taskmanJobs"]["total"], 0);
        assert_eq!(payload["data"]["logs"]["count"], 0);
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_live_dispatcher_stats_like_go_shapes() {
        #[derive(Default)]
        struct MemoryDispatcherInspector;

        impl RuntimeDispatcherInspector for MemoryDispatcherInspector {
            fn stats(&self) -> RuntimeDispatcherStatsData {
                RuntimeDispatcherStatsData {
                    regular_queue_size: 2,
                    immediate_queue_size: 1,
                    processed_total: 42,
                    deduped_total: 3,
                    oldest_regular_age_ms: 1200,
                    oldest_immediate_age_ms: 300,
                }
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                dispatcher_inspector: Some(Arc::new(MemoryDispatcherInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    runtimeState {
                        dispatcher {
                            regularQueueSize
                            immediateQueueSize
                            processedTotal
                            dedupedTotal
                            oldestRegularAgeMs
                            oldestImmediateAgeMs
                        }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime dispatcher GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let dispatcher = &payload["data"]["runtimeState"]["dispatcher"];
        assert_eq!(dispatcher["regularQueueSize"], 2);
        assert_eq!(dispatcher["immediateQueueSize"], 1);
        assert_eq!(dispatcher["processedTotal"], "42");
        assert_eq!(dispatcher["dedupedTotal"], "3");
        assert_eq!(dispatcher["oldestRegularAgeMs"], 1200);
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_live_cache_stats_like_go_shapes() {
        #[derive(Default)]
        struct MemoryCacheInspector;

        impl RuntimeCacheInspector for MemoryCacheInspector {
            fn stats(&self) -> RuntimeCacheSnapshotData {
                RuntimeCacheSnapshotData {
                    cache: RuntimeCacheStatsData {
                        size: 4,
                        capacity: 10_000,
                        hits: 8,
                        misses: 2,
                        mem_size: 96,
                    },
                    planner_cache: RuntimeCacheStatsData {
                        size: 1,
                        capacity: 10_000,
                        hits: 3,
                        misses: 5,
                        mem_size: 24,
                    },
                }
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                cache_inspector: Some(Arc::new(MemoryCacheInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    runtimeState {
                        cache { size capacity hits misses memSize }
                        plannerCache { size capacity hits misses memSize }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime cache GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let cache = &payload["data"]["runtimeState"]["cache"];
        assert_eq!(cache["size"], 4);
        assert_eq!(cache["capacity"], 10_000);
        assert_eq!(cache["hits"], "8");
        assert_eq!(cache["misses"], "2");
        assert_eq!(cache["memSize"], "96");
        let planner = &payload["data"]["runtimeState"]["plannerCache"];
        assert_eq!(planner["size"], 1);
        assert_eq!(planner["hits"], "3");
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_redis_diagnostics_like_go_shapes() {
        #[derive(Default)]
        struct MemoryRedisInspector;

        impl RuntimeRedisInspector for MemoryRedisInspector {
            fn prefix_groups<'a>(
                &'a self,
                prefix: &'a str,
                limit: usize,
            ) -> RuntimeRedisInspectorFuture<'a, Vec<RuntimeRedisPrefixGroup>> {
                Box::pin(async move {
                    assert_eq!(prefix, "plotva:");
                    assert_eq!(limit, 1000);
                    Ok(vec![RuntimeRedisPrefixGroup {
                        prefix: "plotva:updates:".to_owned(),
                        count: 2,
                    }])
                })
            }

            fn keys<'a>(
                &'a self,
                pattern: &'a str,
                limit: usize,
            ) -> RuntimeRedisInspectorFuture<'a, Vec<String>> {
                Box::pin(async move {
                    assert_eq!(pattern, "plotva:*");
                    assert_eq!(limit, 1000);
                    Ok(vec!["plotva:updates:queue".to_owned()])
                })
            }

            fn value<'a>(
                &'a self,
                key: &'a str,
                max_bytes: usize,
            ) -> RuntimeRedisInspectorFuture<'a, Option<RuntimeRedisValue>> {
                Box::pin(async move {
                    assert_eq!(key, "plotva:updates:queue");
                    assert_eq!(max_bytes, 64 * 1024);
                    Ok(Some(RuntimeRedisValue {
                        key: key.to_owned(),
                        value: "[]".to_owned(),
                        truncated: false,
                    }))
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_redis(
            RuntimeApiGraphqlSnapshot::default(),
            Some(Arc::new(MemoryRedisInspector)),
        );
        let response = schema
            .execute(
                r#"
                query {
                    redisPrefixes(prefix: " plotva: ") { prefix count }
                    redisKeys(pattern: "plotva:*", limit: 5000)
                    redisValue(key: "plotva:updates:queue") { key value truncated }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime Redis GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(
            payload["data"]["redisPrefixes"][0]["prefix"],
            "plotva:updates:"
        );
        assert_eq!(payload["data"]["redisPrefixes"][0]["count"], 2);
        assert_eq!(payload["data"]["redisKeys"][0], "plotva:updates:queue");
        assert_eq!(payload["data"]["redisValue"]["value"], "[]");
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_sql_read_through_reader_boundary() {
        #[derive(Default)]
        struct MemorySqlReader;

        impl RuntimeSqlReader for MemorySqlReader {
            fn read<'a>(&'a self, request: RuntimeSqlReadRequest) -> RuntimeSqlReaderFuture<'a> {
                Box::pin(async move {
                    assert_eq!(request.sql, "select $1::int as value");
                    assert_eq!(request.args, vec![serde_json::json!(7)]);
                    assert_eq!(request.timeout_ms, 250);
                    assert_eq!(request.row_limit, 20);
                    assert_eq!(request.result_bytes_limit, 2048);
                    Ok(RuntimeSqlReadResult {
                        columns: vec!["value".to_owned()],
                        rows: vec![serde_json::json!({ "value": 7 })],
                        row_count: 1,
                        elapsed_ms: 3,
                        truncated: false,
                    })
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_diagnostics(
            RuntimeApiGraphqlSnapshot {
                sql_timeout_ms: 1000,
                sql_row_limit: 20,
                sql_result_bytes_limit: 2048,
                ..RuntimeApiGraphqlSnapshot::default()
            },
            None,
            Some(Arc::new(MemorySqlReader)),
        );
        let response = schema
            .execute(
                r#"
                query {
                    sqlRead(input: { sql: "select $1::int as value", args: [7], timeoutMs: 250 }) {
                        columns
                        rows
                        rowCount
                        elapsedMs
                        truncated
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime SQL GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["sqlRead"]["columns"][0], "value");
        assert_eq!(payload["data"]["sqlRead"]["rows"][0]["value"], 7);
        assert_eq!(payload["data"]["sqlRead"]["rowCount"], 1);
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_live_log_replay_like_go_shapes() {
        #[derive(Default)]
        struct MemoryLogInspector;

        impl RuntimeLogInspector for MemoryLogInspector {
            fn logs(&self, query: RuntimeLogQuery) -> Vec<RuntimeLogEntry> {
                assert_eq!(query.after_seq, 7);
                assert_eq!(query.limit, 200);
                assert_eq!(query.level, "info");
                assert_eq!(query.search, "runtime");
                vec![RuntimeLogEntry {
                    seq: 8,
                    time: Some("2026-04-18T10:00:00Z".to_owned()),
                    level: "info".to_owned(),
                    message: "runtime ready".to_owned(),
                    attrs: Some(serde_json::json!({ "component": "runtime" })),
                }]
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                log_inspector: Some(Arc::new(MemoryLogInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    logs(afterSeq: "7", limit: 500, level: " info ", search: " runtime ") {
                        count
                        lastSeq
                        items { seq time level message attrs }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime log GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["logs"]["count"], 1);
        assert_eq!(payload["data"]["logs"]["lastSeq"], "8");
        assert_eq!(
            payload["data"]["logs"]["items"][0]["message"],
            "runtime ready"
        );
        assert_eq!(
            payload["data"]["logs"]["items"][0]["attrs"]["component"],
            "runtime"
        );
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_updates_runtime_snapshot_like_go_shape() {
        #[derive(Default)]
        struct MemoryUpdatesInspector;

        impl RuntimeUpdatesInspector for MemoryUpdatesInspector {
            fn snapshot<'a>(&'a self) -> RuntimeUpdatesInspectorFuture<'a> {
                Box::pin(async move {
                    Ok(RuntimeUpdatesRuntimeData {
                        active: 2,
                        state_active: 1,
                        handle_active: 1,
                        queue_len: 5,
                        queue_error: Some("redis unavailable".to_owned()),
                        started1m: 3,
                        completed1m: 4,
                        timeouts1m: 1,
                        oldest_active_ms: 1200,
                        last_stall_at: Some("2026-05-21T10:00:00Z".to_owned()),
                        gates: None,
                        tasks: vec![RuntimeUpdatesTaskData {
                            stage: "handle".to_owned(),
                            started_at: "2026-05-21T10:00:01Z".to_owned(),
                            age_ms: 250,
                            chat_id: Some(9),
                            user_id: Some(7),
                            update: "message".to_owned(),
                        }],
                        ..RuntimeUpdatesRuntimeData::default()
                    })
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                updates_inspector: Some(Arc::new(MemoryUpdatesInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    healthSnapshot { updatesQueueLength }
                    updatesRuntime {
                        active
                        stateActive
                        handleActive
                        queueLen
                        queueError
                        started1m
                        completed1m
                        timeouts1m
                        oldestActiveMs
                        lastStallAt
                        tasks { stage startedAt ageMs chatID userID update }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime updates GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["healthSnapshot"]["updatesQueueLength"], 5);
        assert_eq!(payload["data"]["updatesRuntime"]["queueLen"], 5);
        assert_eq!(
            payload["data"]["updatesRuntime"]["queueError"],
            "redis unavailable"
        );
        assert_eq!(payload["data"]["updatesRuntime"]["tasks"][0]["chatID"], "9");
    }

    #[tokio::test]
    async fn runtime_api_graphql_delegates_telegram_delivery_queries_and_mutations() {
        use std::sync::Mutex;

        #[derive(Default)]
        struct DeliveryInspector {
            actions: Mutex<Vec<(String, bool)>>,
        }

        fn inbox_item() -> RuntimeTelegramUpdateInboxItemData {
            RuntimeTelegramUpdateInboxItemData {
                id: 41,
                bot_id: 7,
                update_id: 901,
                schema_version: 1,
                source: "webhook".to_owned(),
                stream_ms: 1_700_000_000_000,
                stream_seq: 4,
                last_stream_ms: 1_700_000_000_001,
                last_stream_seq: 1,
                payload_size_bytes: 321,
                payload_sha256: "abcd".to_owned(),
                payload_conflict: true,
                update_type: Some("message".to_owned()),
                first_received_at: "2026-07-11T10:00:00Z".to_owned(),
                last_received_at: "2026-07-11T10:00:01Z".to_owned(),
                materialized_at: "2026-07-11T10:00:02Z".to_owned(),
                delivery_count: 2,
                ordering_key: "dialog:7:9:0".to_owned(),
                chat_id: Some(9),
                user_id: Some(8),
                status: "retry_wait".to_owned(),
                available_at: "2026-07-11T10:00:03Z".to_owned(),
                attempt_count: 1,
                last_error_class: Some("temporary".to_owned()),
                last_error: Some(format!(
                    "request https://api.telegram.org/bot123456:THIS_IS_A_SECRET_SENTINEL/sendMessage failed {}",
                    "x".repeat(3_000)
                )),
                created_at: "2026-07-11T10:00:02Z".to_owned(),
                updated_at: "2026-07-11T10:00:03Z".to_owned(),
                ..RuntimeTelegramUpdateInboxItemData::default()
            }
        }

        fn outbox_item() -> RuntimeTelegramOutboxItemData {
            RuntimeTelegramOutboxItemData {
                id: 51,
                operation_id: "op-1".to_owned(),
                batch_id: "batch-1".to_owned(),
                part_index: 0,
                bot_id: 7,
                chat_id: Some(9),
                ordering_key: "dialog:7:9:0".to_owned(),
                causation_update_id: Some(901),
                dialog_job_id: Some(77),
                trigger_message_id: Some(88),
                method_kind: "sendMessage".to_owned(),
                delivery_policy: "create".to_owned(),
                protected: true,
                state: "ambiguous".to_owned(),
                available_at: "2026-07-11T10:00:03Z".to_owned(),
                attempt_count: 1,
                last_error_class: Some("request_ambiguous".to_owned()),
                telegram_message_ids: vec![101, 102],
                has_receipt: false,
                created_at: "2026-07-11T10:00:02Z".to_owned(),
                updated_at: "2026-07-11T10:00:03Z".to_owned(),
                ..RuntimeTelegramOutboxItemData::default()
            }
        }

        impl RuntimeTelegramDeliveryInspector for DeliveryInspector {
            fn update_inbox_stats<'a>(
                &'a self,
            ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramUpdateInboxStatsData>
            {
                Box::pin(async {
                    Ok(RuntimeTelegramUpdateInboxStatsData {
                        pending: 2,
                        retry_wait: 1,
                        payload_conflicts: 1,
                        quarantined: 3,
                        total_deliveries: 12,
                        oldest_pending_at: Some("2026-07-11T09:59:00Z".to_owned()),
                        ..RuntimeTelegramUpdateInboxStatsData::default()
                    })
                })
            }

            fn update_inbox_item<'a>(
                &'a self,
                id: i64,
            ) -> RuntimeTelegramDeliveryFuture<'a, Option<RuntimeTelegramUpdateInboxItemData>>
            {
                assert_eq!(id, 41);
                Box::pin(async { Ok(Some(inbox_item())) })
            }

            fn update_inbox_items<'a>(
                &'a self,
                filter: RuntimeTelegramDeliveryListFilter,
            ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramUpdateInboxItemData>>
            {
                assert_eq!(filter.before_id, Some(99));
                assert_eq!(filter.state.as_deref(), Some("pending"));
                assert_eq!(filter.limit, 500);
                Box::pin(async { Ok(vec![inbox_item()]) })
            }

            fn update_inbox_attempts<'a>(
                &'a self,
                inbox_id: i64,
                limit: i32,
            ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramUpdateAttemptData>>
            {
                assert_eq!((inbox_id, limit), (41, 2));
                Box::pin(async {
                    Ok(vec![RuntimeTelegramUpdateAttemptData {
                        attempt: 1,
                        lease_token: 5,
                        worker_id: "updates-1".to_owned(),
                        claimed_at: "2026-07-11T10:00:02Z".to_owned(),
                        outcome: Some("retry".to_owned()),
                        ..RuntimeTelegramUpdateAttemptData::default()
                    }])
                })
            }

            fn outbox_stats<'a>(
                &'a self,
            ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxStatsData> {
                Box::pin(async {
                    Ok(RuntimeTelegramOutboxStatsData {
                        pending: 4,
                        ambiguous: 1,
                        protected_unresolved: 1,
                        ..RuntimeTelegramOutboxStatsData::default()
                    })
                })
            }

            fn outbox_item<'a>(
                &'a self,
                operation_id: &'a str,
            ) -> RuntimeTelegramDeliveryFuture<'a, Option<RuntimeTelegramOutboxItemData>>
            {
                assert_eq!(operation_id, "op-1");
                Box::pin(async { Ok(Some(outbox_item())) })
            }

            fn outbox_items<'a>(
                &'a self,
                filter: RuntimeTelegramDeliveryListFilter,
            ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramOutboxItemData>> {
                assert_eq!(filter.before_id, None);
                assert_eq!(filter.state.as_deref(), Some("ambiguous"));
                assert_eq!(filter.limit, 100);
                Box::pin(async { Ok(vec![outbox_item()]) })
            }

            fn outbox_attempts<'a>(
                &'a self,
                outbox_id: i64,
                limit: i32,
            ) -> RuntimeTelegramDeliveryFuture<'a, Vec<RuntimeTelegramOutboxAttemptData>>
            {
                assert_eq!((outbox_id, limit), (51, 3));
                Box::pin(async {
                    Ok(vec![RuntimeTelegramOutboxAttemptData {
                        attempt: 1,
                        lease_token: 6,
                        worker_id: "outbox-1".to_owned(),
                        claimed_at: "2026-07-11T10:00:02Z".to_owned(),
                        http_status: Some(503),
                        latency_ms: Some(123),
                        outcome: Some("ambiguous".to_owned()),
                        ..RuntimeTelegramOutboxAttemptData::default()
                    }])
                })
            }

            fn retry_outbox<'a>(
                &'a self,
                request: RuntimeTelegramOutboxRetryRequest,
            ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxMutationResultData>
            {
                self.actions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((request.operation_id.clone(), request.accept_duplicate_risk));
                Box::pin(async move {
                    Ok(RuntimeTelegramOutboxMutationResultData {
                        operation_id: request.operation_id,
                        changed: true,
                        state: Some("pending".to_owned()),
                    })
                })
            }

            fn cancel_outbox<'a>(
                &'a self,
                operation_id: String,
            ) -> RuntimeTelegramDeliveryFuture<'a, RuntimeTelegramOutboxMutationResultData>
            {
                self.actions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((operation_id.clone(), false));
                Box::pin(async move {
                    Ok(RuntimeTelegramOutboxMutationResultData {
                        operation_id,
                        changed: true,
                        state: Some("cancelled".to_owned()),
                    })
                })
            }
        }

        let inspector = Arc::new(DeliveryInspector::default());
        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                telegram_delivery_inspector: Some(inspector.clone()),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let query = schema
            .execute(
                r#"
                query {
                    telegramUpdateInboxStats {
                        pending retryWait payloadConflicts quarantined totalDeliveries oldestPendingAt
                    }
                    telegramUpdateInbox(id: "41") {
                        id botID updateID source streamMs streamSeq payloadSizeBytes payloadSha256
                        payloadConflict deliveryCount orderingKey chatID userID status lastError
                    }
                    telegramUpdateInboxItems(beforeID: "99", state: " pending ", limit: 900) {
                        id status
                    }
                    telegramUpdateInboxAttempts(inboxID: "41", limit: 2) {
                        attempt leaseToken workerID outcome
                    }
                    telegramOutboxStats { pending ambiguous protectedUnresolved }
                    telegramOutbox(operationID: " op-1 ") {
                        id operationID batchID botID causationUpdateID dialogJobID triggerMessageID
                        methodKind deliveryPolicy protected state telegramMessageIDs hasReceipt
                    }
                    telegramOutboxItems(state: " ambiguous ") { id operationID state }
                    telegramOutboxAttempts(outboxID: "51", limit: 3) {
                        attempt leaseToken workerID outcome httpStatus latencyMs
                    }
                }
                "#,
            )
            .await;
        assert!(
            query.errors.is_empty(),
            "Telegram delivery GraphQL query returned errors: {:?}",
            query.errors
        );
        let query_payload = serde_json::to_value(&query)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(
            query_payload["data"]["telegramUpdateInboxStats"]["pending"],
            2
        );
        assert_eq!(
            query_payload["data"]["telegramUpdateInbox"]["updateID"],
            "901"
        );
        assert_eq!(
            query_payload["data"]["telegramOutbox"]["operationID"],
            "op-1"
        );
        assert_eq!(
            query_payload["data"]["telegramOutboxAttempts"][0]["httpStatus"],
            503
        );
        let serialized_query = query_payload.to_string();
        assert!(!serialized_query.contains("THIS_IS_A_SECRET_SENTINEL"));
        assert!(
            query_payload["data"]["telegramUpdateInbox"]["lastError"]
                .as_str()
                .is_some_and(|error| error.chars().count() <= 2_048)
        );

        let mutation = schema
            .execute(
                r#"
                mutation {
                    retried: retryTelegramOutbox(operationID: " op-1 ", acceptDuplicateRisk: true) {
                        operationID changed state
                    }
                    cancelled: cancelTelegramOutbox(operationID: "op-2") {
                        operationID changed state
                    }
                }
                "#,
            )
            .await;
        assert!(
            mutation.errors.is_empty(),
            "Telegram delivery GraphQL mutation returned errors: {:?}",
            mutation.errors
        );
        let mutation_payload = serde_json::to_value(&mutation)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(mutation_payload["data"]["retried"]["state"], "pending");
        assert_eq!(mutation_payload["data"]["cancelled"]["state"], "cancelled");
        assert_eq!(
            *inspector
                .actions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec![("op-1".to_owned(), true), ("op-2".to_owned(), false)]
        );
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_taskman_diagnostics_like_go_shapes() {
        #[derive(Default)]
        struct MemoryTaskmanInspector;

        impl RuntimeTaskmanInspector for MemoryTaskmanInspector {
            fn list_jobs(
                &self,
                filter: RuntimeTaskmanJobsFilter,
            ) -> Result<RuntimeTaskmanJobListResultData, String> {
                assert_eq!(filter.q, "pay");
                assert_eq!(filter.status, vec!["pending"]);
                assert_eq!(filter.queue, vec!["control"]);
                assert_eq!(filter.user_id, Some(7));
                assert_eq!(filter.chat_id, Some(9));
                assert_eq!(filter.limit, 500);
                Ok(RuntimeTaskmanJobListResultData {
                    total: 1,
                    offset: 0,
                    limit: 500,
                    summary: RuntimeTaskmanJobSummaryData {
                        by_status: serde_json::json!({ "pending": 1 }),
                        by_queue: serde_json::json!({ "control": 1 }),
                    },
                    items: vec![RuntimeTaskmanJobListEntryData {
                        id: 42,
                        queue_name: "control".to_owned(),
                        priority: 2,
                        title: "vip invoice".to_owned(),
                        job_type: "control".to_owned(),
                        status: "pending".to_owned(),
                        user_id: 7,
                        chat_id: 9,
                        trigger_message_id: 11,
                        thread_message_id: Some(12),
                        progress_message_id: None,
                        result_message_id: None,
                        worker_id: Some("payment-control".to_owned()),
                        created_at: "2026-05-21T10:00:00Z".to_owned(),
                        started_at: None,
                        completed_at: None,
                        error_message: None,
                        processing_timeout_seconds: 0,
                        prompt_hash: None,
                        estimated_processing_time: None,
                        actual_processing_time: None,
                        preview: Some("vip_invoice".to_owned()),
                    }],
                })
            }

            fn job<'a>(&'a self, id: i64) -> RuntimeTaskmanJobFuture<'a> {
                assert_eq!(id, 42);
                Box::pin(async {
                    Ok(Some(RuntimeTaskmanJobDetailsData {
                        job: RuntimeTaskmanJobData {
                            id: 42,
                            queue_name: "control".to_owned(),
                            priority: 2,
                            title: "vip invoice".to_owned(),
                            payload: Some(serde_json::json!({ "type": "control" })),
                            status: "pending".to_owned(),
                            user_id: 7,
                            chat_id: 9,
                            trigger_message_id: 11,
                            thread_message_id: Some(12),
                            progress_message_id: None,
                            result_message_id: None,
                            worker_id: Some("payment-control".to_owned()),
                            created_at: "2026-05-21T10:00:00Z".to_owned(),
                            started_at: None,
                            completed_at: None,
                            error_message: None,
                            processing_timeout_seconds: 0,
                            prompt_hash: None,
                            estimated_processing_time: None,
                            actual_processing_time: None,
                        },
                        messages: vec![RuntimeTaskmanJobMessageData {
                            id: 77,
                            job_id: 42,
                            message_type: "result".to_owned(),
                            chat_id: 9,
                            message_id: 88,
                            created_at: "2026-05-21T10:00:01Z".to_owned(),
                            status: "completed".to_owned(),
                        }],
                        events: Some(serde_json::json!({ "source": "memory" })),
                    }))
                })
            }

            fn queue_diagnostics(
                &self,
                queues: Vec<String>,
                priority: i32,
            ) -> Result<RuntimeTaskmanDiagnosticsData, String> {
                assert_eq!(queues, vec!["control"]);
                assert_eq!(priority, 0);
                Ok(RuntimeTaskmanDiagnosticsData {
                    running: true,
                    active: 1,
                    started1m: 2,
                    completed1m: 3,
                    worker_count: 1,
                    queue_signal_count: 1,
                    slow_job_count: 0,
                    queues: vec![RuntimeTaskmanQueueDiagnosticsData {
                        queue_name: "control".to_owned(),
                        priority: 0,
                        pending: 4,
                        pending_or_higher: 5,
                        active: 1,
                        worker_count: 1,
                        eta_seconds: 0,
                    }],
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                taskman_inspector: Some(Arc::new(MemoryTaskmanInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    taskmanJobs(filter: { q: " pay ", status: ["pending"], queue: ["control"], userID: "7", chatID: "9", limit: 500 }) {
                        total
                        limit
                        summary { byStatus byQueue }
                        items {
                            id
                            queueName
                            priority
                            title
                            jobType
                            status
                            userID
                            chatID
                            triggerMessageID
                            threadMessageID
                            workerID
                            createdAt
                            preview
                        }
                    }
                    taskmanJob(id: "42") {
                        job { id payload status userID chatID triggerMessageID }
                        messages { id jobID messageType chatID messageID createdAt status }
                        events
                    }
                    taskmanQueueDiagnostics(queues: ["control"], priority: 0) {
                        running
                        active
                        started1m
                        completed1m
                        workerCount
                        queues { queueName priority pending pendingOrHigher active workerCount etaSeconds }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime taskman GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["taskmanJobs"]["total"], 1);
        assert_eq!(
            payload["data"]["taskmanJobs"]["summary"]["byStatus"]["pending"],
            1
        );
        assert_eq!(payload["data"]["taskmanJobs"]["items"][0]["id"], "42");
        assert_eq!(
            payload["data"]["taskmanJob"]["job"]["payload"]["type"],
            "control"
        );
        assert_eq!(
            payload["data"]["taskmanJob"]["messages"][0]["messageType"],
            "result"
        );
        assert_eq!(payload["data"]["taskmanJob"]["messages"][0]["jobID"], "42");
        assert_eq!(
            payload["data"]["taskmanQueueDiagnostics"]["queues"][0]["pendingOrHigher"],
            5
        );
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_core_entities_like_go_shapes() {
        #[derive(Default)]
        struct EntityReader;

        impl RuntimeEntityReader for EntityReader {
            fn users<'a>(
                &'a self,
                filter: RuntimeUsersFilter,
            ) -> RuntimeEntityReaderFuture<'a, RuntimeUserConnectionData> {
                assert_eq!(
                    filter,
                    RuntimeUsersFilter {
                        q: "ali".to_owned(),
                        offset: 0,
                        limit: 500,
                    }
                );
                Box::pin(async {
                    Ok(RuntimeUserConnectionData {
                        count: 1,
                        offset: 0,
                        limit: 500,
                        items: vec![runtime_user(7, "alice")],
                    })
                })
            }

            fn user<'a>(
                &'a self,
                lookup: RuntimeUserLookup,
            ) -> RuntimeEntityReaderFuture<'a, Option<RuntimeUserDetailsData>> {
                assert_eq!(lookup, RuntimeUserLookup::Username("alice".to_owned()));
                Box::pin(async {
                    Ok(Some(RuntimeUserDetailsData {
                        user: runtime_user(7, "alice"),
                        subscription: Some(RuntimeSubscriptionData {
                            id: 9,
                            user_id: 7,
                            telegram_payment_charge_id: "tg-charge".to_owned(),
                            provider_payment_charge_id: String::new(),
                            expires_at: Some("2026-05-22T00:00:00Z".to_owned()),
                            created_at: Some("2026-05-21T00:00:00Z".to_owned()),
                            updated_at: Some("2026-05-21T00:00:00Z".to_owned()),
                            canceled_at: None,
                            refunded_at: None,
                            status: "active".to_owned(),
                        }),
                        vip: Some(RuntimeVipCacheData {
                            user_id: 7,
                            is_vip: true,
                            expires_at: Some("2026-05-22T00:00:00Z".to_owned()),
                            created_at: Some("2026-05-21T00:00:00Z".to_owned()),
                            updated_at: Some("2026-05-21T00:00:00Z".to_owned()),
                        }),
                        vip_summary: Some(RuntimeVipSummaryData {
                            active: true,
                            has_history: true,
                            expires_at: Some("2026-05-22T00:00:00Z".to_owned()),
                            remaining_seconds: "86400".to_owned(),
                            remaining_days: 1,
                            latest_event_id: Some(99),
                            latest_event_type: Some("payment".to_owned()),
                            latest_reason: Some("test".to_owned()),
                            latest_created_at: Some("2026-05-21T00:00:00Z".to_owned()),
                        }),
                        vip_events: vec![RuntimeVipEventData {
                            id: 99,
                            event_type: "payment".to_owned(),
                            delta_seconds: "86400".to_owned(),
                            delta_days: 1.0,
                            effective_expires_at: Some("2026-05-22T00:00:00Z".to_owned()),
                            actor_user_id: Some(1),
                            actor_label: Some("@admin".to_owned()),
                            reason: Some("test".to_owned()),
                            created_at: Some("2026-05-21T00:00:00Z".to_owned()),
                            subscription_id: Some(9),
                            telegram_payment_charge_id: Some("tg-charge".to_owned()),
                            provider_payment_charge_id: Some(String::new()),
                            subscription_status: Some("active".to_owned()),
                        }],
                        subscriptions: Vec::new(),
                    }))
                })
            }

            fn chats<'a>(
                &'a self,
                filter: RuntimeChatsFilter,
            ) -> RuntimeEntityReaderFuture<'a, RuntimeChatConnectionData> {
                assert_eq!(
                    filter,
                    RuntimeChatsFilter {
                        q: String::new(),
                        offset: 0,
                        limit: 100,
                        member_username: None,
                        member_user_id: Some(7),
                    }
                );
                Box::pin(async {
                    Ok(RuntimeChatConnectionData {
                        count: 1,
                        offset: 0,
                        limit: 100,
                        items: vec![runtime_chat()],
                    })
                })
            }

            fn chat<'a>(
                &'a self,
                id: i64,
            ) -> RuntimeEntityReaderFuture<'a, Option<RuntimeChatData>> {
                assert_eq!(id, -100);
                Box::pin(async { Ok(Some(runtime_chat())) })
            }

            fn chat_members<'a>(
                &'a self,
                chat_id: i64,
            ) -> RuntimeEntityReaderFuture<'a, Vec<RuntimeChatMemberWithUserData>> {
                assert_eq!(chat_id, -100);
                Box::pin(async {
                    Ok(vec![RuntimeChatMemberWithUserData {
                        member: RuntimeChatMemberData {
                            chat_id: -100,
                            user_id: 7,
                            status: "administrator".to_owned(),
                            can_delete_messages: Some(true),
                            updated_at: Some("2026-05-21T00:00:00Z".to_owned()),
                            ..RuntimeChatMemberData::default()
                        },
                        user: Some(runtime_user(7, "alice")),
                    }])
                })
            }
        }

        fn runtime_user(id: i64, username: &str) -> RuntimeUserData {
            RuntimeUserData {
                id,
                is_premium: Some(true),
                first_name: "Alice".to_owned(),
                username: Some(username.to_owned()),
                is_vip: Some(true),
                discovered_at: Some("2026-05-21T00:00:00Z".to_owned()),
                updated_at: Some("2026-05-21T00:00:00Z".to_owned()),
                ..RuntimeUserData::default()
            }
        }

        fn runtime_chat() -> RuntimeChatData {
            RuntimeChatData {
                id: -100,
                chat_type: "supergroup".to_owned(),
                title: Some("Plotva Lab".to_owned()),
                username: Some("plotva_lab".to_owned()),
                is_forum: Some(true),
                discovered_at: Some("2026-05-21T00:00:00Z".to_owned()),
                updated_at: Some("2026-05-21T00:00:00Z".to_owned()),
                ..RuntimeChatData::default()
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                entity_reader: Some(Arc::new(EntityReader)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    users(filter: { q: " ali ", offset: -5, limit: 999 }) {
                        count offset limit
                        items { id firstName username isVip discoveredAt }
                    }
                    user(username: " alice ") {
                        id firstName username isVip
                        subscription { id userID status telegramPaymentChargeID }
                        vip { userID isVip expiresAt }
                        vipSummary { active remainingSeconds latestEventID }
                        vipEvents { id eventType actorUserID subscriptionID subscriptionStatus }
                    }
                    chats(filter: { memberUserID: "7" }) {
                        count offset limit
                        items { id type title username isForum }
                    }
                    chat(id: "-100") { id type title }
                    chatMembers(chatID: "-100") {
                        member { chatID userID status canDeleteMessages updatedAt }
                        user { id username }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime entity GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["users"]["items"][0]["id"], "7");
        assert_eq!(payload["data"]["user"]["subscription"]["status"], "active");
        assert_eq!(payload["data"]["user"]["vipSummary"]["latestEventID"], "99");
        assert_eq!(payload["data"]["chats"]["items"][0]["id"], "-100");
        assert_eq!(payload["data"]["chat"]["type"], "supergroup");
        assert_eq!(
            payload["data"]["chatMembers"][0]["member"]["canDeleteMessages"],
            true
        );
        assert_eq!(
            payload["data"]["chatMembers"][0]["user"]["username"],
            "alice"
        );
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_safety_checks_like_go_shape() {
        #[derive(Default)]
        struct SafetyReader;

        impl RuntimeSafetyCheckReader for SafetyReader {
            fn safety_checks<'a>(
                &'a self,
                filter: RuntimeSafetyChecksFilter,
            ) -> RuntimeSafetyCheckReaderFuture<'a> {
                assert_eq!(
                    filter,
                    RuntimeSafetyChecksFilter {
                        q: "risk".to_owned(),
                        flagged: Some(true),
                        offset: 0,
                        limit: 1000,
                    }
                );
                Box::pin(async {
                    Ok(RuntimeSafetyCheckConnectionData {
                        count: 1,
                        offset: 0,
                        limit: 1000,
                        items: vec![RuntimeSafetyCheckData {
                            id: 21,
                            created_at: "2026-05-21T00:00:00Z".to_owned(),
                            source: "whitecircle".to_owned(),
                            flow: Some("dialog".to_owned()),
                            mode: Some("pre".to_owned()),
                            chat_id: Some(-100),
                            thread_id: Some(5),
                            message_id: Some(77),
                            user_id: Some(7),
                            deployment_id: "dep-1".to_owned(),
                            external_session_id: Some("ext".to_owned()),
                            request_messages: Some(serde_json::json!([{"role": "user"}])),
                            flagged: Some(true),
                            internal_session_id: Some("int".to_owned()),
                            policies: Some(serde_json::json!({"policy": "safe"})),
                            response_json: Some(serde_json::json!({"flagged": true})),
                            duration_ms: 42,
                            error: Some("risk".to_owned()),
                        }],
                    })
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                safety_check_reader: Some(Arc::new(SafetyReader)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    safetyChecks(filter: { q: " risk ", flagged: true, offset: -3, limit: 2000 }) {
                        count
                        offset
                        limit
                        items {
                            id
                            createdAt
                            source
                            flow
                            mode
                            chatID
                            threadID
                            messageID
                            userID
                            deploymentID
                            externalSessionID
                            requestMessages
                            flagged
                            internalSessionID
                            policies
                            responseJSON
                            durationMs
                            error
                        }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime safety GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let checks = &payload["data"]["safetyChecks"];
        assert_eq!(checks["count"], 1);
        assert_eq!(checks["limit"], 1000);
        assert_eq!(checks["items"][0]["id"], "21");
        assert_eq!(checks["items"][0]["chatID"], "-100");
        assert_eq!(checks["items"][0]["requestMessages"][0]["role"], "user");
        assert_eq!(checks["items"][0]["responseJSON"]["flagged"], true);
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_llm_requests_like_go_shape() {
        #[derive(Default)]
        struct LlmTraceInspector;

        impl RuntimeLlmTraceInspector for LlmTraceInspector {
            fn llm_requests(
                &self,
                filter: RuntimeLlmRequestsFilter,
            ) -> Result<Vec<RuntimeLlmRequestData>, String> {
                assert_eq!(
                    filter,
                    RuntimeLlmRequestsFilter {
                        q: "answer".to_owned(),
                        source: "dialog".to_owned(),
                        model: "model-a".to_owned(),
                        chat_id: Some(-100),
                        user_id: Some(7),
                        limit: 1000,
                        ..RuntimeLlmRequestsFilter::default()
                    }
                );
                Ok(vec![RuntimeLlmRequestData {
                    run_id: None,
                    run_seq: None,
                    id: 31,
                    at: "2026-05-21T00:00:00Z".to_owned(),
                    provider: Some("aifarm".to_owned()),
                    request_kind: Some("dialog".to_owned()),
                    source: "dialog".to_owned(),
                    mode: Some("chat".to_owned()),
                    flow: Some("dialog".to_owned()),
                    iteration: 1,
                    model: Some("model-a".to_owned()),
                    chat: RuntimeLlmRequestChatData {
                        chat_id: -100,
                        thread_id: Some(5),
                        chat_title: Some("Plotva Lab".to_owned()),
                    },
                    user: RuntimeLlmRequestUserData {
                        user_id: 7,
                        full_name: Some("Alice".to_owned()),
                    },
                    message: RuntimeLlmRequestMessageData { message_id: 77 },
                    gen_config: RuntimeLlmGenConfigData {
                        max_output_tokens: 512,
                        temperature: 0.7,
                        top_p: 0.9,
                        top_k: 40,
                        safety_settings: Some(serde_json::json!([{"category": "safe"}])),
                    },
                    docs: Some(serde_json::json!(["doc"])),
                    messages: Some(serde_json::json!([{"role": "user"}])),
                    raw_request: Some(serde_json::json!({"prompt": "hi"})),
                    resolved_cache_content: Some(serde_json::json!({"cache": "hit"})),
                    raw_response: Some(serde_json::json!({"content": "answer"})),
                    transport: Some(serde_json::json!({"job_id": "job"})),
                    inference_params: Some(serde_json::json!({"temperature": 0.7})),
                    usage: Some(serde_json::json!({"total_tokens": 9})),
                    timings: Some(serde_json::json!({"generation_ms": 10})),
                    prompt_chars: 12,
                    prompt_messages: 1,
                    docs_chars: 3,
                    duration_ms: 42,
                    result: RuntimeLlmRequestResultData {
                        duration_ms: 42,
                        error: None,
                        response_text_preview: Some("answer".to_owned()),
                    },
                }])
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                llm_trace_inspector: Some(Arc::new(LlmTraceInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    llmRequests(filter: { q: " answer ", source: " dialog ", model: " model-a ", chatID: "-100", userID: "7", limit: 5000 }) {
                        id at provider requestKind source mode flow iteration model
                        chat { chatID threadID chatTitle }
                        user { userID fullName }
                        message { messageID }
                        genConfig { maxOutputTokens temperature topP topK safetySettings }
                        docs messages rawRequest resolvedCacheContent rawResponse transport inferenceParams usage timings
                        promptChars promptMessages docsChars durationMs
                        result { durationMs error responseTextPreview }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime LLM GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let request = &payload["data"]["llmRequests"][0];
        assert_eq!(request["id"], "31");
        assert_eq!(request["chat"]["chatID"], "-100");
        assert_eq!(request["genConfig"]["maxOutputTokens"], 512);
        assert_eq!(request["rawResponse"]["content"], "answer");
        assert_eq!(request["result"]["responseTextPreview"], "answer");
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_dispatcher_send_failures() {
        struct DispatcherFailureInspector;

        impl RuntimeDispatcherFailureInspector for DispatcherFailureInspector {
            fn send_failures(&self, limit: i32) -> Vec<RuntimeDispatchFailureData> {
                assert_eq!(limit, 10);
                vec![RuntimeDispatchFailureData {
                    at: "2026-07-02T12:00:00Z".to_owned(),
                    virtual_id: "dialog-vmsg-1".to_owned(),
                    chat_id: -100,
                    method_kind: "SendMessage".to_owned(),
                    error: "Forbidden: bot was blocked by the user".to_owned(),
                    class: "terminal_permission".to_owned(),
                    protected: true,
                    reply_to_message_id: Some(77),
                }]
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                dispatcher_failure_inspector: Some(Arc::new(DispatcherFailureInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    dispatcherSendFailures(limit: 10) {
                        at virtualID chatID methodKind error class protected replyToMessageID
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime dispatcher failure GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let failure = &payload["data"]["dispatcherSendFailures"][0];
        assert_eq!(failure["virtualID"], "dialog-vmsg-1");
        assert_eq!(failure["chatID"], "-100");
        assert_eq!(failure["methodKind"], "SendMessage");
        assert_eq!(failure["class"], "terminal_permission");
        assert_eq!(failure["protected"], true);
        assert_eq!(failure["replyToMessageID"], 77);
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_llm_routing_events() {
        struct RoutingEventInspector;

        impl RuntimeRoutingEventInspector for RoutingEventInspector {
            fn routing_events(
                &self,
                filter: RuntimeRoutingEventsFilter,
            ) -> Result<Vec<RuntimeRoutingEventData>, String> {
                assert_eq!(filter.workflow_key, "dialog");
                assert_eq!(filter.event_type, "route_unavailable");
                assert_eq!(filter.limit, 10);
                Ok(vec![RuntimeRoutingEventData {
                    id: 9,
                    at: "2026-06-27T12:00:00Z".to_owned(),
                    severity: "error".to_owned(),
                    event_type: "route_unavailable".to_owned(),
                    workflow_key: "dialog".to_owned(),
                    provider_id: Some(10),
                    model_id: Some(20),
                    queue_name: Some("text".to_owned()),
                    job_id: Some(30),
                    chat_id: Some(-100),
                    thread_id: Some(5),
                    message_id: Some(77),
                    dedupe_key: "route:dialog".to_owned(),
                    summary: "dialog route unavailable".to_owned(),
                    detail: serde_json::json!({"admin_report": {"action": "sent"}}),
                }])
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                routing_event_inspector: Some(Arc::new(RoutingEventInspector)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    llmRoutingEvents(filter: { workflowKey: "dialog", eventType: "route_unavailable", limit: 10 }) {
                        id at severity eventType workflowKey providerID modelID queueName jobID chatID threadID messageID dedupeKey summary detail
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime routing events GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let event = &payload["data"]["llmRoutingEvents"][0];
        assert_eq!(event["id"], "9");
        assert_eq!(event["eventType"], "route_unavailable");
        assert_eq!(event["workflowKey"], "dialog");
        assert_eq!(event["providerID"], "10");
        assert_eq!(event["modelID"], "20");
        assert_eq!(event["detail"]["admin_report"]["action"], "sent");
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_llm_analytics_like_go_shape() {
        struct LlmAnalyticsReader;

        impl RuntimeLlmAnalyticsReader for LlmAnalyticsReader {
            fn llm_analytics<'a>(&'a self, range: &'a str) -> RuntimeLlmAnalyticsReaderFuture<'a> {
                assert_eq!(range, " 3d ");
                Box::pin(async {
                    Ok(RuntimeLlmAnalyticsData {
                        range: "72h0m0s".to_owned(),
                        bucket: "hour".to_owned(),
                        since: "2026-05-18T12:00:00Z".to_owned(),
                        totals: RuntimeLlmAnalyticsTotalsData {
                            total_count: 7,
                            error_count: 1,
                            avg_duration_ms: 120,
                        },
                        series: vec![RuntimeLlmAnalyticsSeriesPointData {
                            ts: "2026-05-21T12:00:00Z".to_owned(),
                            total_count: 2,
                            error_count: 1,
                            avg_duration_ms: 150,
                        }],
                        model_series: Vec::new(),
                        top_chats: vec![RuntimeLlmAnalyticsTopChatData {
                            chat_id: -100,
                            title: Some("Plotva Lab".to_owned()),
                            username: Some("plotva_lab".to_owned()),
                            request_count: 4,
                        }],
                        models: vec![RuntimeLlmAnalyticsModelStatData {
                            model: "model-a".to_owned(),
                            request_count: 5,
                            error_count: 1,
                            avg_duration_ms: 100,
                            p50_duration_ms: 90,
                            p95_duration_ms: 220,
                            ..RuntimeLlmAnalyticsModelStatData::default()
                        }],
                        providers: vec![RuntimeLlmAnalyticsProviderStatData {
                            provider: "AI Farm".to_owned(),
                            source: "aifarm, dialog".to_owned(),
                            request_count: 6,
                            error_count: 1,
                            avg_duration_ms: 110,
                            p50_duration_ms: 95,
                            p95_duration_ms: 210,
                            ..RuntimeLlmAnalyticsProviderStatData::default()
                        }],
                        inference_params: Vec::new(),
                        stage_metrics: vec![RuntimeLlmAnalyticsStageMetricData {
                            stage: "shield".to_owned(),
                            source: "chat_flow_shield".to_owned(),
                            request_count: 3,
                            error_count: 0,
                            avg_duration_ms: 80,
                            p50_duration_ms: 70,
                            p95_duration_ms: 120,
                            avg_iteration: 2,
                            max_iteration: 3,
                        }],
                        runtime_jobs: vec![RuntimeJobAnalyticsStatData {
                            job_type: "image_gen".to_owned(),
                            queue_name: "image-vip".to_owned(),
                            provider: "aifarm".to_owned(),
                            created_count: 2,
                            completed_count: 1,
                            failed_count: 1,
                            avg_wait_ms: 10,
                            p95_wait_ms: 20,
                            avg_processing_ms: 30,
                            p95_processing_ms: 40,
                        }],
                        runtime_jobs_error: Some("taskman unavailable".to_owned()),
                        ai_farm_capacity: Some(RuntimeAifarmCapacitySnapshotData {
                            service: "llm-openai".to_owned(),
                            max_concurrent_jobs: 8,
                            running: 2,
                            queued: 1,
                            available: 5,
                            locked: false,
                            ready: true,
                            observed_at: "2026-05-21T12:00:00Z".to_owned(),
                            error: None,
                        }),
                    })
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                llm_analytics_reader: Some(Arc::new(LlmAnalyticsReader)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                query {
                    llmAnalytics(range: " 3d ") {
                        range bucket since
                        totals { totalCount errorCount avgDurationMs }
                        series { ts totalCount errorCount avgDurationMs }
                        topChats { chatID title username requestCount }
                        models { model requestCount errorCount avgDurationMs p50DurationMs p95DurationMs }
                        providers { provider source requestCount errorCount avgDurationMs p50DurationMs p95DurationMs }
                        stageMetrics { stage source requestCount errorCount avgDurationMs p50DurationMs p95DurationMs avgIteration maxIteration }
                        runtimeJobs { jobType queueName provider createdCount completedCount failedCount avgWaitMs p95WaitMs avgProcessingMs p95ProcessingMs }
                        runtimeJobsError
                        aiFarmCapacity { service maxConcurrentJobs running queued available locked ready observedAt error }
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime LLM analytics GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let analytics = &payload["data"]["llmAnalytics"];
        assert_eq!(analytics["range"], "72h0m0s");
        assert_eq!(analytics["totals"]["totalCount"], 7);
        assert_eq!(analytics["topChats"][0]["chatID"], "-100");
        assert_eq!(analytics["models"][0]["p95DurationMs"], 220);
        assert_eq!(analytics["providers"][0]["provider"], "AI Farm");
        assert_eq!(analytics["stageMetrics"][0]["stage"], "shield");
        assert_eq!(analytics["runtimeJobs"][0]["provider"], "aifarm");
        assert_eq!(analytics["runtimeJobsError"], "taskman unavailable");
        assert_eq!(analytics["aiFarmCapacity"]["available"], 5);
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_session_virtual_dialogs() {
        #[derive(Default)]
        struct VirtualDialogs;

        impl RuntimeVirtualDialogManager for VirtualDialogs {
            fn virtual_dialog<'a>(
                &'a self,
                session_id: &'a str,
            ) -> RuntimeVirtualDialogFuture<'a, Option<RuntimeVirtualDialogData>> {
                assert_eq!(session_id, "agent-session");
                Box::pin(async {
                    Ok(Some(RuntimeVirtualDialogData {
                        session_id: "agent-session".to_owned(),
                        chat_id: -9001,
                        user_id: -9002,
                        next_message_id: 4,
                        message_count: 3,
                        last_activity_at: Some("2026-06-30T10:00:00Z".to_owned()),
                        expires_at: Some("2026-07-01T10:00:00Z".to_owned()),
                        messages: vec![RuntimeVirtualDialogMessageData {
                            message_id: 3,
                            role: "model".to_owned(),
                            text: "answer".to_owned(),
                            at: "2026-06-30T10:00:01Z".to_owned(),
                            provider: Some("aifarm".to_owned()),
                            tool_mode: Some(RuntimeVirtualDialogToolMode::Safe),
                            tool_calls: Some(serde_json::json!([{"name": "web_search"}])),
                        }],
                    }))
                })
            }

            fn start_virtual_dialog<'a>(
                &'a self,
                request: RuntimeVirtualDialogStartRequest,
            ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogData> {
                assert_eq!(request.session_id, "agent-session");
                assert!(request.replace_existing);
                Box::pin(async {
                    Ok(RuntimeVirtualDialogData {
                        session_id: "agent-session".to_owned(),
                        chat_id: -9001,
                        user_id: -9002,
                        next_message_id: 1,
                        message_count: 0,
                        last_activity_at: Some("2026-06-30T10:00:00Z".to_owned()),
                        expires_at: Some("2026-07-01T10:00:00Z".to_owned()),
                        messages: Vec::new(),
                    })
                })
            }

            fn send_virtual_dialog_message<'a>(
                &'a self,
                request: RuntimeVirtualDialogSendRequest,
            ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogMessageData> {
                assert_eq!(request.session_id, "agent-session");
                assert_eq!(request.text, "привет");
                assert_eq!(request.tool_mode, RuntimeVirtualDialogToolMode::Real);
                Box::pin(async {
                    Ok(RuntimeVirtualDialogMessageData {
                        message_id: 2,
                        role: "model".to_owned(),
                        text: "answer".to_owned(),
                        at: "2026-06-30T10:00:01Z".to_owned(),
                        provider: Some("aifarm".to_owned()),
                        tool_mode: Some(RuntimeVirtualDialogToolMode::Real),
                        tool_calls: Some(serde_json::json!([{"name": "draw_image"}])),
                    })
                })
            }

            fn delete_virtual_dialog<'a>(
                &'a self,
                session_id: &'a str,
            ) -> RuntimeVirtualDialogFuture<'a, RuntimeVirtualDialogDeleteResultData> {
                assert_eq!(session_id, "agent-session");
                Box::pin(async {
                    Ok(RuntimeVirtualDialogDeleteResultData {
                        found: true,
                        deleted: true,
                        history_deleted: 3,
                        taskman_deleted: 1,
                        llm_traces_deleted: 2,
                    })
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                virtual_dialog_manager: Some(Arc::new(VirtualDialogs)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                mutation {
                    startVirtualDialog(input: { sessionID: "agent-session", replaceExisting: true }) {
                        sessionID
                        chatID
                        userID
                        nextMessageID
                        messageCount
                        expiresAt
                    }
                    sendVirtualDialogMessage(input: { sessionID: "agent-session", text: "привет", toolMode: REAL }) {
                        messageID
                        role
                        text
                        provider
                        toolMode
                        toolCalls
                    }
                    deleteVirtualDialog(sessionID: "agent-session") {
                        found
                        deleted
                        historyDeleted
                        taskmanDeleted
                        llmTracesDeleted
                    }
                }
                "#,
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "runtime virtual dialog mutation GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(
            payload["data"]["startVirtualDialog"]["sessionID"],
            "agent-session"
        );
        assert_eq!(payload["data"]["startVirtualDialog"]["chatID"], "-9001");
        assert_eq!(
            payload["data"]["sendVirtualDialogMessage"]["toolMode"],
            "REAL"
        );
        assert_eq!(
            payload["data"]["sendVirtualDialogMessage"]["toolCalls"][0]["name"],
            "draw_image"
        );
        assert_eq!(payload["data"]["deleteVirtualDialog"]["deleted"], true);

        let response = schema
            .execute(
                r#"
                query {
                    virtualDialog(sessionID: "agent-session") {
                        sessionID
                        chatID
                        userID
                        messageCount
                        messages { messageID role text provider toolMode toolCalls }
                    }
                }
                "#,
            )
            .await;
        assert!(
            response.errors.is_empty(),
            "runtime virtual dialog query GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        assert_eq!(payload["data"]["virtualDialog"]["messageCount"], 3);
        assert_eq!(
            payload["data"]["virtualDialog"]["messages"][0]["toolMode"],
            "SAFE"
        );
    }

    #[tokio::test]
    async fn runtime_api_graphql_serves_mutations_through_live_boundaries() {
        struct MemoryRestarter;

        impl RuntimeMemoryRestarter for MemoryRestarter {
            fn restart<'a>(&'a self, run_id: Option<i64>) -> RuntimeMemoryRestartFuture<'a> {
                assert_eq!(run_id, Some(42));
                Box::pin(async {
                    Ok(RuntimeMemoryRestartResultData {
                        ok: true,
                        run_id: Some(42),
                        retried_failed_runs: 1,
                        queued_runs: 0,
                        started: true,
                        override_: false,
                        provider: Some("aifarm".to_owned()),
                        model: Some("memory-model".to_owned()),
                    })
                })
            }
        }

        struct GeminiPurger;

        impl RuntimeGeminiCachePurger for GeminiPurger {
            fn purge<'a>(&'a self) -> RuntimeGeminiCachePurgerFuture<'a> {
                Box::pin(async {
                    Ok(RuntimeGeminiCachePurgeResultData {
                        scanned: 5,
                        matched: 3,
                        deleted: 2,
                        failed: 1,
                    })
                })
            }
        }

        let schema = runtime_api_graphql_schema_with_live_diagnostics(
            RuntimeApiGraphqlSnapshot::default(),
            RuntimeApiLiveDiagnostics {
                memory_restarter: Some(Arc::new(MemoryRestarter)),
                gemini_cache_purger: Some(Arc::new(GeminiPurger)),
                ..RuntimeApiLiveDiagnostics::default()
            },
        );
        let response = schema
            .execute(
                r#"
                mutation {
                    restartMemory(runID: "42") {
                        ok
                        runID
                        retriedFailedRuns
                        queuedRuns
                        started
                        override
                        provider
                        model
                    }
                    purgeGeminiExplicitCaches {
                        ok
                        scanned
                        matched
                        deleted
                        failed
                    }
                }
                "#,
            )
            .await;

        assert!(
            response.errors.is_empty(),
            "runtime mutation GraphQL returned errors: {:?}",
            response.errors
        );
        let payload = serde_json::to_value(&response)
            .unwrap_or_else(|error| panic!("serialize GraphQL response: {error}"));
        let restart = &payload["data"]["restartMemory"];
        assert_eq!(restart["ok"], true);
        assert_eq!(restart["runID"], "42");
        assert_eq!(restart["retriedFailedRuns"], 1);
        assert_eq!(restart["queuedRuns"], 0);
        assert_eq!(restart["started"], true);
        assert_eq!(restart["override"], false);
        assert_eq!(restart["provider"], "aifarm");
        assert_eq!(restart["model"], "memory-model");

        let purge = &payload["data"]["purgeGeminiExplicitCaches"];
        assert_eq!(purge["ok"], false);
        assert_eq!(purge["scanned"], 5);
        assert_eq!(purge["matched"], 3);
        assert_eq!(purge["deleted"], 2);
        assert_eq!(purge["failed"], 1);
    }

    #[tokio::test]
    async fn runtime_api_error_responses_match_go_auth_shell() {
        let unauthorized = runtime_api_unauthorized_response();
        assert_eq!(unauthorized.status(), axum::http::StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthorized.headers().get(header::WWW_AUTHENTICATE),
            Some(&axum::http::HeaderValue::from_static(
                super::RUNTIME_API_AUTHENTICATE_HEADER
            ))
        );
        let body = to_bytes(unauthorized.into_body(), 1024)
            .await
            .unwrap_or_else(|error| panic!("read unauthorized body: {error}"));
        assert_eq!(&body[..], b"unauthorized\n");

        let method = runtime_api_method_not_allowed_response();
        assert_eq!(method.status(), axum::http::StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(method.into_body(), 1024)
            .await
            .unwrap_or_else(|error| panic!("read method body: {error}"));
        assert_eq!(&body[..], b"{\"error\":\"method not allowed\"}\n");
    }

    #[tokio::test]
    async fn runtime_api_pprof_routes_return_rust_diagnostic_analogues() {
        let index = runtime_api_pprof_response("");
        assert_eq!(index.status(), axum::http::StatusCode::OK);
        let body = to_bytes(index.into_body(), 4096)
            .await
            .unwrap_or_else(|error| panic!("read pprof index body: {error}"));
        let body = String::from_utf8(body.to_vec())
            .unwrap_or_else(|error| panic!("pprof index utf8: {error}"));
        for profile in super::RUNTIME_API_PPROF_SUBTREE_PROFILES {
            assert!(body.contains(&format!("href=\"{profile}\"")));
        }

        let cmdline = runtime_api_pprof_response("cmdline");
        assert_eq!(cmdline.status(), axum::http::StatusCode::OK);
        let body = to_bytes(cmdline.into_body(), 16 * 1024)
            .await
            .unwrap_or_else(|error| panic!("read cmdline body: {error}"));
        assert!(body.ends_with(b"\0"));

        let profile = runtime_api_pprof_response("profile");
        assert_eq!(profile.status(), axum::http::StatusCode::OK);
        let content_type = profile
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        assert!(content_type.starts_with("text/plain"));
        let body = to_bytes(profile.into_body(), 1024 * 1024)
            .await
            .unwrap_or_else(|error| panic!("read profile body: {error}"));
        let body = String::from_utf8(body.to_vec())
            .unwrap_or_else(|error| panic!("profile fallback utf8: {error}"));
        assert!(body.contains("profile: profile"));
        assert!(body.contains("intentionally unavailable"));

        let heap = runtime_api_pprof_response("heap");
        assert_eq!(heap.status(), axum::http::StatusCode::OK);
        let body = to_bytes(heap.into_body(), 4096)
            .await
            .unwrap_or_else(|error| panic!("read heap body: {error}"));
        let body =
            String::from_utf8(body.to_vec()).unwrap_or_else(|error| panic!("heap utf8: {error}"));
        assert!(body.contains("profile: heap"));
        assert!(body.contains("pid:"));
        assert!(body.contains("target:"));

        let threadcreate = runtime_api_pprof_response("threadcreate");
        assert_eq!(threadcreate.status(), axum::http::StatusCode::OK);
        let body = to_bytes(threadcreate.into_body(), 4096)
            .await
            .unwrap_or_else(|error| panic!("read threadcreate body: {error}"));
        let body = String::from_utf8(body.to_vec())
            .unwrap_or_else(|error| panic!("threadcreate utf8: {error}"));
        assert!(body.contains("profile: threadcreate"));

        let symbol = runtime_api_pprof_response("symbol");
        let body = to_bytes(symbol.into_body(), 1024)
            .await
            .unwrap_or_else(|error| panic!("read symbol body: {error}"));
        assert_eq!(&body[..], b"num_symbols: 0\n");

        assert_eq!(
            runtime_api_pprof_response("unknown").status(),
            axum::http::StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn parse_runtime_token_matches_go_token_format() {
        let parsed = parse_runtime_token(" prt_abcd1234.deadbeef.more ").unwrap_or_else(|error| {
            panic!("parse_runtime_token failed: {error}");
        });

        assert_eq!(parsed.id, "abcd1234");
        assert_eq!(parsed.secret, "deadbeef.more");
        assert_eq!(
            format_runtime_token(&parsed.id, &parsed.secret),
            "prt_abcd1234.deadbeef.more"
        );
        assert_eq!(
            parse_runtime_token("prt_.secret").err(),
            Some(RuntimeTokenParseError::InvalidPrefix)
        );
        assert_eq!(
            parse_runtime_token("wrong_abcd.secret").err(),
            Some(RuntimeTokenParseError::InvalidPrefix)
        );
        assert_eq!(
            parse_runtime_token("prt_abcd.").err(),
            Some(RuntimeTokenParseError::InvalidFormat)
        );
    }

    #[test]
    fn runtime_token_hash_and_expiry_match_go_boundaries() {
        let hash = hash_runtime_token_secret("deadbeef");
        assert_eq!(
            hex::encode(hash),
            "2baf1f40105d9501fe319a8ec463fdf4325a2a5df445adf3f572f626253678c9"
        );
        let expected_hash = hash_runtime_token_secret("deadbeef");
        assert!(runtime_token_secret_hash_matches(
            &expected_hash,
            "deadbeef"
        ));
        assert!(!runtime_token_secret_hash_matches(
            &expected_hash,
            "cafebabe"
        ));
        assert!(!runtime_token_secret_hash_matches(
            &expected_hash[..16],
            "deadbeef"
        ));

        let now = OffsetDateTime::UNIX_EPOCH + TimeDuration::days(10);
        let created = now - super::RUNTIME_API_TOKEN_TTL;
        assert!(!runtime_token_expired_at(Some(created), now));
        assert!(runtime_token_expired_at(
            Some(created - TimeDuration::seconds(1)),
            now
        ));
        assert!(runtime_token_expired_at(None, now));
    }

    #[test]
    fn can_perform_action_matches_go_chat_settings_policy() {
        let disabled = openplotva_core::ChatSettings {
            chat_id: 42,
            enable_global_text_reply: false,
            enable_global_draw_reply: false,
            ..openplotva_core::ChatSettings::defaults(42)
        };
        let draw_only = openplotva_core::ChatSettings {
            chat_id: 42,
            enable_global_text_reply: false,
            enable_global_draw_reply: true,
            ..openplotva_core::ChatSettings::defaults(42)
        };

        assert!(can_perform_action(
            "private",
            Some(&disabled),
            ACTION_SEND_TEXT
        ));
        assert!(can_perform_action("supergroup", None, ACTION_SEND_TEXT));
        assert!(!can_perform_action(
            "supergroup",
            Some(&disabled),
            ACTION_SEND_TEXT
        ));
        assert!(!can_perform_action(
            "supergroup",
            Some(&draw_only),
            ACTION_EDIT_MESSAGE
        ));
        assert!(can_perform_action(
            "supergroup",
            Some(&draw_only),
            ACTION_SEND_STICKER
        ));
        assert!(!can_perform_action(
            "supergroup",
            Some(&draw_only),
            ACTION_SEND_VIDEO
        ));
    }

    #[test]
    fn permission_error_settings_update_disables_expected_reply_mode_like_go() {
        let mut settings = openplotva_core::ChatSettings::defaults(42);
        settings.enable_daily_game = None;
        settings.daily_game_theme = None;

        let text_update = permission_error_settings_update(&settings, "supergroup", false);
        let media_update = permission_error_settings_update(&settings, "supergroup", true);

        assert!(!text_update.enable_global_text_reply);
        assert!(text_update.enable_global_draw_reply);
        assert!(media_update.enable_global_text_reply);
        assert!(!media_update.enable_global_draw_reply);
        assert!(text_update.enable_daily_game);
        assert_eq!(text_update.daily_game_theme, "auto");
        assert_eq!(text_update.chat_type, "supergroup");
    }

    #[test]
    fn permission_error_chat_type_uses_known_chat_info_like_go() {
        assert_eq!(permission_error_chat_type(None), "private");
        assert_eq!(permission_error_chat_type(Some("  ")), "private");
        assert_eq!(
            permission_error_chat_type(Some(" supergroup ")),
            "supergroup"
        );
    }
}
