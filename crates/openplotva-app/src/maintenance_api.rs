use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use openplotva_config::{MaintenanceConfig, RuntimeApiConfig};
use openplotva_storage::{
    maintenance::MaintenanceStore,
    maintenance_notifications::{MaintenanceNotificationStore, NOTIFICATION_PREFIX},
};
use openplotva_telegram::{DispatcherQueue, DispatcherSendStatus};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio::{sync::watch, task::JoinHandle};

const PREFIX: &str = "/internal/maintenance/v1";

#[derive(Clone)]
struct ApiState {
    token_hash: [u8; 32],
    recipient_id: i64,
    incidents: MaintenanceStore,
    notifications: MaintenanceNotificationStore,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IncidentQuery {
    #[serde(default)]
    after: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum NotificationStatus {
    PrCreated,
    PrReady,
    NeedsHuman,
    Paused,
    Failed,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NotificationRequest {
    key: String,
    run_id: String,
    status: NotificationStatus,
    issue_number: Option<i64>,
    pr_number: Option<i64>,
}

impl NotificationRequest {
    fn text(&self) -> Result<String, StatusCode> {
        if !valid_key(&self.key)
            || self.run_id.is_empty()
            || self.run_id.len() > 100
            || !self
                .run_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || self.issue_number.is_some_and(|n| n <= 0)
            || self.pr_number.is_some_and(|n| n <= 0)
            || (matches!(
                self.status,
                NotificationStatus::PrCreated | NotificationStatus::PrReady
            ) && (self.pr_number.is_none() || self.issue_number.is_none()))
        {
            return Err(StatusCode::BAD_REQUEST);
        }
        let mut text = match self.status {
            NotificationStatus::PrCreated => "Автоисправление: создан PR, идут проверки.",
            NotificationStatus::PrReady => "Автоисправление: PR прошёл проверки и ревью, можно сливать вручную.",
            NotificationStatus::NeedsHuman => "Авторазбор завершён: требуется ваше решение. Issue оставлен открытым.",
            NotificationStatus::Paused => "Авторазбор приостановлен: исчерпан лимит или недоступны ресурсы. Прогресс сохранён.",
            NotificationStatus::Failed => "Авторазбор не завершён: требуется ваше вмешательство. Прогресс сохранён.",
        }.to_owned();
        if let Some(number) = self.issue_number {
            text.push_str(&format!(
                "\nIssue: https://github.com/iamwavecut/openplotva/issues/{number}"
            ));
        }
        if let Some(number) = self.pr_number {
            text.push_str(&format!(
                "\nPR: https://github.com/iamwavecut/openplotva/pull/{number}"
            ));
        }
        Ok(text)
    }
}

fn valid_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit())
}

fn authorized(hash: &[u8; 32], headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(openplotva_server::parse_bearer_token)
        .is_some_and(|token| openplotva_server::runtime_token_secret_hash_matches(hash, token))
}

async fn authenticate(
    State(state): State<Arc<ApiState>>,
    request: Request,
    next: Next,
) -> Response {
    if !authorized(&state.token_hash, request.headers()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

fn router(state: Arc<ApiState>) -> Router {
    Router::new()
        .nest(
            PREFIX,
            Router::new()
                .route(
                    "/health",
                    get(|| async { Json(json!({"status": "ok", "version": 1})) }),
                )
                .route("/incidents", get(incidents))
                .route("/incidents/{id}/evidence", get(evidence))
                .route("/notifications", post(notify))
                .route("/notifications/{key}", get(notification)),
        )
        .layer(DefaultBodyLimit::max(4096))
        .layer(from_fn_with_state(Arc::clone(&state), authenticate))
        .with_state(state)
}

async fn incidents(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<IncidentQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if query.after < 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let entries = state
        .incidents
        .incidents(query.after, 100)
        .await
        .map_err(storage_error)?;
    let next = entries.last().map_or(query.after, |entry| entry.id);
    Ok(Json(json!({"incidents": entries, "next_cursor": next})))
}

async fn evidence(
    State(state): State<Arc<ApiState>>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if id <= 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .incidents
        .evidence(id)
        .await
        .map_err(storage_error)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn notify(
    State(state): State<Arc<ApiState>>,
    Json(request): Json<NotificationRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let text = request.text()?;
    let saved = state
        .notifications
        .create(&request.key, state.recipient_id, &text)
        .await
        .map_err(storage_error)?
        .ok_or(StatusCode::CONFLICT)?;
    Ok(Json(json!(saved)))
}

async fn notification(
    State(state): State<Arc<ApiState>>,
    Path(key): Path<String>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if !valid_key(&key) {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .notifications
        .get(&key)
        .await
        .map_err(storage_error)?
        .map(|saved| Json(json!(saved)))
        .ok_or(StatusCode::NOT_FOUND)
}

fn storage_error(error: openplotva_storage::StorageError) -> StatusCode {
    tracing::warn!(%error, "maintenance storage operation failed");
    StatusCode::SERVICE_UNAVAILABLE
}

pub async fn start(
    config: &MaintenanceConfig,
    tls_config: &RuntimeApiConfig,
    pool: PgPool,
    queue: Arc<DispatcherQueue>,
    server_stop: watch::Receiver<bool>,
    capture_stop: watch::Receiver<bool>,
) -> anyhow::Result<(JoinHandle<()>, JoinHandle<()>)> {
    let state = Arc::new(ApiState {
        token_hash: Sha256::digest(config.token.as_bytes()).into(),
        recipient_id: config.notify_user_id,
        incidents: MaintenanceStore::new(pool.clone()),
        notifications: MaintenanceNotificationStore::new(pool),
    });
    let address = std::net::SocketAddr::new(config.host.parse()?, config.port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    let material = super::runtime_api_tls_material(tls_config)?;
    let acceptor = openplotva_server::runtime_api_tls_acceptor_from_pem(
        &material.cert_pem,
        &material.key_pem,
    )?;
    let listener = openplotva_server::RuntimeApiTlsListener::new(listener, acceptor);
    let app = router(Arc::clone(&state));
    let server = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app)
            .with_graceful_shutdown(super::wait_for_runtime_stop(server_stop))
            .await
        {
            tracing::error!(%error, "maintenance API stopped");
        }
    });
    let capture = tokio::spawn(run_capture(state, queue, capture_stop));
    Ok((server, capture))
}

async fn run_capture(
    state: Arc<ApiState>,
    queue: Arc<DispatcherQueue>,
    mut stop: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(30));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            _ = tick.tick() => {
                if *stop.borrow() { break; }
                match tokio::time::timeout(Duration::from_secs(10),
                    state.incidents.capture(OffsetDateTime::now_utc())).await {
                    Ok(Ok(_)) => {},
                    Ok(Err(error)) => tracing::warn!(%error, "maintenance capture failed"),
                    Err(_) => tracing::warn!("maintenance capture timed out"),
                }
                let capture = async {
                    let notifications = state.notifications.claim_due(OffsetDateTime::now_utc()).await?;
                    for notification in notifications {
                        let digest = super::routing_admin_reports::FormattedIncidentDigest {
                            fingerprint: notification.key.clone(),
                            text: notification.body,
                            latest_occurrence: None,
                            has_incidents: true,
                        };
                        let virtual_id = format!("{NOTIFICATION_PREFIX}{}", notification.key);
                        match super::routing_admin_reports::build_admin_report_dispatch(
                            notification.recipient_id, &digest,
                            super::routing_admin_reports::AdminReportDeliveryPlan::Send, &virtual_id,
                        ) {
                            Ok(message) => { queue.enqueue(message, true); }
                            Err(error) => tracing::warn!(%error, "maintenance notification rendering failed"),
                        }
                    }
                    Ok::<(), openplotva_storage::StorageError>(())
                };
                match tokio::time::timeout(Duration::from_secs(10), capture).await {
                    Ok(Ok(())) => {},
                    Ok(Err(error)) => tracing::warn!(%error, "maintenance notifications failed"),
                    Err(_) => tracing::warn!("maintenance notifications timed out"),
                }
            }
        }
    }
}

pub async fn begin_notification(
    pool: &PgPool,
    virtual_id: &str,
) -> Result<bool, openplotva_storage::StorageError> {
    let Some(key) = virtual_id.strip_prefix(NOTIFICATION_PREFIX) else {
        return Ok(true);
    };
    MaintenanceNotificationStore::new(pool.clone())
        .begin_send(key)
        .await
}

pub async fn finish_notification(
    pool: &PgPool,
    virtual_id: &str,
    message_id: Option<i32>,
    proven_not_sent: bool,
    terminal: bool,
) -> Result<(), openplotva_storage::StorageError> {
    let Some(key) = virtual_id.strip_prefix(NOTIFICATION_PREFIX) else {
        return Ok(());
    };
    MaintenanceNotificationStore::new(pool.clone())
        .finish_send(key, message_id.map(i64::from), proven_not_sent, terminal)
        .await
}

pub async fn record_dispatch(
    pool: &PgPool,
    report: &super::virtual_messages::DispatchSendReport,
) -> Result<(), openplotva_storage::StorageError> {
    let message_id = if report.status == DispatcherSendStatus::Sent {
        report.sent_message_id
    } else {
        None
    };
    let proven_not_sent = matches!(
        report.error_class,
        Some(
            "terminal_permission"
                | "terminal_bad_request"
                | "retryable_rate_limited"
                | "retryable_transient"
                | "chat_rate_limited"
        )
    );
    let terminal = matches!(
        report.error_class,
        Some("terminal_permission" | "terminal_bad_request")
    );
    finish_notification(
        pool,
        &report.virtual_id,
        message_id,
        proven_not_sent,
        terminal,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn maintenance_lifecycle_captures_without_telegram_and_retains_notifications()
    -> anyhow::Result<()> {
        let Ok(dsn) = std::env::var("OPENPLOTVA_TEST_POSTGRES_DSN") else {
            return Ok(());
        };
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await?;
        sqlx::raw_sql(
            "CREATE TEMP TABLE llm_routing_events (
                id BIGINT, created_at TIMESTAMPTZ, event_type TEXT, workflow_key TEXT,
                provider_id BIGINT, model_id BIGINT, queue_name TEXT, job_id BIGINT,
                chat_id BIGINT, user_id BIGINT, message_id INTEGER, detail JSONB);
             CREATE TEMP TABLE llm_providers (id BIGINT, name TEXT);
             CREATE TEMP TABLE provider_models (id BIGINT, provider_id BIGINT, model_name TEXT);
             INSERT INTO llm_routing_events VALUES
                (1, now() - interval '1 second', 'route_unavailable', 'dialog',
                 NULL, NULL, 'dialog', NULL, 42, NULL, 123, '{}');",
        )
        .execute(&pool)
        .await?;
        for migration in [
            include_str!("../../../migrations/185_maintenance_incidents.up.sql")
                .split("CREATE FUNCTION")
                .next()
                .expect("table definition"),
            include_str!("../../../migrations/186_maintenance_notifications.up.sql"),
        ] {
            sqlx::raw_sql(sqlx::AssertSqlSafe(
                migration.replace("CREATE TABLE", "CREATE TEMP TABLE"),
            ))
            .execute(&pool)
            .await?;
        }
        struct TestDirectory(std::path::PathBuf);
        impl Drop for TestDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let directory = TestDirectory(std::env::temp_dir().join(format!(
            "openplotva-maintenance-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos(),
        )));
        std::fs::create_dir(&directory.0)?;
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();
        drop(socket);
        let config = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            maintenance_enabled: Some("true".into()),
            maintenance_port: Some(port.to_string()),
            maintenance_token: Some("synthetic-maintenance-lifecycle-token".into()),
            maintenance_notify_user_id: Some("42".into()),
            admins_admin_ids: Some("42".into()),
            runtime_api_host: Some("127.0.0.1".into()),
            runtime_api_cert_file: Some(
                directory.0.join("cert.pem").to_string_lossy().into_owned(),
            ),
            runtime_api_key_file: Some(directory.0.join("key.pem").to_string_lossy().into_owned()),
            ..Default::default()
        })?;
        assert!(config.bot.key.is_none());
        let queue = Arc::new(DispatcherQueue::new(
            openplotva_telegram::DispatcherConfig::default(),
        ));
        let (stop, receiver) = watch::channel(false);
        let (server, capture) = start(
            &config.maintenance,
            &config.runtime_api,
            pool.clone(),
            queue,
            receiver.clone(),
            receiver,
        )
        .await?;
        let certificate =
            reqwest::Certificate::from_pem(&std::fs::read(&config.runtime_api.cert_file)?)?;
        let client = reqwest::Client::builder()
            .add_root_certificate(certificate)
            .build()?;
        let base = format!("https://127.0.0.1:{port}{PREFIX}");
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let page: serde_json::Value = client
                    .get(format!("{base}/incidents"))
                    .bearer_auth(&config.maintenance.token)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if page["incidents"]
                    .as_array()
                    .is_some_and(|items| items.len() == 1)
                {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        let receipt: serde_json::Value = client
            .post(format!("{base}/notifications"))
            .bearer_auth(&config.maintenance.token)
            .json(&json!({"key": "a".repeat(64), "run_id": "no-bot", "status": "needs_human"}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert!(matches!(
            receipt["state"].as_str(),
            Some("pending" | "queued")
        ));
        assert!(receipt["telegram_message_id"].is_null());
        stop.send(true)?;
        tokio::time::timeout(Duration::from_secs(2), capture).await??;
        tokio::time::timeout(Duration::from_secs(2), server).await??;
        pool.close().await;
        Ok(())
    }

    #[test]
    fn maintenance_auth_accepts_only_its_own_token() {
        let hash = Sha256::digest(b"dedicated-maintenance-token").into();
        let mut headers = HeaderMap::new();
        assert!(!authorized(&hash, &headers));
        headers.insert(
            "authorization",
            "Bearer runtime-admin-token".parse().expect("header"),
        );
        assert!(!authorized(&hash, &headers));
        headers.insert(
            "authorization",
            "Bearer dedicated-maintenance-token"
                .parse()
                .expect("header"),
        );
        assert!(authorized(&hash, &headers));
    }

    #[test]
    fn maintenance_notification_requires_proven_pr_link_and_cannot_inject_text() {
        let mut request = NotificationRequest {
            key: "a".repeat(64),
            run_id: "run-123".into(),
            status: NotificationStatus::PrReady,
            issue_number: Some(12),
            pr_number: None,
        };
        assert!(request.text().is_err());
        request.pr_number = Some(13);
        assert!(request.text().expect("text").contains("openplotva/pull/13"));
        request.run_id = "$(printenv)".into();
        assert!(request.text().is_err());
        assert!(serde_json::from_value::<NotificationRequest>(json!({
            "key": "a".repeat(64), "run_id": "run-123", "status": "needs_human", "text": "private"
        })).is_err());
    }

    #[tokio::test]
    async fn maintenance_http_gate_rejects_broad_auth_and_out_of_contract_requests()
    -> anyhow::Result<()> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")?;
        let state = Arc::new(ApiState {
            token_hash: Sha256::digest(b"dedicated-token").into(),
            recipient_id: 42,
            incidents: MaintenanceStore::new(pool.clone()),
            notifications: MaintenanceNotificationStore::new(pool),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router(state)).await });
        let client = reqwest::Client::new();
        let url = format!("http://{address}{PREFIX}");
        assert_eq!(
            client.get(format!("{url}/health")).send().await?.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(format!("{url}/health"))
                .bearer_auth("runtime-admin-token")
                .send()
                .await?
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            client
                .get(format!("{url}/health"))
                .bearer_auth("dedicated-token")
                .send()
                .await?
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            client
                .get(format!("{url}/incidents/-1/evidence"))
                .bearer_auth("dedicated-token")
                .send()
                .await?
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            client
                .get(format!("{url}/incidents?sql=select"))
                .bearer_auth("dedicated-token")
                .send()
                .await?
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(client.post(format!("{url}/notifications")).bearer_auth("dedicated-token")
            .json(&json!({"key":"a".repeat(64),"run_id":"run-1","status":"needs_human","text":"canary"}))
            .send().await?.status(), StatusCode::UNPROCESSABLE_ENTITY);
        server.abort();
        Ok(())
    }
}
