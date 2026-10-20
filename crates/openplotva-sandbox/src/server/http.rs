//! HTTP routes of the supervisor.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::json;

use super::home::{self, HomeFileError, HomeStorage};
use super::runner::CodeRunner;
use super::session::{SessionError, SessionManager};
use crate::api::{
    ERROR_BUSY, ERROR_FILE_NOT_FOUND, ERROR_FILE_TOO_LARGE, ERROR_INTERNAL, ERROR_INVALID_REQUEST,
    ERROR_SESSION_NOT_FOUND, ErrorBody, RunRequest, RunResponse, is_valid_session_key,
};

/// Longest accepted script.
pub const MAX_CODE_BYTES: usize = 512 * 1024;

/// Shared state of the routes.
pub struct AppState {
    pub sessions: Arc<SessionManager>,
    pub runner: Arc<dyn CodeRunner>,
    pub max_listed_files: usize,
    pub max_read_bytes: u64,
    pub max_input_bytes: u64,
}

/// Build the supervisor router.
pub fn router(state: Arc<AppState>, max_request_bytes: usize) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/sessions/{key}/run", post(run))
        .route("/v1/sessions/{key}/files/{*path}", get(read_file))
        .route("/v1/sessions/{key}", delete(close))
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .with_state(state)
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(json!({ "status": "ok", "sessions": state.sessions.live() }))
}

async fn run(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    body: Result<Json<RunRequest>, JsonRejection>,
) -> Response {
    if !is_valid_session_key(&key) {
        return error(
            StatusCode::BAD_REQUEST,
            ERROR_INVALID_REQUEST,
            "invalid session key",
        );
    }
    let request = match body {
        Ok(Json(request)) => request,
        Err(rejection) => {
            return error(
                StatusCode::BAD_REQUEST,
                ERROR_INVALID_REQUEST,
                &rejection.body_text(),
            );
        }
    };
    if request.code.trim().is_empty() || request.code.len() > MAX_CODE_BYTES {
        return error(
            StatusCode::BAD_REQUEST,
            ERROR_INVALID_REQUEST,
            &format!("code must be 1..{MAX_CODE_BYTES} bytes"),
        );
    }
    let mut inputs = Vec::with_capacity(request.files.len());
    let mut input_bytes = 0_u64;
    for file in &request.files {
        let Ok(content) = BASE64.decode(file.content_base64.as_bytes()) else {
            return error(
                StatusCode::BAD_REQUEST,
                ERROR_INVALID_REQUEST,
                &format!("{}: content is not base64", file.path),
            );
        };
        input_bytes += content.len() as u64;
        inputs.push((file.path.clone(), content));
    }
    if input_bytes > state.max_input_bytes {
        return error(
            StatusCode::BAD_REQUEST,
            ERROR_INVALID_REQUEST,
            &format!("input files exceed {} bytes", state.max_input_bytes),
        );
    }

    let lease = match state.sessions.lease(&key).await {
        Ok(lease) => lease,
        Err(SessionError::Busy(limit)) => {
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                ERROR_BUSY,
                &format!("all {limit} sandbox sessions are in use; retry later"),
            );
        }
        Err(SessionError::Home(io)) => {
            tracing::error!(%io, "failed to open a sandbox session");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ERROR_INTERNAL,
                "session setup failed",
            );
        }
    };
    let home_dir = lease.home().to_path_buf();
    let storage = state.sessions.storage().clone();
    let before = match blocking(move || write_inputs(&storage, &home_dir, &inputs)).await {
        Ok(Ok(before)) => before,
        Ok(Err((path, file_error))) => {
            return error(
                StatusCode::BAD_REQUEST,
                ERROR_INVALID_REQUEST,
                &format!("{path}: {file_error}"),
            );
        }
        Err(message) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, ERROR_INTERNAL, &message);
        }
    };

    let outcome = state.runner.run(lease.home(), &request.code).await;
    let home_dir = lease.home().to_path_buf();
    let after = blocking(move || home::snapshot(&home_dir))
        .await
        .unwrap_or_default();
    let files = home::file_entries(&before, &after, state.max_listed_files);
    let session = lease.finish();
    Json(RunResponse {
        termination: outcome.termination,
        exit_code: outcome.exit_code,
        stdout: outcome.stdout,
        stdout_truncated: outcome.stdout_truncated,
        stderr: outcome.stderr,
        stderr_truncated: outcome.stderr_truncated,
        duration_ms: u64::try_from(outcome.duration.as_millis()).unwrap_or(u64::MAX),
        files,
        session,
    })
    .into_response()
}

async fn read_file(
    State(state): State<Arc<AppState>>,
    Path((key, path)): Path<(String, String)>,
) -> Response {
    if !is_valid_session_key(&key) {
        return error(
            StatusCode::BAD_REQUEST,
            ERROR_INVALID_REQUEST,
            "invalid session key",
        );
    }
    let Some(lease) = state.sessions.lease_existing(&key).await else {
        return error(
            StatusCode::NOT_FOUND,
            ERROR_SESSION_NOT_FOUND,
            "the sandbox session has expired",
        );
    };
    let home_dir = lease.home().to_path_buf();
    let limit = state.max_read_bytes;
    let requested = path.clone();
    let result = blocking(move || home::read_file(&home_dir, &requested, limit)).await;
    let _session = lease.finish();
    match result {
        Ok(Ok(bytes)) => (
            [(header::CONTENT_TYPE, "application/octet-stream")],
            Body::from(bytes),
        )
            .into_response(),
        Ok(Err(HomeFileError::InvalidPath)) => error(
            StatusCode::BAD_REQUEST,
            ERROR_INVALID_REQUEST,
            "invalid file path",
        ),
        Ok(Err(HomeFileError::TooLarge { size, limit })) => error(
            StatusCode::PAYLOAD_TOO_LARGE,
            ERROR_FILE_TOO_LARGE,
            &format!("{path} is {size} bytes; the limit is {limit}"),
        ),
        Ok(Err(HomeFileError::Io(io))) => {
            tracing::warn!(%io, "failed to read a session file");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ERROR_INTERNAL,
                "file read failed",
            )
        }
        Ok(Err(_)) => error(
            StatusCode::NOT_FOUND,
            ERROR_FILE_NOT_FOUND,
            &format!("{path} does not exist"),
        ),
        Err(message) => error(StatusCode::INTERNAL_SERVER_ERROR, ERROR_INTERNAL, &message),
    }
}

async fn close(State(state): State<Arc<AppState>>, Path(key): Path<String>) -> Response {
    if state.sessions.close(&key).await {
        StatusCode::NO_CONTENT.into_response()
    } else {
        error(
            StatusCode::NOT_FOUND,
            ERROR_SESSION_NOT_FOUND,
            "no such session",
        )
    }
}

type WriteFailure = (String, HomeFileError);

fn write_inputs(
    storage: &HomeStorage,
    home: &std::path::Path,
    inputs: &[(String, Vec<u8>)],
) -> Result<home::HomeSnapshot, WriteFailure> {
    for (path, content) in inputs {
        storage
            .write_file(home, path, content)
            .map_err(|file_error| (path.clone(), file_error))?;
    }
    Ok(home::snapshot(home))
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|join| format!("blocking task failed: {join}"))
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(ErrorBody {
            code: code.to_owned(),
            message: message.to_owned(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::path::Path as FsPath;
    use std::time::Duration;

    use super::*;
    use crate::api::{FileEntry, InputFile, Termination};
    use crate::client::{SandboxClient, SandboxClientError};
    use crate::server::runner::{RunFuture, RunOutcome};
    use crate::server::session::SessionLimits;

    struct EchoRunner;

    impl CodeRunner for EchoRunner {
        fn run<'a>(&'a self, home: &'a FsPath, code: &'a str) -> RunFuture<'a> {
            Box::pin(async move {
                let input = std::fs::read_to_string(home.join("in.txt")).unwrap_or_default();
                std::fs::write(home.join("out.txt"), format!("{code}|{input}")).expect("write");
                RunOutcome {
                    termination: Termination::Exited,
                    exit_code: Some(0),
                    stdout: "done\n".to_owned(),
                    ..RunOutcome::default()
                }
            })
        }
    }

    async fn serve(name: &str) -> (SandboxClient, std::path::PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "openplotva-sandbox-http-{name}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let sessions = Arc::new(SessionManager::new(
            HomeStorage::PlainDir,
            root.clone(),
            None,
            SessionLimits {
                max_sessions: 1,
                idle_ttl: Duration::from_secs(60),
                max_age: Duration::from_secs(300),
                slot_wait: Duration::from_millis(20),
            },
        ));
        let state = Arc::new(AppState {
            sessions,
            runner: Arc::new(EchoRunner),
            max_listed_files: 10,
            max_read_bytes: 1024,
            max_input_bytes: 1024,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let address = listener.local_addr().expect("address");
        tokio::spawn(async move {
            axum::serve(listener, router(state, 1024 * 1024))
                .await
                .expect("serve");
        });
        let client = SandboxClient::new(&format!("http://{address}"), Duration::from_secs(5))
            .expect("client");
        (client, root)
    }

    #[tokio::test]
    async fn run_writes_inputs_lists_outputs_and_serves_them() {
        let (client, root) = serve("run").await;
        let request = RunRequest {
            code: "print(1)".to_owned(),
            files: vec![InputFile {
                path: "in.txt".to_owned(),
                content_base64: BASE64.encode("данные"),
            }],
        };
        let response = client.run("chat-1", &request).await.expect("run");
        assert_eq!(response.exit_code, Some(0));
        assert_eq!(response.stdout, "done\n");
        assert!(response.session.created);
        assert_eq!(
            response.files,
            vec![
                FileEntry {
                    path: "out.txt".to_owned(),
                    size: "print(1)|данные".len() as u64,
                    changed: true,
                },
                FileEntry {
                    path: "in.txt".to_owned(),
                    size: "данные".len() as u64,
                    changed: false,
                },
            ]
        );

        let again = client.run("chat-1", &request).await.expect("second run");
        assert!(!again.session.created);

        let bytes = client.read_file("chat-1", "out.txt").await.expect("read");
        assert_eq!(String::from_utf8(bytes).expect("utf8"), "print(1)|данные");
        assert!(matches!(
            client.read_file("chat-1", "missing.png").await,
            Err(SandboxClientError::NotFound { .. })
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn rejects_bad_requests_and_reports_busy_and_expired_sessions() {
        let (client, root) = serve("errors").await;
        let ok = RunRequest {
            code: "x".to_owned(),
            files: Vec::new(),
        };
        assert!(matches!(
            client.run("bad.key", &ok).await,
            Err(SandboxClientError::Rejected { code, .. }) if code == ERROR_INVALID_REQUEST
        ));
        let escape = RunRequest {
            code: "x".to_owned(),
            files: vec![InputFile {
                path: "../escape".to_owned(),
                content_base64: BASE64.encode("x"),
            }],
        };
        assert!(matches!(
            client.run("a", &escape).await,
            Err(SandboxClientError::Rejected { code, .. }) if code == ERROR_INVALID_REQUEST
        ));
        assert!(matches!(
            client.run("b", &ok).await,
            Err(SandboxClientError::Busy(_))
        ));
        assert!(matches!(
            client.read_file("zzz", "out.txt").await,
            Err(SandboxClientError::NotFound { code, .. }) if code == ERROR_SESSION_NOT_FOUND
        ));
        let _ = std::fs::remove_dir_all(root);
    }
}
