//! HTTP server surfaces for OpenPlotva.

use std::sync::Arc;

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use serde::Serialize;

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

/// Go Redis key prefix for persisted rate-limited chat expiry timestamps.
pub const RATE_LIMITED_CHAT_KEY_PREFIX: &str = "plotva:rate_limited_chat:";

/// Go permission action for sending text messages.
pub const ACTION_SEND_TEXT: &str = "send_text";
/// Go permission action for sending images.
pub const ACTION_SEND_IMAGE: &str = "send_image";
/// Go permission action for sending stickers.
pub const ACTION_SEND_STICKER: &str = "send_sticker";
/// Go permission action for sending videos.
pub const ACTION_SEND_VIDEO: &str = "send_video";
/// Go permission action for sending audio.
pub const ACTION_SEND_AUDIO: &str = "send_audio";
/// Go permission action for sending files.
pub const ACTION_SEND_FILE: &str = "send_file";
/// Go permission action for sending polls.
pub const ACTION_SEND_POLL: &str = "send_poll";
/// Go permission action for generic message sends.
pub const ACTION_SEND_MESSAGE: &str = "send_message";
/// Go permission action for pinning messages.
pub const ACTION_PIN_MESSAGE: &str = "pin_message";
/// Go permission action for editing messages.
pub const ACTION_EDIT_MESSAGE: &str = "edit_message";

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

/// Build the in-memory Go rate-limit cache key for a chat.
pub fn rate_limit_key(chat_id: i64) -> String {
    format!("rate_limit:{chat_id}")
}

/// Build the persisted Go rate-limited-chat key for a chat.
pub fn rate_limited_chat_key(chat_id: i64) -> String {
    format!("{RATE_LIMITED_CHAT_KEY_PREFIX}{chat_id}")
}

/// Build the Go permission cache key for a chat/action pair.
pub fn permission_cache_key(chat_id: i64, action: &str) -> String {
    format!("perm_check:{chat_id}:{action}")
}

/// Build the Go VIP cache key for a user.
pub fn vip_cache_key(user_id: i64) -> String {
    format!("vip:{user_id}")
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
        ACTION_SEND_TEXT, HealthResponse, ReadinessCheck, ReadinessResponse, health_response,
        permission_cache_key, rate_limit_key, rate_limited_chat_key, vip_cache_key,
    };

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
            ReadinessCheck::ok("reference_snapshot", "verified"),
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
}
