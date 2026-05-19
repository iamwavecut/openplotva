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
    use super::{HealthResponse, ReadinessCheck, ReadinessResponse, health_response};

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
}
