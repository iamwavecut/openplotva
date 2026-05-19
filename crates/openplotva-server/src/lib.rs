//! HTTP server surfaces for OpenPlotva.

use axum::{Json, Router, routing::get};
use serde::Serialize;

/// Health response returned by `/api/health`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct HealthResponse {
    /// Service status.
    pub status: &'static str,
    /// Service name.
    pub service: &'static str,
}

/// Build the HTTP router for the current app shell.
pub fn router() -> Router {
    Router::new().route("/api/health", get(health))
}

/// Build the health payload without transport details.
pub fn health_response() -> HealthResponse {
    HealthResponse {
        status: "ok",
        service: openplotva_core::PROJECT_NAME,
    }
}

async fn health() -> Json<HealthResponse> {
    Json(health_response())
}

#[cfg(test)]
mod tests {
    use super::{HealthResponse, health_response};

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
}
