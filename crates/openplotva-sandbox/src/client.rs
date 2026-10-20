//! HTTP client the bot uses to reach the sandbox supervisor.

use std::time::Duration;

use reqwest::StatusCode;
use thiserror::Error;
use url::Url;

use crate::api::{
    ERROR_BUSY, ERROR_FILE_NOT_FOUND, ERROR_SESSION_NOT_FOUND, ErrorBody, RunRequest, RunResponse,
};

/// Error returned by [`SandboxClient`].
#[derive(Debug, Error)]
pub enum SandboxClientError {
    /// The configured base URL is not usable.
    #[error("invalid sandbox url: {0}")]
    InvalidUrl(String),
    /// Every session slot is taken.
    #[error("sandbox is busy: {0}")]
    Busy(String),
    /// The session or file does not exist.
    #[error("{code}: {message}")]
    NotFound { code: String, message: String },
    /// The supervisor rejected the request.
    #[error("{code}: {message}")]
    Rejected { code: String, message: String },
    /// The request did not reach the supervisor or the response was cut.
    #[error("sandbox transport: {0}")]
    Transport(String),
    /// The response body is not the expected JSON.
    #[error("sandbox response: {0}")]
    Decode(String),
}

/// Client for one supervisor base URL.
#[derive(Clone, Debug)]
pub struct SandboxClient {
    http: reqwest::Client,
    base: Url,
}

impl SandboxClient {
    /// Build a client for `base_url`, for example `http://sandbox:8090`.
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, SandboxClientError> {
        let mut base = Url::parse(base_url.trim())
            .map_err(|error| SandboxClientError::InvalidUrl(error.to_string()))?;
        if base.cannot_be_a_base() {
            return Err(SandboxClientError::InvalidUrl(base_url.to_owned()));
        }
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| SandboxClientError::Transport(error.to_string()))?;
        Ok(Self { http, base })
    }

    /// Run a script in the session, creating the session when needed.
    pub async fn run(
        &self,
        session: &str,
        request: &RunRequest,
    ) -> Result<RunResponse, SandboxClientError> {
        let url = self.endpoint(&["v1", "sessions", session, "run"])?;
        let response = self
            .http
            .post(url)
            .json(request)
            .send()
            .await
            .map_err(|error| SandboxClientError::Transport(error.to_string()))?;
        let response = error_for_status(response).await?;
        response
            .json::<RunResponse>()
            .await
            .map_err(|error| SandboxClientError::Decode(error.to_string()))
    }

    /// Read one file from the session home.
    pub async fn read_file(
        &self,
        session: &str,
        path: &str,
    ) -> Result<Vec<u8>, SandboxClientError> {
        let mut segments = vec!["v1", "sessions", session, "files"];
        segments.extend(path.split('/'));
        let url = self.endpoint(&segments)?;
        let response = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|error| SandboxClientError::Transport(error.to_string()))?;
        let response = error_for_status(response).await?;
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| SandboxClientError::Transport(error.to_string()))
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, SandboxClientError> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| SandboxClientError::InvalidUrl(self.base.to_string()))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }
}

async fn error_for_status(
    response: reqwest::Response,
) -> Result<reqwest::Response, SandboxClientError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let text = response.text().await.unwrap_or_default();
    let body = serde_json::from_str::<ErrorBody>(&text).unwrap_or_else(|_| ErrorBody {
        code: status.as_u16().to_string(),
        message: text.chars().take(300).collect(),
    });
    Err(match (status, body.code.as_str()) {
        (_, ERROR_BUSY) | (StatusCode::SERVICE_UNAVAILABLE, _) => {
            SandboxClientError::Busy(body.message)
        }
        (_, ERROR_SESSION_NOT_FOUND | ERROR_FILE_NOT_FOUND) => SandboxClientError::NotFound {
            code: body.code,
            message: body.message,
        },
        _ => SandboxClientError::Rejected {
            code: body.code,
            message: body.message,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_percent_encode_each_path_segment() {
        let client =
            SandboxClient::new("http://sandbox:8090", Duration::from_secs(5)).expect("client");
        let url = client
            .endpoint(&["v1", "sessions", "chat-1", "files", "графики", "a b.png"])
            .expect("url");
        assert_eq!(
            url.as_str(),
            "http://sandbox:8090/v1/sessions/chat-1/files/%D0%B3%D1%80%D0%B0%D1%84%D0%B8%D0%BA%D0%B8/a%20b.png"
        );
    }

    #[test]
    fn base_path_prefix_is_kept() {
        let client =
            SandboxClient::new("http://proxy/sandbox", Duration::from_secs(5)).expect("client");
        let url = client
            .endpoint(&["v1", "sessions", "k", "run"])
            .expect("url");
        assert_eq!(url.as_str(), "http://proxy/sandbox/v1/sessions/k/run");
    }

    #[test]
    fn rejects_unusable_base_urls() {
        assert!(SandboxClient::new("not a url", Duration::from_secs(5)).is_err());
        assert!(SandboxClient::new("mailto:x@example.com", Duration::from_secs(5)).is_err());
    }
}
