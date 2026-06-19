//! Client for the `plotva.geta.moe` media uploader.
//!
//! Telegram Rich Messages embed media only by HTTPS URL (no `file_id`, no multipart).
//! Generated images/songs therefore have to be published to a public URL first. This
//! client uploads bytes (or hands off a remote URL for the service to fetch) and returns
//! the public URL to embed in rich HTML. The uploader keeps a caller-provided file name
//! when given one (so a song's inline player shows "Author — Title.mp3"); otherwise the
//! service assigns a random XID-prefixed name.

use std::time::Duration;

use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use url::Url;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

/// Configuration for the media uploader client.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UploaderConfig {
    /// Public base URL of the uploader (e.g. `https://plotva.geta.moe`).
    pub base_url: String,
    /// Shared upload secret (sent as a bearer token).
    pub secret: String,
    /// Request timeout.
    pub timeout: Duration,
}

impl UploaderConfig {
    #[must_use]
    pub fn with_defaults(mut self) -> Self {
        self.base_url = self.base_url.trim().trim_end_matches('/').to_owned();
        self.secret = self.secret.trim().to_owned();
        if self.timeout == Duration::ZERO {
            self.timeout = DEFAULT_TIMEOUT;
        }
        self
    }

    #[must_use]
    pub fn configured(&self) -> bool {
        !self.base_url.trim().is_empty() && !self.secret.trim().is_empty()
    }
}

/// Errors raised while uploading media.
#[derive(Debug, Error)]
pub enum UploaderError {
    #[error("media uploader is not configured")]
    NotConfigured,
    #[error("failed to build uploader http client: {0}")]
    BuildHttpClient(#[source] reqwest::Error),
    #[error("uploader transport error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("uploader rejected request: status={status} body={body}")]
    Upstream { status: u16, body: String },
    #[error("uploader response decode error: {0}")]
    Decode(String),
    #[error("uploader response missing url")]
    MissingUrl,
}

#[derive(Debug, Deserialize)]
struct UploadResponse {
    #[serde(default)]
    url: String,
    #[serde(default)]
    #[allow(dead_code)]
    name: String,
}

/// HTTP client for the media uploader.
#[derive(Clone, Debug)]
pub struct UploaderClient {
    cfg: UploaderConfig,
    http: reqwest::Client,
}

impl UploaderClient {
    /// Build a reqwest-backed uploader client.
    pub fn new(cfg: UploaderConfig) -> Result<Self, UploaderError> {
        let cfg = cfg.with_defaults();
        let http = reqwest::Client::builder()
            .timeout(cfg.timeout)
            .build()
            .map_err(UploaderError::BuildHttpClient)?;
        Ok(Self { cfg, http })
    }

    #[must_use]
    pub fn configured(&self) -> bool {
        self.cfg.configured()
    }

    /// Upload raw bytes and return the public URL. `content_type` should be the media
    /// MIME (e.g. `image/png`, `audio/mpeg`); `explicit_name` preserves a display file
    /// name (otherwise the service assigns a random one).
    pub async fn upload_bytes(
        &self,
        bytes: Vec<u8>,
        content_type: &str,
        explicit_name: Option<&str>,
    ) -> Result<String, UploaderError> {
        if !self.configured() {
            return Err(UploaderError::NotConfigured);
        }
        let file_name = explicit_name.unwrap_or("upload").to_owned();
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(file_name)
            .mime_str(content_type)?;
        let mut form = reqwest::multipart::Form::new().part("file", part);
        if let Some(name) = explicit_name {
            form = form.text("name", name.to_owned());
        }
        let response = self
            .http
            .post(self.endpoint())
            .bearer_auth(&self.cfg.secret)
            .multipart(form)
            .send()
            .await?;
        Self::read_url(response).await
    }

    /// Hand a remote URL to the uploader to fetch and re-host; returns the public URL.
    pub async fn upload_url(
        &self,
        source_url: &str,
        explicit_name: Option<&str>,
    ) -> Result<String, UploaderError> {
        if !self.configured() {
            return Err(UploaderError::NotConfigured);
        }
        let mut body = json!({ "url": source_url });
        if let Some(name) = explicit_name {
            body["name"] = json!(name);
        }
        let response = self
            .http
            .post(self.endpoint())
            .bearer_auth(&self.cfg.secret)
            .json(&body)
            .send()
            .await?;
        Self::read_url(response).await
    }

    fn endpoint(&self) -> String {
        format!("{}/upload", self.cfg.base_url)
    }

    async fn read_url(response: reqwest::Response) -> Result<String, UploaderError> {
        let status = response.status();
        let text = response.text().await?;
        parse_upload_response(status.is_success(), status.as_u16(), &text)
    }
}

fn parse_upload_response(success: bool, status: u16, text: &str) -> Result<String, UploaderError> {
    if !success {
        return Err(UploaderError::Upstream {
            status,
            body: text.to_owned(),
        });
    }
    let parsed: UploadResponse =
        serde_json::from_str(text).map_err(|err| UploaderError::Decode(err.to_string()))?;
    let url = parsed.url.trim();
    if url.is_empty() {
        return Err(UploaderError::MissingUrl);
    }
    Ok(telegram_safe_upload_url(url))
}

fn telegram_safe_upload_url(url: &str) -> String {
    Url::parse(url).map_or_else(|_| url.to_owned(), |parsed| parsed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_and_configured() {
        let cfg = UploaderConfig {
            base_url: "https://plotva.geta.moe/".to_owned(),
            secret: " s ".to_owned(),
            timeout: Duration::ZERO,
        }
        .with_defaults();
        assert_eq!(cfg.base_url, "https://plotva.geta.moe");
        assert_eq!(cfg.secret, "s");
        assert_eq!(cfg.timeout, DEFAULT_TIMEOUT);
        assert!(cfg.configured());
        assert!(!UploaderConfig::default().configured());
    }

    #[test]
    fn endpoint_is_base_plus_upload() {
        let client = UploaderClient::new(UploaderConfig {
            base_url: "https://plotva.geta.moe".to_owned(),
            secret: "s".to_owned(),
            timeout: Duration::ZERO,
        })
        .unwrap();
        assert_eq!(client.endpoint(), "https://plotva.geta.moe/upload");
    }

    #[test]
    fn parses_url_from_ok_response() {
        let url = parse_upload_response(
            true,
            200,
            r#"{"url":"https://plotva.geta.moe/abc.png","name":"abc.png"}"#,
        )
        .unwrap();
        assert_eq!(url, "https://plotva.geta.moe/abc.png");
    }

    #[test]
    fn parses_ok_response_as_telegram_safe_url() {
        let url = parse_upload_response(
            true,
            200,
            r#"{"url":"https://plotva.geta.moe/media/ЧиХПыХ - Величественная кошачья.mp3"}"#,
        )
        .unwrap();

        assert_eq!(
            url,
            "https://plotva.geta.moe/media/%D0%A7%D0%B8%D0%A5%D0%9F%D1%8B%D0%A5%20-%20%D0%92%D0%B5%D0%BB%D0%B8%D1%87%D0%B5%D1%81%D1%82%D0%B2%D0%B5%D0%BD%D0%BD%D0%B0%D1%8F%20%D0%BA%D0%BE%D1%88%D0%B0%D1%87%D1%8C%D1%8F.mp3"
        );
    }

    #[test]
    fn errors_on_failure_and_empty_url() {
        let err = parse_upload_response(false, 401, "unauthorized").unwrap_err();
        assert!(matches!(err, UploaderError::Upstream { status: 401, .. }));
        let err = parse_upload_response(true, 200, r#"{"url":""}"#).unwrap_err();
        assert!(matches!(err, UploaderError::MissingUrl));
    }
}
