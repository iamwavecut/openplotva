//! Fetch the model list of an OpenAI-compatible endpoint (`GET /v1/models`).
//!
//! Used by the admin "fetch models" action; never on a request path. The base
//! URL may be a bare host, a `/v1` root, or a full `/v1/chat/completions`
//! URL (the shape stored on provider/model rows) — all normalize to the same
//! `/v1/models` listing URL.

use std::time::Duration;

use serde::Deserialize;

/// One model advertised by the endpoint, with the raw entry for diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteModel {
    pub name: String,
    pub raw: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum ModelListError {
    #[error("model listing endpoint URL is empty")]
    EmptyBaseUrl,
    #[error("model listing request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("model listing returned HTTP {status}: {body}")]
    Status {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("model listing response is not valid JSON: {0}")]
    Decode(#[source] serde_json::Error),
}

#[derive(Deserialize)]
struct ModelsResponse {
    #[serde(default)]
    data: Vec<serde_json::Value>,
    /// Some servers (ollama-style) return a bare `models` array instead.
    #[serde(default)]
    models: Vec<serde_json::Value>,
}

/// Normalize any stored endpoint shape to its `/v1/models` listing URL.
#[must_use]
pub fn models_url(base_url: &str) -> String {
    let trimmed = base_url.trim().trim_end_matches('/');
    let root = trimmed
        .strip_suffix("/chat/completions")
        .unwrap_or(trimmed)
        .trim_end_matches('/');
    let root = root.strip_suffix("/v1").unwrap_or(root);
    format!("{root}/v1/models")
}

/// Fetch and parse the endpoint's model list. `api_key`, when present, is sent
/// as a bearer token. Bounded by `timeout` end to end.
pub async fn list_openai_compat_models(
    base_url: &str,
    api_key: Option<&str>,
    timeout: Duration,
) -> Result<Vec<RemoteModel>, ModelListError> {
    if base_url.trim().is_empty() {
        return Err(ModelListError::EmptyBaseUrl);
    }
    let client = reqwest::Client::builder().timeout(timeout).build()?;
    let mut request = client.get(models_url(base_url));
    if let Some(key) = api_key.filter(|key| !key.trim().is_empty()) {
        request = request.bearer_auth(key.trim());
    }
    let response = request.send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(ModelListError::Status {
            status,
            body: body.chars().take(512).collect(),
        });
    }
    let parsed: ModelsResponse = serde_json::from_str(&body).map_err(ModelListError::Decode)?;
    let entries = if parsed.data.is_empty() {
        parsed.models
    } else {
        parsed.data
    };
    let mut models: Vec<RemoteModel> = entries
        .into_iter()
        .filter_map(|entry| {
            let name = entry
                .get("id")
                .or_else(|| entry.get("name"))
                .or_else(|| entry.get("model"))
                .and_then(serde_json::Value::as_str)?
                .trim()
                .to_owned();
            if name.is_empty() {
                return None;
            }
            Some(RemoteModel { name, raw: entry })
        })
        .collect();
    models.sort_by(|a, b| a.name.cmp(&b.name));
    models.dedup_by(|a, b| a.name == b.name);
    Ok(models)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models_url_normalizes_every_stored_endpoint_shape() {
        assert_eq!(models_url("https://x.tld"), "https://x.tld/v1/models");
        assert_eq!(models_url("https://x.tld/"), "https://x.tld/v1/models");
        assert_eq!(models_url("https://x.tld/v1"), "https://x.tld/v1/models");
        assert_eq!(
            models_url("https://x.tld/v1/chat/completions"),
            "https://x.tld/v1/models"
        );
        assert_eq!(
            models_url("https://openrouter.ai/api/v1/chat/completions"),
            "https://openrouter.ai/api/v1/models"
        );
    }

    #[test]
    fn response_parsing_accepts_openai_and_bare_model_shapes() {
        let openai: ModelsResponse = serde_json::from_str(
            r#"{"object":"list","data":[{"id":"qwen3.6-35b-a3b"},{"id":"qwen3.6-27b"}]}"#,
        )
        .expect("openai shape");
        assert_eq!(openai.data.len(), 2);

        let bare: ModelsResponse =
            serde_json::from_str(r#"{"models":[{"name":"vibethinker-3b"}]}"#).expect("bare shape");
        assert_eq!(bare.models.len(), 1);
    }
}
