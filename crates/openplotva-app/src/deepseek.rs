//! DeepSeek Search API adapter for dialog tools.

use std::{fmt, sync::Arc, time::Duration};

use openplotva_config::DeepSeekConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::dialog_tools::{WebSearchFuture, WebSearchProvider};

const SEARCH_ENDPOINT: &str = "https://api.deepseek.com/v1/tools/search";
const MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Clone)]
pub struct DeepSeekClient {
    http: reqwest::Client,
    api_key: Arc<str>,
    endpoint: Arc<str>,
    timeout: Duration,
}

impl fmt::Debug for DeepSeekClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepSeekClient").finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum DeepSeekError {
    #[error("DeepSeek search query must contain 1 to 128 Unicode characters")]
    InvalidQuery,
    #[error("DeepSeek search timed out")]
    Timeout,
    #[error("DeepSeek search HTTP status {0}")]
    Status(reqwest::StatusCode),
    #[error("DeepSeek search transport error: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("DeepSeek search returned an invalid response")]
    InvalidResponse,
}

#[derive(Serialize)]
struct SearchRequest<'a> {
    query: &'a str,
    count: usize,
    need_content: bool,
}

#[derive(Deserialize, Serialize)]
struct SearchResponse {
    results: Vec<SearchResult>,
}

#[derive(Deserialize, Serialize)]
struct SearchResult {
    title: String,
    url: String,
    snippet: String,
    summary: String,
    #[serde(flatten)]
    metadata: serde_json::Map<String, serde_json::Value>,
}

impl DeepSeekClient {
    pub fn from_config(config: &DeepSeekConfig) -> Result<Option<Self>, DeepSeekError> {
        Self::with_endpoint(config, SEARCH_ENDPOINT)
    }

    fn with_endpoint(
        config: &DeepSeekConfig,
        endpoint: &str,
    ) -> Result<Option<Self>, DeepSeekError> {
        if config.api_key.trim().is_empty() {
            return Ok(None);
        }
        let timeout = Duration::from_secs(config.timeout_seconds.max(1) as u64);
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(DeepSeekError::Transport)?;
        Ok(Some(Self {
            http,
            api_key: Arc::from(config.api_key.trim()),
            endpoint: Arc::from(endpoint),
            timeout,
        }))
    }

    pub async fn search(&self, query: &str) -> Result<String, DeepSeekError> {
        let query = query.trim();
        if query.is_empty() || query.chars().count() > 128 {
            return Err(DeepSeekError::InvalidQuery);
        }
        tokio::time::timeout(self.timeout, self.search_with_retry(query))
            .await
            .map_err(|_| DeepSeekError::Timeout)?
    }

    async fn search_with_retry(&self, query: &str) -> Result<String, DeepSeekError> {
        for attempt in 0..3 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(attempt)).await;
            }
            let response = self
                .http
                .post(&*self.endpoint)
                .bearer_auth(&*self.api_key)
                .json(&SearchRequest {
                    query,
                    count: 10,
                    need_content: false,
                })
                .send()
                .await;
            let mut response = match response {
                Ok(response) => response,
                Err(error) if attempt < 2 && (error.is_connect() || error.is_timeout()) => continue,
                Err(error) => return Err(DeepSeekError::Transport(error.without_url())),
            };
            let status = response.status();
            if !status.is_success() {
                if attempt < 2
                    && (status.is_server_error()
                        || status == reqwest::StatusCode::REQUEST_TIMEOUT
                        || status == reqwest::StatusCode::TOO_MANY_REQUESTS)
                {
                    continue;
                }
                return Err(DeepSeekError::Status(status));
            }
            let mut body = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| DeepSeekError::Transport(error.without_url()))?
            {
                if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                    return Err(DeepSeekError::InvalidResponse);
                }
                body.extend_from_slice(&chunk);
            }
            let result: SearchResponse =
                serde_json::from_slice(&body).map_err(|_| DeepSeekError::InvalidResponse)?;
            return serde_json::to_string(&result).map_err(|_| DeepSeekError::InvalidResponse);
        }
        unreachable!("each final attempt returns its result")
    }
}

impl WebSearchProvider for DeepSeekClient {
    fn search<'a>(&'a self, query: &'a str) -> WebSearchFuture<'a> {
        Box::pin(async move { self.search(query).await.map_err(Into::into) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
    };
    use serde_json::{Value, json};
    use std::{collections::VecDeque, sync::Mutex};

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
    const RESULT: &str = r#"{"results":[{"rerank_score":2.4,"title":"Source title","url":"https://example.com/article","snippet":"Short snippet","summary":"Longer source summary","content":"","site_name":null,"site_icon":null,"date":null,"authority":3}]}"#;

    #[derive(Default)]
    struct FixtureState {
        responses: Mutex<VecDeque<(StatusCode, String)>>,
        requests: Mutex<Vec<(HeaderMap, Value)>>,
        delay: Duration,
    }

    struct Fixture {
        state: Arc<FixtureState>,
        client: DeepSeekClient,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn fixture(
        responses: Vec<(StatusCode, String)>,
        delay: Duration,
        timeout: i32,
    ) -> Result<Fixture, Box<dyn std::error::Error + Send + Sync>> {
        let state = Arc::new(FixtureState {
            responses: Mutex::new(responses.into()),
            delay,
            ..FixtureState::default()
        });
        let router = Router::new()
            .route(
                "/v1/tools/search",
                post(
                    async |State(state): State<Arc<FixtureState>>,
                           headers: HeaderMap,
                           Json(body): Json<Value>| {
                        state
                            .requests
                            .lock()
                            .expect("requests lock")
                            .push((headers, body));
                        tokio::time::sleep(state.delay).await;
                        state
                            .responses
                            .lock()
                            .expect("responses lock")
                            .pop_front()
                            .expect("unexpected retry")
                    },
                ),
            )
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}/v1/tools/search", listener.local_addr()?);
        let client = DeepSeekClient::with_endpoint(
            &DeepSeekConfig {
                api_key: " deepseek-test-secret ".to_owned(),
                timeout_seconds: timeout,
            },
            &endpoint,
        )?
        .expect("configured client");
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("fixture server");
        });
        Ok(Fixture {
            state,
            client,
            task,
        })
    }

    #[tokio::test]
    async fn search_posts_documented_contract_and_returns_sources_through_provider_trait()
    -> TestResult {
        let fixture = fixture(vec![(StatusCode::OK, RESULT.to_owned())], Duration::ZERO, 5).await?;
        let result =
            WebSearchProvider::search(&fixture.client, " DeepSeek site:example.com lang:ru ")
                .await?;
        let sources: Value = serde_json::from_str(&result)?;
        assert_eq!(sources["results"][0]["title"], "Source title");
        assert_eq!(sources["results"][0]["url"], "https://example.com/article");
        assert_eq!(sources["results"][0]["snippet"], "Short snippet");
        assert_eq!(sources["results"][0]["summary"], "Longer source summary");
        assert_eq!(sources["results"][0]["authority"], 3);
        assert_eq!(sources["results"][0]["rerank_score"], 2.4);
        let requests = fixture.state.requests.lock().expect("requests lock");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].0["authorization"],
            "Bearer deepseek-test-secret"
        );
        assert_eq!(requests[0].0["content-type"], "application/json");
        assert_eq!(
            requests[0].1,
            json!({"query":"DeepSeek site:example.com lang:ru", "count":10, "need_content":false})
        );
        Ok(())
    }

    #[tokio::test]
    async fn query_limit_counts_unicode_characters_and_rejects_invalid_queries_locally()
    -> TestResult {
        let fixture = fixture(
            vec![(StatusCode::OK, "{\"results\":[]}".to_owned())],
            Duration::ZERO,
            5,
        )
        .await?;
        for query in ["".to_owned(), " \n ".to_owned(), "я".repeat(129)] {
            assert!(matches!(
                fixture.client.search(&query).await,
                Err(DeepSeekError::InvalidQuery)
            ));
        }
        assert!(
            fixture
                .state
                .requests
                .lock()
                .expect("requests lock")
                .is_empty()
        );
        assert_eq!(
            fixture.client.search(&"я".repeat(128)).await?,
            "{\"results\":[]}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn retries_rate_limit_and_server_errors_then_returns_results() -> TestResult {
        let fixture = fixture(
            vec![
                (StatusCode::SERVICE_UNAVAILABLE, String::new()),
                (StatusCode::TOO_MANY_REQUESTS, String::new()),
                (StatusCode::OK, RESULT.to_owned()),
            ],
            Duration::ZERO,
            10,
        )
        .await?;
        assert!(
            fixture
                .client
                .search("query")
                .await?
                .contains("Source title")
        );
        assert_eq!(
            fixture.state.requests.lock().expect("requests lock").len(),
            3
        );
        Ok(())
    }

    #[tokio::test]
    async fn does_not_retry_permanent_errors_or_expose_response_bodies() -> TestResult {
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::PAYMENT_REQUIRED,
            StatusCode::FORBIDDEN,
            StatusCode::FOUND,
        ] {
            let fixture = fixture(
                vec![(status, "deepseek-test-secret private error".to_owned())],
                Duration::ZERO,
                5,
            )
            .await?;
            let error = fixture
                .client
                .search("query")
                .await
                .expect_err("HTTP error");
            assert!(matches!(error, DeepSeekError::Status(actual) if actual == status));
            assert!(!error.to_string().contains("secret"));
            assert_eq!(
                fixture.state.requests.lock().expect("requests lock").len(),
                1
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn stops_after_three_retryable_failures() -> TestResult {
        let fixture = fixture(
            vec![(StatusCode::INTERNAL_SERVER_ERROR, String::new()); 3],
            Duration::ZERO,
            10,
        )
        .await?;
        assert!(matches!(
            fixture.client.search("query").await,
            Err(DeepSeekError::Status(StatusCode::INTERNAL_SERVER_ERROR))
        ));
        assert_eq!(
            fixture.state.requests.lock().expect("requests lock").len(),
            3
        );
        Ok(())
    }

    #[tokio::test]
    async fn rejects_malformed_and_oversized_success_responses() -> TestResult {
        for body in [
            "not JSON".to_owned(),
            "{}".to_owned(),
            "{\"results\":[{\"title\":\"missing URL\"}]}".to_owned(),
            " ".repeat(MAX_RESPONSE_BYTES + 1),
        ] {
            let fixture = fixture(vec![(StatusCode::OK, body)], Duration::ZERO, 5).await?;
            assert!(matches!(
                fixture.client.search("query").await,
                Err(DeepSeekError::InvalidResponse)
            ));
            assert_eq!(
                fixture.state.requests.lock().expect("requests lock").len(),
                1
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn total_timeout_bounds_retries_and_slow_responses() -> TestResult {
        let fixture = fixture(
            vec![(StatusCode::SERVICE_UNAVAILABLE, String::new())],
            Duration::ZERO,
            1,
        )
        .await?;
        assert!(matches!(
            fixture.client.search("query").await,
            Err(DeepSeekError::Timeout)
        ));
        assert_eq!(
            fixture.state.requests.lock().expect("requests lock").len(),
            1
        );
        let fixture = self::fixture(
            vec![(StatusCode::OK, RESULT.to_owned())],
            Duration::from_secs(2),
            1,
        )
        .await?;
        assert!(fixture.client.search("query").await.is_err());
        Ok(())
    }

    #[test]
    fn missing_key_disables_client_and_debug_hides_credentials() -> TestResult {
        assert!(
            DeepSeekClient::from_config(&DeepSeekConfig {
                api_key: " \n ".to_owned(),
                timeout_seconds: 30
            })?
            .is_none()
        );
        let client = DeepSeekClient::from_config(&DeepSeekConfig {
            api_key: "deepseek-test-secret".to_owned(),
            timeout_seconds: 30,
        })?
        .expect("configured client");
        assert!(!format!("{client:?}").contains("secret"));
        Ok(())
    }
}
