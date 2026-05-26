//! TinyFish web search/fetch adapter for dialog tools.

use std::{fmt, future::Future, pin::Pin, sync::Arc, time::Duration as StdDuration};

use base64::{Engine as _, engine::general_purpose};
use openplotva_config::{
    AppConfig, DEFAULT_TINYFISH_FETCH_BASE_URL, DEFAULT_TINYFISH_MAX_CONTENT_CHARS,
    DEFAULT_TINYFISH_MCP_OAUTH_TOKEN_URL, DEFAULT_TINYFISH_MCP_URL,
    DEFAULT_TINYFISH_SEARCH_BASE_URL, DEFAULT_TINYFISH_SEARCH_LANGUAGE,
    DEFAULT_TINYFISH_TIMEOUT_SECONDS, TinyFishConfig,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Mutex;
use url::Url;

use crate::dialog_tools::{CrawlUrlFuture, UrlCrawler, WebSearchFuture, WebSearchProvider};

const TINYFISH_MCP_RUNTIME_STATE_KEY: &str = "tinyfish.mcp.oauth_state";
const TINYFISH_MCP_REFRESH_SKEW: TimeDuration = TimeDuration::minutes(5);

type TinyFishStoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// App-setting store used by the TinyFish MCP OAuth runtime state path.
pub trait TinyFishRuntimeStore: Send + Sync {
    fn get_app_setting<'a>(&'a self, key: &'a str) -> TinyFishStoreFuture<'a, Option<String>>;

    fn upsert_app_setting<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> TinyFishStoreFuture<'a, ()>;
}

#[derive(Clone)]
pub struct PostgresTinyFishRuntimeStore {
    pool: sqlx::PgPool,
}

impl PostgresTinyFishRuntimeStore {
    /// Create a runtime state store using the shared Postgres pool.
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

impl TinyFishRuntimeStore for PostgresTinyFishRuntimeStore {
    fn get_app_setting<'a>(&'a self, key: &'a str) -> TinyFishStoreFuture<'a, Option<String>> {
        Box::pin(async move {
            sqlx::query_scalar::<_, String>("SELECT value FROM app_settings WHERE key = $1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn upsert_app_setting<'a>(
        &'a self,
        key: &'a str,
        value: &'a str,
    ) -> TinyFishStoreFuture<'a, ()> {
        Box::pin(async move {
            sqlx::query(
                "INSERT INTO app_settings (key, value, updated_at) \
                 VALUES ($1, $2, NOW()) \
                 ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
            )
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
        })
    }
}

#[derive(Clone)]
pub enum TinyFishClient {
    /// API-key REST mode.
    Rest(TinyFishRestClient),
    /// OAuth MCP mode.
    Mcp(TinyFishMcpClient),
}

impl TinyFishClient {
    /// Build TinyFish from app config plus optional runtime-state store.
    pub fn from_app_config(
        config: &AppConfig,
        runtime_store: Option<Arc<dyn TinyFishRuntimeStore>>,
    ) -> Result<Option<Self>, TinyFishError> {
        Self::from_config(&config.tinyfish, runtime_store)
    }

    /// Build TinyFish from config plus optional runtime-state store.
    pub fn from_config(
        config: &TinyFishConfig,
        runtime_store: Option<Arc<dyn TinyFishRuntimeStore>>,
    ) -> Result<Option<Self>, TinyFishError> {
        if let Some(rest) = TinyFishRestClient::from_config(config)? {
            return Ok(Some(Self::Rest(rest)));
        }
        TinyFishMcpClient::from_config(config, runtime_store).map(|client| client.map(Self::Mcp))
    }

    /// Bootstrap persisted MCP OAuth state and refresh near-expiry access tokens.
    pub async fn bootstrap_runtime_state(&self) -> Result<(), TinyFishError> {
        if let Self::Mcp(client) = self {
            client.bootstrap_runtime_state().await?;
        }
        Ok(())
    }

    /// Search via the configured TinyFish path.
    pub async fn search(&self, query: &str) -> Result<String, TinyFishError> {
        match self {
            Self::Rest(client) => client.search(query).await,
            Self::Mcp(client) => client.search(query).await,
        }
    }

    /// Fetch via the configured TinyFish path.
    pub async fn fetch(&self, fetch_url: &str) -> Result<String, TinyFishError> {
        match self {
            Self::Rest(client) => client.fetch(fetch_url).await,
            Self::Mcp(client) => client.fetch(fetch_url).await,
        }
    }
}

impl WebSearchProvider for TinyFishClient {
    fn search<'a>(&'a self, query: &'a str) -> WebSearchFuture<'a> {
        Box::pin(async move {
            self.search(query)
                .await
                .map_err(|error| Box::new(error) as openplotva_dialog::ToolboxError)
        })
    }
}

impl UrlCrawler for TinyFishClient {
    fn crawl<'a>(&'a self, url: &'a str) -> CrawlUrlFuture<'a> {
        Box::pin(async move {
            self.fetch(url)
                .await
                .map_err(|error| Box::new(error) as openplotva_dialog::ToolboxError)
        })
    }
}

#[derive(Clone)]
pub struct TinyFishRestClient {
    http: reqwest::Client,
    api_key: Arc<str>,
    search_base_url: Arc<str>,
    fetch_base_url: Arc<str>,
    search_location: Arc<str>,
    search_language: Arc<str>,
    fetch_format: Arc<str>,
    max_content_chars: i32,
}

impl fmt::Debug for TinyFishRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TinyFishRestClient")
            .field("search_base_url", &self.search_base_url)
            .field("fetch_base_url", &self.fetch_base_url)
            .field("search_location", &self.search_location)
            .field("search_language", &self.search_language)
            .field("fetch_format", &self.fetch_format)
            .field("max_content_chars", &self.max_content_chars)
            .finish_non_exhaustive()
    }
}

impl TinyFishRestClient {
    pub fn from_app_config(config: &AppConfig) -> Result<Option<Self>, TinyFishError> {
        Self::from_config(&config.tinyfish)
    }

    /// Build the REST client from TinyFish config.
    pub fn from_config(config: &TinyFishConfig) -> Result<Option<Self>, TinyFishError> {
        if !config.enabled {
            return Ok(None);
        }
        let api_key = config.api_key.trim();
        if api_key.is_empty() {
            return Ok(None);
        }
        let timeout_seconds = if config.timeout_seconds > 0 {
            config.timeout_seconds
        } else {
            DEFAULT_TINYFISH_TIMEOUT_SECONDS
        };
        let http = reqwest::Client::builder()
            .timeout(StdDuration::from_secs(timeout_seconds as u64))
            .build()
            .map_err(TinyFishError::HttpClient)?;
        Ok(Some(Self {
            http,
            api_key: Arc::from(api_key),
            search_base_url: Arc::from(trim_or_default(
                &config.search_base_url,
                DEFAULT_TINYFISH_SEARCH_BASE_URL,
            )),
            fetch_base_url: Arc::from(trim_or_default(
                &config.fetch_base_url,
                DEFAULT_TINYFISH_FETCH_BASE_URL,
            )),
            search_location: Arc::from(config.search_location.trim()),
            search_language: Arc::from(trim_or_default(
                &config.search_language,
                DEFAULT_TINYFISH_SEARCH_LANGUAGE,
            )),
            fetch_format: Arc::from(tinyfish_fetch_format(&config.fetch_format)),
            max_content_chars: tinyfish_max_content_chars(config.max_content_chars),
        }))
    }

    /// Run TinyFish search over the REST API.
    pub async fn search(&self, query: &str) -> Result<String, TinyFishError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(TinyFishError::EmptySearchQuery);
        }
        let mut endpoint =
            Url::parse(&self.search_base_url).map_err(TinyFishError::ParseSearchUrl)?;
        {
            let mut pairs = endpoint.query_pairs_mut();
            pairs.append_pair("query", query);
            if !self.search_location.is_empty() {
                pairs.append_pair("location", &self.search_location);
            }
            if !self.search_language.is_empty() {
                pairs.append_pair("language", &self.search_language);
            }
        }

        let body = self
            .send_rest(self.http.get(endpoint).header("X-API-Key", &*self.api_key))
            .await?;
        Ok(String::from_utf8_lossy(&body).trim().to_owned())
    }

    /// Fetch one URL through TinyFish REST and return extracted text.
    pub async fn fetch(&self, fetch_url: &str) -> Result<String, TinyFishError> {
        let fetch_url = fetch_url.trim();
        if fetch_url.is_empty() {
            return Err(TinyFishError::EmptyUrl);
        }
        let body = self
            .send_rest(
                self.http
                    .post(&*self.fetch_base_url)
                    .header("X-API-Key", &*self.api_key)
                    .json(&TinyFishFetchRequest {
                        urls: vec![fetch_url],
                        format: &self.fetch_format,
                    }),
            )
            .await?;

        let response: TinyFishFetchResponse =
            serde_json::from_slice(&body).map_err(TinyFishError::DecodeFetchResponse)?;
        let Some(first) = response.results.first() else {
            if let Some(error) = response.errors.first() {
                return Err(TinyFishError::message(format!(
                    "tinyfish fetch failed for {}: {}",
                    error.url, error.error
                )));
            }
            return Err(TinyFishError::message(
                "tinyfish fetch returned no results".to_owned(),
            ));
        };

        let text = decode_tinyfish_text(first.text.as_ref())?;
        let text = text.trim();
        if text.is_empty() {
            if let Some(error) = response.errors.first() {
                return Err(TinyFishError::message(format!(
                    "tinyfish fetch failed for {}: {}",
                    error.url, error.error
                )));
            }
            return Err(TinyFishError::message(
                "tinyfish fetch returned empty content".to_owned(),
            ));
        }
        Ok(truncate_tinyfish_text(text, self.max_content_chars))
    }

    async fn send_rest(&self, request: reqwest::RequestBuilder) -> Result<Vec<u8>, TinyFishError> {
        let response = request.send().await.map_err(TinyFishError::Request)?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(TinyFishError::ReadResponseBody)?
            .to_vec();
        if status.is_client_error() || status.is_server_error() {
            return Err(format_tinyfish_http_error(status.as_u16(), &body));
        }
        Ok(body)
    }
}

#[derive(Clone)]
pub struct TinyFishMcpClient {
    http: reqwest::Client,
    mcp_url: Arc<str>,
    search_location: Arc<str>,
    search_language: Arc<str>,
    fetch_format: Arc<str>,
    max_content_chars: i32,
    runtime_store: Option<Arc<dyn TinyFishRuntimeStore>>,
    runtime_enabled: bool,
    now: Arc<dyn Fn() -> OffsetDateTime + Send + Sync>,
    state: Arc<Mutex<TinyFishMcpState>>,
}

impl fmt::Debug for TinyFishMcpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TinyFishMcpClient")
            .field("mcp_url", &self.mcp_url)
            .field("search_location", &self.search_location)
            .field("search_language", &self.search_language)
            .field("fetch_format", &self.fetch_format)
            .field("max_content_chars", &self.max_content_chars)
            .field("runtime_enabled", &self.runtime_enabled)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default)]
struct TinyFishMcpState {
    access_token: String,
    access_token_expires_at: Option<OffsetDateTime>,
    refresh_token: String,
    oauth_client_id: String,
    oauth_token_url: String,
    search_tool_name: String,
    fetch_tool_name: String,
    runtime_state_loaded: bool,
    initialized: bool,
    session_id: String,
    next_id: i64,
    tools: Vec<TinyFishMcpTool>,
}

impl TinyFishMcpClient {
    /// Build the MCP/OAuth client from config.
    pub fn from_config(
        config: &TinyFishConfig,
        runtime_store: Option<Arc<dyn TinyFishRuntimeStore>>,
    ) -> Result<Option<Self>, TinyFishError> {
        if !config.enabled {
            return Ok(None);
        }
        let access_token = config.mcp_access_token.trim().to_owned();
        let refresh_token = config.mcp_refresh_token.trim().to_owned();
        let oauth_client_id = config.mcp_oauth_client_id.trim().to_owned();
        let runtime_seed =
            !access_token.is_empty() || !refresh_token.is_empty() || !oauth_client_id.is_empty();
        if !runtime_seed && runtime_store.is_none() {
            return Ok(None);
        }
        let timeout_seconds = if config.timeout_seconds > 0 {
            config.timeout_seconds
        } else {
            DEFAULT_TINYFISH_TIMEOUT_SECONDS
        };
        let http = reqwest::Client::builder()
            .timeout(StdDuration::from_secs(timeout_seconds as u64))
            .build()
            .map_err(TinyFishError::HttpClient)?;
        let state = TinyFishMcpState {
            access_token_expires_at: tinyfish_jwt_expires_at(&access_token),
            access_token,
            refresh_token,
            oauth_client_id,
            oauth_token_url: trim_or_default(
                &config.mcp_oauth_token_url,
                DEFAULT_TINYFISH_MCP_OAUTH_TOKEN_URL,
            ),
            search_tool_name: config.mcp_search_tool_name.trim().to_owned(),
            fetch_tool_name: config.mcp_fetch_tool_name.trim().to_owned(),
            ..TinyFishMcpState::default()
        };
        Ok(Some(Self {
            http,
            mcp_url: Arc::from(trim_or_default(&config.mcp_url, DEFAULT_TINYFISH_MCP_URL)),
            search_location: Arc::from(config.search_location.trim()),
            search_language: Arc::from(trim_or_default(
                &config.search_language,
                DEFAULT_TINYFISH_SEARCH_LANGUAGE,
            )),
            fetch_format: Arc::from(tinyfish_fetch_format(&config.fetch_format)),
            max_content_chars: tinyfish_max_content_chars(config.max_content_chars),
            runtime_enabled: runtime_seed || runtime_store.is_some(),
            runtime_store,
            now: Arc::new(OffsetDateTime::now_utc),
            state: Arc::new(Mutex::new(state)),
        }))
    }

    #[cfg(test)]
    fn with_now(mut self, now: OffsetDateTime) -> Self {
        self.now = Arc::new(move || now);
        self
    }

    /// Bootstrap persisted MCP OAuth state and refresh near-expiry access tokens.
    pub async fn bootstrap_runtime_state(&self) -> Result<(), TinyFishError> {
        let mut state = self.state.lock().await;
        self.bootstrap_runtime_state_locked(&mut state).await?;
        if state.has_auth_material() && self.should_refresh_access_token_locked(&mut state) {
            self.refresh_mcp_token_locked(&mut state).await?;
        }
        Ok(())
    }

    /// Search via TinyFish MCP.
    pub async fn search(&self, query: &str) -> Result<String, TinyFishError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(TinyFishError::EmptySearchQuery);
        }
        if !self.mcp_available().await? {
            return Err(TinyFishError::Unavailable);
        }
        let (name, arguments) = self.resolve_mcp_tool_and_args("search", query).await?;
        self.call_mcp_tool(&name, arguments).await
    }

    /// Fetch one URL via TinyFish MCP.
    pub async fn fetch(&self, fetch_url: &str) -> Result<String, TinyFishError> {
        let fetch_url = fetch_url.trim();
        if fetch_url.is_empty() {
            return Err(TinyFishError::EmptyUrl);
        }
        if !self.mcp_available().await? {
            return Err(TinyFishError::Unavailable);
        }
        let (name, arguments) = self.resolve_mcp_tool_and_args("fetch", fetch_url).await?;
        self.call_mcp_tool(&name, arguments).await
    }

    async fn mcp_available(&self) -> Result<bool, TinyFishError> {
        let mut state = self.state.lock().await;
        self.bootstrap_runtime_state_locked(&mut state).await?;
        Ok(state.has_auth_material())
    }

    async fn resolve_mcp_tool_and_args(
        &self,
        kind: &str,
        value: &str,
    ) -> Result<(String, serde_json::Map<String, Value>), TinyFishError> {
        let mut state = self.state.lock().await;
        self.ensure_mcp_ready_locked(&mut state).await?;
        if state.tools.is_empty() {
            let result: TinyFishMcpToolsListResult =
                self.mcp_rpc_locked(&mut state, "tools/list", None).await?;
            state.tools = result.tools;
        }
        let preferred = if kind == "search" {
            state.search_tool_name.as_str()
        } else {
            state.fetch_tool_name.as_str()
        };
        let tool = select_tinyfish_mcp_tool(&state.tools, kind, preferred)
            .ok_or_else(|| TinyFishError::message(format!("tinyfish MCP {kind} tool not found")))?;
        let arguments = self.build_mcp_arguments(&tool, kind, value);
        Ok((tool.name, arguments))
    }

    async fn call_mcp_tool(
        &self,
        name: &str,
        arguments: serde_json::Map<String, Value>,
    ) -> Result<String, TinyFishError> {
        let mut state = self.state.lock().await;
        self.ensure_mcp_ready_locked(&mut state).await?;
        let params = serde_json::json!({
            "name": name,
            "arguments": arguments,
        });
        let result: TinyFishMcpToolCallResult = self
            .mcp_rpc_locked(&mut state, "tools/call", Some(params))
            .await?;
        let mut text = tinyfish_mcp_result_text(&result).trim().to_owned();
        if result.is_error {
            if text.is_empty() {
                text = "tool returned an error".to_owned();
            }
            return Err(TinyFishError::message(text));
        }
        if text.is_empty() {
            return Err(TinyFishError::message(
                "tinyfish MCP returned empty content".to_owned(),
            ));
        }
        Ok(truncate_tinyfish_text(&text, self.max_content_chars))
    }

    async fn ensure_mcp_ready_locked(
        &self,
        state: &mut TinyFishMcpState,
    ) -> Result<(), TinyFishError> {
        self.bootstrap_runtime_state_locked(state).await?;
        if state.access_token.trim().is_empty() || self.should_refresh_access_token_locked(state) {
            self.refresh_mcp_token_locked(state).await?;
        }
        if state.initialized {
            return Ok(());
        }
        let params = serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "openplotva",
                "version": "0.1.0",
            },
        });
        let _: Value = self
            .mcp_rpc_locked(state, "initialize", Some(params))
            .await?;
        self.mcp_notify_locked(
            state,
            "notifications/initialized",
            Some(serde_json::json!({})),
        )
        .await?;
        state.initialized = true;
        Ok(())
    }

    async fn bootstrap_runtime_state_locked(
        &self,
        state: &mut TinyFishMcpState,
    ) -> Result<(), TinyFishError> {
        if state.runtime_state_loaded || self.runtime_store.is_none() || !self.runtime_enabled {
            state.runtime_state_loaded = true;
            return Ok(());
        }
        state.runtime_state_loaded = true;
        let Some(store) = self.runtime_store.as_ref() else {
            return Ok(());
        };
        match store
            .get_app_setting(TINYFISH_MCP_RUNTIME_STATE_KEY)
            .await
            .map_err(|error| {
                TinyFishError::message(format!("load tinyfish MCP runtime state: {error}"))
            })? {
            Some(value) if !value.trim().is_empty() => {
                apply_mcp_runtime_state(state, value.as_bytes())?;
            }
            _ if state.has_runtime_tokens() => {
                self.persist_mcp_runtime_state_locked(state).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn persist_mcp_runtime_state_locked(
        &self,
        state: &TinyFishMcpState,
    ) -> Result<(), TinyFishError> {
        if self.runtime_store.is_none() || !self.runtime_enabled || !state.has_runtime_tokens() {
            return Ok(());
        }
        let runtime_state = TinyFishMcpRuntimeState {
            version: 1,
            access_token: empty_string_as_none(state.access_token.trim()),
            refresh_token: empty_string_as_none(state.refresh_token.trim()),
            access_token_expires_at: state
                .access_token_expires_at
                .and_then(|expires_at| expires_at.format(&Rfc3339).ok()),
            oauth_client_id: empty_string_as_none(state.oauth_client_id.trim()),
            oauth_token_url: empty_string_as_none(state.oauth_token_url.trim()),
            mcp_url: Some(self.mcp_url.to_string()),
            search_tool_name: empty_string_as_none(state.search_tool_name.trim()),
            fetch_tool_name: empty_string_as_none(state.fetch_tool_name.trim()),
            updated_at: (self.now)()
                .format(&Rfc3339)
                .unwrap_or_else(|_| OffsetDateTime::now_utc().to_string()),
        };
        let body = serde_json::to_string(&runtime_state).map_err(|error| {
            TinyFishError::message(format!("encode tinyfish MCP runtime state: {error}"))
        })?;
        let Some(store) = self.runtime_store.as_ref() else {
            return Ok(());
        };
        store
            .upsert_app_setting(TINYFISH_MCP_RUNTIME_STATE_KEY, &body)
            .await
            .map_err(|error| {
                TinyFishError::message(format!("persist tinyfish MCP runtime state: {error}"))
            })?;
        Ok(())
    }

    fn should_refresh_access_token_locked(&self, state: &mut TinyFishMcpState) -> bool {
        if state.refresh_token.trim().is_empty() || state.oauth_client_id.trim().is_empty() {
            return false;
        }
        if state.access_token.trim().is_empty() {
            return true;
        }
        if state.access_token_expires_at.is_none() {
            state.access_token_expires_at = tinyfish_jwt_expires_at(&state.access_token);
        }
        let Some(expires_at) = state.access_token_expires_at else {
            return true;
        };
        (self.now)() + TINYFISH_MCP_REFRESH_SKEW >= expires_at
    }

    async fn refresh_mcp_token_locked(
        &self,
        state: &mut TinyFishMcpState,
    ) -> Result<(), TinyFishError> {
        if state.refresh_token.trim().is_empty() || state.oauth_client_id.trim().is_empty() {
            return Err(TinyFishError::message(
                "tinyfish MCP access token is empty".to_owned(),
            ));
        }
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("grant_type", "refresh_token")
            .append_pair("refresh_token", state.refresh_token.trim())
            .append_pair("client_id", state.oauth_client_id.trim())
            .finish();
        let response = self
            .http
            .post(state.oauth_token_url.trim())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await
            .map_err(TinyFishError::Request)?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(TinyFishError::ReadResponseBody)?
            .to_vec();
        if status.is_client_error() || status.is_server_error() {
            return Err(format_tinyfish_http_error(status.as_u16(), &body));
        }
        let token: TinyFishOAuthTokenResponse =
            serde_json::from_slice(&body).map_err(TinyFishError::DecodeOAuthTokenResponse)?;
        let access_token = token.access_token.trim();
        if access_token.is_empty() {
            return Err(TinyFishError::message(
                "tinyfish oauth token response missing access_token".to_owned(),
            ));
        }
        state.access_token = access_token.to_owned();
        state.access_token_expires_at = if token.expires_in > 0 {
            Some((self.now)() + TimeDuration::seconds(token.expires_in))
        } else {
            tinyfish_jwt_expires_at(access_token)
        };
        if !token.refresh_token.trim().is_empty() {
            state.refresh_token = token.refresh_token.trim().to_owned();
        }
        self.persist_mcp_runtime_state_locked(state).await
    }

    async fn mcp_rpc_locked<T: DeserializeOwned>(
        &self,
        state: &mut TinyFishMcpState,
        method: &str,
        params: Option<Value>,
    ) -> Result<T, TinyFishError> {
        state.next_id += 1;
        let req = TinyFishMcpRequest {
            jsonrpc: "2.0",
            id: Some(state.next_id),
            method,
            params,
        };
        let body = self.mcp_do_locked(state, &req, true).await?;
        let response: TinyFishMcpResponse =
            serde_json::from_slice(&body).map_err(TinyFishError::DecodeMcpResponse)?;
        if let Some(error) = response.error {
            return Err(TinyFishError::message(format!(
                "tinyfish MCP {method}: {}",
                error.message
            )));
        }
        let result = response.result.unwrap_or_else(|| serde_json::json!({}));
        serde_json::from_value(result).map_err(TinyFishError::DecodeMcpResult)
    }

    async fn mcp_notify_locked(
        &self,
        state: &mut TinyFishMcpState,
        method: &str,
        params: Option<Value>,
    ) -> Result<(), TinyFishError> {
        let req = TinyFishMcpRequest {
            jsonrpc: "2.0",
            id: None,
            method,
            params,
        };
        self.mcp_do_locked(state, &req, false).await?;
        Ok(())
    }

    async fn mcp_do_locked(
        &self,
        state: &mut TinyFishMcpState,
        request: &TinyFishMcpRequest<'_>,
        retry_auth: bool,
    ) -> Result<Vec<u8>, TinyFishError> {
        let response = self.mcp_send_locked(state, request).await?;
        if response.status_code == 401 && retry_auth && !state.refresh_token.trim().is_empty() {
            self.refresh_mcp_token_locked(state).await?;
            let response = self.mcp_send_locked(state, request).await?;
            return decode_tinyfish_mcp_http_response(
                response.status_code,
                &response.content_type,
                &response.body,
            );
        }
        decode_tinyfish_mcp_http_response(
            response.status_code,
            &response.content_type,
            &response.body,
        )
    }

    async fn mcp_send_locked(
        &self,
        state: &mut TinyFishMcpState,
        request: &TinyFishMcpRequest<'_>,
    ) -> Result<TinyFishMcpHttpResponse, TinyFishError> {
        let mut builder = self
            .http
            .post(&*self.mcp_url)
            .header(
                "Authorization",
                format!("Bearer {}", state.access_token.trim()),
            )
            .header("Accept", "application/json, text/event-stream")
            .header("Content-Type", "application/json")
            .json(request);
        if !state.session_id.trim().is_empty() {
            builder = builder.header("Mcp-Session-Id", state.session_id.trim());
        }
        let response = builder.send().await.map_err(TinyFishError::Request)?;
        if let Some(session_id) = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            state.session_id = session_id.to_owned();
        }
        let status_code = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let body = response
            .bytes()
            .await
            .map_err(TinyFishError::ReadResponseBody)?
            .to_vec();
        Ok(TinyFishMcpHttpResponse {
            status_code,
            content_type,
            body,
        })
    }

    fn build_mcp_arguments(
        &self,
        tool: &TinyFishMcpTool,
        kind: &str,
        value: &str,
    ) -> serde_json::Map<String, Value> {
        let mut args = serde_json::Map::new();
        match kind {
            "search" => {
                self.add_mcp_search_arguments(&mut args, &tool.input_schema.properties, value)
            }
            "fetch" => {
                self.add_mcp_fetch_arguments(&mut args, &tool.input_schema.properties, value)
            }
            _ => {}
        }
        args
    }

    fn add_mcp_search_arguments(
        &self,
        args: &mut serde_json::Map<String, Value>,
        props: &serde_json::Map<String, Value>,
        value: &str,
    ) {
        args.insert(
            tinyfish_mcp_first_property(props, "query", "q"),
            Value::String(value.to_owned()),
        );
        if !self.search_location.is_empty() && props.contains_key("location") {
            args.insert(
                "location".to_owned(),
                Value::String(self.search_location.to_string()),
            );
        }
        if !self.search_language.is_empty() && props.contains_key("language") {
            args.insert(
                "language".to_owned(),
                Value::String(self.search_language.to_string()),
            );
        }
    }

    fn add_mcp_fetch_arguments(
        &self,
        args: &mut serde_json::Map<String, Value>,
        props: &serde_json::Map<String, Value>,
        value: &str,
    ) {
        let key = tinyfish_mcp_first_property(props, "urls", "url");
        if key == "urls" {
            args.insert(key, serde_json::json!([value]));
        } else {
            args.insert(key, Value::String(value.to_owned()));
        }
        if !self.fetch_format.is_empty() && props.contains_key("format") {
            args.insert(
                "format".to_owned(),
                Value::String(self.fetch_format.to_string()),
            );
        }
        if props.contains_key("links") {
            args.insert("links".to_owned(), Value::Bool(false));
        }
        if props.contains_key("image_links") {
            args.insert("image_links".to_owned(), Value::Bool(false));
        }
    }
}

impl TinyFishMcpState {
    fn has_runtime_tokens(&self) -> bool {
        !self.access_token.trim().is_empty() || !self.refresh_token.trim().is_empty()
    }

    fn has_auth_material(&self) -> bool {
        !self.access_token.trim().is_empty()
            || (!self.refresh_token.trim().is_empty() && !self.oauth_client_id.trim().is_empty())
    }
}

impl WebSearchProvider for TinyFishRestClient {
    fn search<'a>(&'a self, query: &'a str) -> WebSearchFuture<'a> {
        Box::pin(async move {
            self.search(query)
                .await
                .map_err(|error| Box::new(error) as openplotva_dialog::ToolboxError)
        })
    }
}

impl UrlCrawler for TinyFishRestClient {
    fn crawl<'a>(&'a self, url: &'a str) -> CrawlUrlFuture<'a> {
        Box::pin(async move {
            self.fetch(url)
                .await
                .map_err(|error| Box::new(error) as openplotva_dialog::ToolboxError)
        })
    }
}

/// TinyFish adapter failures surfaced through dialog tool failed results.
#[derive(Debug, Error)]
pub enum TinyFishError {
    #[error("search query is empty")]
    EmptySearchQuery,
    #[error("url is empty")]
    EmptyUrl,
    /// Reqwest client could not be built.
    #[error("{0}")]
    HttpClient(reqwest::Error),
    /// Search URL could not be parsed.
    #[error("parse tinyfish search url: {0}")]
    ParseSearchUrl(url::ParseError),
    /// TinyFish request failed.
    #[error("{0}")]
    Request(reqwest::Error),
    /// TinyFish response body could not be read.
    #[error("{0}")]
    ReadResponseBody(reqwest::Error),
    /// TinyFish fetch JSON response could not be decoded.
    #[error("decode tinyfish fetch response: {0}")]
    DecodeFetchResponse(serde_json::Error),
    /// TinyFish JSON-RPC response could not be decoded.
    #[error("decode tinyfish MCP response: {0}")]
    DecodeMcpResponse(serde_json::Error),
    /// TinyFish JSON-RPC result could not be decoded.
    #[error("decode tinyfish MCP result: {0}")]
    DecodeMcpResult(serde_json::Error),
    /// TinyFish OAuth token response could not be decoded.
    #[error("decode tinyfish oauth token response: {0}")]
    DecodeOAuthTokenResponse(serde_json::Error),
    /// TinyFish JSON text field could not be decoded.
    #[error("decode tinyfish text: {0}")]
    DecodeText(serde_json::Error),
    /// TinyFish is configured but has neither REST API key nor MCP auth material.
    #[error("tinyfish unavailable")]
    Unavailable,
    #[error("{0}")]
    Message(String),
}

impl TinyFishError {
    fn message(message: String) -> Self {
        Self::Message(message)
    }
}

#[derive(Debug, Serialize)]
struct TinyFishFetchRequest<'a> {
    urls: Vec<&'a str>,
    format: &'a str,
}

#[derive(Debug, Deserialize)]
struct TinyFishFetchResponse {
    #[serde(default)]
    results: Vec<TinyFishFetchResult>,
    #[serde(default)]
    errors: Vec<TinyFishFetchError>,
}

#[derive(Debug, Deserialize)]
struct TinyFishFetchResult {
    text: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct TinyFishFetchError {
    url: String,
    error: String,
}

#[derive(Debug, Deserialize)]
struct TinyFishApiErrorResponse {
    error: TinyFishApiErrorBody,
}

#[derive(Debug, Deserialize)]
struct TinyFishApiErrorBody {
    #[serde(default)]
    code: String,
    #[serde(default)]
    message: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TinyFishMcpRuntimeState {
    #[serde(default)]
    version: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    access_token_expires_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oauth_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oauth_token_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mcp_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    search_tool_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch_tool_name: Option<String>,
    #[serde(default)]
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct TinyFishMcpRequest<'a> {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<i64>,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct TinyFishMcpResponse {
    result: Option<Value>,
    error: Option<TinyFishMcpError>,
}

#[derive(Debug, Deserialize)]
struct TinyFishMcpError {
    message: String,
}

#[derive(Debug, Clone, Deserialize)]
struct TinyFishMcpTool {
    name: String,
    #[serde(default, rename = "inputSchema")]
    input_schema: TinyFishMcpSchema,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct TinyFishMcpSchema {
    #[serde(default)]
    properties: serde_json::Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct TinyFishMcpToolsListResult {
    #[serde(default)]
    tools: Vec<TinyFishMcpTool>,
}

#[derive(Debug, Deserialize)]
struct TinyFishMcpToolCallResult {
    #[serde(default)]
    content: Vec<TinyFishMcpToolContent>,
    #[serde(default, rename = "structuredContent")]
    structured_content: Option<Value>,
    #[serde(default, rename = "isError")]
    is_error: bool,
}

#[derive(Debug, Deserialize)]
struct TinyFishMcpToolContent {
    #[serde(default, rename = "type")]
    content_type: String,
    #[serde(default)]
    text: String,
}

#[derive(Debug, Deserialize)]
struct TinyFishOAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(default)]
    expires_in: i64,
}

#[derive(Debug)]
struct TinyFishMcpHttpResponse {
    status_code: u16,
    content_type: String,
    body: Vec<u8>,
}

fn trim_or_default(value: &str, default: &'static str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        default.to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn empty_string_as_none(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn tinyfish_fetch_format(format: &str) -> String {
    let trimmed = format.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("markdown") {
        "markdown".to_owned()
    } else if trimmed.eq_ignore_ascii_case("html") {
        "html".to_owned()
    } else if trimmed.eq_ignore_ascii_case("json") {
        "json".to_owned()
    } else {
        trimmed.to_ascii_lowercase()
    }
}

fn tinyfish_max_content_chars(max_content_chars: i32) -> i32 {
    if max_content_chars == 0 {
        DEFAULT_TINYFISH_MAX_CONTENT_CHARS
    } else {
        max_content_chars
    }
}

fn apply_mcp_runtime_state(state: &mut TinyFishMcpState, raw: &[u8]) -> Result<(), TinyFishError> {
    let runtime_state: TinyFishMcpRuntimeState = serde_json::from_slice(raw).map_err(|error| {
        TinyFishError::message(format!("decode tinyfish MCP runtime state: {error}"))
    })?;
    if let Some(access_token) = runtime_state
        .access_token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        state.access_token = access_token.to_owned();
        state.access_token_expires_at = runtime_state
            .access_token_expires_at
            .as_deref()
            .and_then(parse_tinyfish_time)
            .or_else(|| tinyfish_jwt_expires_at(access_token));
    }
    assign_trimmed_if_not_empty(&mut state.refresh_token, runtime_state.refresh_token);
    assign_trimmed_if_not_empty(&mut state.oauth_client_id, runtime_state.oauth_client_id);
    assign_trimmed_if_not_empty(&mut state.oauth_token_url, runtime_state.oauth_token_url);
    assign_trimmed_if_empty(&mut state.search_tool_name, runtime_state.search_tool_name);
    assign_trimmed_if_empty(&mut state.fetch_tool_name, runtime_state.fetch_tool_name);
    Ok(())
}

fn assign_trimmed_if_not_empty(target: &mut String, value: Option<String>) {
    if let Some(value) = value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        *target = value;
    }
}

fn assign_trimmed_if_empty(target: &mut String, value: Option<String>) {
    if target.trim().is_empty() {
        assign_trimmed_if_not_empty(target, value);
    }
}

fn parse_tinyfish_time(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value.trim(), &Rfc3339).ok()
}

fn tinyfish_jwt_expires_at(token: &str) -> Option<OffsetDateTime> {
    let mut parts = token.trim().split('.');
    parts.next()?;
    let payload = parts.next()?.trim();
    if payload.is_empty() {
        return None;
    }
    let decoded = general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims: Value = serde_json::from_slice(&decoded).ok()?;
    let exp = claims.get("exp")?.as_i64()?;
    OffsetDateTime::from_unix_timestamp(exp).ok()
}

fn select_tinyfish_mcp_tool(
    tools: &[TinyFishMcpTool],
    kind: &str,
    preferred: &str,
) -> Option<TinyFishMcpTool> {
    preferred_tinyfish_mcp_tool(tools, preferred)
        .or_else(|| candidate_tinyfish_mcp_tool(tools, tinyfish_mcp_tool_candidates(kind)))
        .or_else(|| fallback_tinyfish_mcp_tool(tools, kind))
}

fn preferred_tinyfish_mcp_tool(
    tools: &[TinyFishMcpTool],
    preferred: &str,
) -> Option<TinyFishMcpTool> {
    let preferred = preferred.trim();
    if preferred.is_empty() {
        return None;
    }
    tools
        .iter()
        .find(|tool| tool.name.eq_ignore_ascii_case(preferred))
        .cloned()
}

fn tinyfish_mcp_tool_candidates(kind: &str) -> &'static [&'static str] {
    match kind {
        "search" => &["search", "websearch", "searchweb", "tinyfishsearch"],
        "fetch" => &[
            "fetch",
            "webfetch",
            "fetchurl",
            "fetchurls",
            "tinyfishfetch",
            "crawlurl",
        ],
        _ => &[],
    }
}

fn candidate_tinyfish_mcp_tool(
    tools: &[TinyFishMcpTool],
    candidates: &[&str],
) -> Option<TinyFishMcpTool> {
    candidates.iter().find_map(|candidate| {
        tools
            .iter()
            .find(|tool| tinyfish_tool_name_matches_normalized(&tool.name, candidate))
            .cloned()
    })
}

fn fallback_tinyfish_mcp_tool(tools: &[TinyFishMcpTool], kind: &str) -> Option<TinyFishMcpTool> {
    tools
        .iter()
        .find(|tool| {
            contains_ascii_fold(&tool.name, kind) && !contains_ascii_fold(&tool.name, "usage")
        })
        .cloned()
}

fn tinyfish_tool_name_matches_normalized(name: &str, normalized: &str) -> bool {
    let mut expected = normalized.bytes();
    for mut byte in name.trim().bytes() {
        if matches!(byte, b'_' | b'-' | b' ') {
            continue;
        }
        if byte.is_ascii_uppercase() {
            byte = byte.to_ascii_lowercase();
        }
        if Some(byte) != expected.next() {
            return false;
        }
    }
    expected.next().is_none()
}

fn contains_ascii_fold(haystack: &str, needle: &str) -> bool {
    haystack
        .to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

fn tinyfish_mcp_first_property(
    props: &serde_json::Map<String, Value>,
    primary: &str,
    secondary: &str,
) -> String {
    if props.contains_key(primary) {
        primary.to_owned()
    } else if props.contains_key(secondary) {
        secondary.to_owned()
    } else {
        first_tinyfish_mcp_string_property(props, primary)
    }
}

fn first_tinyfish_mcp_string_property(
    props: &serde_json::Map<String, Value>,
    fallback: &str,
) -> String {
    props
        .iter()
        .find_map(|(name, schema)| {
            (schema
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|value| value == "string"))
            .then(|| name.clone())
        })
        .unwrap_or_else(|| fallback.to_owned())
}

fn tinyfish_mcp_result_text(result: &TinyFishMcpToolCallResult) -> String {
    let parts: Vec<String> = result
        .content
        .iter()
        .filter(|item| item.content_type.eq_ignore_ascii_case("text"))
        .map(|item| item.text.trim())
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
        .collect();
    if !parts.is_empty() {
        return parts.join("\n\n");
    }
    result
        .structured_content
        .as_ref()
        .map(|value| value.to_string())
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn decode_tinyfish_mcp_http_response(
    status_code: u16,
    content_type: &str,
    body: &[u8],
) -> Result<Vec<u8>, TinyFishError> {
    if status_code == 202 || status_code == 204 {
        return Ok(br#"{"jsonrpc":"2.0","result":{}}"#.to_vec());
    }
    if status_code >= 400 {
        return Err(format_tinyfish_http_error(status_code, body));
    }
    decode_tinyfish_mcp_body(body, content_type)
}

fn decode_tinyfish_mcp_body(body: &[u8], content_type: &str) -> Result<Vec<u8>, TinyFishError> {
    if !is_tinyfish_mcp_sse_body(body, content_type) {
        return Ok(body.to_vec());
    }
    tinyfish_mcp_data_payload(body)
        .ok_or_else(|| TinyFishError::message("tinyfish MCP SSE response had no data".to_owned()))
}

fn is_tinyfish_mcp_sse_body(body: &[u8], content_type: &str) -> bool {
    let media_type = content_type.split(';').next().unwrap_or_default().trim();
    let trimmed = trim_ascii_whitespace(body);
    media_type.eq_ignore_ascii_case("text/event-stream")
        || trimmed.starts_with(b"event:")
        || trimmed.starts_with(b"data:")
}

fn tinyfish_mcp_data_payload(mut body: &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    let mut found = false;
    while !body.is_empty() {
        let (line, rest) = match body.iter().position(|byte| *byte == b'\n') {
            Some(index) => (&body[..index], &body[index + 1..]),
            None => (body, &[][..]),
        };
        body = rest;
        let line = trim_ascii_whitespace(line);
        if let Some(data) = line.strip_prefix(b"data:") {
            let data = trim_ascii_whitespace(data);
            if found {
                out.push(b'\n');
            }
            out.extend_from_slice(data);
            found = true;
            continue;
        }
        if line.is_empty() && found {
            break;
        }
    }
    found.then_some(out)
}

fn trim_ascii_whitespace(mut bytes: &[u8]) -> &[u8] {
    while bytes.first().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[1..];
    }
    while bytes.last().is_some_and(u8::is_ascii_whitespace) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn decode_tinyfish_text(raw: Option<&Value>) -> Result<String, TinyFishError> {
    match raw {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(value) => serde_json::to_string(value).map_err(TinyFishError::DecodeText),
    }
}

fn format_tinyfish_http_error(status_code: u16, body: &[u8]) -> TinyFishError {
    if let Ok(api_error) = serde_json::from_slice::<TinyFishApiErrorResponse>(body) {
        let code = api_error.error.code.trim();
        let message = api_error.error.message.trim();
        if !code.is_empty() || !message.is_empty() {
            if code.is_empty() {
                return TinyFishError::message(format!("tinyfish http {status_code}: {message}"));
            }
            if message.is_empty() {
                return TinyFishError::message(format!("tinyfish http {status_code}: {code}"));
            }
            return TinyFishError::message(format!(
                "tinyfish http {status_code}: {code}: {message}"
            ));
        }
    }
    let trimmed = String::from_utf8_lossy(body).trim().to_owned();
    if trimmed.is_empty() {
        TinyFishError::message(format!("tinyfish http {status_code}"))
    } else {
        TinyFishError::message(format!(
            "tinyfish http {status_code}: {}",
            truncate_tinyfish_text(&trimmed, 300)
        ))
    }
}

fn truncate_tinyfish_text(text: &str, max_chars: i32) -> String {
    if max_chars <= 0 {
        return text.to_owned();
    }
    let max_chars = max_chars as usize;
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    text.chars()
        .take(max_chars)
        .collect::<String>()
        .trim()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        body::Bytes,
        extract::{Query, State},
        http::{HeaderMap, HeaderName, StatusCode, header},
        response::IntoResponse,
        routing::{any, get, post},
    };
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use openplotva_config::{AppConfig, RawConfig};
    use serde_json::json;
    use std::{collections::BTreeMap, error::Error};
    use tokio::{net::TcpListener, sync::Mutex};

    #[derive(Debug, Default)]
    struct FixtureState {
        search: Mutex<Vec<SearchRequest>>,
        fetch: Mutex<Vec<FetchRequest>>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct SearchRequest {
        api_key: String,
        query: BTreeMap<String, String>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct FetchRequest {
        api_key: String,
        content_type: String,
        body: Value,
    }

    #[tokio::test]
    async fn rest_search_uses_go_query_headers_and_trims_body() -> Result<(), Box<dyn Error>> {
        let fixture = spawn_fixture_server().await?;
        let config = AppConfig::from_raw(RawConfig {
            tinyfish_api_key: Some("tf_test_key".to_owned()),
            tinyfish_search_base_url: Some(format!("{}/search", fixture.base_url)),
            tinyfish_fetch_base_url: Some(format!("{}/fetch", fixture.base_url)),
            tinyfish_search_location: Some("Warsaw".to_owned()),
            tinyfish_search_language: Some("ru".to_owned()),
            tinyfish_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = TinyFishRestClient::from_app_config(&config)?
            .ok_or_else(|| std::io::Error::other("TinyFish client was not built"))?;

        let result = client.search(" latest go ").await?;

        assert_eq!(result, r#"{"results":["ok"]}"#);
        assert_eq!(
            fixture.state.search.lock().await.as_slice(),
            &[SearchRequest {
                api_key: "tf_test_key".to_owned(),
                query: BTreeMap::from([
                    ("language".to_owned(), "ru".to_owned()),
                    ("location".to_owned(), "Warsaw".to_owned()),
                    ("query".to_owned(), "latest go".to_owned()),
                ]),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn rest_fetch_posts_go_payload_extracts_text_and_truncates() -> Result<(), Box<dyn Error>>
    {
        let fixture = spawn_fixture_server().await?;
        let config = AppConfig::from_raw(RawConfig {
            tinyfish_api_key: Some("tf_test_key".to_owned()),
            tinyfish_search_base_url: Some(format!("{}/search", fixture.base_url)),
            tinyfish_fetch_base_url: Some(format!("{}/fetch", fixture.base_url)),
            tinyfish_fetch_format: Some("markdown".to_owned()),
            tinyfish_max_content_chars: Some("9".to_owned()),
            tinyfish_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = TinyFishRestClient::from_app_config(&config)?
            .ok_or_else(|| std::io::Error::other("TinyFish client was not built"))?;

        let result = client.fetch(" https://example.com/page ").await?;

        assert_eq!(result, "# Example");
        assert_eq!(
            fixture.state.fetch.lock().await.as_slice(),
            &[FetchRequest {
                api_key: "tf_test_key".to_owned(),
                content_type: "application/json".to_owned(),
                body: json!({
                    "urls": ["https://example.com/page"],
                    "format": "markdown",
                }),
            }]
        );
        Ok(())
    }

    #[test]
    fn fetch_text_decodes_json_and_go_http_errors() -> Result<(), TinyFishError> {
        assert_eq!(
            decode_tinyfish_text(Some(&json!({"a": 1, "b": [true]})))?,
            r#"{"a":1,"b":[true]}"#
        );
        assert_eq!(
            format_tinyfish_http_error(
                429,
                br#"{"error":{"code":"rate_limit","message":"slow down"}}"#
            )
            .to_string(),
            "tinyfish http 429: rate_limit: slow down"
        );
        assert_eq!(
            format_tinyfish_http_error(500, b" server down ").to_string(),
            "tinyfish http 500: server down"
        );
        Ok(())
    }

    #[test]
    fn disabled_or_api_keyless_config_leaves_tool_unwired() -> Result<(), Box<dyn Error>> {
        let disabled = AppConfig::from_raw(RawConfig {
            tinyfish_enabled: Some("false".to_owned()),
            ..RawConfig::default()
        })?;
        let missing_key = AppConfig::from_raw(RawConfig::default())?;

        assert!(TinyFishRestClient::from_app_config(&disabled)?.is_none());
        assert!(TinyFishRestClient::from_app_config(&missing_key)?.is_none());
        assert!(TinyFishClient::from_app_config(&missing_key, None)?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn mcp_search_discovers_tool_uses_bearer_and_go_arguments() -> Result<(), Box<dyn Error>>
    {
        let fixture = spawn_mcp_fixture_server(None).await?;
        let config = AppConfig::from_raw(RawConfig {
            tinyfish_api_key: Some(String::new()),
            tinyfish_mcp_url: Some(format!("{}/mcp", fixture.base_url)),
            tinyfish_mcp_access_token: Some("oauth_access".to_owned()),
            tinyfish_search_location: Some("Warsaw".to_owned()),
            tinyfish_search_language: Some("ru".to_owned()),
            tinyfish_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = TinyFishClient::from_app_config(&config, None)?
            .ok_or_else(|| std::io::Error::other("TinyFish MCP client was not built"))?;

        let result = client.search(" current weather ").await?;

        assert_eq!(result, "search ok");
        let calls = fixture.state.mcp_calls.lock().await;
        assert_eq!(
            calls
                .iter()
                .map(|call| call.method.as_str())
                .collect::<Vec<_>>(),
            vec![
                "initialize",
                "notifications/initialized",
                "tools/list",
                "tools/call"
            ]
        );
        assert!(
            calls
                .iter()
                .all(|call| call.authorization == "Bearer oauth_access")
        );
        assert_eq!(calls[0].session_id, "");
        assert_eq!(calls[1].session_id, "session-1");
        let params = calls
            .last()
            .and_then(|call| call.params.as_ref())
            .ok_or_else(|| std::io::Error::other("missing tools/call params"))?;
        assert_eq!(params["name"], "Tiny_Fish-Search");
        assert_eq!(params["arguments"]["q"], "current weather");
        assert_eq!(params["arguments"]["location"], "Warsaw");
        assert_eq!(params["arguments"]["language"], "ru");
        Ok(())
    }

    #[tokio::test]
    async fn mcp_fetch_maps_schema_and_decodes_sse_payload() -> Result<(), Box<dyn Error>> {
        let fixture = spawn_mcp_fixture_server(None).await?;
        let config = AppConfig::from_raw(RawConfig {
            tinyfish_api_key: Some(String::new()),
            tinyfish_mcp_url: Some(format!("{}/mcp", fixture.base_url)),
            tinyfish_mcp_access_token: Some("oauth_access".to_owned()),
            tinyfish_fetch_format: Some("markdown".to_owned()),
            tinyfish_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = TinyFishClient::from_app_config(&config, None)?
            .ok_or_else(|| std::io::Error::other("TinyFish MCP client was not built"))?;

        let result = client.fetch(" https://example.com/page ").await?;

        assert_eq!(result, "# fetched");
        let calls = fixture.state.mcp_calls.lock().await;
        let params = calls
            .last()
            .and_then(|call| call.params.as_ref())
            .ok_or_else(|| std::io::Error::other("missing tools/call params"))?;
        assert_eq!(params["name"], "tinyfish_fetch");
        assert_eq!(
            params["arguments"]["urls"],
            json!(["https://example.com/page"])
        );
        assert_eq!(params["arguments"]["format"], "markdown");
        assert_eq!(params["arguments"]["links"], false);
        assert_eq!(params["arguments"]["image_links"], false);
        Ok(())
    }

    #[tokio::test]
    async fn mcp_uses_persisted_runtime_state_over_seed() -> Result<(), Box<dyn Error>> {
        let fixture = spawn_mcp_fixture_server(None).await?;
        let store = Arc::new(MemoryTinyFishRuntimeStore::default());
        store
            .insert(
                TINYFISH_MCP_RUNTIME_STATE_KEY,
                json!({
                    "version": 1,
                    "access_token": "persisted_access",
                    "refresh_token": "persisted_refresh",
                    "access_token_expires_at": "2030-01-01T00:00:00Z",
                    "oauth_client_id": "persisted-client",
                    "updated_at": "2026-05-24T00:00:00Z"
                })
                .to_string(),
            )
            .await;
        let config = AppConfig::from_raw(RawConfig {
            tinyfish_api_key: Some(String::new()),
            tinyfish_mcp_url: Some(format!("{}/mcp", fixture.base_url)),
            tinyfish_mcp_access_token: Some("seed_access".to_owned()),
            tinyfish_mcp_refresh_token: Some("seed_refresh".to_owned()),
            tinyfish_mcp_oauth_client_id: Some("seed-client".to_owned()),
            tinyfish_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = TinyFishClient::from_app_config(
            &config,
            Some(store.clone() as Arc<dyn TinyFishRuntimeStore>),
        )?
        .ok_or_else(|| std::io::Error::other("TinyFish MCP client was not built"))?;

        let result = client.search("tinyfish").await?;

        assert_eq!(result, "search ok");
        let calls = fixture.state.mcp_calls.lock().await;
        assert!(
            calls
                .iter()
                .all(|call| call.authorization == "Bearer persisted_access")
        );
        Ok(())
    }

    #[tokio::test]
    async fn mcp_refreshes_access_token_before_expiry_and_persists() -> Result<(), Box<dyn Error>> {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000)?;
        let old_token = test_tinyfish_jwt(now + TimeDuration::minutes(2))?;
        let new_token = test_tinyfish_jwt(now + TimeDuration::hours(1))?;
        let fixture = spawn_mcp_fixture_server(Some(new_token.clone())).await?;
        let store = Arc::new(MemoryTinyFishRuntimeStore::default());
        let config = AppConfig::from_raw(RawConfig {
            tinyfish_api_key: Some(String::new()),
            tinyfish_mcp_url: Some(format!("{}/mcp", fixture.base_url)),
            tinyfish_mcp_access_token: Some(old_token),
            tinyfish_mcp_refresh_token: Some("refresh-token".to_owned()),
            tinyfish_mcp_oauth_client_id: Some("client-id".to_owned()),
            tinyfish_mcp_oauth_token_url: Some(format!("{}/oauth/token", fixture.base_url)),
            tinyfish_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = TinyFishMcpClient::from_config(
            &config.tinyfish,
            Some(store.clone() as Arc<dyn TinyFishRuntimeStore>),
        )?
        .ok_or_else(|| std::io::Error::other("TinyFish MCP client was not built"))?
        .with_now(now);

        let result = client.search("tinyfish").await?;

        assert_eq!(result, "search ok");
        assert_eq!(fixture.state.oauth_bodies.lock().await.len(), 1);
        assert_eq!(
            fixture.state.oauth_bodies.lock().await.as_slice(),
            &["grant_type=refresh_token&refresh_token=refresh-token&client_id=client-id"]
        );
        let calls = fixture.state.mcp_calls.lock().await;
        assert!(
            calls
                .iter()
                .all(|call| call.authorization == format!("Bearer {new_token}"))
        );
        let stored = store
            .get_app_setting(TINYFISH_MCP_RUNTIME_STATE_KEY)
            .await?
            .ok_or_else(|| std::io::Error::other("missing persisted TinyFish state"))?;
        let stored: Value = serde_json::from_str(&stored)?;
        assert_eq!(stored["access_token"], new_token);
        assert_eq!(stored["refresh_token"], "rotated-refresh-token");
        assert_eq!(stored["oauth_client_id"], "client-id");
        assert_eq!(stored["access_token_expires_at"], "2023-11-14T23:13:20Z");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "live TinyFish REST provider smoke"]
    async fn live_tinyfish_rest_smoke_searches_and_fetches() -> Result<(), Box<dyn Error>> {
        let api_key = std::env::var("TINYFISH_API_KEY")
            .map_err(|_| "TINYFISH_API_KEY is required for live TinyFish smoke")?;
        let query = std::env::var("OPENPLOTVA_TINYFISH_SMOKE_QUERY")
            .unwrap_or_else(|_| "OpenPlotva Telegram bot".to_owned());
        let fetch_url = std::env::var("OPENPLOTVA_TINYFISH_SMOKE_FETCH_URL")
            .unwrap_or_else(|_| "https://example.com/".to_owned());

        let config = AppConfig::from_raw(RawConfig {
            tinyfish_enabled: std::env::var("TINYFISH_ENABLED").ok(),
            tinyfish_api_key: Some(api_key),
            tinyfish_search_base_url: std::env::var("TINYFISH_SEARCH_BASE_URL").ok(),
            tinyfish_fetch_base_url: std::env::var("TINYFISH_FETCH_BASE_URL").ok(),
            tinyfish_search_location: std::env::var("TINYFISH_SEARCH_LOCATION").ok(),
            tinyfish_search_language: std::env::var("TINYFISH_SEARCH_LANGUAGE").ok(),
            tinyfish_fetch_format: std::env::var("TINYFISH_FETCH_FORMAT").ok(),
            tinyfish_max_content_chars: std::env::var("TINYFISH_MAX_CONTENT_CHARS").ok(),
            tinyfish_timeout_seconds: std::env::var("TINYFISH_TIMEOUT_SECONDS").ok(),
            tinyfish_mcp_url: std::env::var("TINYFISH_MCP_URL").ok(),
            tinyfish_mcp_access_token: std::env::var("TINYFISH_MCP_ACCESS_TOKEN").ok(),
            tinyfish_mcp_refresh_token: std::env::var("TINYFISH_MCP_REFRESH_TOKEN").ok(),
            tinyfish_mcp_oauth_client_id: std::env::var("TINYFISH_MCP_OAUTH_CLIENT_ID").ok(),
            tinyfish_mcp_oauth_token_url: std::env::var("TINYFISH_MCP_OAUTH_TOKEN_URL").ok(),
            tinyfish_mcp_search_tool_name: std::env::var("TINYFISH_MCP_SEARCH_TOOL_NAME").ok(),
            tinyfish_mcp_fetch_tool_name: std::env::var("TINYFISH_MCP_FETCH_TOOL_NAME").ok(),
            ..RawConfig::default()
        })?;
        let client = TinyFishRestClient::from_app_config(&config)?
            .ok_or_else(|| std::io::Error::other("TinyFish REST client was not built"))?;

        let search = client.search(&query).await?;
        assert!(
            !search.trim().is_empty(),
            "TinyFish search response must be non-empty"
        );

        let fetched = client.fetch(&fetch_url).await?;
        assert!(
            !fetched.trim().is_empty(),
            "TinyFish fetch response must be non-empty"
        );
        Ok(())
    }

    #[tokio::test]
    #[ignore = "live TinyFish MCP/OAuth provider smoke"]
    async fn live_tinyfish_mcp_smoke_searches_and_fetches() -> Result<(), Box<dyn Error>> {
        let access_token = std::env::var("TINYFISH_MCP_ACCESS_TOKEN").ok();
        let refresh_token = std::env::var("TINYFISH_MCP_REFRESH_TOKEN").ok();
        let oauth_client_id = std::env::var("TINYFISH_MCP_OAUTH_CLIENT_ID").ok();
        let has_access = access_token
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
        let has_refresh = refresh_token
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            && oauth_client_id
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty());
        if !has_access && !has_refresh {
            return Err("TINYFISH_MCP_ACCESS_TOKEN or TINYFISH_MCP_REFRESH_TOKEN plus TINYFISH_MCP_OAUTH_CLIENT_ID is required".into());
        }
        let query = std::env::var("OPENPLOTVA_TINYFISH_SMOKE_QUERY")
            .unwrap_or_else(|_| "OpenPlotva Telegram bot".to_owned());
        let fetch_url = std::env::var("OPENPLOTVA_TINYFISH_SMOKE_FETCH_URL")
            .unwrap_or_else(|_| "https://example.com/".to_owned());

        let config = AppConfig::from_raw(RawConfig {
            tinyfish_enabled: std::env::var("TINYFISH_ENABLED").ok(),
            tinyfish_api_key: Some(String::new()),
            tinyfish_search_location: std::env::var("TINYFISH_SEARCH_LOCATION").ok(),
            tinyfish_search_language: std::env::var("TINYFISH_SEARCH_LANGUAGE").ok(),
            tinyfish_fetch_format: std::env::var("TINYFISH_FETCH_FORMAT").ok(),
            tinyfish_max_content_chars: std::env::var("TINYFISH_MAX_CONTENT_CHARS").ok(),
            tinyfish_timeout_seconds: std::env::var("TINYFISH_TIMEOUT_SECONDS").ok(),
            tinyfish_mcp_url: std::env::var("TINYFISH_MCP_URL").ok(),
            tinyfish_mcp_access_token: access_token,
            tinyfish_mcp_refresh_token: refresh_token,
            tinyfish_mcp_oauth_client_id: oauth_client_id,
            tinyfish_mcp_oauth_token_url: std::env::var("TINYFISH_MCP_OAUTH_TOKEN_URL").ok(),
            tinyfish_mcp_search_tool_name: std::env::var("TINYFISH_MCP_SEARCH_TOOL_NAME").ok(),
            tinyfish_mcp_fetch_tool_name: std::env::var("TINYFISH_MCP_FETCH_TOOL_NAME").ok(),
            ..RawConfig::default()
        })?;
        let client = TinyFishClient::from_app_config(&config, None)?
            .ok_or_else(|| std::io::Error::other("TinyFish MCP client was not built"))?;
        client.bootstrap_runtime_state().await?;

        let search = client.search(&query).await?;
        assert!(
            !search.trim().is_empty(),
            "TinyFish MCP search response must be non-empty"
        );

        let fetched = client.fetch(&fetch_url).await?;
        assert!(
            !fetched.trim().is_empty(),
            "TinyFish MCP fetch response must be non-empty"
        );
        Ok(())
    }

    struct FixtureServer {
        base_url: String,
        state: Arc<FixtureState>,
    }

    #[derive(Debug, Default)]
    struct McpFixtureState {
        mcp_calls: Mutex<Vec<McpCall>>,
        oauth_bodies: Mutex<Vec<String>>,
        refresh_token: Option<String>,
    }

    #[derive(Debug, Clone)]
    struct McpCall {
        authorization: String,
        session_id: String,
        method: String,
        params: Option<Value>,
    }

    struct McpFixtureServer {
        base_url: String,
        state: Arc<McpFixtureState>,
    }

    async fn spawn_mcp_fixture_server(
        refresh_token: Option<String>,
    ) -> Result<McpFixtureServer, Box<dyn Error>> {
        let state = Arc::new(McpFixtureState {
            refresh_token,
            ..McpFixtureState::default()
        });
        let app = Router::new()
            .route("/mcp", post(mcp_handler))
            .route("/oauth/token", any(oauth_handler))
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::debug!(%error, "TinyFish MCP test fixture stopped");
            }
        });
        Ok(McpFixtureServer {
            base_url: format!("http://{addr}"),
            state,
        })
    }

    async fn mcp_handler(
        State(state): State<Arc<McpFixtureState>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        let method = body
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let params = body.get("params").cloned();
        state.mcp_calls.lock().await.push(McpCall {
            authorization: header_text(&headers, "authorization"),
            session_id: header_text(&headers, "mcp-session-id"),
            method: method.clone(),
            params: params.clone(),
        });
        let id = body.get("id").cloned().unwrap_or(Value::Null);
        let response = match method.as_str() {
            "initialize" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "serverInfo": {"name": "tinyfish-test", "version": "1.0.0"}
                }
            }),
            "notifications/initialized" => json!({"jsonrpc": "2.0", "result": {}}),
            "tools/list" => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": {
                    "tools": [
                        {
                            "name": "usage_search",
                            "description": "usage",
                            "inputSchema": {"type": "object", "properties": {"query": {"type": "string"}}}
                        },
                        {
                            "name": "Tiny_Fish-Search",
                            "description": "Search",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "q": {"type": "string"},
                                    "location": {"type": "string"},
                                    "language": {"type": "string"}
                                }
                            }
                        },
                        {
                            "name": "tinyfish_fetch",
                            "description": "Fetch",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "urls": {"type": "array"},
                                    "format": {"type": "string"},
                                    "links": {"type": "boolean"},
                                    "image_links": {"type": "boolean"}
                                }
                            }
                        }
                    ]
                }
            }),
            "tools/call" => {
                let name = params
                    .as_ref()
                    .and_then(|params| params.get("name"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let text = if name.eq_ignore_ascii_case("tinyfish_fetch") {
                    "# fetched"
                } else {
                    "search ok"
                };
                if name.eq_ignore_ascii_case("tinyfish_fetch") {
                    return (
                        [
                            (header::CONTENT_TYPE, "text/event-stream"),
                            (HeaderName::from_static("mcp-session-id"), "session-1"),
                        ],
                        format!(
                            "event: message\ndata: {}\n\n",
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": {"content": [{"type": "text", "text": text}]}
                            })
                        ),
                    )
                        .into_response();
                }
                json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {"content": [{"type": "text", "text": text}]}
                })
            }
            _ => {
                json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "unknown method"}})
            }
        };
        (
            [
                (header::CONTENT_TYPE, "application/json"),
                (HeaderName::from_static("mcp-session-id"), "session-1"),
            ],
            response.to_string(),
        )
            .into_response()
    }

    async fn oauth_handler(
        State(state): State<Arc<McpFixtureState>>,
        body: Bytes,
    ) -> impl IntoResponse {
        state
            .oauth_bodies
            .lock()
            .await
            .push(String::from_utf8_lossy(&body).to_string());
        let token = state.refresh_token.clone().unwrap_or_default();
        (
            [(header::CONTENT_TYPE, "application/json")],
            json!({
                "access_token": token,
                "refresh_token": "rotated-refresh-token",
                "token_type": "bearer",
                "expires_in": 3600
            })
            .to_string(),
        )
    }

    #[derive(Default)]
    struct MemoryTinyFishRuntimeStore {
        values: Mutex<BTreeMap<String, String>>,
    }

    impl MemoryTinyFishRuntimeStore {
        async fn insert(&self, key: &str, value: String) {
            self.values.lock().await.insert(key.to_owned(), value);
        }
    }

    impl TinyFishRuntimeStore for MemoryTinyFishRuntimeStore {
        fn get_app_setting<'a>(&'a self, key: &'a str) -> TinyFishStoreFuture<'a, Option<String>> {
            Box::pin(async move { Ok(self.values.lock().await.get(key).cloned()) })
        }

        fn upsert_app_setting<'a>(
            &'a self,
            key: &'a str,
            value: &'a str,
        ) -> TinyFishStoreFuture<'a, ()> {
            Box::pin(async move {
                self.values
                    .lock()
                    .await
                    .insert(key.to_owned(), value.to_owned());
                Ok(())
            })
        }
    }

    fn test_tinyfish_jwt(expires_at: OffsetDateTime) -> Result<String, Box<dyn Error>> {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(
            json!({"exp": expires_at.unix_timestamp()})
                .to_string()
                .as_bytes(),
        );
        Ok(format!("{header}.{payload}."))
    }

    async fn spawn_fixture_server() -> Result<FixtureServer, Box<dyn Error>> {
        let state = Arc::new(FixtureState::default());
        let app = Router::new()
            .route("/search", get(search_handler))
            .route("/fetch", post(fetch_handler))
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::debug!(%error, "TinyFish test fixture stopped");
            }
        });
        Ok(FixtureServer {
            base_url: format!("http://{addr}"),
            state,
        })
    }

    async fn search_handler(
        State(state): State<Arc<FixtureState>>,
        headers: HeaderMap,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> impl IntoResponse {
        state.search.lock().await.push(SearchRequest {
            api_key: header_text(&headers, "x-api-key"),
            query,
        });
        (
            [(header::CONTENT_TYPE, "application/json")],
            "  {\"results\":[\"ok\"]}\n",
        )
    }

    async fn fetch_handler(
        State(state): State<Arc<FixtureState>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> impl IntoResponse {
        state.fetch.lock().await.push(FetchRequest {
            api_key: header_text(&headers, "x-api-key"),
            content_type: header_text(&headers, "content-type"),
            body,
        });
        (
            StatusCode::OK,
            Json(json!({
                "results": [{
                    "url": "https://example.com/page",
                    "final_url": "https://example.com/page",
                    "title": "Example",
                    "format": "markdown",
                    "text": "# Example\n\nBody",
                }],
                "errors": [],
            })),
        )
    }

    fn header_text(headers: &HeaderMap, name: &'static str) -> String {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .to_owned()
    }
}
