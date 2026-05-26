//! Runtime API Gemini explicit-cache purge support.

use std::{fs, io, path::Path};

use openplotva_config::GoogleAiConfig;
use openplotva_server::{
    RuntimeGeminiCachePurgeResultData, RuntimeGeminiCachePurger, RuntimeGeminiCachePurgerFuture,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use url::Url;

const GEMINI_API_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
const CACHE_DISPLAY_PREFIX: &str = "pv|1|";
const LEGACY_DISPLAY_PREFIX: &str = "plotva-explicit-cache|v1|";
const GEMINI_CACHE_PAGE_SIZE: &str = "100";

#[derive(Clone)]
pub struct GeminiExplicitCachePurger {
    api_key: String,
    base_url: String,
    http: reqwest::Client,
}

impl GeminiExplicitCachePurger {
    pub fn from_config(config: &GoogleAiConfig) -> Option<Self> {
        Self::new(resolve_google_ai_key(config))
    }

    fn new(api_key: String) -> Option<Self> {
        let api_key = api_key.trim();
        if api_key.is_empty() {
            return None;
        }
        Some(Self {
            api_key: api_key.to_owned(),
            base_url: GEMINI_API_BASE_URL.to_owned(),
            http: reqwest::Client::new(),
        })
    }

    async fn purge_managed(&self) -> Result<RuntimeGeminiCachePurgeResultData, String> {
        let mut result = RuntimeGeminiCachePurgeResultData::default();
        let mut page_token = String::new();
        loop {
            let page = self
                .list_page(&page_token)
                .await
                .map_err(|error| list_error_text(page_token.is_empty(), error))?;
            for cached in page.cached_contents {
                let Some(name) = delete_candidate_for_cached_content(&cached, &mut result) else {
                    continue;
                };
                if self.delete_cache(&name).await.is_ok() {
                    result.deleted += 1;
                } else {
                    result.failed += 1;
                }
            }
            page_token = page.next_page_token.trim().to_owned();
            if page_token.is_empty() {
                return Ok(result);
            }
        }
    }

    async fn list_page(&self, page_token: &str) -> Result<GeminiCachedContentPage, String> {
        let url = gemini_list_cached_contents_url(&self.base_url, &self.api_key, page_token)?;
        let response = self.http.get(url).send().await.map_err(error_text)?;
        let status = response.status();
        if !status.is_success() {
            let body = limited_response_body(response).await;
            return Err(format!("HTTP {}: {}", status.as_u16(), body));
        }
        response
            .json::<GeminiCachedContentPage>()
            .await
            .map_err(error_text)
    }

    async fn delete_cache(&self, name: &str) -> Result<(), String> {
        let url = gemini_delete_cached_content_url(&self.base_url, &self.api_key, name)?;
        let response = self.http.delete(url).send().await.map_err(error_text)?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let body = limited_response_body(response).await;
        Err(format!("HTTP {}: {}", status.as_u16(), body))
    }
}

impl RuntimeGeminiCachePurger for GeminiExplicitCachePurger {
    fn purge<'a>(&'a self) -> RuntimeGeminiCachePurgerFuture<'a> {
        Box::pin(async move { self.purge_managed().await })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCachedContentPage {
    #[serde(default)]
    cached_contents: Vec<GeminiCachedContent>,
    #[serde(default)]
    next_page_token: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GeminiCachedContent {
    #[serde(default)]
    name: String,
    #[serde(default)]
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct PersistedGoogleAiKey {
    #[serde(default)]
    key: String,
    #[serde(default)]
    last_used: Option<String>,
    #[serde(default)]
    invalid_until: Option<String>,
    #[serde(default)]
    validated: bool,
}

pub(crate) fn resolve_google_ai_key(config: &GoogleAiConfig) -> String {
    let key = config.key.trim();
    if !key.is_empty() {
        return key.to_owned();
    }
    read_active_key(&config.key_stats_file).unwrap_or_default()
}

fn read_active_key(path: &str) -> Result<String, String> {
    let entries = load_persisted_keys(path)?;
    if entries.is_empty() {
        return Ok(String::new());
    }
    let now = OffsetDateTime::now_utc();
    let mut active: Option<&PersistedGoogleAiKey> = None;
    for entry in &entries {
        if !entry.usable_at(now) {
            continue;
        }
        if active.is_none_or(|current| entry.used_after(current)) {
            active = Some(entry);
        }
    }
    Ok(active
        .map(|entry| entry.key.trim().to_owned())
        .unwrap_or_default())
}

fn load_persisted_keys(path: &str) -> Result<Vec<PersistedGoogleAiKey>, String> {
    let path = path.trim();
    if path.is_empty() {
        return Ok(Vec::new());
    }
    let data = match fs::read_to_string(Path::new(path)) {
        Ok(data) => data,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("read key stats: {error}")),
    };
    if data.trim().is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(&data).map_err(|error| format!("decode key stats: {error}"))
}

impl PersistedGoogleAiKey {
    fn usable_at(&self, now: OffsetDateTime) -> bool {
        if !self.validated {
            return false;
        }
        self.invalid_until
            .as_deref()
            .and_then(parse_rfc3339)
            .is_none_or(|invalid_until| invalid_until <= now)
    }

    fn used_after(&self, other: &Self) -> bool {
        let Some(last_used) = self.last_used.as_deref().and_then(parse_rfc3339) else {
            return false;
        };
        other
            .last_used
            .as_deref()
            .and_then(parse_rfc3339)
            .is_none_or(|other_used| last_used > other_used)
    }
}

fn delete_candidate_for_cached_content(
    cached: &GeminiCachedContent,
    result: &mut RuntimeGeminiCachePurgeResultData,
) -> Option<String> {
    result.scanned += 1;
    if !is_managed_display_name(&cached.display_name) {
        return None;
    }
    result.matched += 1;
    let name = cached.name.trim();
    if name.is_empty() {
        result.failed += 1;
        return None;
    }
    Some(name.to_owned())
}

fn is_managed_display_name(display_name: &str) -> bool {
    parse_display_name(display_name.trim()).is_some()
}

fn parse_display_name(display_name: &str) -> Option<CacheKey> {
    let prefix = display_name_prefix(display_name)?;
    let (use_case, model, fingerprint) = display_name_parts(display_name, prefix)?;
    let use_case = legacy_unescape_if_needed(prefix, use_case);
    let model = legacy_unescape_if_needed(prefix, model);
    Some(CacheKey {
        use_case: normalize_cache_token(&use_case),
        model: normalize_cache_token(&model),
        fingerprint: fingerprint.to_owned(),
    })
}

fn display_name_prefix(display_name: &str) -> Option<&'static str> {
    if display_name.starts_with(CACHE_DISPLAY_PREFIX) {
        Some(CACHE_DISPLAY_PREFIX)
    } else if display_name.starts_with(LEGACY_DISPLAY_PREFIX) {
        Some(LEGACY_DISPLAY_PREFIX)
    } else {
        None
    }
}

fn display_name_parts<'a>(
    display_name: &'a str,
    prefix: &str,
) -> Option<(&'a str, &'a str, &'a str)> {
    let body = display_name.strip_prefix(prefix)?;
    let (use_case, rest) = body.split_once('|')?;
    let (model, fingerprint) = rest.split_once('|')?;
    if fingerprint.contains('|') {
        return None;
    }
    let use_case = use_case.trim();
    let model = model.trim();
    let fingerprint = fingerprint.trim();
    if use_case.is_empty() || model.is_empty() || fingerprint.is_empty() {
        return None;
    }
    Some((use_case, model, fingerprint))
}

fn normalize_cache_token(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if !trimmed.contains('|') && trimmed.len() <= 24 {
        return trimmed.to_owned();
    }
    let digest = Sha256::digest(trimmed.as_bytes());
    let encoded = hex::encode(digest);
    format!("h{}", &encoded[..23])
}

fn legacy_unescape_if_needed(prefix: &str, value: &str) -> String {
    if prefix != LEGACY_DISPLAY_PREFIX {
        return value.to_owned();
    }
    query_unescape(value).unwrap_or_else(|| value.to_owned())
}

fn query_unescape(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let high = hex_value(bytes[index + 1])?;
                let low = hex_value(bytes[index + 2])?;
                decoded.push((high << 4) | low);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8(decoded).ok()
}

const fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Eq, PartialEq)]
struct CacheKey {
    use_case: String,
    model: String,
    fingerprint: String,
}

fn gemini_list_cached_contents_url(
    base_url: &str,
    api_key: &str,
    page_token: &str,
) -> Result<String, String> {
    let mut url = parse_base_url(base_url)?;
    url.path_segments_mut()
        .map_err(|_| "invalid Gemini API base URL".to_owned())?
        .pop_if_empty()
        .push("cachedContents");
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("key", api_key);
        query.append_pair("pageSize", GEMINI_CACHE_PAGE_SIZE);
        if !page_token.trim().is_empty() {
            query.append_pair("pageToken", page_token.trim());
        }
    }
    Ok(url.to_string())
}

fn gemini_delete_cached_content_url(
    base_url: &str,
    api_key: &str,
    name: &str,
) -> Result<String, String> {
    let mut url = parse_base_url(base_url)?;
    let name = name.trim();
    if name.is_empty() {
        return Err("cached content name is empty".to_owned());
    }
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| "invalid Gemini API base URL".to_owned())?;
        segments.pop_if_empty();
        if let Some((resource, id)) = name.split_once('/') {
            segments.push(resource).push(id);
        } else {
            segments.push(name);
        }
    }
    url.query_pairs_mut().append_pair("key", api_key);
    Ok(url.to_string())
}

fn parse_base_url(base_url: &str) -> Result<Url, String> {
    let mut url = Url::parse(base_url.trim()).map_err(|_| "invalid Gemini API base URL")?;
    if url.host_str().is_none() {
        return Err("invalid Gemini API base URL".to_owned());
    }
    url.set_query(None);
    Ok(url)
}

async fn limited_response_body(response: reqwest::Response) -> String {
    match response.bytes().await {
        Ok(bytes) => {
            let end = bytes.len().min(512);
            String::from_utf8_lossy(&bytes[..end]).trim().to_owned()
        }
        Err(_) => String::new(),
    }
}

fn list_error_text(first_page: bool, error: String) -> String {
    if first_page {
        format!("list caches: {error}")
    } else {
        format!("list caches next page: {error}")
    }
}

fn parse_rfc3339(value: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339).ok()
}

fn error_text<E: std::fmt::Display>(error: E) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        error::Error,
        fs,
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{
        Json, Router,
        extract::{Path, Query, State},
        http::StatusCode,
        response::IntoResponse,
        routing::{delete, get},
    };
    use openplotva_config::GoogleAiConfig;
    use openplotva_server::RuntimeGeminiCachePurgeResultData;
    use serde_json::json;
    use tokio::{net::TcpListener, sync::Mutex};

    use super::{
        CACHE_DISPLAY_PREFIX, CacheKey, GeminiCachedContent, GeminiExplicitCachePurger,
        delete_candidate_for_cached_content, gemini_delete_cached_content_url,
        gemini_list_cached_contents_url, parse_display_name, resolve_google_ai_key,
    };

    #[test]
    fn google_ai_key_resolution_matches_go_direct_and_stats_file() -> Result<(), Box<dyn Error>> {
        let direct = GoogleAiConfig {
            key: " direct-key ".to_owned(),
            key_stats_file: "missing.json".to_owned(),
        };
        assert_eq!(resolve_google_ai_key(&direct), "direct-key");

        let now = time::OffsetDateTime::now_utc();
        let older = (now - time::Duration::hours(2))
            .format(&time::format_description::well_known::Rfc3339)?;
        let newer = (now - time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)?;
        let future = (now + time::Duration::hours(1))
            .format(&time::format_description::well_known::Rfc3339)?;
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let path = std::env::temp_dir().join(format!("openplotva-googleai-keys-{nonce}.json"));
        fs::write(
            &path,
            serde_json::to_vec(&json!([
                {"key": "unvalidated", "validated": false, "last_used": newer},
                {"key": "invalid-now", "validated": true, "last_used": newer, "invalid_until": future},
                {"key": "older", "validated": true, "last_used": older},
                {"key": "newer", "validated": true, "last_used": newer}
            ]))?,
        )?;
        let from_stats = GoogleAiConfig {
            key: String::new(),
            key_stats_file: path.to_string_lossy().into_owned(),
        };

        assert_eq!(resolve_google_ai_key(&from_stats), "newer");
        let _ = fs::remove_file(path);
        Ok(())
    }

    #[test]
    fn gemini_cache_display_name_parsing_accepts_current_and_legacy_owned_names() {
        assert_eq!(
            parse_display_name("pv|1|chat_core|gemini-2.5-flash-lite|abc"),
            Some(CacheKey {
                use_case: "chat_core".to_owned(),
                model: "gemini-2.5-flash-lite".to_owned(),
                fingerprint: "abc".to_owned()
            })
        );
        assert_eq!(
            parse_display_name("plotva-explicit-cache|v1|legacy+space|gemini%2Fflash%3Dx|def"),
            Some(CacheKey {
                use_case: "legacy space".to_owned(),
                model: "gemini/flash=x".to_owned(),
                fingerprint: "def".to_owned()
            })
        );
        assert_eq!(parse_display_name("third_party_cache"), None);
        assert_eq!(parse_display_name("pv|1|a|b|c|d"), None);
    }

    #[test]
    fn gemini_cache_delete_candidate_counts_like_go_purge() {
        let mut result = RuntimeGeminiCachePurgeResultData::default();
        let managed = GeminiCachedContent {
            name: "cachedContents/managed-1".to_owned(),
            display_name: format!("{CACHE_DISPLAY_PREFIX}a|b|c"),
        };
        let foreign = GeminiCachedContent {
            name: "cachedContents/foreign".to_owned(),
            display_name: "third_party_cache".to_owned(),
        };
        let empty_name = GeminiCachedContent {
            name: " ".to_owned(),
            display_name: "plotva-explicit-cache|v1|legacy|gemini|def".to_owned(),
        };

        assert_eq!(
            delete_candidate_for_cached_content(&managed, &mut result),
            Some("cachedContents/managed-1".to_owned())
        );
        assert_eq!(
            delete_candidate_for_cached_content(&foreign, &mut result),
            None
        );
        assert_eq!(
            delete_candidate_for_cached_content(&empty_name, &mut result),
            None
        );

        assert_eq!(result.scanned, 3);
        assert_eq!(result.matched, 2);
        assert_eq!(result.deleted, 0);
        assert_eq!(result.failed, 1);
    }

    #[test]
    fn gemini_cache_urls_match_official_rest_shape() {
        assert_eq!(
            gemini_list_cached_contents_url(
                "https://generativelanguage.googleapis.com/v1beta/",
                "key 1",
                "next token"
            ),
            Ok("https://generativelanguage.googleapis.com/v1beta/cachedContents?key=key+1&pageSize=100&pageToken=next+token".to_owned())
        );
        assert_eq!(
            gemini_delete_cached_content_url(
                "https://generativelanguage.googleapis.com/v1beta",
                "key 1",
                "cachedContents/cache/1"
            ),
            Ok("https://generativelanguage.googleapis.com/v1beta/cachedContents/cache%2F1?key=key+1".to_owned())
        );
    }

    #[tokio::test]
    async fn gemini_cache_purger_loopback_lists_pages_and_deletes_only_managed_names()
    -> Result<(), Box<dyn Error>> {
        let state = Arc::new(GeminiCacheFixtureState::default());
        let app = Router::new()
            .route("/v1beta/cachedContents", get(gemini_cache_list_handler))
            .route(
                "/v1beta/cachedContents/{cache_id}",
                delete(gemini_cache_delete_handler),
            )
            .with_state(Arc::clone(&state));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                tracing::debug!(%error, "Gemini cache purge fixture stopped");
            }
        });

        let mut purger = GeminiExplicitCachePurger::new("loopback-key".to_owned())
            .ok_or_else(|| std::io::Error::other("purger not built"))?;
        purger.base_url = format!("http://{addr}/v1beta");

        let result = purger.purge_managed().await?;

        assert_eq!(result.scanned, 5);
        assert_eq!(result.matched, 4);
        assert_eq!(result.deleted, 2);
        assert_eq!(result.failed, 2);
        assert_eq!(
            state.deletes.lock().await.as_slice(),
            &[
                "managed-ok".to_owned(),
                "managed-fail".to_owned(),
                "managed-ok2".to_owned(),
            ]
        );
        Ok(())
    }

    #[derive(Default)]
    struct GeminiCacheFixtureState {
        deletes: Mutex<Vec<String>>,
    }

    async fn gemini_cache_list_handler(
        Query(query): Query<BTreeMap<String, String>>,
    ) -> Json<serde_json::Value> {
        if query.get("pageToken").is_some_and(|token| token == "next") {
            return Json(json!({
                "cachedContents": [
                    {
                        "name": "cachedContents/managed-fail",
                        "displayName": "pv|1|chat_core|gemini-2.5-flash-lite|fail"
                    },
                    {
                        "name": "cachedContents/managed-ok2",
                        "displayName": "plotva-explicit-cache|v1|legacy|gemini|ok2"
                    }
                ]
            }));
        }
        Json(json!({
            "cachedContents": [
                {
                    "name": "cachedContents/managed-ok",
                    "displayName": "pv|1|chat_core|gemini-2.5-flash-lite|ok"
                },
                {
                    "name": "cachedContents/foreign",
                    "displayName": "third_party_cache"
                },
                {
                    "name": "",
                    "displayName": "plotva-explicit-cache|v1|legacy|gemini|empty"
                }
            ],
            "nextPageToken": "next"
        }))
    }

    async fn gemini_cache_delete_handler(
        State(state): State<Arc<GeminiCacheFixtureState>>,
        Path(cache_id): Path<String>,
    ) -> impl IntoResponse {
        state.deletes.lock().await.push(cache_id.clone());
        if cache_id == "managed-fail" {
            return (StatusCode::INTERNAL_SERVER_ERROR, "delete failed").into_response();
        }
        (StatusCode::OK, "{}").into_response()
    }
}
