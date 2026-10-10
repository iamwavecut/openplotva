//! Serper.dev web search adapter for dialog tools.

use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration as StdDuration,
};

use openplotva_config::{AppConfig, DEFAULT_SERPER_TIMEOUT_SECONDS, SerperConfig};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;
use tokio::time::sleep;
use url::Url;

use crate::dialog_tools::{CrawlUrlFuture, UrlCrawler, WebSearchFuture, WebSearchProvider};

const SERPER_BASE_URL: &str = "https://google.serper.dev";
const SERPER_RETRY_COUNT: usize = 3;
const SERPER_RETRY_BASE_DELAY: StdDuration = StdDuration::from_secs(1);
const SERPER_QUERY_MAX_BYTES: usize = 400;
const SERPER_CRAWL_MAX_BYTES: usize = 6000;
const CRAWL_MAX_REDIRECTS: usize = 5;

#[derive(Clone)]
pub struct SerperClient {
    http: reqwest::Client,
    api_key: Arc<str>,
    base_url: Arc<str>,
    timeout: StdDuration,
}

impl fmt::Debug for SerperClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SerperClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl SerperClient {
    pub fn from_app_config(config: &AppConfig) -> Result<Option<Self>, SerperError> {
        Self::from_config(&config.serper)
    }

    /// Build a Serper client from the Serper config.
    pub fn from_config(config: &SerperConfig) -> Result<Option<Self>, SerperError> {
        Self::from_config_with_base_url(config, SERPER_BASE_URL)
    }

    fn from_config_with_base_url(
        config: &SerperConfig,
        base_url: &str,
    ) -> Result<Option<Self>, SerperError> {
        let api_key = config.api_key.trim();
        if api_key.is_empty() {
            return Ok(None);
        }
        let timeout_seconds = if config.timeout_seconds > 0 {
            config.timeout_seconds
        } else {
            DEFAULT_SERPER_TIMEOUT_SECONDS
        };
        let timeout = StdDuration::from_secs(timeout_seconds as u64);
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(SerperError::HttpClient)?;
        Ok(Some(Self {
            http,
            api_key: Arc::from(api_key),
            base_url: Arc::from(base_url.trim_end_matches('/')),
            timeout,
        }))
    }

    pub async fn search(&self, query: &str) -> Result<String, SerperError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(SerperError::EmptySearchQuery);
        }
        let request = SerperSearchRequest {
            query: clamp_query(query),
            language: "ru",
            auto_correct: true,
            max_results: 10,
            page: 1,
        };
        let raw = self.perform_search_with_retry(&request).await?;
        if raw.is_empty() {
            return Err(SerperError::EmptySearchResult);
        }
        Ok(raw)
    }

    /// Crawl one URL through a direct HTTP GET and convert the returned HTML into compact text.
    pub async fn crawl_url(&self, crawl_url: &str) -> Result<String, SerperError> {
        let crawl_url = crawl_url.trim();
        if crawl_url.is_empty() {
            return Err(SerperError::EmptyUrl);
        }
        tokio::time::timeout(self.timeout, async {
            let mut url = crawl_url.to_owned();
            let mut redirects = 0;
            loop {
                let destination = validate_public_crawl_url(&url).await?;
                let mut builder = reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(self.timeout);
                if let Some(host) = &destination.resolved_host {
                    builder = builder.resolve_to_addrs(host, &destination.addresses);
                }
                let http = builder.build().map_err(SerperError::HttpClient)?;
                let response = http
                    .get(destination.url)
                    .send()
                    .await
                    .map_err(|error| SerperError::Request(error.without_url()))?;
                let Some(target) = crawl_redirect_target(&response, redirects)? else {
                    return read_crawl_response(response).await;
                };
                url = target.into();
                redirects += 1;
            }
        })
        .await
        .map_err(|_| SerperError::message("crawl timed out".to_owned()))?
    }

    async fn perform_search_with_retry(
        &self,
        request: &SerperSearchRequest<'_>,
    ) -> Result<String, SerperError> {
        let mut last_error: Option<SerperError> = None;
        for attempt in 0..=SERPER_RETRY_COUNT {
            if attempt > 0 {
                let delay = SERPER_RETRY_BASE_DELAY * (2 * attempt - 1) as u32;
                sleep(delay).await;
            }
            match self.perform_search_once(request).await {
                Ok(raw) => return Ok(raw),
                Err(error) => {
                    let should_retry = should_retry_serper_error(&error.to_string(), attempt);
                    last_error = Some(error);
                    if !should_retry {
                        break;
                    }
                }
            }
        }
        let detail = last_error
            .map(|error| error.to_string())
            .unwrap_or_else(|| "unknown error".to_owned());
        Err(SerperError::message(format!(
            "request failed after {} attempts: {detail}",
            SERPER_RETRY_COUNT + 1
        )))
    }

    async fn perform_search_once(
        &self,
        request: &SerperSearchRequest<'_>,
    ) -> Result<String, SerperError> {
        let endpoint = Url::parse(&format!("{}/search", self.base_url))
            .map_err(SerperError::ParseSearchUrl)?;
        let response = self
            .http
            .post(endpoint)
            .header("Content-Type", "application/json")
            .header("X-API-KEY", &*self.api_key)
            .json(request)
            .send()
            .await
            .map_err(|error| SerperError::message(format!("request error: {error}")))?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|error| SerperError::message(format!("read response: {error}")))?;
        if !status.is_success() {
            return Err(SerperError::message(format!(
                "api error: status {}, body: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            )));
        }
        serde_json::from_slice::<Value>(&body)
            .map_err(|error| SerperError::message(format!("unmarshal response: {error}")))?;
        Ok(String::from_utf8_lossy(&body).trim().to_owned())
    }
}

fn crawl_redirect_target(
    response: &reqwest::Response,
    redirects: usize,
) -> Result<Option<Url>, SerperError> {
    if !matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
        return Ok(None);
    }
    if redirects >= CRAWL_MAX_REDIRECTS {
        return Err(SerperError::message(
            "crawl exceeded 5 redirects".to_owned(),
        ));
    }
    let location = response
        .headers()
        .get(reqwest::header::LOCATION)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| SerperError::message("crawl redirect has no usable location".to_owned()))?;
    response
        .url()
        .join(location)
        .map(Some)
        .map_err(|_| SerperError::message("crawl redirect has an invalid location".to_owned()))
}

async fn read_crawl_response(mut response: reqwest::Response) -> Result<String, SerperError> {
    if !response.status().is_success() {
        return Err(SerperError::message(format!(
            "crawl failed with status {}",
            response.status().as_u16()
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| SerperError::ReadResponseBody(error.without_url()))?
    {
        if body.len().saturating_add(chunk.len()) > 2 * 1024 * 1024 {
            return Err(SerperError::message(
                "page exceeds 2 MiB decoded limit".to_owned(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    let body = String::from_utf8(body)
        .map_err(|_| SerperError::message("page is not UTF-8 text".to_owned()))?;
    if body
        .chars()
        .any(|ch| ch.is_control() && !ch.is_whitespace())
    {
        return Err(SerperError::message(
            "page contains binary control characters".to_owned(),
        ));
    }
    let text = html_body_to_plain_text(body.trim());
    if text.is_empty() {
        return Err(SerperError::message("empty response body".to_owned()));
    }
    Ok(text)
}

impl WebSearchProvider for SerperClient {
    fn search<'a>(&'a self, query: &'a str) -> WebSearchFuture<'a> {
        Box::pin(async move {
            self.search(query)
                .await
                .map_err(|error| Box::new(error) as openplotva_dialog::ToolboxError)
        })
    }
}

impl UrlCrawler for SerperClient {
    fn crawl<'a>(&'a self, url: &'a str) -> CrawlUrlFuture<'a> {
        Box::pin(async move {
            self.crawl_url(url)
                .await
                .map_err(|error| Box::new(error) as openplotva_dialog::ToolboxError)
        })
    }
}

/// Serper adapter failures surfaced through dialog tool failed results.
#[derive(Debug, Error)]
pub enum SerperError {
    #[error("search query is empty")]
    EmptySearchQuery,
    #[error("empty search result")]
    EmptySearchResult,
    #[error("url is empty")]
    EmptyUrl,
    /// Reqwest client could not be built.
    #[error("{0}")]
    HttpClient(reqwest::Error),
    /// Search URL could not be parsed.
    #[error("parse serper search url: {0}")]
    ParseSearchUrl(url::ParseError),
    /// Crawl request failed.
    #[error("{0}")]
    Request(reqwest::Error),
    /// Crawl response body could not be read.
    #[error("{0}")]
    ReadResponseBody(reqwest::Error),
    #[error("{0}")]
    Message(String),
}

impl SerperError {
    fn message(message: String) -> Self {
        Self::Message(message)
    }
}

struct ValidatedPublicUrl {
    url: Url,
    resolved_host: Option<String>,
    addresses: Vec<SocketAddr>,
}

async fn validate_public_crawl_url(raw: &str) -> Result<ValidatedPublicUrl, SerperError> {
    let url = Url::parse(raw).map_err(|_| SerperError::message("invalid crawl URL".to_owned()))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(SerperError::message(
            "crawl URL is not a permitted public HTTP(S) destination".to_owned(),
        ));
    }
    let host = url
        .host()
        .ok_or_else(|| SerperError::message("crawl URL has no host".to_owned()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| SerperError::message("crawl URL has no usable port".to_owned()))?;
    match host {
        url::Host::Ipv4(address) => {
            let address = IpAddr::V4(address);
            if !is_public_destination_ip(address) {
                return Err(SerperError::message(
                    "crawl URL resolves to a non-public address".to_owned(),
                ));
            }
            Ok(ValidatedPublicUrl {
                url,
                resolved_host: None,
                addresses: vec![SocketAddr::new(address, port)],
            })
        }
        url::Host::Ipv6(address) => {
            let address = IpAddr::V6(address);
            if !is_public_destination_ip(address) {
                return Err(SerperError::message(
                    "crawl URL resolves to a non-public address".to_owned(),
                ));
            }
            Ok(ValidatedPublicUrl {
                url,
                resolved_host: None,
                addresses: vec![SocketAddr::new(address, port)],
            })
        }
        url::Host::Domain(host) => {
            let host = host.to_owned();
            let addresses = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|_| SerperError::message("crawl host lookup failed".to_owned()))?
                .collect::<Vec<_>>();
            if addresses.is_empty()
                || addresses
                    .iter()
                    .any(|address| !is_public_destination_ip(address.ip()))
            {
                return Err(SerperError::message(
                    "crawl URL resolves to a non-public address".to_owned(),
                ));
            }
            Ok(ValidatedPublicUrl {
                url,
                resolved_host: Some(host),
                addresses,
            })
        }
    }
}

fn is_public_destination_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    !(first == 0
        || first == 10
        || first == 127
        || first >= 224
        || first == 100 && (64..=127).contains(&second)
        || first == 169 && second == 254
        || first == 172 && (16..=31).contains(&second)
        || first == 192 && second == 0 && third == 0
        || first == 192 && second == 0 && third == 2
        || first == 192 && second == 88 && third == 99
        || first == 192 && second == 168
        || first == 198 && (second == 18 || second == 19)
        || first == 198 && second == 51 && third == 100
        || first == 203 && second == 0 && third == 113)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    if segments[..5] == [0, 0, 0, 0, 0] && matches!(segments[5], 0 | 0xffff) {
        let octets = address.octets();
        return is_public_ipv4(Ipv4Addr::new(
            octets[12], octets[13], octets[14], octets[15],
        ));
    }
    let first = segments[0];
    !(address.is_unspecified()
        || address.is_loopback()
        || address.is_multicast()
        || first & 0xfe00 == 0xfc00
        || first & 0xffc0 == 0xfe80
        || first & 0xffc0 == 0xfec0
        || segments[0..2] == [0x2001, 0x0db8])
}

#[derive(Debug, Serialize)]
struct SerperSearchRequest<'a> {
    #[serde(rename = "q")]
    query: &'a str,
    #[serde(rename = "hl")]
    language: &'a str,
    #[serde(rename = "autocorrect")]
    auto_correct: bool,
    #[serde(rename = "num")]
    max_results: i32,
    page: i32,
}

fn clamp_query(query: &str) -> &str {
    if query.len() <= SERPER_QUERY_MAX_BYTES {
        return query;
    }
    let mut end = SERPER_QUERY_MAX_BYTES;
    while !query.is_char_boundary(end) {
        end -= 1;
    }
    &query[..end]
}

fn should_retry_serper_error(message: &str, attempt: usize) -> bool {
    if attempt >= SERPER_RETRY_COUNT {
        return false;
    }
    let message = message.to_ascii_lowercase();
    let network_errors = [
        "timeout",
        "connection",
        "network",
        "dial",
        "dns",
        "500",
        "502",
        "503",
        "504",
        "internal server error",
        "bad gateway",
        "service unavailable",
        "gateway timeout",
    ];
    if network_errors
        .iter()
        .any(|fragment| message.contains(fragment))
    {
        return true;
    }
    if message.contains("rate limit") || message.contains("429") {
        return true;
    }
    let no_retry = [
        "401",
        "403",
        "400",
        "unauthorized",
        "forbidden",
        "bad request",
        "invalid api key",
        "authentication",
        "unmarshal",
        "json",
        "parse",
    ];
    !no_retry.iter().any(|fragment| message.contains(fragment))
}

fn html_body_to_plain_text(body: &str) -> String {
    let mut text = String::with_capacity(body.len());
    let mut in_tag = false;
    for ch in body.chars() {
        match ch {
            '<' => {
                in_tag = true;
                text.push(' ');
            }
            '>' => {
                in_tag = false;
                text.push(' ');
            }
            _ if in_tag => {}
            _ => text.push(ch),
        }
    }
    truncate_to_bytes(
        &compact_whitespace(&unescape_html_entities(&text)),
        SERPER_CRAWL_MAX_BYTES,
    )
}

fn compact_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn unescape_html_entities(text: &str) -> String {
    text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
}

fn truncate_to_bytes(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::BTreeMap, env, error::Error, sync::Arc};

    use openplotva_config::{AppConfig, RawConfig};
    use serde_json::json;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::Mutex,
    };

    #[derive(Debug, Default)]
    struct FixtureState {
        requests: Mutex<Vec<RecordedRequest>>,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RecordedRequest {
        method: String,
        path: String,
        headers: BTreeMap<String, String>,
        body: Value,
    }

    struct Fixture {
        base_url: String,
        state: Arc<FixtureState>,
    }

    #[tokio::test]
    async fn search_posts_go_serper_payload_and_returns_raw_response() -> Result<(), Box<dyn Error>>
    {
        let fixture = spawn_fixture_server().await?;
        let config = AppConfig::from_raw(RawConfig {
            serper_api_key: Some("serper-test-key".to_owned()),
            serper_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client = SerperClient::from_config_with_base_url(&config.serper, &fixture.base_url)?
            .ok_or_else(|| std::io::Error::other("Serper client was not built"))?;

        let result = client.search(" latest rust ").await?;

        assert_eq!(result, r#"{"organic":[{"title":"ok"}]}"#);
        assert_eq!(
            fixture.state.requests.lock().await.as_slice(),
            &[RecordedRequest {
                method: "POST".to_owned(),
                path: "/search".to_owned(),
                headers: BTreeMap::from([
                    ("content-type".to_owned(), "application/json".to_owned()),
                    ("x-api-key".to_owned(), "serper-test-key".to_owned()),
                ]),
                body: json!({
                    "q": "latest rust",
                    "hl": "ru",
                    "autocorrect": true,
                    "num": 10,
                    "page": 1,
                }),
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn crawl_rejects_private_and_loopback_destinations() -> Result<(), Box<dyn Error>> {
        let config = AppConfig::from_raw(RawConfig {
            serper_api_key: Some("serper-test-key".to_owned()),
            serper_timeout_seconds: Some("1".to_owned()),
            ..RawConfig::default()
        })?;
        let client =
            SerperClient::from_config_with_base_url(&config.serper, "https://google.serper.dev")?
                .ok_or_else(|| std::io::Error::other("Serper client was not built"))?;

        for url in [
            "http://127.0.0.1/private",
            "http://169.254.169.254/latest/meta-data",
            "http://10.0.0.1/internal",
            "http://[::1]/private",
        ] {
            let error = client.crawl_url(url).await.expect_err("URL must be denied");
            assert!(error.to_string().contains("non-public"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn crawl_decodes_gzip_and_rejects_binary_or_oversized_pages() -> Result<(), Box<dyn Error>>
    {
        let gzip: &[u8] = &[
            31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 179, 201, 40, 201, 205, 177, 179, 73, 202, 79, 169,
            180, 11, 168, 44, 201, 200, 207, 83, 72, 43, 74, 77, 213, 45, 201, 40, 74, 77, 76, 201,
            204, 75, 87, 200, 44, 86, 200, 47, 40, 201, 204, 207, 75, 204, 209, 179, 209, 7, 43,
            180, 209, 7, 235, 2, 0, 4, 118, 198, 225, 60, 0, 0, 0,
        ];
        for (body, encoding, expected) in [
            (
                gzip.to_vec(),
                "gzip",
                Ok("Python free-threading is optional."),
            ),
            (b"text\0binary".to_vec(), "identity", Err("binary control")),
            (vec![0xff], "identity", Err("not UTF-8")),
            (
                vec![b'a'; 2 * 1024 * 1024 + 1],
                "identity",
                Err("decoded limit"),
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await?;
                read_request(&mut stream)
                    .await
                    .map_err(std::io::Error::other)?;
                let headers = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: {encoding}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(headers.as_bytes()).await?;
                stream.write_all(&body).await?;
                Ok::<_, std::io::Error>(())
            });
            let response = reqwest::Client::new()
                .get(format!("http://{address}"))
                .send()
                .await?;
            let result = read_crawl_response(response).await;
            match expected {
                Ok(text) => assert_eq!(result?, text),
                Err(message) => assert!(
                    result
                        .expect_err("invalid page")
                        .to_string()
                        .contains(message)
                ),
            }
            server.await??;
        }
        Ok(())
    }

    #[test]
    fn crawl_url_policy_accepts_only_public_addresses() {
        assert!(is_public_destination_ip(
            "93.184.216.34".parse().expect("public IPv4")
        ));
        assert!(is_public_destination_ip(
            "2606:2800:220:1:248:1893:25c8:1946"
                .parse()
                .expect("public IPv6")
        ));
        assert!(!is_public_destination_ip(
            "0.0.0.0".parse().expect("unspecified IPv4")
        ));
        assert!(!is_public_destination_ip(
            "192.168.1.1".parse().expect("private IPv4")
        ));
        assert!(!is_public_destination_ip(
            "fc00::1".parse().expect("private IPv6")
        ));
        assert!(!is_public_destination_ip(
            "2001:db8::1".parse().expect("documentation IPv6")
        ));
        assert!(!is_public_destination_ip(
            "::ffff:127.0.0.1".parse().expect("mapped loopback IPv6")
        ));
        assert_eq!(
            html_body_to_plain_text("<html><body>Hello <b>Plotva</b> &amp; fish</body></html>"),
            "Hello Plotva & fish"
        );
    }

    #[tokio::test]
    async fn crawl_redirects_resolve_locations_and_revalidate_destinations()
    -> Result<(), Box<dyn Error>> {
        for status in [301, 302, 303, 307, 308] {
            let response = redirect_response(status, Some("../notes?version=1")).await?;
            let base = response.url().clone();
            assert_eq!(
                crawl_redirect_target(&response, 0)?.expect("redirect"),
                base.join("../notes?version=1")?
            );
            assert!(
                crawl_redirect_target(&response, CRAWL_MAX_REDIRECTS)
                    .expect_err("bounded redirects")
                    .to_string()
                    .contains("exceeded 5")
            );
        }
        for location in [
            "http://127.0.0.1/internal",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]/internal",
            "http://user:password@example.com/",
            "file:///etc/passwd",
        ] {
            let response = redirect_response(302, Some(location)).await?;
            let target = crawl_redirect_target(&response, 0)?.expect("redirect");
            assert!(validate_public_crawl_url(target.as_str()).await.is_err());
        }
        for location in [None, Some(""), Some("http://[invalid")] {
            let response = redirect_response(302, location).await?;
            assert!(crawl_redirect_target(&response, 0).is_err());
        }
        let response = redirect_response(304, Some("/cached")).await?;
        assert!(crawl_redirect_target(&response, 0)?.is_none());
        assert!(read_crawl_response(response).await.is_err());
        Ok(())
    }

    async fn redirect_response(
        status: u16,
        location: Option<&str>,
    ) -> Result<reqwest::Response, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let location = location
            .map(|value| format!("Location: {value}\r\n"))
            .unwrap_or_default();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await?;
            read_request(&mut stream)
                .await
                .map_err(std::io::Error::other)?;
            stream
                .write_all(
                    format!("HTTP/1.1 {status} Redirect\r\n{location}Content-Length: 0\r\nConnection: close\r\n\r\n")
                        .as_bytes(),
                )
                .await
        });
        let response = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?
            .get(format!("http://{address}/start/page"))
            .send()
            .await?;
        server.await??;
        Ok(response)
    }

    #[test]
    fn retry_policy_matches_go_serper_classification() {
        assert!(should_retry_serper_error("api error: status 429", 0));
        assert!(should_retry_serper_error("request error: dns failure", 1));
        assert!(!should_retry_serper_error("api error: status 401", 0));
        assert!(!should_retry_serper_error("unmarshal response: EOF", 0));
        assert!(!should_retry_serper_error("api error: status 503", 3));
    }

    #[tokio::test]
    #[ignore]
    async fn live_crawl_smoke_reads_text() -> Result<(), Box<dyn Error>> {
        let url = env::var("OPENPLOTVA_CRAWL_SMOKE_URL")?;
        let expected = env::var("OPENPLOTVA_CRAWL_SMOKE_EXPECT")?;
        let config = AppConfig::from_raw(RawConfig {
            serper_api_key: Some("crawl-does-not-use-serper-key".to_owned()),
            serper_timeout_seconds: Some("20".to_owned()),
            ..RawConfig::default()
        })?;
        let client = SerperClient::from_config(&config.serper)?
            .ok_or_else(|| std::io::Error::other("crawler was not built"))?;
        let text = client.crawl_url(&url).await?;
        assert!(
            text.contains(&expected),
            "page did not contain expected text"
        );
        assert!(!text.contains('\0'));
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn live_serper_smoke_searches() -> Result<(), Box<dyn Error>> {
        if env::var("OPENPLOTVA_PROVIDER_SMOKE_SERPER").unwrap_or_default() != "1" {
            eprintln!("set OPENPLOTVA_PROVIDER_SMOKE_SERPER=1 to run the live Serper smoke");
            return Ok(());
        }
        let config = AppConfig::from_env()?;
        let client = SerperClient::from_app_config(&config)?
            .ok_or_else(|| std::io::Error::other("SERPER_API_KEY is required"))?;
        let query = env::var("OPENPLOTVA_SERPER_SMOKE_QUERY")
            .unwrap_or_else(|_| "OpenPlotva Telegram bot".to_owned());
        let result = client.search(&query).await?;
        assert!(!result.trim().is_empty());
        assert!(!result.trim_start().starts_with('<'));
        Ok(())
    }

    async fn spawn_fixture_server() -> Result<Fixture, Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(FixtureState::default());
        let server_state = Arc::clone(&state);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = Arc::clone(&server_state);
                tokio::spawn(async move {
                    let _ = handle_fixture_connection(stream, state).await;
                });
            }
        });
        Ok(Fixture {
            base_url: format!("http://{addr}"),
            state,
        })
    }

    async fn handle_fixture_connection(
        mut stream: TcpStream,
        state: Arc<FixtureState>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let (method, path, headers, body) = read_request(&mut stream).await?;
        let (status, content_type, response_body) = if method == "POST" && path == "/search" {
            let selected_headers = BTreeMap::from([
                (
                    "content-type".to_owned(),
                    headers.get("content-type").cloned().unwrap_or_default(),
                ),
                (
                    "x-api-key".to_owned(),
                    headers.get("x-api-key").cloned().unwrap_or_default(),
                ),
            ]);
            let body_json: Value = serde_json::from_slice(&body)?;
            state.requests.lock().await.push(RecordedRequest {
                method,
                path,
                headers: selected_headers,
                body: body_json,
            });
            (
                "200 OK",
                "application/json",
                r#"{"organic":[{"title":"ok"}]}"#.as_bytes().to_vec(),
            )
        } else if method == "GET" && path == "/page" {
            (
                "200 OK",
                "text/html; charset=utf-8",
                b"<html><body>Hello&nbsp;<b>Plotva</b> &amp; fish</body></html>".to_vec(),
            )
        } else {
            ("404 Not Found", "text/plain", b"missing".to_vec())
        };
        write_response(&mut stream, status, content_type, &response_body).await?;
        Ok(())
    }

    async fn read_request(
        stream: &mut TcpStream,
    ) -> Result<(String, String, BTreeMap<String, String>, Vec<u8>), Box<dyn Error + Send + Sync>>
    {
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                return Err("connection closed before headers".into());
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(pos) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let header_text = String::from_utf8_lossy(&buffer[..header_end]);
        let mut lines = header_text.split("\r\n");
        let request_line = lines.next().ok_or("missing request line")?;
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default().to_owned();
        let path = request_parts.next().unwrap_or_default().to_owned();
        let mut headers = BTreeMap::new();
        for line in lines {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
        let content_length = headers
            .get("content-length")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = buffer[header_end..].to_vec();
        while body.len() < content_length {
            let read = stream.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            body.extend_from_slice(&chunk[..read]);
        }
        body.truncate(content_length);
        Ok((method, path, headers, body))
    }

    async fn write_response(
        stream: &mut TcpStream,
        status: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        stream
            .write_all(
                format!(
                    "HTTP/1.1 {status}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
        stream.write_all(body).await?;
        Ok(())
    }
}
