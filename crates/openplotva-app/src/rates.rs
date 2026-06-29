//! App-level fetcher currency-rates command and dialog tool behavior.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_dialog::{
    TOOL_RESULT_STATUS_EXECUTED, TOOL_RESULT_STATUS_FAILED, TOOL_RESULT_STATUS_NOOP,
    TOOL_RESULT_STATUS_OK, TOOL_RESULT_STATUS_QUEUED, ToolContext, ToolError, ToolResult,
    ToolSideEffect,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML,
    TextMessageRequest,
};
use reqwest::header::USER_AGENT;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, monotonic_virtual_id_factory, queue_text_message_parts,
};

const RATES_UNAVAILABLE_TEXT: &str = "❌ Сервис курсов временно недоступен";
const RATES_FETCH_ERRORS_PREFIX: &str = "❌ Ошибки получения курсов:\n";
pub const RATES_MESSAGE_KIND: &str = "rates_message";
const RATES_TOOL_MODE_MULTI_TURN: &str = "multi_turn";
const RATES_TOOL_MODE_LEGACY: &str = "legacy";
const MARKET_RATES_YAHOO_TTL: Duration = Duration::from_secs(5 * 60);
const MARKET_RATES_DAILY_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const MARKET_RATES_DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
const MARKET_RATES_USER_AGENT: &str = "OpenPlotva/0.1";
const YAHOO_CHART_BASE_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";
const CBR_DAILY_XML_URL: &str = "https://www.cbr.ru/scripts/XML_daily.asp";
const FRANKFURTER_RATES_URL: &str = "https://api.frankfurter.dev/v2/rates";
const FRED_CSV_BASE_URL: &str = "https://fred.stlouisfed.org/graph/fredgraph.csv";

const RATE_ID_USD_RUB: &str = "usd_rub";
const RATE_ID_EUR_RUB: &str = "eur_rub";
const RATE_ID_EUR_USD: &str = "eur_usd";
const RATE_ID_BTC_USD: &str = "btc_usd";
const DEFAULT_RATE_IDS: &[&str] = &[
    RATE_ID_USD_RUB,
    RATE_ID_EUR_RUB,
    RATE_ID_EUR_USD,
    RATE_ID_BTC_USD,
    "brent",
];

/// Boxed future returned by rates providers.
pub type RatesFetchFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<RatesSnapshot, E>> + Send + 'a>>;

/// Boxed future returned by rates text side effects.
pub type RatesEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by rates dialog side-effect dispatchers.
pub type RatesDispatchFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<RatesSideEffectDispatchResult, E>> + Send + 'a>>;

pub trait RatesFetcher {
    /// Error returned by the provider.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Return requested market-rate rows.
    fn fetch_rates<'a>(&'a self, selection: RatesSelection) -> RatesFetchFuture<'a, Self::Error>;
}

#[derive(Clone)]
pub struct MarketRatesClient {
    http: reqwest::Client,
    urls: MarketRateUrls,
    cache: Arc<AsyncRwLock<MarketRatesCache>>,
    // ponytail: one global refresh lock is enough for this low-QPS command; split per URL if rates become hot.
    refresh_lock: Arc<AsyncMutex<()>>,
}

impl MarketRatesClient {
    pub fn from_timeout_seconds(timeout_seconds: i32) -> Result<Self, MarketRatesError> {
        let timeout = if timeout_seconds > 0 {
            Duration::from_secs(timeout_seconds as u64)
        } else {
            MARKET_RATES_DEFAULT_TIMEOUT
        };
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(MarketRatesError::HttpClient)?;
        Ok(Self {
            http,
            urls: MarketRateUrls::default(),
            cache: Arc::new(AsyncRwLock::new(MarketRatesCache::default())),
            refresh_lock: Arc::new(AsyncMutex::new(())),
        })
    }

    /// Build a market-rates client from the app config.
    pub fn from_config(
        config: &openplotva_config::MarketRatesConfig,
    ) -> Result<Self, MarketRatesError> {
        Self::from_timeout_seconds(config.timeout_seconds)
    }

    async fn fetch_selection(
        &self,
        selection: RatesSelection,
    ) -> Result<RatesSnapshot, MarketRatesError> {
        let mut snapshot = RatesSnapshot::default();
        for unknown in selection.unknown {
            snapshot.errors.push(RatesFetchProblem {
                label: unknown,
                message: "unknown rate alias".to_owned(),
            });
        }
        for pair in selection.pairs {
            match self.fetch_cbr_pair_quote(&pair).await {
                Ok(row) => snapshot.rows.push(row),
                Err(error) => snapshot.errors.push(RatesFetchProblem {
                    label: pair.label(),
                    message: error.to_string(),
                }),
            }
        }
        for id in selection.ids {
            let Some(instrument) = rate_instrument(&id) else {
                snapshot.errors.push(RatesFetchProblem {
                    label: id,
                    message: "unknown rate id".to_owned(),
                });
                continue;
            };
            match self.fetch_instrument(instrument).await {
                Ok(row) => snapshot.rows.push(row),
                Err(error) => snapshot.errors.push(RatesFetchProblem {
                    label: instrument.label.to_owned(),
                    message: error.to_string(),
                }),
            }
        }
        Ok(snapshot)
    }

    async fn fetch_cbr_pair_quote(
        &self,
        pair: &RatePairSelection,
    ) -> Result<RateQuote, MarketRatesError> {
        let base = self.cbr_or_rub_rate(&pair.base).await?;
        let quote = self.cbr_or_rub_rate(&pair.quote).await?;
        if quote.value == 0.0 {
            return Err(MarketRatesError::RateNotFound);
        }
        Ok(RateQuote {
            id: pair.id(),
            label: pair.label(),
            value: base.value / quote.value,
            precision: if pair.quote == "RUB" { 2 } else { 4 },
            unit: if pair.quote == "RUB" {
                "₽".to_owned()
            } else {
                pair.quote.clone()
            },
            delta: None,
            source: "cbr_cross".to_owned(),
            timestamp: base.date.or(quote.date),
            stale: base.stale || quote.stale,
        })
    }

    async fn cbr_or_rub_rate(&self, code: &str) -> Result<CbrRate, MarketRatesError> {
        if code.eq_ignore_ascii_case("RUB") {
            return Ok(CbrRate {
                value: 1.0,
                date: None,
                stale: false,
            });
        }
        self.cbr_rate(code).await
    }

    async fn fetch_instrument(
        &self,
        instrument: RateInstrument,
    ) -> Result<RateQuote, MarketRatesError> {
        if let Some(symbol) = instrument.yahoo_symbol {
            match self.fetch_yahoo_quote(instrument, symbol).await {
                Ok(row) => return Ok(row),
                Err(yahoo_error) => {
                    if let Some(series) = instrument.fred_series {
                        return self.fetch_fred_quote(instrument, series).await;
                    }
                    if instrument.cbr_code.is_some() {
                        return self.fetch_cbr_quote(instrument).await;
                    }
                    if let Some((base, quote)) = instrument.frankfurter_pair {
                        return self.fetch_frankfurter_quote(instrument, base, quote).await;
                    }
                    return Err(yahoo_error);
                }
            }
        }
        if instrument.cbr_code.is_some() {
            return self.fetch_cbr_quote(instrument).await;
        }
        if let Some((base, quote)) = instrument.frankfurter_pair {
            return self.fetch_frankfurter_quote(instrument, base, quote).await;
        }
        Err(MarketRatesError::RateNotFound)
    }

    async fn fetch_yahoo_quote(
        &self,
        instrument: RateInstrument,
        symbol: &str,
    ) -> Result<RateQuote, MarketRatesError> {
        let url = self.urls.yahoo_chart_url(symbol);
        let (body, stale) = self.fetch_bytes(&url, MARKET_RATES_YAHOO_TTL).await?;
        let response: YahooChartResponse = serde_json::from_slice(&body)
            .map_err(|error| MarketRatesError::Decode(error.to_string()))?;
        let chart = response.chart;
        if let Some(error) = chart.error {
            return Err(MarketRatesError::Source(error.to_string()));
        }
        let result = chart
            .result
            .and_then(|mut results| results.drain(..).next())
            .ok_or(MarketRatesError::RateNotFound)?;
        let meta = result.meta;
        let closes = result
            .indicators
            .and_then(|indicators| indicators.quote)
            .and_then(|mut quotes| quotes.drain(..).next())
            .and_then(|quote| quote.close)
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .filter(|value| value.is_finite())
            .collect::<Vec<_>>();
        let price = first_market_number([meta.regular_market_price, closes.last().copied()])
            .ok_or(MarketRatesError::RateNotFound)?;
        let range_start = first_market_number([closes.first().copied(), meta.chart_previous_close]);
        let mut delta = range_start.and_then(|start| rate_delta(price, start, RateDeltaBasis::Day));
        if delta.is_none()
            && instrument.kind == RateInstrumentKind::ForexRub
            && let Some(code) = instrument.cbr_code
            && let Ok(cbr) = self.cbr_rate(code).await
        {
            delta = rate_delta(price, cbr.value, RateDeltaBasis::CbrOfficial);
        }
        Ok(RateQuote {
            id: instrument.id.to_owned(),
            label: instrument.label.to_owned(),
            value: price,
            precision: instrument.precision,
            unit: instrument.unit.to_owned(),
            delta,
            source: "yahoo_chart".to_owned(),
            timestamp: first_market_timestamp(meta.regular_market_time, result.timestamp),
            stale,
        })
    }

    async fn fetch_cbr_quote(
        &self,
        instrument: RateInstrument,
    ) -> Result<RateQuote, MarketRatesError> {
        let code = instrument.cbr_code.ok_or(MarketRatesError::RateNotFound)?;
        let cbr = self.cbr_rate(code).await?;
        Ok(RateQuote {
            id: instrument.id.to_owned(),
            label: instrument.label.to_owned(),
            value: cbr.value,
            precision: instrument.precision,
            unit: instrument.unit.to_owned(),
            delta: None,
            source: "cbr".to_owned(),
            timestamp: cbr.date,
            stale: cbr.stale,
        })
    }

    async fn fetch_frankfurter_quote(
        &self,
        instrument: RateInstrument,
        base: &str,
        quote: &str,
    ) -> Result<RateQuote, MarketRatesError> {
        let url = self.urls.frankfurter_url(base, quote);
        let (body, stale) = self.fetch_bytes(&url, MARKET_RATES_DAILY_TTL).await?;
        let value: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|error| MarketRatesError::Decode(error.to_string()))?;
        let rate = frankfurter_rate(&value, quote).ok_or(MarketRatesError::RateNotFound)?;
        Ok(RateQuote {
            id: instrument.id.to_owned(),
            label: instrument.label.to_owned(),
            value: rate,
            precision: instrument.precision,
            unit: instrument.unit.to_owned(),
            delta: None,
            source: "frankfurter".to_owned(),
            timestamp: frankfurter_date(&value),
            stale,
        })
    }

    async fn fetch_fred_quote(
        &self,
        instrument: RateInstrument,
        series: &str,
    ) -> Result<RateQuote, MarketRatesError> {
        let url = self.urls.fred_url(series);
        let (body, stale) = self.fetch_bytes(&url, MARKET_RATES_DAILY_TTL).await?;
        let text =
            String::from_utf8(body).map_err(|error| MarketRatesError::Decode(error.to_string()))?;
        let (value, previous, date) = fred_last_two(&text, series)?;
        Ok(RateQuote {
            id: instrument.id.to_owned(),
            label: instrument.label.to_owned(),
            value,
            precision: instrument.precision,
            unit: instrument.unit.to_owned(),
            delta: previous.and_then(|prev| rate_delta(value, prev, RateDeltaBasis::Day)),
            source: "fred".to_owned(),
            timestamp: date,
            stale,
        })
    }

    async fn cbr_rate(&self, code: &str) -> Result<CbrRate, MarketRatesError> {
        let (body, stale) = self
            .fetch_bytes(&self.urls.cbr_xml_url, MARKET_RATES_DAILY_TTL)
            .await?;
        let text =
            String::from_utf8(body).map_err(|error| MarketRatesError::Decode(error.to_string()))?;
        let parsed: CbrValCurs = quick_xml::de::from_str(&text)
            .map_err(|error| MarketRatesError::Decode(error.to_string()))?;
        parsed
            .valutes
            .into_iter()
            .find(|item| item.char_code.eq_ignore_ascii_case(code))
            .and_then(|item| item.rate(parsed.date.clone(), stale))
            .ok_or(MarketRatesError::RateNotFound)
    }

    async fn fetch_bytes(
        &self,
        url: &str,
        ttl: Duration,
    ) -> Result<(Vec<u8>, bool), MarketRatesError> {
        if let Some(body) = self.cache.read().await.fresh(url, ttl) {
            return Ok((body, false));
        }
        let _guard = self.refresh_lock.lock().await;
        if let Some(body) = self.cache.read().await.fresh(url, ttl) {
            return Ok((body, false));
        }
        match self.fetch_uncached_bytes(url).await {
            Ok(body) => {
                self.cache.write().await.set(url, body.clone());
                Ok((body, false))
            }
            Err(error) => match self.cache.read().await.stale(url) {
                Some(body) => Ok((body, true)),
                None => Err(error),
            },
        }
    }

    async fn fetch_uncached_bytes(&self, url: &str) -> Result<Vec<u8>, MarketRatesError> {
        let response = self
            .http
            .get(url)
            .header(USER_AGENT, MARKET_RATES_USER_AGENT)
            .send()
            .await
            .map_err(|error| MarketRatesError::Request(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(MarketRatesError::HttpStatus(status.as_u16()));
        }
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(|error| MarketRatesError::Request(error.to_string()))
    }

    #[cfg(test)]
    fn with_urls_for_test(mut self, urls: MarketRateUrls) -> Self {
        self.urls = urls;
        self
    }
}

impl fmt::Debug for MarketRatesClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MarketRatesClient").finish_non_exhaustive()
    }
}

impl RatesFetcher for MarketRatesClient {
    type Error = MarketRatesError;

    fn fetch_rates<'a>(&'a self, selection: RatesSelection) -> RatesFetchFuture<'a, Self::Error> {
        Box::pin(async move { self.fetch_selection(selection).await })
    }
}

/// Market-rates provider errors.
#[derive(Debug, Error)]
pub enum MarketRatesError {
    /// HTTP client construction failed.
    #[error("failed to build market-rates HTTP client: {0}")]
    HttpClient(reqwest::Error),
    /// Request failed before an HTTP response arrived.
    #[error("failed to execute market-rates request: {0}")]
    Request(String),
    /// Source returned a non-2xx response.
    #[error("market-rates http status {0}")]
    HttpStatus(u16),
    /// Source returned a structured error.
    #[error("market-rates source error: {0}")]
    Source(String),
    /// Source body did not match the expected response.
    #[error("failed to decode market-rates response: {0}")]
    Decode(String),
    /// No cached, refreshed, or fallback value is available.
    #[error("rate not found")]
    RateNotFound,
}

#[derive(Clone, Debug, Default)]
struct MarketRatesCache {
    bodies: HashMap<String, CachedBody>,
}

impl MarketRatesCache {
    fn set(&mut self, url: &str, body: Vec<u8>) {
        self.bodies.insert(
            url.to_owned(),
            CachedBody {
                body,
                updated_at: Instant::now(),
            },
        );
    }

    fn fresh(&self, url: &str, ttl: Duration) -> Option<Vec<u8>> {
        let item = self.bodies.get(url)?;
        (item.updated_at.elapsed() <= ttl).then(|| item.body.clone())
    }

    fn stale(&self, url: &str) -> Option<Vec<u8>> {
        self.bodies.get(url).map(|item| item.body.clone())
    }
}

#[derive(Clone, Debug)]
struct CachedBody {
    body: Vec<u8>,
    updated_at: Instant,
}

#[derive(Clone, Debug)]
struct MarketRateUrls {
    yahoo_chart_base_url: String,
    cbr_xml_url: String,
    frankfurter_rates_url: String,
    fred_csv_base_url: String,
}

impl Default for MarketRateUrls {
    fn default() -> Self {
        Self {
            yahoo_chart_base_url: YAHOO_CHART_BASE_URL.to_owned(),
            cbr_xml_url: CBR_DAILY_XML_URL.to_owned(),
            frankfurter_rates_url: FRANKFURTER_RATES_URL.to_owned(),
            fred_csv_base_url: FRED_CSV_BASE_URL.to_owned(),
        }
    }
}

impl MarketRateUrls {
    fn yahoo_chart_url(&self, symbol: &str) -> String {
        let encoded: String = url::form_urlencoded::byte_serialize(symbol.as_bytes()).collect();
        format!(
            "{}/{encoded}?range=1d&interval=1m",
            self.yahoo_chart_base_url.trim_end_matches('/')
        )
    }

    fn frankfurter_url(&self, base: &str, quote: &str) -> String {
        format!("{}?base={base}&quotes={quote}", self.frankfurter_rates_url)
    }

    fn fred_url(&self, series: &str) -> String {
        format!("{}?id={series}", self.fred_csv_base_url)
    }
}

#[derive(Debug, Deserialize)]
struct YahooChartResponse {
    chart: YahooChart,
}

#[derive(Debug, Deserialize)]
struct YahooChart {
    #[serde(default)]
    result: Option<Vec<YahooChartResult>>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct YahooChartResult {
    meta: YahooChartMeta,
    #[serde(default)]
    timestamp: Option<Vec<i64>>,
    #[serde(default)]
    indicators: Option<YahooIndicators>,
}

#[derive(Debug, Deserialize)]
struct YahooChartMeta {
    #[serde(default, rename = "regularMarketPrice")]
    regular_market_price: Option<f64>,
    #[serde(default, rename = "regularMarketTime")]
    regular_market_time: Option<i64>,
    #[serde(default, rename = "chartPreviousClose")]
    chart_previous_close: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct YahooIndicators {
    #[serde(default)]
    quote: Option<Vec<YahooQuote>>,
}

#[derive(Debug, Deserialize)]
struct YahooQuote {
    #[serde(default)]
    close: Option<Vec<Option<f64>>>,
}

#[derive(Debug, Deserialize)]
struct CbrValCurs {
    #[serde(default, rename = "@Date")]
    date: Option<String>,
    #[serde(default, rename = "Valute")]
    valutes: Vec<CbrValute>,
}

#[derive(Debug, Deserialize)]
struct CbrValute {
    #[serde(default, rename = "CharCode")]
    char_code: String,
    #[serde(default, rename = "Nominal")]
    nominal: Option<f64>,
    #[serde(default, rename = "Value")]
    value: Option<String>,
    #[serde(default, rename = "VunitRate")]
    vunit_rate: Option<String>,
}

impl CbrValute {
    fn rate(self, date: Option<String>, stale: bool) -> Option<CbrRate> {
        let value = parse_source_number(self.vunit_rate.as_deref()).or_else(|| {
            let nominal = self.nominal.unwrap_or(1.0);
            parse_source_number(self.value.as_deref()).map(|value| value / nominal)
        })?;
        Some(CbrRate { value, date, stale })
    }
}

#[derive(Clone, Debug)]
struct CbrRate {
    value: f64,
    date: Option<String>,
    stale: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RateInstrumentKind {
    ForexRub,
    Forex,
    Crypto,
    Future,
    Index,
    Equity,
    CbrRub,
}

#[derive(Clone, Copy, Debug)]
struct RateInstrument {
    id: &'static str,
    label: &'static str,
    yahoo_symbol: Option<&'static str>,
    fred_series: Option<&'static str>,
    cbr_code: Option<&'static str>,
    frankfurter_pair: Option<(&'static str, &'static str)>,
    precision: usize,
    unit: &'static str,
    kind: RateInstrumentKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RatesSelection {
    pub ids: Vec<String>,
    pub pairs: Vec<RatePairSelection>,
    pub unknown: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RatePairSelection {
    pub base: String,
    pub quote: String,
}

impl RatePairSelection {
    #[must_use]
    pub fn new(base: &str, quote: &str) -> Self {
        Self {
            base: base.trim().to_ascii_uppercase(),
            quote: quote.trim().to_ascii_uppercase(),
        }
    }

    fn id(&self) -> String {
        format!(
            "{}_{}",
            self.base.to_ascii_lowercase(),
            self.quote.to_ascii_lowercase()
        )
    }

    fn label(&self) -> String {
        format!("💱 {}/{}", self.base, self.quote)
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RatesSnapshot {
    pub rows: Vec<RateQuote>,
    pub errors: Vec<RatesFetchProblem>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RateQuote {
    pub id: String,
    pub label: String,
    pub value: f64,
    pub precision: usize,
    pub unit: String,
    pub delta: Option<RateDelta>,
    pub source: String,
    pub timestamp: Option<String>,
    pub stale: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RateDelta {
    pub percent: f64,
    pub basis: RateDeltaBasis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateDeltaBasis {
    Day,
    CbrOfficial,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RatesFetchProblem {
    pub label: String,
    pub message: String,
}

#[must_use]
pub fn parse_rates_selection(raw: &str) -> RatesSelection {
    let explicit = !raw.trim().is_empty();
    let normalized = raw
        .replace("S&P 500", "sp500")
        .replace("s&p 500", "sp500")
        .replace("S&P500", "sp500")
        .replace("s&p500", "sp500");
    let mut ids = Vec::new();
    let mut pairs = Vec::new();
    let mut unknown = Vec::new();
    for token in normalized
        .split(|ch: char| ch.is_whitespace() || matches!(ch, ',' | ';' | '\n'))
        .map(normalize_rate_alias)
        .filter(|token| !token.is_empty())
    {
        if ignored_rate_alias(&token) {
            continue;
        }
        if let Some(pair) = cbr_pair_alias(&token) {
            if !pairs.iter().any(|existing| existing == &pair) {
                pairs.push(pair);
            }
            continue;
        }
        if let Some(id) = rate_alias_to_id(&token) {
            if !ids.iter().any(|existing| existing == id) {
                ids.push(id.to_owned());
            }
            continue;
        }
        if let Some(code) = cbr_quote_alias(&token) {
            for pair in [
                RatePairSelection::new("USD", &code),
                RatePairSelection::new("EUR", &code),
            ] {
                if !pairs.iter().any(|existing| existing == &pair) {
                    pairs.push(pair);
                }
            }
            continue;
        }
        if explicit {
            unknown.push(token);
        }
    }
    if ids.is_empty() && pairs.is_empty() && unknown.is_empty() {
        ids.extend(DEFAULT_RATE_IDS.iter().map(|id| (*id).to_owned()));
    }
    RatesSelection {
        ids,
        pairs,
        unknown,
    }
}

fn normalize_rate_alias(raw: &str) -> String {
    raw.trim()
        .trim_matches(|ch: char| {
            matches!(
                ch,
                '"' | '\'' | '`' | '(' | ')' | '[' | ']' | '{' | '}' | ':' | '!' | '?' | '.'
            )
        })
        .to_lowercase()
        .replace('₽', "rub")
}

fn ignored_rate_alias(token: &str) -> bool {
    matches!(
        token,
        "rub"
            | "rub/rub"
            | "руб"
            | "рубль"
            | "рубля"
            | "курс"
            | "курсы"
            | "rates"
            | "rate"
            | "now"
            | "сейчас"
    )
}

fn rate_alias_to_id(token: &str) -> Option<&'static str> {
    match token {
        "usd" | "dollar" | "доллар" | "долларов" | "usd/rub" | "usdrub" | "rub=x" | "usdrub=x" => {
            Some(RATE_ID_USD_RUB)
        }
        "eur" | "euro" | "евро" | "eur/rub" | "eurrub" | "eurrub=x" => Some(RATE_ID_EUR_RUB),
        "eur/usd" | "eurusd" | "eurusd=x" => Some(RATE_ID_EUR_USD),
        "usd/pln" | "usdpln" | "usdpln=x" => Some("usd_pln"),
        "btc" | "bitcoin" | "биткоин" | "btc/usd" | "btcusd" | "btc-usd" => {
            Some(RATE_ID_BTC_USD)
        }
        "eth" | "ethereum" | "эфир" | "eth/usd" | "ethusd" | "eth-usd" => Some("eth_usd"),
        "gold" | "золото" | "gc=f" => Some("gold"),
        "wti" | "cl=f" => Some("wti"),
        "brent" | "bz=f" => Some("brent"),
        "sp500" | "s&p" | "^gspc" | "gspc" => Some("sp500"),
        "nasdaq" | "^ixic" | "ixic" => Some("nasdaq"),
        "dow" | "dji" | "^dji" => Some("dow"),
        "aapl" | "apple" => Some("aapl"),
        "msft" | "microsoft" => Some("msft"),
        "nvda" | "nvidia" => Some("nvda"),
        "tsla" | "tesla" => Some("tsla"),
        "spcx" | "spacex" => Some("spcx"),
        "cny" | "yuan" | "юань" | "юаня" | "cny/rub" | "cnyrub" => Some("cny_rub"),
        _ => None,
    }
}

fn cbr_pair_alias(token: &str) -> Option<RatePairSelection> {
    let (base, quote) = token.split_once('/')?;
    Some(RatePairSelection::new(
        &supported_cbr_code(base)?,
        &supported_cbr_code(quote)?,
    ))
}

fn cbr_quote_alias(token: &str) -> Option<String> {
    let code = supported_cbr_code(token)?;
    if code == "RUB" || code == "USD" || code == "EUR" {
        return None;
    }
    Some(code)
}

fn supported_cbr_code(token: &str) -> Option<String> {
    let code = token.trim().to_ascii_uppercase();
    if matches!(
        code.as_str(),
        "RUB"
            | "USD"
            | "EUR"
            | "CNY"
            | "AMD"
            | "AZN"
            | "BYN"
            | "KGS"
            | "KZT"
            | "MDL"
            | "TJS"
            | "TMT"
            | "UZS"
    ) {
        Some(code)
    } else {
        None
    }
}

fn rate_instrument(id: &str) -> Option<RateInstrument> {
    Some(match id {
        RATE_ID_USD_RUB => RateInstrument {
            id: RATE_ID_USD_RUB,
            label: "💵 USD/RUB",
            yahoo_symbol: Some("RUB=X"),
            fred_series: None,
            cbr_code: Some("USD"),
            frankfurter_pair: None,
            precision: 2,
            unit: "₽",
            kind: RateInstrumentKind::ForexRub,
        },
        RATE_ID_EUR_RUB => RateInstrument {
            id: RATE_ID_EUR_RUB,
            label: "💶 EUR/RUB",
            yahoo_symbol: Some("EURRUB=X"),
            fred_series: None,
            cbr_code: Some("EUR"),
            frankfurter_pair: None,
            precision: 2,
            unit: "₽",
            kind: RateInstrumentKind::ForexRub,
        },
        RATE_ID_EUR_USD => RateInstrument {
            id: RATE_ID_EUR_USD,
            label: "💶/💵 EUR/USD",
            yahoo_symbol: Some("EURUSD=X"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: Some(("EUR", "USD")),
            precision: 4,
            unit: "",
            kind: RateInstrumentKind::Forex,
        },
        "usd_pln" => RateInstrument {
            id: "usd_pln",
            label: "💵 USD/PLN",
            yahoo_symbol: Some("USDPLN=X"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: Some(("USD", "PLN")),
            precision: 4,
            unit: "zł",
            kind: RateInstrumentKind::Forex,
        },
        RATE_ID_BTC_USD => RateInstrument {
            id: RATE_ID_BTC_USD,
            label: "₿ BTC/USD",
            yahoo_symbol: Some("BTC-USD"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: None,
            precision: 0,
            unit: "$",
            kind: RateInstrumentKind::Crypto,
        },
        "eth_usd" => RateInstrument {
            id: "eth_usd",
            label: "◆ ETH/USD",
            yahoo_symbol: Some("ETH-USD"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: None,
            precision: 0,
            unit: "$",
            kind: RateInstrumentKind::Crypto,
        },
        "gold" => RateInstrument {
            id: "gold",
            label: "🥇 Gold",
            yahoo_symbol: Some("GC=F"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: None,
            precision: 2,
            unit: "$",
            kind: RateInstrumentKind::Future,
        },
        "wti" => RateInstrument {
            id: "wti",
            label: "🛢 WTI",
            yahoo_symbol: Some("CL=F"),
            fred_series: Some("DCOILWTICO"),
            cbr_code: None,
            frankfurter_pair: None,
            precision: 2,
            unit: "$",
            kind: RateInstrumentKind::Future,
        },
        "brent" => RateInstrument {
            id: "brent",
            label: "🛢 Brent",
            yahoo_symbol: Some("BZ=F"),
            fred_series: Some("DCOILBRENTEU"),
            cbr_code: None,
            frankfurter_pair: None,
            precision: 2,
            unit: "$",
            kind: RateInstrumentKind::Future,
        },
        "sp500" => RateInstrument {
            id: "sp500",
            label: "📈 S&P 500",
            yahoo_symbol: Some("^GSPC"),
            fred_series: Some("SP500"),
            cbr_code: None,
            frankfurter_pair: None,
            precision: 2,
            unit: "",
            kind: RateInstrumentKind::Index,
        },
        "nasdaq" => RateInstrument {
            id: "nasdaq",
            label: "📈 Nasdaq",
            yahoo_symbol: Some("^IXIC"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: None,
            precision: 2,
            unit: "",
            kind: RateInstrumentKind::Index,
        },
        "dow" => RateInstrument {
            id: "dow",
            label: "📈 Dow Jones",
            yahoo_symbol: Some("^DJI"),
            fred_series: None,
            cbr_code: None,
            frankfurter_pair: None,
            precision: 2,
            unit: "",
            kind: RateInstrumentKind::Index,
        },
        "aapl" => equity_instrument("aapl", " AAPL", "AAPL"),
        "msft" => equity_instrument("msft", "🪟 MSFT", "MSFT"),
        "nvda" => equity_instrument("nvda", "🟩 NVDA", "NVDA"),
        "tsla" => equity_instrument("tsla", "🚗 TSLA", "TSLA"),
        "spcx" => equity_instrument("spcx", "🚀 SPCX", "SPCX"),
        "cny_rub" => RateInstrument {
            id: "cny_rub",
            label: "🇨🇳 CNY/RUB",
            yahoo_symbol: None,
            fred_series: None,
            cbr_code: Some("CNY"),
            frankfurter_pair: None,
            precision: 2,
            unit: "₽",
            kind: RateInstrumentKind::CbrRub,
        },
        _ => return None,
    })
}

fn equity_instrument(
    id: &'static str,
    label: &'static str,
    symbol: &'static str,
) -> RateInstrument {
    RateInstrument {
        id,
        label,
        yahoo_symbol: Some(symbol),
        fred_series: None,
        cbr_code: None,
        frankfurter_pair: None,
        precision: 2,
        unit: "$",
        kind: RateInstrumentKind::Equity,
    }
}

fn first_market_number<const N: usize>(values: [Option<f64>; N]) -> Option<f64> {
    values.into_iter().flatten().find(|value| value.is_finite())
}

fn parse_source_number(value: Option<&str>) -> Option<f64> {
    value?
        .trim()
        .replace(',', ".")
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn rate_delta(current: f64, previous: f64, basis: RateDeltaBasis) -> Option<RateDelta> {
    if previous == 0.0 {
        return None;
    }
    Some(RateDelta {
        percent: (current - previous) / previous * 100.0,
        basis,
    })
}

fn first_market_timestamp(primary: Option<i64>, fallback: Option<Vec<i64>>) -> Option<String> {
    primary
        .or_else(|| fallback.and_then(|timestamps| timestamps.into_iter().last()))
        .and_then(format_epoch_timestamp)
}

fn format_epoch_timestamp(value: i64) -> Option<String> {
    time::OffsetDateTime::from_unix_timestamp(value)
        .ok()
        .and_then(|time| time.format(&Rfc3339).ok())
}

fn frankfurter_rate(value: &serde_json::Value, quote: &str) -> Option<f64> {
    if let Some(rows) = value.as_array() {
        return rows
            .iter()
            .find(|row| row.get("quote").and_then(serde_json::Value::as_str) == Some(quote))
            .and_then(|row| row.get("rate").and_then(serde_json::Value::as_f64));
    }
    value
        .get("rates")
        .and_then(|rates| rates.get(quote))
        .and_then(serde_json::Value::as_f64)
}

fn frankfurter_date(value: &serde_json::Value) -> Option<String> {
    value
        .get("date")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

fn fred_last_two(
    text: &str,
    series: &str,
) -> Result<(f64, Option<f64>, Option<String>), MarketRatesError> {
    let mut rows = text.lines().filter_map(|line| {
        let (date, raw) = line.split_once(',')?;
        if date == "observation_date" || raw.trim() == "." {
            return None;
        }
        parse_source_number(Some(raw)).map(|value| (date.to_owned(), value))
    });
    let mut previous = None;
    let mut current = None;
    for row in rows.by_ref() {
        previous = current;
        current = Some(row);
    }
    let Some((date, value)) = current else {
        return Err(MarketRatesError::Source(format!(
            "empty FRED series {series}"
        )));
    };
    Ok((value, previous.map(|(_, value)| value), Some(date)))
}

pub trait RatesSendPermission {
    fn can_send_rates_text(&self, chat: &TelegramChat) -> bool;
}

pub trait RatesHeaderProvider {
    /// Return the already-randomized rates header for this user.
    fn rates_header(&self, user_full_name: &str) -> String;
}

pub trait RatesEffects {
    /// Error returned by the concrete sender.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Send one rates text message.
    fn send_rates_text<'a>(&'a self, plan: RatesTextPlan) -> RatesEffectFuture<'a, Self::Error>;
}

/// Generator for virtual-message IDs used by rates command sends.
pub type RatesVirtualIdFactory = VirtualIdFactory;

/// Rich-message-backed rates command side effects.
#[derive(Clone)]
pub struct RatesRichEffects {
    rich: Arc<dyn crate::rich::RichSender>,
}

impl RatesRichEffects {
    /// Build rates effects that reply with a rich message.
    #[must_use]
    pub fn new(rich: Arc<dyn crate::rich::RichSender>) -> Self {
        Self { rich }
    }
}

impl RatesEffects for RatesRichEffects {
    type Error = RatesRichEffectError;

    fn send_rates_text<'a>(&'a self, plan: RatesTextPlan) -> RatesEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat_id = plan
                .message
                .chat
                .as_ref()
                .map_or(plan.reply_to.chat.id, |chat| chat.id);
            let options = openplotva_telegram::RichSendOptions {
                message_thread_id: if plan.message.message_thread_id != 0 {
                    Some(plan.message.message_thread_id)
                } else if plan.reply_to.is_topic_message {
                    Some(plan.reply_to.message_thread_id)
                } else {
                    None
                },
                reply_to_message_id: Some(plan.reply_to.message_id),
                allow_sending_without_reply: plan
                    .message
                    .allow_sending_without_reply
                    .unwrap_or(false),
                disable_notification: plan.message.disable_notification,
                reply_markup: None,
            };
            self.rich
                .send_rich(chat_id, &plan.message.text, &options)
                .await
                .map_err(|err| RatesRichEffectError::Send(err.to_string()))?;
            Ok(())
        })
    }
}

/// Recoverable errors from rich rates effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RatesRichEffectError {
    #[error("failed to send rates rich message: {0}")]
    Send(String),
}

/// Rich-message-backed dialog-tool rates side effects.
#[derive(Clone)]
pub struct RatesToolRichEffects {
    rich: Arc<dyn crate::rich::RichSender>,
}

impl RatesToolRichEffects {
    /// Build dialog rates effects that send the rich rates table directly.
    #[must_use]
    pub fn new(rich: Arc<dyn crate::rich::RichSender>) -> Self {
        Self { rich }
    }
}

impl RatesSideEffectDispatcher for RatesToolRichEffects {
    type Error = RatesRichEffectError;

    fn dispatch_rates<'a>(
        &'a self,
        meta: RatesToolInvocationContext,
        text: String,
    ) -> RatesDispatchFuture<'a, Self::Error> {
        Box::pin(async move {
            let trimmed = text.trim().to_owned();
            if trimmed.is_empty() {
                return Ok(RatesSideEffectDispatchResult {
                    kind: RATES_MESSAGE_KIND.to_owned(),
                    state: "noop".to_owned(),
                    ..RatesSideEffectDispatchResult::default()
                });
            }

            let options = openplotva_telegram::RichSendOptions {
                message_thread_id: meta.thread_id.map(i64::from),
                reply_to_message_id: Some(i64::from(meta.message_id)),
                allow_sending_without_reply: true,
                disable_notification: false,
                reply_markup: None,
            };
            let sent_message_id = self
                .rich
                .send_rich(meta.chat_id, &trimmed, &options)
                .await
                .map_err(|err| RatesRichEffectError::Send(err.to_string()))?;

            Ok(RatesSideEffectDispatchResult {
                kind: RATES_MESSAGE_KIND.to_owned(),
                ticket_id: sent_message_id.to_string(),
                eta: String::new(),
                state: "executed".to_owned(),
            })
        })
    }
}

/// Generator for async rates side-effect ticket IDs.
pub type RatesTicketFactory = Arc<dyn Fn() -> String + Send + Sync>;

/// Dispatcher-backed dialog-tool rates side effects.
#[derive(Clone)]
pub struct RatesToolDispatcherEffects {
    queue: Arc<DispatcherQueue>,
    next_virtual_id: RatesVirtualIdFactory,
    next_ticket_id: RatesTicketFactory,
}

impl RatesToolDispatcherEffects {
    /// Build dialog rates effects backed by the normal outbound dispatcher.
    #[must_use]
    pub fn new(queue: Arc<DispatcherQueue>) -> Self {
        Self {
            queue,
            next_virtual_id: monotonic_virtual_id_factory("rates-tool-vmsg"),
            next_ticket_id: Arc::new(|| {
                time::OffsetDateTime::now_utc()
                    .unix_timestamp_nanos()
                    .to_string()
            }),
        }
    }

    /// Override ID generation for deterministic tests.
    #[must_use]
    pub fn with_id_factories(
        mut self,
        next_virtual_id: RatesVirtualIdFactory,
        next_ticket_id: RatesTicketFactory,
    ) -> Self {
        self.next_virtual_id = next_virtual_id;
        self.next_ticket_id = next_ticket_id;
        self
    }
}

impl fmt::Debug for RatesToolDispatcherEffects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RatesToolDispatcherEffects")
            .finish_non_exhaustive()
    }
}

impl RatesSideEffectDispatcher for RatesToolDispatcherEffects {
    type Error = RatesToolDispatchEffectError;

    fn dispatch_rates<'a>(
        &'a self,
        meta: RatesToolInvocationContext,
        text: String,
    ) -> RatesDispatchFuture<'a, Self::Error> {
        Box::pin(async move {
            let trimmed = text.trim().to_owned();
            if trimmed.is_empty() {
                return Ok(RatesSideEffectDispatchResult {
                    kind: RATES_MESSAGE_KIND.to_owned(),
                    state: "noop".to_owned(),
                    ..RatesSideEffectDispatchResult::default()
                });
            }

            let chat = ChatRef {
                id: meta.chat_id,
                is_forum: meta.thread_id.is_some(),
            };
            let message = TextMessageRequest {
                chat: Some(chat),
                message_thread_id: meta.thread_id.map_or(0, i64::from),
                disable_notification: false,
                allow_sending_without_reply: None,
                text: trimmed,
                render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
                reply_markup: None,
            };
            let reply_to = ReplyMessageRef {
                message_id: i64::from(meta.message_id),
                chat,
                is_topic_message: meta.thread_id.is_some(),
                message_thread_id: i64::from(meta.thread_id.unwrap_or_default()),
            };
            queue_text_message_parts(
                &self.queue,
                QueueTextRequest {
                    message: &message,
                    reply_to: Some(&reply_to),
                    immediate_first: false,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: None,
                },
                || (self.next_virtual_id)(),
            )
            .await?;

            let is_legacy = meta
                .mode
                .trim()
                .eq_ignore_ascii_case(RATES_TOOL_MODE_LEGACY);
            Ok(RatesSideEffectDispatchResult {
                kind: RATES_MESSAGE_KIND.to_owned(),
                ticket_id: if is_legacy {
                    String::new()
                } else {
                    (self.next_ticket_id)()
                },
                eta: String::new(),
                state: if is_legacy { "executed" } else { "queued" }.to_owned(),
            })
        })
    }
}

/// Recoverable errors from dispatcher-backed dialog rates side effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RatesToolDispatchEffectError {
    #[error("failed to queue rates side effect: {0}")]
    Queue(#[from] OutboundBuildError),
}

pub trait RatesSideEffectDispatcher {
    /// Error returned by the dispatcher.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Dispatch the rates tool side-effect message.
    fn dispatch_rates<'a>(
        &'a self,
        meta: RatesToolInvocationContext,
        text: String,
    ) -> RatesDispatchFuture<'a, Self::Error>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct RatesBotIdentity {
    /// Current Telegram bot user.
    pub user: TelegramUser,
}

/// Planned rates text send.
#[derive(Clone, Debug, PartialEq)]
pub struct RatesTextPlan {
    pub message: TextMessageRequest,
    pub reply_to: ReplyMessageRef,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RatesToolInvocationContext {
    /// Telegram chat ID.
    pub chat_id: i64,
    /// Forum topic ID, when present.
    pub thread_id: Option<i32>,
    /// Source message ID.
    pub message_id: i32,
    /// Caller user ID.
    pub user_id: i64,
    /// Caller display name.
    pub user_full_name: String,
    /// Source message text.
    pub message_text: String,
    /// Dialog mode.
    pub mode: String,
    /// Message metadata.
    pub message_meta: openplotva_core::ChatMessageMeta,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RatesSideEffectDispatchResult {
    /// Side-effect kind.
    pub kind: String,
    /// Async dispatch ticket ID.
    pub ticket_id: String,
    /// Optional ETA.
    pub eta: String,
    /// Dispatch state.
    pub state: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RatesCommandOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    PermissionDenied,
    UnavailableSent,
    /// Rates client was missing and the unavailable text send failed.
    UnavailableSendError {
        message: String,
    },
    FetchErrorsSent {
        errors: Vec<String>,
    },
    /// Provider returned errors and the error text send failed.
    FetchErrorsSendError {
        /// Labeled provider errors.
        errors: Vec<String>,
        /// Sender failure.
        message: String,
    },
    /// Rates response send succeeded.
    Sent,
    /// Rates response send failed.
    SendError {
        message: String,
    },
}

/// Fatal errors from the rates command wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RatesCommandError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct RatesCommandUpdateHandler<Permission, Fetcher, Header, Effects, Next> {
    bot: RatesBotIdentity,
    permission: Arc<Permission>,
    fetcher: Option<Arc<Fetcher>>,
    header: Arc<Header>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Permission, Fetcher, Header, Effects, Next>
    RatesCommandUpdateHandler<Permission, Fetcher, Header, Effects, Next>
{
    /// Build a rates command handler around the real downstream handler.
    pub fn new(
        bot: RatesBotIdentity,
        permission: Arc<Permission>,
        fetcher: Option<Arc<Fetcher>>,
        header: Arc<Header>,
        effects: Arc<Effects>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            bot,
            permission,
            fetcher,
            header,
            effects,
            next,
        }
    }
}

impl<Permission, Fetcher, Header, Effects, Next> UpdateHandler
    for RatesCommandUpdateHandler<Permission, Fetcher, Header, Effects, Next>
where
    Permission: RatesSendPermission + Send + Sync,
    Fetcher: RatesFetcher + Send + Sync,
    Header: RatesHeaderProvider + Send + Sync,
    Effects: RatesEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = RatesCommandError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_rates_command_update_or_else(
                &self.bot,
                self.permission.as_ref(),
                self.fetcher.as_deref(),
                self.header.as_ref(),
                self.effects.as_ref(),
                update,
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_rates_command_update_or_else<
    Permission,
    Fetcher,
    Header,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    bot: &RatesBotIdentity,
    permission: &Permission,
    fetcher: Option<&Fetcher>,
    header: &Header,
    effects: &Effects,
    update: TelegramUpdate,
    handle_other: HandleFn,
) -> Result<RatesCommandOutcome, RatesCommandError>
where
    Permission: RatesSendPermission + Sync,
    Fetcher: RatesFetcher + Sync,
    Header: RatesHeaderProvider + Sync,
    Effects: RatesEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| RatesCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(RatesCommandOutcome::Delegated);
    };

    if !is_rates_command_message(message, &bot.user) {
        handle_other(update)
            .await
            .map_err(|error| RatesCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(RatesCommandOutcome::Delegated);
    }

    if !permission.can_send_rates_text(&message.chat) {
        return Ok(RatesCommandOutcome::PermissionDenied);
    }

    let Some(fetcher) = fetcher else {
        return Ok(send_rates_plan(effects, unavailable_rates_plan(message))
            .await
            .map_or_else(
                |error| RatesCommandOutcome::UnavailableSendError {
                    message: error.to_string(),
                },
                |()| RatesCommandOutcome::UnavailableSent,
            ));
    };

    let selection = rates_command_selection(message, &bot.user);
    let snapshot = fetch_dialog_rates(fetcher, selection).await;
    let errors = command_rates_errors(&snapshot);
    if snapshot.rows.is_empty() && !errors.is_empty() {
        let text = format!("{RATES_FETCH_ERRORS_PREFIX}{}", errors.join("\n"));
        return Ok(
            match send_rates_plan(effects, rates_text_plan(message, text)).await {
                Ok(()) => RatesCommandOutcome::FetchErrorsSent { errors },
                Err(error) => RatesCommandOutcome::FetchErrorsSendError {
                    errors,
                    message: error.to_string(),
                },
            },
        );
    }

    let text = format_rates_command_message(
        &header.rates_header(&message_user_full_name(message)),
        &snapshot,
    );
    Ok(send_rates_plan(effects, rates_text_plan(message, text))
        .await
        .map_or_else(
            |error| RatesCommandOutcome::SendError {
                message: error.to_string(),
            },
            |()| RatesCommandOutcome::Sent,
        ))
}

#[must_use]
pub fn is_rates_command_message(message: &TelegramMessage, bot: &TelegramUser) -> bool {
    let parsed = openplotva_updates::parse_if_addressed(message, bot);
    matches!(parsed.first_word.to_lowercase().as_str(), "$" | ";")
}

fn rates_command_selection(message: &TelegramMessage, bot: &TelegramUser) -> RatesSelection {
    let parsed = openplotva_updates::parse_if_addressed(message, bot);
    parse_rates_selection(&parsed.rest_text)
}

pub async fn fetch_dialog_rates<Fetcher>(
    fetcher: &Fetcher,
    selection: RatesSelection,
) -> RatesSnapshot
where
    Fetcher: RatesFetcher + Sync,
{
    fetcher
        .fetch_rates(selection)
        .await
        .unwrap_or_else(|error| RatesSnapshot {
            rows: Vec::new(),
            errors: vec![RatesFetchProblem {
                label: "rates".to_owned(),
                message: error.to_string(),
            }],
        })
}

pub async fn currency_rates_tool<Fetcher>(
    fetcher: Option<&Fetcher>,
    meta: &ToolContext,
    pairs: &str,
) -> ToolResult
where
    Fetcher: RatesFetcher + Sync,
{
    let Some(fetcher) = fetcher else {
        return ToolResult::failed("currency_rates_unavailable", "rates service unavailable");
    };

    let snapshot = fetch_dialog_rates(fetcher, parse_rates_selection(pairs)).await;
    let errors = dialog_rates_errors(&snapshot);
    if snapshot.rows.is_empty() && !errors.is_empty() {
        return dialog_rates_failure_result(&errors);
    }

    let text = format_dialog_rates_message(&snapshot);
    let _ = meta;
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message: text.clone(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({
            "rates_text": text,
            "rows": snapshot.rows.iter().map(dialog_rate_row_json).collect::<Vec<_>>(),
            "errors": errors,
        })),
        error: None,
    }
}

fn dialog_rate_row_json(row: &RateQuote) -> serde_json::Value {
    json!({
        "id": row.id.clone(),
        "label": row.label.clone(),
        "value": row.value,
        "formatted": format_rate_quote_value(row),
        "delta": row.delta.map(format_rate_quote_delta).unwrap_or_default(),
        "source": row.source.clone(),
        "timestamp": row.timestamp.clone(),
        "stale": row.stale,
    })
}

#[must_use]
pub fn command_rates_errors(rates: &RatesSnapshot) -> Vec<String> {
    rates_error_lines(rates)
}

#[must_use]
pub fn dialog_rates_errors(rates: &RatesSnapshot) -> Vec<String> {
    rates_error_lines(rates)
}

fn rates_error_lines(rates: &RatesSnapshot) -> Vec<String> {
    rates
        .errors
        .iter()
        .map(|error| format!("{}: {}", error.label, error.message))
        .collect()
}

#[must_use]
pub fn format_rates_command_message(header: &str, rates: &RatesSnapshot) -> String {
    let rows = rates
        .rows
        .iter()
        .map(|row| crate::rich::RateRow {
            label: row.label.clone(),
            value: format_rate_quote_value(row),
            delta: row.delta.map(format_rate_quote_delta).unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    crate::rich::compose_rates_table(header, &rows, &format_rates_footer(rates))
}

#[must_use]
pub fn format_dialog_rates_message(rates: &RatesSnapshot) -> String {
    rates
        .rows
        .iter()
        .map(|row| {
            let delta = row.delta.map(format_rate_quote_delta).unwrap_or_default();
            if delta.is_empty() {
                format!("{}={}", row.label, format_rate_quote_value(row))
            } else {
                format!("{}={} ({delta})", row.label, format_rate_quote_value(row))
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[must_use]
pub fn dialog_rates_tool_context(meta: &ToolContext) -> RatesToolInvocationContext {
    RatesToolInvocationContext {
        chat_id: meta.chat_id,
        thread_id: meta.thread_id,
        message_id: meta.message_id,
        user_id: meta.user_id,
        user_full_name: meta.user_full_name.clone(),
        message_text: meta.message_text.clone(),
        mode: RATES_TOOL_MODE_MULTI_TURN.to_owned(),
        message_meta: meta.message_meta.clone(),
    }
}

#[must_use]
pub fn dialog_rates_failure_result(errors: &[String]) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_FAILED.to_owned(),
        message: "rates fetch failed".to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({ "errors": errors })),
        error: Some(ToolError {
            code: "currency_rates_failed".to_owned(),
            reason: errors.join("; "),
            retryable: true,
        }),
    }
}

#[must_use]
pub fn dialog_rates_dispatch_failure_result(
    error_text: &str,
    error: &impl fmt::Display,
) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_FAILED.to_owned(),
        message: "rates fetched but side effect dispatch failed".to_owned(),
        no_reply: false,
        side_effect: None,
        data: Some(json!({ "rates_text": error_text })),
        error: Some(ToolError {
            code: "currency_rates_side_effect_failed".to_owned(),
            reason: error.to_string(),
            retryable: true,
        }),
    }
}

#[must_use]
pub fn dialog_rates_queued_result(
    text: &str,
    dispatch: &RatesSideEffectDispatchResult,
) -> ToolResult {
    let status = match dispatch.state.trim().to_ascii_lowercase().as_str() {
        "queued" => TOOL_RESULT_STATUS_QUEUED,
        "noop" => TOOL_RESULT_STATUS_NOOP,
        _ => TOOL_RESULT_STATUS_EXECUTED,
    };
    let no_reply = !dispatch.state.eq_ignore_ascii_case("noop");
    ToolResult {
        status: status.to_owned(),
        message: text.to_owned(),
        no_reply,
        side_effect: Some(ToolSideEffect {
            kind: dispatch.kind.clone(),
            ticket_id: dispatch.ticket_id.clone(),
            eta: dispatch.eta.clone(),
            state: dispatch.state.clone(),
        }),
        data: Some(json!({
            "rates_text": text,
            "dispatch": {
                "kind": dispatch.kind,
                "ticket_id": dispatch.ticket_id,
                "state": dispatch.state,
            },
        })),
        error: None,
    }
}

#[must_use]
pub fn percent_change(prev: f64, curr: f64) -> f64 {
    if prev == 0.0 {
        return 0.0;
    }
    (curr - prev) / prev * 100.0
}

#[must_use]
pub fn format_rate_delta(prev: f64, curr: f64) -> String {
    if prev == 0.0 {
        return "flat".to_owned();
    }
    let percent = percent_change(prev, curr);
    match percent.total_cmp(&0.0) {
        std::cmp::Ordering::Greater => format!("+{}%", format_rate_value(percent, 1)),
        std::cmp::Ordering::Less => format!("{}%", format_rate_value(percent, 1)),
        std::cmp::Ordering::Equal => "0.0%".to_owned(),
    }
}

fn format_rate_quote_value(row: &RateQuote) -> String {
    let value = format_rate_value(row.value, row.precision);
    if row.unit.is_empty() {
        value
    } else {
        format!("{value} {}", row.unit)
    }
}

fn format_rate_quote_delta(delta: RateDelta) -> String {
    let base = match delta.percent.total_cmp(&0.0) {
        std::cmp::Ordering::Greater => format!("+{}%", format_rate_value(delta.percent, 1)),
        std::cmp::Ordering::Less => format!("{}%", format_rate_value(delta.percent, 1)),
        std::cmp::Ordering::Equal => "0.0%".to_owned(),
    };
    match delta.basis {
        RateDeltaBasis::Day => base,
        RateDeltaBasis::CbrOfficial => format!("{base} к ЦБ"),
    }
}

fn format_rates_footer(rates: &RatesSnapshot) -> String {
    let mut parts = Vec::new();
    if rates.rows.iter().any(|row| row.stale) {
        parts.push("часть данных из кэша".to_owned());
    }
    if !rates.errors.is_empty() && !rates.rows.is_empty() {
        parts.push(format!(
            "не удалось: {}",
            rates_error_lines(rates).join("; ")
        ));
    }
    parts.join(" · ")
}

fn unavailable_rates_plan(message: &TelegramMessage) -> RatesTextPlan {
    rates_text_plan(message, RATES_UNAVAILABLE_TEXT.to_owned())
}

fn rates_text_plan(message: &TelegramMessage, text: String) -> RatesTextPlan {
    let chat = ChatRef {
        id: message.chat.get_id().into(),
        is_forum: message.message_thread_id.is_some(),
    };
    RatesTextPlan {
        message: TextMessageRequest {
            chat: Some(chat),
            message_thread_id: 0,
            disable_notification: false,
            allow_sending_without_reply: None,
            text,
            render_as: TELEGRAM_PARSE_MODE_HTML.to_owned(),
            reply_markup: None,
        },
        reply_to: ReplyMessageRef {
            message_id: message.id,
            chat,
            is_topic_message: message.message_thread_id.is_some(),
            message_thread_id: message.message_thread_id.unwrap_or_default(),
        },
    }
}

async fn send_rates_plan<Effects>(
    effects: &Effects,
    plan: RatesTextPlan,
) -> Result<(), Effects::Error>
where
    Effects: RatesEffects + Sync,
{
    effects.send_rates_text(plan).await
}

fn format_rate_value(value: f64, precision: usize) -> String {
    format!("{value:.precision$}")
}

fn message_user_full_name(message: &TelegramMessage) -> String {
    let sender = openplotva_updates::resolve_message_sender(Some(message));
    let user = message.sender.get_user();
    let user_full_name = user.map(user_full_name).unwrap_or_default();
    if user_full_name.trim().is_empty() {
        sender.display_name()
    } else {
        user_full_name
    }
}

fn user_full_name(user: &TelegramUser) -> String {
    format!(
        "{} {}",
        user.first_name,
        user.last_name.as_deref().unwrap_or_default()
    )
    .trim()
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::updates::{UpdateHandler, UpdateHandlerFuture};
    use serde_json::json;
    use std::{
        io::{self, Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex, MutexGuard},
        thread,
        time::Duration as StdDuration,
    };

    #[tokio::test]
    async fn rates_command_handles_unaddressed_group_dollar_and_sends_html()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let fetcher = RatesFetcherStub::successful();
        let effects = EffectsStub::default();
        let next = NextStub::default();

        let outcome = handle_rates_command_update_or_else(
            &bot,
            &PermissionStub::allowed(),
            Some(&fetcher),
            &HeaderStub,
            &effects,
            sample_update(group_message("$ now", 77)?)?,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, RatesCommandOutcome::Sent);
        assert!(next.calls().is_empty());
        assert_eq!(
            fetcher.calls(),
            vec!["fetch usd_rub,eur_rub,eur_usd,btc_usd,brent"]
        );
        let sent = effects.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(sent[0].reply_to.message_id, 77);
        assert!(
            sent[0]
                .message
                .text
                .starts_with("<h3>Header for Ada Lovelace</h3><table")
        );
        assert!(sent[0].message.text.contains("<td>💵 USD/RUB</td>"));
        assert!(sent[0].message.text.contains("90.00 ₽"));
        assert!(sent[0].message.text.contains("65000 $"));
        Ok(())
    }

    #[tokio::test]
    async fn rates_update_handler_sends_rich_message_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let mock = Arc::new(crate::rich::MockRichSender::default());
        let next = Arc::new(NextStub::default());
        let handler = RatesCommandUpdateHandler::new(
            bot_identity()?,
            Arc::new(PermissionStub::allowed()),
            Some(Arc::new(RatesFetcherStub::successful())),
            Arc::new(HeaderStub),
            Arc::new(RatesRichEffects::new(mock.clone())),
            Arc::clone(&next),
        );

        handler
            .handle_update(sample_update(group_message("$ now", 82)?)?)
            .await?;

        assert!(next.calls().is_empty());
        let sent = mock
            .sent
            .lock()
            .expect("rates rich effects sent mutex should not be poisoned");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].chat_id, -10042);
        assert_eq!(sent[0].message_thread_id, Some(9));
        assert_eq!(sent[0].reply_to_message_id, Some(82));
        assert!(
            sent[0]
                .html
                .starts_with("<h3>Header for Ada Lovelace</h3><table")
        );
        assert!(sent[0].html.contains("<td>💵 USD/RUB</td>"));
        assert!(sent[0].html.contains("90.00 ₽"));
        assert!(sent[0].html.contains("₿ BTC/USD"));
        Ok(())
    }

    #[tokio::test]
    async fn rates_command_accepts_bot_name_prefix_but_delegates_other_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let effects = EffectsStub::default();
        let next = NextStub::default();

        let outcome = handle_rates_command_update_or_else(
            &bot,
            &PermissionStub::allowed(),
            Some(&RatesFetcherStub::successful()),
            &HeaderStub,
            &effects,
            sample_update(group_message("Plotva, ;", 78)?)?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(outcome, RatesCommandOutcome::Sent);
        assert!(next.calls().is_empty());

        let outcome = handle_rates_command_update_or_else(
            &bot,
            &PermissionStub::allowed(),
            Some(&RatesFetcherStub::successful()),
            &HeaderStub,
            &EffectsStub::default(),
            sample_update(group_message("hello", 79)?)?,
            |update| next.handle_update(update),
        )
        .await?;
        assert_eq!(outcome, RatesCommandOutcome::Delegated);
        assert_eq!(next.calls(), vec![500]);
        Ok(())
    }

    #[tokio::test]
    async fn rates_command_missing_provider_sends_go_unavailable_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();

        let outcome = handle_rates_command_update_or_else(
            &bot_identity()?,
            &PermissionStub::allowed(),
            None::<&RatesFetcherStub>,
            &HeaderStub,
            &effects,
            sample_update(private_message("$", 80)?)?,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, RatesCommandOutcome::UnavailableSent);
        let sent = effects.sent();
        assert_eq!(sent[0].message.text, RATES_UNAVAILABLE_TEXT);
        assert_eq!(sent[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        Ok(())
    }

    #[tokio::test]
    async fn rates_command_errors_keep_legacy_btc_label() -> Result<(), Box<dyn std::error::Error>>
    {
        let fetcher = RatesFetcherStub::failing_btc();
        let effects = EffectsStub::default();

        let outcome = handle_rates_command_update_or_else(
            &bot_identity()?,
            &PermissionStub::allowed(),
            Some(&fetcher),
            &HeaderStub,
            &effects,
            sample_update(private_message("$ btc", 81)?)?,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            RatesCommandOutcome::FetchErrorsSent {
                errors: vec!["₿ BTC/USD: btc down".to_owned()]
            }
        );
        assert_eq!(
            effects.sent()[0].message.text,
            "❌ Ошибки получения курсов:\n₿ BTC/USD: btc down"
        );
        Ok(())
    }

    #[tokio::test]
    async fn currency_rates_tool_preserves_dialog_result_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let dispatcher = DispatcherStub::default();
        let meta = tool_context();

        let result = currency_rates_tool(Some(&RatesFetcherStub::successful()), &meta, "").await;

        assert_eq!(result.status, TOOL_RESULT_STATUS_OK);
        assert!(!result.no_reply);
        assert!(result.side_effect.is_none());
        assert!(result.message.contains("💵 USD/RUB=90.00 ₽ (+1.1%)"));
        assert!(result.message.contains("₿ BTC/USD=65000 $"));
        assert_eq!(
            result.data.as_ref().expect("data")["rates_text"],
            result.message
        );
        assert_eq!(dispatcher.calls().len(), 0);

        let missing = currency_rates_tool(None::<&RatesFetcherStub>, &meta, "").await;
        assert_eq!(
            missing,
            ToolResult::failed("currency_rates_unavailable", "rates service unavailable")
        );
        Ok(())
    }

    #[tokio::test]
    async fn currency_rates_tool_errors_use_dialog_btc_pair_label()
    -> Result<(), Box<dyn std::error::Error>> {
        let result = currency_rates_tool(
            Some(&RatesFetcherStub::failing_btc()),
            &tool_context(),
            "btc",
        )
        .await;

        assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(result.message, "rates fetch failed");
        assert_eq!(
            result.data,
            Some(json!({ "errors": ["₿ BTC/USD: btc down"] }))
        );
        assert_eq!(
            result.error,
            Some(ToolError {
                code: "currency_rates_failed".to_owned(),
                reason: "₿ BTC/USD: btc down".to_owned(),
                retryable: true,
            })
        );
        Ok(())
    }

    #[test]
    fn rate_formatters_match_go_edge_cases() {
        assert_eq!(format_rate_delta(0.0, 90.0), "flat");
        assert_eq!(format_rate_delta(100.0, 100.0), "0.0%");
        assert_eq!(format_rate_delta(100.0, 98.0), "-2.0%");
        let rendered = format_rates_command_message("H", &RatesFetcherStub::successful_snapshot());
        assert!(rendered.starts_with("<h3>H</h3><table bordered striped>"));
        assert!(rendered.contains("<td>💵 USD/RUB</td>"));
        assert!(rendered.contains("90.00 ₽"));
        assert!(rendered.contains("BTC/USD"));
        assert!(rendered.contains("Brent"));
    }

    #[test]
    fn rates_selection_uses_default_or_known_aliases_only() {
        assert_eq!(parse_rates_selection("").ids, DEFAULT_RATE_IDS);
        assert_eq!(
            parse_rates_selection("btc eth").ids,
            vec!["btc_usd".to_owned(), "eth_usd".to_owned()]
        );
        assert_eq!(
            parse_rates_selection("usd eur").ids,
            vec![RATE_ID_USD_RUB.to_owned(), RATE_ID_EUR_RUB.to_owned()]
        );
        assert_eq!(parse_rates_selection("wat").unknown, vec!["wat".to_owned()]);
    }

    #[test]
    fn rates_selection_supports_cbr_cis_quote_and_pairs() {
        let byn = parse_rates_selection("BYN");
        assert!(byn.ids.is_empty());
        assert_eq!(
            byn.pairs,
            vec![
                RatePairSelection::new("USD", "BYN"),
                RatePairSelection::new("EUR", "BYN"),
            ]
        );
        assert!(byn.unknown.is_empty());

        for code in [
            "AMD", "AZN", "BYN", "KGS", "KZT", "MDL", "TJS", "TMT", "UZS",
        ] {
            let selection = parse_rates_selection(code);
            assert_eq!(selection.pairs[0], RatePairSelection::new("USD", code));
            assert_eq!(selection.pairs[1], RatePairSelection::new("EUR", code));
        }

        assert_eq!(
            parse_rates_selection("USD/BYN").pairs,
            vec![RatePairSelection::new("USD", "BYN")]
        );
        assert_eq!(
            parse_rates_selection("BYN/USD").pairs,
            vec![RatePairSelection::new("BYN", "USD")]
        );
        assert_eq!(parse_rates_selection("XYZ").unknown, vec!["xyz".to_owned()]);
    }

    #[tokio::test]
    async fn market_rates_client_parses_yahoo_chart_and_uses_cache()
    -> Result<(), Box<dyn std::error::Error>> {
        let (urls, server) =
            spawn_market_fixture_server(vec![FixtureResponse::ok(yahoo_fixture())])?;
        let client = market_test_client(urls)?;

        let first = fetch_dialog_rates(&client, parse_rates_selection("btc")).await;
        let second = fetch_dialog_rates(&client, parse_rates_selection("btc")).await;

        assert_eq!(first.rows[0].value, 65000.0);
        assert_eq!(first.rows[0].source, "yahoo_chart");
        assert_eq!(
            first.rows[0]
                .delta
                .expect("BTC row should include day delta")
                .basis,
            RateDeltaBasis::Day
        );
        assert_eq!(second.rows[0].value, 65000.0);
        let requests = server
            .join()
            .map_err(|_| io::Error::other("market fixture server panicked"))??;
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("/chart/BTC-USD?"));
        Ok(())
    }

    #[tokio::test]
    async fn market_rates_client_parses_cbr_xml() -> Result<(), Box<dyn std::error::Error>> {
        let (urls, server) = spawn_market_fixture_server(vec![FixtureResponse::ok(cbr_fixture())])?;
        let client = market_test_client(urls)?;

        let snapshot = fetch_dialog_rates(&client, parse_rates_selection("cny")).await;

        assert_eq!(snapshot.rows[0].label, "🇨🇳 CNY/RUB");
        assert_eq!(snapshot.rows[0].value, 10.5);
        assert_eq!(snapshot.rows[0].timestamp.as_deref(), Some("19.06.2026"));
        let requests = server
            .join()
            .map_err(|_| io::Error::other("market fixture server panicked"))??;
        assert_eq!(requests, vec!["/cbr".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn market_rates_client_fetches_cbr_cross_pairs() -> Result<(), Box<dyn std::error::Error>>
    {
        let (urls, server) = spawn_market_fixture_server(vec![FixtureResponse::ok(cbr_fixture())])?;
        let client = market_test_client(urls)?;

        let byn = fetch_dialog_rates(&client, parse_rates_selection("BYN")).await;
        assert_eq!(
            byn.rows
                .iter()
                .map(|row| row.label.as_str())
                .collect::<Vec<_>>(),
            vec!["💱 USD/BYN", "💱 EUR/BYN"]
        );
        assert_eq!(byn.rows[0].value, 3.0);
        assert!((byn.rows[1].value - 100.0 / 30.0).abs() < 0.0001);

        let pair = fetch_dialog_rates(&client, parse_rates_selection("BYN/USD")).await;
        assert_eq!(pair.rows[0].label, "💱 BYN/USD");
        assert!((pair.rows[0].value - 30.0 / 90.0).abs() < 0.0001);
        assert!(pair.errors.is_empty());

        let requests = server
            .join()
            .map_err(|_| io::Error::other("market fixture server panicked"))??;
        assert_eq!(requests, vec!["/cbr".to_owned()]);
        Ok(())
    }

    #[tokio::test]
    async fn market_rates_client_falls_back_to_frankfurter()
    -> Result<(), Box<dyn std::error::Error>> {
        let (urls, server) = spawn_market_fixture_server(vec![
            FixtureResponse::status(500, "{}"),
            FixtureResponse::ok(r#"{"date":"2026-06-18","rates":{"PLN":3.7}}"#),
        ])?;
        let client = market_test_client(urls)?;

        let snapshot = fetch_dialog_rates(&client, parse_rates_selection("usd/pln")).await;

        assert_eq!(snapshot.rows[0].source, "frankfurter");
        assert_eq!(snapshot.rows[0].value, 3.7);
        assert!(snapshot.errors.is_empty());
        let requests = server
            .join()
            .map_err(|_| io::Error::other("market fixture server panicked"))??;
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("/frankfurter?base=USD&quotes=PLN"));
        Ok(())
    }

    #[tokio::test]
    async fn market_rates_client_falls_back_to_fred_csv() -> Result<(), Box<dyn std::error::Error>>
    {
        let (urls, server) = spawn_market_fixture_server(vec![
            FixtureResponse::status(500, "{}"),
            FixtureResponse::ok("observation_date,DCOILWTICO\n2026-06-17,70\n2026-06-18,77\n"),
        ])?;
        let client = market_test_client(urls)?;

        let snapshot = fetch_dialog_rates(&client, parse_rates_selection("wti")).await;

        assert_eq!(snapshot.rows[0].source, "fred");
        assert_eq!(snapshot.rows[0].value, 77.0);
        assert_eq!(
            snapshot.rows[0]
                .delta
                .expect("WTI row should include delta")
                .percent,
            10.0
        );
        let requests = server
            .join()
            .map_err(|_| io::Error::other("market fixture server panicked"))??;
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("/fred?id=DCOILWTICO"));
        Ok(())
    }

    #[tokio::test]
    async fn rates_tool_dispatcher_queues_trimmed_html_side_effect()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = Arc::new(DispatcherQueue::new(Default::default()));
        let dispatcher = RatesToolDispatcherEffects::new(Arc::clone(&queue)).with_id_factories(
            Arc::new(|| "rates-vmsg-1".to_owned()),
            Arc::new(|| "123456789".to_owned()),
        );

        let result = dispatcher
            .dispatch_rates(
                dialog_rates_tool_context(&tool_context()),
                "  <b>rates</b>  ".to_owned(),
            )
            .await?;

        assert_eq!(
            result,
            RatesSideEffectDispatchResult {
                kind: RATES_MESSAGE_KIND.to_owned(),
                ticket_id: "123456789".to_owned(),
                eta: String::new(),
                state: "queued".to_owned(),
            }
        );
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "rates-vmsg-1");
        assert!(snapshot.immediate.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rates_tool_rich_dispatcher_sends_trimmed_rich_side_effect()
    -> Result<(), Box<dyn std::error::Error>> {
        let mock = Arc::new(crate::rich::MockRichSender::default());
        let dispatcher = RatesToolRichEffects::new(mock.clone());

        let result = dispatcher
            .dispatch_rates(
                dialog_rates_tool_context(&tool_context()),
                "  <h3>Rates</h3><table><tr><td>USD/PLN</td></tr></table>  ".to_owned(),
            )
            .await?;

        assert_eq!(
            result,
            RatesSideEffectDispatchResult {
                kind: RATES_MESSAGE_KIND.to_owned(),
                ticket_id: "1001".to_owned(),
                eta: String::new(),
                state: "executed".to_owned(),
            }
        );
        let sent = mock
            .sent
            .lock()
            .expect("rates rich effects sent mutex should not be poisoned");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].chat_id, -10042);
        assert_eq!(sent[0].message_thread_id, Some(9));
        assert_eq!(sent[0].reply_to_message_id, Some(77));
        assert!(sent[0].allow_sending_without_reply);
        assert_eq!(
            sent[0].html,
            "<h3>Rates</h3><table><tr><td>USD/PLN</td></tr></table>"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rates_tool_dispatcher_empty_text_is_go_noop() -> Result<(), Box<dyn std::error::Error>>
    {
        let dispatcher =
            RatesToolDispatcherEffects::new(Arc::new(DispatcherQueue::new(Default::default())));

        let result = dispatcher
            .dispatch_rates(
                dialog_rates_tool_context(&tool_context()),
                " \n ".to_owned(),
            )
            .await?;

        assert_eq!(result.kind, RATES_MESSAGE_KIND);
        assert_eq!(result.state, "noop");
        assert_eq!(result.ticket_id, "");
        Ok(())
    }

    fn bot_identity() -> Result<RatesBotIdentity, serde_json::Error> {
        Ok(RatesBotIdentity {
            user: serde_json::from_value(json!({
                "id": 777000,
                "is_bot": true,
                "first_name": "Plotva",
                "username": "PlotvaBot"
            }))?,
        })
    }

    fn sample_update(message: TelegramMessage) -> Result<TelegramUpdate, serde_json::Error> {
        serde_json::from_value(json!({
            "update_id": 500,
            "message": serde_json::to_value(message)?,
        }))
    }

    fn private_message(text: &str, message_id: i64) -> Result<TelegramMessage, serde_json::Error> {
        message_json(
            json!({"id": 42, "type": "private", "first_name": "Ada"}),
            text,
            message_id,
            false,
            1_710_000_000,
        )
    }

    fn group_message(text: &str, message_id: i64) -> Result<TelegramMessage, serde_json::Error> {
        group_message_at(text, message_id, 1_710_000_000)
    }

    fn group_message_at(
        text: &str,
        message_id: i64,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        message_json(
            json!({"id": -10042, "type": "supergroup", "title": "Plotva Group"}),
            text,
            message_id,
            true,
            date,
        )
    }

    fn message_json(
        chat: serde_json::Value,
        text: &str,
        message_id: i64,
        topic: bool,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        let mut value = json!({
            "message_id": message_id,
            "date": date,
            "chat": chat,
            "from": {
                "id": 42,
                "is_bot": false,
                "first_name": "Ada",
                "last_name": "Lovelace",
                "username": "ada"
            },
            "text": text
        });
        if topic {
            value["message_thread_id"] = json!(9);
        }
        serde_json::from_value(value)
    }

    fn tool_context() -> ToolContext {
        ToolContext {
            chat_id: -10042,
            thread_id: Some(9),
            message_id: 77,
            user_id: 42,
            user_full_name: "Ada Lovelace".to_owned(),
            message_text: "$".to_owned(),
            message_meta: openplotva_core::ChatMessageMeta {
                message_type: "text".to_owned(),
                ..openplotva_core::ChatMessageMeta::default()
            },
        }
    }

    fn market_test_client(urls: MarketRateUrls) -> Result<MarketRatesClient, MarketRatesError> {
        Ok(MarketRatesClient::from_timeout_seconds(15)?.with_urls_for_test(urls))
    }

    #[derive(Clone)]
    struct FixtureResponse {
        status: u16,
        body: &'static str,
    }

    impl FixtureResponse {
        fn ok(body: &'static str) -> Self {
            Self { status: 200, body }
        }

        fn status(status: u16, body: &'static str) -> Self {
            Self { status, body }
        }
    }

    fn spawn_market_fixture_server(
        responses: Vec<FixtureResponse>,
    ) -> io::Result<(MarketRateUrls, thread::JoinHandle<io::Result<Vec<String>>>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept()?;
                stream.set_read_timeout(Some(StdDuration::from_secs(5)))?;

                let mut request = Vec::new();
                let mut chunk = [0_u8; 1024];
                loop {
                    let read = stream.read(&mut chunk)?;
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                requests.push(
                    request
                        .split_whitespace()
                        .nth(1)
                        .unwrap_or_default()
                        .to_owned(),
                );
                write!(
                    stream,
                    "HTTP/1.1 {} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    response.status,
                    response.body.len(),
                    response.body
                )?;
            }
            Ok(requests)
        });
        let base = format!("http://{addr}");
        Ok((
            MarketRateUrls {
                yahoo_chart_base_url: format!("{base}/chart"),
                cbr_xml_url: format!("{base}/cbr"),
                frankfurter_rates_url: format!("{base}/frankfurter"),
                fred_csv_base_url: format!("{base}/fred"),
            },
            handle,
        ))
    }

    fn yahoo_fixture() -> &'static str {
        r#"{"chart":{"result":[{"meta":{"regularMarketPrice":65000.0,"regularMarketTime":1781800000,"chartPreviousClose":64000.0},"timestamp":[1781799940,1781800000],"indicators":{"quote":[{"close":[64000.0,65000.0]}]}}],"error":null}}"#
    }

    fn cbr_fixture() -> &'static str {
        r#"<ValCurs Date="19.06.2026" name="Foreign Currency Market"><Valute ID="R01235"><CharCode>USD</CharCode><Nominal>1</Nominal><Name>Dollar</Name><Value>90,0000</Value></Valute><Valute ID="R01239"><CharCode>EUR</CharCode><Nominal>1</Nominal><Name>Euro</Name><Value>100,0000</Value></Valute><Valute ID="R01090B"><CharCode>BYN</CharCode><Nominal>1</Nominal><Name>Belarusian ruble</Name><Value>30,0000</Value></Valute><Valute ID="R01060"><CharCode>AMD</CharCode><Nominal>100</Nominal><Name>Armenian dram</Name><Value>23,0000</Value></Valute><Valute ID="R01020A"><CharCode>AZN</CharCode><Nominal>1</Nominal><Name>Azerbaijani manat</Name><Value>53,0000</Value></Valute><Valute ID="R01370"><CharCode>KGS</CharCode><Nominal>10</Nominal><Name>Kyrgyz som</Name><Value>10,0000</Value></Valute><Valute ID="R01335"><CharCode>KZT</CharCode><Nominal>100</Nominal><Name>Kazakhstani tenge</Name><Value>18,0000</Value></Valute><Valute ID="R01500"><CharCode>MDL</CharCode><Nominal>10</Nominal><Name>Moldovan leu</Name><Value>50,0000</Value></Valute><Valute ID="R01670"><CharCode>TJS</CharCode><Nominal>10</Nominal><Name>Tajikistani somoni</Name><Value>85,0000</Value></Valute><Valute ID="R01710A"><CharCode>TMT</CharCode><Nominal>1</Nominal><Name>Turkmen manat</Name><Value>25,0000</Value></Valute><Valute ID="R01717"><CharCode>UZS</CharCode><Nominal>10000</Nominal><Name>Uzbekistani sum</Name><Value>70,0000</Value></Valute><Valute ID="R01375"><CharCode>CNY</CharCode><Nominal>1</Nominal><Name>Yuan</Name><Value>10,5000</Value><VunitRate>10,5000</VunitRate></Valute></ValCurs>"#
    }

    #[derive(Clone, Copy)]
    struct PermissionStub {
        allowed: bool,
    }

    impl PermissionStub {
        fn allowed() -> Self {
            Self { allowed: true }
        }
    }

    impl RatesSendPermission for PermissionStub {
        fn can_send_rates_text(&self, _chat: &TelegramChat) -> bool {
            self.allowed
        }
    }

    struct HeaderStub;

    impl RatesHeaderProvider for HeaderStub {
        fn rates_header(&self, user_full_name: &str) -> String {
            format!("Header for {user_full_name}")
        }
    }

    #[derive(Default)]
    struct RatesFetcherStub {
        state: Mutex<RatesFetcherState>,
    }

    #[derive(Default)]
    struct RatesFetcherState {
        calls: Vec<String>,
        btc_error: bool,
    }

    impl RatesFetcherStub {
        fn successful() -> Self {
            Self::default()
        }

        fn successful_snapshot() -> RatesSnapshot {
            RatesSnapshot {
                rows: vec![
                    RateQuote {
                        id: RATE_ID_USD_RUB.to_owned(),
                        label: "💵 USD/RUB".to_owned(),
                        value: 90.0,
                        precision: 2,
                        unit: "₽".to_owned(),
                        delta: rate_delta(90.0, 89.0, RateDeltaBasis::Day),
                        source: "test".to_owned(),
                        timestamp: None,
                        stale: false,
                    },
                    RateQuote {
                        id: RATE_ID_EUR_RUB.to_owned(),
                        label: "💶 EUR/RUB".to_owned(),
                        value: 100.0,
                        precision: 2,
                        unit: "₽".to_owned(),
                        delta: rate_delta(100.0, 98.0, RateDeltaBasis::Day),
                        source: "test".to_owned(),
                        timestamp: None,
                        stale: false,
                    },
                    RateQuote {
                        id: RATE_ID_EUR_USD.to_owned(),
                        label: "💶/💵 EUR/USD".to_owned(),
                        value: 1.1111,
                        precision: 4,
                        unit: String::new(),
                        delta: rate_delta(1.1111, 1.1, RateDeltaBasis::Day),
                        source: "test".to_owned(),
                        timestamp: None,
                        stale: false,
                    },
                    RateQuote {
                        id: RATE_ID_BTC_USD.to_owned(),
                        label: "₿ BTC/USD".to_owned(),
                        value: 65000.0,
                        precision: 0,
                        unit: "$".to_owned(),
                        delta: None,
                        source: "test".to_owned(),
                        timestamp: None,
                        stale: false,
                    },
                    RateQuote {
                        id: "brent".to_owned(),
                        label: "🛢 Brent".to_owned(),
                        value: 76.5,
                        precision: 2,
                        unit: "$".to_owned(),
                        delta: rate_delta(76.5, 75.0, RateDeltaBasis::Day),
                        source: "test".to_owned(),
                        timestamp: None,
                        stale: false,
                    },
                ],
                errors: Vec::new(),
            }
        }

        fn failing_btc() -> Self {
            Self {
                state: Mutex::new(RatesFetcherState {
                    btc_error: true,
                    ..RatesFetcherState::default()
                }),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.lock().calls.clone()
        }

        fn lock(&self) -> MutexGuard<'_, RatesFetcherState> {
            self.state.lock().expect("rates fetcher state")
        }
    }

    impl RatesFetcher for RatesFetcherStub {
        type Error = io::Error;

        fn fetch_rates<'a>(
            &'a self,
            selection: RatesSelection,
        ) -> RatesFetchFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state
                    .calls
                    .push(format!("fetch {}", selection.ids.join(",")));
                let mut snapshot = RatesFetcherStub::successful_snapshot();
                snapshot
                    .rows
                    .retain(|row| selection.ids.iter().any(|id| id == &row.id));
                if state.btc_error && selection.ids.iter().any(|id| id == RATE_ID_BTC_USD) {
                    snapshot.rows.retain(|row| row.id != RATE_ID_BTC_USD);
                    snapshot.errors.push(RatesFetchProblem {
                        label: "₿ BTC/USD".to_owned(),
                        message: "btc down".to_owned(),
                    });
                }
                for unknown in selection.unknown {
                    snapshot.errors.push(RatesFetchProblem {
                        label: unknown,
                        message: "unknown rate alias".to_owned(),
                    });
                }
                Ok(snapshot)
            })
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        state: Mutex<EffectsState>,
    }

    #[derive(Default)]
    struct EffectsState {
        sent: Vec<RatesTextPlan>,
    }

    impl EffectsStub {
        fn sent(&self) -> Vec<RatesTextPlan> {
            self.lock().sent.clone()
        }

        fn lock(&self) -> MutexGuard<'_, EffectsState> {
            self.state.lock().expect("rates effects state")
        }
    }

    impl RatesEffects for EffectsStub {
        type Error = io::Error;

        fn send_rates_text<'a>(
            &'a self,
            plan: RatesTextPlan,
        ) -> RatesEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().sent.push(plan);
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct DispatcherStub {
        calls: Mutex<Vec<(RatesToolInvocationContext, String)>>,
    }

    impl DispatcherStub {
        fn calls(&self) -> Vec<(RatesToolInvocationContext, String)> {
            self.calls.lock().expect("rates dispatch calls").clone()
        }
    }

    impl RatesSideEffectDispatcher for DispatcherStub {
        type Error = io::Error;

        fn dispatch_rates<'a>(
            &'a self,
            meta: RatesToolInvocationContext,
            text: String,
        ) -> RatesDispatchFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("rates dispatch calls")
                    .push((meta, text));
                Ok(RatesSideEffectDispatchResult {
                    kind: RATES_MESSAGE_KIND.to_owned(),
                    ticket_id: "ticket-1".to_owned(),
                    eta: String::new(),
                    state: "queued".to_owned(),
                })
            })
        }
    }

    #[derive(Default)]
    struct NextStub {
        calls: Mutex<Vec<i64>>,
    }

    impl NextStub {
        fn calls(&self) -> Vec<i64> {
            self.calls.lock().expect("next calls").clone()
        }
    }

    impl UpdateHandler for NextStub {
        type Error = io::Error;

        fn handle_update<'a>(
            &'a self,
            update: TelegramUpdate,
        ) -> UpdateHandlerFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls.lock().expect("next calls").push(update.id);
                Ok(())
            })
        }
    }
}
