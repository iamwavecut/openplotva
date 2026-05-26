//! App-level fetcher currency-rates command and dialog tool behavior.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, Update as TelegramUpdate,
    UpdateType as TelegramUpdateType, User as TelegramUser,
};
use openplotva_dialog::{
    TOOL_RESULT_STATUS_FAILED, TOOL_RESULT_STATUS_QUEUED, ToolContext, ToolError, ToolResult,
    ToolSideEffect,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMessageRef, TELEGRAM_PARSE_MODE_HTML,
    TextMessageRequest,
};
use reqwest::header::{
    ACCEPT, ACCEPT_LANGUAGE, CACHE_CONTROL, HeaderMap, HeaderName, HeaderValue, PRAGMA, REFERER,
    USER_AGENT,
};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueTextRequest, VirtualIdFactory, VirtualMessageStore, monotonic_virtual_id_factory,
    queue_text_message_parts,
};

const RATES_UNAVAILABLE_TEXT: &str = "❌ Сервис курсов временно недоступен (RBC не настроен)";
const RATES_FETCH_ERRORS_PREFIX: &str = "❌ Ошибки получения курсов:\n";
pub const RATES_MESSAGE_KIND: &str = "rates_message";
const RATES_TOOL_MODE_MULTI_TURN: &str = "multi_turn";
const RATES_TOOL_MODE_LEGACY: &str = "legacy";
const RBC_CACHE_TTL: Duration = Duration::from_secs(180);
const RBC_DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
const RBC_KEY_INDICATORS_URL_PREFIX: &str =
    "https://www.rbc.ru/quote/ajax/key-indicator-update/?_=";
const RBC_REFERER: &str = "https://www.rbc.ru/quote/ticker/338247";
const RBC_HTTP_USER_AGENT: &str = "OpenPlotva/0.1";
const RBC_CB_SUBNAME: &str = "ЦБ";
const RBC_TICKER_CNYRUB: &str = "CNYRUB";
const RBC_CURRENCY_USD: &str = "USD";
const RBC_CURRENCY_EUR: &str = "EUR";

/// Boxed future returned by rates daily-close providers.
pub type RatesDailyFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(f64, f64), E>> + Send + 'a>>;

/// Boxed future returned by rates spot providers.
pub type RatesSpotFuture<'a, E> = Pin<Box<dyn Future<Output = Result<f64, E>> + Send + 'a>>;

/// Boxed future returned by rates text side effects.
pub type RatesEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by rates dialog side-effect dispatchers.
pub type RatesDispatchFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<RatesSideEffectDispatchResult, E>> + Send + 'a>>;

pub trait RatesFetcher {
    /// Error returned by the provider.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Return current and previous daily closes for a fiat pair.
    fn fx_daily_last_two_closes<'a>(
        &'a self,
        from: &'static str,
        to: &'static str,
    ) -> RatesDailyFuture<'a, Self::Error>;

    /// Return latest spot exchange rate for a currency pair.
    fn currency_exchange_rate<'a>(
        &'a self,
        from: &'static str,
        to: &'static str,
    ) -> RatesSpotFuture<'a, Self::Error>;
}

#[derive(Clone)]
pub struct RbcRatesClient {
    http: reqwest::Client,
    key_indicators_url_prefix: Arc<str>,
    cache: Arc<AsyncRwLock<RbcRatesCache>>,
    last_refresh: Arc<AsyncMutex<Option<Instant>>>,
}

impl RbcRatesClient {
    pub fn from_timeout_seconds(timeout_seconds: i32) -> Result<Self, RbcRatesError> {
        let timeout = if timeout_seconds > 0 {
            Duration::from_secs(timeout_seconds as u64)
        } else {
            RBC_DEFAULT_TIMEOUT
        };
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(RbcRatesError::HttpClient)?;
        Ok(Self {
            http,
            key_indicators_url_prefix: Arc::from(RBC_KEY_INDICATORS_URL_PREFIX),
            cache: Arc::new(AsyncRwLock::new(RbcRatesCache::new(RBC_CACHE_TTL))),
            last_refresh: Arc::new(AsyncMutex::new(None)),
        })
    }

    /// Build an RBC client from the app config.
    pub fn from_config(config: &openplotva_config::RbcConfig) -> Result<Self, RbcRatesError> {
        Self::from_timeout_seconds(config.timeout_seconds)
    }

    async fn fetch_fx_daily_last_two_closes(
        &self,
        from: &str,
        to: &str,
    ) -> Result<(f64, f64), RbcRatesError> {
        let ticker = format!("{from}{to}");
        if let Some((last, prev)) = self.cached_both(&ticker).await {
            return Ok((last, prev));
        }
        if let Some((last, prev)) = self.cross_both(from, to).await {
            return Ok((last, prev));
        }
        if let Some(stale) = self.stale_ticker(&ticker).await {
            return Ok((stale.last_price, stale.prev_price));
        }
        Err(RbcRatesError::RateNotFound)
    }

    async fn fetch_currency_exchange_rate(
        &self,
        from: &str,
        to: &str,
    ) -> Result<f64, RbcRatesError> {
        let ticker = format!("{from}{to}");
        if let Some(rate) = self.cached_rate(&ticker).await {
            return Ok(rate);
        }
        if let Some(rate) = self.cross_rate(from, to).await {
            return Ok(rate);
        }
        if let Some(stale) = self.stale_ticker(&ticker).await {
            return Ok(stale.last_price);
        }
        Err(RbcRatesError::RateNotFound)
    }

    async fn cached_rate(&self, ticker: &str) -> Option<f64> {
        if let Some(data) = self.fresh_ticker(ticker).await {
            return Some(data.last_price);
        }
        let _ = self.refresh_if_needed().await;
        if let Some(data) = self.fresh_ticker(ticker).await {
            return Some(data.last_price);
        }
        let cb_ticker = format!("{ticker}_CB");
        self.fresh_ticker(&cb_ticker)
            .await
            .map(|data| data.last_price)
    }

    async fn cached_both(&self, ticker: &str) -> Option<(f64, f64)> {
        if let Some(data) = self.fresh_ticker(ticker).await {
            return Some((data.last_price, data.prev_price));
        }
        let _ = self.refresh_if_needed().await;
        if let Some(data) = self.fresh_ticker(ticker).await {
            return Some((data.last_price, data.prev_price));
        }
        let cb_ticker = format!("{ticker}_CB");
        self.fresh_ticker(&cb_ticker)
            .await
            .map(|data| (data.last_price, data.prev_price))
    }

    async fn cross_rate(&self, from: &str, to: &str) -> Option<f64> {
        match (from, to) {
            (RBC_CURRENCY_EUR, RBC_CURRENCY_USD) => self.eur_usd_rate().await,
            (RBC_CURRENCY_USD, RBC_CURRENCY_EUR) => {
                let eur_usd = self.eur_usd_rate_for_inverse().await?;
                inverse(eur_usd)
            }
            _ => None,
        }
    }

    async fn cross_both(&self, from: &str, to: &str) -> Option<(f64, f64)> {
        match (from, to) {
            (RBC_CURRENCY_EUR, RBC_CURRENCY_USD) => self.eur_usd_both().await,
            (RBC_CURRENCY_USD, RBC_CURRENCY_EUR) => {
                let (last, prev) = self.eur_usd_both_for_inverse().await?;
                inverse_both(last, prev)
            }
            _ => None,
        }
    }

    async fn eur_usd_rate_for_inverse(&self) -> Option<f64> {
        if let Some(rate) = self.cached_rate("EURUSD").await {
            return Some(rate);
        }
        if let Some(rate) = self.eur_usd_rate().await {
            return Some(rate);
        }
        self.stale_ticker("EURUSD")
            .await
            .map(|data| data.last_price)
    }

    async fn eur_usd_both_for_inverse(&self) -> Option<(f64, f64)> {
        if let Some((last, prev)) = self.cached_both("EURUSD").await {
            return Some((last, prev));
        }
        if let Some((last, prev)) = self.eur_usd_both().await {
            return Some((last, prev));
        }
        self.stale_ticker("EURUSD")
            .await
            .map(|data| (data.last_price, data.prev_price))
    }

    async fn eur_usd_rate(&self) -> Option<f64> {
        let eur_rub = self.lookup_last_with_cb("EURRUB", "EURRUB_CB").await?;
        let usd_rub = self.lookup_last_with_cb("USDRUB", "USDRUB_CB").await?;
        if usd_rub == 0.0 {
            return None;
        }
        Some(eur_rub / usd_rub)
    }

    async fn eur_usd_both(&self) -> Option<(f64, f64)> {
        let (eur_rub_last, eur_rub_prev) = self.lookup_both_with_cb("EURRUB", "EURRUB_CB").await?;
        let (usd_rub_last, usd_rub_prev) = self.lookup_both_with_cb("USDRUB", "USDRUB_CB").await?;
        if usd_rub_last == 0.0 {
            return None;
        }
        let prev = if eur_rub_prev != 0.0 && usd_rub_prev != 0.0 {
            eur_rub_prev / usd_rub_prev
        } else {
            0.0
        };
        Some((eur_rub_last / usd_rub_last, prev))
    }

    async fn lookup_last_with_cb(&self, symbol: &str, cb_symbol: &str) -> Option<f64> {
        if let Some(value) = self.lookup_last(symbol).await {
            return Some(value);
        }
        self.lookup_last(cb_symbol).await
    }

    async fn lookup_both_with_cb(&self, symbol: &str, cb_symbol: &str) -> Option<(f64, f64)> {
        match self.lookup_both(symbol).await {
            Some((last, prev)) if last != 0.0 => Some((last, prev)),
            _ => self.lookup_both(cb_symbol).await,
        }
    }

    async fn lookup_last(&self, symbol: &str) -> Option<f64> {
        let data = match self.fresh_ticker(symbol).await {
            Some(data) => data,
            None => self.stale_ticker(symbol).await?,
        };
        Some(data.last_price)
    }

    async fn lookup_both(&self, symbol: &str) -> Option<(f64, f64)> {
        let data = match self.fresh_ticker(symbol).await {
            Some(data) => data,
            None => self.stale_ticker(symbol).await?,
        };
        Some((data.last_price, data.prev_price))
    }

    async fn fresh_ticker(&self, symbol: &str) -> Option<RbcTickerData> {
        self.cache.read().await.get(symbol)
    }

    async fn stale_ticker(&self, symbol: &str) -> Option<RbcTickerData> {
        self.cache.read().await.get_stale(symbol)
    }

    async fn refresh_if_needed(&self) -> Result<(), RbcRatesError> {
        let mut last_refresh = self.last_refresh.lock().await;
        if last_refresh.is_some_and(|last| last.elapsed() < RBC_CACHE_TTL) {
            return Ok(());
        }
        *last_refresh = Some(Instant::now());
        drop(last_refresh);
        self.fetch_key_indicators().await
    }

    async fn fetch_key_indicators(&self) -> Result<(), RbcRatesError> {
        let url = self.key_indicators_url_for(rbc_timestamp_millis(SystemTime::now()));
        let response = self
            .http
            .get(url)
            .headers(rbc_headers())
            .send()
            .await
            .map_err(RbcRatesError::Request)?;
        let status = response.status();
        if !status.is_success() {
            return Err(RbcRatesError::HttpStatus(status.as_u16()));
        }
        let response = response
            .json::<RbcKeyIndicatorsResponse>()
            .await
            .map_err(RbcRatesError::Decode)?;
        for wrapper in response.shared_key_indicators_under_topline {
            self.cache_key_indicator(wrapper.item).await;
        }
        Ok(())
    }

    fn key_indicators_url_for(&self, timestamp_millis: u128) -> String {
        rbc_key_indicators_url_for(&self.key_indicators_url_prefix, timestamp_millis)
    }

    async fn cache_key_indicator(&self, item: RbcKeyIndicatorItem) {
        if item.closevalue <= 0.0 {
            return;
        }
        let normalized = normalize_rbc_ticker(&item.ticker, &item.name, item.subname.as_deref());
        let prev = compute_rbc_prev(&item);
        let mut cache = self.cache.write().await;
        match prev {
            Some(prev) => {
                cache.set_both(&item.ticker, item.closevalue, prev);
                if normalized != item.ticker {
                    cache.set_both(&normalized, item.closevalue, prev);
                }
            }
            None => {
                cache.set(&item.ticker, item.closevalue);
                if normalized != item.ticker {
                    cache.set(&normalized, item.closevalue);
                }
            }
        }
    }

    #[cfg(test)]
    async fn mark_fresh_for_test(&self) {
        *self.last_refresh.lock().await = Some(Instant::now());
    }

    #[cfg(test)]
    async fn set_both_for_test(&self, symbol: &str, last: f64, prev: f64) {
        self.cache.write().await.set_both(symbol, last, prev);
    }

    #[cfg(test)]
    async fn get_stale_for_test(&self, symbol: &str) -> Option<RbcTickerData> {
        self.cache.read().await.get_stale(symbol)
    }

    #[cfg(test)]
    fn with_key_indicators_url_prefix_for_test(mut self, prefix: impl Into<Arc<str>>) -> Self {
        self.key_indicators_url_prefix = prefix.into();
        self
    }
}

impl fmt::Debug for RbcRatesClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RbcRatesClient").finish_non_exhaustive()
    }
}

impl RatesFetcher for RbcRatesClient {
    type Error = RbcRatesError;

    fn fx_daily_last_two_closes<'a>(
        &'a self,
        from: &'static str,
        to: &'static str,
    ) -> RatesDailyFuture<'a, Self::Error> {
        Box::pin(async move { self.fetch_fx_daily_last_two_closes(from, to).await })
    }

    fn currency_exchange_rate<'a>(
        &'a self,
        from: &'static str,
        to: &'static str,
    ) -> RatesSpotFuture<'a, Self::Error> {
        Box::pin(async move { self.fetch_currency_exchange_rate(from, to).await })
    }
}

/// so command/tool callers usually see only `rate not found`.
#[derive(Debug, Error)]
pub enum RbcRatesError {
    /// HTTP client construction failed.
    #[error("failed to build RBC HTTP client: {0}")]
    HttpClient(reqwest::Error),
    /// RBC request failed before an HTTP response arrived.
    #[error("failed to execute RBC request: {0}")]
    Request(reqwest::Error),
    /// RBC returned a non-2xx response.
    #[error("rbc http status {0}")]
    HttpStatus(u16),
    /// RBC JSON body did not match the expected key-indicator response.
    #[error("failed to decode RBC JSON: {0}")]
    Decode(reqwest::Error),
    /// No cached, refreshed, cross, or stale value is available.
    #[error("rate not found")]
    RateNotFound,
}

#[derive(Clone, Debug)]
struct RbcRatesCache {
    ttl: Duration,
    data: HashMap<String, RbcTickerData>,
}

impl RbcRatesCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            data: HashMap::new(),
        }
    }

    fn set(&mut self, symbol: &str, price: f64) {
        let prev_price = self
            .data
            .get(symbol)
            .map_or(0.0, |existing| existing.last_price);
        self.data.insert(
            symbol.to_owned(),
            RbcTickerData {
                last_price: price,
                prev_price,
                last_update: Instant::now(),
            },
        );
    }

    fn set_both(&mut self, symbol: &str, last: f64, prev: f64) {
        self.data.insert(
            symbol.to_owned(),
            RbcTickerData {
                last_price: last,
                prev_price: prev,
                last_update: Instant::now(),
            },
        );
    }

    fn get(&self, symbol: &str) -> Option<RbcTickerData> {
        let data = self.data.get(symbol)?;
        if data.last_update.elapsed() > self.ttl {
            return None;
        }
        Some(*data)
    }

    fn get_stale(&self, symbol: &str) -> Option<RbcTickerData> {
        self.data.get(symbol).copied()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RbcTickerData {
    last_price: f64,
    prev_price: f64,
    last_update: Instant,
}

#[derive(Debug, Deserialize)]
struct RbcKeyIndicatorsResponse {
    #[serde(default)]
    shared_key_indicators_under_topline: Vec<RbcKeyIndicatorWrapper>,
}

#[derive(Debug, Deserialize)]
struct RbcKeyIndicatorWrapper {
    item: RbcKeyIndicatorItem,
}

#[derive(Debug, Deserialize)]
struct RbcKeyIndicatorItem {
    #[serde(default)]
    ticker: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    subname: Option<String>,
    #[serde(default)]
    change: f64,
    #[serde(default)]
    closevalue: f64,
    #[serde(default)]
    prepared: RbcPreparedValues,
}

#[derive(Debug, Default, Deserialize)]
struct RbcPreparedValues {
    #[serde(default)]
    change: String,
}

fn rbc_key_indicators_url_for(prefix: &str, timestamp_millis: u128) -> String {
    format!("{prefix}{timestamp_millis}")
}

fn rbc_timestamp_millis(now: SystemTime) -> u128 {
    match now.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis(),
        Err(_) => 0,
    }
}

fn rbc_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
    headers.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static("ru,be;q=0.9,en-US;q=0.8,en;q=0.7"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
    headers.insert(USER_AGENT, HeaderValue::from_static(RBC_HTTP_USER_AGENT));
    headers.insert(
        HeaderName::from_static("sec-ch-ua"),
        HeaderValue::from_static(
            "\"Not;A=Brand\";v=\"99\", \"Google Chrome\";v=\"139\", \"Chromium\";v=\"139\"",
        ),
    );
    headers.insert(
        HeaderName::from_static("sec-ch-ua-mobile"),
        HeaderValue::from_static("?0"),
    );
    headers.insert(
        HeaderName::from_static("sec-ch-ua-platform"),
        HeaderValue::from_static("\"macOS\""),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-dest"),
        HeaderValue::from_static("empty"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-mode"),
        HeaderValue::from_static("cors"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-site"),
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(REFERER, HeaderValue::from_static(RBC_REFERER));
    headers
}

fn normalize_rbc_ticker(ticker: &str, name: &str, subname: Option<&str>) -> String {
    match ticker {
        "USD/RUB F" => "USDRUB".to_owned(),
        "EUR/RUB F" => "EURRUB".to_owned(),
        "CNY Бирж" => RBC_TICKER_CNYRUB.to_owned(),
        "BTC/USD" => "BTCUSD".to_owned(),
        "ETH/USD" => "ETHUSD".to_owned(),
        "USD ЦБ" => "USDRUB_CB".to_owned(),
        "EUR ЦБ" => "EURRUB_CB".to_owned(),
        "CNY ЦБ" => RBC_TICKER_CNYRUB.to_owned(),
        _ if subname == Some(RBC_CB_SUBNAME) => match name {
            RBC_CURRENCY_USD => "USDRUB_CB".to_owned(),
            RBC_CURRENCY_EUR => "EURRUB_CB".to_owned(),
            "CNY" => RBC_TICKER_CNYRUB.to_owned(),
            _ => ticker.to_owned(),
        },
        _ => ticker.to_owned(),
    }
}

fn compute_rbc_prev(item: &RbcKeyIndicatorItem) -> Option<f64> {
    let last = item.closevalue;
    if last <= 0.0 {
        return None;
    }
    if item.prepared.change.contains('%') {
        let percent = item.change / 100.0;
        if percent <= -1.0 {
            return None;
        }
        return Some(last / (1.0 + percent));
    }
    if item.change != 0.0 {
        return Some(last - item.change);
    }
    None
}

fn inverse(value: f64) -> Option<f64> {
    if value == 0.0 {
        return None;
    }
    Some(1.0 / value)
}

fn inverse_both(last: f64, prev: f64) -> Option<(f64, f64)> {
    if last == 0.0 {
        return None;
    }
    let prev = if prev == 0.0 { 0.0 } else { 1.0 / prev };
    Some((1.0 / last, prev))
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

/// Dispatcher-backed rates command side effects.
#[derive(Clone)]
pub struct RatesDispatcherEffects<Store> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    next_virtual_id: RatesVirtualIdFactory,
}

impl<Store> RatesDispatcherEffects<Store> {
    /// Build rates effects backed by the normal outbound dispatcher.
    #[must_use]
    pub fn new(store: Store, queue: Arc<DispatcherQueue>) -> Self {
        Self {
            store,
            queue,
            next_virtual_id: monotonic_virtual_id_factory("rates-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: RatesVirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Store> RatesEffects for RatesDispatcherEffects<Store>
where
    Store: VirtualMessageStore + Send + Sync,
{
    type Error = RatesDispatchEffectError;

    fn send_rates_text<'a>(&'a self, plan: RatesTextPlan) -> RatesEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            queue_text_message_parts(
                &self.store,
                &self.queue,
                QueueTextRequest {
                    message: &plan.message,
                    reply_to: Some(&plan.reply_to),
                    immediate_first: false,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: None,
                },
                || (self.next_virtual_id)(),
            )
            .await?;
            Ok(())
        })
    }
}

/// Recoverable errors from dispatcher-backed rates effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum RatesDispatchEffectError {
    #[error("failed to queue rates text: {0}")]
    Queue(#[from] OutboundBuildError),
}

/// Generator for async rates side-effect ticket IDs.
pub type RatesTicketFactory = Arc<dyn Fn() -> String + Send + Sync>;

/// Dispatcher-backed dialog-tool rates side effects.
#[derive(Clone)]
pub struct RatesToolDispatcherEffects<Store> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    next_virtual_id: RatesVirtualIdFactory,
    next_ticket_id: RatesTicketFactory,
}

impl<Store> RatesToolDispatcherEffects<Store> {
    /// Build dialog rates effects backed by the normal outbound dispatcher.
    #[must_use]
    pub fn new(store: Store, queue: Arc<DispatcherQueue>) -> Self {
        Self {
            store,
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

impl<Store> fmt::Debug for RatesToolDispatcherEffects<Store> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RatesToolDispatcherEffects")
            .finish_non_exhaustive()
    }
}

impl<Store> RatesSideEffectDispatcher for RatesToolDispatcherEffects<Store>
where
    Store: VirtualMessageStore + Send + Sync,
{
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
                &self.store,
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

/// Rates values and per-leg fetch errors.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RatesSnapshot {
    /// USD/RUB current daily close.
    pub usd: f64,
    /// USD/RUB previous daily close.
    pub usd_prev: f64,
    /// USD/RUB fetch error.
    pub usd_error: Option<String>,
    /// EUR/RUB current daily close.
    pub eur: f64,
    /// EUR/RUB previous daily close.
    pub eur_prev: f64,
    /// EUR/RUB fetch error.
    pub eur_error: Option<String>,
    /// EUR/USD current daily close.
    pub eur_usd: f64,
    /// EUR/USD previous daily close.
    pub eur_usd_prev: f64,
    /// EUR/USD fetch error.
    pub eur_usd_error: Option<String>,
    /// BTC/USD spot rate.
    pub btc: f64,
    /// BTC/USD fetch error.
    pub btc_error: Option<String>,
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
    /// RBC client was missing and the unavailable text send failed.
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

    let snapshot = fetch_dialog_rates(fetcher).await;
    let errors = command_rates_errors(&snapshot);
    if !errors.is_empty() {
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

pub async fn fetch_dialog_rates<Fetcher>(fetcher: &Fetcher) -> RatesSnapshot
where
    Fetcher: RatesFetcher + Sync,
{
    let (usd, usd_prev, usd_error) = match fetcher.fx_daily_last_two_closes("USD", "RUB").await {
        Ok((last, prev)) => (last, prev, None),
        Err(error) => (0.0, 0.0, Some(error.to_string())),
    };
    let (eur, eur_prev, eur_error) = match fetcher.fx_daily_last_two_closes("EUR", "RUB").await {
        Ok((last, prev)) => (last, prev, None),
        Err(error) => (0.0, 0.0, Some(error.to_string())),
    };
    let (eur_usd, eur_usd_prev, eur_usd_error) =
        match fetcher.fx_daily_last_two_closes("EUR", "USD").await {
            Ok((last, prev)) => (last, prev, None),
            Err(error) => (0.0, 0.0, Some(error.to_string())),
        };
    let (btc, btc_error) = match fetcher.currency_exchange_rate("BTC", "USD").await {
        Ok(rate) => (rate, None),
        Err(error) => (0.0, Some(error.to_string())),
    };

    RatesSnapshot {
        usd,
        usd_prev,
        usd_error,
        eur,
        eur_prev,
        eur_error,
        eur_usd,
        eur_usd_prev,
        eur_usd_error,
        btc,
        btc_error,
    }
}

pub async fn currency_rates_tool<Fetcher, Dispatcher>(
    fetcher: Option<&Fetcher>,
    dispatcher: &Dispatcher,
    meta: &ToolContext,
) -> ToolResult
where
    Fetcher: RatesFetcher + Sync,
    Dispatcher: RatesSideEffectDispatcher + Sync,
{
    let Some(fetcher) = fetcher else {
        return ToolResult::failed("currency_rates_unavailable", "rates service unavailable");
    };

    let snapshot = fetch_dialog_rates(fetcher).await;
    let errors = dialog_rates_errors(&snapshot);
    if !errors.is_empty() {
        return dialog_rates_failure_result(&errors);
    }

    let text = format_dialog_rates_message(&snapshot);
    match dispatcher
        .dispatch_rates(dialog_rates_tool_context(meta), text.clone())
        .await
    {
        Ok(dispatch) => dialog_rates_queued_result(&text, &dispatch),
        Err(error) => dialog_rates_dispatch_failure_result(&text, &error),
    }
}

#[must_use]
pub fn command_rates_errors(rates: &RatesSnapshot) -> Vec<String> {
    let mut errors = Vec::with_capacity(4);
    if let Some(error) = &rates.usd_error {
        errors.push(format!("USD/RUB: {error}"));
    }
    if let Some(error) = &rates.eur_error {
        errors.push(format!("EUR/RUB: {error}"));
    }
    if let Some(error) = &rates.eur_usd_error {
        errors.push(format!("EUR/USD: {error}"));
    }
    if let Some(error) = &rates.btc_error {
        errors.push(format!("BTC: {error}"));
    }
    errors
}

#[must_use]
pub fn dialog_rates_errors(rates: &RatesSnapshot) -> Vec<String> {
    let mut errors = Vec::with_capacity(4);
    if let Some(error) = &rates.usd_error {
        errors.push(format!("USD/RUB: {error}"));
    }
    if let Some(error) = &rates.eur_error {
        errors.push(format!("EUR/RUB: {error}"));
    }
    if let Some(error) = &rates.eur_usd_error {
        errors.push(format!("EUR/USD: {error}"));
    }
    if let Some(error) = &rates.btc_error {
        errors.push(format!("BTC/USD: {error}"));
    }
    errors
}

#[must_use]
pub fn format_rates_command_message(header: &str, rates: &RatesSnapshot) -> String {
    let usd_pct = percent_change(rates.usd_prev, rates.usd) as f32 as f64;
    let eur_pct = percent_change(rates.eur_prev, rates.eur) as f32 as f64;
    let rate_usd = format_fiat_rate("💵", usd_pct, rates.usd, "₽");
    let rate_eur = format_fiat_rate("💶", eur_pct, rates.eur, "₽");
    let rate_usd_eur = format!(
        "[ 💶 / 💵 = <code>{}</code> ]",
        format_rate_value(rates.eur_usd, 4)
    );
    let crypto_line = if rates.btc_error.is_none() && rates.btc != 0.0 {
        format_crypto_rate(rates.btc, 0.0, "BTC", "$")
    } else {
        String::new()
    };
    format!("{header}\n{rate_usd}    {rate_eur}\n{crypto_line}    {rate_usd_eur}")
}

#[must_use]
pub fn format_dialog_rates_message(rates: &RatesSnapshot) -> String {
    [
        format!(
            "USD/RUB={} ({})",
            format_rate_value(rates.usd, 2),
            format_rate_delta(rates.usd_prev, rates.usd)
        ),
        format!(
            "EUR/RUB={} ({})",
            format_rate_value(rates.eur, 2),
            format_rate_delta(rates.eur_prev, rates.eur)
        ),
        format!(
            "EUR/USD={} ({})",
            format_rate_value(rates.eur_usd, 4),
            format_rate_delta(rates.eur_usd_prev, rates.eur_usd)
        ),
        format!("BTC/USD={}", format_rate_value(rates.btc, 0)),
    ]
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
    ToolResult {
        status: TOOL_RESULT_STATUS_QUEUED.to_owned(),
        message: text.to_owned(),
        no_reply: false,
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

fn format_fiat_rate(symbol: &str, percent: f64, rate: f64, unit: &str) -> String {
    let chart = trend_emoji(percent);
    format!(
        "[ {symbol}{chart} <b>{}%</b> = <code>{} {unit}</code>]",
        format_rate_value(percent, 1),
        format_rate_value(rate, 2),
    )
}

fn format_crypto_rate(rate: f64, percent: f64, symbol: &str, unit: &str) -> String {
    let chart = trend_emoji(percent);
    format!(
        "[ <b>{symbol}</b>{chart} <b>{}%</b> = <code>{} {unit}</code> ]",
        format_rate_value(percent, 1),
        format_rate_value(rate, 0),
    )
}

fn trend_emoji(percent: f64) -> &'static str {
    match percent.total_cmp(&0.0) {
        std::cmp::Ordering::Greater => "📈",
        std::cmp::Ordering::Less => "📉",
        std::cmp::Ordering::Equal => "",
    }
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
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::json;
    use std::{
        env,
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
            vec![
                "daily USD/RUB",
                "daily EUR/RUB",
                "daily EUR/USD",
                "spot BTC/USD"
            ]
        );
        let sent = effects.sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].message.render_as, TELEGRAM_PARSE_MODE_HTML);
        assert_eq!(sent[0].reply_to.message_id, 77);
        assert_eq!(
            sent[0].message.text,
            "Header for Ada Lovelace\n[ 💵📈 <b>1.1%</b> = <code>90.00 ₽</code>]    [ 💶📈 <b>2.0%</b> = <code>100.00 ₽</code>]\n[ <b>BTC</b> <b>0.0%</b> = <code>65000 $</code> ]    [ 💶 / 💵 = <code>1.1111</code> ]"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rates_update_handler_queues_dispatcher_send_message_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = VirtualStoreStub::default();
        let queue = Arc::new(openplotva_telegram::DispatcherQueue::new(Default::default()));
        let effects = RatesDispatcherEffects::new(store.clone(), Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "rates-command-vmsg-1".to_owned()));
        let next = Arc::new(NextStub::default());
        let handler = RatesCommandUpdateHandler::new(
            bot_identity()?,
            Arc::new(PermissionStub::allowed()),
            Some(Arc::new(RatesFetcherStub::successful())),
            Arc::new(HeaderStub),
            Arc::new(effects),
            Arc::clone(&next),
        );

        handler
            .handle_update(sample_update(group_message("$ now", 82)?)?)
            .await?;

        assert!(next.calls().is_empty());
        assert_eq!(
            store.inserts(),
            vec![("rates-command-vmsg-1".to_owned(), -10042, Some(0))]
        );
        assert!(queue.dequeue_immediate().is_none());
        let item = queue
            .dequeue_regular()
            .ok_or_else(|| io::Error::other("expected queued rates message"))?;
        assert_eq!(item.metadata().virtual_id, "rates-command-vmsg-1");
        assert_eq!(item.metadata().chat_id, -10042);
        assert_eq!(
            item.method_kind(),
            Some(openplotva_telegram::TelegramOutboundMethodKind::SendMessage)
        );
        assert!(!item.bypasses_chat_restrictions());
        assert!(item.ephemeral_delete_after().is_none());

        let method = item
            .into_method()
            .ok_or_else(|| io::Error::other("expected rates sendMessage payload"))?;
        let value = method_as_value(method)?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(value["message_thread_id"], json!(9));
        assert_eq!(value["reply_parameters"]["message_id"], json!(82));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(value["parse_mode"], json!("HTML"));
        assert_eq!(
            value["text"],
            json!(
                "Header for Ada Lovelace\n[ 💵📈 <b>1.1%</b> = <code>90.00 ₽</code>]    [ 💶📈 <b>2.0%</b> = <code>100.00 ₽</code>]\n[ <b>BTC</b> <b>0.0%</b> = <code>65000 $</code> ]    [ 💶 / 💵 = <code>1.1111</code> ]"
            )
        );
        assert!(queue.dequeue_regular().is_none());
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
            sample_update(private_message("$", 81)?)?,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            RatesCommandOutcome::FetchErrorsSent {
                errors: vec!["BTC: btc down".to_owned()]
            }
        );
        assert_eq!(
            effects.sent()[0].message.text,
            "❌ Ошибки получения курсов:\nBTC: btc down"
        );
        Ok(())
    }

    #[tokio::test]
    async fn currency_rates_tool_preserves_dialog_result_shapes()
    -> Result<(), Box<dyn std::error::Error>> {
        let dispatcher = DispatcherStub::default();
        let meta = tool_context();

        let result =
            currency_rates_tool(Some(&RatesFetcherStub::successful()), &dispatcher, &meta).await;

        assert_eq!(result.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(
            result.message,
            "USD/RUB=90.00 (+1.1%)\nEUR/RUB=100.00 (+2.0%)\nEUR/USD=1.1111 (+1.0%)\nBTC/USD=65000"
        );
        assert_eq!(
            result.data,
            Some(json!({
                "rates_text": result.message,
                "dispatch": {
                    "kind": "rates_message",
                    "ticket_id": "ticket-1",
                    "state": "queued",
                },
            }))
        );
        let calls = dispatcher.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0.mode, "multi_turn");
        assert_eq!(calls[0].0.thread_id, Some(9));
        assert_eq!(calls[0].1, result.message);

        let missing = currency_rates_tool(None::<&RatesFetcherStub>, &dispatcher, &meta).await;
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
            &DispatcherStub::default(),
            &tool_context(),
        )
        .await;

        assert_eq!(result.status, TOOL_RESULT_STATUS_FAILED);
        assert_eq!(result.message, "rates fetch failed");
        assert_eq!(
            result.data,
            Some(json!({ "errors": ["BTC/USD: btc down"] }))
        );
        assert_eq!(
            result.error,
            Some(ToolError {
                code: "currency_rates_failed".to_owned(),
                reason: "BTC/USD: btc down".to_owned(),
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
        let no_btc = RatesSnapshot {
            usd: 90.0,
            usd_prev: 90.0,
            eur: 100.0,
            eur_prev: 100.0,
            eur_usd: 1.1,
            eur_usd_prev: 1.1,
            ..RatesSnapshot::default()
        };
        assert_eq!(
            format_rates_command_message("H", &no_btc),
            "H\n[ 💵 <b>0.0%</b> = <code>90.00 ₽</code>]    [ 💶 <b>0.0%</b> = <code>100.00 ₽</code>]\n    [ 💶 / 💵 = <code>1.1000</code> ]"
        );
    }

    #[test]
    fn rbc_request_shape_matches_go_key_indicator_fetch() {
        assert_eq!(
            rbc_key_indicators_url_for(RBC_KEY_INDICATORS_URL_PREFIX, 123),
            "https://www.rbc.ru/quote/ajax/key-indicator-update/?_=123"
        );
        let headers = rbc_headers();
        assert_eq!(headers.get(ACCEPT), Some(&HeaderValue::from_static("*/*")));
        assert_eq!(
            headers.get(ACCEPT_LANGUAGE),
            Some(&HeaderValue::from_static(
                "ru,be;q=0.9,en-US;q=0.8,en;q=0.7"
            ))
        );
        assert_eq!(
            headers.get(HeaderName::from_static("sec-ch-ua-platform")),
            Some(&HeaderValue::from_static("\"macOS\""))
        );
        assert_eq!(
            headers.get(REFERER),
            Some(&HeaderValue::from_static(RBC_REFERER))
        );
        assert_eq!(
            headers.get(USER_AGENT),
            Some(&HeaderValue::from_static(RBC_HTTP_USER_AGENT))
        );
    }

    #[test]
    fn rbc_normalize_ticker_matches_go_aliases() {
        let cases = [
            ("USD/RUB F", "", None, "USDRUB"),
            ("EUR/RUB F", "", None, "EURRUB"),
            ("CNY Бирж", "", None, RBC_TICKER_CNYRUB),
            ("BTC/USD", "", None, "BTCUSD"),
            ("ETH/USD", "", None, "ETHUSD"),
            ("USD ЦБ", "", None, "USDRUB_CB"),
            ("EUR ЦБ", "", None, "EURRUB_CB"),
            ("CNY ЦБ", "", None, RBC_TICKER_CNYRUB),
            ("USD", RBC_CURRENCY_USD, Some(RBC_CB_SUBNAME), "USDRUB_CB"),
            ("EUR", RBC_CURRENCY_EUR, Some(RBC_CB_SUBNAME), "EURRUB_CB"),
            ("SBER", "SBER", None, "SBER"),
        ];

        for (ticker, name, subname, want) in cases {
            assert_eq!(normalize_rbc_ticker(ticker, name, subname), want);
        }
    }

    #[tokio::test]
    async fn rbc_client_stores_raw_and_normalized_tickers() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = rbc_test_client()?;

        client
            .cache_key_indicator(rbc_item("USD/RUB F", "", None, 100.0, 10.0, ""))
            .await;

        for ticker in ["USD/RUB F", "USDRUB"] {
            let data = client
                .get_stale_for_test(ticker)
                .await
                .ok_or_else(|| io::Error::other(format!("missing cached {ticker}")))?;
            assert_eq!(data.last_price, 100.0);
            assert_eq!(data.prev_price, 90.0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn rbc_client_uses_cb_and_eur_usd_cross_rates() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = rbc_test_client()?;
        client.mark_fresh_for_test().await;
        client.set_both_for_test("USDRUB_CB", 90.0, 89.0).await;

        assert_eq!(
            client
                .fetch_currency_exchange_rate(RBC_CURRENCY_USD, "RUB")
                .await?,
            90.0
        );

        client.set_both_for_test("EURRUB", 100.0, 98.0).await;
        client.set_both_for_test("USDRUB", 80.0, 79.0).await;
        assert_eq!(
            client
                .fetch_currency_exchange_rate(RBC_CURRENCY_EUR, RBC_CURRENCY_USD)
                .await?,
            1.25
        );
        Ok(())
    }

    #[tokio::test]
    async fn rbc_client_daily_closes_use_go_inverse_cross_rate()
    -> Result<(), Box<dyn std::error::Error>> {
        let client = rbc_test_client()?;
        client.mark_fresh_for_test().await;
        client.set_both_for_test("EURRUB", 100.0, 98.0).await;
        client.set_both_for_test("USDRUB", 80.0, 70.0).await;

        let (last, prev) = client
            .fetch_fx_daily_last_two_closes(RBC_CURRENCY_USD, RBC_CURRENCY_EUR)
            .await?;

        assert_eq!(last, 0.8);
        assert_eq!(prev, 70.0 / 98.0);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "live public RBC provider smoke"]
    async fn rbc_rates_client_live_smoke_fetches_usd_rub() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = rbc_test_client()?;

        let (last, prev) = client
            .fx_daily_last_two_closes(RBC_CURRENCY_USD, "RUB")
            .await?;

        assert!(
            last.is_finite() && last > 0.0,
            "expected positive finite USD/RUB last close, got {last}"
        );
        assert!(
            prev.is_finite() && prev > 0.0,
            "expected positive finite USD/RUB previous close, got {prev}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn rbc_fixture_smoke_fetches_rates_with_go_headers()
    -> Result<(), Box<dyn std::error::Error>> {
        let (prefix, server) = spawn_rbc_fixture_server(rbc_key_indicators_fixture())?;
        let client =
            rbc_test_client()?.with_key_indicators_url_prefix_for_test(Arc::<str>::from(prefix));

        let snapshot = fetch_dialog_rates(&client).await;

        assert_eq!(snapshot.usd_error, None);
        assert_eq!(snapshot.eur_error, None);
        assert_eq!(snapshot.eur_usd_error, None);
        assert_eq!(snapshot.btc_error, None);
        assert_eq!(snapshot.usd, 90.0);
        assert_eq!(snapshot.eur, 100.0);
        assert_eq!(snapshot.eur_usd, 100.0 / 90.0);
        assert_eq!(snapshot.btc, 65_000.0);
        assert!(snapshot.usd_prev > 0.0 && snapshot.eur_prev > 0.0 && snapshot.eur_usd_prev > 0.0);

        let request = server
            .join()
            .map_err(|_| io::Error::other("RBC fixture server panicked"))??;
        assert_rbc_fixture_request_matches_go(&request);
        Ok(())
    }

    #[tokio::test]
    async fn rates_rbc_fixture_smoke_queues_command_and_tool_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let (prefix, server) = spawn_rbc_fixture_server(rbc_key_indicators_fixture())?;
        let provider = Arc::new(
            rbc_test_client()?.with_key_indicators_url_prefix_for_test(Arc::<str>::from(prefix)),
        );

        let command_store = VirtualStoreStub::default();
        let command_queue = Arc::new(openplotva_telegram::DispatcherQueue::new(Default::default()));
        let command_effects =
            RatesDispatcherEffects::new(command_store.clone(), Arc::clone(&command_queue))
                .with_virtual_id_factory(Arc::new(|| "rates-command-rbc-vmsg-1".to_owned()));
        let next = Arc::new(NextStub::default());
        let handler = RatesCommandUpdateHandler::new(
            bot_identity()?,
            Arc::new(PermissionStub::allowed()),
            Some(Arc::clone(&provider)),
            Arc::new(HeaderStub),
            Arc::new(command_effects),
            Arc::clone(&next),
        );

        handler
            .handle_update(sample_update(group_message("$", 83)?)?)
            .await?;

        assert!(next.calls().is_empty());
        assert_eq!(
            command_store.inserts(),
            vec![("rates-command-rbc-vmsg-1".to_owned(), -10042, Some(0))]
        );
        let command_item = command_queue
            .dequeue_regular()
            .ok_or_else(|| io::Error::other("expected queued RBC-backed command payload"))?;
        let command_method = command_item
            .into_method()
            .ok_or_else(|| io::Error::other("expected queued RBC-backed command method"))?;
        let command = method_as_value(command_method)?;
        assert_eq!(command["chat_id"], json!(-10042));
        assert_eq!(command["message_thread_id"], json!(9));
        assert_eq!(command["reply_parameters"]["message_id"], json!(83));
        assert_eq!(command["parse_mode"], json!("HTML"));
        assert!(
            command["text"]
                .as_str()
                .is_some_and(|text| text.contains("90.00 ₽") && text.contains("65000 $"))
        );

        let tool_store = VirtualStoreStub::default();
        let tool_queue = Arc::new(openplotva_telegram::DispatcherQueue::new(Default::default()));
        let tool_dispatcher = RatesToolDispatcherEffects::new(tool_store.clone(), tool_queue)
            .with_id_factories(
                Arc::new(|| "rates-tool-rbc-vmsg-1".to_owned()),
                Arc::new(|| "rbc-ticket-1".to_owned()),
            );

        let tool_result =
            currency_rates_tool(Some(provider.as_ref()), &tool_dispatcher, &tool_context()).await;

        assert_eq!(tool_result.status, TOOL_RESULT_STATUS_QUEUED);
        assert_eq!(
            tool_result.message,
            "USD/RUB=90.00 (+1.1%)\nEUR/RUB=100.00 (+2.0%)\nEUR/USD=1.1111 (+0.9%)\nBTC/USD=65000"
        );
        assert_eq!(
            tool_result.data,
            Some(json!({
                "rates_text": tool_result.message,
                "dispatch": {
                    "kind": "rates_message",
                    "ticket_id": "rbc-ticket-1",
                    "state": "queued",
                },
            }))
        );
        assert_eq!(
            tool_store.inserts(),
            vec![("rates-tool-rbc-vmsg-1".to_owned(), -10042, Some(9))]
        );

        let request = server
            .join()
            .map_err(|_| io::Error::other("RBC fixture server panicked"))??;
        assert_rbc_fixture_request_matches_go(&request);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_rates_commands_queue_payload_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        for (command, message_id, virtual_id) in [
            ("$", 84, "rates-live-redis-vmsg-dollar"),
            (";", 85, "rates-live-redis-vmsg-semicolon"),
        ] {
            let redis_client = redis::Client::open(redis_url.as_str())?;
            let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
            let key = format!("openplotva:test:decoded-rates:{suffix}");
            let update_queue =
                openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
            let mut redis = redis_client.get_multiplexed_async_connection().await?;
            let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

            let (prefix, server) = spawn_rbc_fixture_server(rbc_key_indicators_fixture())?;
            let provider = Arc::new(
                rbc_test_client()?
                    .with_key_indicators_url_prefix_for_test(Arc::<str>::from(prefix)),
            );
            let state_store = UpdateStateStoreStub::default();
            let virtual_store = VirtualStoreStub::default();
            let dispatcher_queue =
                Arc::new(openplotva_telegram::DispatcherQueue::new(Default::default()));
            let effects =
                RatesDispatcherEffects::new(virtual_store.clone(), Arc::clone(&dispatcher_queue))
                    .with_virtual_id_factory(Arc::new(move || virtual_id.to_owned()));
            let next = Arc::new(NextStub::default());
            let handler = RatesCommandUpdateHandler::new(
                bot_identity()?,
                Arc::new(PermissionStub::allowed()),
                Some(Arc::clone(&provider)),
                Arc::new(HeaderStub),
                Arc::new(effects),
                Arc::clone(&next),
            );

            let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            let update = sample_update(group_message_at(command, message_id, now_secs as i64)?)?;
            update_queue.enqueue_update(&update).await?;
            assert_eq!(update_queue.len().await?, 1);
            let decoded = update_queue
                .dequeue_update(Duration::from_secs(1))
                .await?
                .ok_or_else(|| io::Error::other("expected decoded rates update"))?;

            let report = process_update_with_state_store_at(
                decoded,
                UpdateConsumerConfig {
                    dequeue_timeout: Duration::from_millis(1),
                    state_timeout: Duration::from_secs(1),
                    handle_timeout: Duration::from_secs(1),
                    side_effect_max_age: Duration::from_secs(60),
                    worker_limit: 1,
                },
                UNIX_EPOCH + Duration::from_secs(now_secs),
                &state_store,
                |update| handler.handle_update(update),
            )
            .await;

            assert_eq!(report.update_id, 500);
            assert_eq!(report.update_name, "message");
            assert_eq!(report.state.outcome, UpdateStageOutcome::Completed);
            assert_eq!(
                report.handle.as_ref().map(|stage| &stage.outcome),
                Some(&UpdateStageOutcome::Completed)
            );
            assert!(!report.skipped_handle);
            assert_eq!(
                state_store.calls(),
                vec![
                    "chat:-10042:supergroup::".to_owned(),
                    "user:42:Ada:ada".to_owned()
                ]
            );
            assert!(next.calls().is_empty());
            assert_eq!(
                virtual_store.inserts(),
                vec![(virtual_id.to_owned(), -10042, Some(0))]
            );
            let item = dispatcher_queue
                .dequeue_regular()
                .ok_or_else(|| io::Error::other("expected decoded rates payload"))?;
            assert_eq!(item.metadata().virtual_id, virtual_id);
            let value = method_as_value(
                item.into_method()
                    .ok_or_else(|| io::Error::other("expected sendMessage method"))?,
            )?;
            assert_eq!(value["chat_id"], json!(-10042));
            assert_eq!(value["message_thread_id"], json!(9));
            assert_eq!(value["reply_parameters"]["message_id"], json!(message_id));
            assert_eq!(value["parse_mode"], json!("HTML"));
            assert!(
                value["text"]
                    .as_str()
                    .is_some_and(|text| text.contains("90.00 ₽") && text.contains("65000 $"))
            );
            assert_eq!(update_queue.len().await?, 0);

            let request = server
                .join()
                .map_err(|_| io::Error::other("RBC fixture server panicked"))??;
            assert_rbc_fixture_request_matches_go(&request);
            let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn rates_tool_dispatcher_queues_trimmed_html_side_effect()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = VirtualStoreStub::default();
        let queue = Arc::new(DispatcherQueue::new(Default::default()));
        let dispatcher = RatesToolDispatcherEffects::new(store.clone(), Arc::clone(&queue))
            .with_id_factories(
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
        assert_eq!(
            store.inserts(),
            vec![("rates-vmsg-1".to_owned(), -10042, Some(9))]
        );
        let snapshot = queue.snapshot();
        assert_eq!(snapshot.regular.len(), 1);
        assert_eq!(snapshot.regular[0].virtual_id, "rates-vmsg-1");
        assert!(snapshot.immediate.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn rates_tool_dispatcher_empty_text_is_go_noop() -> Result<(), Box<dyn std::error::Error>>
    {
        let dispatcher = RatesToolDispatcherEffects::new(
            VirtualStoreStub::default(),
            Arc::new(DispatcherQueue::new(Default::default())),
        );

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

    #[derive(Clone, Default)]
    struct UpdateStateStoreStub {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl UpdateStateStoreStub {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("state calls").clone()
        }
    }

    impl UpdateStateStore for UpdateStateStoreStub {
        type Error = io::Error;

        fn upsert_chat_state<'a>(
            &'a self,
            chat: &'a openplotva_core::ChatState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "chat:{}:{}:{}:{}",
                        chat.id,
                        chat.chat_type,
                        chat.first_name.as_deref().unwrap_or_default(),
                        chat.username.as_deref().unwrap_or_default()
                    ));
                Ok(())
            })
        }

        fn upsert_user_state<'a>(
            &'a self,
            user: &'a openplotva_core::UserState,
        ) -> UpdateStateStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!(
                        "user:{}:{}:{}",
                        user.id,
                        user.first_name,
                        user.username.as_deref().unwrap_or_default()
                    ));
                Ok(())
            })
        }

        fn upsert_telegram_file_metadata<'a>(
            &'a self,
            params: &'a openplotva_storage::TelegramFileMetadataUpsert,
        ) -> TelegramFileMetadataStoreFuture<'a, Self::Error> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .map_err(|err| io::Error::other(err.to_string()))?
                    .push(format!("file:{}", params.file_unique_id));
                Ok(())
            })
        }
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

    fn rbc_test_client() -> Result<RbcRatesClient, RbcRatesError> {
        RbcRatesClient::from_timeout_seconds(15)
    }

    fn rbc_key_indicators_fixture() -> &'static str {
        r#"{"shared_key_indicators_under_topline":[{"item":{"ticker":"USD/RUB F","name":"","subname":null,"change":1.1,"closevalue":90.0,"prepared":{"change":"+1.1%"}}},{"item":{"ticker":"EUR/RUB F","name":"","subname":null,"change":2.0,"closevalue":100.0,"prepared":{"change":"+2.0%"}}},{"item":{"ticker":"BTC/USD","name":"","subname":null,"change":0.0,"closevalue":65000.0,"prepared":{"change":""}}}]}"#
    }

    fn spawn_rbc_fixture_server(
        body: &'static str,
    ) -> io::Result<(String, thread::JoinHandle<io::Result<String>>)> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let handle = thread::spawn(move || {
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

            write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            )?;

            String::from_utf8(request)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
        });
        Ok((
            format!("http://{addr}/quote/ajax/key-indicator-update/?_="),
            handle,
        ))
    }

    fn assert_rbc_fixture_request_matches_go(request: &str) {
        let request_lower = request.to_ascii_lowercase();
        assert!(request.starts_with("GET /quote/ajax/key-indicator-update/?_="));
        assert!(request_lower.contains("\r\naccept: */*"));
        assert!(request_lower.contains("\r\naccept-language: ru,be;q=0.9,en-us;q=0.8,en;q=0.7"));
        assert!(request_lower.contains("\r\ncache-control: no-cache"));
        assert!(request_lower.contains("\r\npragma: no-cache"));
        assert!(request_lower.contains("\r\nreferer: https://www.rbc.ru/quote/ticker/338247"));
        assert!(request_lower.contains(&format!(
            "\r\nuser-agent: {}",
            RBC_HTTP_USER_AGENT.to_ascii_lowercase()
        )));
    }

    fn rbc_item(
        ticker: &str,
        name: &str,
        subname: Option<&str>,
        closevalue: f64,
        change: f64,
        prepared_change: &str,
    ) -> RbcKeyIndicatorItem {
        RbcKeyIndicatorItem {
            ticker: ticker.to_owned(),
            name: name.to_owned(),
            subname: subname.map(str::to_owned),
            change,
            closevalue,
            prepared: RbcPreparedValues {
                change: prepared_change.to_owned(),
            },
        }
    }

    type VirtualInsertCalls = Arc<Mutex<Vec<(String, i64, Option<i32>)>>>;

    #[derive(Clone, Default)]
    struct VirtualStoreStub {
        inserts: VirtualInsertCalls,
    }

    impl VirtualStoreStub {
        fn inserts(&self) -> Vec<(String, i64, Option<i32>)> {
            self.inserts.lock().expect("virtual store inserts").clone()
        }
    }

    impl VirtualMessageStore for VirtualStoreStub {
        type Error = io::Error;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Option<openplotva_core::MessageIdMapping>, Self::Error>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async { Ok(None) })
        }

        fn insert_virtual_message<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            thread_id: Option<i32>,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async move {
                self.inserts
                    .lock()
                    .expect("virtual store inserts")
                    .push((vmsg_id, chat_id, thread_id));
                Ok(())
            })
        }

        fn resolve_virtual_message<'a>(
            &'a self,
            _vmsg_id: String,
            _real_message_id: i32,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn enqueue_message_op<'a>(
            &'a self,
            _vmsg_id: String,
            _chat_id: i64,
            _op: &'static str,
            _payload_json: Option<String>,
        ) -> Pin<Box<dyn Future<Output = Result<i64, Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(1) })
        }

        fn delete_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
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

        fn fx_daily_last_two_closes<'a>(
            &'a self,
            from: &'static str,
            to: &'static str,
        ) -> RatesDailyFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().calls.push(format!("daily {from}/{to}"));
                Ok(match (from, to) {
                    ("USD", "RUB") => (90.0, 89.0),
                    ("EUR", "RUB") => (100.0, 98.0),
                    ("EUR", "USD") => (1.1111, 1.1),
                    _ => (0.0, 0.0),
                })
            })
        }

        fn currency_exchange_rate<'a>(
            &'a self,
            from: &'static str,
            to: &'static str,
        ) -> RatesSpotFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push(format!("spot {from}/{to}"));
                if state.btc_error {
                    return Err(io::Error::other("btc down"));
                }
                Ok(65000.0)
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

    fn method_as_value(
        method: openplotva_telegram::TelegramOutboundMethod,
    ) -> io::Result<serde_json::Value> {
        match method {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => {
                serde_json::to_value(method.as_ref()).map_err(io::Error::other)
            }
            other => Err(io::Error::other(format!(
                "unexpected Telegram method: {}",
                other.method_name()
            ))),
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
