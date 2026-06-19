//! App-level fetcher translation command and control-job behavior.

use std::{convert::Infallible, fmt, future::Future, pin::Pin, sync::Arc, time::Duration};

use carapax::types::{
    Chat as TelegramChat, Message as TelegramMessage, MessageData as TelegramMessageData,
    ReplyTo as TelegramReplyTo, Update as TelegramUpdate, UpdateType as TelegramUpdateType,
    User as TelegramUser,
};
use openplotva_taskman::{
    CONTROL_QUEUE_NAME, ControlJobData, ControlJobParams, ControlKind, HIGH_PRIORITY, JobType,
    StatelessJobItem, control_job_params_from_stateless_job, new_control_job_at,
};
use openplotva_telegram::{
    ChatRef, DispatcherQueue, OutboundBuildError, ReplyMessageRef, RichMessageRequest,
    TextMessageRequest, escape_telegram_html_text,
};
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;
use url::form_urlencoded;

use crate::updates::{UpdateHandler, UpdateHandlerFuture};
use crate::virtual_messages::{
    QueueRichRequest, QueueTextRequest, VirtualIdFactory, VirtualMessageStore,
    monotonic_virtual_id_factory, queue_rich_message, queue_text_message_parts,
};

const TRANSLATE_CONTROL_JOB_TITLE: &str = "translate";
const UNKNOWN_LANGUAGE_NOTICE_DELETE_AFTER: Duration = Duration::from_secs(120);
const QUEUE_ERROR_NOTICE_DELETE_AFTER: Duration = Duration::from_secs(60);
const QUEUE_ERROR_TEXT: &str = "❌ Не удалось поставить задачу в очередь.";

/// Boxed future returned by translation control-job queue calls.
pub type TranslateControlJobQueueFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by translation control-job worker queue calls.
pub type TranslateControlJobWorkerFuture<'a, T, E> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by translation side effects.
pub type TranslateEffectFuture<'a, E> = Pin<Box<dyn Future<Output = Result<(), E>> + Send + 'a>>;

/// Boxed future returned by translation providers.
pub type TranslateProviderFuture<'a, E> =
    Pin<Box<dyn Future<Output = Result<String, E>> + Send + 'a>>;

/// Boxed future returned by translation cache calls.
pub type TranslateCacheFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Boxed future returned by concrete translation provider clients.
pub type TranslateClientFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

pub trait TranslateSendPermission {
    fn can_send_translate_text(&self, chat: &TelegramChat) -> bool;
}

pub trait TranslateControlJobQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn assign_translate_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> TranslateControlJobQueueFuture<'a, Self::Error>;
}

impl<T> TranslateControlJobQueue for T
where
    T: crate::payments::PaymentControlJobQueue + Sync,
{
    type Error = T::Error;

    fn assign_translate_control_job<'a>(
        &'a self,
        queue_name: &'static str,
        job: StatelessJobItem,
    ) -> TranslateControlJobQueueFuture<'a, Self::Error> {
        self.assign_payment_control_job(queue_name, job)
    }
}

/// Queue/status boundary for translation-owned taskman control-job workers.
pub trait TranslateControlJobWorkerQueue {
    /// Error returned by the concrete queue implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn dequeue_translate_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> TranslateControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    >;

    /// Finalize one translation control job as completed.
    fn complete_translate_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> TranslateControlJobWorkerFuture<'a, (), Self::Error>;

    /// Finalize one translation control job as failed.
    fn fail_translate_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> TranslateControlJobWorkerFuture<'a, (), Self::Error>;
}

impl<T> TranslateControlJobWorkerQueue for T
where
    T: crate::payments::SharedControlJobWorkerQueue + Sync,
{
    type Error = T::Error;

    fn dequeue_translate_control_job<'a>(
        &'a self,
        queue_name: &'static str,
    ) -> TranslateControlJobWorkerFuture<
        'a,
        Option<crate::payments::PaymentControlJobWorkItem>,
        Self::Error,
    > {
        self.dequeue_shared_control_job_matching(
            queue_name,
            "translate-control",
            is_translate_control_job,
        )
    }

    fn complete_translate_control_job<'a>(
        &'a self,
        job_id: i64,
    ) -> TranslateControlJobWorkerFuture<'a, (), Self::Error> {
        self.complete_shared_control_job(job_id)
    }

    fn fail_translate_control_job<'a>(
        &'a self,
        job_id: i64,
        error: &'a str,
    ) -> TranslateControlJobWorkerFuture<'a, (), Self::Error> {
        self.fail_shared_control_job(job_id, error)
    }
}

pub trait TranslateEffects {
    /// Error returned by the concrete side-effect implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    fn send_translate_notice<'a>(
        &'a self,
        message: TranslateCommandMessage,
        text: String,
        delete_after: Duration,
    ) -> TranslateEffectFuture<'a, Self::Error>;

    fn send_translation_result<'a>(
        &'a self,
        plan: TranslateResultPlan,
    ) -> TranslateEffectFuture<'a, Self::Error>;
}

/// Generator for virtual-message IDs used by translation text sends.
pub type TranslateVirtualIdFactory = VirtualIdFactory;

#[derive(Clone)]
pub struct TranslateDispatcherEffects<Store> {
    store: Store,
    queue: Arc<DispatcherQueue>,
    next_virtual_id: TranslateVirtualIdFactory,
}

impl<Store> TranslateDispatcherEffects<Store> {
    /// Build translation effects backed by the normal outbound dispatcher.
    #[must_use]
    pub fn new(store: Store, queue: Arc<DispatcherQueue>) -> Self {
        Self {
            store,
            queue,
            next_virtual_id: monotonic_virtual_id_factory("translate-vmsg"),
        }
    }

    /// Override virtual-message ID generation for deterministic tests.
    #[must_use]
    pub fn with_virtual_id_factory(mut self, next_virtual_id: TranslateVirtualIdFactory) -> Self {
        self.next_virtual_id = next_virtual_id;
        self
    }
}

impl<Store> TranslateEffects for TranslateDispatcherEffects<Store>
where
    Store: VirtualMessageStore + Send + Sync,
{
    type Error = TranslateDispatchEffectError;

    fn send_translate_notice<'a>(
        &'a self,
        message: TranslateCommandMessage,
        text: String,
        delete_after: Duration,
    ) -> TranslateEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let chat = ChatRef {
                id: message.chat_id,
                is_forum: message.thread_id.is_some(),
            };
            let request = TextMessageRequest {
                chat: Some(chat),
                message_thread_id: 0,
                disable_notification: false,
                allow_sending_without_reply: None,
                text,
                render_as: String::new(),
                reply_markup: None,
            };
            let reply_to = ReplyMessageRef {
                message_id: message.message_id,
                chat,
                is_topic_message: message.thread_id.is_some(),
                message_thread_id: message.thread_id.map(i64::from).unwrap_or_default(),
            };

            queue_text_message_parts(
                &self.store,
                &self.queue,
                QueueTextRequest {
                    message: &request,
                    reply_to: Some(&reply_to),
                    immediate_first: true,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: Some(delete_after),
                },
                || (self.next_virtual_id)(),
            )
            .await?;
            Ok(())
        })
    }

    fn send_translation_result<'a>(
        &'a self,
        plan: TranslateResultPlan,
    ) -> TranslateEffectFuture<'a, Self::Error> {
        Box::pin(async move {
            let rich = queue_rich_message(
                &self.store,
                &self.queue,
                QueueRichRequest {
                    message: &plan.rich_message,
                    reply_to: Some(&plan.reply_to),
                    immediate: false,
                    bypass_chat_restrictions: false,
                    ephemeral_delete_after: None,
                },
                || (self.next_virtual_id)(),
            )
            .await;
            if let Err(OutboundBuildError::RichMessageTooLong(_, _)) = rich {
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
            } else {
                rich?;
            }
            Ok(())
        })
    }
}

/// Recoverable errors from dispatcher-backed translation effects.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum TranslateDispatchEffectError {
    #[error("failed to queue translation text: {0}")]
    Queue(#[from] OutboundBuildError),
}

pub trait TextTranslator {
    /// Error returned by the concrete provider.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Translate text into a target language.
    fn translate_text<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> TranslateProviderFuture<'a, Self::Error>;
}

pub trait TranslationCache {
    /// Error returned by the concrete cache implementation.
    type Error: fmt::Display + Send + Sync + 'static;

    /// Load a cached translation by target language and exact source text.
    fn cached_translation<'a>(
        &'a self,
        target_lang: &'a str,
        text: &'a str,
    ) -> TranslateCacheFuture<'a, Option<String>, Self::Error>;

    /// Store a cached translation by target language and exact source text.
    fn store_translation<'a>(
        &'a self,
        target_lang: &'a str,
        text: &'a str,
        translation: &'a str,
        ttl: Duration,
    ) -> TranslateCacheFuture<'a, (), Self::Error>;
}

impl TranslationCache for openplotva_storage::RedisTranslationCacheStore {
    type Error = openplotva_storage::StorageError;

    fn cached_translation<'a>(
        &'a self,
        target_lang: &'a str,
        text: &'a str,
    ) -> TranslateCacheFuture<'a, Option<String>, Self::Error> {
        Box::pin(async move {
            openplotva_storage::RedisTranslationCacheStore::cached_translation(
                self,
                target_lang,
                text,
            )
            .await
        })
    }

    fn store_translation<'a>(
        &'a self,
        target_lang: &'a str,
        text: &'a str,
        translation: &'a str,
        ttl: Duration,
    ) -> TranslateCacheFuture<'a, (), Self::Error> {
        Box::pin(async move {
            openplotva_storage::RedisTranslationCacheStore::set_cached_translation_with_ttl(
                self,
                target_lang,
                text,
                translation,
                ttl,
            )
            .await
        })
    }
}

/// Translation cache that always misses and drops writes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoopTranslationCache;

impl TranslationCache for NoopTranslationCache {
    type Error = Infallible;

    fn cached_translation<'a>(
        &'a self,
        _target_lang: &'a str,
        _text: &'a str,
    ) -> TranslateCacheFuture<'a, Option<String>, Self::Error> {
        Box::pin(async { Ok(None) })
    }

    fn store_translation<'a>(
        &'a self,
        _target_lang: &'a str,
        _text: &'a str,
        _translation: &'a str,
        _ttl: Duration,
    ) -> TranslateCacheFuture<'a, (), Self::Error> {
        Box::pin(async { Ok(()) })
    }
}

pub trait DeeplTranslateClient {
    /// Error returned by the concrete DeepL client.
    type Error: fmt::Display + Send + Sync + 'static;

    fn translate_deepl<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> TranslateClientFuture<'a, Option<String>, Self::Error>;
}

pub trait GoogleTranslateClient {
    /// Error returned by the concrete Google client.
    type Error: fmt::Display + Send + Sync + 'static;

    fn translate_google<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> TranslateClientFuture<'a, String, Self::Error>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeeplTranslationConfig {
    /// DeepL auth key.
    pub key: String,
    /// DeepL endpoint prefix.
    pub url: String,
}

/// Build DeepL translation config from app config.
#[must_use]
pub fn deepl_translation_config_from_app_config(
    config: &openplotva_config::AppConfig,
) -> DeeplTranslationConfig {
    DeeplTranslationConfig {
        key: config.translation.deepl.key.clone(),
        url: config.translation.deepl.url.clone(),
    }
}

#[derive(Clone, Debug)]
pub struct T8Translator<Cache, Deepl, Google> {
    cache: Cache,
    deepl: Deepl,
    google: Google,
    cache_ttl: Duration,
}

impl<Cache, Deepl, Google> T8Translator<Cache, Deepl, Google> {
    #[must_use]
    pub fn new(cache: Cache, deepl: Deepl, google: Google) -> Self {
        Self {
            cache,
            deepl,
            google,
            cache_ttl: openplotva_storage::TRANSLATION_CACHE_TTL,
        }
    }

    /// Override cache TTL for deterministic tests.
    #[must_use]
    pub fn with_cache_ttl(mut self, cache_ttl: Duration) -> Self {
        self.cache_ttl = cache_ttl;
        self
    }
}

impl<Cache, Deepl, Google> TextTranslator for T8Translator<Cache, Deepl, Google>
where
    Cache: TranslationCache + Send + Sync,
    Deepl: DeeplTranslateClient + Send + Sync,
    Google: GoogleTranslateClient + Send + Sync,
{
    type Error = T8TranslatorError;

    fn translate_text<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> TranslateProviderFuture<'a, Self::Error> {
        Box::pin(async move {
            let target_lang = target_lang.trim();
            if translation_target_matches_detected_language(text, target_lang) {
                return Ok(text.to_owned());
            }

            match self.cache.cached_translation(target_lang, text).await {
                Ok(Some(translation)) => return Ok(translation),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(message = %error, "translation cache read failed");
                }
            }

            let translated = match self.deepl.translate_deepl(text, target_lang).await {
                Ok(Some(translation)) => translation,
                Ok(None) => self
                    .google
                    .translate_google(text, target_lang)
                    .await
                    .map_err(|error| T8TranslatorError::Google {
                        message: error.to_string(),
                    })?,
                Err(error) => {
                    return Err(T8TranslatorError::Deepl {
                        message: error.to_string(),
                    });
                }
            };

            if !translated.is_empty()
                && let Err(error) = self
                    .cache
                    .store_translation(target_lang, text, &translated, self.cache_ttl)
                    .await
            {
                tracing::warn!(message = %error, "translation cache write failed");
            }

            Ok(translated)
        })
    }
}

/// Fatal errors from the concrete translation orchestrator.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum T8TranslatorError {
    /// DeepL primary provider failed before fallback was available.
    #[error("DeepL translation failed: {message}")]
    Deepl {
        /// Error text.
        message: String,
    },
    /// Google fallback provider failed.
    #[error("Google translation failed: {message}")]
    Google {
        /// Error text.
        message: String,
    },
}

/// Reqwest-backed DeepL primary client.
#[derive(Clone, Debug)]
pub struct ReqwestDeeplTranslateClient {
    client: reqwest::Client,
    config: DeeplTranslationConfig,
}

impl ReqwestDeeplTranslateClient {
    pub fn new(config: DeeplTranslationConfig) -> Result<Self, ReqwestTranslationClientBuildError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|source| ReqwestTranslationClientBuildError::HttpClient {
                message: source.to_string(),
            })?;
        Ok(Self { client, config })
    }

    /// Build around an injected client, useful for tests.
    #[must_use]
    pub fn with_client(config: DeeplTranslationConfig, client: reqwest::Client) -> Self {
        Self { client, config }
    }
}

impl DeeplTranslateClient for ReqwestDeeplTranslateClient {
    type Error = ReqwestTranslationClientError;

    fn translate_deepl<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> TranslateClientFuture<'a, Option<String>, Self::Error> {
        Box::pin(async move {
            let request = build_deepl_translate_request(&self.config, text, target_lang);
            let mut builder = self.client.post(&request.url).body(request.body);
            for (name, value) in request.headers {
                builder = builder.header(name, value);
            }

            let response = match builder.send().await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(message = %error, "DeepL translation request failed; falling back");
                    return Ok(None);
                }
            };

            if !response.status().is_success() {
                tracing::warn!(
                    status = response.status().as_u16(),
                    "DeepL translation response was not successful; falling back"
                );
                return Ok(None);
            }

            let bytes = response.bytes().await.map_err(|source| {
                ReqwestTranslationClientError::ResponseRead {
                    message: source.to_string(),
                }
            })?;
            decode_deepl_translate_response(&bytes)
                .map(Some)
                .map_err(|source| ReqwestTranslationClientError::Decode {
                    message: source.to_string(),
                })
        })
    }
}

/// Reqwest-backed Google fallback client.
#[derive(Clone, Debug)]
pub struct ReqwestGoogleTranslateClient {
    client: reqwest::Client,
}

impl ReqwestGoogleTranslateClient {
    /// Build a reqwest client for the Google fallback.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Build around an injected client, useful for tests.
    #[must_use]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

impl Default for ReqwestGoogleTranslateClient {
    fn default() -> Self {
        Self::new()
    }
}

impl GoogleTranslateClient for ReqwestGoogleTranslateClient {
    type Error = ReqwestTranslationClientError;

    fn translate_google<'a>(
        &'a self,
        text: &'a str,
        target_lang: &'a str,
    ) -> TranslateClientFuture<'a, String, Self::Error> {
        Box::pin(async move {
            let request = build_google_translate_request(text, target_lang);
            let response = self
                .client
                .get(&request.url)
                .send()
                .await
                .map_err(|source| ReqwestTranslationClientError::Request {
                    message: source.to_string(),
                })?;
            let status = response.status();
            let bytes = response.bytes().await.map_err(|source| {
                ReqwestTranslationClientError::ResponseRead {
                    message: source.to_string(),
                }
            })?;
            if !status.is_success() {
                return Err(ReqwestTranslationClientError::HttpStatus {
                    status: status.as_u16(),
                });
            }
            if String::from_utf8_lossy(&bytes).contains("<title>Error 400 (Bad Request)") {
                return Err(ReqwestTranslationClientError::GoogleBadRequest);
            }
            decode_google_translate_response(&bytes).map_err(|source| {
                ReqwestTranslationClientError::Decode {
                    message: source.to_string(),
                }
            })
        })
    }
}

/// Runtime translation provider used by the app shell once control jobs are unified.
pub type RuntimeT8Translator = T8Translator<
    openplotva_storage::RedisTranslationCacheStore,
    ReqwestDeeplTranslateClient,
    ReqwestGoogleTranslateClient,
>;

/// Build the concrete runtime translator from app config and Redis cache.
pub fn t8_translator_from_app_config(
    config: &openplotva_config::AppConfig,
    cache: openplotva_storage::RedisTranslationCacheStore,
) -> Result<RuntimeT8Translator, T8TranslatorBuildError> {
    let deepl = deepl_translation_config_from_app_config(config);
    if deepl.key.is_empty() {
        return Err(T8TranslatorBuildError::MissingDeeplKey);
    }
    if deepl.url.is_empty() {
        return Err(T8TranslatorBuildError::MissingDeeplUrl);
    }
    let deepl_client = ReqwestDeeplTranslateClient::new(deepl)?;
    Ok(T8Translator::new(
        cache,
        deepl_client,
        ReqwestGoogleTranslateClient::new(),
    ))
}

/// Translation provider build errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum T8TranslatorBuildError {
    /// `DEEPL_KEY` is required when building the runtime translator.
    #[error("DEEPL_KEY is required for translation runtime")]
    MissingDeeplKey,
    /// `DEEPL_URL` is required when building the runtime translator.
    #[error("DEEPL_URL is required for translation runtime")]
    MissingDeeplUrl,
    /// Reqwest client construction failed.
    #[error("{0}")]
    HttpClient(#[from] ReqwestTranslationClientBuildError),
}

/// Reqwest translation client build errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ReqwestTranslationClientBuildError {
    /// Reqwest client construction failed.
    #[error("build translation HTTP client: {message}")]
    HttpClient {
        /// Error text.
        message: String,
    },
}

/// Reqwest translation client errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum ReqwestTranslationClientError {
    /// Request failed.
    #[error("translation request failed: {message}")]
    Request {
        /// Error text.
        message: String,
    },
    /// Response body read failed.
    #[error("read translation response: {message}")]
    ResponseRead {
        /// Error text.
        message: String,
    },
    /// Upstream returned a non-success status.
    #[error("translation HTTP status {status}")]
    HttpStatus {
        /// HTTP status code.
        status: u16,
    },
    /// Google returned its HTML bad-request marker.
    #[error("Error 400 (Bad Request)")]
    GoogleBadRequest,
    /// Response payload decode failed.
    #[error("decode translation response: {message}")]
    Decode {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeeplTranslateRequest {
    /// Request URL.
    pub url: String,
    /// Request headers in insertion order.
    pub headers: Vec<(String, String)>,
    /// URL-encoded request body.
    pub body: String,
}

/// Google fallback request plan matching `googletranslatefree`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GoogleTranslateRequest {
    /// Request URL.
    pub url: String,
}

#[must_use]
pub fn build_deepl_translate_request(
    config: &DeeplTranslationConfig,
    text: &str,
    target_lang: &str,
) -> DeeplTranslateRequest {
    let mut body = form_urlencoded::Serializer::new(String::new());
    body.append_pair("preserve_formatting", "1");
    body.append_pair("split_sentences", "0");
    body.append_pair("target_lang", &target_lang.to_uppercase());
    body.append_pair("text", text);

    DeeplTranslateRequest {
        url: format!("{}translate", config.url),
        headers: vec![
            (
                "Authorization".to_owned(),
                format!("DeepL-Auth-Key {}", config.key),
            ),
            (
                "Content-Type".to_owned(),
                "application/x-www-form-urlencoded".to_owned(),
            ),
        ],
        body: body.finish(),
    }
}

#[must_use]
pub fn build_google_translate_request(text: &str, target_lang: &str) -> GoogleTranslateRequest {
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("client", "gtx");
    query.append_pair("sl", "auto");
    query.append_pair("tl", target_lang);
    query.append_pair("dt", "t");
    query.append_pair("q", text);
    GoogleTranslateRequest {
        url: format!(
            "https://translate.googleapis.com/translate_a/single?{}",
            query.finish()
        ),
    }
}

#[derive(Debug, Deserialize)]
struct DeeplTranslateResponse {
    translations: Vec<DeeplTranslateText>,
}

#[derive(Debug, Deserialize)]
struct DeeplTranslateText {
    text: String,
}

fn decode_deepl_translate_response(body: &[u8]) -> Result<String, serde_json::Error> {
    let response: DeeplTranslateResponse = serde_json::from_slice(body)?;
    Ok(response
        .translations
        .first()
        .map(|translation| translation.text.clone())
        .unwrap_or_default())
}

pub fn decode_google_translate_response(body: &[u8]) -> Result<String, GoogleTranslateDecodeError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|source| GoogleTranslateDecodeError::Json {
            message: source.to_string(),
        })?;
    let sentences = value
        .get(0)
        .and_then(serde_json::Value::as_array)
        .ok_or(GoogleTranslateDecodeError::MissingTranslatedData)?;
    if sentences.is_empty() {
        return Err(GoogleTranslateDecodeError::MissingTranslatedData);
    }

    let mut translated = String::new();
    for sentence in sentences {
        let words = sentence
            .as_array()
            .ok_or(GoogleTranslateDecodeError::InvalidTranslatedData)?;
        let Some(first) = words.first() else {
            continue;
        };
        match first {
            serde_json::Value::String(value) => translated.push_str(value),
            serde_json::Value::Null => {}
            value => translated.push_str(&value.to_string()),
        }
    }
    if translated.is_empty() {
        return Err(GoogleTranslateDecodeError::MissingTranslatedData);
    }
    Ok(translated)
}

/// Google fallback response decode errors.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum GoogleTranslateDecodeError {
    /// JSON parsing failed.
    #[error("Error unmarshaling data: {message}")]
    Json {
        /// Error text.
        message: String,
    },
    /// Response did not contain translated data.
    #[error("No translated data in responce")]
    MissingTranslatedData,
    #[error("invalid translated data")]
    InvalidTranslatedData,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TranslateBotIdentity {
    /// Current Telegram bot user.
    pub user: TelegramUser,
}

/// Message context for translation command notices.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TranslateCommandMessage {
    /// Source chat ID.
    pub chat_id: i64,
    /// Source message ID.
    pub message_id: i64,
    /// Forum topic ID when present.
    pub thread_id: Option<i32>,
}

/// Planned translation result send.
#[derive(Clone, Debug, PartialEq)]
pub struct TranslateResultPlan {
    pub message: TextMessageRequest,
    pub rich_message: RichMessageRequest,
    pub reply_to: ReplyMessageRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TranslateCommandOutcome {
    /// Update did not belong to this slice and was delegated.
    Delegated,
    SkippedBotSender,
    PermissionDenied,
    /// Unknown language notice was attempted and the command was consumed.
    UnknownLanguage {
        language: String,
    },
    /// High-priority translation control job was queued.
    Queued,
    QueueError,
}

/// Result of one translation control-job execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TranslateControlJobOutcome {
    /// Translation result send succeeded.
    Sent,
    SendError {
        message: String,
    },
}

/// Result of one translation-owned taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TranslateControlJobWorkerReport {
    /// Whether a job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Translation executor result, when payload decoding succeeded.
    pub execution: Option<TranslateControlJobOutcome>,
    /// The job was finalized as completed.
    pub completed: bool,
    /// The job was finalized as failed.
    pub failed: bool,
    /// Queue dequeue failed.
    pub dequeue_error: Option<String>,
    pub decode_error: Option<String>,
    /// Translation provider or executor error.
    pub execution_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranslationResult {
    /// Trimmed source text.
    pub original: String,
    /// Provider result.
    pub translated: String,
    pub source_lang: String,
    /// Trimmed/defaulted target language.
    pub target_lang: String,
}

/// Fatal errors from the translation command wrapper.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TranslateCommandError {
    /// Downstream update handling failed.
    #[error("downstream update handler failed: {message}")]
    Downstream {
        /// Error text.
        message: String,
    },
}

/// Fatal errors from translation control-job execution.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum TranslateControlJobError {
    /// Translation provider failed.
    #[error("translation provider failed: {message}")]
    Provider {
        /// Error text.
        message: String,
    },
}

#[derive(Clone, Debug)]
pub struct TranslateCommandUpdateHandler<Permission, Queue, Effects, Next> {
    bot: TranslateBotIdentity,
    permission: Arc<Permission>,
    queue: Arc<Queue>,
    effects: Arc<Effects>,
    next: Arc<Next>,
}

impl<Permission, Queue, Effects, Next>
    TranslateCommandUpdateHandler<Permission, Queue, Effects, Next>
{
    /// Build a translation command handler around the real downstream handler.
    pub fn new(
        bot: TranslateBotIdentity,
        permission: Arc<Permission>,
        queue: Arc<Queue>,
        effects: Arc<Effects>,
        next: Arc<Next>,
    ) -> Self {
        Self {
            bot,
            permission,
            queue,
            effects,
            next,
        }
    }
}

impl<Permission, Queue, Effects, Next> UpdateHandler
    for TranslateCommandUpdateHandler<Permission, Queue, Effects, Next>
where
    Permission: TranslateSendPermission + Send + Sync,
    Queue: TranslateControlJobQueue + Send + Sync,
    Effects: TranslateEffects + Send + Sync,
    Next: UpdateHandler + Send + Sync,
{
    type Error = TranslateCommandError;

    fn handle_update<'a>(&'a self, update: TelegramUpdate) -> UpdateHandlerFuture<'a, Self::Error> {
        Box::pin(async move {
            handle_translate_command_update_or_else_at(
                &self.bot,
                self.permission.as_ref(),
                self.queue.as_ref(),
                self.effects.as_ref(),
                update,
                OffsetDateTime::now_utc(),
                |update| self.next.handle_update(update),
            )
            .await
            .map(|_| ())
        })
    }
}

pub async fn handle_translate_command_update_or_else_at<
    Permission,
    Queue,
    Effects,
    HandleFn,
    HandleFuture,
    HandleError,
>(
    bot: &TranslateBotIdentity,
    permission: &Permission,
    queue: &Queue,
    effects: &Effects,
    update: TelegramUpdate,
    created: OffsetDateTime,
    handle_other: HandleFn,
) -> Result<TranslateCommandOutcome, TranslateCommandError>
where
    Permission: TranslateSendPermission + Sync,
    Queue: TranslateControlJobQueue + Sync,
    Effects: TranslateEffects + Sync,
    HandleFn: FnOnce(TelegramUpdate) -> HandleFuture,
    HandleFuture: Future<Output = Result<(), HandleError>>,
    HandleError: fmt::Display,
{
    let TelegramUpdateType::Message(message) = &update.update_type else {
        handle_other(update)
            .await
            .map_err(|error| TranslateCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(TranslateCommandOutcome::Delegated);
    };

    let Some(words) = translate_command_words(message, &bot.user) else {
        handle_other(update)
            .await
            .map_err(|error| TranslateCommandError::Downstream {
                message: error.to_string(),
            })?;
        return Ok(TranslateCommandOutcome::Delegated);
    };

    let sender = openplotva_updates::resolve_message_sender(Some(message));
    if !openplotva_updates::should_handle_addressed_message(
        Some(message),
        &sender,
        i64::from(bot.user.id),
    ) {
        return Ok(TranslateCommandOutcome::SkippedBotSender);
    }

    if !permission.can_send_translate_text(&message.chat) {
        return Ok(TranslateCommandOutcome::PermissionDenied);
    }

    let plan = translate_control_job_from_message_words_at(message, &words, created);
    let job = match plan {
        TranslateCommandPlan::Job(job) => *job,
        TranslateCommandPlan::UnknownLanguage { language } => {
            try_send_translate_notice(
                effects,
                message,
                format!("Я не знаю такого языка: {language}"),
                UNKNOWN_LANGUAGE_NOTICE_DELETE_AFTER,
                "unknown language",
            )
            .await;
            return Ok(TranslateCommandOutcome::UnknownLanguage { language });
        }
    };

    if let Err(error) = queue
        .assign_translate_control_job(CONTROL_QUEUE_NAME, job)
        .await
    {
        tracing::warn!(message = %error, "failed to assign translation control job");
        try_send_translate_notice(
            effects,
            message,
            QUEUE_ERROR_TEXT.to_owned(),
            QUEUE_ERROR_NOTICE_DELETE_AFTER,
            "queue error",
        )
        .await;
        return Ok(TranslateCommandOutcome::QueueError);
    }

    Ok(TranslateCommandOutcome::Queued)
}

#[must_use]
pub fn translate_command_words(
    message: &TelegramMessage,
    bot: &TelegramUser,
) -> Option<Vec<String>> {
    let parsed = openplotva_updates::parse_if_addressed(message, bot);
    if !parsed.is_addressed {
        return None;
    }
    let first_word_lower = parsed.first_word.to_lowercase();
    if !openplotva_telegram::TRANSLATE_ALIASES.contains(&first_word_lower.as_str()) {
        return None;
    }
    Some(openplotva_updates::react_message_words(
        &first_word_lower,
        &parsed.rest_text,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TranslateCommandPlan {
    Job(Box<StatelessJobItem>),
    UnknownLanguage {
        /// Original user-provided language token.
        language: String,
    },
}

#[must_use]
pub fn translate_control_job_from_message_words_at(
    message: &TelegramMessage,
    words: &[String],
    created: OffsetDateTime,
) -> TranslateCommandPlan {
    let mut target_lang = "ru".to_owned();
    let mut body_words = words;

    if body_words.len() > 1 && body_words[0] == "на" {
        let language = body_words[1].clone();
        let lower = language.to_lowercase();
        let Some(code) = get_code_by_lang(&lower) else {
            return TranslateCommandPlan::UnknownLanguage { language };
        };
        target_lang = code.to_owned();
        body_words = &body_words[2..];
    }

    let body_text = if body_words.is_empty() {
        reply_message_text(message)
    } else {
        body_words.join(" ")
    };
    let reply_text = reply_message_text(message);

    let mut data = control_data_from_translate_message(message);
    data.kind = ControlKind::Translate;
    data.text = body_text;
    data.target_lang = target_lang;
    data.reply_text = reply_text;

    TranslateCommandPlan::Job(Box::new(
        new_control_job_at(control_job_params_from_message(message, data), created)
            .with_name(TRANSLATE_CONTROL_JOB_TITLE)
            .with_priority(HIGH_PRIORITY),
    ))
}

pub async fn execute_translate_control_job<Translator, Effects>(
    translator: &Translator,
    effects: &Effects,
    params: &ControlJobParams,
) -> Result<TranslateControlJobOutcome, TranslateControlJobError>
where
    Translator: TextTranslator + Sync,
    Effects: TranslateEffects + Sync,
{
    let translated = translator
        .translate_text(&params.data.text, &params.data.target_lang)
        .await
        .map_err(|error| TranslateControlJobError::Provider {
            message: error.to_string(),
        })?;
    let plan = translate_result_plan_from_control_job(params, translated);
    match effects.send_translation_result(plan).await {
        Ok(()) => Ok(TranslateControlJobOutcome::Sent),
        Err(error) => Ok(TranslateControlJobOutcome::SendError {
            message: error.to_string(),
        }),
    }
}

/// Process one translation-owned taskman control job, if available.
pub async fn process_translate_control_job_once_at<Queue, Translator, Effects>(
    queue: &Queue,
    translator: &Translator,
    effects: &Effects,
) -> TranslateControlJobWorkerReport
where
    Queue: TranslateControlJobWorkerQueue + Sync,
    Translator: TextTranslator + Sync,
    Effects: TranslateEffects + Sync,
{
    let mut report = TranslateControlJobWorkerReport::default();
    let item = match queue
        .dequeue_translate_control_job(CONTROL_QUEUE_NAME)
        .await
    {
        Ok(item) => item,
        Err(error) => {
            report.dequeue_error = Some(error.to_string());
            return report;
        }
    };

    let Some(item) = item else {
        return report;
    };
    report.dequeued = true;
    report.job_id = Some(item.id);

    let params = match control_job_params_from_stateless_job(&item.job) {
        Ok(params) => params,
        Err(error) => {
            let error = error.to_string();
            report.decode_error = Some(error.clone());
            mark_translate_control_job_failed(queue, item.id, &error, &mut report).await;
            return report;
        }
    };

    if params.data.kind != ControlKind::Translate {
        let error = format!(
            "unsupported translate control job kind: {:?}",
            params.data.kind
        );
        report.execution_error = Some(error.clone());
        mark_translate_control_job_failed(queue, item.id, &error, &mut report).await;
        return report;
    }

    match execute_translate_control_job(translator, effects, &params).await {
        Ok(execution) => {
            match queue.complete_translate_control_job(item.id).await {
                Ok(()) => report.completed = true,
                Err(error) => report.status_error = Some(error.to_string()),
            }
            report.execution = Some(execution);
        }
        Err(error) => {
            let error = error.to_string();
            report.execution_error = Some(error.clone());
            mark_translate_control_job_failed(queue, item.id, &error, &mut report).await;
        }
    }
    report
}

async fn mark_translate_control_job_failed<Queue>(
    queue: &Queue,
    job_id: i64,
    error: &str,
    report: &mut TranslateControlJobWorkerReport,
) where
    Queue: TranslateControlJobWorkerQueue + Sync,
{
    match queue.fail_translate_control_job(job_id, error).await {
        Ok(()) => report.failed = true,
        Err(error) => report.status_error = Some(error.to_string()),
    }
}

fn is_translate_control_job(job: &StatelessJobItem) -> bool {
    job.data.job_type == JobType::Control
        && matches!(
            job.data.control_data.as_ref().map(|data| data.kind),
            Some(ControlKind::Translate)
        )
}

#[must_use]
pub fn translate_result_plan_from_control_job(
    params: &ControlJobParams,
    translated: String,
) -> TranslateResultPlan {
    let chat = ChatRef {
        id: params.chat_id,
        is_forum: params.thread_id.is_some(),
    };
    TranslateResultPlan {
        message: TextMessageRequest {
            chat: Some(chat),
            message_thread_id: 0,
            disable_notification: false,
            allow_sending_without_reply: None,
            text: translated.clone(),
            render_as: String::new(),
            reply_markup: None,
        },
        rich_message: RichMessageRequest {
            chat: Some(chat),
            message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
            disable_notification: false,
            allow_sending_without_reply: None,
            html: compose_translation_rich(&translated, &params.data.target_lang),
            reply_markup: None,
        },
        reply_to: ReplyMessageRef {
            message_id: i64::from(params.message_id),
            chat,
            is_topic_message: params.thread_id.is_some(),
            message_thread_id: params.thread_id.map(i64::from).unwrap_or_default(),
        },
    }
}

fn compose_translation_rich(translated: &str, target_lang: &str) -> String {
    let mut html = format!(
        "<h3>Перевод</h3><p>{}</p>",
        escape_telegram_html_text(translated).replace('\n', "<br>")
    );
    let target = target_lang.trim();
    if !target.is_empty() {
        html.push_str(&format!(
            "<footer>Язык: {}</footer>",
            escape_telegram_html_text(target)
        ));
    }
    html
}

pub async fn translate_text_tool<Translator>(
    translator: &Translator,
    text: &str,
    target_lang: &str,
) -> Result<Option<TranslationResult>, Translator::Error>
where
    Translator: TextTranslator + Sync,
{
    let original = text.trim();
    if original.is_empty() {
        return Ok(None);
    }

    let target_lang = match target_lang.trim() {
        "" => "ru",
        lang => lang,
    };
    let translated = translator.translate_text(original, target_lang).await?;

    Ok(Some(TranslationResult {
        original: original.to_owned(),
        translated,
        source_lang: detect_translation_source_language(original).to_owned(),
        target_lang: target_lang.to_owned(),
    }))
}

#[must_use]
pub fn get_code_by_lang(lang: &str) -> Option<&'static str> {
    Some(match lang {
        "английский" => "en",
        "белорусский" | "беларуский" => "be",
        "болгарский" => "bg",
        "венгерский" => "hu",
        "голландский" => "nl",
        "греческий" => "el",
        "датский" => "da",
        "испанский" => "es",
        "итальянский" => "it",
        "китайский" => "zh",
        "латышский" => "lv",
        "литовский" => "lt",
        "немецкий" => "de",
        "польский" => "pl",
        "португальский" => "pt",
        "румынский" => "ro",
        "русский" => "ru",
        "сербский" => "sr",
        "словацкий" => "sk",
        "словенский" => "sl",
        "украинский" => "uk",
        "финский" => "fi",
        "французский" => "fr",
        "чешский" => "cs",
        "шведский" => "sv",
        "японский" => "ja",
        _ => return None,
    })
}

#[must_use]
pub fn detect_translation_source_language(text: &str) -> &'static str {
    if text.chars().count() < 3 {
        return "";
    }
    if text
        .chars()
        .any(|ch| ('\u{0400}'..='\u{04ff}').contains(&ch))
    {
        return "ru";
    }
    "en"
}

fn translation_target_matches_detected_language(text: &str, target_lang: &str) -> bool {
    let source_lang = detect_translation_source_language(text);
    !source_lang.is_empty() && source_lang.eq_ignore_ascii_case(target_lang)
}

fn control_job_params_from_message(
    message: &TelegramMessage,
    data: ControlJobData,
) -> ControlJobParams {
    let sender = openplotva_updates::resolve_message_sender(Some(message));
    let user = message.sender.get_user();
    let user_id = if sender.id != 0 {
        sender.id
    } else {
        user.map(|user| i64::from(user.id)).unwrap_or_default()
    };
    let user_full_name = user.map(user_full_name).unwrap_or_default();
    let user_full_name = if user_full_name.trim().is_empty() {
        sender.display_name()
    } else {
        user_full_name
    };
    ControlJobParams {
        chat_id: message.chat.get_id().into(),
        message_id: i32::try_from(message.id).unwrap_or_default(),
        user_id,
        user_full_name,
        thread_id: message
            .message_thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok())
            .filter(|thread_id| *thread_id != 0),
        data,
    }
}

fn control_data_from_translate_message(message: &TelegramMessage) -> ControlJobData {
    let mut data = ControlJobData {
        chat_type: chat_type_name(&message.chat).to_owned(),
        ..ControlJobData::default()
    };
    if let Some(user) = message.sender.get_user() {
        data.user_name = user
            .username
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        data.first_name = user.first_name.clone();
        data.last_name = user.last_name.clone().unwrap_or_default();
        data.language_code = user.language_code.clone().unwrap_or_default();
        data.is_premium = user.is_premium.unwrap_or_default();
    }
    data
}

fn reply_message_text(message: &TelegramMessage) -> String {
    let Some(TelegramReplyTo::Message(reply)) = message.reply_to.as_ref() else {
        return String::new();
    };
    let TelegramMessageData::Text(text) = &reply.data else {
        return String::new();
    };
    text.data.clone()
}

fn chat_type_name(chat: &TelegramChat) -> &'static str {
    match chat {
        TelegramChat::Channel(_) => "channel",
        TelegramChat::Group(_) => "group",
        TelegramChat::Private(_) => "private",
        TelegramChat::Supergroup(_) => "supergroup",
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

fn translate_command_message(message: &TelegramMessage) -> TranslateCommandMessage {
    TranslateCommandMessage {
        chat_id: message.chat.get_id().into(),
        message_id: message.id,
        thread_id: message
            .message_thread_id
            .and_then(|thread_id| i32::try_from(thread_id).ok())
            .filter(|thread_id| *thread_id != 0),
    }
}

async fn try_send_translate_notice<Effects>(
    effects: &Effects,
    message: &TelegramMessage,
    text: String,
    delete_after: Duration,
    context: &'static str,
) where
    Effects: TranslateEffects + Sync,
{
    if let Err(error) = effects
        .send_translate_notice(translate_command_message(message), text, delete_after)
        .await
    {
        tracing::warn!(
            message = %error,
            context,
            "failed to send translation command notice"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payments::{InMemoryPaymentControlJobQueue, InMemoryPaymentControlJobStatus};
    use crate::updates::{
        TelegramFileMetadataStoreFuture, UpdateHandler, UpdateHandlerFuture, UpdateStateStore,
        UpdateStateStoreFuture, process_update_with_state_store_at,
    };
    use crate::virtual_messages::VirtualMessageStore;
    use openplotva_core::MessageIdMapping;
    use openplotva_taskman::{JobPayload, JobType, control_job_params_from_stateless_job};
    use openplotva_telegram::{DispatcherConfig, DispatcherQueue, TelegramOutboundMethodKind};
    use openplotva_updates::{UpdateConsumerConfig, UpdateStageOutcome};
    use serde_json::{Value, json};
    use std::{
        env,
        future::Future,
        io,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard},
        time::{SystemTime, UNIX_EPOCH},
    };

    #[tokio::test]
    async fn translate_command_queues_go_control_job_for_addressed_group_message()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let permission = PermissionStub::allowed();
        let queue = QueueStub::default();
        let effects = EffectsStub::default();
        let next = NextStub::default();
        let created = OffsetDateTime::from_unix_timestamp(1_779_193_800)?;

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &permission,
            &queue,
            &effects,
            sample_update(group_message(
                "Plotva, переведи на английский привет",
                77,
                None,
            )?)?,
            created,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, TranslateCommandOutcome::Queued);
        assert!(next.calls().is_empty());
        assert!(effects.notices().is_empty());
        let assigned = queue.assigned();
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].0, CONTROL_QUEUE_NAME);
        assert_eq!(assigned[0].1.title, "translate");
        assert_eq!(assigned[0].1.priority, HIGH_PRIORITY);
        let JobPayload {
            job_type,
            telegram_data,
            control_data,
            ..
        } = &assigned[0].1.data;
        assert_eq!(*job_type, JobType::Control);
        let telegram = telegram_data.as_ref().expect("telegram data");
        assert_eq!(telegram.chat_id, -10042);
        assert_eq!(telegram.message_id, 77);
        assert_eq!(telegram.user_id, 42);
        assert_eq!(telegram.thread_message_id, Some(9));
        assert_eq!(telegram.user_full_name, "Ada Lovelace");
        let data = control_data.as_ref().expect("control data");
        assert_eq!(data.kind, ControlKind::Translate);
        assert_eq!(data.text, "привет");
        assert_eq!(data.target_lang, "en");
        assert_eq!(data.reply_text, "");
        assert_eq!(data.chat_type, "supergroup");
        assert_eq!(data.user_name, "ada");
        assert_eq!(data.first_name, "Ada");
        assert_eq!(data.last_name, "Lovelace");
        assert_eq!(data.language_code, "en");
        assert!(data.is_premium);
        Ok(())
    }

    #[tokio::test]
    async fn translate_command_uses_reply_text_after_language_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let queue = QueueStub::default();

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &PermissionStub::allowed(),
            &queue,
            &EffectsStub::default(),
            sample_update(group_message(
                "Plotva, переведи на немецкий",
                78,
                Some("reply source"),
            )?)?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, TranslateCommandOutcome::Queued);
        let assigned = queue.assigned();
        let data = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("control data");
        assert_eq!(data.text, "reply source");
        assert_eq!(data.target_lang, "de");
        assert_eq!(data.reply_text, "reply source");
        Ok(())
    }

    #[tokio::test]
    async fn translate_command_preserves_go_no_reply_fallback_without_language_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let queue = QueueStub::default();

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &PermissionStub::allowed(),
            &queue,
            &EffectsStub::default(),
            sample_update(group_message("Plotva, переведи", 79, Some("reply source"))?)?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, TranslateCommandOutcome::Queued);
        let assigned = queue.assigned();
        let data = assigned[0]
            .1
            .data
            .control_data
            .as_ref()
            .expect("control data");
        assert_eq!(data.text, "переведи");
        assert_eq!(data.target_lang, "ru");
        assert_eq!(data.reply_text, "reply source");
        Ok(())
    }

    #[tokio::test]
    async fn translate_command_unknown_language_sends_two_minute_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let queue = QueueStub::default();
        let effects = EffectsStub::default();

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &PermissionStub::allowed(),
            &queue,
            &effects,
            sample_update(private_message("translate на клингонский test", 80, None)?)?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(
            outcome,
            TranslateCommandOutcome::UnknownLanguage {
                language: "клингонский".to_owned()
            }
        );
        assert!(queue.assigned().is_empty());
        assert_eq!(
            effects.notices(),
            vec![(
                TranslateCommandMessage {
                    chat_id: 42,
                    message_id: 80,
                    thread_id: None
                },
                "Я не знаю такого языка: клингонский".to_owned(),
                Duration::from_secs(120)
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn translate_command_queue_error_sends_one_minute_notice()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let queue = QueueStub::failing();
        let effects = EffectsStub::default();

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &PermissionStub::allowed(),
            &queue,
            &effects,
            sample_update(private_message("translate hello", 81, None)?)?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, TranslateCommandOutcome::QueueError);
        assert_eq!(
            effects.notices(),
            vec![(
                TranslateCommandMessage {
                    chat_id: 42,
                    message_id: 81,
                    thread_id: None
                },
                QUEUE_ERROR_TEXT.to_owned(),
                Duration::from_secs(60)
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn translate_command_delegates_unaddressed_group_message()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let next = NextStub::default();

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &PermissionStub::allowed(),
            &QueueStub::default(),
            &EffectsStub::default(),
            sample_update(group_message("переведи hello", 82, None)?)?,
            OffsetDateTime::UNIX_EPOCH,
            |update| next.handle_update(update),
        )
        .await?;

        assert_eq!(outcome, TranslateCommandOutcome::Delegated);
        assert_eq!(next.calls(), vec![500]);
        Ok(())
    }

    #[tokio::test]
    async fn translate_command_permission_denied_consumes_without_queue()
    -> Result<(), Box<dyn std::error::Error>> {
        let bot = bot_identity()?;
        let queue = QueueStub::default();

        let outcome = handle_translate_command_update_or_else_at(
            &bot,
            &PermissionStub::denied(),
            &queue,
            &EffectsStub::default(),
            sample_update(private_message("translate hello", 83, None)?)?,
            OffsetDateTime::UNIX_EPOCH,
            |_update| async { Ok::<(), io::Error>(()) },
        )
        .await?;

        assert_eq!(outcome, TranslateCommandOutcome::PermissionDenied);
        assert!(queue.assigned().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn translate_control_job_executes_provider_and_sends_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let effects = EffectsStub::default();
        let translator = TranslatorStub::default();
        let params = control_job_params_from_stateless_job(&sample_translate_job()?)?;

        let outcome = execute_translate_control_job(&translator, &effects, &params).await?;

        assert_eq!(outcome, TranslateControlJobOutcome::Sent);
        assert_eq!(
            translator.calls(),
            vec![("source text".to_owned(), "en".to_owned())]
        );
        let results = effects.results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message.text, "[en] source text");
        assert_eq!(results[0].message.render_as, "");
        assert_eq!(results[0].message.message_thread_id, 0);
        assert_eq!(results[0].reply_to.message_id, 77);
        assert_eq!(results[0].reply_to.message_thread_id, 9);
        assert!(results[0].reply_to.is_topic_message);
        Ok(())
    }

    #[tokio::test]
    async fn translate_control_worker_skips_settings_job_and_completes_translate_job()
    -> Result<(), Box<dyn std::error::Error>> {
        let queue = InMemoryPaymentControlJobQueue::new();
        let effects = EffectsStub::default();
        let translator = TranslatorStub::default();
        let settings_job =
            translate_test_control_job(ControlKind::GroupSettings, "group settings")?;
        let translate_job = sample_translate_job()?;
        queue
            .assign_translate_control_job(CONTROL_QUEUE_NAME, settings_job)
            .await?;
        queue
            .assign_translate_control_job(CONTROL_QUEUE_NAME, translate_job.clone())
            .await?;

        let report = process_translate_control_job_once_at(&queue, &translator, &effects).await;

        assert!(report.dequeued);
        assert_eq!(report.job_id, Some(2));
        assert_eq!(report.execution, Some(TranslateControlJobOutcome::Sent));
        assert!(report.completed);
        assert!(!report.failed);
        assert_eq!(
            translator.calls(),
            vec![("source text".to_owned(), "en".to_owned())]
        );
        assert_eq!(effects.results().len(), 1);
        let snapshot = queue.snapshot();
        assert_eq!(snapshot[0].status, InMemoryPaymentControlJobStatus::Pending);
        assert_eq!(
            snapshot[1].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(snapshot[1].job, translate_job);
        Ok(())
    }

    #[tokio::test]
    async fn live_redis_decoded_translate_command_assigns_executes_and_queues_result_when_url_is_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(redis_url) = env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let redis_client = redis::Client::open(redis_url)?;
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let key = format!("openplotva:test:decoded-translate:{suffix}");
        let update_queue =
            openplotva_updates::RedisUpdateQueue::with_key(redis_client.clone(), key.clone());
        let mut redis = redis_client.get_multiplexed_async_connection().await?;
        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;

        let state_store = UpdateStateStoreStub::default();
        let control_queue = Arc::new(InMemoryPaymentControlJobQueue::new());
        let virtual_store = VirtualStoreStub::default();
        let dispatcher_queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let effects =
            TranslateDispatcherEffects::new(virtual_store.clone(), Arc::clone(&dispatcher_queue))
                .with_virtual_id_factory(Arc::new(|| "translate-live-redis-vmsg-1".to_owned()));
        let effects = Arc::new(effects);
        let next = Arc::new(NextStub::default());
        let handler = TranslateCommandUpdateHandler::new(
            bot_identity()?,
            Arc::new(PermissionStub::allowed()),
            Arc::clone(&control_queue),
            Arc::clone(&effects),
            Arc::clone(&next),
        );

        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let update = sample_update(group_message_at(
            "Plotva, translate на английский hello",
            85,
            None,
            now_secs as i64,
        )?)?;
        update_queue.enqueue_update(&update).await?;
        assert_eq!(update_queue.len().await?, 1);
        let decoded = update_queue
            .dequeue_update(Duration::from_secs(1))
            .await?
            .ok_or_else(|| io::Error::other("expected decoded translate update"))?;

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
        assert_eq!(control_queue.snapshot().len(), 1);
        let job = &control_queue.snapshot()[0].job;
        assert_eq!(job.title, "translate");
        let data = job.data.control_data.as_ref().expect("control data");
        assert_eq!(data.kind, ControlKind::Translate);
        assert_eq!(data.text, "hello");
        assert_eq!(data.target_lang, "en");

        let translator = TranslatorStub::default();
        let worker_report = process_translate_control_job_once_at(
            control_queue.as_ref(),
            &translator,
            effects.as_ref(),
        )
        .await;
        assert!(worker_report.dequeued);
        assert_eq!(worker_report.job_id, Some(1));
        assert_eq!(
            worker_report.execution,
            Some(TranslateControlJobOutcome::Sent)
        );
        assert!(worker_report.completed);
        assert!(!worker_report.failed);
        assert_eq!(
            translator.calls(),
            vec![("hello".to_owned(), "en".to_owned())]
        );
        assert_eq!(
            control_queue.snapshot()[0].status,
            InMemoryPaymentControlJobStatus::Completed
        );
        assert_eq!(
            virtual_store.inserted(),
            vec![("translate-live-redis-vmsg-1".to_owned(), -10042, Some(0))]
        );
        let item = dispatcher_queue
            .dequeue_regular()
            .ok_or_else(|| io::Error::other("expected translation result payload"))?;
        assert_eq!(item.metadata().virtual_id, "translate-live-redis-vmsg-1");
        let value = method_as_value(
            item.into_method()
                .ok_or_else(|| io::Error::other("expected sendMessage method"))?,
        )?;
        assert_eq!(value["chat_id"], json!(-10042));
        assert_eq!(value["text"], json!("[en] hello"));
        assert_eq!(value["reply_parameters"]["message_id"], json!(85));
        assert_eq!(value["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            value["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        assert_eq!(update_queue.len().await?, 0);

        let _: i64 = redis::cmd("DEL").arg(&key).query_async(&mut redis).await?;
        Ok(())
    }

    #[tokio::test]
    async fn translate_control_job_returns_provider_error_without_send()
    -> Result<(), Box<dyn std::error::Error>> {
        let translator = TranslatorStub::failing();
        let effects = EffectsStub::default();
        let params = control_job_params_from_stateless_job(&sample_translate_job()?)?;

        let error = execute_translate_control_job(&translator, &effects, &params)
            .await
            .expect_err("provider error");

        assert_eq!(
            error,
            TranslateControlJobError::Provider {
                message: "translator down".to_owned()
            }
        );
        assert!(effects.results().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn translate_tool_matches_go_adapter_trim_default_and_source_guess()
    -> Result<(), Box<dyn std::error::Error>> {
        let translator = TranslatorStub::default();

        let result = translate_text_tool(&translator, "  hello  ", "").await?;

        assert_eq!(
            result,
            Some(TranslationResult {
                original: "hello".to_owned(),
                translated: "[ru] hello".to_owned(),
                source_lang: "en".to_owned(),
                target_lang: "ru".to_owned(),
            })
        );
        assert!(
            translate_text_tool(&translator, "  ", "en")
                .await?
                .is_none()
        );
        assert_eq!(detect_translation_source_language("привет"), "ru");
        assert_eq!(detect_translation_source_language("hi"), "");
        Ok(())
    }

    #[test]
    fn language_lookup_matches_go_t8r_map() {
        assert_eq!(get_code_by_lang("английский"), Some("en"));
        assert_eq!(get_code_by_lang("беларуский"), Some("be"));
        assert_eq!(get_code_by_lang("японский"), Some("ja"));
        assert_eq!(get_code_by_lang("клингонский"), None);
    }

    #[tokio::test]
    async fn t8_translator_returns_original_when_detected_language_matches_target()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = TranslationCacheStub::default();
        let deepl = DeeplClientStub::with_translations([Some("should-not-call".to_owned())]);
        let google = GoogleClientStub::with_translations(["should-not-call".to_owned()]);
        let translator = T8Translator::new(cache.clone(), deepl.clone(), google.clone());

        let translated = translator.translate_text("hello", "en").await?;

        assert_eq!(translated, "hello");
        assert!(cache.get_calls().is_empty());
        assert!(deepl.calls().is_empty());
        assert!(google.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn t8_translator_uses_cache_before_deepl_or_google()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = TranslationCacheStub::with_cached("cached привет");
        let deepl = DeeplClientStub::with_translations([Some("deepl привет".to_owned())]);
        let google = GoogleClientStub::with_translations(["google привет".to_owned()]);
        let translator = T8Translator::new(cache.clone(), deepl.clone(), google.clone());

        let translated = translator.translate_text("hello", "ru").await?;

        assert_eq!(translated, "cached привет");
        assert_eq!(
            cache.get_calls(),
            vec![("ru".to_owned(), "hello".to_owned())]
        );
        assert!(cache.set_calls().is_empty());
        assert!(deepl.calls().is_empty());
        assert!(google.calls().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn t8_translator_uses_deepl_then_caches_non_empty_result()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = TranslationCacheStub::default();
        let deepl = DeeplClientStub::with_translations([Some("deepl привет".to_owned())]);
        let google = GoogleClientStub::with_translations(["google привет".to_owned()]);
        let translator = T8Translator::new(cache.clone(), deepl.clone(), google.clone());

        let translated = translator.translate_text("hello", "ru").await?;

        assert_eq!(translated, "deepl привет");
        assert_eq!(deepl.calls(), vec![("hello".to_owned(), "ru".to_owned())]);
        assert!(google.calls().is_empty());
        assert_eq!(
            cache.set_calls(),
            vec![(
                "ru".to_owned(),
                "hello".to_owned(),
                "deepl привет".to_owned(),
                Duration::from_secs(24 * 60 * 60),
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn t8_translator_falls_back_to_google_when_deepl_has_no_response()
    -> Result<(), Box<dyn std::error::Error>> {
        let cache = TranslationCacheStub::default();
        let deepl = DeeplClientStub::with_translations([None]);
        let google = GoogleClientStub::with_translations(["google привет".to_owned()]);
        let translator = T8Translator::new(cache.clone(), deepl.clone(), google.clone());

        let translated = translator.translate_text("hello", "ru").await?;

        assert_eq!(translated, "google привет");
        assert_eq!(deepl.calls(), vec![("hello".to_owned(), "ru".to_owned())]);
        assert_eq!(google.calls(), vec![("hello".to_owned(), "ru".to_owned())]);
        assert_eq!(cache.set_calls()[0].2, "google привет");
        Ok(())
    }

    #[test]
    fn deepl_translate_request_matches_go_deepl_binding_shape() {
        let request = build_deepl_translate_request(
            &DeeplTranslationConfig {
                key: "secret".to_owned(),
                url: "https://api-free.deepl.com/v2/".to_owned(),
            },
            "hello world",
            "ru",
        );

        assert_eq!(request.url, "https://api-free.deepl.com/v2/translate");
        assert_eq!(
            request.headers,
            vec![
                (
                    "Authorization".to_owned(),
                    "DeepL-Auth-Key secret".to_owned()
                ),
                (
                    "Content-Type".to_owned(),
                    "application/x-www-form-urlencoded".to_owned()
                ),
            ]
        );
        assert_eq!(
            request.body,
            "preserve_formatting=1&split_sentences=0&target_lang=RU&text=hello+world"
        );
    }

    #[test]
    fn google_translate_request_and_decoder_match_go_fallback_shape()
    -> Result<(), Box<dyn std::error::Error>> {
        let request = build_google_translate_request("hello world", "ru");

        assert_eq!(
            request.url,
            "https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=ru&dt=t&q=hello+world"
        );
        assert_eq!(
            decode_google_translate_response(
                r#"[[["Привет ","hello ",null,null],["мир","world",null,null]],null,"en"]"#
                    .as_bytes(),
            )?,
            "Привет мир"
        );
        Ok(())
    }

    #[tokio::test]
    async fn translate_dispatcher_effects_queue_result_through_regular_send_text()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = VirtualStoreStub::default();
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let effects = TranslateDispatcherEffects::new(store.clone(), Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "translate-vmsg-1".to_owned()));
        let chat = ChatRef {
            id: -10042,
            is_forum: true,
        };

        effects
            .send_translation_result(TranslateResultPlan {
                message: TextMessageRequest {
                    chat: Some(chat),
                    message_thread_id: 0,
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    text: "привет".to_owned(),
                    render_as: String::new(),
                    reply_markup: None,
                },
                rich_message: RichMessageRequest {
                    chat: Some(chat),
                    message_thread_id: 9,
                    disable_notification: false,
                    allow_sending_without_reply: None,
                    html: compose_translation_rich("привет", "ru"),
                    reply_markup: None,
                },
                reply_to: ReplyMessageRef {
                    message_id: 77,
                    chat,
                    is_topic_message: true,
                    message_thread_id: 9,
                },
            })
            .await?;

        assert_eq!(
            store.inserted(),
            vec![("translate-vmsg-1".to_owned(), -10042, Some(9))]
        );
        assert!(queue.dequeue_immediate().is_none());
        let item = queue.dequeue_regular().expect("queued translation result");
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendRichMessage)
        );
        assert!(item.ephemeral_delete_after().is_none());
        assert!(!item.bypasses_chat_restrictions());
        let (_metadata, method) = item.into_parts();
        let payload = method_as_value(method.expect("queued method"))?;
        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(
            payload["html"],
            json!("<h3>Перевод</h3><p>привет</p><footer>Язык: ru</footer>")
        );
        assert_eq!(payload["options"]["reply_to_message_id"], json!(77));
        assert_eq!(
            payload["options"]["allow_sending_without_reply"],
            json!(true)
        );
        Ok(())
    }

    #[tokio::test]
    async fn translate_dispatcher_effects_queue_unknown_language_notice_as_ephemeral_immediate()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = VirtualStoreStub::default();
        let queue = Arc::new(DispatcherQueue::new(DispatcherConfig::default()));
        let effects = TranslateDispatcherEffects::new(store.clone(), Arc::clone(&queue))
            .with_virtual_id_factory(Arc::new(|| "translate-notice-vmsg-1".to_owned()));

        effects
            .send_translate_notice(
                TranslateCommandMessage {
                    chat_id: -10042,
                    message_id: 78,
                    thread_id: Some(9),
                },
                "Я не знаю такого языка: xx".to_owned(),
                Duration::from_secs(120),
            )
            .await?;

        assert_eq!(
            store.inserted(),
            vec![("translate-notice-vmsg-1".to_owned(), -10042, Some(0))]
        );
        let item = queue
            .dequeue_immediate()
            .expect("queued translation notice");
        assert!(queue.dequeue_regular().is_none());
        assert_eq!(
            item.method_kind(),
            Some(TelegramOutboundMethodKind::SendMessage)
        );
        assert_eq!(
            item.ephemeral_delete_after(),
            Some(Duration::from_secs(120))
        );
        assert!(!item.bypasses_chat_restrictions());
        let (_metadata, method) = item.into_parts();
        let payload = method_as_value(method.expect("queued method"))?;
        assert_eq!(payload["chat_id"], json!(-10042));
        assert_eq!(payload["text"], json!("Я не знаю такого языка: xx"));
        assert_eq!(payload["reply_parameters"]["message_id"], json!(78));
        assert_eq!(payload["reply_parameters"]["chat_id"], json!(-10042));
        assert_eq!(
            payload["reply_parameters"]["allow_sending_without_reply"],
            json!(true)
        );
        Ok(())
    }

    fn bot_identity() -> Result<TranslateBotIdentity, serde_json::Error> {
        Ok(TranslateBotIdentity {
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

    fn private_message(
        text: &str,
        message_id: i64,
        reply: Option<&str>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        message_json(
            json!({"id": 42, "type": "private", "first_name": "Ada"}),
            text,
            message_id,
            reply,
            false,
            1_710_000_000,
        )
    }

    fn group_message(
        text: &str,
        message_id: i64,
        reply: Option<&str>,
    ) -> Result<TelegramMessage, serde_json::Error> {
        group_message_at(text, message_id, reply, 1_710_000_000)
    }

    fn group_message_at(
        text: &str,
        message_id: i64,
        reply: Option<&str>,
        date: i64,
    ) -> Result<TelegramMessage, serde_json::Error> {
        message_json(
            json!({"id": -10042, "type": "supergroup", "title": "Plotva Group"}),
            text,
            message_id,
            reply,
            true,
            date,
        )
    }

    fn message_json(
        chat: serde_json::Value,
        text: &str,
        message_id: i64,
        reply: Option<&str>,
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
                "username": "ada",
                "language_code": "en",
                "is_premium": true
            },
            "text": text
        });
        if topic {
            value["message_thread_id"] = json!(9);
        }
        if let Some(reply) = reply {
            value["reply_to_message"] = json!({
                "message_id": 700,
                "date": date.saturating_sub(1),
                "chat": value["chat"].clone(),
                "from": {"id": 43, "is_bot": false, "first_name": "Grace"},
                "text": reply
            });
        }
        serde_json::from_value(value)
    }

    fn sample_translate_job() -> Result<StatelessJobItem, serde_json::Error> {
        let message = group_message("Plotva, переведи на английский source text", 77, None)?;
        let plan = translate_control_job_from_message_words_at(
            &message,
            &[
                "на".to_owned(),
                "английский".to_owned(),
                "source".to_owned(),
                "text".to_owned(),
            ],
            OffsetDateTime::UNIX_EPOCH,
        );
        let TranslateCommandPlan::Job(job) = plan else {
            panic!("expected job");
        };
        Ok(*job)
    }

    fn translate_test_control_job(
        kind: ControlKind,
        title: &str,
    ) -> Result<StatelessJobItem, serde_json::Error> {
        let mut job = sample_translate_job()?;
        job.title = title.to_owned();
        if let Some(data) = job.data.control_data.as_mut() {
            data.kind = kind;
        }
        Ok(job)
    }

    #[derive(Clone, Copy)]
    struct PermissionStub {
        allowed: bool,
    }

    impl PermissionStub {
        fn allowed() -> Self {
            Self { allowed: true }
        }

        fn denied() -> Self {
            Self { allowed: false }
        }
    }

    impl TranslateSendPermission for PermissionStub {
        fn can_send_translate_text(&self, _chat: &TelegramChat) -> bool {
            self.allowed
        }
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

    #[derive(Default)]
    struct QueueStub {
        state: Mutex<QueueState>,
    }

    #[derive(Default)]
    struct QueueState {
        assigned: Vec<(&'static str, StatelessJobItem)>,
        fail: bool,
    }

    impl QueueStub {
        fn failing() -> Self {
            Self {
                state: Mutex::new(QueueState {
                    fail: true,
                    ..QueueState::default()
                }),
            }
        }

        fn assigned(&self) -> Vec<(&'static str, StatelessJobItem)> {
            self.lock().assigned.clone()
        }

        fn lock(&self) -> MutexGuard<'_, QueueState> {
            self.state.lock().expect("queue state")
        }
    }

    impl TranslateControlJobQueue for QueueStub {
        type Error = io::Error;

        fn assign_translate_control_job<'a>(
            &'a self,
            queue_name: &'static str,
            job: StatelessJobItem,
        ) -> TranslateControlJobQueueFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.assigned.push((queue_name, job));
                if state.fail {
                    return Err(io::Error::other("queue down"));
                }
                Ok(())
            })
        }
    }

    #[derive(Default)]
    struct EffectsStub {
        state: Mutex<EffectsState>,
    }

    #[derive(Default)]
    struct EffectsState {
        notices: Vec<(TranslateCommandMessage, String, Duration)>,
        results: Vec<TranslateResultPlan>,
        fail_result: bool,
    }

    impl EffectsStub {
        fn notices(&self) -> Vec<(TranslateCommandMessage, String, Duration)> {
            self.lock().notices.clone()
        }

        fn results(&self) -> Vec<TranslateResultPlan> {
            self.lock().results.clone()
        }

        fn lock(&self) -> MutexGuard<'_, EffectsState> {
            self.state.lock().expect("effects state")
        }
    }

    impl TranslateEffects for EffectsStub {
        type Error = io::Error;

        fn send_translate_notice<'a>(
            &'a self,
            message: TranslateCommandMessage,
            text: String,
            delete_after: Duration,
        ) -> TranslateEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                self.lock().notices.push((message, text, delete_after));
                Ok(())
            })
        }

        fn send_translation_result<'a>(
            &'a self,
            plan: TranslateResultPlan,
        ) -> TranslateEffectFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.results.push(plan);
                if state.fail_result {
                    return Err(io::Error::other("send down"));
                }
                Ok(())
            })
        }
    }

    #[derive(Clone, Default)]
    struct TranslationCacheStub {
        state: Arc<Mutex<TranslationCacheState>>,
    }

    #[derive(Default)]
    struct TranslationCacheState {
        cached: Option<String>,
        get_calls: Vec<(String, String)>,
        set_calls: Vec<(String, String, String, Duration)>,
    }

    impl TranslationCacheStub {
        fn with_cached(value: &str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TranslationCacheState {
                    cached: Some(value.to_owned()),
                    ..TranslationCacheState::default()
                })),
            }
        }

        fn get_calls(&self) -> Vec<(String, String)> {
            self.state
                .lock()
                .expect("translation cache")
                .get_calls
                .clone()
        }

        fn set_calls(&self) -> Vec<(String, String, String, Duration)> {
            self.state
                .lock()
                .expect("translation cache")
                .set_calls
                .clone()
        }
    }

    impl TranslationCache for TranslationCacheStub {
        type Error = io::Error;

        fn cached_translation<'a>(
            &'a self,
            target_lang: &'a str,
            text: &'a str,
        ) -> TranslateCacheFuture<'a, Option<String>, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("translation cache");
                state
                    .get_calls
                    .push((target_lang.to_owned(), text.to_owned()));
                Ok(state.cached.clone())
            })
        }

        fn store_translation<'a>(
            &'a self,
            target_lang: &'a str,
            text: &'a str,
            translation: &'a str,
            ttl: Duration,
        ) -> TranslateCacheFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.state
                    .lock()
                    .expect("translation cache")
                    .set_calls
                    .push((
                        target_lang.to_owned(),
                        text.to_owned(),
                        translation.to_owned(),
                        ttl,
                    ));
                Ok(())
            })
        }
    }

    #[derive(Clone)]
    struct DeeplClientStub {
        state: Arc<Mutex<DeeplClientState>>,
    }

    struct DeeplClientState {
        translations: Vec<Option<String>>,
        calls: Vec<(String, String)>,
    }

    impl DeeplClientStub {
        fn with_translations<const N: usize>(translations: [Option<String>; N]) -> Self {
            Self {
                state: Arc::new(Mutex::new(DeeplClientState {
                    translations: translations.into_iter().rev().collect(),
                    calls: Vec::new(),
                })),
            }
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.state.lock().expect("deepl client").calls.clone()
        }
    }

    impl DeeplTranslateClient for DeeplClientStub {
        type Error = io::Error;

        fn translate_deepl<'a>(
            &'a self,
            text: &'a str,
            target_lang: &'a str,
        ) -> TranslateClientFuture<'a, Option<String>, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("deepl client");
                state.calls.push((text.to_owned(), target_lang.to_owned()));
                Ok(state.translations.pop().unwrap_or(None))
            })
        }
    }

    #[derive(Clone)]
    struct GoogleClientStub {
        state: Arc<Mutex<GoogleClientState>>,
    }

    struct GoogleClientState {
        translations: Vec<String>,
        calls: Vec<(String, String)>,
    }

    impl GoogleClientStub {
        fn with_translations<const N: usize>(translations: [String; N]) -> Self {
            Self {
                state: Arc::new(Mutex::new(GoogleClientState {
                    translations: translations.into_iter().rev().collect(),
                    calls: Vec::new(),
                })),
            }
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.state.lock().expect("google client").calls.clone()
        }
    }

    impl GoogleTranslateClient for GoogleClientStub {
        type Error = io::Error;

        fn translate_google<'a>(
            &'a self,
            text: &'a str,
            target_lang: &'a str,
        ) -> TranslateClientFuture<'a, String, Self::Error> {
            Box::pin(async move {
                let mut state = self.state.lock().expect("google client");
                state.calls.push((text.to_owned(), target_lang.to_owned()));
                state
                    .translations
                    .pop()
                    .ok_or_else(|| io::Error::other("missing google response"))
            })
        }
    }

    #[derive(Default)]
    struct TranslatorStub {
        state: Mutex<TranslatorState>,
    }

    #[derive(Default)]
    struct TranslatorState {
        calls: Vec<(String, String)>,
        fail: bool,
    }

    impl TranslatorStub {
        fn failing() -> Self {
            Self {
                state: Mutex::new(TranslatorState {
                    fail: true,
                    ..TranslatorState::default()
                }),
            }
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.lock().calls.clone()
        }

        fn lock(&self) -> MutexGuard<'_, TranslatorState> {
            self.state.lock().expect("translator state")
        }
    }

    impl TextTranslator for TranslatorStub {
        type Error = io::Error;

        fn translate_text<'a>(
            &'a self,
            text: &'a str,
            target_lang: &'a str,
        ) -> TranslateProviderFuture<'a, Self::Error> {
            Box::pin(async move {
                let mut state = self.lock();
                state.calls.push((text.to_owned(), target_lang.to_owned()));
                if state.fail {
                    return Err(io::Error::other("translator down"));
                }
                Ok(format!("[{target_lang}] {text}"))
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

    type InsertedVirtualMessages = Arc<Mutex<Vec<(String, i64, Option<i32>)>>>;

    #[derive(Clone, Default)]
    struct VirtualStoreStub {
        inserted: InsertedVirtualMessages,
    }

    impl VirtualStoreStub {
        fn inserted(&self) -> Vec<(String, i64, Option<i32>)> {
            self.inserted.lock().expect("inserted virtual ids").clone()
        }
    }

    impl VirtualMessageStore for VirtualStoreStub {
        type Error = io::Error;

        fn get_mapping_by_virtual<'a>(
            &'a self,
            _vmsg_id: String,
        ) -> Pin<Box<dyn Future<Output = Result<Option<MessageIdMapping>, Self::Error>> + Send + 'a>>
        {
            Box::pin(async { Ok(None) })
        }

        fn insert_virtual_message<'a>(
            &'a self,
            vmsg_id: String,
            chat_id: i64,
            thread_id: Option<i32>,
        ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + 'a>> {
            self.inserted
                .lock()
                .expect("inserted virtual ids")
                .push((vmsg_id, chat_id, thread_id));
            Box::pin(async { Ok(()) })
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

    fn method_as_value(method: openplotva_telegram::TelegramOutboundMethod) -> io::Result<Value> {
        match method {
            openplotva_telegram::TelegramOutboundMethod::SendMessage(method) => {
                serde_json::to_value(method.as_ref()).map_err(io::Error::other)
            }
            openplotva_telegram::TelegramOutboundMethod::SendRichMessage(method) => {
                serde_json::to_value(method.as_ref()).map_err(io::Error::other)
            }
            other => Err(io::Error::other(format!(
                "unexpected Telegram method: {}",
                other.method_name()
            ))),
        }
    }
}
