//! App-level chat-history summary input assembly.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use futures_util::StreamExt;
use openplotva_config::AppConfig;
use openplotva_dialog::{HistorySummaryRequest, ToolboxError};
use openplotva_history::stages::{
    EVENTS_CHUNK_MAX_TOKENS, EVENTS_MIN_SUBSTANTIVE_MESSAGES, HistoryStage, chunk_items,
    decode_stage_events, decode_stage_recap, events_payload, item_carries_event, merge_events,
    recap_payload, substantive_message_count, summary_content, validated_events,
};
use openplotva_history::{
    DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS, DEFAULT_SUMMARY_MAX_INPUT_TOKENS,
    HistorySummaryDecodeError, HistorySummarySinceParseError, MIN_EDGE_RAW_MESSAGES,
    SUMMARY_PROMPT_VERSION, StoredSummary, SummaryDocument, SummaryInput, SummaryInputItem,
    SummaryRequest, SummaryScope, build_edge_raw_summary_input,
    build_ordered_summary_input_assembly, build_summary_input,
    decode_summary_message_entry_payloads, estimate_summary_tokens,
    filter_summary_entries_with_content, fit_summary_input_to_token_limit, hash_text,
    history_summary_generate_error_retryable, history_summary_model, history_summary_provider,
    history_summary_scope, history_summary_timeout_seconds, merge_edge_summary_input,
    normalize_generated_summary_content, normalize_summary_request, normalized_summary_window,
    parse_history_summary_since, render_history_summary_html, select_reusable_summary_spans,
    stamp_summary_input_metadata, summary_message_entry_timestamp, summary_requested_range,
    summary_reset_at,
};
use openplotva_llm::aifarm::{
    AifarmClientConfig, AifarmHistorySummaryConfig, AifarmHistorySummaryError,
    AifarmHistorySummaryGenerator, GenkitOpenAiCompatibleHistorySummaryConfig,
    GenkitOpenAiCompatibleHistorySummaryError, GenkitOpenAiCompatibleHistorySummaryGenerator,
    ReqwestAifarmTransport,
};
use openplotva_llm::gemini::{GeminiHistorySummaryConfig, GeminiHistorySummaryGenerator};
use openplotva_llm::retry::{FailureReason, retryable_reason, retryable_reason_from_message};
use openplotva_storage::{PostgresHistoryStore, StorageError};
use thiserror::Error;
use time::OffsetDateTime;

use crate::dialog_tools::{
    ChatHistorySummarizer, ChatHistorySummaryFuture, ChatHistorySummaryResult,
};
use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
};
use crate::runtime_gemini_cache::resolve_google_ai_key;

const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";
/// Stage-one chunks summarized at the same time.
const STAGE_ONE_CONCURRENCY: usize = 3;

/// Concrete app history-summary service used by the dialog toolbox runtime.
pub type AppHistorySummaryService =
    ChatHistorySummaryService<PostgresHistoryStore, RuntimeHistorySummaryGenerator>;

/// Boxed future returned by history reset stores.
pub type HistoryResetFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Option<OffsetDateTime>, HistorySummaryInputError>> + Send + 'a>,
>;

/// Boxed future returned by summary entry payload stores.
pub type HistorySummaryPayloadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<Vec<u8>>, HistorySummaryInputError>> + Send + 'a>>;

/// Boxed future returned by reusable summary stores.
pub type ReusableHistorySummariesFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<StoredSummary>, HistorySummaryInputError>> + Send + 'a>>;

/// Boxed future returned by history summary stage calls.
pub type HistoryStageFuture<'a> = Pin<
    Box<dyn Future<Output = Result<HistoryStageReply, HistorySummaryServiceError>> + Send + 'a>,
>;

/// A stage answer: the model's JSON text and the model that wrote it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HistoryStageReply {
    pub text: String,
    pub model: String,
}

/// Boxed future returned by history summary savers.
pub type HistorySummarySaveFuture<'a> =
    Pin<Box<dyn Future<Output = Result<StoredSummary, HistorySummaryServiceError>> + Send + 'a>>;

pub trait HistorySummaryInputStore: Send + Sync {
    fn history_reset_at<'a>(&'a self, chat_id: i64, thread_id: i32) -> HistoryResetFuture<'a>;

    fn summary_entry_payloads<'a>(
        &'a self,
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
    ) -> HistorySummaryPayloadFuture<'a>;

    /// Load reusable stored summaries for the normalized scope/range.
    fn reusable_history_summaries<'a>(
        &'a self,
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
        reset_at: OffsetDateTime,
    ) -> ReusableHistorySummariesFuture<'a>;
}

/// Provider boundary for one model call of the two-stage summary.
pub trait HistorySummaryGenerator: Send + Sync {
    /// Run one stage for the summary of `input`; `payload` is the user message.
    fn generate_history_stage<'a>(
        &'a self,
        input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a>;
}

/// Store boundary for persisting generated history summaries.
pub trait HistorySummarySaver: Send + Sync {
    /// Save one generated summary.
    fn save_history_summary<'a>(
        &'a self,
        input: &'a SummaryInput,
        doc: &'a SummaryDocument,
    ) -> HistorySummarySaveFuture<'a>;
}

/// Error returned while assembling a history-summary prompt input.
#[derive(Debug, Error)]
pub enum HistorySummaryInputError {
    #[error("chat_id is required")]
    ChatIdRequired,
    #[error("no chat history messages found for requested range")]
    NoMessages,
    #[error(transparent)]
    DecodeEntry(#[from] openplotva_history::SummaryEntryDecodeError),
    /// SQL/cache storage failed.
    #[error("history summary storage: {source}")]
    Storage {
        /// Concrete storage error.
        #[source]
        source: StorageError,
    },
    /// Test or injected store failed before a concrete storage adapter was involved.
    #[error("history summary store: {message}")]
    Store {
        /// Error message.
        message: String,
    },
}

/// Error returned by app-level history-summary generation orchestration.
#[derive(Debug, Error)]
pub enum HistorySummaryServiceError {
    /// The tool request carried an invalid RFC3339 `since` value.
    #[error(transparent)]
    Since(#[from] HistorySummarySinceParseError),
    /// Summary input loading failed.
    #[error(transparent)]
    Input(#[from] HistorySummaryInputError),
    /// Summary generation failed.
    #[error("history summary generation: {message}")]
    Generate {
        /// Error message.
        message: String,
    },
    /// Summary persistence failed.
    #[error("history summary save: {source}")]
    Save {
        /// Concrete storage error.
        #[source]
        source: StorageError,
    },
    #[error("history summary timed out after {seconds}s")]
    Timeout {
        /// Timeout in whole seconds.
        seconds: u64,
    },
}

/// Error returned while building a concrete history-summary runtime.
#[derive(Debug, Error)]
pub enum HistorySummaryRuntimeBuildError {
    #[error("google ai key is required for genkit history summary provider")]
    GoogleAiKeyRequired,
    /// GenKit OpenAI-compatible provider was requested without its API key.
    #[error("{provider} api key is required for genkit history summary provider")]
    ProviderApiKeyRequired {
        /// Provider name.
        provider: &'static str,
    },
    #[error("unsupported history summary provider {provider:?}")]
    UnsupportedProvider {
        /// Canonical provider name.
        provider: String,
    },
}

#[derive(Clone)]
pub enum RuntimeHistorySummaryGenerator {
    /// AIFarm history-summary path.
    Aifarm {
        /// Primary AIFarm generator.
        primary: Box<AifarmHistorySummaryGenerator>,
        primary_model: String,
        /// Runtime GenKit fallback branch, present only when a concrete runtime provider resolves.
        fallback: Option<Box<AppGenkitHistorySummaryGenerator>>,
    },
    Genkit(Box<AppGenkitHistorySummaryGenerator>),
}

/// App-owned GenKit history-summary branch.
#[derive(Clone)]
pub enum AppGenkitHistorySummaryGenerator {
    /// Direct Gemini implementation.
    Gemini(GeminiHistorySummaryGenerator<ReqwestAifarmTransport>),
    /// OpenAI-compatible GenKit plugin implementation.
    OpenAiCompatible(Box<GenkitOpenAiCompatibleHistorySummaryGenerator<ReqwestAifarmTransport>>),
}

/// App-owned GenKit history-summary error.
#[derive(Debug, Error)]
pub enum AppGenkitHistorySummaryError {
    /// Direct Gemini implementation failed.
    #[error(transparent)]
    Gemini(#[from] openplotva_llm::gemini::GeminiHistorySummaryError),
    /// OpenAI-compatible plugin implementation failed.
    #[error(transparent)]
    OpenAiCompatible(#[from] GenkitOpenAiCompatibleHistorySummaryError),
}

#[derive(Clone)]
pub struct RoutedHistorySummaryGenerator {
    walker: RoutedAttemptWalker,
    config: AppConfig,
}

impl RoutedHistorySummaryGenerator {
    #[must_use]
    pub fn new(walker: RoutedAttemptWalker, config: &AppConfig) -> Self {
        Self {
            walker,
            config: config.clone(),
        }
    }
}

/// App-level `chat_history_summary` service over injected store and generator boundaries.
#[derive(Clone)]
pub struct ChatHistorySummaryService<Store, Generator> {
    store: Arc<Store>,
    generator: Arc<Generator>,
    options: HistorySummaryServiceOptions,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistorySummaryServiceOptions {
    /// Estimated tokens of one stage-one chunk.
    pub max_input_tokens: i32,
    /// Minimum contiguous raw edge messages before pre-summary cascade.
    pub edge_min_raw_messages: i32,
    /// Stage prompts, used for the input-size estimate and the prompt hash.
    pub system_prompt: String,
    pub timeout: Duration,
}

impl Default for HistorySummaryServiceOptions {
    fn default() -> Self {
        Self {
            max_input_tokens: DEFAULT_SUMMARY_MAX_INPUT_TOKENS,
            edge_min_raw_messages: MIN_EDGE_RAW_MESSAGES,
            system_prompt: [
                include_str!("../../../prompts/history/events.prompt"),
                include_str!("../../../prompts/history/recap.prompt"),
            ]
            .concat(),
            timeout: Duration::from_secs(DEFAULT_HISTORY_SUMMARY_TIMEOUT_SECONDS as u64),
        }
    }
}

impl HistorySummaryServiceOptions {
    #[must_use]
    pub fn with_max_input_tokens(mut self, max_input_tokens: i32) -> Self {
        if max_input_tokens > 0 {
            self.max_input_tokens = max_input_tokens.min(DEFAULT_SUMMARY_MAX_INPUT_TOKENS);
        }
        self
    }

    /// Override edge pre-summary threshold for tests or future config wiring.
    #[must_use]
    pub fn with_edge_min_raw_messages(mut self, edge_min_raw_messages: i32) -> Self {
        if edge_min_raw_messages > 0 {
            self.edge_min_raw_messages = edge_min_raw_messages;
        }
        self
    }

    /// Use the same prompt text as the concrete generator.
    #[must_use]
    pub fn with_system_prompt(mut self, system_prompt: impl Into<String>) -> Self {
        self.system_prompt = system_prompt.into();
        self
    }

    #[must_use]
    pub fn with_timeout_seconds(mut self, timeout_seconds: i32) -> Self {
        self.timeout = Duration::from_secs(history_summary_timeout_seconds(timeout_seconds) as u64);
        self
    }
}

impl<Store, Generator> ChatHistorySummaryService<Store, Generator> {
    /// Build the service from shared store/generator handles.
    #[must_use]
    pub fn new(store: Arc<Store>, generator: Arc<Generator>) -> Self {
        Self::new_with_options(store, generator, HistorySummaryServiceOptions::default())
    }

    /// Build the service with explicit orchestration options.
    #[must_use]
    pub fn new_with_options(
        store: Arc<Store>,
        generator: Arc<Generator>,
        options: HistorySummaryServiceOptions,
    ) -> Self {
        Self {
            store,
            generator,
            options,
        }
    }
}

impl<Store, Generator> ChatHistorySummaryService<Store, Generator>
where
    Store: HistorySummaryInputStore + HistorySummarySaver + 'static,
    Generator: HistorySummaryGenerator + 'static,
{
    /// Execute the app-level history summary flow.
    pub async fn summarize(
        &self,
        request: HistorySummaryRequest,
    ) -> Result<ChatHistorySummaryResult, HistorySummaryServiceError> {
        let timeout = self.options.timeout;
        match tokio::time::timeout(timeout, self.summarize_inner(request)).await {
            Ok(result) => result,
            Err(_) => Err(HistorySummaryServiceError::Timeout {
                seconds: timeout.as_secs(),
            }),
        }
    }

    async fn summarize_inner(
        &self,
        request: HistorySummaryRequest,
    ) -> Result<ChatHistorySummaryResult, HistorySummaryServiceError> {
        let summary_request = summary_request_from_dialog_tool(&request)?;
        let mut input = build_history_summary_input(self.store.as_ref(), summary_request).await?;
        input = self.maybe_pre_summarize_edge_gap(input).await?;
        input = self.stamp_summary_input(input);
        let doc = self.generate_document(&input).await?;
        let stored = self.store.save_history_summary(&input, &doc).await?;
        Ok(ChatHistorySummaryResult::from_stored_summary(
            &stored,
            input.reused_summary_count,
        ))
    }

    async fn maybe_pre_summarize_edge_gap(
        &self,
        input: SummaryInput,
    ) -> Result<SummaryInput, HistorySummaryServiceError> {
        let Some(raw_input) =
            build_edge_raw_summary_input(&input, self.options.edge_min_raw_messages)
        else {
            return Ok(input);
        };
        let raw_input = self.stamp_summary_input(raw_input);
        let edge_doc = self.generate_document(&raw_input).await.map_err(|err| {
            HistorySummaryServiceError::Generate {
                message: format!("generate edge chat history summary: {err}"),
            }
        })?;
        let edge_stored = self
            .store
            .save_history_summary(&raw_input, &edge_doc)
            .await
            .map_err(|err| match err {
                HistorySummaryServiceError::Save { source } => {
                    HistorySummaryServiceError::Save { source }
                }
                other => HistorySummaryServiceError::Generate {
                    message: format!("save edge chat history summary: {other}"),
                },
            })?;
        Ok(merge_edge_summary_input(&input, &raw_input, &edge_stored))
    }

    /// Stamp the input hash and token estimate; every message stays, because
    /// long windows are split into chunks instead of thinned.
    fn stamp_summary_input(&self, input: SummaryInput) -> SummaryInput {
        fit_summary_input_to_token_limit(input, None, 0, &self.options.system_prompt).0
    }

    /// Decode a stage answer, asking once more when it is unreadable: stage one
    /// sends no response schema, and some transports send none for either stage.
    async fn decoded_stage<T>(
        &self,
        input: &SummaryInput,
        stage: HistoryStage,
        payload: &str,
        reply: HistoryStageReply,
        decode: fn(&str) -> Result<T, HistorySummaryDecodeError>,
    ) -> Result<(T, String), HistorySummaryServiceError> {
        if let Ok(answer) = decode(&reply.text) {
            return Ok((answer, reply.model));
        }
        let reply = self
            .generator
            .generate_history_stage(input, stage, payload)
            .await?;
        let answer = decode(&reply.text).map_err(|err| {
            let what = match stage {
                HistoryStage::Events => "events",
                HistoryStage::Recap => "recap",
            };
            HistorySummaryServiceError::Generate {
                message: format!("decode chat history {what}: {err}"),
            }
        })?;
        Ok((answer, reply.model))
    }

    /// Two stages: events from every chunk of the window (at most
    /// `STAGE_ONE_CONCURRENCY` chunks at a time), merged in code, then a recap
    /// written from those events. A window without events gets the quiet recap
    /// and no model call.
    async fn generate_document(
        &self,
        input: &SummaryInput,
    ) -> Result<SummaryDocument, HistorySummaryServiceError> {
        let items: Vec<SummaryInputItem> = input
            .items
            .iter()
            .filter(|item| item_carries_event(item))
            .cloned()
            .collect();
        let chunk_tokens = self.options.max_input_tokens.min(EVENTS_CHUNK_MAX_TOKENS);
        let chunks = chunk_items(&items, chunk_tokens);
        let last = chunks.len().saturating_sub(1);
        // A middle chunk with only a few substantive messages is noise; the last
        // chunk holds the newest messages and is kept whenever it has any.
        let chunks: Vec<Vec<SummaryInputItem>> = chunks
            .into_iter()
            .enumerate()
            .filter(|(index, chunk)| {
                let substantive = substantive_message_count(chunk);
                chunk.iter().any(|item| item.kind == "summary")
                    || (substantive > 0
                        && (*index == last || substantive >= EVENTS_MIN_SUBSTANTIVE_MESSAGES))
            })
            .map(|(_, chunk)| chunk)
            .collect();
        let payloads: Vec<String> = chunks
            .iter()
            .map(|chunk| events_payload(input, chunk))
            .collect();
        let calls: Vec<HistoryStageFuture<'_>> = payloads
            .iter()
            .map(|payload| {
                self.generator
                    .generate_history_stage(input, HistoryStage::Events, payload)
            })
            .collect();
        let replies: Vec<Result<HistoryStageReply, HistorySummaryServiceError>> =
            futures_util::stream::iter(calls)
                .buffered(STAGE_ONE_CONCURRENCY)
                .collect()
                .await;
        let mut model = String::new();
        let mut per_chunk = Vec::with_capacity(chunks.len());
        for ((chunk, payload), reply) in chunks.iter().zip(&payloads).zip(replies) {
            let (answer, reply_model) = self
                .decoded_stage(
                    input,
                    HistoryStage::Events,
                    payload,
                    reply?,
                    decode_stage_events,
                )
                .await?;
            per_chunk.push(validated_events(chunk, answer));
            model = reply_model;
        }
        let events = merge_events(per_chunk);
        let recap = if events.is_empty() {
            None
        } else {
            let payload = recap_payload(input, &events, OffsetDateTime::now_utc());
            let reply = self
                .generator
                .generate_history_stage(input, HistoryStage::Recap, &payload)
                .await?;
            let (recap, reply_model) = self
                .decoded_stage(
                    input,
                    HistoryStage::Recap,
                    &payload,
                    reply,
                    decode_stage_recap,
                )
                .await?;
            model = reply_model;
            Some(recap)
        };
        let content = normalize_generated_summary_content(summary_content(&events, recap));
        Ok(SummaryDocument {
            html: render_history_summary_html(&content),
            output_token_estimate: estimate_summary_tokens(&content),
            quality_score: content.quality_score,
            quality_notes: content.quality_notes.clone(),
            content,
            model,
            prompt_version: SUMMARY_PROMPT_VERSION.to_owned(),
            prompt_hash: hash_text(&self.options.system_prompt),
            input_hash: input.input_hash.clone(),
            input_token_estimate: input.input_token_estimate,
            cascade_depth: input.cascade_depth,
        })
    }
}

#[must_use]
pub fn aifarm_history_summary_config_from_app_config(
    config: &AppConfig,
) -> AifarmHistorySummaryConfig {
    let history = &config.llm.history_summary;
    let memory = &config.memory;
    let model = history_summary_model(
        &history.model,
        &history.provider,
        &memory.consolidation_model,
        None,
    );
    AifarmHistorySummaryConfig {
        client: AifarmClientConfig {
            base_url: config.llm.discovery.base_url.clone(),
            service_name: memory.aifarm_service_name.clone(),
            endpoint_name: memory.aifarm_endpoint_name.clone(),
            request_timeout: positive_seconds(memory.aifarm_request_timeout_seconds),
            poll_interval: positive_seconds(memory.aifarm_poll_interval_seconds),
            task_timeout: positive_seconds(memory.aifarm_task_timeout_seconds),
            capacity_wait: positive_seconds(memory.aifarm_capacity_wait_seconds),
            capacity_poll_interval: positive_seconds(memory.aifarm_capacity_poll_seconds),
            default_model: model.clone(),
            ..AifarmClientConfig::default()
        },
        model,
        max_output_tokens: 4096,
        temperature: memory.aifarm_temperature,
        top_p: None,
        top_k: None,
        enable_thinking: Some(false),
        include_reasoning: Some(false),
    }
    .with_defaults()
}

#[must_use]
pub fn gemini_history_summary_config_from_app_config(
    config: &AppConfig,
) -> GeminiHistorySummaryConfig {
    let history = &config.llm.history_summary;
    GeminiHistorySummaryConfig {
        api_key: resolve_google_ai_key(&config.google_ai),
        model: history_summary_model(
            &history.model,
            &history.provider,
            &config.memory.consolidation_model,
            Some(&crate::memory_runtime::genkit_runtime_default_model(config)),
        ),
        request_timeout: positive_seconds(history.timeout_seconds),
        ..GeminiHistorySummaryConfig::default()
    }
    .with_defaults()
}

#[must_use]
pub fn gemini_history_summary_fallback_config_from_app_config(
    config: &AppConfig,
) -> GeminiHistorySummaryConfig {
    GeminiHistorySummaryConfig {
        api_key: resolve_google_ai_key(&config.google_ai),
        model: crate::memory_runtime::genkit_runtime_default_model(config),
        request_timeout: positive_seconds(config.llm.history_summary.timeout_seconds),
        ..GeminiHistorySummaryConfig::default()
    }
    .with_defaults()
}

fn genkit_history_summary_generator_from_app_config(
    config: &AppConfig,
    model_override: Option<&str>,
) -> Result<AppGenkitHistorySummaryGenerator, HistorySummaryRuntimeBuildError> {
    let history = &config.llm.history_summary;
    let model = model_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map_or_else(
            || {
                history_summary_model(
                    &history.model,
                    &history.provider,
                    &config.memory.consolidation_model,
                    Some(&crate::memory_runtime::genkit_runtime_default_model(config)),
                )
            },
            ToOwned::to_owned,
        );
    if let Some(cfg) =
        genkit_openai_compatible_history_summary_config_from_app_config(config, &model)?
    {
        return Ok(AppGenkitHistorySummaryGenerator::OpenAiCompatible(
            Box::new(GenkitOpenAiCompatibleHistorySummaryGenerator::new(cfg)),
        ));
    }
    let cfg = GeminiHistorySummaryConfig {
        api_key: resolve_google_ai_key(&config.google_ai),
        model,
        request_timeout: positive_seconds(history.timeout_seconds),
        ..GeminiHistorySummaryConfig::default()
    }
    .with_defaults();
    if cfg.api_key.trim().is_empty() {
        return Err(HistorySummaryRuntimeBuildError::GoogleAiKeyRequired);
    }
    Ok(AppGenkitHistorySummaryGenerator::Gemini(
        GeminiHistorySummaryGenerator::new(cfg),
    ))
}

fn genkit_openai_compatible_history_summary_config_from_app_config(
    config: &AppConfig,
    model: &str,
) -> Result<Option<GenkitOpenAiCompatibleHistorySummaryConfig>, HistorySummaryRuntimeBuildError> {
    let model = model.trim();
    let (direct_url, api_key, model, request_timeout_seconds, provider) =
        if let Some(model) = strip_provider_prefix_fold(model, OPENROUTER_MODEL_PREFIX) {
            (
                OPENROUTER_CHAT_COMPLETIONS_URL,
                config.open_router.key.trim().to_owned(),
                model.trim().to_owned(),
                config.open_router.request_timeout_seconds,
                "openrouter",
            )
        } else {
            return Ok(None);
        };
    if model.is_empty() {
        return Ok(None);
    }
    if api_key.trim().is_empty() {
        return Err(HistorySummaryRuntimeBuildError::ProviderApiKeyRequired { provider });
    }
    Ok(Some(GenkitOpenAiCompatibleHistorySummaryConfig {
        direct_url: direct_url.to_owned(),
        api_key,
        model,
        request_timeout: positive_seconds(request_timeout_seconds),
        ..GenkitOpenAiCompatibleHistorySummaryConfig::default()
    }))
}

fn strip_provider_prefix_fold<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

/// Build the concrete history-summary service for dialog toolbox wiring.
pub fn history_summary_service_from_app_config(
    config: &AppConfig,
    store: Arc<PostgresHistoryStore>,
) -> Result<AppHistorySummaryService, HistorySummaryRuntimeBuildError> {
    let generator = history_summary_generator_from_app_config(config)?;
    let options = HistorySummaryServiceOptions::default()
        .with_max_input_tokens(config.memory.max_input_tokens)
        .with_timeout_seconds(config.llm.history_summary.timeout_seconds);
    Ok(ChatHistorySummaryService::new_with_options(
        store,
        Arc::new(generator),
        options,
    ))
}

pub fn routed_history_summary_service_from_app_config(
    config: &AppConfig,
    store: Arc<PostgresHistoryStore>,
    walker: RoutedAttemptWalker,
) -> ChatHistorySummaryService<PostgresHistoryStore, RoutedHistorySummaryGenerator> {
    let generator = RoutedHistorySummaryGenerator::new(walker, config);
    let options = HistorySummaryServiceOptions::default()
        .with_max_input_tokens(config.memory.max_input_tokens)
        .with_timeout_seconds(config.llm.history_summary.timeout_seconds);
    ChatHistorySummaryService::new_with_options(store, Arc::new(generator), options)
}

/// Build the concrete provider generator without binding storage.
pub fn history_summary_generator_from_app_config(
    config: &AppConfig,
) -> Result<RuntimeHistorySummaryGenerator, HistorySummaryRuntimeBuildError> {
    let provider = history_summary_provider(&config.llm.history_summary.provider);
    let generator = match provider {
        "aifarm" => {
            let cfg = aifarm_history_summary_config_from_app_config(config);
            let fallback = genkit_history_summary_generator_from_app_config(
                config,
                Some(&crate::memory_runtime::genkit_runtime_default_model(config)),
            )
            .ok()
            .map(Box::new);
            RuntimeHistorySummaryGenerator::Aifarm {
                primary_model: cfg.model.clone(),
                primary: Box::new(AifarmHistorySummaryGenerator::new(cfg)),
                fallback,
            }
        }
        "genkit" => RuntimeHistorySummaryGenerator::Genkit(Box::new(
            genkit_history_summary_generator_from_app_config(config, None)?,
        )),
        provider => {
            return Err(HistorySummaryRuntimeBuildError::UnsupportedProvider {
                provider: provider.to_owned(),
            });
        }
    };
    Ok(generator)
}

fn positive_seconds(seconds: i32) -> Duration {
    if seconds <= 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(seconds as u64)
    }
}

impl<Store, Generator> ChatHistorySummarizer for ChatHistorySummaryService<Store, Generator>
where
    Store: HistorySummaryInputStore + HistorySummarySaver + 'static,
    Generator: HistorySummaryGenerator + 'static,
{
    fn chat_history_summary<'a>(
        &'a self,
        request: HistorySummaryRequest,
    ) -> ChatHistorySummaryFuture<'a> {
        Box::pin(async move {
            self.summarize(request)
                .await
                .map_err(|err| Box::new(err) as ToolboxError)
        })
    }
}

pub fn summary_request_from_dialog_tool(
    request: &HistorySummaryRequest,
) -> Result<SummaryRequest, HistorySummaryServiceError> {
    let (scope, thread_id) = history_summary_scope(&request.scope, request.context.thread_id);
    Ok(SummaryRequest {
        chat_id: request.context.chat_id,
        thread_id,
        requested_by_user_id: request.context.user_id,
        scope,
        window: normalized_summary_window(&request.window),
        hours: request.hours,
        message_count: request.message_count,
        since: parse_history_summary_since(&request.since)?,
        now: None,
    })
}

pub async fn build_history_summary_input<S>(
    store: &S,
    request: SummaryRequest,
) -> Result<SummaryInput, HistorySummaryInputError>
where
    S: HistorySummaryInputStore + ?Sized,
{
    if request.chat_id == 0 {
        return Err(HistorySummaryInputError::ChatIdRequired);
    }
    let request = normalize_summary_request(request);
    let chat_reset_at = store.history_reset_at(request.chat_id, 0).await?;
    let thread_reset_at = if request.scope == SummaryScope::Thread && request.thread_id != 0 {
        store
            .history_reset_at(request.chat_id, request.thread_id)
            .await?
    } else {
        None
    };
    let reset_at = summary_reset_at(
        chat_reset_at,
        request.scope,
        request.thread_id,
        thread_reset_at,
    );
    let (range_start_at, range_end_at) = summary_requested_range(&request, reset_at);
    let payloads = store
        .summary_entry_payloads(
            request.chat_id,
            request.thread_id,
            request.scope,
            range_start_at,
            range_end_at,
        )
        .await?;
    let (entries, range_start_at, range_end_at) =
        prepare_summary_entries(&request, payloads, range_start_at, range_end_at)?;
    let reusable = store
        .reusable_history_summaries(
            request.chat_id,
            request.thread_id,
            request.scope,
            range_start_at,
            range_end_at,
            reset_at.unwrap_or_else(go_zero_time),
        )
        .await?;
    let selected = select_reusable_summary_spans(&reusable);
    let assembly = build_ordered_summary_input_assembly(&entries, &selected);
    let mut input = build_summary_input(
        &request,
        range_start_at,
        range_end_at,
        &entries,
        &selected,
        assembly,
    )
    .ok_or(HistorySummaryInputError::NoMessages)?;
    stamp_summary_input_metadata(&mut input);
    Ok(input)
}

fn prepare_summary_entries(
    request: &SummaryRequest,
    payloads: Vec<Vec<u8>>,
    mut range_start_at: OffsetDateTime,
    mut range_end_at: OffsetDateTime,
) -> Result<
    (
        Vec<openplotva_history::SummaryMessageEntry>,
        OffsetDateTime,
        OffsetDateTime,
    ),
    HistorySummaryInputError,
> {
    let mut entries =
        filter_summary_entries_with_content(&decode_summary_message_entry_payloads(&payloads)?);
    if request.window == openplotva_history::SummaryWindow::Messages
        && entries.len() > request.message_count as usize
    {
        entries = entries[entries.len() - request.message_count as usize..].to_vec();
    }
    if entries.is_empty() {
        return Err(HistorySummaryInputError::NoMessages);
    }
    if request.window == openplotva_history::SummaryWindow::Messages {
        range_start_at = summary_message_entry_timestamp(&entries[0]);
        range_end_at = summary_message_entry_timestamp(&entries[entries.len() - 1]);
    }
    Ok((entries, range_start_at, range_end_at))
}

impl HistorySummaryInputStore for PostgresHistoryStore {
    fn history_reset_at<'a>(&'a self, chat_id: i64, thread_id: i32) -> HistoryResetFuture<'a> {
        Box::pin(async move {
            PostgresHistoryStore::history_reset_at(self, chat_id, thread_id)
                .await
                .map_err(|source| HistorySummaryInputError::Storage { source })
        })
    }

    fn summary_entry_payloads<'a>(
        &'a self,
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
    ) -> HistorySummaryPayloadFuture<'a> {
        Box::pin(async move {
            PostgresHistoryStore::summary_entry_payloads(
                self,
                chat_id,
                thread_id,
                scope,
                range_start_at,
                range_end_at,
            )
            .await
            .map_err(|source| HistorySummaryInputError::Storage { source })
        })
    }

    fn reusable_history_summaries<'a>(
        &'a self,
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
        reset_at: OffsetDateTime,
    ) -> ReusableHistorySummariesFuture<'a> {
        Box::pin(async move {
            PostgresHistoryStore::reusable_history_summaries(
                self,
                chat_id,
                thread_id,
                scope,
                range_start_at,
                range_end_at,
                reset_at,
            )
            .await
            .map_err(|source| HistorySummaryInputError::Storage { source })
        })
    }
}

impl HistorySummarySaver for PostgresHistoryStore {
    fn save_history_summary<'a>(
        &'a self,
        input: &'a SummaryInput,
        doc: &'a SummaryDocument,
    ) -> HistorySummarySaveFuture<'a> {
        Box::pin(async move {
            PostgresHistoryStore::save_summary(self, input, doc)
                .await
                .map_err(|source| HistorySummaryServiceError::Save { source })
        })
    }
}

impl<T> HistorySummaryGenerator for openplotva_llm::aifarm::AifarmHistorySummaryGenerator<T>
where
    T: openplotva_llm::aifarm::AifarmHttpTransport + Clone + 'static,
{
    fn generate_history_stage<'a>(
        &'a self,
        _input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a> {
        Box::pin(async move {
            let mut ignore_status = |_status: openplotva_llm::aifarm::StatusUpdate| {};
            let text = self
                .complete_stage(stage, payload, &mut ignore_status)
                .await
                .map_err(|err| HistorySummaryServiceError::Generate {
                    message: err.to_string(),
                })?;
            Ok(HistoryStageReply {
                text,
                model: self.model().to_owned(),
            })
        })
    }
}

impl<T> HistorySummaryGenerator for openplotva_llm::gemini::GeminiHistorySummaryGenerator<T>
where
    T: openplotva_llm::aifarm::AifarmHttpTransport + 'static,
{
    fn generate_history_stage<'a>(
        &'a self,
        _input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a> {
        Box::pin(async move {
            let text = self.complete_stage(stage, payload).await.map_err(|err| {
                HistorySummaryServiceError::Generate {
                    message: err.to_string(),
                }
            })?;
            Ok(HistoryStageReply {
                text,
                model: self.model().to_owned(),
            })
        })
    }
}

impl<T> HistorySummaryGenerator for GenkitOpenAiCompatibleHistorySummaryGenerator<T>
where
    T: openplotva_llm::aifarm::AifarmHttpTransport + 'static,
{
    fn generate_history_stage<'a>(
        &'a self,
        _input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a> {
        Box::pin(async move {
            let text = self.complete_stage(stage, payload).await.map_err(|err| {
                HistorySummaryServiceError::Generate {
                    message: err.to_string(),
                }
            })?;
            Ok(HistoryStageReply {
                text,
                model: self.model().to_owned(),
            })
        })
    }
}

impl HistorySummaryGenerator for AppGenkitHistorySummaryGenerator {
    fn generate_history_stage<'a>(
        &'a self,
        input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a> {
        Box::pin(async move {
            match self {
                Self::Gemini(generator) => {
                    generator
                        .generate_history_stage(input, stage, payload)
                        .await
                }
                Self::OpenAiCompatible(generator) => {
                    generator
                        .generate_history_stage(input, stage, payload)
                        .await
                }
            }
        })
    }
}

impl HistorySummaryGenerator for RoutedHistorySummaryGenerator {
    fn generate_history_stage<'a>(
        &'a self,
        input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a> {
        Box::pin(async move {
            let config = self.config.clone();
            let result = self
                .walker
                .run(
                    RoutedRequestContext {
                        workflow_key: "history_summary".to_owned(),
                        chat_id: (input.chat_id != 0).then_some(input.chat_id),
                        user_id: (input.requested_by_user_id != 0)
                            .then_some(input.requested_by_user_id),
                        thread_id: (input.thread_id != 0).then_some(input.thread_id),
                        message_id: (input.last_message_id != 0).then_some(input.last_message_id),
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let config = config.clone();
                        async move {
                            generate_history_stage_with_attempt(
                                &config, attempt, input, stage, payload,
                            )
                            .await
                        }
                    },
                    history_summary_service_retryable_reason,
                )
                .await;
            match result {
                Ok(reply) => Ok(reply),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => {
                    Err(HistorySummaryServiceError::Generate {
                        message: error.to_string(),
                    })
                }
            }
        })
    }
}

async fn generate_history_stage_with_attempt(
    config: &AppConfig,
    attempt: RoutedAttempt,
    input: &SummaryInput,
    stage: HistoryStage,
    payload: &str,
) -> Result<HistoryStageReply, HistorySummaryServiceError> {
    if routed_attempt_is_genkit(&attempt) {
        let model = genkit_model_for_attempt(&attempt);
        let generator = genkit_history_summary_generator_from_app_config(config, Some(&model))
            .map_err(|error| HistorySummaryServiceError::Generate {
                message: error.to_string(),
            })?;
        return generator
            .generate_history_stage(input, stage, payload)
            .await;
    }
    let generator =
        AifarmHistorySummaryGenerator::new(aifarm_history_config_for_attempt(config, &attempt));
    generator
        .generate_history_stage(input, stage, payload)
        .await
}

fn routed_attempt_is_genkit(attempt: &RoutedAttempt) -> bool {
    attempt.provider_name.eq_ignore_ascii_case("genkit")
        || attempt.provider_name.eq_ignore_ascii_case("gemini")
        || attempt.provider_name.eq_ignore_ascii_case("openrouter")
}

fn genkit_model_for_attempt(attempt: &RoutedAttempt) -> String {
    if attempt.provider_name.eq_ignore_ascii_case("openrouter")
        && !attempt
            .model_name
            .get(..OPENROUTER_MODEL_PREFIX.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(OPENROUTER_MODEL_PREFIX))
    {
        format!("{OPENROUTER_MODEL_PREFIX}{}", attempt.model_name.trim())
    } else {
        attempt.model_name.clone()
    }
}

fn aifarm_history_config_for_attempt(
    config: &AppConfig,
    attempt: &RoutedAttempt,
) -> AifarmHistorySummaryConfig {
    let mut cfg = aifarm_history_summary_config_from_app_config(config);
    cfg.model = attempt.model_name.clone();
    cfg.client.default_model = attempt.model_name.clone();
    if let Some(endpoint) = routed_attempt_endpoint(attempt) {
        if attempt.discovery_service_name.is_some() || attempt.discovery_endpoint_name.is_some() {
            cfg.client.base_url = endpoint;
        } else {
            cfg.client.direct_url =
                openplotva_llm::aifarm::normalize_chat_completions_url(&endpoint);
            if attempt
                .provider_name
                .eq_ignore_ascii_case(crate::dialog_runtime::VRAM_CLOUD_PROVIDER_NAME)
            {
                cfg.client.api_key = config.llm.dialog.aifarm_pool_api_key.clone();
            }
        }
    }
    if let Some(service) = attempt.discovery_service_name.as_deref() {
        cfg.client.service_name = service.to_owned();
    }
    cfg.client.runtime_hint = attempt.provider_runtime_hint.clone().unwrap_or_default();
    cfg.client.supports_message_name = attempt.supports_message_name();
    if let Some(endpoint) = attempt.discovery_endpoint_name.as_deref() {
        cfg.client.endpoint_name = endpoint.to_owned();
    }
    if let Some(max_tokens) = attempt.overrides.max_tokens
        && max_tokens > 0
    {
        cfg.max_output_tokens = max_tokens;
    }
    if let Some(temperature) = attempt.overrides.temperature {
        cfg.temperature = Some(temperature);
    }
    if let Some(enable_thinking) = attempt
        .overrides
        .extra
        .get("enable_thinking")
        .and_then(serde_json::Value::as_bool)
    {
        cfg.enable_thinking = Some(enable_thinking);
    }
    if let Some(include_reasoning) = attempt
        .overrides
        .extra
        .get("include_reasoning")
        .and_then(serde_json::Value::as_bool)
    {
        cfg.include_reasoning = Some(include_reasoning);
    }
    cfg.with_defaults()
}

fn routed_attempt_endpoint(attempt: &RoutedAttempt) -> Option<String> {
    attempt
        .model_base_url
        .as_deref()
        .or(attempt.provider_endpoint.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn history_summary_service_retryable_reason(
    error: &HistorySummaryServiceError,
) -> Option<FailureReason> {
    match error {
        HistorySummaryServiceError::Generate { message } => retryable_reason_from_message(message),
        _ => None,
    }
}

impl HistorySummaryGenerator for RuntimeHistorySummaryGenerator {
    fn generate_history_stage<'a>(
        &'a self,
        input: &'a SummaryInput,
        stage: HistoryStage,
        payload: &'a str,
    ) -> HistoryStageFuture<'a> {
        match self {
            Self::Aifarm {
                primary,
                primary_model,
                fallback,
            } => Box::pin(async move {
                let mut ignore_status = |_status: openplotva_llm::aifarm::StatusUpdate| {};
                match primary
                    .complete_stage(stage, payload, &mut ignore_status)
                    .await
                {
                    Ok(text) => Ok(HistoryStageReply {
                        text,
                        model: primary_model.trim().to_owned(),
                    }),
                    Err(err) => {
                        if !aifarm_history_summary_error_retryable(&err) {
                            return Err(HistorySummaryServiceError::Generate {
                                message: err.to_string(),
                            });
                        }
                        let Some(fallback) = fallback.as_ref() else {
                            return Err(HistorySummaryServiceError::Generate {
                                message: err.to_string(),
                            });
                        };
                        match fallback.generate_history_stage(input, stage, payload).await {
                            Ok(mut reply) => {
                                reply.model = primary_model.trim().to_owned();
                                Ok(reply)
                            }
                            Err(fallback_err) => Err(HistorySummaryServiceError::Generate {
                                message: format!(
                                    "aifarm chat history summary failed: {err}; genkit fallback failed: {fallback_err}"
                                ),
                            }),
                        }
                    }
                }
            }),
            Self::Genkit(generator) => generator.generate_history_stage(input, stage, payload),
        }
    }
}

fn aifarm_history_summary_error_retryable(err: &AifarmHistorySummaryError) -> bool {
    let retry_reason = retryable_reason(err).map(|reason| reason.as_str());
    history_summary_generate_error_retryable(&err.to_string(), false, false, retry_reason)
}

fn go_zero_time() -> OffsetDateTime {
    let date = time::Date::from_calendar_date(1, time::Month::January, 1)
        .expect("year 1 January 1 is representable");
    time::PrimitiveDateTime::new(date, time::Time::MIDNIGHT).assume_utc()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use openplotva_dialog::ToolContext;
    use openplotva_history::{
        StoredSummary, SummaryMessageEntry, SummaryWindow, prepare_stored_summary,
    };
    use time::format_description::well_known::Rfc3339;

    use super::*;
    use openplotva_llm::gemini::MODEL_GEMINI_FLASH_LITE;

    #[derive(Clone, Debug, PartialEq)]
    struct PayloadCall {
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
    }

    #[derive(Clone, Debug, PartialEq)]
    struct ReusableCall {
        chat_id: i64,
        thread_id: i32,
        scope: SummaryScope,
        range_start_at: OffsetDateTime,
        range_end_at: OffsetDateTime,
        reset_at: OffsetDateTime,
    }

    #[derive(Default)]
    struct FakeHistorySummaryStore {
        chat_reset_at: Mutex<Option<OffsetDateTime>>,
        thread_reset_at: Mutex<Option<OffsetDateTime>>,
        payloads: Mutex<Vec<Vec<u8>>>,
        reusable: Mutex<Vec<StoredSummary>>,
        saved: Mutex<Vec<(SummaryInput, SummaryDocument)>>,
        payload_calls: Mutex<Vec<PayloadCall>>,
        reusable_calls: Mutex<Vec<ReusableCall>>,
    }

    impl HistorySummaryInputStore for FakeHistorySummaryStore {
        fn history_reset_at<'a>(&'a self, _chat_id: i64, thread_id: i32) -> HistoryResetFuture<'a> {
            Box::pin(async move {
                if thread_id == 0 {
                    Ok(*self.chat_reset_at.lock().expect("chat reset lock"))
                } else {
                    Ok(*self.thread_reset_at.lock().expect("thread reset lock"))
                }
            })
        }

        fn summary_entry_payloads<'a>(
            &'a self,
            chat_id: i64,
            thread_id: i32,
            scope: SummaryScope,
            range_start_at: OffsetDateTime,
            range_end_at: OffsetDateTime,
        ) -> HistorySummaryPayloadFuture<'a> {
            Box::pin(async move {
                self.payload_calls
                    .lock()
                    .expect("payload calls lock")
                    .push(PayloadCall {
                        chat_id,
                        thread_id,
                        scope,
                        range_start_at,
                        range_end_at,
                    });
                Ok(self.payloads.lock().expect("payloads lock").clone())
            })
        }

        fn reusable_history_summaries<'a>(
            &'a self,
            chat_id: i64,
            thread_id: i32,
            scope: SummaryScope,
            range_start_at: OffsetDateTime,
            range_end_at: OffsetDateTime,
            reset_at: OffsetDateTime,
        ) -> ReusableHistorySummariesFuture<'a> {
            Box::pin(async move {
                self.reusable_calls
                    .lock()
                    .expect("reusable calls lock")
                    .push(ReusableCall {
                        chat_id,
                        thread_id,
                        scope,
                        range_start_at,
                        range_end_at,
                        reset_at,
                    });
                Ok(self.reusable.lock().expect("reusable lock").clone())
            })
        }
    }

    impl HistorySummarySaver for FakeHistorySummaryStore {
        fn save_history_summary<'a>(
            &'a self,
            input: &'a SummaryInput,
            doc: &'a SummaryDocument,
        ) -> HistorySummarySaveFuture<'a> {
            Box::pin(async move {
                self.saved
                    .lock()
                    .expect("saved lock")
                    .push((input.clone(), doc.clone()));
                let mut stored = prepare_stored_summary(input, doc)
                    .map_err(|err| HistorySummaryServiceError::Generate {
                        message: err.to_string(),
                    })?
                    .stored;
                stored.id = 700;
                stored.created_at =
                    OffsetDateTime::parse("2026-05-20T12:00:00Z", &Rfc3339).expect("created");
                Ok(stored)
            })
        }
    }

    #[derive(Default)]
    struct FakeHistorySummaryGenerator {
        inputs: Mutex<Vec<SummaryInput>>,
        stages: Mutex<Vec<HistoryStage>>,
        garbled_events_answers: Mutex<usize>,
        garbled_recap_answers: Mutex<usize>,
    }

    impl HistorySummaryGenerator for FakeHistorySummaryGenerator {
        fn generate_history_stage<'a>(
            &'a self,
            input: &'a SummaryInput,
            stage: HistoryStage,
            _payload: &'a str,
        ) -> HistoryStageFuture<'a> {
            Box::pin(async move {
                self.stages.lock().expect("stages").push(stage);
                let text = match stage {
                    HistoryStage::Events => {
                        self.inputs
                            .lock()
                            .expect("generator inputs")
                            .push(input.clone());
                        let mut garbled = self.garbled_events_answers.lock().expect("garbled");
                        if *garbled > 0 {
                            *garbled -= 1;
                            return Ok(HistoryStageReply {
                                text: "Событий нет".to_owned(),
                                model: "test-model".to_owned(),
                            });
                        }
                        serde_json::json!({
                            "events": [{
                                "source_ids": ["1"],
                                "title": "thread recap",
                                "description": "",
                                "actors": [],
                                "confidence": 0.9
                            }],
                            "nothing_notable": false
                        })
                        .to_string()
                    }
                    HistoryStage::Recap => {
                        let mut garbled = self.garbled_recap_answers.lock().expect("garbled");
                        if *garbled > 0 {
                            *garbled -= 1;
                            return Ok(HistoryStageReply {
                                text: "{\"recap\": \"\"}".to_owned(),
                                model: "test-model".to_owned(),
                            });
                        }
                        serde_json::json!({
                            "recap": "done",
                            "actors": [],
                            "open_questions": [],
                            "source_style": "хроника",
                            "quality_score": 0.5,
                            "quality_notes": ""
                        })
                        .to_string()
                    }
                };
                Ok(HistoryStageReply {
                    text,
                    model: "test-model".to_owned(),
                })
            })
        }
    }

    #[tokio::test]
    async fn build_history_summary_input_uses_reset_filter_truncation_and_reuse() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let store = FakeHistorySummaryStore::default();
        *store.chat_reset_at.lock().expect("chat reset") = Some(base + time::Duration::minutes(5));
        *store.thread_reset_at.lock().expect("thread reset") =
            Some(base + time::Duration::minutes(15));
        *store.payloads.lock().expect("payloads") = vec![
            entry_payload(1, base + time::Duration::minutes(16), "one"),
            entry_payload(2, base + time::Duration::minutes(17), "two"),
            entry_payload(3, base + time::Duration::minutes(18), "three"),
        ];
        *store.reusable.lock().expect("reusable") = vec![StoredSummary {
            id: 90,
            range_start_at: base + time::Duration::minutes(17),
            range_end_at: base + time::Duration::minutes(17),
            first_message_id: 2,
            last_message_id: 2,
            first_entry_id: "msg:2".to_owned(),
            last_entry_id: "msg:2".to_owned(),
            raw_message_count: 1,
            covered_message_count: 4,
            summary_html: "two summary".to_owned(),
            ..StoredSummary::default()
        }];

        let input = build_history_summary_input(
            &store,
            SummaryRequest {
                chat_id: 100,
                thread_id: 7,
                scope: SummaryScope::Thread,
                window: SummaryWindow::Messages,
                message_count: 2,
                requested_by_user_id: 55,
                now: Some(base + time::Duration::hours(24)),
                ..SummaryRequest::default()
            },
        )
        .await
        .expect("summary input");

        assert_eq!(input.first_message_id, 2);
        assert_eq!(input.last_message_id, 3);
        assert_eq!(input.raw_message_count, 2);
        assert_eq!(input.covered_message_count, 5);
        assert_eq!(input.reused_summary_count, 1);
        assert_eq!(input.source_summary_ids, vec![90]);
        assert_eq!(
            input
                .items
                .iter()
                .map(|item| (item.kind.as_str(), item.message_id, item.summary_id))
                .collect::<Vec<_>>(),
            vec![("summary", 0, 90), ("message", 3, 0)]
        );
        assert!(!input.input_hash.is_empty());
        assert!(input.input_token_estimate > 0);

        assert_eq!(
            store
                .payload_calls
                .lock()
                .expect("payload calls")
                .as_slice(),
            &[PayloadCall {
                chat_id: 100,
                thread_id: 7,
                scope: SummaryScope::Thread,
                range_start_at: base + time::Duration::minutes(15),
                range_end_at: base + time::Duration::hours(24),
            }]
        );
        assert_eq!(
            store
                .reusable_calls
                .lock()
                .expect("reusable calls")
                .as_slice(),
            &[ReusableCall {
                chat_id: 100,
                thread_id: 7,
                scope: SummaryScope::Thread,
                range_start_at: base + time::Duration::minutes(17),
                range_end_at: base + time::Duration::minutes(18),
                reset_at: base + time::Duration::minutes(15),
            }]
        );
    }

    #[tokio::test]
    async fn chat_history_summary_service_generates_saves_and_maps_result() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let store = Arc::new(FakeHistorySummaryStore::default());
        *store.payloads.lock().expect("payloads") = vec![entry_payload(
            10,
            base + time::Duration::minutes(1),
            "recap me",
        )];
        let generator = Arc::new(FakeHistorySummaryGenerator::default());
        let service = ChatHistorySummaryService::new(store.clone(), generator.clone());

        let result = service
            .summarize(HistorySummaryRequest {
                context: ToolContext {
                    chat_id: 100,
                    thread_id: Some(9),
                    user_id: 55,
                    ..ToolContext::default()
                },
                window: "messages".to_owned(),
                message_count: 1,
                ..HistorySummaryRequest::default()
            })
            .await
            .expect("summary result");

        assert_eq!(result.summary_id, 700);
        assert_eq!(result.chat_id, 100);
        assert_eq!(result.thread_id, 9);
        assert_eq!(result.scope, "thread");
        assert_eq!(result.raw_message_count, 1);
        assert_eq!(result.covered_message_count, 1);
        assert_eq!(result.reused_summary_count, 0);
        assert_eq!(result.summary_html, "• thread recap\n\ndone");
        assert_eq!(result.model, "test-model");
        assert_eq!(result.summary_json.events, vec!["thread recap"]);

        assert_eq!(generator.inputs.lock().expect("generator inputs").len(), 1);
        assert_eq!(store.saved.lock().expect("saved").len(), 1);
    }

    #[tokio::test]
    async fn chat_history_summary_service_pre_summarizes_edge_gap_like_go() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let store = Arc::new(FakeHistorySummaryStore::default());
        *store.payloads.lock().expect("payloads") = vec![
            entry_payload(1, base + time::Duration::minutes(1), "covered"),
            entry_payload(2, base + time::Duration::minutes(2), "edge a"),
            entry_payload(3, base + time::Duration::minutes(3), "edge b"),
            entry_payload(4, base + time::Duration::minutes(4), "edge c"),
        ];
        *store.reusable.lock().expect("reusable") = vec![StoredSummary {
            id: 90,
            chat_id: 100,
            thread_id: 9,
            scope: SummaryScope::Thread,
            range_start_at: base + time::Duration::minutes(1),
            range_end_at: base + time::Duration::minutes(1),
            first_message_id: 1,
            last_message_id: 1,
            first_entry_id: "msg:1".to_owned(),
            last_entry_id: "msg:1".to_owned(),
            raw_message_count: 1,
            covered_message_count: 4,
            summary_html: "covered summary".to_owned(),
            ..StoredSummary::default()
        }];
        let generator = Arc::new(FakeHistorySummaryGenerator::default());
        let service = ChatHistorySummaryService::new_with_options(
            store.clone(),
            generator.clone(),
            HistorySummaryServiceOptions::default().with_edge_min_raw_messages(2),
        );

        let result = service
            .summarize(HistorySummaryRequest {
                context: ToolContext {
                    chat_id: 100,
                    thread_id: Some(9),
                    user_id: 55,
                    ..ToolContext::default()
                },
                window: "messages".to_owned(),
                message_count: 4,
                ..HistorySummaryRequest::default()
            })
            .await
            .expect("summary result");

        let inputs = generator.inputs.lock().expect("generator inputs");
        assert_eq!(inputs.len(), 2);
        assert_eq!(
            inputs[0]
                .items
                .iter()
                .map(|item| (item.kind.as_str(), item.message_id))
                .collect::<Vec<_>>(),
            vec![("message", 2), ("message", 3), ("message", 4)]
        );
        assert_eq!(
            inputs[1]
                .items
                .iter()
                .map(|item| (item.kind.as_str(), item.summary_id))
                .collect::<Vec<_>>(),
            vec![("summary", 90), ("summary", 700)]
        );
        drop(inputs);

        assert_eq!(store.saved.lock().expect("saved").len(), 2);
        assert_eq!(result.reused_summary_count, 2);
        assert_eq!(result.source_summary_ids, vec![90, 700]);
    }

    #[tokio::test]
    async fn chat_history_summary_keeps_every_message_and_chunks_long_windows() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let store = Arc::new(FakeHistorySummaryStore::default());
        *store.payloads.lock().expect("payloads") = (1..=40)
            .map(|index| {
                entry_payload(
                    index,
                    base + time::Duration::minutes(i64::from(index)),
                    "long visible message about plans for the weekend and the new project",
                )
            })
            .collect();
        let generator = Arc::new(FakeHistorySummaryGenerator::default());
        let service = ChatHistorySummaryService::new_with_options(
            store,
            generator.clone(),
            HistorySummaryServiceOptions::default().with_max_input_tokens(400),
        );

        let result = service
            .summarize(HistorySummaryRequest {
                context: ToolContext {
                    chat_id: 100,
                    user_id: 55,
                    ..ToolContext::default()
                },
                window: "messages".to_owned(),
                message_count: 40,
                ..HistorySummaryRequest::default()
            })
            .await
            .expect("summary result");

        let stages = generator.stages.lock().expect("stages");
        let event_calls = stages
            .iter()
            .filter(|stage| **stage == HistoryStage::Events)
            .count();
        assert!(event_calls > 1, "the window is split into chunks");
        assert_eq!(stages.last(), Some(&HistoryStage::Recap));
        let inputs = generator.inputs.lock().expect("generator inputs");
        assert!(inputs.iter().all(|input| input.omitted_message_count == 0));
        assert_eq!(inputs[0].items.len(), 40);
        assert_eq!(result.raw_message_count, 40);
    }

    #[tokio::test]
    async fn unreadable_stage_answers_are_asked_once_more() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let store = Arc::new(FakeHistorySummaryStore::default());
        *store.payloads.lock().expect("payloads") = (1..=10)
            .map(|index| {
                entry_payload(
                    index,
                    base + time::Duration::minutes(i64::from(index)),
                    "обсуждаем переезд офиса на следующей неделе",
                )
            })
            .collect();
        let generator = Arc::new(FakeHistorySummaryGenerator::default());
        *generator.garbled_events_answers.lock().expect("garbled") = 1;
        *generator.garbled_recap_answers.lock().expect("garbled") = 1;
        let service = ChatHistorySummaryService::new(store, generator.clone());

        let result = service
            .summarize(HistorySummaryRequest {
                context: ToolContext {
                    chat_id: 100,
                    user_id: 55,
                    ..ToolContext::default()
                },
                window: "messages".to_owned(),
                message_count: 10,
                ..HistorySummaryRequest::default()
            })
            .await
            .expect("summary result");

        assert_eq!(
            *generator.stages.lock().expect("stages"),
            vec![
                HistoryStage::Events,
                HistoryStage::Events,
                HistoryStage::Recap,
                HistoryStage::Recap
            ]
        );
        assert_eq!(result.summary_json.events, vec!["thread recap"]);
        assert_eq!(result.summary_json.recap, "done");
    }

    #[tokio::test]
    async fn quiet_window_is_summarized_without_a_model_call() {
        let base = OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base");
        let store = Arc::new(FakeHistorySummaryStore::default());
        *store.payloads.lock().expect("payloads") = vec![
            entry_payload(1, base + time::Duration::minutes(1), "ок"),
            entry_payload(2, base + time::Duration::minutes(2), "+"),
        ];
        let generator = Arc::new(FakeHistorySummaryGenerator::default());
        let service = ChatHistorySummaryService::new(store, generator.clone());

        let result = service
            .summarize(HistorySummaryRequest {
                context: ToolContext {
                    chat_id: 100,
                    user_id: 55,
                    ..ToolContext::default()
                },
                window: "messages".to_owned(),
                message_count: 2,
                ..HistorySummaryRequest::default()
            })
            .await
            .expect("summary result");

        assert!(generator.stages.lock().expect("stages").is_empty());
        assert!(result.summary_json.events.is_empty());
        assert_eq!(
            result.summary_json.recap,
            openplotva_history::stages::QUIET_RECAP
        );
    }

    #[test]
    fn aifarm_history_summary_config_maps_go_memory_env_and_genkit_overrides() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            discovery_base_url: Some("http://discovery.test".to_owned()),
            genkit_history_summary_model: Some("summary-model".to_owned()),
            genkit_history_summary_timeout_seconds: Some("333".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            memory_max_input_tokens: Some("4444".to_owned()),
            memory_aifarm_service_name: Some("summary-service".to_owned()),
            memory_aifarm_endpoint_name: Some("summary-endpoint".to_owned()),
            memory_aifarm_request_timeout_seconds: Some("66".to_owned()),
            memory_aifarm_poll_interval_seconds: Some("3".to_owned()),
            memory_aifarm_task_timeout_seconds: Some("77".to_owned()),
            memory_aifarm_capacity_wait_seconds: Some("88".to_owned()),
            memory_aifarm_capacity_poll_seconds: Some("4".to_owned()),
            memory_aifarm_temperature: Some("0.35".to_owned()),
            memory_aifarm_enable_thinking: Some("true".to_owned()),
            dialog_aifarm_pool_models: Some("pool-a".to_owned()),
            dialog_aifarm_pool_base_urls: Some("http://pool-a.test".to_owned()),
            dialog_aifarm_pool_api_key: Some("pool-token".to_owned()),
            dialog_aifarm_pool_primary_capacity_wait_ms: Some("250".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = aifarm_history_summary_config_from_app_config(&config);

        assert_eq!(cfg.client.base_url, "http://discovery.test");
        assert_eq!(cfg.client.service_name, "summary-service");
        assert_eq!(cfg.client.endpoint_name, "summary-endpoint");
        assert_eq!(cfg.client.default_model, "summary-model");
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(66));
        assert_eq!(cfg.client.poll_interval, Duration::from_secs(3));
        assert_eq!(cfg.client.task_timeout, Duration::from_secs(77));
        assert_eq!(cfg.client.capacity_wait, Duration::from_secs(88));
        assert_eq!(cfg.client.capacity_poll_interval, Duration::from_secs(4));
        assert_eq!(
            cfg.client.workload,
            openplotva_llm::aifarm::AIFARM_WORKLOAD_SUMMARY
        );
        assert_eq!(cfg.model, "summary-model");
        assert_eq!(cfg.max_output_tokens, 4096);
        assert_eq!(cfg.temperature, Some(0.35));
        assert_eq!(cfg.enable_thinking, Some(false));
        assert_eq!(cfg.include_reasoning, Some(false));

        let options = HistorySummaryServiceOptions::default()
            .with_max_input_tokens(config.memory.max_input_tokens)
            .with_timeout_seconds(config.llm.history_summary.timeout_seconds);
        assert_eq!(options.max_input_tokens, 4444);
        assert_eq!(options.timeout, Duration::from_secs(333));
    }

    #[test]
    fn aifarm_history_summary_config_uses_memory_model_for_aifarm_default() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            genkit_history_summary_model: Some(" ".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = aifarm_history_summary_config_from_app_config(&config);

        assert_eq!(cfg.model, "memory-model");
        assert_eq!(cfg.client.default_model, "memory-model");
    }

    #[test]
    fn gemini_history_summary_config_maps_go_genkit_branch() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some(" gemini-key ".to_owned()),
            genkit_history_summary_provider: Some(" genkit ".to_owned()),
            genkit_history_summary_model: Some(" summary-model ".to_owned()),
            genkit_history_summary_timeout_seconds: Some("333".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = gemini_history_summary_config_from_app_config(&config);

        assert_eq!(cfg.api_key, "gemini-key");
        assert_eq!(cfg.model, "summary-model");
        assert_eq!(cfg.request_timeout, Duration::from_secs(333));
        assert_eq!(cfg.max_output_tokens, 1024);
        assert_eq!(cfg.temperature, 0.45);
        assert_eq!(cfg.top_p, 0.9);
    }

    #[test]
    fn gemini_history_summary_config_uses_runtime_fallback_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("gemini-key".to_owned()),
            genkit_history_summary_provider: Some("genkit".to_owned()),
            genkit_history_summary_model: Some(" ".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = gemini_history_summary_config_from_app_config(&config);

        assert_eq!(cfg.model, MODEL_GEMINI_FLASH_LITE);
    }

    #[test]
    fn history_summary_generator_builds_genkit_openai_compatible_default_model() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            genkit_history_summary_provider: Some("genkit".to_owned()),
            genkit_history_summary_model: Some(" ".to_owned()),
            genkit_default_model: Some(" openrouter/summary-model ".to_owned()),
            openrouter_key: Some(" openrouter-key ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let generator = history_summary_generator_from_app_config(&config).expect("generator");

        let RuntimeHistorySummaryGenerator::Genkit(generator) = generator else {
            panic!("expected genkit generator");
        };
        assert!(matches!(
            *generator,
            AppGenkitHistorySummaryGenerator::OpenAiCompatible(_)
        ));
    }

    #[test]
    fn aifarm_history_summary_generator_attaches_gemini_fallback_when_key_resolves() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some("gemini-key".to_owned()),
            genkit_history_summary_provider: Some("aifarm".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let generator = history_summary_generator_from_app_config(&config).expect("generator");

        let RuntimeHistorySummaryGenerator::Aifarm {
            primary_model,
            fallback,
            ..
        } = generator
        else {
            panic!("expected aifarm generator");
        };
        assert_eq!(primary_model, "memory-model");
        assert!(fallback.is_some());
    }

    #[test]
    fn aifarm_history_summary_generator_skips_gemini_fallback_without_key() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig::default()).expect("config");

        let generator = history_summary_generator_from_app_config(&config).expect("generator");

        let RuntimeHistorySummaryGenerator::Aifarm { fallback, .. } = generator else {
            panic!("expected aifarm generator");
        };
        assert!(fallback.is_none());
    }

    #[test]
    fn summary_request_from_dialog_tool_matches_go_scope_and_since_parse() {
        let got = summary_request_from_dialog_tool(&HistorySummaryRequest {
            context: ToolContext {
                chat_id: 100,
                thread_id: Some(7),
                user_id: 55,
                ..ToolContext::default()
            },
            scope: "chat".to_owned(),
            window: "since".to_owned(),
            since: "2026-05-20T10:00:00+02:00".to_owned(),
            ..HistorySummaryRequest::default()
        })
        .expect("request");

        assert_eq!(got.chat_id, 100);
        assert_eq!(got.thread_id, 0);
        assert_eq!(got.requested_by_user_id, 55);
        assert_eq!(got.scope, SummaryScope::Chat);
        assert_eq!(got.window, SummaryWindow::Since);
        assert_eq!(
            got.since,
            Some(OffsetDateTime::parse("2026-05-20T08:00:00Z", &Rfc3339).expect("utc"))
        );

        let err = summary_request_from_dialog_tool(&HistorySummaryRequest {
            since: "not-time".to_owned(),
            ..HistorySummaryRequest::default()
        })
        .expect_err("invalid since");
        assert!(matches!(err, HistorySummaryServiceError::Since(_)));
    }

    #[tokio::test]
    async fn build_history_summary_input_reports_go_no_messages_error() {
        let store = FakeHistorySummaryStore::default();
        *store.payloads.lock().expect("payloads") = vec![entry_payload(
            1,
            OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("base"),
            " ",
        )];

        let err = build_history_summary_input(
            &store,
            SummaryRequest {
                chat_id: 100,
                now: Some(OffsetDateTime::parse("2026-05-20T10:00:00Z", &Rfc3339).expect("now")),
                ..SummaryRequest::default()
            },
        )
        .await
        .expect_err("no messages");

        assert!(matches!(err, HistorySummaryInputError::NoMessages));
        assert_eq!(
            err.to_string(),
            "no chat history messages found for requested range"
        );
    }

    fn entry_payload(message_id: i32, timestamp: OffsetDateTime, text: &str) -> Vec<u8> {
        let entry = SummaryMessageEntry {
            entry_id: format!("msg:{message_id}"),
            message_id,
            timestamp: Some(timestamp),
            date: timestamp.unix_timestamp(),
            text: text.to_owned(),
            ..SummaryMessageEntry::default()
        };
        serde_json::to_vec(&serde_json::json!({
            "entry_id": entry.entry_id,
            "message_id": entry.message_id,
            "timestamp": entry.timestamp.expect("timestamp").format(&Rfc3339).expect("timestamp format"),
            "date": entry.date,
            "text": entry.text,
        }))
        .expect("entry payload")
    }
}
