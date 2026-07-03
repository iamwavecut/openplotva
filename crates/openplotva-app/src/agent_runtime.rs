//! Composition-root wiring for the agent-loop engine: a registry of named
//! single-completion LLM clients, the `Reasoner`/`AgentTools` adapters over the
//! real AIFarm client and dialog tool box, and the search-agent profile.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openplotva_agent::{
    AgentBudgets, AgentError, AgentMessage, AgentOrigin, AgentOutcome, AgentProfile, AgentRole,
    AgentState, AgentTools, Reasoner, ReasonerCall, ReasonerFuture, ReasonerReply, StepProgress,
    ToolDispatchFuture, advance_one_step,
};
use openplotva_config::AppConfig;
use openplotva_dialog::{
    NativeToolCall, STEP_CRAWL_URL, STEP_HISTORY_SEARCH, STEP_MEMORY_SEARCH, STEP_WEB_SEARCH,
    TOOL_RESULT_STATUS_OK, ToolContext, ToolResult, ToolStep,
};
use openplotva_history::{SummaryMessageEntry, decode_summary_message_entry_payloads};
use openplotva_llm::{
    aifarm::{
        AIFARM_WORKLOAD_DIALOG, AifarmClientConfig, AifarmHttpClient, ChatCompletionRequest,
        ChatMessage, CompletionResult, DISCOVERY_PRIORITY_INTERACTIVE, StatusUpdate,
        normalize_chat_completions_url,
    },
    retry::{FailureReason, retryable_reason_from_message},
};
use openplotva_memory::{RetrievalRequest, RetrievalScope, RetrievedMemory};
use openplotva_storage::{PostgresHistoryStore, PostgresMemoryStore};
use serde_json::{Value, json};
use time::{Duration as TimeDuration, OffsetDateTime};

use openplotva_taskman::{MUSIC_VIP_QUEUE_NAME, MusicGenJobParams};

use crate::dialog_tools::{CrawlUrlFuture, UrlCrawler, WebSearchFuture, WebSearchProvider};
use crate::image_jobs::{
    ImageGenerationFuture, ImageGenerationProgressSink, ImageGenerationRequest, ImageGenerator,
};
use crate::media::{agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config};
use crate::music_jobs::{SongMaterial, SongMaterialFuture, SongMaterialProvider};
use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
};

const AGENTIC_SONG_WORKFLOW: &str = "agentic_song";
const AGENTIC_IMAGE_WORKFLOW: &str = "agentic_image";

/// The implicit provider name that always maps to the primary dialog config.
pub const CONVERSATIONAL_PROVIDER: &str = "conversational";

/// Default Discovery service the auto-registered `qwen-reasoner` provider targets.
pub const DEFAULT_QWEN_SERVICE_NAME: &str = "llm-openai-qwen27b-gguf";
/// Default model id sent to the qwen llama.cpp router. The router exposes this
/// model under the `[model.qwen3.6-27b-moq]` section in `llamacpp.ini`; override
/// via `LLM_PROVIDERS_MODELS`. (Underlying GGUF: Qwen3.6-27B, kaitchup MoQ-4.75 —
/// higher bits-per-weight than the prior 35B-A3B Q3_K_XL at a comparable file size.)
pub const DEFAULT_QWEN_MODEL: &str = "qwen3.6-27b-moq";

/// System prompt for the song-writing agent.
pub const SONG_SYSTEM_PROMPT: &str = include_str!("../../../prompts/agentic/song_system.prompt");
/// System prompt for the image-prompt agent.
pub const IMAGE_SYSTEM_PROMPT: &str = include_str!("../../../prompts/agentic/image_system.prompt");

/// A single-completion LLM client plus the request defaults for one provider.
#[derive(Clone)]
pub struct AgentProviderClient {
    pub client: AifarmHttpClient,
    pub model: String,
    pub include_reasoning: Option<bool>,
    pub enable_thinking: Option<bool>,
    pub temperature: Option<f64>,
    pub max_tokens: i32,
    routed: Option<RoutedAgentExecution>,
}

#[derive(Clone)]
struct RoutedAgentExecution {
    walker: RoutedAttemptWalker,
    config: AppConfig,
}

/// Name-keyed registry of providers selectable per agent profile.
#[derive(Clone, Default)]
pub struct AgentProviderRegistry {
    by_name: HashMap<String, Arc<AgentProviderClient>>,
}

impl AgentProviderRegistry {
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<AgentProviderClient>> {
        self.by_name.get(&normalize_name(name)).cloned()
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(&normalize_name(name))
    }
}

fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

/// Build the provider registry from config: always the `conversational` entry
/// (primary dialog config) plus one entry per `LLM_PROVIDERS_*` spec.
#[must_use]
pub fn build_agent_provider_registry(config: &AppConfig) -> AgentProviderRegistry {
    let mut by_name = HashMap::new();

    let dialog = aifarm_dialog_config_from_app_config(config);
    by_name.insert(
        CONVERSATIONAL_PROVIDER.to_owned(),
        Arc::new(AgentProviderClient {
            client: AifarmHttpClient::new(dialog.client),
            model: dialog.model.clone(),
            include_reasoning: dialog.include_reasoning,
            enable_thinking: dialog.enable_thinking,
            temperature: dialog.temperature,
            max_tokens: dialog.max_tokens,
            routed: None,
        }),
    );

    for spec in &config.llm.providers {
        let client_config = agent_client_config_from_named_provider(config, spec);
        by_name.insert(
            normalize_name(&spec.name),
            Arc::new(AgentProviderClient {
                client: AifarmHttpClient::new(client_config),
                model: spec.model.clone(),
                include_reasoning: spec.include_reasoning,
                enable_thinking: spec.enable_thinking,
                temperature: spec.temperature,
                max_tokens: spec.max_tokens,
                routed: None,
            }),
        );
    }

    // Auto-register the default qwen reasoner so the search agent works out of the
    // box; an explicit `LLM_PROVIDERS_*` entry of the same name takes precedence.
    let default_reasoner = normalize_name(openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER);
    if let std::collections::hash_map::Entry::Vacant(entry) = by_name.entry(default_reasoner) {
        let spec = qwen_reasoner_named_provider_config(config);
        let client_config = agent_client_config_from_named_provider(config, &spec);
        entry.insert(Arc::new(AgentProviderClient {
            client: AifarmHttpClient::new(client_config),
            model: spec.model.clone(),
            include_reasoning: spec.include_reasoning,
            enable_thinking: spec.enable_thinking,
            temperature: spec.temperature,
            max_tokens: spec.max_tokens,
            routed: None,
        }));
    }

    AgentProviderRegistry { by_name }
}

#[must_use]
pub fn build_routed_agent_provider_registry(
    config: &AppConfig,
    walker: RoutedAttemptWalker,
) -> AgentProviderRegistry {
    let mut registry = build_agent_provider_registry(config);
    let routed = RoutedAgentExecution {
        walker,
        config: config.clone(),
    };
    for provider in registry.by_name.values_mut() {
        Arc::make_mut(provider).routed = Some(routed.clone());
    }
    registry
}

#[must_use]
pub fn qwen_reasoner_named_provider_config(
    config: &AppConfig,
) -> openplotva_config::NamedProviderConfig {
    let default_reasoner = normalize_name(openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER);
    config
        .llm
        .providers
        .iter()
        .find(|spec| normalize_name(&spec.name) == default_reasoner)
        .cloned()
        .unwrap_or_else(|| openplotva_config::NamedProviderConfig {
            name: openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER.to_owned(),
            kind: openplotva_config::DEFAULT_LLM_PROVIDER_KIND.to_owned(),
            discovery_service_name: DEFAULT_QWEN_SERVICE_NAME.to_owned(),
            discovery_endpoint_name: config.llm.dialog.discovery_endpoint_name.clone(),
            model: DEFAULT_QWEN_MODEL.to_owned(),
            base_url: String::new(),
            url: String::new(),
            api_key: String::new(),
            include_reasoning: Some(false),
            enable_thinking: Some(false),
            max_tokens: openplotva_config::DEFAULT_LLM_PROVIDER_MAX_TOKENS,
            temperature: None,
            task_timeout_seconds: openplotva_config::DEFAULT_LLM_PROVIDER_TASK_TIMEOUT_SECONDS,
        })
}

/// `Reasoner` adapter that performs one chat round-trip via the AIFarm client.
pub struct AifarmReasoner {
    provider: Arc<AgentProviderClient>,
    context: RoutedRequestContext,
}

impl AifarmReasoner {
    #[must_use]
    pub fn for_workflow(
        provider: Arc<AgentProviderClient>,
        workflow_key: impl Into<String>,
    ) -> Self {
        Self {
            provider,
            context: RoutedRequestContext {
                workflow_key: workflow_key.into(),
                ..RoutedRequestContext::default()
            },
        }
    }

    #[must_use]
    pub fn with_context(provider: Arc<AgentProviderClient>, context: RoutedRequestContext) -> Self {
        Self { provider, context }
    }
}

impl Reasoner for AifarmReasoner {
    fn complete<'a>(&'a self, call: ReasonerCall) -> ReasonerFuture<'a> {
        Box::pin(async move {
            let request = build_request(&self.provider, &call, true);
            if let Some(routed) = self.provider.routed.clone() {
                let base_provider = Arc::clone(&self.provider);
                let base_request = request;
                let config = routed.config.clone();
                let result = routed
                    .walker
                    .run(
                        self.context.clone(),
                        move |attempt| {
                            let base_provider = Arc::clone(&base_provider);
                            let mut request = base_request.clone();
                            let config = config.clone();
                            async move {
                                let provider = agent_provider_client_for_attempt(
                                    &config,
                                    &base_provider,
                                    &attempt,
                                );
                                apply_agent_attempt_to_request(&mut request, &provider, &attempt);
                                let mut sink = |_status: StatusUpdate| {};
                                let result = provider
                                    .client
                                    .complete(request, &mut sink)
                                    .await
                                    .map_err(|error| AgentError::Reasoner(error.to_string()))?;
                                parse_reply(&result)
                            }
                        },
                        agent_retryable_reason,
                    )
                    .await;
                return match result {
                    Ok(reply) => Ok(reply),
                    Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                    Err(RoutedAttemptRunError::Routing(error)) => {
                        Err(AgentError::Reasoner(error.to_string()))
                    }
                };
            }
            let mut sink = |_status: StatusUpdate| {};
            let result = self
                .provider
                .client
                .complete(request, &mut sink)
                .await
                .map_err(|error| AgentError::Reasoner(error.to_string()))?;
            parse_reply(&result)
        })
    }
}

/// Boxed future returned by the context-gathering searchers.
pub type ContextSearchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, AgentError>> + Send + 'a>>;

/// Searches THIS chat's past messages for relevant context.
pub trait HistorySearcher: Send + Sync {
    fn search<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
        query: String,
    ) -> ContextSearchFuture<'a>;
}

/// Searches long-term memory (facts/episodes) for relevant context.
pub trait MemorySearcher: Send + Sync {
    fn search<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        thread_id: Option<i32>,
        query: String,
    ) -> ContextSearchFuture<'a>;
}

/// `AgentTools` adapter that calls RAW providers directly (Serper, history,
/// memory) — never the conversational dialog tools — so the agent loop is
/// independent of the (possibly agentic) `web_search` tool and cannot recurse.
/// Transport failures become recoverable `ToolResult`s.
pub struct AppAgentTools {
    web_searcher: Arc<dyn WebSearchProvider>,
    url_crawler: Arc<dyn UrlCrawler>,
    history_searcher: Option<Arc<dyn HistorySearcher>>,
    memory_searcher: Option<Arc<dyn MemorySearcher>>,
}

impl AppAgentTools {
    #[must_use]
    pub fn new(web_searcher: Arc<dyn WebSearchProvider>, url_crawler: Arc<dyn UrlCrawler>) -> Self {
        Self {
            web_searcher,
            url_crawler,
            history_searcher: None,
            memory_searcher: None,
        }
    }

    #[must_use]
    pub fn with_history_searcher(mut self, searcher: Arc<dyn HistorySearcher>) -> Self {
        self.history_searcher = Some(searcher);
        self
    }

    #[must_use]
    pub fn with_memory_searcher(mut self, searcher: Arc<dyn MemorySearcher>) -> Self {
        self.memory_searcher = Some(searcher);
        self
    }
}

fn ok_tool_result(message: String, data: Value) -> ToolResult {
    ToolResult {
        status: TOOL_RESULT_STATUS_OK.to_owned(),
        message,
        data: Some(data),
        ..ToolResult::default()
    }
}

impl AgentTools for AppAgentTools {
    fn dispatch<'a>(&'a self, ctx: ToolContext, step: ToolStep) -> ToolDispatchFuture<'a> {
        Box::pin(async move {
            let result = match step.step.as_str() {
                STEP_WEB_SEARCH => {
                    let query = step.query.clone();
                    match self.web_searcher.search(&query).await {
                        Ok(results) => ok_tool_result(results, json!({ "query": query })),
                        Err(error) => ToolResult::failed("web_search_failed", error.to_string()),
                    }
                }
                STEP_CRAWL_URL => {
                    let url = step.url.clone();
                    match self.url_crawler.crawl(&url).await {
                        Ok(content) => ok_tool_result(content, json!({ "url": url })),
                        Err(error) => ToolResult::failed("crawl_url_failed", error.to_string()),
                    }
                }
                STEP_HISTORY_SEARCH => match &self.history_searcher {
                    Some(searcher) => {
                        match searcher
                            .search(ctx.chat_id, ctx.thread_id, step.query.clone())
                            .await
                        {
                            Ok(text) => ok_tool_result(text, json!({ "query": step.query })),
                            Err(error) => {
                                ToolResult::failed("history_search_failed", error.to_string())
                            }
                        }
                    }
                    None => ToolResult::failed(
                        "history_search_unavailable",
                        "history search is not configured",
                    ),
                },
                STEP_MEMORY_SEARCH => match &self.memory_searcher {
                    Some(searcher) => {
                        match searcher
                            .search(ctx.chat_id, ctx.user_id, ctx.thread_id, step.query.clone())
                            .await
                        {
                            Ok(text) => ok_tool_result(text, json!({ "query": step.query })),
                            Err(error) => {
                                ToolResult::failed("memory_search_failed", error.to_string())
                            }
                        }
                    }
                    None => ToolResult::failed(
                        "memory_search_unavailable",
                        "memory search is not configured",
                    ),
                },
                other => ToolResult::failed(
                    "tool_unsupported",
                    format!("agent tool `{other}` is not supported"),
                ),
            };
            Ok(result)
        })
    }
}

/// History searcher backed by `PostgresHistoryStore` (keyword ILIKE search).
pub struct PostgresHistorySearch {
    store: PostgresHistoryStore,
    window_hours: i64,
    limit: i32,
}

impl PostgresHistorySearch {
    #[must_use]
    pub fn new(store: PostgresHistoryStore) -> Self {
        Self {
            store,
            window_hours: 24 * 30,
            limit: 40,
        }
    }
}

impl HistorySearcher for PostgresHistorySearch {
    fn search<'a>(
        &'a self,
        chat_id: i64,
        thread_id: Option<i32>,
        query: String,
    ) -> ContextSearchFuture<'a> {
        Box::pin(async move {
            let cutoff = OffsetDateTime::now_utc() - TimeDuration::hours(self.window_hours);
            let thread_id = thread_id.unwrap_or(0);
            let payloads = if let Some(username) = author_username_from_history_query(&query) {
                match self
                    .store
                    .user_id_by_username(&username)
                    .await
                    .map_err(|error| AgentError::ToolDispatch(error.to_string()))?
                {
                    Some(sender_id) => self
                        .store
                        .search_history_entries_by_sender_id(
                            chat_id, thread_id, sender_id, cutoff, self.limit,
                        )
                        .await
                        .map_err(|error| AgentError::ToolDispatch(error.to_string()))?,
                    None => Vec::new(),
                }
            } else {
                self.store
                    .search_history_entries(chat_id, thread_id, &query, cutoff, self.limit)
                    .await
                    .map_err(|error| AgentError::ToolDispatch(error.to_string()))?
            };
            let entries = decode_summary_message_entry_payloads(&payloads)
                .map_err(|error| AgentError::ToolDispatch(error.to_string()))?;
            Ok(format_history_entries(&entries))
        })
    }
}

fn author_username_from_history_query(query: &str) -> Option<String> {
    let at = query.find('@')?;
    let candidate = query[at + 1..]
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>();
    let len = candidate.len();
    (5..=32).contains(&len).then_some(candidate)
}

fn format_history_entries(entries: &[SummaryMessageEntry]) -> String {
    let mut out = String::new();
    for entry in entries {
        let text = if entry.text.trim().is_empty() {
            entry.original_text.trim()
        } else {
            entry.text.trim()
        };
        if text.is_empty() {
            continue;
        }
        let who = entry
            .from
            .as_ref()
            .map(|user| user.first_name.trim())
            .filter(|name| !name.is_empty())
            .unwrap_or(entry.role.as_str());
        out.push_str(&format!("- {who}: {text}\n"));
    }
    if out.trim().is_empty() {
        "No matching messages found in this chat's history.".to_owned()
    } else {
        out.trim_end().to_owned()
    }
}

/// Memory searcher backed by `PostgresMemoryStore`. v1 uses lexical retrieval
/// (no query embedding); the engine still ranks/merges results.
pub struct PostgresMemorySearch {
    store: PostgresMemoryStore,
    card_limit: i32,
    episode_limit: i32,
}

impl PostgresMemorySearch {
    #[must_use]
    pub fn new(store: PostgresMemoryStore) -> Self {
        Self {
            store,
            card_limit: 12,
            episode_limit: 2,
        }
    }
}

impl MemorySearcher for PostgresMemorySearch {
    fn search<'a>(
        &'a self,
        chat_id: i64,
        user_id: i64,
        thread_id: Option<i32>,
        query: String,
    ) -> ContextSearchFuture<'a> {
        Box::pin(async move {
            let request = RetrievalRequest {
                scope: RetrievalScope {
                    chat_id,
                    thread_id: thread_id.unwrap_or(0),
                    user_id,
                    chat_type: String::new(),
                    username: String::new(),
                    active_usernames: Vec::new(),
                },
                query,
                card_limit: self.card_limit,
                episode_limit: self.episode_limit,
            };
            let memory = self
                .store
                .retrieve_with_vector(&request, None)
                .await
                .map_err(|error| AgentError::ToolDispatch(error.to_string()))?;
            Ok(format_memory(&memory))
        })
    }
}

fn format_memory(memory: &RetrievedMemory) -> String {
    let mut out = String::new();
    for card in &memory.cards {
        if card.fact_text.trim().is_empty() {
            continue;
        }
        out.push_str(&format!(
            "- {} (confidence {:.2})\n",
            card.fact_text.trim(),
            card.confidence
        ));
    }
    for episode in &memory.episodes {
        if episode.summary_text.trim().is_empty() {
            continue;
        }
        out.push_str(&format!(
            "- (recent episode) {}\n",
            episode.summary_text.trim()
        ));
    }
    if out.trim().is_empty() {
        "No relevant long-term memory found.".to_owned()
    } else {
        out.trim_end().to_owned()
    }
}

/// Settings for the song-writing agent (prompt + reasoner + budgets).
#[derive(Clone)]
pub struct SongAgentSettings {
    pub enabled: bool,
    pub system_prompt: String,
    pub reasoner_provider: String,
    pub budgets: AgentBudgets,
    pub reasoner_max_tokens: i32,
}

impl SongAgentSettings {
    #[must_use]
    pub fn from_app_config(config: &AppConfig, system_prompt: String) -> Self {
        let reasoner_provider = if config.llm.agentic.reasoner_provider.trim().is_empty() {
            openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER.to_owned()
        } else {
            config.llm.agentic.reasoner_provider.clone()
        };
        Self {
            enabled: config.llm.agentic.song_enabled,
            system_prompt,
            reasoner_provider,
            budgets: AgentBudgets {
                max_steps: 10,
                max_total_tokens: 60_000,
                max_wall_ms: 180_000,
                max_tool_calls: 5,
                max_tool_errors: 3,
            },
            reasoner_max_tokens: 4096,
        }
    }

    fn profile(&self, reasoner_model: String) -> AgentProfile {
        AgentProfile {
            id: "song".to_owned(),
            system_prompt: self.system_prompt.clone(),
            allowed_tools: vec![
                STEP_WEB_SEARCH.to_owned(),
                STEP_CRAWL_URL.to_owned(),
                STEP_HISTORY_SEARCH.to_owned(),
                STEP_MEMORY_SEARCH.to_owned(),
            ],
            reasoner_model: reasoner_model.clone(),
            writer_model: reasoner_model,
            budgets: self.budgets,
            reasoner_max_tokens: self.reasoner_max_tokens,
            writer_max_tokens: self.reasoner_max_tokens,
        }
    }
}

/// A `SongMaterialProvider` that writes lyrics with the multi-step song agent
/// (gathering context via web/history/memory) and parses the structured result.
/// Falls back to the wrapped provider (the single-pass reprompt) when the agent
/// is disabled or produces nothing usable.
pub struct SongAgentMaterialProvider {
    reasoner: Option<Arc<AgentProviderClient>>,
    settings: SongAgentSettings,
    tools: Option<Arc<dyn AgentTools>>,
    fallback: Arc<dyn SongMaterialProvider + Send + Sync>,
}

impl SongAgentMaterialProvider {
    #[must_use]
    pub fn new(
        reasoner: Option<Arc<AgentProviderClient>>,
        settings: SongAgentSettings,
        tools: Option<Arc<dyn AgentTools>>,
        fallback: Arc<dyn SongMaterialProvider + Send + Sync>,
    ) -> Self {
        Self {
            reasoner,
            settings,
            tools,
            fallback,
        }
    }

    async fn run_agent(
        &self,
        reasoner: &Arc<AgentProviderClient>,
        tools: &Arc<dyn AgentTools>,
        params: &MusicGenJobParams,
        topic: &str,
    ) -> Option<SongMaterial> {
        let origin = AgentOrigin {
            chat_id: params.chat_id,
            message_id: params.message_id,
            user_id: params.user_id,
            thread_id: params.thread_id,
            user_full_name: params.user_full_name.clone(),
        };
        let profile = self.settings.profile(reasoner.model.clone());
        let reasoner_adapter = AifarmReasoner::with_context(
            Arc::clone(reasoner),
            RoutedRequestContext {
                workflow_key: AGENTIC_SONG_WORKFLOW.to_owned(),
                queue_name: Some(MUSIC_VIP_QUEUE_NAME.to_owned()),
                chat_id: (params.chat_id != 0).then_some(params.chat_id),
                thread_id: params.thread_id,
                message_id: (params.message_id != 0).then_some(params.message_id),
                ..RoutedRequestContext::default()
            },
        );
        let mut state = AgentState::new("song", topic, origin, now_unix_ms());
        loop {
            match advance_one_step(
                &profile,
                &reasoner_adapter,
                tools.as_ref(),
                state,
                now_unix_ms(),
            )
            .await
            {
                Ok(StepProgress::Continue(next)) => state = next,
                Ok(StepProgress::Terminal(next)) => {
                    state = next;
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, "song agent step failed");
                    return None;
                }
            }
        }
        match &state.outcome {
            Some(AgentOutcome::Completed { answer }) => parse_song_material(answer),
            Some(AgentOutcome::Stopped { partial, .. }) => parse_song_material(partial),
            _ => None,
        }
    }
}

impl SongMaterialProvider for SongAgentMaterialProvider {
    fn build_song_material<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
        topic: &'a str,
    ) -> SongMaterialFuture<'a> {
        Box::pin(async move {
            if self.settings.enabled
                && let (Some(reasoner), Some(tools)) = (&self.reasoner, &self.tools)
                && let Some(material) = self.run_agent(reasoner, tools, params, topic).await
            {
                return Ok(material);
            }
            tracing::debug!("song agent inactive or empty; using reprompt fallback");
            self.fallback.build_song_material(params, topic).await
        })
    }
}

/// Parse the song agent's structured final answer into `SongMaterial`. Returns
/// `None` when the required parts are missing, so the caller falls back.
fn parse_song_material(answer: &str) -> Option<SongMaterial> {
    let mut style = String::new();
    let mut language = String::new();
    let mut title = String::new();
    let mut lyrics = String::new();
    let mut in_lyrics = false;
    for line in answer.lines() {
        if in_lyrics {
            lyrics.push_str(line);
            lyrics.push('\n');
            continue;
        }
        let trimmed = line.trim();
        if let Some(value) = strip_label(trimmed, "STYLE:") {
            style = value.to_owned();
        } else if let Some(value) = strip_label(trimmed, "LANGUAGE:") {
            language = value.to_owned();
        } else if let Some(value) = strip_label(trimmed, "TITLE:") {
            title = value.to_owned();
        } else if let Some(rest) = strip_label(trimmed, "LYRICS:") {
            in_lyrics = true;
            if !rest.is_empty() {
                lyrics.push_str(rest);
                lyrics.push('\n');
            }
        }
    }
    let lyrics = lyrics.trim().to_owned();
    let style = style.trim().to_owned();
    let language = language.trim().to_owned();
    if lyrics.is_empty() || style.is_empty() || language.is_empty() {
        return None;
    }
    Some(SongMaterial {
        title: title.trim().to_owned(),
        lyrics,
        style: style.clone(),
        raw_style: style,
        vocal_language: language,
    })
}

fn strip_label<'a>(line: &'a str, label: &str) -> Option<&'a str> {
    line.get(..label.len())
        .filter(|head| head.eq_ignore_ascii_case(label))
        .map(|_| line[label.len()..].trim())
}

/// Notice returned by the stubbed search tools so the agent degrades gracefully
/// instead of erroring when live web search is intentionally disabled.
const SEARCH_UNAVAILABLE_NOTICE: &str = "Web search is currently unavailable. Continue using the user's memory, the chat \
     history, and your own knowledge.";

/// Web-search provider that performs no live search. Used by flows where search is
/// intentionally stubbed (the image agent, until the search pipeline is reworked);
/// swap it for the real Serper client to turn search on.
pub(crate) struct UnavailableWebSearch;

impl WebSearchProvider for UnavailableWebSearch {
    fn search<'a>(&'a self, _query: &'a str) -> WebSearchFuture<'a> {
        Box::pin(async { Ok(SEARCH_UNAVAILABLE_NOTICE.to_owned()) })
    }
}

/// URL crawler counterpart to [`UnavailableWebSearch`]; performs no live fetch.
pub(crate) struct UnavailableUrlCrawler;

impl UrlCrawler for UnavailableUrlCrawler {
    fn crawl<'a>(&'a self, _url: &'a str) -> CrawlUrlFuture<'a> {
        Box::pin(async { Ok(SEARCH_UNAVAILABLE_NOTICE.to_owned()) })
    }
}

/// Build the stubbed web searcher as a trait object.
#[must_use]
pub fn unavailable_web_search() -> Arc<dyn WebSearchProvider> {
    Arc::new(UnavailableWebSearch)
}

/// Build the stubbed URL crawler as a trait object.
#[must_use]
pub fn unavailable_url_crawler() -> Arc<dyn UrlCrawler> {
    Arc::new(UnavailableUrlCrawler)
}

/// Settings for the image-prompt agent (prompt + reasoner + budgets).
#[derive(Clone)]
pub struct ImageAgentSettings {
    pub enabled: bool,
    pub system_prompt: String,
    pub reasoner_provider: String,
    pub budgets: AgentBudgets,
    pub reasoner_max_tokens: i32,
}

impl ImageAgentSettings {
    #[must_use]
    pub fn from_app_config(config: &AppConfig, system_prompt: String) -> Self {
        let reasoner_provider = if config.llm.agentic.reasoner_provider.trim().is_empty() {
            openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER.to_owned()
        } else {
            config.llm.agentic.reasoner_provider.clone()
        };
        Self {
            enabled: config.llm.agentic.image_enabled,
            system_prompt,
            reasoner_provider,
            budgets: AgentBudgets {
                max_steps: 6,
                max_total_tokens: 30_000,
                max_wall_ms: 90_000,
                max_tool_calls: 3,
                max_tool_errors: 2,
            },
            reasoner_max_tokens: 2048,
        }
    }

    fn profile(&self, reasoner_model: String) -> AgentProfile {
        AgentProfile {
            id: "image".to_owned(),
            system_prompt: self.system_prompt.clone(),
            allowed_tools: vec![
                STEP_MEMORY_SEARCH.to_owned(),
                STEP_HISTORY_SEARCH.to_owned(),
                STEP_WEB_SEARCH.to_owned(),
                STEP_CRAWL_URL.to_owned(),
            ],
            reasoner_model: reasoner_model.clone(),
            writer_model: reasoner_model,
            budgets: self.budgets,
            reasoner_max_tokens: self.reasoner_max_tokens,
            writer_max_tokens: self.reasoner_max_tokens,
        }
    }
}

/// An [`ImageGenerator`] wrapper that refines the draw prompt with the multi-step
/// image agent (using the user's memory and chat history; web search stubbed) before
/// delegating to the inner generator. The refined prompt is written into
/// `prompt_variants`, which makes the inner optimizing generator skip its own
/// single-pass reprompt. When the agent is disabled, unavailable, or yields nothing
/// usable, the request passes through unchanged and the inner optimizer runs as before.
pub struct ImageAgentImageGenerator<Inner> {
    inner: Inner,
    reasoner: Option<Arc<AgentProviderClient>>,
    tools: Option<Arc<dyn AgentTools>>,
    settings: ImageAgentSettings,
}

impl<Inner> ImageAgentImageGenerator<Inner> {
    #[must_use]
    pub fn new(
        inner: Inner,
        reasoner: Option<Arc<AgentProviderClient>>,
        tools: Option<Arc<dyn AgentTools>>,
        settings: ImageAgentSettings,
    ) -> Self {
        Self {
            inner,
            reasoner,
            tools,
            settings,
        }
    }
}

impl<Inner> ImageAgentImageGenerator<Inner>
where
    Inner: ImageGenerator + Sync,
{
    async fn refine_prompt(
        &self,
        reasoner: &Arc<AgentProviderClient>,
        tools: &Arc<dyn AgentTools>,
        request: &ImageGenerationRequest,
    ) -> Option<String> {
        let origin = AgentOrigin {
            chat_id: request.chat_id,
            message_id: request.message_id,
            user_id: request.user_id,
            thread_id: request.thread_id,
            user_full_name: request.user_full_name.clone(),
        };
        let profile = self.settings.profile(reasoner.model.clone());
        let reasoner_adapter = AifarmReasoner::with_context(
            Arc::clone(reasoner),
            RoutedRequestContext {
                workflow_key: AGENTIC_IMAGE_WORKFLOW.to_owned(),
                chat_id: (request.chat_id != 0).then_some(request.chat_id),
                thread_id: request.thread_id,
                message_id: (request.message_id != 0).then_some(request.message_id),
                suppress_all_attempts_exhausted_admin_report: true,
                suppressed_all_attempts_exhausted_reason: Some("image_agent_fallback".to_owned()),
                ..RoutedRequestContext::default()
            },
        );
        let mut state = AgentState::new("image", request.prompt.as_str(), origin, now_unix_ms());
        loop {
            match advance_one_step(
                &profile,
                &reasoner_adapter,
                tools.as_ref(),
                state,
                now_unix_ms(),
            )
            .await
            {
                Ok(StepProgress::Continue(next)) => state = next,
                Ok(StepProgress::Terminal(next)) => {
                    state = next;
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, "image agent step failed");
                    return None;
                }
            }
        }
        match &state.outcome {
            Some(AgentOutcome::Completed { answer }) => parse_image_prompt(answer),
            Some(AgentOutcome::Stopped { partial, .. }) => parse_image_prompt(partial),
            _ => None,
        }
    }

    /// Run the agent when active and return the request with `prompt_variants` filled
    /// by the refined prompt; otherwise return the request unchanged.
    async fn maybe_refined_request(
        &self,
        mut request: ImageGenerationRequest,
    ) -> ImageGenerationRequest {
        if !self.settings.enabled || request.prompt.trim().is_empty() {
            return request;
        }
        if request
            .prompt_variants
            .iter()
            .any(|variant| !variant.trim().is_empty())
        {
            return request;
        }
        let (Some(reasoner), Some(tools)) = (&self.reasoner, &self.tools) else {
            return request;
        };
        match self.refine_prompt(reasoner, tools, &request).await {
            Some(refined) if !refined.trim().is_empty() => {
                let count = self.inner.expected_image_count().max(1);
                request.prompt_variants = vec![refined; count];
            }
            _ => tracing::debug!("image agent inactive or empty; using reprompt fallback"),
        }
        request
    }
}

impl<Inner> ImageGenerator for ImageAgentImageGenerator<Inner>
where
    Inner: ImageGenerator + Sync,
{
    fn expected_image_count(&self) -> usize {
        self.inner.expected_image_count()
    }

    fn generate_image<'a>(&'a self, request: ImageGenerationRequest) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let request = self.maybe_refined_request(request).await;
            self.inner.generate_image(request).await
        })
    }

    fn generate_image_streaming<'a>(
        &'a self,
        request: ImageGenerationRequest,
        progress: ImageGenerationProgressSink,
    ) -> ImageGenerationFuture<'a> {
        Box::pin(async move {
            let request = self.maybe_refined_request(request).await;
            self.inner.generate_image_streaming(request, progress).await
        })
    }
}

/// Parse the image agent's structured final answer into a refined prompt. Returns
/// `None` when no `PROMPT:` block is present, so the caller falls back to the optimizer.
fn parse_image_prompt(answer: &str) -> Option<String> {
    let mut prompt = String::new();
    let mut in_prompt = false;
    for line in answer.lines() {
        let trimmed = line.trim();
        if in_prompt {
            if strip_label(trimmed, "NEGATIVE:").is_some() {
                break;
            }
            prompt.push_str(line);
            prompt.push('\n');
            continue;
        }
        if let Some(rest) = strip_label(trimmed, "PROMPT:") {
            in_prompt = true;
            if !rest.is_empty() {
                prompt.push_str(rest);
                prompt.push('\n');
            }
        }
    }
    let prompt = prompt.trim().to_owned();
    if prompt.is_empty() {
        return None;
    }
    Some(prompt)
}

/// Current unix time in milliseconds for budget accounting.
#[must_use]
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn agent_provider_client_for_attempt(
    config: &AppConfig,
    base: &AgentProviderClient,
    attempt: &RoutedAttempt,
) -> AgentProviderClient {
    let model = attempt.model_name.clone();
    AgentProviderClient {
        client: AifarmHttpClient::new(agent_client_config_from_attempt(config, attempt)),
        model,
        include_reasoning: bool_override(attempt, "include_reasoning").or(base.include_reasoning),
        enable_thinking: bool_override(attempt, "enable_thinking").or(base.enable_thinking),
        temperature: attempt.overrides.temperature.or(base.temperature),
        max_tokens: attempt.overrides.max_tokens.unwrap_or(base.max_tokens),
        routed: None,
    }
}

fn agent_client_config_from_attempt(
    config: &AppConfig,
    attempt: &RoutedAttempt,
) -> AifarmClientConfig {
    let dialog = &config.llm.dialog;
    let mut client = AifarmClientConfig {
        base_url: config.llm.discovery.base_url.clone(),
        service_name: dialog.discovery_service_name.clone(),
        endpoint_name: dialog.discovery_endpoint_name.clone(),
        request_timeout: positive_seconds(dialog.request_timeout_seconds),
        poll_interval: positive_seconds(dialog.poll_interval_seconds),
        task_timeout: positive_seconds(dialog.task_timeout_seconds),
        capacity_wait: positive_seconds(dialog.aifarm_capacity_wait_seconds),
        capacity_poll_interval: positive_seconds(dialog.aifarm_capacity_poll_seconds),
        default_model: attempt.model_name.clone(),
        priority: DISCOVERY_PRIORITY_INTERACTIVE,
        workload: AIFARM_WORKLOAD_DIALOG.to_owned(),
        ..AifarmClientConfig::default()
    };
    if let Some(service) = attempt.discovery_service_name.as_deref() {
        client.service_name = service.to_owned();
    }
    if let Some(endpoint) = attempt.discovery_endpoint_name.as_deref() {
        client.endpoint_name = endpoint.to_owned();
    }
    if let Some(endpoint) = attempt
        .model_base_url
        .as_deref()
        .or(attempt.provider_endpoint.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if attempt.discovery_service_name.is_some() || attempt.discovery_endpoint_name.is_some() {
            client.base_url = endpoint.to_owned();
        } else {
            client.direct_url = normalize_chat_completions_url(endpoint);
        }
    }
    if attempt.provider_name.eq_ignore_ascii_case("openrouter") {
        client.direct_url = "https://openrouter.ai/api/v1/chat/completions".to_owned();
        client.api_key = config.open_router.key.clone();
    } else if attempt
        .provider_name
        .eq_ignore_ascii_case(crate::dialog_runtime::VRAM_CLOUD_PROVIDER_NAME)
    {
        client.api_key = dialog.aifarm_pool_api_key.clone();
    }
    client.with_defaults()
}

fn apply_agent_attempt_to_request(
    request: &mut ChatCompletionRequest,
    provider: &AgentProviderClient,
    attempt: &RoutedAttempt,
) {
    if !provider.model.trim().is_empty() {
        request.model = provider.model.clone();
    }
    if provider.max_tokens > 0 {
        request.max_tokens = provider.max_tokens;
    }
    request.temperature = provider.temperature;
    request.include_reasoning = provider.include_reasoning;
    if let Some(frequency_penalty) = attempt.overrides.frequency_penalty {
        request.frequency_penalty = Some(frequency_penalty);
    }
    if let Some(presence_penalty) = attempt.overrides.presence_penalty {
        request.presence_penalty = Some(presence_penalty);
    }
    if let Some(repeat_penalty) = attempt.overrides.repeat_penalty {
        request.repeat_penalty = Some(repeat_penalty);
    }
    if let Some(top_p) = f64_override(attempt, "top_p") {
        request.top_p = Some(top_p);
    }
    if let Some(top_k) = f64_override(attempt, "top_k") {
        request.top_k = Some(top_k);
    }
    if let Some(enable) = provider.enable_thinking {
        request.chat_template_kwargs = Some(json!({ "enable_thinking": enable }));
    }
}

fn bool_override(attempt: &RoutedAttempt, key: &str) -> Option<bool> {
    attempt.overrides.extra.get(key).and_then(Value::as_bool)
}

fn f64_override(attempt: &RoutedAttempt, key: &str) -> Option<f64> {
    attempt.overrides.extra.get(key).and_then(Value::as_f64)
}

fn agent_retryable_reason(error: &AgentError) -> Option<FailureReason> {
    match error {
        AgentError::Reasoner(message) => retryable_reason_from_message(message),
        AgentError::ToolDispatch(_) | AgentError::ToolParse(_) => None,
    }
}

fn positive_seconds(seconds: i32) -> std::time::Duration {
    if seconds <= 0 {
        std::time::Duration::ZERO
    } else {
        std::time::Duration::from_secs(seconds as u64)
    }
}

fn build_request(
    provider: &AgentProviderClient,
    call: &ReasonerCall,
    with_tools: bool,
) -> ChatCompletionRequest {
    let messages = call.messages.iter().map(to_chat_message).collect();
    let max_tokens = if call.max_tokens > 0 {
        call.max_tokens
    } else {
        provider.max_tokens
    };
    let mut request = ChatCompletionRequest {
        model: call.model.clone(),
        messages,
        max_tokens,
        temperature: provider.temperature,
        include_reasoning: provider.include_reasoning,
        ..ChatCompletionRequest::default()
    };
    if with_tools {
        let tools: Vec<Value> = call
            .tools
            .iter()
            .filter_map(|tool| serde_json::to_value(tool).ok())
            .collect();
        if !tools.is_empty() {
            request.tools = tools;
            request.tool_choice = Some(json!("auto"));
            request.parallel_tool_calls = Some(false);
        }
    }
    if let Some(enable) = provider.enable_thinking {
        request.chat_template_kwargs = Some(json!({ "enable_thinking": enable }));
    }
    request
}

fn to_chat_message(message: &AgentMessage) -> ChatMessage {
    match message.role {
        AgentRole::Tool => {
            let name = message.tool_name.as_deref().unwrap_or("tool");
            ChatMessage {
                role: "user".to_owned(),
                content: format!("Observation from tool `{name}`:\n{}", message.content),
                ..ChatMessage::default()
            }
        }
        role => ChatMessage {
            role: chat_role(role).to_owned(),
            content: message.content.clone(),
            ..ChatMessage::default()
        },
    }
}

fn chat_role(role: AgentRole) -> &'static str {
    match role {
        AgentRole::System => "system",
        AgentRole::User => "user",
        AgentRole::Assistant | AgentRole::Tool => "assistant",
    }
}

fn parse_reply(result: &CompletionResult) -> Result<ReasonerReply, AgentError> {
    let Some(response) = &result.response else {
        return Err(AgentError::Reasoner(format!(
            "empty response body (status {})",
            result.status_code
        )));
    };
    let message = response
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"));
    let text = message
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let tool_calls = message
        .and_then(|message| message.get("tool_calls"))
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .filter_map(|call| serde_json::from_value::<NativeToolCall>(call.clone()).ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let usage = response.get("usage");
    let prompt_tokens = usage
        .and_then(|usage| usage.get("prompt_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion_tokens = usage
        .and_then(|usage| usage.get("completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let (prompt_tokens, completion_tokens) = if prompt_tokens == 0 && completion_tokens == 0 {
        // Fallback estimate when the backend omits usage, so budgets still trip.
        (0, u64::try_from(text.len() / 4).unwrap_or(0))
    } else {
        (prompt_tokens, completion_tokens)
    };

    Ok(ReasonerReply {
        text,
        tool_calls,
        prompt_tokens,
        completion_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn completion(response: Value) -> CompletionResult {
        CompletionResult {
            job_id: "j".to_owned(),
            status_code: 200,
            raw_body: String::new(),
            response: Some(response),
        }
    }

    #[test]
    fn parses_tool_call_and_usage() {
        let result = completion(json!({
            "choices": [{
                "message": {
                    "content": "",
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": { "name": "web_search", "arguments": "{\"query\":\"rust\"}" }
                    }]
                }
            }],
            "usage": { "prompt_tokens": 12, "completion_tokens": 5 }
        }));
        let reply = parse_reply(&result).expect("reply");
        assert_eq!(reply.tool_calls.len(), 1);
        assert_eq!(reply.tool_calls[0].function.name, "web_search");
        assert_eq!(reply.prompt_tokens, 12);
        assert_eq!(reply.completion_tokens, 5);
    }

    #[test]
    fn build_request_includes_native_tools_when_call_has_tools() {
        let provider = AgentProviderClient {
            client: AifarmHttpClient::new(AifarmClientConfig::default()),
            model: "model".to_owned(),
            include_reasoning: None,
            enable_thinking: None,
            temperature: None,
            max_tokens: 100,
            routed: None,
        };
        let call = ReasonerCall {
            model: "model".to_owned(),
            max_tokens: 100,
            messages: vec![AgentMessage::new(AgentRole::User, "draw a fish")],
            tools: openplotva_dialog::chat_completion_tools_for_names(&[STEP_HISTORY_SEARCH]),
        };

        let request = build_request(&provider, &call, true);

        assert!(!request.tools.is_empty());
        assert_eq!(request.tool_choice, Some(json!("auto")));
        assert_eq!(request.parallel_tool_calls, Some(false));
    }

    #[test]
    fn parses_song_material_from_structured_answer() {
        let answer = "STYLE: indie pop, acoustic guitar, warm, 96 BPM\nLANGUAGE: ru\nTITLE: Тёплый вечер\nLYRICS:\n[Verse 1]\nстрока раз\nстрока два\n[Chorus]\nприпев";
        let material = parse_song_material(answer).expect("parsed");
        assert_eq!(material.vocal_language, "ru");
        assert_eq!(material.title, "Тёплый вечер");
        assert!(material.style.contains("indie pop"));
        assert!(material.lyrics.starts_with("[Verse 1]"));
        assert!(material.lyrics.contains("припев"));
    }

    #[test]
    fn rejects_song_material_without_required_parts() {
        assert!(parse_song_material("just prose, no structure").is_none());
        // Missing the LYRICS block.
        assert!(parse_song_material("STYLE: pop\nLANGUAGE: en\nTITLE: x").is_none());
    }

    #[test]
    fn parses_image_prompt_block_and_drops_negative() {
        let answer = "PROMPT: a red fox in a snowy pine forest, soft morning light, \
                      watercolor, detailed\nNEGATIVE: blurry, text, watermark";
        let prompt = parse_image_prompt(answer).expect("parsed");
        assert!(prompt.starts_with("a red fox"));
        assert!(prompt.contains("watercolor"));
        assert!(!prompt.contains("NEGATIVE"));
        assert!(!prompt.contains("watermark"));
    }

    #[test]
    fn parses_multiline_image_prompt() {
        let answer = "PROMPT:\na lighthouse at dusk\nstormy sea, dramatic clouds";
        let prompt = parse_image_prompt(answer).expect("parsed");
        assert!(prompt.contains("lighthouse"));
        assert!(prompt.contains("stormy sea"));
    }

    #[test]
    fn rejects_image_prompt_without_marker() {
        assert!(parse_image_prompt("just some prose with no marker").is_none());
        assert!(parse_image_prompt("PROMPT:\n   ").is_none());
    }

    #[test]
    fn history_search_query_detects_author_username_mentions() {
        assert_eq!(
            author_username_from_history_query("@CherryCherry123"),
            Some("CherryCherry123".to_owned())
        );
        assert_eq!(
            author_username_from_history_query("сообщения от @CherryCherry123"),
            Some("CherryCherry123".to_owned())
        );
        assert_eq!(author_username_from_history_query("CherryCherry123"), None);
        assert_eq!(author_username_from_history_query("@"), None);
    }

    #[tokio::test]
    async fn stubbed_search_tools_return_unavailable_notice() {
        let web = unavailable_web_search();
        let crawl = unavailable_url_crawler();
        let search = web.search("anything").await.expect("ok");
        let fetched = crawl.crawl("https://example.com").await.expect("ok");
        assert!(search.contains("unavailable"));
        assert!(fetched.contains("unavailable"));
    }

    #[test]
    fn parses_final_text_and_estimates_tokens_without_usage() {
        let result = completion(json!({
            "choices": [{ "message": { "content": "final answer text" } }]
        }));
        let reply = parse_reply(&result).expect("reply");
        assert!(reply.tool_calls.is_empty());
        assert_eq!(reply.text, "final answer text");
        assert!(reply.completion_tokens > 0);
    }

    #[test]
    fn tool_role_is_rendered_as_user_observation() {
        let message = AgentMessage {
            role: AgentRole::Tool,
            content: "results".to_owned(),
            tool_name: Some("web_search".to_owned()),
        };
        let chat = to_chat_message(&message);
        assert_eq!(chat.role, "user");
        assert!(chat.content.contains("web_search"));
        assert!(chat.content.contains("results"));
    }

    #[test]
    fn registry_auto_registers_qwen_reasoner_by_default() {
        let config =
            openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig::default())
                .expect("default config");
        let registry = build_agent_provider_registry(&config);
        assert!(registry.contains(CONVERSATIONAL_PROVIDER));
        assert!(registry.contains(openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER));
    }
}
