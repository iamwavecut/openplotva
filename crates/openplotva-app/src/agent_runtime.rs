//! Composition-root wiring for the agent-loop engine: a registry of named
//! single-completion LLM clients, the `Reasoner`/`AgentTools` adapters over the
//! real AIFarm client and dialog tool box, and the search-agent profile.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use openplotva_agent::{
    AgentError, AgentMessage, AgentRole, AgentTools, Reasoner, ReasonerCall, ReasonerFuture,
    ReasonerReply, ToolDispatchFuture,
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

use openplotva_taskman::MusicGenJobParams;

use crate::dialog_tools::{UrlCrawler, WebSearchProvider};
use crate::image_jobs::{ImageContextFuture, ImageContextProvider, ImageGenerationRequest};
use crate::media::{agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config};
use crate::music_jobs::{SongContextFuture, SongContextProvider};
use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
};

/// The implicit provider name that always maps to the primary dialog config.
pub const CONVERSATIONAL_PROVIDER: &str = "conversational";

/// Stable routing provider id for the dedicated GPU2 NInfer service.
pub const LOCAL_REASONER_PROVIDER_NAME: &str = "aifarm-ninfer-gpu2";
/// Historical VibeThinker provider id retained for telemetry and rollback.
pub const VIBETHINKER_PROVIDER_NAME: &str = "aifarm-llamacpp-gpu2";
/// Historical VibeThinker Discovery service retained for telemetry and rollback.
pub const VIBETHINKER_SERVICE_NAME: &str = "llm-openai-qwen27b-gguf";
/// Discovery service targeted by the auto-registered `qwen-reasoner`
/// compatibility key.
pub const DEFAULT_LOCAL_REASONER_SERVICE_NAME: &str =
    openplotva_config::DEFAULT_NINFER_DISCOVERY_SERVICE_NAME;
/// Canonical model id sent to the dedicated NInfer service.
pub const DEFAULT_LOCAL_REASONER_MODEL: &str = openplotva_config::DEFAULT_NINFER_MODEL;

/// Legacy Rust API alias; use [`DEFAULT_LOCAL_REASONER_SERVICE_NAME`].
#[deprecated(note = "use DEFAULT_LOCAL_REASONER_SERVICE_NAME")]
pub const DEFAULT_QWEN_SERVICE_NAME: &str = DEFAULT_LOCAL_REASONER_SERVICE_NAME;
/// Legacy Rust API alias; use [`DEFAULT_LOCAL_REASONER_MODEL`].
#[deprecated(note = "use DEFAULT_LOCAL_REASONER_MODEL")]
pub const DEFAULT_QWEN_MODEL: &str = DEFAULT_LOCAL_REASONER_MODEL;

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

    // Auto-register the local NInfer reasoner so the search agent works out of the
    // box. The persisted `qwen-reasoner` compatibility key stays stable; an explicit
    // `LLM_PROVIDERS_*` entry of the same name takes precedence.
    let default_reasoner = normalize_name(openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER);
    if let std::collections::hash_map::Entry::Vacant(entry) = by_name.entry(default_reasoner) {
        let spec = local_reasoner_named_provider_config(config);
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
pub fn local_reasoner_named_provider_config(
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
            discovery_service_name: DEFAULT_LOCAL_REASONER_SERVICE_NAME.to_owned(),
            discovery_endpoint_name: config.llm.dialog.discovery_endpoint_name.clone(),
            model: DEFAULT_LOCAL_REASONER_MODEL.to_owned(),
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

/// Legacy Rust API alias; use [`local_reasoner_named_provider_config`].
#[deprecated(note = "use local_reasoner_named_provider_config")]
#[must_use]
pub fn qwen_reasoner_named_provider_config(
    config: &AppConfig,
) -> openplotva_config::NamedProviderConfig {
    local_reasoner_named_provider_config(config)
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
            let mut request = build_request(&self.provider, &call, true);
            // Agent rounds report to the trace sink like every other model
            // call; without this the optimizer runs are invisible to the
            // trace ring, llm_request_events, and the run correlation.
            request.trace = Some(openplotva_llm::LlmCallTrace {
                context: openplotva_llm::LlmCallContext {
                    chat_id: self.context.chat_id.unwrap_or_default(),
                    thread_id: self.context.thread_id,
                    user_id: self.context.user_id.unwrap_or_default(),
                    message_id: self.context.message_id.unwrap_or_default(),
                    ..openplotva_llm::LlmCallContext::default()
                },
                tags: openplotva_llm::LlmCallTags {
                    provider: openplotva_dialog::PROVIDER_AIFARM.to_owned(),
                    source: "aifarm_agent".to_owned(),
                    flow: self.context.workflow_key.clone(),
                    mode: "agent".to_owned(),
                    request_kind: "openai.chat.completions".to_owned(),
                    ..openplotva_llm::LlmCallTags::default()
                },
            });
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

/// Best-effort chat history and requester memory for the song director and the
/// image prompt optimizer: two concurrent time-boxed searches whose results are
/// trimmed and handed over as plain text.
pub struct ChatContextGatherer {
    history: Arc<dyn HistorySearcher>,
    memory: Arc<dyn MemorySearcher>,
}

const SONG_CONTEXT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
const IMAGE_CONTEXT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const SONG_CONTEXT_MAX_CHARS: usize = 1500;
const IMAGE_CONTEXT_MAX_CHARS: usize = 1200;
const CONTEXT_QUERY_MAX_CHARS: usize = 200;

struct ContextScope<'a> {
    chat_id: i64,
    user_id: i64,
    thread_id: Option<i32>,
    query: &'a str,
    person_label: &'a str,
    timeout: std::time::Duration,
    max_chars: usize,
}

impl ChatContextGatherer {
    #[must_use]
    pub fn new(history: Arc<dyn HistorySearcher>, memory: Arc<dyn MemorySearcher>) -> Self {
        Self { history, memory }
    }

    async fn gather(&self, scope: ContextScope<'_>) -> String {
        let query: String = scope.query.chars().take(CONTEXT_QUERY_MAX_CHARS).collect();
        if query.trim().is_empty() {
            return String::new();
        }
        let (history, memory) = tokio::join!(
            tokio::time::timeout(
                scope.timeout,
                self.history
                    .search(scope.chat_id, scope.thread_id, query.clone()),
            ),
            tokio::time::timeout(
                scope.timeout,
                self.memory
                    .search(scope.chat_id, scope.user_id, scope.thread_id, query),
            ),
        );
        let mut blocks = Vec::new();
        push_context_block(
            &mut blocks,
            "Recent chat context",
            history,
            "history",
            scope.max_chars,
        );
        push_context_block(
            &mut blocks,
            scope.person_label,
            memory,
            "memory",
            scope.max_chars,
        );
        blocks.join("\n\n")
    }
}

impl SongContextProvider for ChatContextGatherer {
    fn song_context<'a>(
        &'a self,
        params: &'a MusicGenJobParams,
        topic: &'a str,
    ) -> SongContextFuture<'a> {
        Box::pin(self.gather(ContextScope {
            chat_id: params.chat_id,
            user_id: params.user_id,
            thread_id: params.thread_id,
            query: topic,
            person_label: "What is known about the listener",
            timeout: SONG_CONTEXT_TIMEOUT,
            max_chars: SONG_CONTEXT_MAX_CHARS,
        }))
    }
}

impl ImageContextProvider for ChatContextGatherer {
    fn image_context<'a>(&'a self, request: &'a ImageGenerationRequest) -> ImageContextFuture<'a> {
        Box::pin(self.gather(ContextScope {
            chat_id: request.chat_id,
            user_id: request.user_id,
            thread_id: request.thread_id,
            query: &request.prompt,
            person_label: "What is known about the requester",
            timeout: IMAGE_CONTEXT_TIMEOUT,
            max_chars: IMAGE_CONTEXT_MAX_CHARS,
        }))
    }
}

fn push_context_block(
    blocks: &mut Vec<String>,
    label: &str,
    result: Result<Result<String, AgentError>, tokio::time::error::Elapsed>,
    source: &'static str,
    max_chars: usize,
) {
    match result {
        Ok(Ok(text)) => {
            let text = text.trim();
            if text.is_empty() {
                return;
            }
            let mut text: String = text.chars().take(max_chars).collect();
            if text.chars().count() == max_chars {
                text.push('…');
            }
            blocks.push(format!("{label}:\n{text}"));
        }
        Ok(Err(error)) => tracing::debug!(%error, source, "context search failed"),
        Err(_) => tracing::debug!(source, "context search timed out"),
    }
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
    client.runtime_hint = attempt.provider_runtime_hint.clone().unwrap_or_default();
    client.supports_message_name = attempt.supports_message_name();
    client.gateway_fields =
        openplotva_llm::aifarm::GatewayRequestFields::from_overrides(&attempt.overrides.extra);
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
    fn history_search_query_detects_author_username_mentions() {
        assert_eq!(
            author_username_from_history_query("@cherry_example"),
            Some("cherry_example".to_owned())
        );
        assert_eq!(
            author_username_from_history_query("сообщения от @cherry_example"),
            Some("cherry_example".to_owned())
        );
        assert_eq!(author_username_from_history_query("cherry_example"), None);
        assert_eq!(author_username_from_history_query("@"), None);
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
    fn registry_auto_registers_ninfer_under_legacy_reasoner_key() {
        let config =
            openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig::default())
                .expect("default config");
        let registry = build_agent_provider_registry(&config);
        assert!(registry.contains(CONVERSATIONAL_PROVIDER));
        assert!(registry.contains(openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER));
        let reasoner = registry
            .get(openplotva_config::DEFAULT_AGENT_REASONER_PROVIDER)
            .expect("local reasoner");
        assert_eq!(reasoner.model, DEFAULT_LOCAL_REASONER_MODEL);
        let spec = local_reasoner_named_provider_config(&config);
        assert_eq!(
            spec.discovery_service_name,
            DEFAULT_LOCAL_REASONER_SERVICE_NAME
        );
        assert_eq!(reasoner.include_reasoning, Some(false));
        assert_eq!(reasoner.enable_thinking, Some(false));
    }

    #[test]
    fn explicit_legacy_reasoner_config_still_takes_precedence() {
        let config = openplotva_config::AppConfig::from_raw(openplotva_config::RawConfig {
            llm_provider_names: Some("qwen-reasoner".to_owned()),
            llm_provider_discovery_service_names: Some("custom-local-llm".to_owned()),
            llm_provider_models: Some("custom-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("explicit config");

        let spec = local_reasoner_named_provider_config(&config);

        assert_eq!(spec.name, "qwen-reasoner");
        assert_eq!(spec.discovery_service_name, "custom-local-llm");
        assert_eq!(spec.model, "custom-model");
    }
}
