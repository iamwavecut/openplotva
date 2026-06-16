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
    ToolDispatchFuture, advance_one_step, render_evidence,
};
use openplotva_config::AppConfig;
use openplotva_dialog::{
    NativeToolCall, STEP_CRAWL_URL, STEP_HISTORY_SEARCH, STEP_MEMORY_SEARCH, STEP_WEB_SEARCH,
    TOOL_RESULT_STATUS_OK, ToolContext, ToolResult, ToolStep,
};
use openplotva_history::{SummaryMessageEntry, decode_summary_message_entry_payloads};
use openplotva_llm::aifarm::{
    AifarmHttpClient, ChatCompletionRequest, ChatMessage, CompletionResult, StatusUpdate,
};
use openplotva_memory::{RetrievalRequest, RetrievalScope, RetrievedMemory};
use openplotva_storage::{PostgresHistoryStore, PostgresMemoryStore};
use serde_json::{Value, json};
use time::{Duration as TimeDuration, OffsetDateTime};

use openplotva_taskman::MusicGenJobParams;

use crate::dialog_tools::{UrlCrawler, WebSearchProvider};
use crate::media::{agent_client_config_from_named_provider, aifarm_dialog_config_from_app_config};
use crate::music_jobs::{SongMaterial, SongMaterialFuture, SongMaterialProvider};

/// The implicit provider name that always maps to the primary dialog config.
pub const CONVERSATIONAL_PROVIDER: &str = "conversational";

/// Default Discovery service the auto-registered `qwen-reasoner` provider targets.
pub const DEFAULT_QWEN_SERVICE_NAME: &str = "llm-openai-qwen35b-gguf";
/// Default model id sent to the qwen llama.cpp server. The server runs with
/// `--alias default` and reports its model id as `default`; override via
/// `LLM_PROVIDERS_MODELS`. (Underlying GGUF:
/// Qwen3.6-35B-A3B-Claude-4.6-Opus-Reasoning-Distilled-UD-Q3_K_XL.)
pub const DEFAULT_QWEN_MODEL: &str = "default";

/// Reasoner orchestration prompt for the search agent.
pub const SEARCH_SYSTEM_PROMPT: &str =
    include_str!("../../../prompts/agentic/search_system.prompt");
/// Writer synthesis prompt for the search agent.
pub const SEARCH_SYNTHESIS_PROMPT: &str =
    include_str!("../../../prompts/agentic/search_synthesis.prompt");
/// System prompt for the song-writing agent.
pub const SONG_SYSTEM_PROMPT: &str = include_str!("../../../prompts/agentic/song_system.prompt");

/// A single-completion LLM client plus the request defaults for one provider.
#[derive(Clone)]
pub struct AgentProviderClient {
    pub client: AifarmHttpClient,
    pub model: String,
    pub include_reasoning: Option<bool>,
    pub enable_thinking: Option<bool>,
    pub temperature: Option<f64>,
    pub max_tokens: i32,
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
            model: dialog.model,
            include_reasoning: dialog.include_reasoning,
            enable_thinking: dialog.enable_thinking,
            temperature: dialog.temperature,
            max_tokens: dialog.max_tokens,
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
            }),
        );
    }

    // Auto-register the default qwen reasoner so the search agent works out of the
    // box; an explicit `LLM_PROVIDERS_*` entry of the same name takes precedence.
    let default_reasoner =
        normalize_name(openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER);
    if !by_name.contains_key(&default_reasoner) {
        let spec = openplotva_config::NamedProviderConfig {
            name: openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER.to_owned(),
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
        };
        let client_config = agent_client_config_from_named_provider(config, &spec);
        by_name.insert(
            default_reasoner,
            Arc::new(AgentProviderClient {
                client: AifarmHttpClient::new(client_config),
                model: spec.model.clone(),
                include_reasoning: spec.include_reasoning,
                enable_thinking: spec.enable_thinking,
                temperature: spec.temperature,
                max_tokens: spec.max_tokens,
            }),
        );
    }

    AgentProviderRegistry { by_name }
}

/// Resolved search-agent settings (prompts + budgets + default providers), built
/// once from config so the worker needs no `AppConfig` reference.
#[derive(Clone)]
pub struct SearchAgentSettings {
    pub enabled: bool,
    pub system_prompt: String,
    pub synthesis_prompt: String,
    pub reasoner_provider: String,
    pub writer_provider: String,
    pub budgets: AgentBudgets,
    pub reasoner_max_tokens: i32,
    pub writer_max_tokens: i32,
}

impl SearchAgentSettings {
    #[must_use]
    pub fn from_app_config(
        config: &AppConfig,
        system_prompt: String,
        synthesis_prompt: String,
    ) -> Self {
        let search = &config.llm.agentic.search;
        let max_tool_calls = search.max_searches.saturating_add(search.max_crawls).max(1);
        let reasoner_provider = if search.reasoner_provider.trim().is_empty() {
            openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER.to_owned()
        } else {
            search.reasoner_provider.clone()
        };
        let writer_provider = if search.writer_provider.trim().is_empty() {
            CONVERSATIONAL_PROVIDER.to_owned()
        } else {
            search.writer_provider.clone()
        };
        Self {
            enabled: search.enabled,
            system_prompt,
            synthesis_prompt,
            reasoner_provider,
            writer_provider,
            budgets: AgentBudgets {
                max_steps: u32::try_from(search.max_steps.max(1)).unwrap_or(8),
                max_total_tokens: u64::try_from(search.max_total_tokens.max(0)).unwrap_or(0),
                max_wall_ms: u64::try_from(search.wall_timeout_seconds.max(0))
                    .unwrap_or(0)
                    .saturating_mul(1000),
                max_tool_calls: u32::try_from(max_tool_calls).unwrap_or(7),
                max_tool_errors: 3,
            },
            reasoner_max_tokens: 2048,
            writer_max_tokens: 4096,
        }
    }

    /// Build the engine profile for one run with the resolved provider models.
    #[must_use]
    pub fn profile(&self, reasoner_model: String, writer_model: String) -> AgentProfile {
        AgentProfile {
            id: "search".to_owned(),
            system_prompt: self.system_prompt.clone(),
            allowed_tools: vec![
                STEP_WEB_SEARCH.to_owned(),
                STEP_CRAWL_URL.to_owned(),
                STEP_HISTORY_SEARCH.to_owned(),
                STEP_MEMORY_SEARCH.to_owned(),
            ],
            reasoner_model,
            writer_model,
            budgets: self.budgets,
            reasoner_max_tokens: self.reasoner_max_tokens,
            writer_max_tokens: self.writer_max_tokens,
        }
    }
}

/// `Reasoner` adapter that performs one chat round-trip via the AIFarm client.
pub struct AifarmReasoner {
    provider: Arc<AgentProviderClient>,
}

impl AifarmReasoner {
    #[must_use]
    pub fn new(provider: Arc<AgentProviderClient>) -> Self {
        Self { provider }
    }
}

impl Reasoner for AifarmReasoner {
    fn complete<'a>(&'a self, call: ReasonerCall) -> ReasonerFuture<'a> {
        Box::pin(async move {
            let request = build_request(&self.provider, &call, true);
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

/// Run a single no-tools completion on a writer provider to synthesize the final
/// answer from the gathered evidence.
pub async fn synthesize_answer(
    provider: &AgentProviderClient,
    model: &str,
    max_tokens: i32,
    system_prompt: &str,
    user_content: &str,
) -> Result<String, AgentError> {
    let call = ReasonerCall {
        model: model.to_owned(),
        max_tokens,
        messages: vec![
            AgentMessage::new(AgentRole::System, system_prompt.to_owned()),
            AgentMessage::new(AgentRole::User, user_content.to_owned()),
        ],
        tools: Vec::new(),
    };
    let request = build_request(provider, &call, false);
    let mut sink = |_status: StatusUpdate| {};
    let result = provider
        .client
        .complete(request, &mut sink)
        .await
        .map_err(|error| AgentError::Reasoner(error.to_string()))?;
    let reply = parse_reply(&result)?;
    Ok(reply.text)
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
            let payloads = self
                .store
                .search_history_entries(chat_id, thread_id.unwrap_or(0), &query, cutoff, self.limit)
                .await
                .map_err(|error| AgentError::ToolDispatch(error.to_string()))?;
            let entries = decode_summary_message_entry_payloads(&payloads)
                .map_err(|error| AgentError::ToolDispatch(error.to_string()))?;
            Ok(format_history_entries(&entries))
        })
    }
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

/// Boxed future returned by [`AgenticWebSearch`].
pub type AgenticWebSearchFuture<'a> =
    Pin<Box<dyn Future<Output = Result<String, AgentError>> + Send + 'a>>;

/// Runs the search agent for a single query and returns a summarized answer text.
/// The conversational `web_search` tool uses this so the bot's brain gets a
/// researched summary instead of raw single-pass results.
pub trait AgenticWebSearch: Send + Sync {
    fn search_summary<'a>(&'a self, query: String) -> AgenticWebSearchFuture<'a>;
}

/// Synchronous (within one call) search-agent run: drive the engine loop to a
/// terminal state and return the reasoner's summary (plus gathered evidence).
/// No durable checkpointing — it lives inside one dialog tool call.
pub struct InlineSearchAgent {
    reasoner: Arc<AgentProviderClient>,
    settings: SearchAgentSettings,
    tools: Arc<dyn AgentTools>,
}

impl InlineSearchAgent {
    #[must_use]
    pub fn new(
        reasoner: Arc<AgentProviderClient>,
        settings: SearchAgentSettings,
        tools: Arc<dyn AgentTools>,
    ) -> Self {
        Self {
            reasoner,
            settings,
            tools,
        }
    }
}

impl AgenticWebSearch for InlineSearchAgent {
    fn search_summary<'a>(&'a self, query: String) -> AgenticWebSearchFuture<'a> {
        Box::pin(async move {
            let model = self.reasoner.model.clone();
            let profile = self.settings.profile(model.clone(), model);
            let reasoner = AifarmReasoner::new(Arc::clone(&self.reasoner));
            let mut state = AgentState::new("search", query, AgentOrigin::default(), now_unix_ms());
            loop {
                match advance_one_step(
                    &profile,
                    &reasoner,
                    self.tools.as_ref(),
                    state,
                    now_unix_ms(),
                )
                .await?
                {
                    StepProgress::Continue(next) => state = next,
                    StepProgress::Terminal(next) => {
                        state = next;
                        break;
                    }
                }
            }
            Ok(summarize_run(&state))
        })
    }
}

fn summarize_run(state: &AgentState) -> String {
    let evidence = render_evidence(state);
    let summary = match &state.outcome {
        Some(AgentOutcome::Completed { answer }) if !answer.trim().is_empty() => answer.clone(),
        Some(AgentOutcome::Stopped { partial, .. }) if !partial.trim().is_empty() => {
            partial.clone()
        }
        _ => String::new(),
    };
    match (summary.trim().is_empty(), evidence.trim().is_empty()) {
        (true, true) => "No relevant results were found.".to_owned(),
        (true, false) => evidence,
        (false, true) => summary,
        (false, false) => format!("{summary}\n\nGathered evidence:\n{evidence}"),
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
        let reasoner_provider = if config
            .llm
            .agentic
            .search
            .reasoner_provider
            .trim()
            .is_empty()
        {
            openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER.to_owned()
        } else {
            config.llm.agentic.search.reasoner_provider.clone()
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
        let reasoner_adapter = AifarmReasoner::new(Arc::clone(reasoner));
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

/// Current unix time in milliseconds for budget accounting.
#[must_use]
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
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
        assert!(registry.contains(openplotva_config::DEFAULT_AGENTIC_SEARCH_REASONER_PROVIDER));
    }
}
