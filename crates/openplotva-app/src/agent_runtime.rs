//! Context retrieval and local model configuration shared by dialog and media tools.
use crate::image_jobs::{ImageContextFuture, ImageContextProvider, ImageGenerationRequest};
use crate::music_jobs::{SongContextFuture, SongContextProvider};
use openplotva_agent::AgentError;
use openplotva_config::AppConfig;
use openplotva_history::{SummaryMessageEntry, decode_summary_message_entry_payloads};
use openplotva_memory::{RetrievalRequest, RetrievalScope, RetrievedMemory};
use openplotva_storage::{PostgresHistoryStore, PostgresMemoryStore};
use openplotva_taskman::MusicGenJobParams;
use std::{future::Future, pin::Pin, sync::Arc};
use time::{Duration as TimeDuration, OffsetDateTime};
fn normalize_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}
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
