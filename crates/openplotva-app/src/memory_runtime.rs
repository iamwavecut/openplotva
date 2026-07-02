//! App-level memory provider wiring.

use std::{error::Error as StdError, future::Future, pin::Pin, sync::Arc, time::Duration};

use openplotva_config::{AppConfig, MemoryConfig, ShieldConfig};
use openplotva_llm::{
    aifarm::{
        AifarmClientConfig, AifarmMemoryExtractor, AifarmMemoryExtractorConfig,
        AifarmMemoryExtractorError, GenkitOpenAiCompatibleMemoryExtractor,
        GenkitOpenAiCompatibleMemoryExtractorConfig, GenkitOpenAiCompatibleMemoryExtractorError,
        ReqwestAifarmTransport, normalize_chat_completions_url,
    },
    gemini::{
        GeminiMemoryExtractor, GeminiMemoryExtractorConfig, GeminiMemoryExtractorError,
        MODEL_GEMINI_FLASH_FALLBACK, MODEL_GEMINI_FLASH_LITE,
    },
    retry::{FailureReason, retryable_reason, retryable_reason_from_message},
};
use openplotva_memory::{
    Card, CardInput, DiscoveryRedactor, DiscoveryRedactorConfig, DiscoveryRedactorError, Episode,
    ExtractInput, ExtractOutput, FallbackMemoryExtractor, FallbackMemoryExtractorError, LinkInput,
    MemoryExtractor, MemoryExtractorFuture, RedactingMemoryExtractor, RunStats, TextRedactor,
    TextRedactorFuture, redact_extract_output_with_async,
};
use openplotva_storage::{
    DialogMemoryChatMeta, PgEmbeddingVector, PostgresMemoryStore, StorageError,
};
use openplotva_taskman::{
    InMemoryTaskQueue, JobType, MEMORY_CONSOLIDATION_QUEUE_NAME, new_memory_consolidation_job_at,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use time::{OffsetDateTime, Time};

use crate::embedder::DiscoveryEmbedderClient;
use crate::routed_attempts::{
    RoutedAttempt, RoutedAttemptRunError, RoutedAttemptWalker, RoutedRequestContext,
    RoutedRoutingError,
};
use crate::runtime_gemini_cache::resolve_google_ai_key;

pub const EMBEDDER_DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);
pub const MEMORY_RETRIEVAL_EMBEDDING_TIMEOUT: Duration = Duration::from_secs(15);
pub const MEMORY_CONSOLIDATION_JOB_POLL_INTERVAL: Duration = Duration::from_secs(1);
pub const MEMORY_CONSOLIDATION_JOB_WORKER_PREFIX: &str = "memory-consolidation-worker";
const MEMORY_RETRIEVAL_QUERY_TASK: &str = "Conversational query for scoped memory retrieval";
const MEMORY_CARD_EMBEDDING_TASK: &str = "Memory card for scoped conversational retrieval";
const MEMORY_EPISODE_EMBEDDING_TASK: &str =
    "Daily chat episode for scoped conversational retrieval";
const MEMORY_MAX_EXTRACTION_BATCH_INPUT_TOKENS: i32 = 10_000;
const EXISTING_CARDS_LIMIT: i32 = 80;
const EXISTING_USER_CARDS_LIMIT: i32 = 20;
const EXISTING_PARTICIPANT_CARDS_MAX_USER: usize = 20;
const OPENROUTER_MODEL_PREFIX: &str = "openrouter/";
const OPENROUTER_CHAT_COMPLETIONS_URL: &str = "https://openrouter.ai/api/v1/chat/completions";

type AifarmGenkitMemoryExtractor = FallbackMemoryExtractor<
    AifarmMemoryExtractor<ReqwestAifarmTransport>,
    AppGenkitMemoryExtractor,
    fn(&AifarmMemoryExtractorError) -> bool,
>;

type RedactingAifarmGenkitMemoryExtractor =
    RedactingMemoryExtractor<AifarmGenkitMemoryExtractor, DiscoveryRedactor>;

type RedactingAifarmRouteMemoryExtractor =
    RedactingMemoryExtractor<AifarmRouteMemoryExtractor, DiscoveryRedactor>;

/// Boxed future returned by embedding providers.
pub type EmbeddingProviderFuture<'a> = Pin<
    Box<dyn Future<Output = Result<Option<PgEmbeddingVector>, EmbedderClientError>> + Send + 'a>,
>;

/// Boxed future returned by batch embedding providers.
pub type EmbeddingBatchFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<Vec<Option<PgEmbeddingVector>>, EmbedderClientError>>
            + Send
            + 'a,
    >,
>;

/// Runtime embedding provider used by dialog materialization.
pub trait EmbeddingProvider: std::fmt::Debug + Send + Sync {
    fn embed_one<'a>(
        &'a self,
        text: &'a str,
        dimension: i32,
        task_description: &'a str,
    ) -> EmbeddingProviderFuture<'a>;

    /// Embed a batch of texts, preserving input order; empty texts map to `None`.
    ///
    /// The default loops over [`EmbeddingProvider::embed_one`]; providers that
    /// can batch natively (e.g. one Discovery job for all prompts) should
    /// override this.
    fn embed_batch<'a>(
        &'a self,
        texts: &'a [String],
        dimension: i32,
        task_description: &'a str,
    ) -> EmbeddingBatchFuture<'a> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(texts.len());
            for text in texts {
                out.push(self.embed_one(text, dimension, task_description).await?);
            }
            Ok(out)
        })
    }

    /// Whether the embedder is currently reachable. Used to gate work that must
    /// not start while the embedder is down (memory consolidation). Defaults to
    /// always available for providers without a breaker.
    fn is_available(&self) -> bool {
        true
    }
}

/// Boxed future returned by memory write stores.
pub type MemoryWriteStoreFuture<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'a>>;

/// Storage boundary needed to persist one extracted memory batch.
pub trait MemoryWriteStore: std::fmt::Debug + Send + Sync {
    /// Store error type.
    type Error: StdError + Send + Sync + 'static;

    /// Upsert extracted cards and optional embeddings.
    fn upsert_cards_with_embeddings<'a>(
        &'a self,
        cards: &'a [CardInput],
        embeddings: &'a [Option<PgEmbeddingVector>],
    ) -> MemoryWriteStoreFuture<'a, (RunStats, Vec<i64>), Self::Error>;

    /// Insert memory links.
    fn insert_links<'a>(
        &'a self,
        links: &'a [LinkInput],
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Mark one active card as superseded.
    fn supersede_card<'a>(
        &'a self,
        old_id: i64,
        new_id: i64,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Flag two contradictory cards as competing (both kept so the bot hedges).
    fn mark_cards_competing<'a>(
        &'a self,
        old_id: i64,
        new_id: i64,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Insert the run episode and optional embedding.
    fn insert_episode_with_embedding<'a>(
        &'a self,
        episode: Episode,
        model: &'a str,
        prompt_version: &'a str,
        embedding: Option<PgEmbeddingVector>,
    ) -> MemoryWriteStoreFuture<'a, (i64, bool), Self::Error>;
}

/// Storage boundary needed to claim and finalize memory runs.
pub trait MemoryRunStore: MemoryWriteStore {
    /// Ensure daily memory runs exist for the configured retention window.
    fn ensure_daily_runs<'a>(
        &'a self,
        now: OffsetDateTime,
        retention: Duration,
        policy: openplotva_memory::MemoryRunEnqueuePolicy,
    ) -> MemoryWriteStoreFuture<'a, u64, Self::Error>;

    /// Claim one memory run.
    fn claim_run<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
    ) -> MemoryWriteStoreFuture<'a, Option<openplotva_memory::Run>, Self::Error>;

    fn load_run_messages<'a>(
        &'a self,
        run: &'a openplotva_memory::Run,
        max_messages_per_run: i32,
    ) -> MemoryWriteStoreFuture<'a, Vec<openplotva_memory::Message>, Self::Error>;

    /// Load chat metadata for memory extraction input.
    fn load_run_chat_meta<'a>(
        &'a self,
        chat_id: i64,
    ) -> MemoryWriteStoreFuture<'a, Option<DialogMemoryChatMeta>, Self::Error>;

    /// List visible cards for existing-card extraction context.
    fn list_visible_cards<'a>(
        &'a self,
        scope: &'a openplotva_memory::RetrievalScope,
        limit: i32,
    ) -> MemoryWriteStoreFuture<'a, Vec<Card>, Self::Error>;

    /// Queue the next continuation cursor.
    fn enqueue_run_continuation<'a>(
        &'a self,
        run: &'a openplotva_memory::Run,
        after: &'a openplotva_memory::Message,
        remaining_messages: i32,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Complete one claimed run.
    fn complete_run<'a>(
        &'a self,
        run_id: i64,
        stats: RunStats,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Fail one claimed run.
    fn fail_run<'a>(
        &'a self,
        run_id: i64,
        cause: &'a str,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Release one claimed run back to the queue without consuming an attempt.
    fn release_run<'a>(&'a self, run_id: i64) -> MemoryWriteStoreFuture<'a, (), Self::Error>;
}

/// Storage boundary needed by runtime API memory restart mutation.
pub trait MemoryRestartStore: std::fmt::Debug + Send + Sync {
    /// Store error type.
    type Error: StdError + Send + Sync + 'static;

    /// Retry one failed memory run.
    fn retry_run<'a>(&'a self, run_id: i64) -> MemoryWriteStoreFuture<'a, (), Self::Error>;

    /// Retry all failed memory runs.
    fn retry_failed_runs<'a>(&'a self) -> MemoryWriteStoreFuture<'a, u64, Self::Error>;
}

impl MemoryWriteStore for PostgresMemoryStore {
    type Error = StorageError;

    fn upsert_cards_with_embeddings<'a>(
        &'a self,
        cards: &'a [CardInput],
        embeddings: &'a [Option<PgEmbeddingVector>],
    ) -> MemoryWriteStoreFuture<'a, (RunStats, Vec<i64>), Self::Error> {
        Box::pin(async move {
            PostgresMemoryStore::upsert_cards_with_embeddings(self, cards, embeddings).await
        })
    }

    fn insert_links<'a>(
        &'a self,
        links: &'a [LinkInput],
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { PostgresMemoryStore::insert_links(self, links).await })
    }

    fn supersede_card<'a>(
        &'a self,
        old_id: i64,
        new_id: i64,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { PostgresMemoryStore::supersede_card(self, old_id, new_id).await })
    }

    fn mark_cards_competing<'a>(
        &'a self,
        old_id: i64,
        new_id: i64,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(
            async move { PostgresMemoryStore::mark_cards_competing(self, old_id, new_id).await },
        )
    }

    fn insert_episode_with_embedding<'a>(
        &'a self,
        episode: Episode,
        model: &'a str,
        prompt_version: &'a str,
        embedding: Option<PgEmbeddingVector>,
    ) -> MemoryWriteStoreFuture<'a, (i64, bool), Self::Error> {
        Box::pin(async move {
            PostgresMemoryStore::insert_episode_with_embedding(
                self,
                episode,
                model,
                prompt_version,
                embedding.as_ref(),
            )
            .await
        })
    }
}

impl MemoryRunStore for PostgresMemoryStore {
    fn ensure_daily_runs<'a>(
        &'a self,
        now: OffsetDateTime,
        retention: Duration,
        policy: openplotva_memory::MemoryRunEnqueuePolicy,
    ) -> MemoryWriteStoreFuture<'a, u64, Self::Error> {
        Box::pin(async move {
            PostgresMemoryStore::ensure_daily_runs(self, now, retention, policy).await
        })
    }

    fn claim_run<'a>(
        &'a self,
        owner: &'a str,
        lease: Duration,
    ) -> MemoryWriteStoreFuture<'a, Option<openplotva_memory::Run>, Self::Error> {
        Box::pin(async move { PostgresMemoryStore::claim_run(self, owner, lease).await })
    }

    fn load_run_messages<'a>(
        &'a self,
        run: &'a openplotva_memory::Run,
        max_messages_per_run: i32,
    ) -> MemoryWriteStoreFuture<'a, Vec<openplotva_memory::Message>, Self::Error> {
        Box::pin(async move {
            PostgresMemoryStore::load_run_messages(self, run, max_messages_per_run).await
        })
    }

    fn load_run_chat_meta<'a>(
        &'a self,
        chat_id: i64,
    ) -> MemoryWriteStoreFuture<'a, Option<DialogMemoryChatMeta>, Self::Error> {
        Box::pin(
            async move { PostgresMemoryStore::get_dialog_memory_chat_meta(self, chat_id).await },
        )
    }

    fn list_visible_cards<'a>(
        &'a self,
        scope: &'a openplotva_memory::RetrievalScope,
        limit: i32,
    ) -> MemoryWriteStoreFuture<'a, Vec<Card>, Self::Error> {
        Box::pin(async move { PostgresMemoryStore::list_visible_cards(self, scope, limit).await })
    }

    fn enqueue_run_continuation<'a>(
        &'a self,
        run: &'a openplotva_memory::Run,
        after: &'a openplotva_memory::Message,
        remaining_messages: i32,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move {
            PostgresMemoryStore::enqueue_run_continuation(self, run, after, remaining_messages)
                .await
        })
    }

    fn complete_run<'a>(
        &'a self,
        run_id: i64,
        stats: RunStats,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { PostgresMemoryStore::complete_run(self, run_id, stats).await })
    }

    fn fail_run<'a>(
        &'a self,
        run_id: i64,
        cause: &'a str,
    ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { PostgresMemoryStore::fail_run(self, run_id, cause).await })
    }

    fn release_run<'a>(&'a self, run_id: i64) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { PostgresMemoryStore::release_run(self, run_id).await })
    }
}

impl MemoryRestartStore for PostgresMemoryStore {
    type Error = StorageError;

    fn retry_run<'a>(&'a self, run_id: i64) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
        Box::pin(async move { PostgresMemoryStore::retry_run(self, run_id).await })
    }

    fn retry_failed_runs<'a>(&'a self) -> MemoryWriteStoreFuture<'a, u64, Self::Error> {
        Box::pin(async move { PostgresMemoryStore::retry_failed_runs(self).await })
    }
}

/// Result of one extracted memory batch write.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryExtractionWriteReport {
    /// Raw extractor output.
    pub output: ExtractOutput,
    pub stats: RunStats,
    /// Persisted card IDs in input order.
    pub card_ids: Vec<i64>,
    /// Insertable link count derived from extracted facts.
    pub links_inserted: usize,
    /// Supersession pairs attempted.
    pub superseded: Vec<(i64, i64)>,
    /// Episode row ID, when an episode was inserted or updated.
    pub episode_id: i64,
    /// Whether the episode insert returned a fresh row.
    pub episode_inserted: bool,
    /// Non-fatal card embedding failure.
    pub card_embedding_error: Option<String>,
    /// Non-fatal episode embedding failure.
    pub episode_embedding_error: Option<String>,
    /// Non-fatal link write failure.
    pub link_error: Option<String>,
    /// Non-fatal supersession write failures.
    pub supersession_errors: Vec<String>,
    /// Competing pairs attempted.
    pub competing: Vec<(i64, i64)>,
    /// Non-fatal competing write failures.
    pub competing_errors: Vec<String>,
}

/// Result of one memory run processing tick.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MemoryRunProcessReport {
    /// Whether a run was claimed.
    pub processed: bool,
    /// Claimed run ID.
    pub run_id: i64,
    /// Messages loaded from history.
    pub loaded_messages: usize,
    /// Messages selected before noise filtering.
    pub selected_messages: usize,
    pub filtered_messages: usize,
    /// Whether a continuation run was queued.
    pub continuation_queued: bool,
    /// Extraction/write report, when extraction ran.
    pub write: Option<MemoryExtractionWriteReport>,
}

/// Result of one memory service scheduler tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryServiceTickReport {
    /// Whether memory service work was enabled.
    pub enabled: bool,
    /// Whether this tick is the startup tick.
    pub startup: bool,
    /// Whether the configured daily window was active.
    pub daily_window_active: bool,
    /// Newly queued daily runs reported by storage.
    pub queued_daily_runs: u64,
    /// Runs completed or skipped without a process error.
    pub processed_runs: i64,
    /// Claimed runs that ended in a process failure and were marked failed.
    pub failed_runs: i64,
    /// Claim/load/store errors before a claimed process error.
    pub store_errors: i64,
}

/// Aggregate report from the memory service worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryServiceWorkerReport {
    /// Whether memory service work was enabled.
    pub enabled: bool,
    /// Number of scheduled ticks after startup.
    pub ticks: i64,
    /// Number of runtime API triggered queue-drain ticks.
    pub triggered_ticks: i64,
    /// Newly queued daily runs across startup and scheduled ticks.
    pub queued_daily_runs: u64,
    /// Runs completed or skipped without a process error.
    pub processed_runs: i64,
    /// Claimed runs that ended in a process failure and were marked failed.
    pub failed_runs: i64,
    /// Claim/load/store errors before a claimed process error.
    pub store_errors: i64,
}

/// Result of one memory-consolidation taskman scheduler tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryConsolidationQueueScheduleReport {
    /// Whether memory scheduling is enabled.
    pub enabled: bool,
    /// Whether this tick is the startup tick.
    pub startup: bool,
    /// Whether the configured daily window was active.
    pub daily_window_active: bool,
    /// Newly queued daily runs reported by storage.
    pub queued_daily_runs: u64,
    /// Assigned taskman job ID.
    pub assigned_job_id: Option<i64>,
    /// Daily-run enqueue error, if any.
    pub store_error: Option<String>,
}

/// Aggregate report from the memory-consolidation taskman scheduler.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryConsolidationQueueSchedulerReport {
    /// Whether memory scheduling is enabled.
    pub enabled: bool,
    /// Number of scheduled ticks after startup.
    pub ticks: i64,
    /// Number of runtime API trigger ticks.
    pub triggered_ticks: i64,
    /// Newly queued daily runs across startup and scheduled ticks.
    pub queued_daily_runs: u64,
    /// Number of taskman jobs assigned.
    pub assigned_jobs: i64,
    /// Daily-run enqueue errors.
    pub store_errors: i64,
}

/// Result of one memory-consolidation taskman worker tick.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryConsolidationQueueWorkerReport {
    /// Whether a taskman job was dequeued.
    pub dequeued: bool,
    /// Dequeued taskman job ID.
    pub job_id: Option<i64>,
    /// Whether a DB memory run was claimed and processed.
    pub processed_run: bool,
    /// The taskman job was finalized as completed.
    pub completed: bool,
    /// The taskman job was finalized as failed.
    pub failed: bool,
    /// A follow-up memory-consolidation job was assigned after a claimed run.
    pub scheduled_followup: bool,
    /// Follow-up taskman job ID.
    pub followup_job_id: Option<i64>,
    pub decode_error: Option<String>,
    /// Memory run processing failed.
    pub process_error: Option<String>,
    /// Completion/failure status write failed.
    pub status_error: Option<String>,
}

/// Aggregate report from memory-consolidation taskman worker.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MemoryConsolidationQueueWorkerRunReport {
    /// Number of poll ticks.
    pub ticks: i64,
    /// Number of ticks that dequeued a job.
    pub dequeued: i64,
    /// Number of jobs completed.
    pub completed: i64,
    /// Number of jobs failed.
    pub failed: i64,
    /// Number of follow-up taskman jobs assigned.
    pub scheduled_followups: i64,
    /// Number of status write errors.
    pub status_errors: i64,
}

/// Config for `process_next_memory_run`.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryRunProcessConfig {
    /// Lease owner string.
    pub owner: String,
    /// Claim lease duration.
    pub lease: Duration,
    /// Max messages per run before continuation.
    pub max_messages_per_run: i32,
    /// Max extraction input tokens before continuation.
    pub max_input_tokens: i32,
    /// Embedding dimension requested from the embedder.
    pub embedding_dimension: i32,
    /// Episode model stored with `memory_episodes`.
    pub episode_model: String,
}

/// Config for the app-level memory service worker.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryServiceWorkerConfig {
    /// Whether memory consolidation is enabled.
    pub enabled: bool,
    /// Retention window used when ensuring daily runs.
    pub retention: Duration,
    pub daily_schedule: String,
    pub daily_window: Duration,
    pub tick_interval: Duration,
    pub enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy,
    /// Per-run processing config.
    pub process: MemoryRunProcessConfig,
}

/// Runtime options for one memory-consolidation taskman worker.
#[derive(Clone, Debug, PartialEq)]
pub struct MemoryConsolidationQueueWorkerConfig {
    /// Per-run processing config.
    pub process: MemoryRunProcessConfig,
    pub worker_id: String,
    /// Queue poll interval.
    pub interval: Duration,
    /// Desired number of in-flight consolidation jobs. Each productive tick
    /// tops the queue up toward this, so a parallel worker fleet ramps up
    /// (1 → 2 → 4 → …) while runs remain and collapses to zero when they dry
    /// up. `1` reproduces the historical strictly-sequential chain.
    pub pipeline_target: usize,
}

/// Runtime API memory restart mutation adapter.
#[derive(Clone, Debug)]
pub struct RuntimeMemoryRestarter<Store> {
    store: Store,
    trigger: Arc<tokio::sync::Notify>,
    model: String,
}

impl<Store> RuntimeMemoryRestarter<Store> {
    /// Build a memory restarter over a retry store and worker trigger.
    pub fn new(store: Store, trigger: Arc<tokio::sync::Notify>, model: impl Into<String>) -> Self {
        Self {
            store,
            trigger,
            model: model.into(),
        }
    }
}

impl<Store> openplotva_server::RuntimeMemoryRestarter for RuntimeMemoryRestarter<Store>
where
    Store: MemoryRestartStore + Send + Sync + 'static,
    Store::Error: StdError + Send + Sync + 'static,
{
    fn restart<'a>(
        &'a self,
        run_id: Option<i64>,
    ) -> openplotva_server::RuntimeMemoryRestartFuture<'a> {
        Box::pin(async move {
            let retried_failed_runs = match run_id {
                Some(id) => {
                    self.store
                        .retry_run(id)
                        .await
                        .map_err(|error| error.to_string())?;
                    1
                }
                None => clamp_u64_to_i32(
                    self.store
                        .retry_failed_runs()
                        .await
                        .map_err(|error| error.to_string())?,
                ),
            };
            self.trigger.notify_one();
            Ok(openplotva_server::RuntimeMemoryRestartResultData {
                ok: true,
                run_id,
                retried_failed_runs,
                queued_runs: 0,
                started: true,
                override_: false,
                provider: Some("aifarm".to_owned()),
                model: Some(runtime_memory_restart_model(&self.model)),
            })
        })
    }
}

impl Default for MemoryServiceWorkerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            retention: Duration::from_secs(7 * 24 * 60 * 60),
            daily_schedule: "03:17".to_owned(),
            daily_window: Duration::from_secs(24 * 60 * 60),
            tick_interval: Duration::from_secs(60 * 60),
            enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy::default(),
            process: MemoryRunProcessConfig::default(),
        }
    }
}

impl Default for MemoryConsolidationQueueWorkerConfig {
    fn default() -> Self {
        Self {
            process: MemoryRunProcessConfig::default(),
            worker_id: format!("{MEMORY_CONSOLIDATION_JOB_WORKER_PREFIX}-0"),
            interval: MEMORY_CONSOLIDATION_JOB_POLL_INTERVAL,
            pipeline_target: 1,
        }
    }
}

impl MemoryServiceWorkerConfig {
    fn normalized(&self) -> Self {
        let fallback = Self::default();
        Self {
            enabled: self.enabled,
            retention: if self.retention.is_zero() {
                fallback.retention
            } else {
                self.retention
            },
            daily_schedule: if self.daily_schedule.trim().is_empty() {
                fallback.daily_schedule
            } else {
                self.daily_schedule.trim().to_owned()
            },
            daily_window: if self.daily_window.is_zero() {
                fallback.daily_window
            } else {
                self.daily_window
            },
            tick_interval: if self.tick_interval.is_zero() {
                fallback.tick_interval
            } else {
                self.tick_interval
            },
            enqueue_policy: self.enqueue_policy.normalized(),
            process: self.process.normalized(),
        }
    }
}

impl MemoryConsolidationQueueWorkerConfig {
    fn normalized(&self) -> Self {
        let fallback = Self::default();
        Self {
            process: self.process.normalized(),
            worker_id: if self.worker_id.trim().is_empty() {
                fallback.worker_id
            } else {
                self.worker_id.trim().to_owned()
            },
            interval: if self.interval.is_zero() {
                fallback.interval
            } else {
                self.interval
            },
            pipeline_target: self.pipeline_target.max(1),
        }
    }
}

impl Default for MemoryRunProcessConfig {
    fn default() -> Self {
        Self {
            owner: "openplotva-memory".to_owned(),
            lease: Duration::from_secs(10 * 60),
            max_messages_per_run: 200,
            max_input_tokens: MEMORY_MAX_EXTRACTION_BATCH_INPUT_TOKENS,
            embedding_dimension: 512,
            episode_model: openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL.to_owned(),
        }
    }
}

impl MemoryRunProcessConfig {
    fn normalized(&self) -> Self {
        Self {
            owner: if self.owner.trim().is_empty() {
                "openplotva-memory".to_owned()
            } else {
                self.owner.trim().to_owned()
            },
            lease: if self.lease.is_zero() {
                Duration::from_secs(10 * 60)
            } else {
                self.lease
            },
            max_messages_per_run: if self.max_messages_per_run <= 0 {
                200
            } else {
                self.max_messages_per_run
            },
            max_input_tokens: clamp_memory_max_input_tokens(self.max_input_tokens),
            embedding_dimension: if self.embedding_dimension <= 0 {
                512
            } else {
                self.embedding_dimension
            },
            episode_model: if self.episode_model.trim().is_empty() {
                openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL.to_owned()
            } else {
                self.episode_model.trim().to_owned()
            },
        }
    }
}

/// Memory extraction write failure.
#[derive(Debug, Error)]
pub enum MemoryExtractionWriteError<ExtractorError, StoreError>
where
    ExtractorError: StdError + Send + Sync + 'static,
    StoreError: StdError + Send + Sync + 'static,
{
    /// Extractor failed.
    #[error("extract memory batch: {source}")]
    Extract {
        /// Source extractor error.
        #[source]
        source: ExtractorError,
    },
    /// Card write failed.
    #[error("write memory cards: {source}")]
    StoreCards {
        /// Source store error.
        #[source]
        source: StoreError,
    },
    /// Episode write failed.
    #[error("write memory episode: {source}")]
    StoreEpisode {
        /// Source store error.
        #[source]
        source: StoreError,
    },
    /// Embedder became unavailable mid-run; abort and keep the run for retry.
    #[error("embedder unavailable during consolidation: {source}")]
    EmbedderUnavailable {
        /// Source embedder error.
        #[source]
        source: EmbedderClientError,
    },
}

impl<ExtractorError, StoreError> MemoryExtractionWriteError<ExtractorError, StoreError>
where
    ExtractorError: StdError + Send + Sync + 'static,
    StoreError: StdError + Send + Sync + 'static,
{
    /// Whether the run failed because the embedder was unavailable, meaning it
    /// should be released back to the queue rather than counted as a failure.
    #[must_use]
    pub fn is_embedder_unavailable(&self) -> bool {
        matches!(self, Self::EmbedderUnavailable { .. })
    }
}

/// Memory run processing failure.
#[derive(Debug, Error)]
pub enum MemoryRunProcessError<ExtractorError, StoreError>
where
    ExtractorError: StdError + Send + Sync + 'static,
    StoreError: StdError + Send + Sync + 'static,
{
    /// Claim/load/continuation/complete/fail store call failed.
    #[error("memory run store operation failed: {source}")]
    Store {
        /// Source store error.
        #[source]
        source: StoreError,
    },
    /// Extraction or write failed after a run was claimed.
    #[error("process memory run {run_id}: {source}")]
    Process {
        /// Claimed run ID.
        run_id: i64,
        /// Source extraction write error.
        #[source]
        source: MemoryExtractionWriteError<ExtractorError, StoreError>,
    },
}

pub async fn execute_memory_extraction_batch<Extractor, Store, Embedder>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    input: &ExtractInput,
    cfg: MemoryExtractionBatchConfig<'_>,
) -> Result<MemoryExtractionWriteReport, MemoryExtractionWriteError<Extractor::Error, Store::Error>>
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryWriteStore,
    Embedder: EmbeddingProvider,
{
    let output = extractor
        .extract(input)
        .await
        .map_err(|source| MemoryExtractionWriteError::Extract { source })?;
    write_memory_extraction_batch_inner(store, embedder, input, output, cfg).await
}

/// Claim and process one queued memory run.
pub async fn process_next_memory_run<Extractor, Store, Embedder>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: MemoryRunProcessConfig,
) -> Result<MemoryRunProcessReport, MemoryRunProcessError<Extractor::Error, Store::Error>>
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
{
    let cfg = cfg.normalized();
    // Gate: never start a consolidation run while the embedder is down. The run
    // stays queued (no claim, no attempt consumed) and the cheap breaker check
    // keeps AI Farm untouched until the embedder recovers.
    if embedder.is_some_and(|provider| !provider.is_available()) {
        return Ok(MemoryRunProcessReport::default());
    }
    let Some(run) = store
        .claim_run(&cfg.owner, cfg.lease)
        .await
        .map_err(|source| MemoryRunProcessError::Store { source })?
    else {
        return Ok(MemoryRunProcessReport::default());
    };
    let run_id = run.id;
    let result = process_claimed_memory_run(extractor, store, embedder, &run, &cfg).await;
    match result {
        Ok(report) => Ok(report),
        // Embedder died mid-run: abort without burning an attempt and leave the
        // run queued for retry once the embedder is back.
        Err(error) if error.is_embedder_unavailable() => {
            tracing::warn!(
                run_id,
                error = %error,
                "embedder unavailable mid-run; releasing memory run for retry"
            );
            store
                .release_run(run_id)
                .await
                .map_err(|source| MemoryRunProcessError::Store { source })?;
            Ok(MemoryRunProcessReport::default())
        }
        Err(error) => {
            let text = error.to_string();
            store
                .fail_run(run_id, &text)
                .await
                .map_err(|source| MemoryRunProcessError::Store { source })?;
            Err(MemoryRunProcessError::Process {
                run_id,
                source: error,
            })
        }
    }
}

pub async fn run_memory_service_worker_until<Extractor, Store, Embedder, Stop>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: MemoryServiceWorkerConfig,
    stop: Stop,
) -> MemoryServiceWorkerReport
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
    Stop: Future<Output = ()>,
{
    run_memory_service_worker_with_trigger_until(
        extractor,
        store,
        embedder,
        cfg,
        Arc::new(tokio::sync::Notify::new()),
        stop,
    )
    .await
}

pub async fn run_memory_service_worker_with_trigger_until<Extractor, Store, Embedder, Stop>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: MemoryServiceWorkerConfig,
    trigger: Arc<tokio::sync::Notify>,
    stop: Stop,
) -> MemoryServiceWorkerReport
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
    Stop: Future<Output = ()>,
{
    let cfg = cfg.normalized();
    let mut report = MemoryServiceWorkerReport {
        enabled: cfg.enabled,
        ..MemoryServiceWorkerReport::default()
    };
    if !cfg.enabled {
        return report;
    }

    let startup = run_memory_service_once_at(
        extractor,
        store,
        embedder,
        &cfg,
        OffsetDateTime::now_utc(),
        true,
    )
    .await;
    report.add_tick(&startup);

    let stop = stop;
    tokio::pin!(stop);
    loop {
        let sleep = tokio::time::sleep(cfg.tick_interval);
        tokio::pin!(sleep);
        let trigger_notified = trigger.notified();
        tokio::pin!(trigger_notified);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {
                let tick = run_memory_service_once_at(
                    extractor,
                    store,
                    embedder,
                    &cfg,
                    OffsetDateTime::now_utc(),
                    false,
                )
                .await;
                report.ticks += 1;
                report.add_tick(&tick);
            }
            () = &mut trigger_notified => {
                let tick = run_memory_service_once_at_with_daily(
                    extractor,
                    store,
                    embedder,
                    &cfg,
                    OffsetDateTime::now_utc(),
                    false,
                    false,
                )
                .await;
                report.triggered_ticks += 1;
                report.add_tick(&tick);
            }
        }
    }

    report
}

pub async fn run_memory_service_once_at<Extractor, Store, Embedder>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: &MemoryServiceWorkerConfig,
    now: OffsetDateTime,
    startup: bool,
) -> MemoryServiceTickReport
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
{
    run_memory_service_once_at_with_daily(extractor, store, embedder, cfg, now, startup, true).await
}

async fn run_memory_service_once_at_with_daily<Extractor, Store, Embedder>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: &MemoryServiceWorkerConfig,
    now: OffsetDateTime,
    startup: bool,
    ensure_daily: bool,
) -> MemoryServiceTickReport
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
{
    let cfg = cfg.normalized();
    let mut report = MemoryServiceTickReport {
        enabled: cfg.enabled,
        startup,
        daily_window_active: memory_schedule_active_at(now, &cfg.daily_schedule, cfg.daily_window),
        ..MemoryServiceTickReport::default()
    };
    if !cfg.enabled {
        return report;
    }

    if ensure_daily && (startup || report.daily_window_active) {
        match store
            .ensure_daily_runs(now, cfg.retention, cfg.enqueue_policy)
            .await
        {
            Ok(queued) => report.queued_daily_runs = queued,
            Err(error) => {
                report.store_errors += 1;
                tracing::warn!(%error, "memory run enqueue failed");
            }
        }
    }

    loop {
        match process_next_memory_run(extractor, store, embedder, cfg.process.clone()).await {
            Ok(processed) if processed.processed => {
                report.processed_runs += 1;
                tracing::info!(?processed, "memory run processed");
            }
            Ok(_) => break,
            Err(MemoryRunProcessError::Process { run_id, source }) => {
                report.failed_runs += 1;
                tracing::warn!(%source, run_id, "memory run failed");
            }
            Err(MemoryRunProcessError::Store { source }) => {
                report.store_errors += 1;
                tracing::warn!(%source, "memory run store operation failed");
                break;
            }
        }
    }

    report
}

pub async fn schedule_memory_consolidation_taskman_once_at<Store>(
    store: &Store,
    queue: &InMemoryTaskQueue,
    cfg: &MemoryServiceWorkerConfig,
    now: OffsetDateTime,
    startup: bool,
    ensure_daily: bool,
) -> MemoryConsolidationQueueScheduleReport
where
    Store: MemoryRunStore,
{
    let cfg = cfg.normalized();
    let mut report = MemoryConsolidationQueueScheduleReport {
        enabled: cfg.enabled,
        startup,
        daily_window_active: memory_schedule_active_at(now, &cfg.daily_schedule, cfg.daily_window),
        ..MemoryConsolidationQueueScheduleReport::default()
    };
    if !cfg.enabled {
        return report;
    }

    if ensure_daily && (startup || report.daily_window_active) {
        match store
            .ensure_daily_runs(now, cfg.retention, cfg.enqueue_policy)
            .await
        {
            Ok(queued) => report.queued_daily_runs = queued,
            Err(error) => {
                report.store_error = Some(error.to_string());
                tracing::warn!(%error, "memory run enqueue failed");
            }
        }
    }

    if queue.queue_depth(
        MEMORY_CONSOLIDATION_QUEUE_NAME,
        openplotva_taskman::LOWEST_PRIORITY,
    ) > 0
    {
        return report;
    }

    let job_id = queue.assign(
        MEMORY_CONSOLIDATION_QUEUE_NAME,
        new_memory_consolidation_job_at(now),
    );
    report.assigned_job_id = Some(job_id);
    report
}

pub async fn run_memory_consolidation_taskman_scheduler_with_trigger_until<Store, Stop>(
    store: &Store,
    queue: &InMemoryTaskQueue,
    cfg: MemoryServiceWorkerConfig,
    trigger: Arc<tokio::sync::Notify>,
    stop: Stop,
) -> MemoryConsolidationQueueSchedulerReport
where
    Store: MemoryRunStore,
    Stop: Future<Output = ()>,
{
    let cfg = cfg.normalized();
    let mut report = MemoryConsolidationQueueSchedulerReport {
        enabled: cfg.enabled,
        ..MemoryConsolidationQueueSchedulerReport::default()
    };
    if !cfg.enabled {
        return report;
    }

    let startup = schedule_memory_consolidation_taskman_once_at(
        store,
        queue,
        &cfg,
        OffsetDateTime::now_utc(),
        true,
        true,
    )
    .await;
    report.add_schedule_tick(&startup);

    let stop = stop;
    tokio::pin!(stop);
    loop {
        let sleep = tokio::time::sleep(cfg.tick_interval);
        tokio::pin!(sleep);
        let trigger_notified = trigger.notified();
        tokio::pin!(trigger_notified);
        tokio::select! {
            () = &mut stop => break,
            () = &mut sleep => {
                let tick = schedule_memory_consolidation_taskman_once_at(
                    store,
                    queue,
                    &cfg,
                    OffsetDateTime::now_utc(),
                    false,
                    true,
                )
                .await;
                report.ticks += 1;
                report.add_schedule_tick(&tick);
            }
            () = &mut trigger_notified => {
                let tick = schedule_memory_consolidation_taskman_once_at(
                    store,
                    queue,
                    &cfg,
                    OffsetDateTime::now_utc(),
                    false,
                    false,
                )
                .await;
                report.triggered_ticks += 1;
                report.add_schedule_tick(&tick);
            }
        }
    }

    report
}

pub async fn process_memory_consolidation_taskman_job_once_at<Extractor, Store, Embedder>(
    queue: &InMemoryTaskQueue,
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: MemoryRunProcessConfig,
    worker_id: &str,
    pipeline_target: usize,
    now: OffsetDateTime,
) -> MemoryConsolidationQueueWorkerReport
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
{
    let mut report = MemoryConsolidationQueueWorkerReport::default();
    let Some(work) = queue.dequeue(MEMORY_CONSOLIDATION_QUEUE_NAME, worker_id, now) else {
        return report;
    };
    report.dequeued = true;
    report.job_id = Some(work.id);

    if work.job.data.job_type != JobType::MemoryConsolidation {
        let error = format!(
            "invalid job type for MemoryConsolidationJobExecutor: {}",
            taskman_job_type_string(work.job.data.job_type)
        );
        report.decode_error = Some(error.clone());
        mark_memory_consolidation_taskman_job_failed(queue, work.id, &error, now, &mut report);
        return report;
    }

    match process_next_memory_run(extractor, store, embedder, cfg).await {
        Ok(processed) => {
            report.processed_run = processed.processed;
            match queue.complete(work.id, now) {
                Ok(()) => report.completed = true,
                Err(error) => report.status_error = Some(error.to_string()),
            }
            if processed.processed {
                let followup = queue.assign(
                    MEMORY_CONSOLIDATION_QUEUE_NAME,
                    new_memory_consolidation_job_at(now),
                );
                report.scheduled_followup = true;
                report.followup_job_id = Some(followup);
                // Pipeline amplification: while runs keep coming, each
                // productive tick adds one extra job until enough are in
                // flight to feed every parallel worker. Unproductive ticks
                // schedule nothing, so the pipeline drains itself dry.
                if pipeline_target > 1 {
                    let depth = queue.queue_depth(
                        MEMORY_CONSOLIDATION_QUEUE_NAME,
                        openplotva_taskman::LOWEST_PRIORITY,
                    );
                    if depth < pipeline_target {
                        queue.assign(
                            MEMORY_CONSOLIDATION_QUEUE_NAME,
                            new_memory_consolidation_job_at(now),
                        );
                    }
                }
            }
        }
        Err(MemoryRunProcessError::Process { source, .. }) => {
            let error = source.to_string();
            report.process_error = Some(error.clone());
            mark_memory_consolidation_taskman_job_failed(queue, work.id, &error, now, &mut report);
            let followup = queue.assign(
                MEMORY_CONSOLIDATION_QUEUE_NAME,
                new_memory_consolidation_job_at(now),
            );
            report.scheduled_followup = true;
            report.followup_job_id = Some(followup);
        }
        Err(MemoryRunProcessError::Store { source }) => {
            let error = source.to_string();
            report.process_error = Some(error.clone());
            mark_memory_consolidation_taskman_job_failed(queue, work.id, &error, now, &mut report);
        }
    }

    report
}

pub async fn run_memory_consolidation_taskman_worker_until<Extractor, Store, Embedder, Stop>(
    queue: &InMemoryTaskQueue,
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    cfg: MemoryConsolidationQueueWorkerConfig,
    stop: Stop,
) -> MemoryConsolidationQueueWorkerRunReport
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
    Stop: Future<Output = ()>,
{
    let mut report = MemoryConsolidationQueueWorkerRunReport::default();
    let cfg = cfg.normalized();
    let mut stop = std::pin::pin!(stop);

    loop {
        tokio::select! {
            () = &mut stop => break,
            () = tokio::time::sleep(cfg.interval) => {
                let tick = process_memory_consolidation_taskman_job_once_at(
                    queue,
                    extractor,
                    store,
                    embedder,
                    cfg.process.clone(),
                    &cfg.worker_id,
                    cfg.pipeline_target,
                    OffsetDateTime::now_utc(),
                ).await;
                tracing::debug!(?tick, "memory-consolidation taskman worker tick");
                report.record_worker_tick(&tick);
            }
        }
    }

    report
}

impl MemoryServiceWorkerReport {
    fn add_tick(&mut self, tick: &MemoryServiceTickReport) {
        self.queued_daily_runs += tick.queued_daily_runs;
        self.processed_runs += tick.processed_runs;
        self.failed_runs += tick.failed_runs;
        self.store_errors += tick.store_errors;
    }
}

impl MemoryConsolidationQueueSchedulerReport {
    fn add_schedule_tick(&mut self, tick: &MemoryConsolidationQueueScheduleReport) {
        self.queued_daily_runs += tick.queued_daily_runs;
        if tick.assigned_job_id.is_some() {
            self.assigned_jobs += 1;
        }
        if tick.store_error.is_some() {
            self.store_errors += 1;
        }
    }
}

impl MemoryConsolidationQueueWorkerRunReport {
    fn record_worker_tick(&mut self, tick: &MemoryConsolidationQueueWorkerReport) {
        self.ticks += 1;
        if tick.dequeued {
            self.dequeued += 1;
        }
        if tick.completed {
            self.completed += 1;
        }
        if tick.failed {
            self.failed += 1;
        }
        if tick.scheduled_followup {
            self.scheduled_followups += 1;
        }
        if tick.status_error.is_some() {
            self.status_errors += 1;
        }
    }
}

fn mark_memory_consolidation_taskman_job_failed(
    queue: &InMemoryTaskQueue,
    job_id: i64,
    error: &str,
    now: OffsetDateTime,
    report: &mut MemoryConsolidationQueueWorkerReport,
) {
    match queue.fail(job_id, error.to_owned(), now) {
        Ok(()) => report.failed = true,
        Err(status_error) => report.status_error = Some(status_error.to_string()),
    }
}

fn taskman_job_type_string(job_type: JobType) -> &'static str {
    match job_type {
        JobType::Dialog => "dialog",
        JobType::ImageGen => "image_gen",
        JobType::ImageEdit => "image_edit",
        JobType::MusicGen => "music_gen",
        JobType::Translation => "translation",
        JobType::MemoryConsolidation => "memory_consolidation",
        JobType::Control => "control",
        JobType::Agent => "agent",
    }
}

async fn process_claimed_memory_run<Extractor, Store, Embedder>(
    extractor: &Extractor,
    store: &Store,
    embedder: Option<&Embedder>,
    run: &openplotva_memory::Run,
    cfg: &MemoryRunProcessConfig,
) -> Result<MemoryRunProcessReport, MemoryExtractionWriteError<Extractor::Error, Store::Error>>
where
    Extractor: MemoryExtractor + Send + Sync,
    Extractor::Error: StdError + Send + Sync + 'static,
    Store: MemoryRunStore,
    Embedder: EmbeddingProvider,
{
    let loaded = store
        .load_run_messages(run, cfg.max_messages_per_run)
        .await
        .map_err(|source| MemoryExtractionWriteError::StoreCards { source })?;
    let mut report = MemoryRunProcessReport {
        processed: true,
        run_id: run.id,
        loaded_messages: loaded.len(),
        ..MemoryRunProcessReport::default()
    };
    if loaded.is_empty() {
        store
            .complete_run(run.id, RunStats::default())
            .await
            .map_err(|source| MemoryExtractionWriteError::StoreEpisode { source })?;
        return Ok(report);
    }

    let chat = store
        .load_run_chat_meta(run.chat_id)
        .await
        .unwrap_or_default()
        .unwrap_or_default();
    let selection = select_run_input(store, run, &chat, &loaded, cfg).await;
    report.selected_messages = selection.messages.len();
    if let Some(after) = selection.continuation.as_ref() {
        store
            .enqueue_run_continuation(
                run,
                after,
                run.message_count
                    .saturating_sub(selection.messages.len() as i32),
            )
            .await
            .map_err(|source| MemoryExtractionWriteError::StoreCards { source })?;
        report.continuation_queued = true;
    }

    let filtered = openplotva_memory::filter_consolidation_messages(&selection.messages);
    report.filtered_messages = filtered.len();
    if filtered.is_empty() {
        store
            .complete_run(run.id, RunStats::default())
            .await
            .map_err(|source| MemoryExtractionWriteError::StoreEpisode { source })?;
        return Ok(report);
    }

    let batches = split_extraction_batches(store, run, &chat, &filtered, cfg).await;
    let mut state = RunExtractionState::with_batch_count(batches.len());
    let fallback_observed_at = OffsetDateTime::now_utc();
    for batch in &batches {
        let input = extraction_input_for_batch(run, &chat, batch);
        let output = extractor
            .extract(&input)
            .await
            .map_err(|source| MemoryExtractionWriteError::Extract { source })?;
        state.add_extraction_metadata(batch, &output);
        let card_report = write_memory_extraction_cards(
            store,
            embedder,
            &input,
            &output,
            MemoryExtractionBatchConfig {
                episode_model: &cfg.episode_model,
                prompt_version: run.prompt_version.trim(),
                embedding_dimension: cfg.embedding_dimension,
                fallback_observed_at,
                batch_input_tokens: batch.input_tokens,
            },
        )
        .await?;
        state.add_card_report(card_report);
    }
    state.finalize_output_metadata();
    if state.card_input_count > 0 {
        let episode = episode_from_run(run, &state.report.output, filtered.len());
        insert_memory_episode(
            store,
            embedder,
            episode,
            MemoryExtractionBatchConfig {
                episode_model: &cfg.episode_model,
                prompt_version: run.prompt_version.trim(),
                embedding_dimension: cfg.embedding_dimension,
                fallback_observed_at,
                batch_input_tokens: state.report.stats.input_tokens,
            },
            &mut state.report,
        )
        .await?;
    }
    let write = state.report;
    let stats = write.stats.clone();
    report.write = Some(write);
    store
        .complete_run(run.id, stats)
        .await
        .map_err(|source| MemoryExtractionWriteError::StoreEpisode { source })?;
    Ok(report)
}

#[derive(Clone, Debug, Default, PartialEq)]
struct RunInputSelection {
    messages: Vec<openplotva_memory::Message>,
    continuation: Option<openplotva_memory::Message>,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct ExtractionBatch {
    messages: Vec<openplotva_memory::Message>,
    existing_cards: Vec<Card>,
    input_tokens: i32,
}

#[derive(Debug, Default)]
struct RunExtractionState {
    report: MemoryExtractionWriteReport,
    card_input_count: usize,
    episode_summary_parts: Vec<String>,
    topics: Vec<String>,
    participants: Vec<String>,
}

impl RunExtractionState {
    fn with_batch_count(batch_count: usize) -> Self {
        Self {
            episode_summary_parts: Vec::with_capacity(batch_count),
            ..Self::default()
        }
    }

    fn add_extraction_metadata(&mut self, batch: &ExtractionBatch, output: &ExtractOutput) {
        self.report.stats.input_tokens += if output.input_tokens == 0 {
            batch.input_tokens
        } else {
            output.input_tokens
        };
        self.report.stats.output_tokens += output.output_tokens;
        let summary = output.episode_summary.trim();
        if !summary.is_empty() {
            self.episode_summary_parts.push(summary.to_owned());
        }
        self.topics.extend(output.topics.iter().cloned());
        self.participants
            .extend(output.participants.iter().cloned());
        self.report
            .output
            .candidate_cards
            .extend(output.candidate_cards.iter().cloned());
        self.report
            .output
            .supersessions
            .extend(output.supersessions.iter().cloned());
        self.report
            .output
            .links
            .extend(output.links.iter().cloned());
    }

    fn add_card_report(&mut self, card_report: CardExtractionWriteReport) {
        self.card_input_count += card_report.card_input_count;
        merge_card_report(&mut self.report, card_report);
    }

    fn finalize_output_metadata(&mut self) {
        self.report.output.episode_summary = self.episode_summary_parts.join("\n\n");
        self.report.output.topics = unique_normalized_strings(&self.topics);
        self.report.output.participants = unique_normalized_strings(&self.participants);
        self.report.output.input_tokens = self.report.stats.input_tokens;
        self.report.output.output_tokens = self.report.stats.output_tokens;
    }
}

async fn select_run_input<Store>(
    store: &Store,
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    loaded: &[openplotva_memory::Message],
    cfg: &MemoryRunProcessConfig,
) -> RunInputSelection
where
    Store: MemoryRunStore,
{
    if loaded.is_empty() {
        return RunInputSelection::default();
    }
    let candidate_limit = usize::try_from(cfg.max_messages_per_run)
        .unwrap_or(200)
        .min(loaded.len());
    let candidates = &loaded[..candidate_limit];
    if candidates.is_empty() {
        return RunInputSelection::default();
    }

    let existing = existing_cards_for_run(store, run, chat, candidates).await;
    let best_count = best_run_input_count(run, chat, candidates, &existing, cfg.max_input_tokens);
    let selected = candidates[..best_count].to_vec();
    let continuation = (best_count < loaded.len())
        .then(|| selected.last().cloned())
        .flatten();

    RunInputSelection {
        messages: selected,
        continuation,
    }
}

async fn split_extraction_batches<Store>(
    store: &Store,
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    messages: &[openplotva_memory::Message],
    cfg: &MemoryRunProcessConfig,
) -> Vec<ExtractionBatch>
where
    Store: MemoryRunStore,
{
    if messages.is_empty() {
        return Vec::new();
    }
    let limit = cfg.max_input_tokens;
    let mut out = Vec::new();
    let mut current = Vec::with_capacity(messages.len());
    let mut current_batch = None;

    for message in messages {
        if !current.is_empty() {
            let mut candidate = current.clone();
            candidate.push(message.clone());
            let candidate_batch =
                build_extraction_batch(store, run, chat, &candidate, None, limit).await;
            if candidate_batch.input_tokens <= limit {
                current.push(message.clone());
                current_batch = Some(candidate_batch);
                continue;
            }
            flush_extraction_batch(
                store,
                run,
                chat,
                &mut out,
                &mut current,
                &mut current_batch,
                limit,
            )
            .await;
        }

        let single = vec![message.clone()];
        let existing = existing_cards_for_run(store, run, chat, &single).await;
        let single_batch =
            build_extraction_batch(store, run, chat, &single, Some(existing.clone()), limit).await;
        if single_batch.input_tokens > limit {
            out.extend(split_oversized_message(store, run, chat, message, &existing, limit).await);
            continue;
        }
        current.push(message.clone());
        current_batch = Some(single_batch);
    }

    flush_extraction_batch(
        store,
        run,
        chat,
        &mut out,
        &mut current,
        &mut current_batch,
        limit,
    )
    .await;
    out
}

async fn flush_extraction_batch<Store>(
    store: &Store,
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    out: &mut Vec<ExtractionBatch>,
    current: &mut Vec<openplotva_memory::Message>,
    current_batch: &mut Option<ExtractionBatch>,
    limit: i32,
) where
    Store: MemoryRunStore,
{
    if current.is_empty() {
        return;
    }
    let batch = if let Some(batch) = current_batch.take() {
        batch
    } else {
        build_extraction_batch(store, run, chat, current, None, limit).await
    };
    out.push(batch);
    current.clear();
}

async fn build_extraction_batch<Store>(
    store: &Store,
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    messages: &[openplotva_memory::Message],
    existing: Option<Vec<Card>>,
    limit: i32,
) -> ExtractionBatch
where
    Store: MemoryRunStore,
{
    let batch_messages = messages.to_vec();
    let mut existing_cards = match existing {
        Some(cards) => cards,
        None => existing_cards_for_run(store, run, chat, &batch_messages).await,
    };
    let mut input_tokens = estimate_extraction_tokens(run, chat, &batch_messages, &existing_cards);
    if limit > 0 && input_tokens > limit && !existing_cards.is_empty() {
        let mut best_count = 0;
        let mut best_tokens = estimate_extraction_tokens(run, chat, &batch_messages, &[]);
        let mut low = 1;
        let mut high = existing_cards.len();
        while low <= high {
            let mid = low + (high - low) / 2;
            let tokens =
                estimate_extraction_tokens(run, chat, &batch_messages, &existing_cards[..mid]);
            if tokens <= limit {
                best_count = mid;
                best_tokens = tokens;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        existing_cards.truncate(best_count);
        input_tokens = best_tokens;
    }
    ExtractionBatch {
        messages: batch_messages,
        existing_cards,
        input_tokens,
    }
}

async fn split_oversized_message<Store>(
    store: &Store,
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    message: &openplotva_memory::Message,
    existing: &[Card],
    limit: i32,
) -> Vec<ExtractionBatch>
where
    Store: MemoryRunStore,
{
    let text = message.text.trim();
    if text.is_empty() {
        return vec![
            build_extraction_batch(
                store,
                run,
                chat,
                std::slice::from_ref(message),
                Some(existing.to_vec()),
                limit,
            )
            .await,
        ];
    }

    let mut runes: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    while !runes.is_empty() {
        let mut low = 1;
        let mut high = runes.len();
        let mut best = 1;
        while low <= high {
            let mid = low + (high - low) / 2;
            let mut chunk = message.clone();
            chunk.text = runes[..mid].iter().collect::<String>();
            let tokens = estimate_extraction_tokens(run, chat, std::slice::from_ref(&chunk), &[]);
            if tokens <= limit || mid == 1 {
                best = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        let split_at = message_split_boundary(&runes, best);
        let mut chunk = message.clone();
        chunk.text = runes[..split_at]
            .iter()
            .collect::<String>()
            .trim()
            .to_owned();
        if !chunk.text.is_empty() {
            out.push(
                build_extraction_batch(
                    store,
                    run,
                    chat,
                    std::slice::from_ref(&chunk),
                    Some(existing.to_vec()),
                    limit,
                )
                .await,
            );
        }
        runes = trim_leading_space_chars(&runes[split_at..]);
    }
    out
}

fn message_split_boundary(runes: &[char], limit: usize) -> usize {
    if limit >= runes.len() {
        return runes.len();
    }
    if limit <= 1 {
        return 1;
    }
    let min_boundary = (limit / 2).max(1);
    for index in (min_boundary + 1..=limit).rev() {
        if runes[index - 1].is_whitespace() {
            return index;
        }
    }
    limit
}

fn trim_leading_space_chars(runes: &[char]) -> Vec<char> {
    let mut index = 0;
    while index < runes.len() && runes[index].is_whitespace() {
        index += 1;
    }
    runes[index..].to_vec()
}

fn extraction_input_for_batch(
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    batch: &ExtractionBatch,
) -> ExtractInput {
    ExtractInput {
        run: run.clone(),
        chat_type: chat.chat_type.clone(),
        chat_username: chat.username.clone(),
        chat_active_usernames: chat.active_usernames.clone(),
        messages: batch.messages.clone(),
        existing_cards: batch.existing_cards.clone(),
    }
}

fn best_run_input_count(
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    candidates: &[openplotva_memory::Message],
    existing: &[Card],
    max_input_tokens: i32,
) -> usize {
    let estimated = estimate_extraction_tokens(run, chat, candidates, existing);
    if max_input_tokens <= 0 || estimated <= max_input_tokens || candidates.len() <= 1 {
        return candidates.len();
    }
    let mut low = 1;
    let mut high = candidates.len();
    let mut best = 1;
    while low <= high {
        let mid = low + (high - low) / 2;
        let tokens = estimate_extraction_tokens(run, chat, &candidates[..mid], existing);
        if tokens <= max_input_tokens || mid == 1 {
            best = mid;
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    best
}

async fn existing_cards_for_run<Store>(
    store: &Store,
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    messages: &[openplotva_memory::Message],
) -> Vec<Card>
where
    Store: MemoryRunStore,
{
    let mut out = Vec::new();
    let mut seen = Vec::new();
    let scope = retrieval_scope_for_run(run, chat, 0);
    if let Ok(cards) = store.list_visible_cards(&scope, EXISTING_CARDS_LIMIT).await {
        add_existing_cards(&mut out, &mut seen, cards, EXISTING_CARDS_LIMIT as usize);
    }
    for user_id in participant_user_ids(messages, EXISTING_PARTICIPANT_CARDS_MAX_USER) {
        let scope = retrieval_scope_for_run(run, chat, user_id);
        if let Ok(cards) = store
            .list_visible_cards(&scope, EXISTING_USER_CARDS_LIMIT)
            .await
        {
            add_existing_cards(&mut out, &mut seen, cards, EXISTING_CARDS_LIMIT as usize);
        }
        if out.len() >= EXISTING_CARDS_LIMIT as usize {
            break;
        }
    }
    out
}

fn add_existing_cards(out: &mut Vec<Card>, seen: &mut Vec<i64>, cards: Vec<Card>, limit: usize) {
    for card in cards {
        if out.len() >= limit {
            break;
        }
        if card.id != 0 {
            if seen.contains(&card.id) {
                continue;
            }
            seen.push(card.id);
        }
        out.push(card);
    }
}

fn retrieval_scope_for_run(
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    user_id: i64,
) -> openplotva_memory::RetrievalScope {
    openplotva_memory::RetrievalScope {
        chat_id: run.chat_id,
        thread_id: run.thread_id,
        user_id,
        chat_type: chat.chat_type.clone(),
        username: chat.username.clone(),
        active_usernames: chat.active_usernames.clone(),
    }
}

fn participant_user_ids(messages: &[openplotva_memory::Message], max_users: usize) -> Vec<i64> {
    if max_users == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    for message in messages {
        if message.user_id == 0 || out.contains(&message.user_id) {
            continue;
        }
        out.push(message.user_id);
        if out.len() >= max_users {
            break;
        }
    }
    out
}

fn estimate_extraction_tokens(
    run: &openplotva_memory::Run,
    chat: &DialogMemoryChatMeta,
    messages: &[openplotva_memory::Message],
    existing_cards: &[Card],
) -> i32 {
    openplotva_memory::estimate_extraction_input_with(
        &ExtractInput {
            run: run.clone(),
            chat_type: chat.chat_type.clone(),
            chat_username: chat.username.clone(),
            chat_active_usernames: chat.active_usernames.clone(),
            messages: messages.to_vec(),
            existing_cards: existing_cards.to_vec(),
        },
        "",
        openplotva_memory::estimate_memory_tokens,
    )
}

#[must_use]
pub fn memory_schedule_due_at(now: OffsetDateTime, schedule: &str) -> bool {
    let (hour, minute) = parse_daily_schedule(schedule);
    now.hour() > hour || (now.hour() == hour && now.minute() >= minute)
}

#[must_use]
pub fn memory_schedule_active_at(now: OffsetDateTime, schedule: &str, window: Duration) -> bool {
    if window.is_zero() || window >= Duration::from_secs(24 * 60 * 60) {
        return memory_schedule_due_at(now, schedule);
    }
    let (hour, minute) = parse_daily_schedule(schedule);
    let start_time = Time::from_hms(hour, minute, 0).unwrap_or(Time::MIDNIGHT);
    let mut start = now.replace_time(start_time);
    if now < start {
        start -= time::Duration::days(1);
    }
    let window = time::Duration::try_from(window).unwrap_or(time::Duration::MAX);
    now >= start && now < start + window
}

fn parse_daily_schedule(schedule: &str) -> (u8, u8) {
    let Some((hour, minute)) = schedule.trim().split_once(':') else {
        return (3, 17);
    };
    if minute.contains(':') {
        return (3, 17);
    }
    let Ok(hour) = hour.parse::<u8>() else {
        return (3, 17);
    };
    let Ok(minute) = minute.parse::<u8>() else {
        return (3, 17);
    };
    if hour > 23 || minute > 59 {
        return (3, 17);
    }
    (hour, minute)
}

/// Config for one extracted memory batch write.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemoryExtractionBatchConfig<'a> {
    /// Episode model stored with `memory_episodes`.
    pub episode_model: &'a str,
    /// Prompt version stored with `memory_episodes`.
    pub prompt_version: &'a str,
    /// Embedding dimension requested from the embedder.
    pub embedding_dimension: i32,
    /// Fallback observed timestamp for cards with zero message times.
    pub fallback_observed_at: OffsetDateTime,
    /// Batch input token estimate used when the extractor omits it.
    pub batch_input_tokens: i32,
}

/// Persist one already extracted memory batch.
pub async fn write_memory_extraction_batch<Store, Embedder>(
    store: &Store,
    embedder: Option<&Embedder>,
    input: &ExtractInput,
    output: ExtractOutput,
    cfg: MemoryExtractionBatchConfig<'_>,
) -> Result<
    MemoryExtractionWriteReport,
    MemoryExtractionWriteError<MemoryExtractorUnavailableForWrite, Store::Error>,
>
where
    Store: MemoryWriteStore,
    Embedder: EmbeddingProvider,
{
    write_memory_extraction_batch_inner(store, embedder, input, output, cfg).await
}

async fn write_memory_extraction_batch_inner<ExtractorError, Store, Embedder>(
    store: &Store,
    embedder: Option<&Embedder>,
    input: &ExtractInput,
    output: ExtractOutput,
    cfg: MemoryExtractionBatchConfig<'_>,
) -> Result<MemoryExtractionWriteReport, MemoryExtractionWriteError<ExtractorError, Store::Error>>
where
    ExtractorError: StdError + Send + Sync + 'static,
    Store: MemoryWriteStore,
    Embedder: EmbeddingProvider,
{
    let mut report = MemoryExtractionWriteReport {
        stats: RunStats {
            input_tokens: if output.input_tokens == 0 {
                cfg.batch_input_tokens
            } else {
                output.input_tokens
            },
            output_tokens: output.output_tokens,
            ..RunStats::default()
        },
        output,
        ..MemoryExtractionWriteReport::default()
    };
    let card_report =
        write_memory_extraction_cards(store, embedder, input, &report.output, cfg).await?;
    let card_input_count = card_report.card_input_count;
    merge_card_report(&mut report, card_report);
    if card_input_count == 0 {
        return Ok(report);
    }

    let episode = episode_from_extraction_batch(input, &report.output);
    insert_memory_episode(store, embedder, episode, cfg, &mut report).await?;

    Ok(report)
}

#[derive(Debug, Default)]
struct CardExtractionWriteReport {
    stats: RunStats,
    card_ids: Vec<i64>,
    links_inserted: usize,
    superseded: Vec<(i64, i64)>,
    card_input_count: usize,
    card_embedding_error: Option<String>,
    link_error: Option<String>,
    supersession_errors: Vec<String>,
    competing: Vec<(i64, i64)>,
    competing_errors: Vec<String>,
}

async fn write_memory_extraction_cards<ExtractorError, Store, Embedder>(
    store: &Store,
    embedder: Option<&Embedder>,
    input: &ExtractInput,
    output: &ExtractOutput,
    cfg: MemoryExtractionBatchConfig<'_>,
) -> Result<CardExtractionWriteReport, MemoryExtractionWriteError<ExtractorError, Store::Error>>
where
    ExtractorError: StdError + Send + Sync + 'static,
    Store: MemoryWriteStore,
    Embedder: EmbeddingProvider,
{
    let mut report = CardExtractionWriteReport::default();
    let cards =
        openplotva_memory::cards_from_extraction_at(input, output, cfg.fallback_observed_at);
    report.card_input_count = cards.len();
    if cards.is_empty() {
        return Ok(report);
    }

    let embeddings = match embedder {
        Some(provider) => {
            match embed_memory_cards(provider, &cards, cfg.embedding_dimension).await {
                Ok(embeddings) => embeddings,
                Err(error) if error.is_availability_failure() => {
                    return Err(MemoryExtractionWriteError::EmbedderUnavailable { source: error });
                }
                Err(error) => {
                    report.card_embedding_error = Some(error.to_string());
                    Vec::new()
                }
            }
        }
        None => Vec::new(),
    };
    let (card_stats, card_ids) = store
        .upsert_cards_with_embeddings(&cards, &embeddings)
        .await
        .map_err(|source| MemoryExtractionWriteError::StoreCards { source })?;
    report.stats.cards_inserted += card_stats.cards_inserted;
    report.stats.cards_updated += card_stats.cards_updated;
    report.card_ids = card_ids;

    let links = openplotva_memory::links_from_extraction(&cards, &report.card_ids, &output.links);
    report.links_inserted = links.len();
    if let Err(error) = store.insert_links(&links).await {
        report.link_error = Some(error.to_string());
    }

    // The model's scored resolutions split into outright supersessions (the new
    // fact invalidates the old) and competing pairs (both plausible, keep both).
    // Legacy `supersessions` are treated as supersede for backward compatibility.
    let mut supersede_inputs = output.supersessions.clone();
    let mut competing_inputs = Vec::new();
    for resolution in &output.resolutions {
        match resolution.decision {
            openplotva_memory::ResolutionDecision::Competing => {
                competing_inputs.push(resolution.as_supersession());
            }
            openplotva_memory::ResolutionDecision::Supersede => {
                supersede_inputs.push(resolution.as_supersession());
            }
        }
    }

    report.superseded = openplotva_memory::supersession_pairs_from_extraction(
        &cards,
        &report.card_ids,
        &supersede_inputs,
    );
    for (old_id, new_id) in &report.superseded {
        match store.supersede_card(*old_id, *new_id).await {
            Ok(()) => report.stats.cards_superseded += 1,
            Err(error) => report
                .supersession_errors
                .push(format!("{old_id}->{new_id}: {error}")),
        }
    }

    report.competing = openplotva_memory::supersession_pairs_from_extraction(
        &cards,
        &report.card_ids,
        &competing_inputs,
    );
    for (old_id, new_id) in &report.competing {
        match store.mark_cards_competing(*old_id, *new_id).await {
            Ok(()) => report.stats.cards_competing += 1,
            Err(error) => report
                .competing_errors
                .push(format!("{old_id}<>{new_id}: {error}")),
        }
    }

    Ok(report)
}

fn merge_card_report(
    report: &mut MemoryExtractionWriteReport,
    card_report: CardExtractionWriteReport,
) {
    report.stats.cards_inserted += card_report.stats.cards_inserted;
    report.stats.cards_updated += card_report.stats.cards_updated;
    report.stats.cards_superseded += card_report.stats.cards_superseded;
    report.stats.cards_competing += card_report.stats.cards_competing;
    report.card_ids.extend(card_report.card_ids);
    report.links_inserted += card_report.links_inserted;
    report.superseded.extend(card_report.superseded);
    report.competing.extend(card_report.competing);
    if report.card_embedding_error.is_none() {
        report.card_embedding_error = card_report.card_embedding_error;
    }
    if report.link_error.is_none() {
        report.link_error = card_report.link_error;
    }
    report
        .supersession_errors
        .extend(card_report.supersession_errors);
    report.competing_errors.extend(card_report.competing_errors);
}

async fn insert_memory_episode<ExtractorError, Store, Embedder>(
    store: &Store,
    embedder: Option<&Embedder>,
    episode: Episode,
    cfg: MemoryExtractionBatchConfig<'_>,
    report: &mut MemoryExtractionWriteReport,
) -> Result<(), MemoryExtractionWriteError<ExtractorError, Store::Error>>
where
    ExtractorError: StdError + Send + Sync + 'static,
    Store: MemoryWriteStore,
    Embedder: EmbeddingProvider,
{
    let episode_embedding = match (embedder, !episode.summary_text.trim().is_empty()) {
        (Some(provider), true) => match provider
            .embed_one(
                &episode.summary_text,
                cfg.embedding_dimension,
                MEMORY_EPISODE_EMBEDDING_TASK,
            )
            .await
        {
            Ok(embedding) => embedding,
            Err(error) if error.is_availability_failure() => {
                return Err(MemoryExtractionWriteError::EmbedderUnavailable { source: error });
            }
            Err(error) => {
                report.episode_embedding_error = Some(error.to_string());
                None
            }
        },
        _ => None,
    };
    let (episode_id, inserted) = store
        .insert_episode_with_embedding(
            episode,
            cfg.episode_model,
            cfg.prompt_version,
            episode_embedding,
        )
        .await
        .map_err(|source| MemoryExtractionWriteError::StoreEpisode { source })?;
    report.episode_id = episode_id;
    report.episode_inserted = inserted;
    if inserted && episode_id != 0 {
        report.stats.episodes_inserted = 1;
    }

    Ok(())
}

async fn embed_memory_cards<Embedder>(
    provider: &Embedder,
    cards: &[CardInput],
    dimension: i32,
) -> Result<Vec<Option<PgEmbeddingVector>>, EmbedderClientError>
where
    Embedder: EmbeddingProvider,
{
    let texts: Vec<String> = cards.iter().map(|card| card.fact_text.clone()).collect();
    provider
        .embed_batch(&texts, dimension, MEMORY_CARD_EMBEDDING_TASK)
        .await
}

fn episode_from_extraction_batch(input: &ExtractInput, output: &ExtractOutput) -> Episode {
    episode_from_run(&input.run, output, input.messages.len())
}

fn episode_from_run(
    run: &openplotva_memory::Run,
    output: &ExtractOutput,
    message_count: usize,
) -> Episode {
    Episode {
        visibility: openplotva_memory::chat_visibility(
            openplotva_memory::CARD_KIND_CHAT,
            run.thread_id,
        ),
        chat_id: run.chat_id,
        thread_id: run.thread_id,
        range_start_at: run.range_start_at,
        range_end_at: run.range_end_at,
        cursor_after_at: run.cursor_after_at,
        cursor_message_id: run.cursor_message_id,
        cursor_entry_id: run.cursor_entry_id.clone(),
        message_count: message_count as i32,
        summary_text: output.episode_summary.trim().to_owned(),
        topics: unique_normalized_strings(&output.topics),
        participants: unique_normalized_strings(&output.participants),
        ..Episode::default()
    }
}

fn unique_normalized_strings(values: &[String]) -> Vec<String> {
    let mut seen = Vec::<String>::new();
    let mut out = Vec::new();
    for value in values {
        let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
        if normalized.is_empty() {
            continue;
        }
        let key = normalized.to_lowercase();
        if seen.iter().any(|known| known == &key) {
            continue;
        }
        seen.push(key);
        out.push(normalized);
    }
    out
}

/// Placeholder error type used by `write_memory_extraction_batch`, which has no extractor phase.
#[derive(Debug, Error)]
#[error("memory extractor is unavailable")]
pub struct MemoryExtractorUnavailableForWrite;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct EmbedderEncodeRequest {
    /// Input prompts.
    pub prompts: Vec<String>,
    /// Requested embedding dimension.
    #[serde(skip_serializing_if = "is_zero_i32")]
    pub dimension: i32,
    /// Task description passed to Jina-style embedders.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub task_description: String,
}

#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct EmbedderEncodeResponse {
    /// Returned embeddings.
    pub embeddings: Vec<Vec<f64>>,
    /// Returned dimension.
    pub dimension: i32,
    /// Returned count.
    pub count: i32,
}

/// Embedder failure.
#[derive(Debug, Error)]
pub enum EmbedderClientError {
    /// Empty prompt batch.
    #[error("prompts cannot be empty")]
    EmptyPrompts,
    /// Request or response decode error.
    #[error("embedder request failed: {source}")]
    Request {
        /// Source reqwest error.
        #[from]
        source: reqwest::Error,
    },
    /// Non-200 HTTP response.
    #[error("HTTP {status}: {detail}")]
    Status {
        /// HTTP status code.
        status: u16,
        detail: String,
    },
    /// Retry attempts exhausted.
    #[error("operation failed after {attempts} attempts: {last_error}")]
    RetryExhausted {
        /// Total attempts including the first one.
        attempts: usize,
        /// Last error string.
        last_error: String,
    },
    /// Circuit breaker open; the embedder is treated as unavailable.
    #[error("embedder unavailable (circuit breaker open)")]
    Unavailable,
    /// Discovery transport, submit, poll, or decode failure.
    #[error("embedder discovery call failed: {message}")]
    Discovery {
        /// Failure detail.
        message: String,
    },
}

impl EmbedderClientError {
    /// Build a discovery-stage failure.
    #[must_use]
    pub fn discovery(message: String) -> Self {
        Self::Discovery { message }
    }

    /// Whether this failure means the embedder is unreachable/erroring (as
    /// opposed to a caller-side error like an empty batch). Drives the breaker
    /// and the consolidation abort decision.
    #[must_use]
    pub fn is_availability_failure(&self) -> bool {
        match self {
            Self::EmptyPrompts => false,
            Self::Unavailable | Self::Discovery { .. } | Self::RetryExhausted { .. } => true,
            Self::Request { .. } => retryable_embedder_error(self),
            Self::Status { status, .. } => *status >= 500,
        }
    }
}

pub fn memory_retrieval_embedder_from_config(
    config: &MemoryConfig,
    discovery_base_url: &str,
) -> Result<Option<DiscoveryEmbedderClient>, reqwest::Error> {
    DiscoveryEmbedderClient::with_budget(
        discovery_base_url,
        &config.embedder_service_name,
        &config.embedder_endpoint_name,
        MEMORY_RETRIEVAL_EMBEDDING_TIMEOUT,
    )
}

pub fn memory_write_embedder_from_config(
    config: &MemoryConfig,
    discovery_base_url: &str,
) -> Result<Option<DiscoveryEmbedderClient>, reqwest::Error> {
    DiscoveryEmbedderClient::with_budget(
        discovery_base_url,
        &config.embedder_service_name,
        &config.embedder_endpoint_name,
        EMBEDDER_DEFAULT_TIMEOUT,
    )
}

pub fn shield_embedder_from_config(
    config: &ShieldConfig,
    discovery_base_url: &str,
) -> Result<Option<DiscoveryEmbedderClient>, reqwest::Error> {
    let timeout = positive_seconds(config.retrieval_timeout_seconds).max(Duration::from_secs(1));
    DiscoveryEmbedderClient::with_budget(
        discovery_base_url,
        &config.embedder_service_name,
        &config.embedder_endpoint_name,
        timeout,
    )
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

fn retryable_embedder_error(error: &EmbedderClientError) -> bool {
    match error {
        EmbedderClientError::Request { source } => {
            source.is_timeout()
                || source.is_connect()
                || retryable_embedder_phrase(&source.to_string())
        }
        _ => false,
    }
}

fn retryable_embedder_phrase(value: &str) -> bool {
    ["EOF", "connection reset", "timeout"]
        .iter()
        .any(|phrase| value.contains(phrase))
}

#[must_use]
pub fn memory_retrieval_query_task() -> &'static str {
    MEMORY_RETRIEVAL_QUERY_TASK
}

#[must_use]
pub fn aifarm_memory_extractor_config_from_app_config(
    config: &AppConfig,
) -> AifarmMemoryExtractorConfig {
    aifarm_memory_extractor_config_from_app_config_with_model(config, None)
}

#[must_use]
pub fn aifarm_memory_extractor_config_from_app_config_with_model(
    config: &AppConfig,
    model_override: Option<&str>,
) -> AifarmMemoryExtractorConfig {
    let memory = &config.memory;
    let model = aifarm_memory_extractor_model(memory, model_override);
    AifarmMemoryExtractorConfig {
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
        max_output_tokens: memory.aifarm_max_output_tokens,
        temperature: Some(memory.aifarm_temperature),
        enable_thinking: Some(memory.aifarm_enable_thinking),
        include_reasoning: Some(false),
    }
    .with_defaults()
}

/// Build the reqwest-backed AIFarm memory extractor.
#[must_use]
pub fn aifarm_memory_extractor_from_app_config(
    config: &AppConfig,
) -> AifarmMemoryExtractor<ReqwestAifarmTransport> {
    AifarmMemoryExtractor::new(aifarm_memory_extractor_config_from_app_config(config))
}

/// Build the reqwest-backed AIFarm memory extractor with an override model.
#[must_use]
pub fn aifarm_memory_extractor_from_app_config_with_model(
    config: &AppConfig,
    model_override: Option<&str>,
) -> AifarmMemoryExtractor<ReqwestAifarmTransport> {
    AifarmMemoryExtractor::new(aifarm_memory_extractor_config_from_app_config_with_model(
        config,
        model_override,
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AifarmMemoryRouteBackendPlan {
    pub label: String,
    pub model: String,
    pub service_name: String,
    pub direct_url: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AifarmMemoryRoutePlan {
    pub backends: Vec<AifarmMemoryRouteBackendPlan>,
}

struct AifarmRouteMemoryBackend {
    plan: AifarmMemoryRouteBackendPlan,
    extractor: AifarmMemoryExtractor<ReqwestAifarmTransport>,
}

pub struct AifarmRouteMemoryExtractor {
    backends: Vec<AifarmRouteMemoryBackend>,
}

#[derive(Debug, Error)]
pub enum AifarmRouteMemoryExtractorError {
    #[error("memory extractor route has no available backends")]
    NoBackends,
    #[error("memory extractor backend {backend} failed: {source}")]
    Backend {
        backend: String,
        #[source]
        source: AifarmMemoryExtractorError,
    },
    #[error("memory extractor route exhausted retryable backends: {0}")]
    Exhausted(String),
}

impl MemoryExtractor for AifarmRouteMemoryExtractor {
    type Error = AifarmRouteMemoryExtractorError;

    fn extract<'a>(&'a self, input: &'a ExtractInput) -> MemoryExtractorFuture<'a, Self::Error> {
        Box::pin(async move {
            let mut retryable_errors = Vec::new();
            let mut attempted = false;
            for backend in &self.backends {
                attempted = true;
                match MemoryExtractor::extract(&backend.extractor, input).await {
                    Ok(output) => return Ok(output),
                    Err(source) if retryable_reason(&source).is_some() => {
                        retryable_errors.push(format!("{}: {source}", backend.plan.label));
                    }
                    Err(source) => {
                        return Err(AifarmRouteMemoryExtractorError::Backend {
                            backend: backend.plan.label.clone(),
                            source,
                        });
                    }
                }
            }
            if !attempted {
                return Err(AifarmRouteMemoryExtractorError::NoBackends);
            }
            Err(AifarmRouteMemoryExtractorError::Exhausted(
                retryable_errors.join("\n"),
            ))
        })
    }
}

#[must_use]
pub fn aifarm_memory_route_plan_from_app_config(config: &AppConfig) -> AifarmMemoryRoutePlan {
    AifarmMemoryRoutePlan {
        backends: aifarm_memory_route_backend_configs_from_app_config(config, None)
            .into_iter()
            .map(|(plan, _)| plan)
            .collect(),
    }
}

fn aifarm_route_memory_extractor_from_app_config_with_model(
    config: &AppConfig,
    model_override: Option<&str>,
) -> AifarmRouteMemoryExtractor {
    AifarmRouteMemoryExtractor {
        backends: aifarm_memory_route_backend_configs_from_app_config(config, model_override)
            .into_iter()
            .map(|(plan, cfg)| AifarmRouteMemoryBackend {
                plan,
                extractor: AifarmMemoryExtractor::new(cfg),
            })
            .collect(),
    }
}

fn aifarm_memory_route_backend_configs_from_app_config(
    config: &AppConfig,
    model_override: Option<&str>,
) -> Vec<(AifarmMemoryRouteBackendPlan, AifarmMemoryExtractorConfig)> {
    let primary = aifarm_memory_extractor_config_from_app_config_with_model(config, model_override);
    vec![(
        aifarm_memory_route_backend_plan("primary", &primary),
        primary,
    )]
}

fn aifarm_memory_route_backend_plan(
    label: impl Into<String>,
    cfg: &AifarmMemoryExtractorConfig,
) -> AifarmMemoryRouteBackendPlan {
    AifarmMemoryRouteBackendPlan {
        label: label.into(),
        model: cfg.model.clone(),
        service_name: cfg.client.service_name.clone(),
        direct_url: cfg.client.direct_url.clone(),
    }
}

/// App-owned GenKit memory extractor branch.
pub enum AppGenkitMemoryExtractor {
    /// Direct Gemini implementation.
    Gemini(GeminiMemoryExtractor<ReqwestAifarmTransport>),
    OpenAiCompatible(Box<GenkitOpenAiCompatibleMemoryExtractor<ReqwestAifarmTransport>>),
}

/// App-owned GenKit memory extractor error.
#[derive(Debug, Error)]
pub enum AppGenkitMemoryExtractorError {
    /// Direct Gemini implementation failed.
    #[error(transparent)]
    Gemini(#[from] GeminiMemoryExtractorError),
    /// OpenAI-compatible plugin implementation failed.
    #[error(transparent)]
    OpenAiCompatible(#[from] GenkitOpenAiCompatibleMemoryExtractorError),
}

impl MemoryExtractor for AppGenkitMemoryExtractor {
    type Error = AppGenkitMemoryExtractorError;

    fn extract<'a>(
        &'a self,
        input: &'a openplotva_memory::ExtractInput,
    ) -> MemoryExtractorFuture<'a, Self::Error> {
        Box::pin(async move {
            match self {
                Self::Gemini(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::OpenAiCompatible(extractor) => {
                    MemoryExtractor::extract(extractor.as_ref(), input)
                        .await
                        .map_err(Into::into)
                }
            }
        })
    }
}

/// App-owned memory extractor composition.
pub enum AppMemoryExtractor {
    /// Plain AIFarm extractor without redaction.
    Plain(AifarmMemoryExtractor<ReqwestAifarmTransport>),
    Redacting(
        RedactingMemoryExtractor<AifarmMemoryExtractor<ReqwestAifarmTransport>, DiscoveryRedactor>,
    ),
    AifarmFallback(AifarmGenkitMemoryExtractor),
    RedactingAifarmFallback(RedactingAifarmGenkitMemoryExtractor),
    AifarmRoute(AifarmRouteMemoryExtractor),
    RedactingAifarmRoute(RedactingAifarmRouteMemoryExtractor),
    /// Plain Gemini/GenKit extractor without redaction.
    GeminiPlain(AppGenkitMemoryExtractor),
    GeminiRedacting(RedactingMemoryExtractor<AppGenkitMemoryExtractor, DiscoveryRedactor>),
}

#[derive(Clone)]
pub struct RoutedMemoryExtractor {
    walker: RoutedAttemptWalker,
    config: AppConfig,
    redactor: Option<RoutedDiscoveryRedactor>,
}

impl RoutedMemoryExtractor {
    #[must_use]
    pub fn new(walker: RoutedAttemptWalker, config: &AppConfig) -> Self {
        let redactor = config
            .memory
            .redaction_enabled
            .then(|| RoutedDiscoveryRedactor::new(walker.clone(), config));
        Self {
            walker,
            config: config.clone(),
            redactor,
        }
    }
}

#[derive(Clone)]
pub struct RoutedDiscoveryRedactor {
    walker: RoutedAttemptWalker,
    config: AppConfig,
}

impl RoutedDiscoveryRedactor {
    #[must_use]
    pub fn new(walker: RoutedAttemptWalker, config: &AppConfig) -> Self {
        Self {
            walker,
            config: config.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum RoutedDiscoveryRedactorError {
    #[error(transparent)]
    Build(#[from] reqwest::Error),
    #[error(transparent)]
    Redactor(#[from] DiscoveryRedactorError),
    #[error(transparent)]
    Routing(#[from] RoutedRoutingError),
}

impl TextRedactor for RoutedDiscoveryRedactor {
    type Error = RoutedDiscoveryRedactorError;

    fn redact_text<'a>(&'a self, text: String) -> TextRedactorFuture<'a, Self::Error> {
        Box::pin(async move {
            let config = self.config.clone();
            let result = self
                .walker
                .run(
                    RoutedRequestContext {
                        workflow_key: "redaction".to_owned(),
                        queue_name: Some(MEMORY_CONSOLIDATION_QUEUE_NAME.to_owned()),
                        suppress_all_attempts_exhausted_admin_report: true,
                        suppressed_all_attempts_exhausted_reason: Some(
                            "redaction_fail_open".to_owned(),
                        ),
                        ..RoutedRequestContext::default()
                    },
                    move |attempt| {
                        let config = config.clone();
                        let text = text.clone();
                        async move { redact_text_with_routed_attempt(&config, attempt, &text).await }
                    },
                    routed_redactor_retryable_reason,
                )
                .await;
            match result {
                Ok(redacted) => Ok(redacted),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => Err(error.into()),
            }
        })
    }
}

#[derive(Debug, Error)]
pub enum RoutedMemoryExtractorError {
    #[error(transparent)]
    Build(#[from] AppMemoryExtractorBuildError),
    #[error(transparent)]
    Aifarm(#[from] AifarmMemoryExtractorError),
    #[error(transparent)]
    Genkit(#[from] AppGenkitMemoryExtractorError),
    #[error(transparent)]
    Routing(#[from] RoutedRoutingError),
}

impl MemoryExtractor for RoutedMemoryExtractor {
    type Error = RoutedMemoryExtractorError;

    fn extract<'a>(&'a self, input: &'a ExtractInput) -> MemoryExtractorFuture<'a, Self::Error> {
        Box::pin(async move {
            let config = self.config.clone();
            let result =
                self.walker
                    .run(
                        RoutedRequestContext {
                            workflow_key: "memory_consolidation".to_owned(),
                            queue_name: Some(MEMORY_CONSOLIDATION_QUEUE_NAME.to_owned()),
                            ..RoutedRequestContext::default()
                        },
                        move |attempt| {
                            let config = config.clone();
                            async move {
                                extract_memory_with_routed_attempt(&config, attempt, input).await
                            }
                        },
                        |error| retryable_reason(error),
                    )
                    .await;
            let output = match result {
                Ok(output) => Ok(output),
                Err(RoutedAttemptRunError::Attempt(error)) => Err(error),
                Err(RoutedAttemptRunError::Routing(error)) => Err(error.into()),
            }?;
            let Some(redactor) = &self.redactor else {
                return Ok(output);
            };
            let original = output.clone();
            match redact_extract_output_with_async(output, |value| redactor.redact_text(value))
                .await
            {
                Ok(redacted) => Ok(redacted),
                Err(_) => Ok(original),
            }
        })
    }
}

async fn redact_text_with_routed_attempt(
    config: &AppConfig,
    attempt: RoutedAttempt,
    text: &str,
) -> Result<String, RoutedDiscoveryRedactorError> {
    let Some(mut cfg) = discovery_redactor_config_from_app_config(config) else {
        return Ok(text.to_owned());
    };
    if let Some(endpoint) = routed_attempt_endpoint(&attempt) {
        cfg.base_url = endpoint;
    }
    if let Some(service) = attempt.discovery_service_name.as_deref() {
        cfg.service_name = service.to_owned();
    }
    if let Some(endpoint) = attempt.discovery_endpoint_name.as_deref() {
        cfg.endpoint_name = endpoint.to_owned();
    }
    let redactor = DiscoveryRedactor::new(cfg)?;
    redactor.redact_text(text).await.map_err(Into::into)
}

fn routed_redactor_retryable_reason(error: &RoutedDiscoveryRedactorError) -> Option<FailureReason> {
    match error {
        RoutedDiscoveryRedactorError::Build(_) => Some(FailureReason::ProviderUnavailable),
        RoutedDiscoveryRedactorError::Redactor(error) => {
            retryable_reason_from_message(&error.to_string())
        }
        RoutedDiscoveryRedactorError::Routing(_) => None,
    }
}

async fn extract_memory_with_routed_attempt(
    config: &AppConfig,
    attempt: RoutedAttempt,
    input: &ExtractInput,
) -> Result<ExtractOutput, RoutedMemoryExtractorError> {
    if routed_attempt_is_genkit(&attempt) {
        let model = genkit_model_for_attempt(&attempt);
        let extractor = genkit_memory_extractor_from_app_config_with_model(config, Some(&model))?;
        return MemoryExtractor::extract(&extractor, input)
            .await
            .map_err(Into::into);
    }

    let extractor = AifarmMemoryExtractor::new(aifarm_memory_config_for_attempt(config, &attempt));
    MemoryExtractor::extract(&extractor, input)
        .await
        .map_err(Into::into)
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

fn aifarm_memory_config_for_attempt(
    config: &AppConfig,
    attempt: &RoutedAttempt,
) -> AifarmMemoryExtractorConfig {
    let mut cfg = aifarm_memory_extractor_config_from_app_config_with_model(
        config,
        Some(attempt.model_name.trim()),
    );
    cfg.client.default_model = attempt.model_name.clone();

    if let Some(endpoint) = routed_attempt_endpoint(attempt) {
        if attempt.discovery_service_name.is_some() || attempt.discovery_endpoint_name.is_some() {
            cfg.client.base_url = endpoint;
        } else {
            cfg.client.direct_url = normalize_chat_completions_url(&endpoint);
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
    if let Some(enable_thinking) = routed_bool_override(attempt, "enable_thinking") {
        cfg.enable_thinking = Some(enable_thinking);
    }
    if let Some(include_reasoning) = routed_bool_override(attempt, "include_reasoning") {
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

fn routed_bool_override(attempt: &RoutedAttempt, key: &str) -> Option<bool> {
    attempt.overrides.extra.get(key).and_then(Value::as_bool)
}

/// App memory extractor build error.
#[derive(Debug, Error)]
pub enum AppMemoryExtractorBuildError {
    /// HTTP client construction failed.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("unsupported memory consolidation provider {0:?}")]
    UnsupportedProvider(String),
    /// Gemini/GenKit provider was requested without an active Google AI key.
    #[error("google ai key is required for genkit memory extractor")]
    GoogleAiKeyRequired,
    /// GenKit OpenAI-compatible provider was requested without its API key.
    #[error("{provider} api key is required for genkit memory extractor")]
    ProviderApiKeyRequired {
        /// Provider name.
        provider: &'static str,
    },
}

/// App memory extractor runtime error.
#[derive(Debug, Error)]
pub enum AppMemoryExtractorError {
    /// AIFarm extraction failed.
    #[error(transparent)]
    Aifarm(#[from] openplotva_llm::aifarm::AifarmMemoryExtractorError),
    /// Gemini/GenKit extraction failed.
    #[error(transparent)]
    Gemini(#[from] AppGenkitMemoryExtractorError),
    /// AIFarm primary failed retryably and Gemini/GenKit fallback failed too, or AIFarm failed terminally.
    #[error(transparent)]
    AifarmFallback(
        #[from]
        FallbackMemoryExtractorError<AifarmMemoryExtractorError, AppGenkitMemoryExtractorError>,
    ),
    #[error(transparent)]
    AifarmRoute(#[from] AifarmRouteMemoryExtractorError),
}

impl MemoryExtractor for AppMemoryExtractor {
    type Error = AppMemoryExtractorError;

    fn extract<'a>(
        &'a self,
        input: &'a openplotva_memory::ExtractInput,
    ) -> MemoryExtractorFuture<'a, Self::Error> {
        Box::pin(async move {
            match self {
                Self::Plain(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::Redacting(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::AifarmFallback(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::RedactingAifarmFallback(extractor) => {
                    MemoryExtractor::extract(extractor, input)
                        .await
                        .map_err(Into::into)
                }
                Self::AifarmRoute(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::RedactingAifarmRoute(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::GeminiPlain(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
                Self::GeminiRedacting(extractor) => MemoryExtractor::extract(extractor, input)
                    .await
                    .map_err(Into::into),
            }
        })
    }
}

pub fn memory_extractor_from_app_config(
    config: &AppConfig,
) -> Result<AppMemoryExtractor, AppMemoryExtractorBuildError> {
    memory_extractor_from_app_config_with_override(config, None, None)
}

#[must_use]
pub fn routed_memory_extractor_from_app_config(
    config: &AppConfig,
    walker: RoutedAttemptWalker,
) -> RoutedMemoryExtractor {
    RoutedMemoryExtractor::new(walker, config)
}

pub fn memory_extractor_from_app_config_with_model(
    config: &AppConfig,
    model_override: Option<&str>,
) -> Result<AppMemoryExtractor, AppMemoryExtractorBuildError> {
    memory_extractor_from_app_config_with_override(config, None, model_override)
}

/// Build app memory extractor with optional provider/model override and optional redaction.
pub fn memory_extractor_from_app_config_with_override(
    config: &AppConfig,
    provider_override: Option<&str>,
    model_override: Option<&str>,
) -> Result<AppMemoryExtractor, AppMemoryExtractorBuildError> {
    let provider = memory_extractor_provider(config, provider_override);
    match provider.as_str() {
        "aifarm" => {
            let extractor =
                aifarm_route_memory_extractor_from_app_config_with_model(config, model_override);
            let redactor = discovery_redactor_from_app_config(config)?;
            if let Some(redactor) = redactor {
                Ok(AppMemoryExtractor::RedactingAifarmRoute(
                    RedactingMemoryExtractor::new(extractor, redactor),
                ))
            } else {
                Ok(AppMemoryExtractor::AifarmRoute(extractor))
            }
        }
        "genkit" | "gemini" => {
            let extractor =
                genkit_memory_extractor_from_app_config_with_model(config, model_override)?;
            if let Some(redactor) = discovery_redactor_from_app_config(config)? {
                Ok(AppMemoryExtractor::GeminiRedacting(
                    RedactingMemoryExtractor::new(extractor, redactor),
                ))
            } else {
                Ok(AppMemoryExtractor::GeminiPlain(extractor))
            }
        }
        other => Err(AppMemoryExtractorBuildError::UnsupportedProvider(
            other.to_owned(),
        )),
    }
}

fn memory_extractor_provider(config: &AppConfig, provider_override: Option<&str>) -> String {
    let provider = provider_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| config.memory.consolidation_provider.trim())
        .to_ascii_lowercase();
    if provider.is_empty() {
        "aifarm".to_owned()
    } else {
        provider
    }
}

/// Build the reqwest-backed Gemini/GenKit memory extractor with an optional override model.
pub fn genkit_memory_extractor_from_app_config_with_model(
    config: &AppConfig,
    model_override: Option<&str>,
) -> Result<AppGenkitMemoryExtractor, AppMemoryExtractorBuildError> {
    let model = genkit_memory_extractor_model(config, model_override);
    if let Some(cfg) =
        genkit_openai_compatible_memory_extractor_config_from_app_config(config, &model)?
    {
        return Ok(AppGenkitMemoryExtractor::OpenAiCompatible(Box::new(
            GenkitOpenAiCompatibleMemoryExtractor::new(cfg),
        )));
    }
    let api_key = resolve_google_ai_key(&config.google_ai);
    if api_key.trim().is_empty() {
        return Err(AppMemoryExtractorBuildError::GoogleAiKeyRequired);
    }
    Ok(AppGenkitMemoryExtractor::Gemini(
        GeminiMemoryExtractor::new(GeminiMemoryExtractorConfig {
            api_key,
            model,
            request_timeout: positive_seconds(config.memory.consolidation_timeout_seconds),
            ..GeminiMemoryExtractorConfig::default()
        }),
    ))
}

#[must_use]
pub fn genkit_memory_extractor_model(config: &AppConfig, model_override: Option<&str>) -> String {
    if let Some(model) = model_override
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return model.to_owned();
    }
    genkit_runtime_default_model(config)
}

#[must_use]
pub fn genkit_runtime_default_model(config: &AppConfig) -> String {
    let configured = config.llm.genkit.default_model.trim();
    if !configured.is_empty() {
        return configured.to_owned();
    }
    if !resolve_google_ai_key(&config.google_ai).trim().is_empty() {
        MODEL_GEMINI_FLASH_LITE.to_owned()
    } else {
        MODEL_GEMINI_FLASH_FALLBACK.to_owned()
    }
}

fn genkit_openai_compatible_memory_extractor_config_from_app_config(
    config: &AppConfig,
    model: &str,
) -> Result<Option<GenkitOpenAiCompatibleMemoryExtractorConfig>, AppMemoryExtractorBuildError> {
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
        return Err(AppMemoryExtractorBuildError::ProviderApiKeyRequired { provider });
    }
    Ok(Some(GenkitOpenAiCompatibleMemoryExtractorConfig {
        direct_url: direct_url.to_owned(),
        api_key,
        model,
        request_timeout: positive_seconds(request_timeout_seconds),
        ..GenkitOpenAiCompatibleMemoryExtractorConfig::default()
    }))
}

fn strip_provider_prefix_fold<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    value
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
        .then(|| &value[prefix.len()..])
}

#[must_use]
pub fn memory_service_worker_config_from_memory_config(
    config: &MemoryConfig,
) -> MemoryServiceWorkerConfig {
    MemoryServiceWorkerConfig {
        enabled: config.enabled,
        retention: positive_hours(config.retention_hours, 7 * 24),
        daily_schedule: config.daily_schedule.clone(),
        daily_window: positive_hours(config.daily_window_hours, 24),
        tick_interval: Duration::from_secs(60 * 60),
        enqueue_policy: openplotva_memory::MemoryRunEnqueuePolicy {
            min_messages_per_run: config.min_messages_per_run,
            max_queued_runs: config.max_queued_runs,
            max_daily_enqueued_runs: config.max_daily_enqueued_runs,
        },
        process: MemoryRunProcessConfig {
            lease: positive_seconds(config.consolidation_timeout_seconds),
            max_messages_per_run: config.max_messages_per_run,
            max_input_tokens: config.max_input_tokens,
            embedding_dimension: config.embedding_dim,
            episode_model: config.consolidation_model.clone(),
            ..MemoryRunProcessConfig::default()
        },
    }
    .normalized()
}

#[must_use]
pub fn discovery_redactor_config_from_app_config(
    config: &AppConfig,
) -> Option<DiscoveryRedactorConfig> {
    let memory = &config.memory;
    if !memory.redaction_enabled {
        return None;
    }
    Some(
        DiscoveryRedactorConfig {
            base_url: config.llm.discovery.base_url.clone(),
            service_name: memory.redaction_service_name.clone(),
            endpoint_name: memory.redaction_endpoint_name.clone(),
            timeout: positive_seconds(memory.redaction_timeout_seconds),
            task_timeout: positive_seconds(memory.redaction_task_timeout_seconds),
            poll_interval: positive_seconds(memory.redaction_poll_seconds),
            capacity_wait: positive_seconds(memory.redaction_capacity_wait_seconds),
            capacity_poll_interval: positive_seconds(memory.redaction_capacity_poll_seconds),
            categories: memory.redaction_categories.clone(),
        }
        .with_defaults(),
    )
}

/// Build the reqwest-backed Discovery privacy-filter redactor.
pub fn discovery_redactor_from_app_config(
    config: &AppConfig,
) -> Result<Option<DiscoveryRedactor>, reqwest::Error> {
    discovery_redactor_config_from_app_config(config)
        .map(DiscoveryRedactor::new)
        .transpose()
}

#[must_use]
pub fn aifarm_memory_extractor_model(
    memory: &MemoryConfig,
    override_model: Option<&str>,
) -> String {
    let candidate = override_model
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| memory.consolidation_model.trim());
    if candidate.is_empty() {
        "default".to_owned()
    } else {
        candidate.to_owned()
    }
}

fn positive_seconds(seconds: i32) -> Duration {
    if seconds <= 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(seconds as u64)
    }
}

fn positive_hours(hours: i32, fallback: i32) -> Duration {
    let hours = if hours <= 0 { fallback } else { hours };
    Duration::from_secs(hours as u64 * 60 * 60)
}

fn clamp_memory_max_input_tokens(value: i32) -> i32 {
    if value <= 0 {
        MEMORY_MAX_EXTRACTION_BATCH_INPUT_TOKENS
    } else {
        value.min(MEMORY_MAX_EXTRACTION_BATCH_INPUT_TOKENS)
    }
}

fn clamp_u64_to_i32(value: u64) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

fn runtime_memory_restart_model(model: &str) -> String {
    let value = model.trim();
    if value.is_empty() {
        openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL.to_owned()
    } else {
        value.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openplotva_storage::pg_embedding_vector;
    use std::sync::Mutex;

    #[derive(Debug, Error)]
    #[error("store failed")]
    struct TestStoreError;

    #[derive(Debug, Error)]
    #[error("extract failed")]
    struct TestExtractorError;

    #[derive(Debug, Default)]
    struct FakeMemoryWriteStore {
        ids: Vec<i64>,
        ensure_daily_count: u64,
        ensured: Mutex<
            Vec<(
                OffsetDateTime,
                Duration,
                openplotva_memory::MemoryRunEnqueuePolicy,
            )>,
        >,
        run: Mutex<Option<openplotva_memory::Run>>,
        chat_meta: Mutex<Option<DialogMemoryChatMeta>>,
        messages: Mutex<Vec<openplotva_memory::Message>>,
        visible_cards: Mutex<Vec<Card>>,
        visible_card_scopes: Mutex<Vec<openplotva_memory::RetrievalScope>>,
        continuations: Mutex<Vec<(i32, i32)>>,
        completed: Mutex<Vec<(i64, RunStats)>>,
        failed: Mutex<Vec<(i64, String)>>,
        released: Mutex<Vec<i64>>,
        retried_runs: Mutex<Vec<i64>>,
        retry_failed_count: u64,
        retry_failed_calls: Mutex<i32>,
        cards: Mutex<Vec<CardInput>>,
        card_embedding_slots: Mutex<Vec<bool>>,
        links: Mutex<Vec<LinkInput>>,
        superseded: Mutex<Vec<(i64, i64)>>,
        episodes: Mutex<Vec<Episode>>,
        episode_had_embedding: Mutex<bool>,
    }

    impl MemoryWriteStore for FakeMemoryWriteStore {
        type Error = TestStoreError;

        fn upsert_cards_with_embeddings<'a>(
            &'a self,
            cards: &'a [CardInput],
            embeddings: &'a [Option<PgEmbeddingVector>],
        ) -> MemoryWriteStoreFuture<'a, (RunStats, Vec<i64>), Self::Error> {
            Box::pin(async move {
                let prior_cards = {
                    let mut stored = self.cards.lock().expect("cards");
                    let prior_cards = stored.len();
                    stored.extend_from_slice(cards);
                    prior_cards
                };
                self.card_embedding_slots
                    .lock()
                    .expect("embeddings")
                    .extend(embeddings.iter().map(Option::is_some));
                let mut ids: Vec<i64> = self
                    .ids
                    .iter()
                    .skip(prior_cards)
                    .take(cards.len())
                    .copied()
                    .collect();
                if ids.len() < cards.len() {
                    ids = self.ids.iter().take(cards.len()).copied().collect();
                }
                Ok((
                    RunStats {
                        cards_inserted: cards.len() as i32,
                        ..RunStats::default()
                    },
                    ids,
                ))
            })
        }

        fn insert_links<'a>(
            &'a self,
            links: &'a [LinkInput],
        ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.links.lock().expect("links").extend_from_slice(links);
                Ok(())
            })
        }

        fn supersede_card<'a>(
            &'a self,
            old_id: i64,
            new_id: i64,
        ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.superseded
                    .lock()
                    .expect("superseded")
                    .push((old_id, new_id));
                Ok(())
            })
        }

        fn mark_cards_competing<'a>(
            &'a self,
            _old_id: i64,
            _new_id: i64,
        ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move { Ok(()) })
        }

        fn insert_episode_with_embedding<'a>(
            &'a self,
            episode: Episode,
            _model: &'a str,
            _prompt_version: &'a str,
            embedding: Option<PgEmbeddingVector>,
        ) -> MemoryWriteStoreFuture<'a, (i64, bool), Self::Error> {
            Box::pin(async move {
                self.episodes.lock().expect("episodes").push(episode);
                *self
                    .episode_had_embedding
                    .lock()
                    .expect("episode embedding") = embedding.is_some();
                Ok((500, true))
            })
        }
    }

    impl MemoryRestartStore for FakeMemoryWriteStore {
        type Error = TestStoreError;

        fn retry_run<'a>(&'a self, run_id: i64) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.retried_runs.lock().expect("retried runs").push(run_id);
                Ok(())
            })
        }

        fn retry_failed_runs<'a>(&'a self) -> MemoryWriteStoreFuture<'a, u64, Self::Error> {
            Box::pin(async move {
                *self.retry_failed_calls.lock().expect("retry failed calls") += 1;
                Ok(self.retry_failed_count)
            })
        }
    }

    impl MemoryRunStore for FakeMemoryWriteStore {
        fn ensure_daily_runs<'a>(
            &'a self,
            now: OffsetDateTime,
            retention: Duration,
            policy: openplotva_memory::MemoryRunEnqueuePolicy,
        ) -> MemoryWriteStoreFuture<'a, u64, Self::Error> {
            Box::pin(async move {
                self.ensured
                    .lock()
                    .expect("ensured")
                    .push((now, retention, policy));
                Ok(self.ensure_daily_count)
            })
        }

        fn claim_run<'a>(
            &'a self,
            _owner: &'a str,
            _lease: Duration,
        ) -> MemoryWriteStoreFuture<'a, Option<openplotva_memory::Run>, Self::Error> {
            Box::pin(async move { Ok(self.run.lock().expect("run").take()) })
        }

        fn load_run_messages<'a>(
            &'a self,
            _run: &'a openplotva_memory::Run,
            _max_messages_per_run: i32,
        ) -> MemoryWriteStoreFuture<'a, Vec<openplotva_memory::Message>, Self::Error> {
            Box::pin(async move { Ok(self.messages.lock().expect("messages").clone()) })
        }

        fn load_run_chat_meta<'a>(
            &'a self,
            _chat_id: i64,
        ) -> MemoryWriteStoreFuture<'a, Option<DialogMemoryChatMeta>, Self::Error> {
            Box::pin(async move { Ok(self.chat_meta.lock().expect("chat meta").clone()) })
        }

        fn list_visible_cards<'a>(
            &'a self,
            scope: &'a openplotva_memory::RetrievalScope,
            _limit: i32,
        ) -> MemoryWriteStoreFuture<'a, Vec<Card>, Self::Error> {
            Box::pin(async move {
                self.visible_card_scopes
                    .lock()
                    .expect("visible card scopes")
                    .push(scope.clone());
                Ok(self.visible_cards.lock().expect("visible cards").clone())
            })
        }

        fn enqueue_run_continuation<'a>(
            &'a self,
            _run: &'a openplotva_memory::Run,
            after: &'a openplotva_memory::Message,
            remaining_messages: i32,
        ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.continuations
                    .lock()
                    .expect("continuations")
                    .push((after.message_id, remaining_messages));
                Ok(())
            })
        }

        fn complete_run<'a>(
            &'a self,
            run_id: i64,
            stats: RunStats,
        ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.completed
                    .lock()
                    .expect("completed")
                    .push((run_id, stats));
                Ok(())
            })
        }

        fn fail_run<'a>(
            &'a self,
            run_id: i64,
            cause: &'a str,
        ) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.failed
                    .lock()
                    .expect("failed")
                    .push((run_id, cause.to_owned()));
                Ok(())
            })
        }

        fn release_run<'a>(&'a self, run_id: i64) -> MemoryWriteStoreFuture<'a, (), Self::Error> {
            Box::pin(async move {
                self.released.lock().expect("released").push(run_id);
                Ok(())
            })
        }
    }

    #[derive(Debug, Default)]
    struct FakeEmbedder {
        calls: Mutex<Vec<(String, i32, String)>>,
    }

    impl EmbeddingProvider for FakeEmbedder {
        fn embed_one<'a>(
            &'a self,
            text: &'a str,
            dimension: i32,
            task_description: &'a str,
        ) -> EmbeddingProviderFuture<'a> {
            Box::pin(async move {
                self.calls.lock().expect("calls").push((
                    text.to_owned(),
                    dimension,
                    task_description.to_owned(),
                ));
                Ok(Some(pg_embedding_vector(vec![1.0, 0.5])))
            })
        }
    }

    /// Embedder reporting itself unavailable; gates consolidation off.
    #[derive(Debug, Default)]
    struct UnavailableEmbedder;

    impl EmbeddingProvider for UnavailableEmbedder {
        fn embed_one<'a>(
            &'a self,
            _text: &'a str,
            _dimension: i32,
            _task_description: &'a str,
        ) -> EmbeddingProviderFuture<'a> {
            Box::pin(async move { Err(EmbedderClientError::Unavailable) })
        }

        fn is_available(&self) -> bool {
            false
        }
    }

    /// Embedder that passes the health gate but fails every embed with an
    /// availability error, simulating the embedder dying mid-run.
    #[derive(Debug, Default)]
    struct MidRunFailingEmbedder;

    impl EmbeddingProvider for MidRunFailingEmbedder {
        fn embed_one<'a>(
            &'a self,
            _text: &'a str,
            _dimension: i32,
            _task_description: &'a str,
        ) -> EmbeddingProviderFuture<'a> {
            Box::pin(async move { Err(EmbedderClientError::Unavailable) })
        }
    }

    #[derive(Debug)]
    struct FakeMemoryExtractor {
        output: ExtractOutput,
        inputs: Mutex<Vec<ExtractInput>>,
    }

    impl MemoryExtractor for FakeMemoryExtractor {
        type Error = TestExtractorError;

        fn extract<'a>(
            &'a self,
            input: &'a ExtractInput,
        ) -> MemoryExtractorFuture<'a, Self::Error> {
            Box::pin(async move {
                self.inputs.lock().expect("inputs").push(input.clone());
                Ok(self.output.clone())
            })
        }
    }

    #[test]
    fn aifarm_memory_extractor_config_maps_memory_env_with_optional_pool() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            discovery_base_url: Some("http://discovery.test".to_owned()),
            memory_consolidation_model: Some("memory-model".to_owned()),
            memory_aifarm_service_name: Some("memory-service".to_owned()),
            memory_aifarm_endpoint_name: Some("memory-endpoint".to_owned()),
            memory_aifarm_max_output_tokens: Some("2048".to_owned()),
            memory_aifarm_request_timeout_seconds: Some("66".to_owned()),
            memory_aifarm_poll_interval_seconds: Some("3".to_owned()),
            memory_aifarm_task_timeout_seconds: Some("77".to_owned()),
            memory_aifarm_capacity_wait_seconds: Some("88".to_owned()),
            memory_aifarm_capacity_poll_seconds: Some("4".to_owned()),
            memory_aifarm_temperature: Some("0.35".to_owned()),
            memory_aifarm_enable_thinking: Some("true".to_owned()),
            dialog_aifarm_pool_models: Some("pool-a, pool-b".to_owned()),
            dialog_aifarm_pool_base_urls: Some(
                "http://pool-a.test/v1, http://pool-b.test".to_owned(),
            ),
            dialog_aifarm_pool_api_key: Some("pool-token".to_owned()),
            dialog_aifarm_pool_primary_capacity_wait_ms: Some("250".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = aifarm_memory_extractor_config_from_app_config(&config);

        assert_eq!(cfg.client.base_url, "http://discovery.test");
        assert_eq!(cfg.client.service_name, "memory-service");
        assert_eq!(cfg.client.endpoint_name, "memory-endpoint");
        assert_eq!(cfg.client.default_model, "memory-model");
        assert_eq!(cfg.client.request_timeout, Duration::from_secs(66));
        assert_eq!(cfg.client.poll_interval, Duration::from_secs(3));
        assert_eq!(cfg.client.task_timeout, Duration::from_secs(77));
        assert_eq!(cfg.client.capacity_wait, Duration::from_secs(88));
        assert_eq!(cfg.client.capacity_poll_interval, Duration::from_secs(4));
        assert_eq!(
            cfg.client.priority,
            openplotva_llm::aifarm::DISCOVERY_PRIORITY_MEMORY
        );
        assert_eq!(
            cfg.client.workload,
            openplotva_llm::aifarm::AIFARM_WORKLOAD_MEMORY
        );
        assert_eq!(cfg.model, "memory-model");
        assert_eq!(cfg.max_output_tokens, 2048);
        assert_eq!(cfg.temperature, Some(0.35));
        assert_eq!(cfg.enable_thinking, Some(false));
        assert_eq!(cfg.include_reasoning, Some(false));
    }

    #[test]
    fn aifarm_memory_extractor_model_preserves_go_override_and_empty_fallback() {
        let mut config = AppConfig::from_raw(openplotva_config::RawConfig::default())
            .expect("config")
            .memory;

        assert_eq!(
            aifarm_memory_extractor_model(&config, Some(" override ")),
            "override"
        );
        config.consolidation_model.clear();
        assert_eq!(aifarm_memory_extractor_model(&config, None), "default");
    }

    #[test]
    fn aifarm_memory_extractor_config_uses_override_model_in_client_and_request() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_consolidation_model: Some("default-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg =
            aifarm_memory_extractor_config_from_app_config_with_model(&config, Some(" override "));

        assert_eq!(cfg.model, "override");
        assert_eq!(cfg.client.default_model, "override");
    }

    #[test]
    fn memory_extractor_config_routes_vibethinker_to_qwen_service() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_consolidation_model: Some("vibethinker-3b".to_owned()),
            memory_aifarm_service_name: Some("llm-openai-qwen27b-gguf".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = aifarm_memory_extractor_config_from_app_config(&config);

        assert_eq!(cfg.model, "vibethinker-3b");
        assert_eq!(cfg.client.default_model, "vibethinker-3b");
        assert_eq!(cfg.client.service_name, "llm-openai-qwen27b-gguf");
    }

    #[test]
    fn memory_service_worker_config_includes_enqueue_policy() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_min_messages_per_run: Some("21".to_owned()),
            memory_max_queued_runs: Some("4321".to_owned()),
            memory_max_daily_enqueued_runs: Some("1234".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = memory_service_worker_config_from_memory_config(&config.memory);

        assert_eq!(cfg.enqueue_policy.min_messages_per_run, 21);
        assert_eq!(cfg.enqueue_policy.max_queued_runs, 4321);
        assert_eq!(cfg.enqueue_policy.max_daily_enqueued_runs, 1234);
    }

    #[test]
    fn aifarm_memory_route_plan_ignores_hidden_pool_backends() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            dialog_aifarm_pool_models: Some("pool-a,pool-b".to_owned()),
            dialog_aifarm_pool_base_urls: Some(
                "https://pool-a.test/v1,https://pool-b.test/v1/chat/completions".to_owned(),
            ),
            dialog_aifarm_pool_api_key: Some("pool-key".to_owned()),
            llm_provider_names: Some("qwen-reasoner".to_owned()),
            llm_provider_kinds: Some("aifarm".to_owned()),
            llm_provider_discovery_service_names: Some("custom-qwen".to_owned()),
            llm_provider_models: Some("custom-qwen-model".to_owned()),
            memory_consolidation_model: Some("primary-memory".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let plan = aifarm_memory_route_plan_from_app_config(&config);

        assert_eq!(
            plan.backends
                .iter()
                .map(|backend| backend.label.as_str())
                .collect::<Vec<_>>(),
            vec!["primary"]
        );
        assert_eq!(plan.backends[0].service_name, "llm-openai");
        assert_eq!(plan.backends[0].model, "primary-memory");
    }

    #[tokio::test]
    async fn runtime_memory_restarter_retries_specific_run_and_triggers_worker() {
        let trigger = Arc::new(tokio::sync::Notify::new());
        let restarter = RuntimeMemoryRestarter::new(
            FakeMemoryWriteStore::default(),
            Arc::clone(&trigger),
            " memory-model ",
        );

        let report = openplotva_server::RuntimeMemoryRestarter::restart(&restarter, Some(42))
            .await
            .expect("restart");

        assert!(report.ok);
        assert_eq!(report.run_id, Some(42));
        assert_eq!(report.retried_failed_runs, 1);
        assert_eq!(report.queued_runs, 0);
        assert!(report.started);
        assert!(!report.override_);
        assert_eq!(report.provider.as_deref(), Some("aifarm"));
        assert_eq!(report.model.as_deref(), Some("memory-model"));
        assert_eq!(
            restarter
                .store
                .retried_runs
                .lock()
                .expect("retried runs")
                .as_slice(),
            &[42]
        );
        tokio::time::timeout(Duration::from_millis(100), trigger.notified())
            .await
            .expect("worker trigger");
    }

    #[tokio::test]
    async fn runtime_memory_restarter_retries_all_failed_runs_with_default_model() {
        let trigger = Arc::new(tokio::sync::Notify::new());
        let restarter = RuntimeMemoryRestarter::new(
            FakeMemoryWriteStore {
                retry_failed_count: 7,
                ..FakeMemoryWriteStore::default()
            },
            Arc::clone(&trigger),
            " ",
        );

        let report = openplotva_server::RuntimeMemoryRestarter::restart(&restarter, None)
            .await
            .expect("restart");

        assert!(report.ok);
        assert_eq!(report.run_id, None);
        assert_eq!(report.retried_failed_runs, 7);
        assert_eq!(
            report.model.as_deref(),
            Some(openplotva_memory::DEFAULT_MEMORY_CONSOLIDATION_MODEL)
        );
        assert_eq!(
            *restarter
                .store
                .retry_failed_calls
                .lock()
                .expect("retry failed calls"),
            1
        );
        tokio::time::timeout(Duration::from_millis(100), trigger.notified())
            .await
            .expect("worker trigger");
    }

    #[test]
    fn discovery_redactor_config_maps_go_memory_redaction_env() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            discovery_base_url: Some("http://discovery.test".to_owned()),
            memory_redaction_enabled: Some("true".to_owned()),
            memory_redaction_service_name: Some("privacy".to_owned()),
            memory_redaction_endpoint_name: Some("redact_text".to_owned()),
            memory_redaction_timeout_seconds: Some("12".to_owned()),
            memory_redaction_task_timeout_seconds: Some("13".to_owned()),
            memory_redaction_poll_seconds: Some("2".to_owned()),
            memory_redaction_capacity_wait_seconds: Some("0".to_owned()),
            memory_redaction_capacity_poll_seconds: Some("3".to_owned()),
            memory_redaction_categories: Some(" private_email, secret ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        let cfg = discovery_redactor_config_from_app_config(&config).expect("enabled");

        assert_eq!(cfg.base_url, "http://discovery.test");
        assert_eq!(cfg.service_name, "privacy");
        assert_eq!(cfg.endpoint_name, "redact_text");
        assert_eq!(cfg.timeout, Duration::from_secs(12));
        assert_eq!(cfg.task_timeout, Duration::from_secs(13));
        assert_eq!(cfg.poll_interval, Duration::from_secs(2));
        assert_eq!(
            cfg.capacity_wait,
            openplotva_memory::DEFAULT_MEMORY_REDACTION_CAPACITY_WAIT
        );
        assert_eq!(cfg.capacity_poll_interval, Duration::from_secs(3));
        assert_eq!(cfg.categories, vec!["private_email", "secret"]);
    }

    #[test]
    fn discovery_redactor_config_respects_disabled_redaction() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_redaction_enabled: Some("false".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        assert!(discovery_redactor_config_from_app_config(&config).is_none());
    }

    #[test]
    fn app_memory_extractor_composes_fail_open_redaction_when_enabled() {
        let redacting = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_redaction_enabled: Some("true".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("redacting config");
        let plain = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_redaction_enabled: Some("false".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("plain config");

        assert!(matches!(
            memory_extractor_from_app_config(&redacting).expect("redacting extractor"),
            AppMemoryExtractor::RedactingAifarmRoute(_)
        ));
        assert!(matches!(
            memory_extractor_from_app_config(&plain).expect("plain extractor"),
            AppMemoryExtractor::AifarmRoute(_)
        ));
    }

    #[test]
    fn app_memory_extractor_keeps_aifarm_route_when_google_key_resolves() {
        let plain = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some(" google-key ".to_owned()),
            memory_redaction_enabled: Some("false".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("plain config");
        let redacting = AppConfig::from_raw(openplotva_config::RawConfig {
            googleai_key: Some(" google-key ".to_owned()),
            memory_redaction_enabled: Some("true".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("redacting config");

        assert!(matches!(
            memory_extractor_from_app_config(&plain).expect("route extractor"),
            AppMemoryExtractor::AifarmRoute(_)
        ));
        assert!(matches!(
            memory_extractor_from_app_config_with_override(
                &redacting,
                Some("aifarm"),
                Some(" override-model ")
            )
            .expect("redacting route extractor"),
            AppMemoryExtractor::RedactingAifarmRoute(_)
        ));
    }

    #[test]
    fn app_memory_extractor_builds_genkit_gemini_when_configured() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_consolidation_provider: Some(" genkit ".to_owned()),
            memory_redaction_enabled: Some("false".to_owned()),
            googleai_key: Some(" google-key ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        assert!(matches!(
            memory_extractor_from_app_config(&config).expect("gemini extractor"),
            AppMemoryExtractor::GeminiPlain(_)
        ));
        assert_eq!(
            genkit_memory_extractor_model(&config, None),
            MODEL_GEMINI_FLASH_LITE
        );
        assert_eq!(
            genkit_memory_extractor_model(&config, Some(" custom-model ")),
            "custom-model"
        );
    }

    #[test]
    fn app_memory_extractor_builds_genkit_openai_compatible_models() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_consolidation_provider: Some(" genkit ".to_owned()),
            memory_redaction_enabled: Some("false".to_owned()),
            genkit_default_model: Some(" openrouter/provider-model ".to_owned()),
            openrouter_key: Some(" openrouter-key ".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        assert!(matches!(
            memory_extractor_from_app_config(&config).expect("openrouter extractor"),
            AppMemoryExtractor::GeminiPlain(AppGenkitMemoryExtractor::OpenAiCompatible(_))
        ));
        assert_eq!(
            genkit_memory_extractor_model(&config, None),
            "openrouter/provider-model"
        );
    }

    #[test]
    fn app_memory_extractor_requires_google_key_for_genkit() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_consolidation_provider: Some("gemini".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        assert!(matches!(
            memory_extractor_from_app_config(&config),
            Err(AppMemoryExtractorBuildError::GoogleAiKeyRequired)
        ));
        assert_eq!(
            genkit_memory_extractor_model(&config, None),
            MODEL_GEMINI_FLASH_FALLBACK
        );
    }

    #[test]
    fn app_memory_extractor_requires_plugin_key_for_genkit_openai_compatible_models() {
        let config = AppConfig::from_raw(openplotva_config::RawConfig {
            memory_consolidation_provider: Some("genkit".to_owned()),
            genkit_default_model: Some("openrouter/provider-model".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config");

        assert!(matches!(
            memory_extractor_from_app_config(&config),
            Err(AppMemoryExtractorBuildError::ProviderApiKeyRequired {
                provider: "openrouter"
            })
        ));
    }

    #[test]
    fn memory_embedder_builds_discovery_client_with_defaults() {
        let memory = AppConfig::from_raw(openplotva_config::RawConfig::default())
            .expect("config")
            .memory;
        assert!(
            memory_retrieval_embedder_from_config(&memory, "   ")
                .expect("blank")
                .is_none()
        );

        let client = memory_retrieval_embedder_from_config(&memory, "http://discovery.test")
            .expect("client")
            .expect("memory embedder configured");
        assert_eq!(client.config().base_url, "http://discovery.test");
        assert_eq!(client.config().service_name, "embedder");
        assert_eq!(client.config().endpoint_name, "encode");
        assert_eq!(
            client.config().request_timeout,
            MEMORY_RETRIEVAL_EMBEDDING_TIMEOUT
        );
    }

    #[test]
    fn embedder_encode_request_matches_go_json_shape() {
        let request = EmbedderEncodeRequest {
            prompts: vec!["hello".to_owned()],
            dimension: 512,
            task_description: "task".to_owned(),
        };
        let value = serde_json::to_value(&request).expect("json");

        assert_eq!(
            value,
            serde_json::json!({
                "prompts": ["hello"],
                "dimension": 512,
                "task_description": "task"
            })
        );
    }

    #[test]
    fn shield_embedder_uses_shield_timeout_and_blank_url_disables() {
        let shield = AppConfig::from_raw(openplotva_config::RawConfig {
            shield_retrieval_timeout_seconds: Some("7".to_owned()),
            ..openplotva_config::RawConfig::default()
        })
        .expect("config")
        .shield;
        assert!(
            shield_embedder_from_config(&shield, "")
                .expect("blank")
                .is_none()
        );

        let client = shield_embedder_from_config(&shield, "http://discovery.test")
            .expect("client")
            .expect("shield embedder configured");
        assert_eq!(client.config().base_url, "http://discovery.test");
        assert_eq!(client.config().service_name, "embedder");
        assert_eq!(client.config().request_timeout, Duration::from_secs(7));
    }

    #[tokio::test]
    async fn write_memory_extraction_batch_persists_cards_links_supersessions_and_episode() {
        let input = ExtractInput {
            run: openplotva_memory::Run {
                chat_id: 42,
                thread_id: 9,
                range_start_at: OffsetDateTime::UNIX_EPOCH,
                range_end_at: OffsetDateTime::UNIX_EPOCH + time::Duration::minutes(5),
                prompt_version: "pv".to_owned(),
                ..openplotva_memory::Run::default()
            },
            messages: vec![
                openplotva_memory::Message {
                    entry_id: "msg:1".to_owned(),
                    message_id: 1,
                    user_id: 100,
                    occurred_at: OffsetDateTime::UNIX_EPOCH,
                    ..openplotva_memory::Message::default()
                },
                openplotva_memory::Message {
                    entry_id: "msg:2".to_owned(),
                    message_id: 2,
                    user_id: 200,
                    occurred_at: OffsetDateTime::UNIX_EPOCH,
                    ..openplotva_memory::Message::default()
                },
            ],
            ..ExtractInput::default()
        };
        let output = ExtractOutput {
            episode_summary: "  Summary  ".to_owned(),
            topics: vec![" Rust ".to_owned(), "rust".to_owned()],
            participants: vec![" Alice ".to_owned()],
            candidate_cards: vec![
                openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice likes Rust".to_owned(),
                    source_entry_ids: vec!["msg:1".to_owned()],
                    confidence: 0.7,
                    salience: 0.8,
                    ..openplotva_memory::CandidateCard::default()
                },
                openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Bob likes Rust".to_owned(),
                    source_message_ids: vec![2],
                    confidence: 0.6,
                    salience: 0.9,
                    ..openplotva_memory::CandidateCard::default()
                },
            ],
            links: vec![openplotva_memory::CandidateLink {
                from_fact_text: "alice LIKES rust".to_owned(),
                to_fact_text: "Bob likes Rust".to_owned(),
                relation: "same_topic".to_owned(),
                confidence: 0.4,
            }],
            supersessions: vec![openplotva_memory::Supersession {
                old_card_id: 77,
                new_fact_text: "Bob likes Rust".to_owned(),
                reason: "new".to_owned(),
            }],
            resolutions: vec![openplotva_memory::Resolution {
                old_card_id: 88,
                new_fact_text: "Alice likes Rust".to_owned(),
                decision: openplotva_memory::ResolutionDecision::Competing,
                conflict_score: 0.5,
                reason: "both plausible".to_owned(),
            }],
            input_tokens: 11,
            output_tokens: 12,
        };
        let store = FakeMemoryWriteStore {
            ids: vec![101, 102],
            ..FakeMemoryWriteStore::default()
        };
        let embedder = FakeEmbedder::default();

        let report = write_memory_extraction_batch(
            &store,
            Some(&embedder),
            &input,
            output,
            MemoryExtractionBatchConfig {
                episode_model: "model",
                prompt_version: "pv",
                embedding_dimension: 2,
                fallback_observed_at: OffsetDateTime::UNIX_EPOCH,
                batch_input_tokens: 99,
            },
        )
        .await
        .expect("write report");

        assert_eq!(report.stats.cards_inserted, 2);
        assert_eq!(report.stats.cards_superseded, 1);
        assert_eq!(report.stats.episodes_inserted, 1);
        assert_eq!(report.stats.input_tokens, 11);
        assert_eq!(report.stats.output_tokens, 12);
        assert_eq!(report.card_ids, vec![101, 102]);
        assert_eq!(report.links_inserted, 1);
        assert_eq!(report.superseded, vec![(77, 102)]);
        // A competing resolution keeps both facts (new fact resolves to card 101).
        assert_eq!(report.competing, vec![(88, 101)]);
        assert_eq!(report.stats.cards_competing, 1);
        assert_eq!(
            store
                .card_embedding_slots
                .lock()
                .expect("embedding slots")
                .as_slice(),
            &[true, true]
        );
        assert_eq!(
            store.links.lock().expect("links").as_slice(),
            &[LinkInput {
                from_card_id: 101,
                to_card_id: 102,
                relation: "same_topic".to_owned(),
                confidence: 0.4,
            }]
        );
        assert_eq!(
            store.superseded.lock().expect("superseded").as_slice(),
            &[(77, 102)]
        );
        let episode = store.episodes.lock().expect("episodes")[0].clone();
        assert_eq!(episode.summary_text, "Summary");
        assert_eq!(episode.topics, vec!["Rust"]);
        assert_eq!(episode.participants, vec!["Alice"]);
        assert!(
            *store
                .episode_had_embedding
                .lock()
                .expect("episode embedding")
        );
        let calls = embedder.calls.lock().expect("calls");
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].2, MEMORY_CARD_EMBEDDING_TASK);
        assert_eq!(calls[2].2, MEMORY_EPISODE_EMBEDDING_TASK);
    }

    #[tokio::test]
    async fn process_next_memory_run_claims_filters_writes_continuation_and_completes() {
        let run = openplotva_memory::Run {
            id: 9,
            chat_id: 42,
            thread_id: 3,
            range_start_at: OffsetDateTime::UNIX_EPOCH,
            range_end_at: OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 3,
            ..openplotva_memory::Run::default()
        };
        let messages = vec![
            openplotva_memory::Message {
                entry_id: "msg:1".to_owned(),
                message_id: 1,
                text: "Alice likes Rust".to_owned(),
                occurred_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
                ..openplotva_memory::Message::default()
            },
            openplotva_memory::Message {
                entry_id: "msg:2".to_owned(),
                message_id: 2,
                text: "bot noise".to_owned(),
                sender_is_bot: true,
                occurred_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2),
                ..openplotva_memory::Message::default()
            },
            openplotva_memory::Message {
                entry_id: "msg:3".to_owned(),
                message_id: 3,
                text: "later".to_owned(),
                occurred_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(3),
                ..openplotva_memory::Message::default()
            },
        ];
        let store = FakeMemoryWriteStore {
            ids: vec![101],
            run: Mutex::new(Some(run)),
            messages: Mutex::new(messages),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput {
                episode_summary: "Summary".to_owned(),
                candidate_cards: vec![openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice likes Rust".to_owned(),
                    source_entry_ids: vec!["msg:1".to_owned()],
                    confidence: 0.8,
                    salience: 0.9,
                    ..openplotva_memory::CandidateCard::default()
                }],
                output_tokens: 7,
                ..ExtractOutput::default()
            },
            inputs: Mutex::new(Vec::new()),
        };

        let report = process_next_memory_run(
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            MemoryRunProcessConfig {
                max_messages_per_run: 2,
                episode_model: "model".to_owned(),
                ..MemoryRunProcessConfig::default()
            },
        )
        .await
        .expect("processed");

        assert!(report.processed);
        assert_eq!(report.run_id, 9);
        assert_eq!(report.loaded_messages, 3);
        assert_eq!(report.selected_messages, 2);
        assert_eq!(report.filtered_messages, 1);
        assert!(report.continuation_queued);
        assert_eq!(
            store
                .continuations
                .lock()
                .expect("continuations")
                .as_slice(),
            &[(2, 1)]
        );
        let inputs = extractor.inputs.lock().expect("inputs");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].messages.len(), 1);
        assert_eq!(inputs[0].messages[0].entry_id, "msg:1");
        let completed = store.completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, 9);
        assert_eq!(completed[0].1.cards_inserted, 1);
        assert_eq!(completed[0].1.episodes_inserted, 1);
        assert!(store.failed.lock().expect("failed").is_empty());
    }

    #[tokio::test]
    async fn process_next_memory_run_skips_when_embedder_unavailable() {
        let run = openplotva_memory::Run {
            id: 7,
            chat_id: 1,
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            ..openplotva_memory::Run::default()
        };
        let store = FakeMemoryWriteStore {
            run: Mutex::new(Some(run)),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput::default(),
            inputs: Mutex::new(Vec::new()),
        };

        let report = process_next_memory_run(
            &extractor,
            &store,
            Some(&UnavailableEmbedder),
            MemoryRunProcessConfig::default(),
        )
        .await
        .expect("gated");

        assert!(
            !report.processed,
            "consolidation must not run while the embedder is down"
        );
        assert!(
            store.run.lock().expect("run").is_some(),
            "run stays queued; it was never claimed"
        );
        assert!(store.completed.lock().expect("completed").is_empty());
        assert!(store.failed.lock().expect("failed").is_empty());
        assert!(store.released.lock().expect("released").is_empty());
        assert!(extractor.inputs.lock().expect("inputs").is_empty());
    }

    #[tokio::test]
    async fn process_next_memory_run_releases_run_when_embedder_fails_midrun() {
        let run = openplotva_memory::Run {
            id: 11,
            chat_id: 42,
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 1,
            ..openplotva_memory::Run::default()
        };
        let messages = vec![openplotva_memory::Message {
            entry_id: "msg:1".to_owned(),
            message_id: 1,
            text: "Alice likes Rust".to_owned(),
            occurred_at: OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1),
            ..openplotva_memory::Message::default()
        }];
        let store = FakeMemoryWriteStore {
            ids: vec![101],
            run: Mutex::new(Some(run)),
            messages: Mutex::new(messages),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput {
                episode_summary: "Summary".to_owned(),
                candidate_cards: vec![openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice likes Rust".to_owned(),
                    source_entry_ids: vec!["msg:1".to_owned()],
                    confidence: 0.8,
                    salience: 0.9,
                    ..openplotva_memory::CandidateCard::default()
                }],
                output_tokens: 7,
                ..ExtractOutput::default()
            },
            inputs: Mutex::new(Vec::new()),
        };

        let report = process_next_memory_run(
            &extractor,
            &store,
            Some(&MidRunFailingEmbedder),
            MemoryRunProcessConfig {
                max_messages_per_run: 10,
                episode_model: "model".to_owned(),
                ..MemoryRunProcessConfig::default()
            },
        )
        .await
        .expect("aborted without surfacing an error");

        assert!(!report.processed);
        assert_eq!(
            store.released.lock().expect("released").as_slice(),
            &[11],
            "the run is released back to the queue"
        );
        assert!(
            store.failed.lock().expect("failed").is_empty(),
            "an embedder outage must not burn the run's retry budget"
        );
        assert!(
            store.completed.lock().expect("completed").is_empty(),
            "the run must not complete with partial data"
        );
    }

    #[tokio::test]
    async fn process_next_memory_run_selects_by_token_budget_and_adds_context() {
        let run = openplotva_memory::Run {
            id: 91,
            chat_id: -100,
            thread_id: 5,
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 3,
            ..openplotva_memory::Run::default()
        };
        let messages = vec![
            memory_user_message(1, 10, "short first message"),
            memory_user_message(2, 20, &"x".repeat(4_000)),
            memory_user_message(3, 30, "third message"),
        ];
        let store = FakeMemoryWriteStore {
            ids: vec![910],
            run: Mutex::new(Some(run)),
            chat_meta: Mutex::new(Some(DialogMemoryChatMeta {
                chat_type: "supergroup".to_owned(),
                username: "plotva_lab".to_owned(),
                active_usernames: vec!["plotva_lab".to_owned(), "plotva_alt".to_owned()],
            })),
            visible_cards: Mutex::new(vec![memory_card(42, "Alice likes Rust")]),
            messages: Mutex::new(messages),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput {
                episode_summary: "Summary".to_owned(),
                candidate_cards: vec![openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice likes Rust".to_owned(),
                    source_entry_ids: vec!["msg:1".to_owned()],
                    confidence: 0.8,
                    salience: 0.8,
                    ..openplotva_memory::CandidateCard::default()
                }],
                ..ExtractOutput::default()
            },
            inputs: Mutex::new(Vec::new()),
        };

        let report = process_next_memory_run(
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            MemoryRunProcessConfig {
                max_messages_per_run: 10,
                max_input_tokens: 900,
                ..MemoryRunProcessConfig::default()
            },
        )
        .await
        .expect("processed");

        assert!(report.continuation_queued);
        assert_eq!(report.selected_messages, 1);
        assert_eq!(
            store
                .continuations
                .lock()
                .expect("continuations")
                .as_slice(),
            &[(1, 2)]
        );
        let inputs = extractor.inputs.lock().expect("inputs");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].chat_type, "supergroup");
        assert_eq!(inputs[0].chat_username, "plotva_lab");
        assert_eq!(
            inputs[0].chat_active_usernames,
            vec!["plotva_lab".to_owned(), "plotva_alt".to_owned()]
        );
        assert_eq!(inputs[0].messages.len(), 1);
        assert_eq!(inputs[0].existing_cards.len(), 1);
        assert_eq!(inputs[0].existing_cards[0].id, 42);
        let scopes = store.visible_card_scopes.lock().expect("scopes");
        assert!(scopes.iter().any(|scope| scope.user_id == 10));
    }

    #[tokio::test]
    async fn process_next_memory_run_splits_oversized_message_and_inserts_one_episode() {
        let run = openplotva_memory::Run {
            id: 92,
            chat_id: -100,
            thread_id: 7,
            range_start_at: OffsetDateTime::UNIX_EPOCH,
            range_end_at: OffsetDateTime::UNIX_EPOCH + time::Duration::hours(1),
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 1,
            ..openplotva_memory::Run::default()
        };
        let store = FakeMemoryWriteStore {
            ids: (200..260).collect(),
            run: Mutex::new(Some(run)),
            messages: Mutex::new(vec![memory_user_message(1, 10, &"wide words ".repeat(900))]),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput {
                episode_summary: " Batch summary ".to_owned(),
                topics: vec![" Rust ".to_owned(), "rust".to_owned()],
                participants: vec![" Alice ".to_owned()],
                candidate_cards: vec![openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice talks about Rust".to_owned(),
                    source_entry_ids: vec!["msg:1".to_owned()],
                    confidence: 0.8,
                    salience: 0.8,
                    ..openplotva_memory::CandidateCard::default()
                }],
                output_tokens: 3,
                ..ExtractOutput::default()
            },
            inputs: Mutex::new(Vec::new()),
        };

        let report = process_next_memory_run(
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            MemoryRunProcessConfig {
                max_messages_per_run: 10,
                max_input_tokens: 450,
                episode_model: "model".to_owned(),
                ..MemoryRunProcessConfig::default()
            },
        )
        .await
        .expect("processed");

        let inputs = extractor.inputs.lock().expect("inputs");
        assert!(inputs.len() > 1);
        assert!(inputs.iter().all(|input| input.messages.len() == 1));
        assert!(
            inputs
                .iter()
                .all(|input| !input.messages[0].text.trim().is_empty())
        );
        let batch_count = inputs.len();
        drop(inputs);

        let completed = store.completed.lock().expect("completed");
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].0, 92);
        assert_eq!(completed[0].1.cards_inserted, batch_count as i32);
        assert_eq!(completed[0].1.episodes_inserted, 1);
        assert_eq!(completed[0].1.output_tokens, (batch_count * 3) as i32);

        let write = report.write.expect("write");
        assert_eq!(write.stats, completed[0].1);
        assert_eq!(write.output.topics, vec!["Rust"]);
        assert_eq!(write.output.participants, vec!["Alice"]);
        assert_eq!(
            write.output.episode_summary,
            vec!["Batch summary"; batch_count].join("\n\n")
        );

        let episodes = store.episodes.lock().expect("episodes");
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes[0].message_count, 1);
        assert_eq!(episodes[0].summary_text, write.output.episode_summary);
        assert_eq!(episodes[0].topics, vec!["Rust"]);
        assert_eq!(episodes[0].participants, vec!["Alice"]);
    }

    #[test]
    fn memory_schedule_matches_go_daily_window_rules() {
        let before = utc_test_time(3, 16);
        let after = utc_test_time(3, 17);
        assert!(!memory_schedule_due_at(before, "03:17"));
        assert!(memory_schedule_due_at(after, "03:17"));
        assert!(memory_schedule_due_at(after, "03:17:00"));

        let inside = utc_test_time(5, 0);
        let outside = utc_test_time(8, 0);
        assert!(memory_schedule_active_at(
            inside,
            "03:17",
            Duration::from_secs(4 * 60 * 60)
        ));
        assert!(!memory_schedule_active_at(
            outside,
            "03:17",
            Duration::from_secs(4 * 60 * 60)
        ));
    }

    #[tokio::test]
    async fn memory_service_startup_ensures_daily_runs_and_drains_queue() {
        let now = utc_test_time(3, 17);
        let run = openplotva_memory::Run {
            id: 90,
            chat_id: -100,
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 1,
            ..openplotva_memory::Run::default()
        };
        let store = FakeMemoryWriteStore {
            ensure_daily_count: 4,
            ids: vec![901],
            run: Mutex::new(Some(run)),
            messages: Mutex::new(vec![openplotva_memory::Message {
                entry_id: "msg:10".to_owned(),
                message_id: 10,
                user_id: 7,
                sender_type: openplotva_core::SENDER_TYPE_USER.to_owned(),
                text: "Alice prefers Rust".to_owned(),
                occurred_at: now,
                ..openplotva_memory::Message::default()
            }]),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput {
                episode_summary: "Summary".to_owned(),
                candidate_cards: vec![openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice prefers Rust".to_owned(),
                    source_entry_ids: vec!["msg:10".to_owned()],
                    confidence: 0.7,
                    salience: 0.8,
                    ..openplotva_memory::CandidateCard::default()
                }],
                ..ExtractOutput::default()
            },
            inputs: Mutex::new(Vec::new()),
        };

        let report = run_memory_service_once_at(
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            &MemoryServiceWorkerConfig {
                retention: Duration::from_secs(12 * 60 * 60),
                daily_window: Duration::from_secs(60),
                ..MemoryServiceWorkerConfig::default()
            },
            now,
            true,
        )
        .await;

        assert!(report.enabled);
        assert!(report.startup);
        assert_eq!(report.queued_daily_runs, 4);
        assert_eq!(report.processed_runs, 1);
        assert_eq!(report.failed_runs, 0);
        assert_eq!(report.store_errors, 0);
        assert_eq!(store.ensured.lock().expect("ensured").len(), 1);
        assert_eq!(store.completed.lock().expect("completed")[0].0, 90);
        assert_eq!(extractor.inputs.lock().expect("inputs").len(), 1);
    }

    #[tokio::test]
    async fn memory_taskman_scheduler_ensures_daily_runs_and_assigns_go_job() {
        let now = utc_test_time(3, 17);
        let store = FakeMemoryWriteStore {
            ensure_daily_count: 3,
            ..FakeMemoryWriteStore::default()
        };
        let queue = openplotva_taskman::InMemoryTaskQueue::new();

        let report = schedule_memory_consolidation_taskman_once_at(
            &store,
            &queue,
            &MemoryServiceWorkerConfig {
                retention: Duration::from_secs(12 * 60 * 60),
                daily_window: Duration::from_secs(60),
                ..MemoryServiceWorkerConfig::default()
            },
            now,
            true,
            true,
        )
        .await;

        assert_eq!(
            report,
            MemoryConsolidationQueueScheduleReport {
                enabled: true,
                startup: true,
                daily_window_active: true,
                queued_daily_runs: 3,
                assigned_job_id: Some(1),
                store_error: None,
            }
        );
        assert_eq!(store.ensured.lock().expect("ensured").len(), 1);
        let work = queue
            .dequeue(
                openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME,
                "memory-worker",
                now,
            )
            .expect("memory job");
        assert_eq!(
            work.job.data.job_type,
            openplotva_taskman::JobType::MemoryConsolidation
        );
        assert_eq!(work.job.title, "memory_consolidation");
        assert_eq!(work.job.priority, openplotva_taskman::LOWEST_PRIORITY);
        assert!(work.job.data.telegram_data.is_none());
    }

    #[tokio::test]
    async fn memory_taskman_scheduler_skips_assignment_when_pending_lowest_priority_job_exists() {
        let now = utc_test_time(3, 17);
        let store = FakeMemoryWriteStore::default();
        let queue = openplotva_taskman::InMemoryTaskQueue::new();

        let first = schedule_memory_consolidation_taskman_once_at(
            &store,
            &queue,
            &MemoryServiceWorkerConfig::default(),
            now,
            true,
            false,
        )
        .await;
        let second = schedule_memory_consolidation_taskman_once_at(
            &store,
            &queue,
            &MemoryServiceWorkerConfig::default(),
            now,
            false,
            false,
        )
        .await;

        assert_eq!(first.assigned_job_id, Some(1));
        assert_eq!(second.assigned_job_id, None);
        assert_eq!(
            queue.queue_depth(
                openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME,
                openplotva_taskman::LOWEST_PRIORITY,
            ),
            1
        );
    }

    #[tokio::test]
    async fn memory_taskman_worker_processes_one_run_and_schedules_followup() {
        let now = utc_test_time(3, 17);
        let run = openplotva_memory::Run {
            id: 91,
            chat_id: -100,
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 1,
            ..openplotva_memory::Run::default()
        };
        let store = FakeMemoryWriteStore {
            ids: vec![910],
            run: Mutex::new(Some(run)),
            messages: Mutex::new(vec![openplotva_memory::Message {
                entry_id: "msg:11".to_owned(),
                message_id: 11,
                user_id: 7,
                sender_type: openplotva_core::SENDER_TYPE_USER.to_owned(),
                text: "Alice likes Rust queues".to_owned(),
                occurred_at: now,
                ..openplotva_memory::Message::default()
            }]),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput {
                episode_summary: "Summary".to_owned(),
                candidate_cards: vec![openplotva_memory::CandidateCard {
                    scope_type: openplotva_memory::CARD_KIND_CHAT.to_owned(),
                    fact_text: "Alice likes Rust queues".to_owned(),
                    source_entry_ids: vec!["msg:11".to_owned()],
                    confidence: 0.7,
                    salience: 0.8,
                    ..openplotva_memory::CandidateCard::default()
                }],
                ..ExtractOutput::default()
            },
            inputs: Mutex::new(Vec::new()),
        };
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let first_id = queue.assign(
            openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME,
            openplotva_taskman::new_memory_consolidation_job_at(now),
        );

        let report = process_memory_consolidation_taskman_job_once_at(
            &queue,
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            MemoryRunProcessConfig::default(),
            "memory-consolidation-worker-0",
            1,
            now,
        )
        .await;

        assert_eq!(
            report,
            MemoryConsolidationQueueWorkerReport {
                dequeued: true,
                job_id: Some(first_id),
                processed_run: true,
                completed: true,
                failed: false,
                scheduled_followup: true,
                followup_job_id: Some(2),
                decode_error: None,
                process_error: None,
                status_error: None,
            }
        );
        assert_eq!(store.completed.lock().expect("completed")[0].0, 91);
        assert_eq!(extractor.inputs.lock().expect("inputs").len(), 1);
        assert_eq!(
            queue.record(first_id).expect("first job").status,
            openplotva_taskman::JobStatus::Completed
        );
        assert_eq!(
            queue.record(2).expect("followup job").status,
            openplotva_taskman::JobStatus::Pending
        );
    }

    #[tokio::test]
    async fn memory_taskman_pipeline_amplifies_toward_target_while_runs_flow() {
        let now = utc_test_time(3, 17);
        let run = openplotva_memory::Run {
            id: 92,
            chat_id: -100,
            prompt_version: openplotva_memory::PROMPT_VERSION.to_owned(),
            message_count: 1,
            ..openplotva_memory::Run::default()
        };
        let store = FakeMemoryWriteStore {
            ids: vec![920],
            run: Mutex::new(Some(run)),
            messages: Mutex::new(vec![openplotva_memory::Message {
                entry_id: "msg:12".to_owned(),
                message_id: 12,
                user_id: 7,
                sender_type: openplotva_core::SENDER_TYPE_USER.to_owned(),
                text: "Bob likes parallel queues".to_owned(),
                occurred_at: now,
                ..openplotva_memory::Message::default()
            }]),
            ..FakeMemoryWriteStore::default()
        };
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput::default(),
            inputs: Mutex::new(Vec::new()),
        };
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        queue.assign(
            openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME,
            openplotva_taskman::new_memory_consolidation_job_at(now),
        );

        let report = process_memory_consolidation_taskman_job_once_at(
            &queue,
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            MemoryRunProcessConfig::default(),
            "memory-consolidation-worker-0",
            4,
            now,
        )
        .await;

        assert!(report.processed_run);
        assert!(report.scheduled_followup);
        assert_eq!(
            queue.queue_depth(
                openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME,
                openplotva_taskman::LOWEST_PRIORITY,
            ),
            2,
            "a productive tick adds the followup plus one amplification job"
        );
    }

    #[tokio::test]
    async fn memory_taskman_worker_fails_invalid_job_type_like_go_executor() {
        let now = utc_test_time(3, 17);
        let queue = openplotva_taskman::InMemoryTaskQueue::new();
        let bad_id = queue.assign(
            openplotva_taskman::MEMORY_CONSOLIDATION_QUEUE_NAME,
            openplotva_taskman::new_dialog_job_at(
                openplotva_taskman::DialogJobParams {
                    chat_id: 1,
                    message_id: 2,
                    user_id: 3,
                    user_full_name: "Alice".to_owned(),
                    message_text: "hello".to_owned(),
                    original_text: String::new(),
                    meta: serde_json::Value::Null,
                    max_output_tokens: 0,
                    thread_id: None,
                },
                now,
            ),
        );
        let store = FakeMemoryWriteStore::default();
        let extractor = FakeMemoryExtractor {
            output: ExtractOutput::default(),
            inputs: Mutex::new(Vec::new()),
        };

        let report = process_memory_consolidation_taskman_job_once_at(
            &queue,
            &extractor,
            &store,
            Option::<&FakeEmbedder>::None,
            MemoryRunProcessConfig::default(),
            "memory-consolidation-worker-0",
            1,
            now,
        )
        .await;

        assert!(report.dequeued);
        assert_eq!(report.job_id, Some(bad_id));
        assert!(report.failed);
        assert_eq!(
            report.decode_error.as_deref(),
            Some("invalid job type for MemoryConsolidationJobExecutor: dialog")
        );
        let record = queue.record(bad_id).expect("failed job");
        assert_eq!(record.status, openplotva_taskman::JobStatus::Failed);
        assert_eq!(
            record.error.as_deref(),
            Some("invalid job type for MemoryConsolidationJobExecutor: dialog")
        );
    }

    fn utc_test_time(hour: u8, minute: u8) -> OffsetDateTime {
        time::Date::from_calendar_date(2026, time::Month::April, 26)
            .expect("date")
            .with_hms(hour, minute, 0)
            .expect("time")
            .assume_utc()
    }

    fn memory_user_message(
        message_id: i32,
        user_id: i64,
        text: &str,
    ) -> openplotva_memory::Message {
        openplotva_memory::Message {
            entry_id: format!("msg:{message_id}"),
            message_id,
            user_id,
            sender_type: openplotva_core::SENDER_TYPE_USER.to_owned(),
            text: text.to_owned(),
            occurred_at: utc_test_time(3, 17),
            ..openplotva_memory::Message::default()
        }
    }

    fn memory_card(id: i64, fact_text: &str) -> Card {
        Card {
            id,
            visibility: openplotva_memory::VISIBILITY_CHAT.to_owned(),
            card_type: openplotva_memory::CARD_TYPE_PREFERENCE.to_owned(),
            status: openplotva_memory::CARD_STATUS_ACTIVE.to_owned(),
            subject: "Alice".to_owned(),
            predicate: "likes".to_owned(),
            object: "Rust".to_owned(),
            fact_text: fact_text.to_owned(),
            confidence: 0.8,
            salience: 0.8,
            observation_count: 1,
            origin_chat_id: -100,
            origin_user_id: 10,
            chat_id: -100,
            thread_id: 0,
            user_id: 0,
            valid_from: None,
            valid_until: None,
            last_observed_at: None,
            last_used_at: None,
            use_count: 0,
            created_at: None,
            updated_at: None,
            ..Card::default()
        }
    }
}
